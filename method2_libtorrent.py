import http.server
import json
import os
import re
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import libtorrent as lt

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

FALLBACK_TRACKERS = [
    "http://tracker.opentrackr.org:1337/announce",
    "https://tracker.tamersunion.org:443/announce",
    "https://tracker.gbitt.info:443/announce",
    "http://tracker.gbitt.info:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
]

LIVE_TRACKER_LISTS = [
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all_http.txt",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all_https.txt",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt",
]

METADATA_TIMEOUT_SEC = 60
PIECE_TIMEOUT_SEC = 60
CHUNK_SIZE = 512 * 1024  # 512 KB per range step
VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".mov", ".avi")


def fetch_live_trackers(max_trackers=150):
    seen = set()
    trackers = []

    for url in LIVE_TRACKER_LISTS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                text = resp.read().decode("utf-8", errors="ignore")
            for line in text.splitlines():
                tr = line.strip()
                if tr and not tr.startswith("#") and tr not in seen:
                    seen.add(tr)
                    trackers.append(tr)
                    if len(trackers) >= max_trackers:
                        return trackers
        except Exception as e:
            print(f"Tracker list warning ({url}): {e}")

    for tr in FALLBACK_TRACKERS:
        if tr not in seen:
            seen.add(tr)
            trackers.append(tr)

    return trackers


def try_fetch_cached_torrent(magnet_uri, save_path):
    """Optional fast path: fetch .torrent metadata from public cache by infohash."""
    match = re.search(r"xt=urn:btih:([0-9a-fA-F]{40})", magnet_uri)
    if not match:
        return None
    ih = match.group(1).upper()
    cache_urls = [
        f"https://itorrents.org/torrent/{ih}.torrent",
        f"https://torrage.info/torrent.php?h={ih}",
    ]
    for url in cache_urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6) as resp:
                data = resp.read()
            if data and data[:1] == b"d":
                torrent_path = os.path.join(save_path, f"{ih}.torrent")
                with open(torrent_path, "wb") as f:
                    f.write(data)
                ti = lt.torrent_info(torrent_path)
                print(f"Loaded .torrent metadata instantly from cache ({url}).")
                return ti
        except Exception:
            continue
    return None


def get_alert_mask():
    if hasattr(lt, "alert_category"):
        return lt.alert_category.error | lt.alert_category.peer | lt.alert_category.storage
    return (
        lt.alert.category_t.error_notification
        | lt.alert.category_t.peer_notification
        | lt.alert.category_t.storage_notification
    )


def wait_for_zero_piece_priorities(ses, handle, target_idx, num_pieces):
    """Avoid libtorrent race where async file_priority() overwrites prioritize_pieces([0]*N)."""
    handle.pause()

    current_file_prios = handle.get_file_priorities()
    if not current_file_prios or current_file_prios[target_idx] == 0:
        handle.file_priority(target_idx, 1)
        file_prio_alert_cls = getattr(lt, "file_prio_alert", None)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            got_alert = False
            for alert in ses.pop_alerts():
                if file_prio_alert_cls and isinstance(alert, file_prio_alert_cls):
                    got_alert = True
            if got_alert:
                break
            time.sleep(0.05)

    # Now safely zero every piece and verify before resuming
    handle.prioritize_pieces([0] * num_pieces)
    verify_deadline = time.time() + 3.0
    while time.time() < verify_deadline:
        prios = handle.get_piece_priorities()
        if prios and all(p == 0 for p in prios):
            break
        time.sleep(0.05)

    # Confirm zeroed count in logs
    nonzero = sum(1 for p in handle.get_piece_priorities() if p != 0)
    print(f"Piece priorities locked to 0 before resume (non-zero pieces: {nonzero}).")
    handle.resume()


def probe_video(stream_url):
    proc = subprocess.run(
        [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration",
            "-of", "json",
            stream_url,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout)
    duration = float(data.get("format", {}).get("duration", 0))
    stream = (data.get("streams") or [{}])[0]
    if duration <= 0:
        raise RuntimeError(f"Could not determine video duration: {proc.stdout}")
    return duration, stream.get("width", 0), stream.get("height", 0)


class PieceManager:
    """Enables libtorrent pieces strictly on demand and cancels them if the client disconnects."""

    def __init__(self, handle, info, target_idx, file_path, file_size):
        self.handle = handle
        self.info = info
        self.target_idx = target_idx
        self.file_path = file_path
        self.file_size = file_size
        self.lock = threading.Lock()
        self.active_refs = {}

    def _pieces_for_range(self, start, end):
        p_start = self.info.map_file(self.target_idx, start, 1).piece
        p_end = self.info.map_file(self.target_idx, end, 1).piece
        return list(range(p_start, p_end + 1))

    def ensure_range(self, start, end, stop_event):
        pieces = self._pieces_for_range(start, end)
        pending = [p for p in pieces if not self.handle.have_piece(p)]

        if not pending:
            return True

        with self.lock:
            for idx, p in enumerate(pending):
                self.active_refs[p] = self.active_refs.get(p, 0) + 1
                self.handle.piece_priority(p, 7)
                self.handle.set_piece_deadline(p, idx * 100)

        deadline = time.time() + PIECE_TIMEOUT_SEC
        try:
            while True:
                if stop_event.is_set():
                    return False
                if all(self.handle.have_piece(p) for p in pending):
                    wait_until = time.time() + 2.0
                    while not os.path.exists(self.file_path) and time.time() < wait_until:
                        time.sleep(0.05)
                    return True
                if time.time() > deadline:
                    st = self.handle.status()
                    raise TimeoutError(
                        f"Timed out waiting for pieces {pending} "
                        f"(peers: {st.num_peers}/{st.list_peers}, "
                        f"seeds: {st.num_seeds}/{st.list_seeds}, "
                        f"dl: {st.download_rate / 1024:.1f} KB/s)"
                    )
                time.sleep(0.05)
        finally:
            with self.lock:
                for p in pending:
                    refs = self.active_refs.get(p, 1) - 1
                    if refs <= 0:
                        self.active_refs.pop(p, None)
                        if not self.handle.have_piece(p):
                            self.handle.piece_priority(p, 0)
                            self.handle.reset_piece_deadline(p)
                    else:
                        self.active_refs[p] = refs


class ReusableThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_range_server(piece_mgr):
    class RangeHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _parse_range(self):
            total = piece_mgr.file_size
            range_header = self.headers.get("Range")
            if not range_header:
                return 200, 0, total - 1

            match = re.match(r"bytes=(\d*)-(\d*)", range_header)
            if not match:
                return 200, 0, total - 1

            start = int(match.group(1)) if match.group(1) else 0
            end = int(match.group(2)) if match.group(2) else total - 1
            end = min(end, total - 1)
            if start >= total or start > end:
                return 416, start, end
            return 206, start, end

        def do_HEAD(self):
            if self.path != "/video":
                self.send_error(404)
                return
            status, start, end = self._parse_range()
            if status == 416:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{piece_mgr.file_size}")
                self.end_headers()
                return
            self.send_response(status)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "application/octet-stream")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{piece_mgr.file_size}")
            self.send_header("Content-Length", str(end - start + 1))
            self.end_headers()

        def do_GET(self):
            if self.path != "/video":
                self.send_error(404)
                return

            status, start, end = self._parse_range()
            if status == 416:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{piece_mgr.file_size}")
                self.end_headers()
                return

            self.send_response(status)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "application/octet-stream")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{piece_mgr.file_size}")
            self.send_header("Content-Length", str(end - start + 1))
            self.end_headers()

            stop_event = threading.Event()
            pos = start
            try:
                while pos <= end:
                    chunk_end = min(pos + CHUNK_SIZE - 1, end)
                    if not piece_mgr.ensure_range(pos, chunk_end, stop_event):
                        break

                    with open(piece_mgr.file_path, "rb") as f:
                        f.seek(pos)
                        data = f.read(chunk_end - pos + 1)

                    if not data:
                        break
                    self.wfile.write(data)
                    self.wfile.flush()
                    pos += len(data)
            except (BrokenPipeError, ConnectionResetError, OSError):
                stop_event.set()
            except TimeoutError as e:
                stop_event.set()
                print(f"Range read timeout: {e}", file=sys.stderr)

    server = ReusableThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}/video"


def main():
    if len(sys.argv) < 2 or not sys.argv[1].startswith("magnet:"):
        print('Usage: python3 method2_libtorrent.py "<magnet_uri>" [percent_list]')
        sys.exit(1)

    magnet_uri = sys.argv[1]
    points_arg = sys.argv[2] if len(sys.argv) >= 3 and sys.argv[2].strip() else "20,40,60,80"

    sample_percents = []
    for item in points_arg.split(","):
        try:
            val = float(item.strip())
            if 0 < val < 100:
                sample_percents.append(val / 100.0)
        except ValueError:
            pass

    if not sample_percents:
        print("Provide valid percentages between 1 and 99 (e.g., 20,40,60,80)")
        sys.exit(1)

    start_time = time.time()
    save_path = tempfile.mkdtemp(prefix="lt-torrent-")
    tmp_dir = tempfile.mkdtemp(prefix="lt-clips-")
    server = None

    try:
        live_trackers = fetch_live_trackers()
        print(f"Loaded {len(live_trackers)} live trackers.")

        ses = lt.session()
        ses.apply_settings(
            {
                "listen_interfaces": "0.0.0.0:0",
                "enable_dht": True,
                "enable_lsd": False,
                "enable_upnp": False,
                "enable_natpmp": False,
                "announce_to_all_trackers": True,
                "announce_to_all_tiers": True,
                "connections_limit": 200,
                "alert_mask": get_alert_mask(),
                "dht_bootstrap_nodes": (
                    "router.bittorrent.com:6881,"
                    "router.utorrent.com:6881,"
                    "dht.transmissionbt.com:6881,"
                    "dht.aelitis.com:6881"
                ),
            }
        )

        params = lt.parse_magnet_uri(magnet_uri)
        params.save_path = save_path
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse

        cached_ti = try_fetch_cached_torrent(magnet_uri, save_path)
        if cached_ti is not None:
            params.ti = cached_ti

        flags = params.flags
        if hasattr(lt, "torrent_flags"):
            flags &= ~getattr(lt.torrent_flags, "auto_managed", 0)
            flags |= getattr(lt.torrent_flags, "default_dont_download", 0)
            if cached_ti is not None:
                # Already have metadata; keep paused until piece priorities are locked to 0
                flags |= getattr(lt.torrent_flags, "paused", 0)
            else:
                flags &= ~getattr(lt.torrent_flags, "paused", 0)
        params.flags = flags

        existing = list(params.trackers)
        params.trackers = existing + [tr for tr in live_trackers if tr not in existing]

        handle = ses.add_torrent(params)
        if cached_ti is None:
            handle.resume()

        if not handle.status().has_metadata:
            print(f"Fetching magnet metadata via libtorrent ({len(handle.trackers())} trackers active)...")
            meta_deadline = time.time() + METADATA_TIMEOUT_SEC
            last_log = 0.0
            while not handle.status().has_metadata:
                ses.pop_alerts()
                now = time.time()
                st = handle.status()
                if now > meta_deadline:
                    raise TimeoutError(
                        f"Metadata timed out after {METADATA_TIMEOUT_SEC}s "
                        f"(state: {st.state}, peers: {st.num_peers}/{st.list_peers}, "
                        f"seeds: {st.num_seeds}/{st.list_seeds})"
                    )
                if now - last_log >= 5.0:
                    print(
                        f"Waiting for metadata... state: {st.state} | "
                        f"peers: {st.num_peers} (known: {st.list_peers}) | "
                        f"seeds: {st.num_seeds} (known: {st.list_seeds})"
                    )
                    last_log = now
                time.sleep(0.2)

        info = handle.torrent_file()
        files = info.files()

        video_indices = [
            i for i in range(files.num_files())
            if files.file_path(i).lower().endswith(VIDEO_EXTS)
        ]
        if not video_indices:
            raise RuntimeError("No video file found in magnet.")

        target_idx = max(video_indices, key=lambda i: files.file_size(i))
        file_size = files.file_size(target_idx)
        file_path = os.path.join(save_path, files.file_path(target_idx))

        # Wait for async file_priority() to finish BEFORE locking all piece priorities to 0
        wait_for_zero_piece_priorities(ses, handle, target_idx, info.num_pieces())

        print(
            f"Metadata ready in {time.time() - start_time:.1f}s | "
            f"File: {files.file_path(target_idx)} ({file_size / 1e6:.2f} MB | "
            f"piece size: {info.piece_length() / 1024:.0f} KB | "
            f"total pieces: {info.num_pieces()})"
        )

        # Wipe output/ so old files are completely replaced
        out_dir = os.path.abspath("output")
        shutil.rmtree(out_dir, ignore_errors=True)
        os.makedirs(out_dir, exist_ok=True)

        thumb_path = os.path.join(out_dir, "thumbnail.jpg")
        preview_path = os.path.join(out_dir, "preview.mp4")

        piece_mgr = PieceManager(handle, info, target_idx, file_path, file_size)
        server, stream_url = start_range_server(piece_mgr)

        duration, width, height = probe_video(stream_url)
        print(f"Probed stream: {width}x{height} | {duration:.1f}s")

        clip_paths = []
        for idx, pct in enumerate(sample_percents):
            ts = f"{max(0.0, min(duration - 1.1, duration * pct)):.2f}"
            clip_path = os.path.join(tmp_dir, f"clip_{idx}.mp4")
            clip_paths.append(clip_path)

            print(f"Capturing 1s clip at {round(pct * 100)}% ({ts}s)...")
            cmd = ["ffmpeg", "-y", "-ss", ts, "-i", stream_url]

            if idx == 0:
                cmd.extend(["-frames:v", "1", "-q:v", "2", thumb_path])

            cmd.extend(
                [
                    "-t", "1",
                    "-an",
                    "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", "23",
                    "-pix_fmt", "yuv420p",
                    "-bf", "0",
                    "-video_track_timescale", "90000",
                    clip_path,
                ]
            )
            subprocess.run(cmd, check=True)

        concat_list = os.path.join(tmp_dir, "concat.txt")
        with open(concat_list, "w", encoding="utf-8") as f:
            for p in clip_paths:
                escaped = p.replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        print(f"Merging {len(clip_paths)} clips into output/preview.mp4...")
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", concat_list,
                "-c", "copy",
                "-movflags", "+faststart",
                preview_path,
            ],
            check=True,
        )

        st = handle.status()
        handle.pause()
        pieces_have = sum(1 for i in range(info.num_pieces()) if handle.have_piece(i))
        downloaded_mb = st.total_payload_download / 1e6
        total_mb = file_size / 1e6
        pct_downloaded = (st.total_payload_download / file_size) * 100
        elapsed = time.time() - start_time

        print("\n--- METHOD 2 (LIBTORRENT ON-DEMAND FULL-RES) RESULTS ---")
        print(f"Resolution:   {width}x{height} (native)")
        print(f"Elapsed time: {elapsed:.1f}s")
        print(f"Pieces:       {pieces_have} / {info.num_pieces()}")
        print(f"Downloaded:   {downloaded_mb:.2f} MB / {total_mb:.2f} MB ({pct_downloaded:.2f}%)")
        print("Saved files:  output/thumbnail.jpg, output/preview.mp4")

    except Exception as e:
        print(f"Failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if server:
            server.shutdown()
            server.server_close()
        shutil.rmtree(tmp_dir, ignore_errors=True)
        shutil.rmtree(save_path, ignore_errors=True)


if __name__ == "__main__":
    main()
