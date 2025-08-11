# download_watcher.py
import os
import re
import time
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export_ingester import LinkedInExportIngestor

ALLOWED_EXT = ('.csv', '.xls', '.xlsx', '.zip')
TEMP_SUFFIXES = ('.crdownload', '.part', '.tmp')

DEFAULT_PATTERNS = [
    r'linkedin', r'page', r'analytics', r'export', r'content', r'updates',
    r'posts', r'followers', r'demographics'
]

LOG = logging.getLogger("download_watcher")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)

def looks_like_linkedin_export(name: str) -> bool:
    n = name.lower()
    if not n.endswith(ALLOWED_EXT):
        return False
    return any(re.search(pat, n) for pat in DEFAULT_PATTERNS)

def is_temp(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(suf) for suf in TEMP_SUFFIXES)

def is_stable(path: Path, wait_seconds: int = 2) -> bool:
    try:
        size1 = path.stat().st_size
        time.sleep(wait_seconds)
        size2 = path.stat().st_size
        return size1 == size2
    except FileNotFoundError:
        return False

def move_file(src: Path, dst_dir: Path) -> Optional[Path]:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dst_dir.mkdir(parents=True, exist_ok=True)
    dest = dst_dir / f"{ts}_{src.name}"
    try:
        shutil.move(str(src), str(dest))
        return dest
    except Exception:
        try:
            shutil.copy2(str(src), str(dest))
            src.unlink(missing_ok=True)
            return dest
        except Exception as e:
            LOG.warning(f"Could not move {src.name}: {e}")
            return None

class DownloadHandler(FileSystemEventHandler):
    def __init__(self, source_dir: Path, target_dir: Path):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.ingestor = LinkedInExportIngestor()

    def _try_process(self, file_path: Path):
        if not file_path.is_file():
            return
        name = file_path.name
        if is_temp(name):
            return
        if not looks_like_linkedin_export(name):
            return
        if not is_stable(file_path, wait_seconds=2):
            return

        moved = move_file(file_path, self.target_dir)
        if moved:
            LOG.info(f"Moved {file_path.name} -> {moved.name}")
            posts, demos = self.ingestor.scan_and_process_folder()
            LOG.info(f"Ingested. Posts: {posts}, Demographics: {demos}")

    def on_created(self, event):
        try:
            if not event.is_directory:
                self._try_process(Path(event.src_path))
        except Exception as e:
            LOG.exception(f"on_created error: {e}")

    def on_modified(self, event):
        try:
            if not event.is_directory:
                self._try_process(Path(event.src_path))
        except Exception as e:
            LOG.exception(f"on_modified error: {e}")

def main():
    load_dotenv()
    source_dir = os.getenv("DOWNLOAD_SOURCE_DIR")
    target_dir = os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports")

    if not source_dir:
        raise ValueError("Set DOWNLOAD_SOURCE_DIR in .env to your Downloads path")

    src = Path(source_dir).expanduser().resolve()
    dst = Path(target_dir).expanduser().resolve()

    if not src.exists():
        raise FileNotFoundError(f"Source directory not found: {src}")

    LOG.info(f"Watching: {src}")
    LOG.info(f"Target ingest dir: {dst}")

    handler = DownloadHandler(src, dst)
    observer = Observer()
    observer.schedule(handler, str(src), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()