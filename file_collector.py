# file_collector.py
import os
import re
import time
import shutil
from pathlib import Path
from datetime import datetime

ALLOWED_EXT = ('.csv', '.xls', '.xlsx', '.zip')
TEMP_SUFFIXES = ('.crdownload', '.part', '.tmp')

DEFAULT_PATTERNS = [
    r'linkedin', r'page', r'analytics', r'export', r'content', r'updates',
    r'posts', r'followers', r'demographics'
]

def _looks_like_linkedin_export(name: str) -> bool:
    n = name.lower()
    if not n.endswith(ALLOWED_EXT):
        return False
    return any(re.search(pat, n) for pat in DEFAULT_PATTERNS)

def _is_temp(name: str) -> bool:
    n = name.lower()
    return any(n.endswith(suf) for suf in TEMP_SUFFIXES)

def _is_stable(path: Path, wait_seconds: int = 2) -> bool:
    try:
        size1 = path.stat().st_size
        time.sleep(wait_seconds)
        size2 = path.stat().st_size
        return size1 == size2
    except FileNotFoundError:
        return False

def collect_downloads(source_dir: str, target_dir: str) -> list[Path]:
    """
    Move finished LinkedIn export files from source_dir to target_dir.
    Returns list of moved destination Paths.
    """
    src = Path(source_dir).expanduser().resolve()
    dst = Path(target_dir).expanduser().resolve()
    dst.mkdir(parents=True, exist_ok=True)
    moved = []

    if not src.exists():
        return moved

    for p in src.iterdir():
        if not p.is_file():
            continue
        if _is_temp(p.name):
            continue
        if not _looks_like_linkedin_export(p.name):
            continue
        if not _is_stable(p, wait_seconds=2):
            continue

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dest = dst / f"{ts}_{p.name}"
        try:
            shutil.move(str(p), str(dest))
            moved.append(dest)
        except Exception:
            try:
                shutil.copy2(str(p), str(dest))
                p.unlink(missing_ok=True)
                moved.append(dest)
            except:
                continue

    return moved