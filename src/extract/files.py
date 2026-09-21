from pathlib import Path
import hashlib
import json
import os
import shutil
import tempfile

from src.config import path_for
from src.common.audit import utc_now_iso


SOURCE_FILES = [
    ('customers', 'customers.csv'),
    ('products',  'products.json'),
    ('orders',    'orders.csv'),
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    """Write atomically: temp file in same dir, then os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(text)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    Creates data/raw/run_id=<run_id>/, copies customers.csv, products.json,
    orders.csv byte-for-byte, and writes a manifest.json describing each
    copied file (size, sha256). Source files are never modified.

    Returns the run-specific raw path.
    """
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    raw_dir.mkdir(parents=True, exist_ok=True)

    manifest_entries = []
    for logical_name, filename in SOURCE_FILES:
        src = source_dir / filename
        if not src.is_file():
            raise FileNotFoundError(f'Missing source file: {src}')
        dst = raw_dir / filename
        shutil.copy2(src, dst)  # preserves mtime, content unchanged
        manifest_entries.append({
            'logical_name': logical_name,
            'filename': filename,
            'bytes': dst.stat().st_size,
            'sha256': _sha256(dst),
        })

    manifest = {
        'run_id': run_id,
        'extracted_at_utc': utc_now_iso(),
        'source_dir': str(source_dir),
        'raw_dir': str(raw_dir),
        'files': manifest_entries,
    }
    _atomic_write_text(
        raw_dir / 'manifest.json',
        json.dumps(manifest, indent=2) + '\n',
    )

    return raw_dir
