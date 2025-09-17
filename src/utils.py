from pathlib import Path
import hashlib
def sha1_head(path: Path, n_bytes: int = 65536) -> str:
    h = hashlib.sha1()
    with path.open('rb') as f:
        h.update(f.read(n_bytes))
    return h.hexdigest()