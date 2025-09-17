from pathlib import Path
import sqlite3
from typing import Optional

def _connect(db: Path):
    con = sqlite3.connect(db)
    import time as _t
    def _recency_boost(mtime):
        if mtime is None:
            return 0.0
        # Decae linealmente hasta 0 en ~10 años
        days = (_t.time() - mtime) / 86400.0
        return max(0.0, 1.0 - (days / 3650.0))
    con.create_function('recency_boost', 1, _recency_boost)
    return con

def search(db: Path, query: str, file_like: Optional[str] = None,
           sheet: Optional[str] = None, limit: int = 50):
    con = _connect(db)

    # 👇 Traemos 7 columnas: id, path, sheet, row_idx, text (fila completa),
    #    snippet (resaltado) y score
    sql = (
        "SELECT r.id, f.path, r.sheet, r.row_idx, "
        "r.text, "
        "snippet(records_fts, '<b>','</b>','…', -1, 16) AS snippet, "
        "bm25(records_fts) + 2.0*recency_boost(f.mtime) AS score "
        "FROM records_fts "
        "JOIN records r ON r.id = records_fts.rowid "
        "JOIN files f ON f.id = r.file_id "
        "WHERE records_fts MATCH ? "
    )
    params = [query]

    if file_like:
        sql += "AND f.path LIKE ? "
        params.append(f"%{file_like}%")

    if sheet:
        sql += "AND r.sheet = ? "
        params.append(sheet)

    sql += "ORDER BY score LIMIT ?"
    params.append(limit)

    rows = con.execute(sql, params).fetchall()
    con.close()
    return rows
