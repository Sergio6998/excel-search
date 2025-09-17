from pathlib import Path
import sqlite3
import pandas as pd
from .utils import sha1_head

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS files(
  id INTEGER PRIMARY KEY, path TEXT UNIQUE, size INTEGER, mtime REAL, sha1_64 TEXT
);
CREATE TABLE IF NOT EXISTS records(
  id INTEGER PRIMARY KEY, file_id INTEGER, sheet TEXT, row_idx INTEGER,
  col TEXT, header TEXT, text TEXT, created_at REAL DEFAULT (strftime('%s','now')),
  FOREIGN KEY(file_id) REFERENCES files(id)
);
CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(text, content='records', content_rowid='id');
"""

def _connect(db_path: Path):
    con = sqlite3.connect(db_path)
    con.execute('PRAGMA case_sensitive_like=OFF;')
    return con

def init_db(db_path: Path):
    con = _connect(db_path)
    con.executescript(SCHEMA)
    con.commit()
    con.close()

def upsert_file(con, path: Path):
    st = path.stat()
    sha = sha1_head(path)
    cur = con.execute("SELECT id, mtime, sha1_64 FROM files WHERE path=?", (str(path),))
    row = cur.fetchone()
    if row and (row[1] == st.st_mtime and row[2] == sha):
        return row[0], False  # sin cambios
    if row:
        file_id = row[0]
        con.execute("UPDATE files SET size=?, mtime=?, sha1_64=? WHERE id=?", (st.st_size, st.st_mtime, sha, file_id))
        con.execute("DELETE FROM records WHERE file_id=?", (file_id,))
    else:
        cur = con.execute("INSERT INTO files(path,size,mtime,sha1_64) VALUES(?,?,?,?)",
                          (str(path), st.st_size, st.st_mtime, sha))
        file_id = cur.lastrowid
    return file_id, True

def _row_to_text(row: pd.Series) -> str:
    parts = []
    for k, v in row.items():
        if pd.isna(v) or str(v).strip() == "":
            continue
        parts.append(f"{k}: {str(v)}")
    return " | ".join(parts)

def index_folder(root: Path, db_path: Path):
    root = Path(root)
    db_path = Path(db_path)
    init_db(db_path)
    con = _connect(db_path)

    exts = ('.xlsx', '.xls', '.csv')
    files = [p for p in root.rglob('*') if p.suffix.lower() in exts]

    for p in files:
        try:
            file_id, changed = upsert_file(con, p)
            if not changed:
                continue

            if p.suffix.lower() == '.csv':
                df = pd.read_csv(p, dtype=str)
                sheets = {p.stem: df}
            else:
                xl = pd.ExcelFile(p)
                sheets = {name: xl.parse(name, dtype=str) for name in xl.sheet_names}

            for sheet, df in sheets.items():
                df = df.fillna('')
                headers = [str(h) for h in df.columns]
                header_str = '|'.join(headers)

                for i, row in df.iterrows():
                    text = _row_to_text(row)
                    cur = con.execute(
                        "INSERT INTO records(file_id, sheet, row_idx, col, header, text) VALUES(?,?,?,?,?,?)",
                        (file_id, sheet, int(i) + 2, None, header_str, text)
                    )
                    rid = cur.lastrowid
                    con.execute("INSERT INTO records_fts(rowid, text) VALUES(?,?)", (rid, text))

            con.commit()

        except Exception as e:
            con.rollback()
            print(f"[WARN] {p}: {e}")

    con.close()