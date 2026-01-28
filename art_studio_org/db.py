from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Any


def project_root() -> Path:
    # art_studio_org/ is one level down from project root
    return Path(__file__).resolve().parents[1]


def default_db_path() -> Path:
    return project_root() / "studio_inventory.sqlite"


@dataclass
class DB:
    path: Path

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def scalar(self, sql: str, params: Optional[Iterable[Any]] = None) -> Any:
        with self.connect() as con:
            cur = con.execute(sql, params or [])
            row = cur.fetchone()
            return None if row is None else row[0]

    def rows(self, sql: str, params: Optional[Iterable[Any]] = None) -> list[sqlite3.Row]:
        with self.connect() as con:
            cur = con.execute(sql, params or [])
            return cur.fetchall()

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        with self.connect() as con:
            cur = con.execute(sql, params or [])
            con.commit()
            return cur.rowcount
