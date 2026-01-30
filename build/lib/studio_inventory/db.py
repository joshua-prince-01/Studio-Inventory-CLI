from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Any


# ----------------------------
# Roots / paths
# ----------------------------
from studio_inventory.paths import workspace_root, project_root


def default_db_path() -> Path:
    root = workspace_root()
    root.mkdir(parents=True, exist_ok=True)
    return root / "studio_inventory.sqlite"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ----------------------------
# DB wrapper
# ----------------------------
@dataclass
class DB:
    path: Path

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def scalar(self, sql: str, params: Optional[Iterable[Any]] = None) -> Any:
        with self.connect() as con:
            cur = con.execute(sql, list(params or []))
            row = cur.fetchone()
            return None if row is None else row[0]

    def rows(self, sql: str, params: Optional[Iterable[Any]] = None) -> list[sqlite3.Row]:
        with self.connect() as con:
            cur = con.execute(sql, list(params or []))
            return cur.fetchall()

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        with self.connect() as con:
            cur = con.execute(sql, list(params or []))
            con.commit()
            return cur.rowcount
