from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Any


def project_root() -> Path:
    # art_studio_org/ is one level down from project root
    return Path(__file__).resolve().parents[1]


def default_db_path() -> Path:
    return project_root() / "studio_inventory.sqlite"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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

    def upsert_vendor_enrichment(
        self,
        *,
        part_key: str,
        vendor: str,
        sku: str,
        source: str,
        title: str | None,
        description: str | None,
        product_url: str | None,
        image_url: str | None,
        specs_json: dict | None,
        raw_json: dict,
    ) -> None:
        self.execute(
            """
            INSERT INTO vendor_enrichment (
                part_key, vendor, sku, source,
                title, description, product_url, image_url,
                specs_json, raw_json, fetched_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(part_key) DO UPDATE SET
                title=excluded.title,
                description=excluded.description,
                product_url=excluded.product_url,
                image_url=excluded.image_url,
                specs_json=excluded.specs_json,
                raw_json=excluded.raw_json,
                fetched_utc=excluded.fetched_utc
            """,
            [
                part_key,
                vendor,
                sku,
                source,
                title,
                description,
                product_url,
                image_url,
                json.dumps(specs_json) if specs_json else None,
                json.dumps(raw_json),
                utc_now_iso(),
            ],
        )
