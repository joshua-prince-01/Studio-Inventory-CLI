from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from requests_pkcs12 import post, put, get

BASE = "https://api.mcmaster.com/v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class McMasterCreds:
    username: str
    password: str
    pfx_path: str
    pfx_password: str

    @classmethod
    def from_env(cls) -> "McMasterCreds":
        load_dotenv("secrets/mcmaster.env")
        return cls(
            username=os.environ["MCMASTER_USERNAME"],
            password=os.environ["MCMASTER_PASSWORD"],
            pfx_path=os.environ["MCMASTER_PFX_PATH"],
            pfx_password=os.environ["MCMASTER_PFX_PASSWORD"],
        )


class McMasterClient:
    def __init__(self, creds: McMasterCreds):
        self.creds = creds
        self._token: Optional[str] = None

    # -------- Auth --------
    def login(self) -> None:
        r = post(
            f"{BASE}/login",
            json={
                "UserName": self.creds.username,
                "Password": self.creds.password,
            },
            pkcs12_filename=self.creds.pfx_path,
            pkcs12_password=self.creds.pfx_password,
            timeout=30,
        )
        r.raise_for_status()
        self._token = r.json()["AuthToken"]

    def headers(self) -> Dict[str, str]:
        if not self._token:
            self.login()
        return {"Authorization": f"Bearer {self._token}"}

    # -------- API calls --------
    def add_product(self, part_number: str) -> None:
        # Required subscription step
        r = put(
            f"{BASE}/products",
            headers=self.headers(),
            json={"PartNumber": part_number},
            pkcs12_filename=self.creds.pfx_path,
            pkcs12_password=self.creds.pfx_password,
            timeout=30,
        )
        # 409 = already subscribed (fine)
        if r.status_code not in (200, 201, 409):
            r.raise_for_status()

    def product_info(self, part_number: str) -> Dict[str, Any]:
        r = get(
            f"{BASE}/products/{part_number}",
            headers=self.headers(),
            pkcs12_filename=self.creds.pfx_path,
            pkcs12_password=self.creds.pfx_password,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
