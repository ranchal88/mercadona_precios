# scripts/release_data_loader.py
import os
import re
import zipfile
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REPO = os.environ.get("GITHUB_REPOSITORY", "ranchal88/mercadona_precios")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")


@dataclass
class ReleaseSnapshot:
    date_str: str
    csv_path: str
    release_tag: str


def github_headers():
    headers = {
        "Accept": "application/vnd.github+json"
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    return headers


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_releases() -> List[dict]:
    url = f"https://api.github.com/repos/{REPO}/releases"
    releases = []
    page = 1
    session = _make_session()

    while True:
        r = session.get(
            url,
            headers=github_headers(),
            params={"per_page": 100, "page": page},
            timeout=60
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        releases.extend(batch)
        page += 1

    releases.sort(key=lambda x: x["created_at"])
    return releases


def extract_date_from_release(release: dict) -> Optional[str]:
    candidates = [
        release.get("tag_name", ""),
        release.get("name", ""),
        release.get("published_at", ""),
        release.get("created_at", ""),
    ]

    for text in candidates:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        if m:
            return m.group(1)

    return None


def download_csv_from_release(release: dict, tmpdir: str, ccaa: str) -> Optional[str]:
    session = _make_session()
    for asset in release.get("assets", []):
        if not asset["name"].endswith(".zip"):
            continue

        # Stream to avoid loading the full zip into memory and triggering MemoryError.
        with session.get(asset["browser_download_url"], headers=github_headers(), stream=True, timeout=(30, 180)) as r:
            r.raise_for_status()

            zippath = os.path.join(tmpdir, asset["name"])
            with open(zippath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        with zipfile.ZipFile(zippath) as z:
            names = z.namelist()

            # buscamos un csv concreto de la ccaa
            candidates = [
                name for name in names
                if name.endswith(".csv") and f"{ccaa}/mercadona_{ccaa}_" in name
            ]

            if not candidates:
                continue

            # normalmente habrá uno
            csv_name = candidates[0]
            z.extract(csv_name, tmpdir)
            return os.path.join(tmpdir, csv_name)

    return None


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def load_release_snapshots(ccaa: str, tmpdir: str) -> List[ReleaseSnapshot]:
    releases = get_releases()
    snapshots: List[ReleaseSnapshot] = []

    for release in releases:
        date_str = extract_date_from_release(release)
        if not date_str:
            continue

        csv_path = download_csv_from_release(release, tmpdir, ccaa)
        if not csv_path:
            continue

        snapshots.append(
            ReleaseSnapshot(
                date_str=date_str,
                csv_path=csv_path,
                release_tag=release.get("tag_name", "")
            )
        )

    # orden cronológico y deduplicar por fecha
    dedup = {}
    for snap in snapshots:
        dedup[snap.date_str] = snap

    ordered = sorted(dedup.values(), key=lambda x: x.date_str)
    return ordered


def get_snapshot_by_date(
    snapshots: List[ReleaseSnapshot],
    target_date: str
) -> Optional[ReleaseSnapshot]:
    for snap in snapshots:
        if snap.date_str == target_date:
            return snap
    return None


def get_latest_snapshot(snapshots: List[ReleaseSnapshot]) -> ReleaseSnapshot:
    if not snapshots:
        raise RuntimeError("No hay snapshots disponibles")
    return snapshots[-1]


def get_year_baseline_snapshot(
    snapshots: List[ReleaseSnapshot],
    year: int
) -> ReleaseSnapshot:
    year_snaps = [s for s in snapshots if s.date_str.startswith(f"{year}-")]
    if not year_snaps:
        raise RuntimeError(f"No hay snapshots del año {year}")
    return year_snaps[0]


def get_snapshot_n_days_before(
    snapshots: List[ReleaseSnapshot],
    latest_date: pd.Timestamp,
    days: int
) -> Optional[ReleaseSnapshot]:
    cutoff = (latest_date - pd.Timedelta(days=days)).date()

    valid = [
        s for s in snapshots
        if pd.to_datetime(s.date_str).date() <= cutoff
    ]

    return valid[-1] if valid else None