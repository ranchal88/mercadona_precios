# scripts/release_data_loader.py
import os
import re
import zipfile
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import requests


REPO = os.environ["GITHUB_REPOSITORY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]


@dataclass
class ReleaseSnapshot:
    date_str: str
    csv_path: str
    release_tag: str


def github_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }


def get_releases() -> List[dict]:
    url = f"https://api.github.com/repos/{REPO}/releases"
    releases = []
    page = 1

    while True:
        r = requests.get(
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
    for asset in release.get("assets", []):
        if not asset["name"].endswith(".zip"):
            continue

        r = requests.get(asset["browser_download_url"], timeout=120)
        r.raise_for_status()

        zippath = os.path.join(tmpdir, asset["name"])
        with open(zippath, "wb") as f:
            f.write(r.content)

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