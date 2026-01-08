import os
import zipfile
import tempfile
import requests
import pandas as pd
from datetime import datetime, timedelta
import re

# ==============================
# CONFIG
# ==============================

REPO = os.environ["GITHUB_REPOSITORY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

CCAA = "madrid"
TOP_N = 3
DAYS_WEEK = 7

BASELINE_DATE = datetime(2026, 1, 4).date()
BASELINE_LABEL = "enero de 2026"

OUTPUT_DIR = "output"

# ==============================
# GITHUB HELPERS
# ==============================

def gh_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

def get_releases():
    url = f"https://api.github.com/repos/{REPO}/releases"
    r = requests.get(url, headers=gh_headers())
    r.raise_for_status()
    return r.json()

# ==============================
# DATE EXTRACTION (CLAVE)
# ==============================

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

def extract_date_from_release(release):
    """
    Extrae YYYY-MM-DD del tag_name o del nombre del asset ZIP.
    Devuelve date o None.
    """
    # 1️⃣ Intentar tag_name
    m = DATE_RE.search(release.get("tag_name", ""))
    if m:
        return datetime.strptime(m.group(), "%Y-%m-%d").date()

    # 2️⃣ Intentar nombre del asset
    for asset in release.get("assets", []):
        m = DATE_RE.search(asset.get("name", ""))
        if m:
            return datetime.strptime(m.group(), "%Y-%m-%d").date()

    return None

def select_releases_by_date(releases):
    dated = []
    for r in releases:
        d = extract_date_from_release(r)
        if d:
            dated.append((d, r))

    if not dated:
        raise RuntimeError("No se pudo extraer fecha de ningún release")

    dated.sort(key=lambda x: x[0])
    return dated  # lista ordenada (date, release)

# ==============================
# ZIP / CSV EXTRACTION
# ==============================

def extract_csv_from_release(release, tmpdir):
    for asset in release["assets"]:
        if not asset["name"].endswith(".zip"):
            continue

        r = requests.get(asset["browser_download_url"])
        zip_path = os.path.join(tmpdir, asset["name"])

        with open(zip_path, "wb") as f:
            f.write(r.content)

        with zipfile.ZipFile(zip_path) as z:
            for name in z.namelist():
                if (
                    name.endswith(".csv")
                    and f"data/{CCAA}/mercadona_{CCAA}_" in name
                ):
                    z.extract(name, tmpdir)
                    return os.path.join(tmpdir, name)

    raise RuntimeError("No se encontró CSV válido en el release")

# ==============================
# DATA LOADING (ROBUSTO)
# ==============================

def load_csv_clean(path):
    df = pd.read_csv(
        path,
        sep=";",
        engine="python",
        on_bad_lines="skip"
    )

    df = df[["product_id", "product_name", "price"]].copy()

    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df[df["price"].notna() & (df["price"] > 0)]

    return df

def aggregate_by_product(df, suffix):
    return (
        df.groupby("product_id", as_index=False)
          .agg(
              **{
                  f"product_name{suffix}": ("product_name", "first"),
                  f"price{suffix}": ("price", "mean"),
              }
          )
    )

# ==============================
# MAIN
# ==============================

def main():
    today = datetime.utcnow().date()
    week_date = today - timedelta(days=DAYS_WEEK)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        releases = get_releases()
        dated_releases = select_releases_by_date(releases)

        # Baseline: primer release >= BASELINE_DATE
        baseline = next(
            (r for d, r in dated_releases if d >= BASELINE_DATE),
            None
        )
        if not baseline:
            raise RuntimeError("No se encontró baseline válido")

        # Latest: mayor fecha disponible
        latest_date, latest = dated_releases[-1]

        # Week release: más cercano <= week_date
        week = None
        for d, r in reversed(dated_releases):
            if d <= week_date:
                week = r
                break

        # ==============================
        # EXTRAER CSVs
        # ==============================

        base_csv = extract_csv_from_release(baseline, tmpdir)
        today_csv = extract_csv_from_release(latest, tmpdir)
        week_csv = extract_csv_from_release(week, tmpdir) if week else None

        # ==============================
        # CARGA Y NORMALIZACIÓN
        # ==============================

        df_base = aggregate_by_product(load_csv_clean(base_csv), "_base")
        df_today = aggregate_by_product(load_csv_clean(today_csv), "_today")

        df_hist = df_today.merge(df_base, on="product_id", how="inner")

        # ==============================
        # PRECIO MEDIO (VARIACIÓN REAL)
        # ==============================

        mean_base = df_hist["price_base"].mean()
        mean_today = df_hist["price_today"].mean()

        avg_change = ((mean_today - mean_base) / mean_base) * 100
        if abs(avg_change) < 0.00005:
            avg_change = 0.0

        # ==============================
        # TOPS
        # ==============================

        df_hist["pct_change"] = (
            (df_hist["price_today"] - df_hist["price_base"])
            / df_hist["price_base"] * 100
        )

        df_changes = df_hist[df_hist["pct_change"] != 0]

        top_up = df_changes.sort_values("pct_change", ascending=False).head(TOP_N)
        top_down = df_changes.sort_values("pct_change").head(TOP_N)

        # ==============================
        # BUILD TXT
        # ==============================

        lines = [
            "📊 Precios Mercadona · Madrid",
            "",
            f"Desde {BASELINE_LABEL}:",
            f"📈 Precio medio {avg_change:+.4f}%",
            "",
            "⬆️ Top subidas desde enero de 2026:"
        ]

        if top_up.empty:
            lines.append("Sin cambios relevantes")
        else:
            for _, r in top_up.iterrows():
                lines.append(
                    f"• {r['product_name_today']} "
                    f"({r['pct_change']:+.1f}%): "
                    f"{r['price_base']:.2f}€ → {r['price_today']:.2f}€"
                )

        lines.append("")
        lines.append("⬇️ Top bajadas desde enero de 2026:")

        if top_down.empty:
            lines.append("Sin cambios relevantes")
        else:
            for _, r in top_down.iterrows():
                lines.append(
                    f"• {r['product_name_today']} "
                    f"({r['pct_change']:+.1f}%): "
                    f"{r['price_base']:.2f}€ → {r['price_today']:.2f}€"
                )

        lines.append("")
        lines.append("#Mercadona #Precios #Inflación")

        text = "\n".join(lines)

        out_file = os.path.join(
            OUTPUT_DIR,
            f"tweet_madrid_{today.isoformat()}.txt"
        )

        with open(out_file, "w", encoding="utf-8") as f:
            f.write(text)

        print("✅ TXT generado con datos de:", latest_date)
        print(text)

# ==============================
# ENTRYPOINT
# ==============================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERROR CRÍTICO:", e)
        raise
