import os
import zipfile
import tempfile
import requests
import pandas as pd
from datetime import datetime, timedelta

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
    return sorted(r.json(), key=lambda x: x["created_at"])

def find_release_on_or_after(releases, target_date):
    for r in releases:
        created = datetime.fromisoformat(
            r["created_at"].replace("Z", "")
        ).date()
        if created >= target_date:
            return r
    return None

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
                if not name.endswith(".csv"):
                    continue
                if f"data/{CCAA}/mercadona_{CCAA}_" in name:
                    z.extract(name, tmpdir)
                    return os.path.join(tmpdir, name)
    return None

# ==============================
# DATA LOADING (ROBUSTO)
# ==============================

def load_csv_clean(path: str) -> pd.DataFrame:
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

    df = df[df["price"].notna()]
    df = df[df["price"] > 0]

    return df

def aggregate_by_product(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
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

        baseline_release = find_release_on_or_after(releases, BASELINE_DATE)
        latest_release = releases[-1]

        if not baseline_release:
            raise RuntimeError("No se encontró release baseline válido")

        week_release = next(
            (r for r in releases if week_date.isoformat() in r["tag_name"]),
            None
        )

        base_csv = extract_csv_from_release(baseline_release, tmpdir)
        today_csv = extract_csv_from_release(latest_release, tmpdir)
        week_csv = extract_csv_from_release(week_release, tmpdir) if week_release else None

        if not base_csv or not today_csv:
            raise RuntimeError("No se pudo cargar CSV baseline o today")

        # ==============================
        # CARGA Y NORMALIZACIÓN
        # ==============================

        df_base_raw = load_csv_clean(base_csv)
        df_today_raw = load_csv_clean(today_csv)

        df_base = aggregate_by_product(df_base_raw, "_base")
        df_today = aggregate_by_product(df_today_raw, "_today")

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
        # TOPS (POR PRODUCTO)
        # ==============================

        df_hist["pct_change"] = (
            (df_hist["price_today"] - df_hist["price_base"])
            / df_hist["price_base"] * 100
        )

        df_changes = df_hist[df_hist["pct_change"] != 0]

        top_up = df_changes.sort_values("pct_change", ascending=False).head(TOP_N)
        top_down = df_changes.sort_values("pct_change").head(TOP_N)

        # ==============================
        # ÚLTIMA SEMANA
        # ==============================

        weekly_block = ["Última semana:", "Sin histórico suficiente"]

        if week_csv:
            df_week_raw = load_csv_clean(week_csv)
            df_week = aggregate_by_product(df_week_raw, "_week")

            df_w = df_today.merge(df_week, on="product_id", how="inner")

            df_w["pct_change"] = (
                (df_w["price_today"] - df_w["price_week"])
                / df_w["price_week"] * 100
            )

            ups = df_w[df_w["pct_change"] > 0]
            downs = df_w[df_w["pct_change"] < 0]

            weekly_block = [
                "Última semana:",
                f"🔺 {len(ups)} productos suben",
                f"🔻 {len(downs)} productos bajan"
            ]

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
        lines.extend(weekly_block)

        lines.append("")
        lines.append("#Mercadona #Precios #Inflación")

        text = "\n".join(lines)

        out_file = os.path.join(
            OUTPUT_DIR,
            f"tweet_madrid_{today.isoformat()}.txt"
        )

        with open(out_file, "w", encoding="utf-8") as f:
            f.write(text)

        print("✅ TXT generado")
        print(text)

# ==============================
# ENTRYPOINT
# ==============================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("⚠️ Error generando TXT:", e)
        print("⚠️ Workflow continúa sin informe")
