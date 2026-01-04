import os
import zipfile
import tempfile
import requests
import pandas as pd
from datetime import datetime, timedelta

# ==============================
# CONFIG
# ==============================

REPO = os.environ["GITHUB_REPOSITORY"]   # ej: ranchal88/mercadona_precios
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

CCAA = "madrid"
TOP_N = 3
DAYS_WEEK = 7
BASELINE_LABEL = "01/01/2026"

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

def extract_csv_from_release(release, tmpdir):
    for asset in release["assets"]:
        if asset["name"].endswith(".zip"):
            r = requests.get(asset["browser_download_url"])
            zip_path = os.path.join(tmpdir, asset["name"])

            with open(zip_path, "wb") as f:
                f.write(r.content)

            with zipfile.ZipFile(zip_path) as z:
                for name in z.namelist():
                    if name.endswith(f"data/{CCAA}/") or not name.endswith(".csv"):
                        continue
                    if f"data/{CCAA}/mercadona_{CCAA}_" in name:
                        z.extract(name, tmpdir)
                        return os.path.join(tmpdir, name)
    return None

def load_csv(path):
    return pd.read_csv(path, sep=";")

# ==============================
# MAIN
# ==============================

def main():
    today = datetime.utcnow().date()
    week_date = today - timedelta(days=DAYS_WEEK)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        releases = get_releases()

        baseline_release = releases[0]
        latest_release = releases[-1]

        week_release = None
        for r in releases:
            if week_date.isoformat() in r["tag_name"]:
                week_release = r
                break

        base_csv = extract_csv_from_release(baseline_release, tmpdir)
        today_csv = extract_csv_from_release(latest_release, tmpdir)
        week_csv = extract_csv_from_release(week_release, tmpdir) if week_release else None

        if not base_csv or not today_csv:
            raise RuntimeError("No se ha podido cargar baseline o CSV de hoy")

        df_base = load_csv(base_csv)
        df_today = load_csv(today_csv)

        # ==============================
        # DESDE INICIO 2026 (BASELINE)
        # ==============================

        df_hist = df_today.merge(
            df_base[["product_id", "price"]],
            on="product_id",
            suffixes=("_today", "_base")
        )

        df_hist = df_hist[df_hist["price_base"] > 0]
        df_hist["pct_change"] = (
            (df_hist["price_today"] - df_hist["price_base"])
            / df_hist["price_base"] * 100
        )

        avg_change = df_hist["pct_change"].mean()

        top_up_hist = df_hist.sort_values("pct_change", ascending=False).head(TOP_N)
        top_down_hist = df_hist.sort_values("pct_change").head(TOP_N)

        # ==============================
        # ÚLTIMA SEMANA
        # ==============================

        weekly_block = [
            "Última semana:",
            "Sin histórico suficiente"
        ]

        top_up_week = []
        top_down_week = []

        if week_csv:
            df_week = load_csv(week_csv)

            df_w = df_today.merge(
                df_week[["product_id", "price"]],
                on="product_id",
                suffixes=("_today", "_week")
            )

            df_w = df_w[df_w["price_week"] > 0]
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

            top_up_week = ups.sort_values("pct_change", ascending=False).head(TOP_N)
            top_down_week = downs.sort_values("pct_change").head(TOP_N)

        # ==============================
        # BUILD TXT
        # ==============================

        lines = [
            "📊 Precios Mercadona · Madrid",
            "",
            f"Desde {BASELINE_LABEL}:",
            f"📈 Precio medio {avg_change:+.1f}%",
            "",
            "⬆️ Top subidas desde inicio de 2026:"
        ]

        for _, r in top_up_hist.iterrows():
            lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.append("⬇️ Top bajadas desde inicio de 2026:")

        for _, r in top_down_hist.iterrows():
            lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.extend(weekly_block)

        if len(top_up_week) > 0:
            lines.append("")
            lines.append("⬆️ Top subidas esta semana:")
            for _, r in top_up_week.iterrows():
                lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        if len(top_down_week) > 0:
            lines.append("")
            lines.append("⬇️ Top bajadas esta semana:")
            for _, r in top_down_week.iterrows():
                lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.append("#Mercadona #Precios #Inflación")

        text = "\n".join(lines)

        out_file = os.path.join(
            OUTPUT_DIR,
            f"tweet_madrid_{today.isoformat()}.txt"
        )

        with open(out_file, "w", encoding="utf-8") as f:
            f.write(text)

        print("✅ TXT generado:")
        print(out_file)
        print("\n--- CONTENIDO ---\n")
        print(text)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("⚠️ Error generando TXT:", e)
        print("⚠️ El workflow continúa sin informe")
