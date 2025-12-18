import os
import zipfile
import tempfile
import requests
import pandas as pd
from datetime import datetime, timedelta
import tweepy

# ==============================
# CONFIG
# ==============================

REPO = os.environ["GITHUB_REPOSITORY"]   # ej: ranchal88/mercadona_precios
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

CCAA = "madrid"
TOP_N = 3
DAYS_WEEK = 7

# Twitter / X
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
TWITTER_API_SECRET = os.environ["TWITTER_API_SECRET"]
TWITTER_ACCESS_TOKEN = os.environ["TWITTER_ACCESS_TOKEN"]
TWITTER_ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

# ==============================
# HELPERS
# ==============================

def github_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

def get_releases():
    url = f"https://api.github.com/repos/{REPO}/releases"
    r = requests.get(url, headers=github_headers())
    r.raise_for_status()
    return sorted(r.json(), key=lambda x: x["created_at"])

def download_csv_from_release(release, tmpdir):
    for asset in release["assets"]:
        if asset["name"].endswith(".zip"):
            r = requests.get(asset["browser_download_url"])
            zippath = os.path.join(tmpdir, asset["name"])
            with open(zippath, "wb") as f:
                f.write(r.content)

            with zipfile.ZipFile(zippath) as z:
                for name in z.namelist():
                    if name.endswith(f"{CCAA}/") or not name.endswith(".csv"):
                        continue
                    if f"{CCAA}/mercadona_{CCAA}_" in name:
                        z.extract(name, tmpdir)
                        return os.path.join(tmpdir, name)
    return None

def load_csv(path):
    return pd.read_csv(path, sep=";")

# ==============================
# MAIN LOGIC
# ==============================

def main():
    today = datetime.utcnow().date()
    week_date = today - timedelta(days=DAYS_WEEK)

    with tempfile.TemporaryDirectory() as tmpdir:
        releases = get_releases()

        baseline_release = releases[0]
        latest_release = releases[-1]
        weekly_release = None

        for r in releases:
            if week_date.isoformat() in r["tag_name"]:
                weekly_release = r
                break

        baseline_csv = download_csv_from_release(baseline_release, tmpdir)
        today_csv = download_csv_from_release(latest_release, tmpdir)
        week_csv = download_csv_from_release(weekly_release, tmpdir) if weekly_release else None

        if not baseline_csv or not today_csv:
            raise RuntimeError("Baseline o CSV de hoy no encontrado")

        df_base = load_csv(baseline_csv)
        df_today = load_csv(today_csv)

        df = df_today.merge(
            df_base[["product_id", "price"]],
            on="product_id",
            suffixes=("_today", "_base")
        )

        df = df[df["price_base"] > 0]

        df["pct_change"] = (df["price_today"] - df["price_base"]) / df["price_base"] * 100

        avg_change = df["pct_change"].mean()

        top_up_hist = df.sort_values("pct_change", ascending=False).head(TOP_N)
        top_down_hist = df.sort_values("pct_change").head(TOP_N)

        weekly_text = "Última semana:\nSin histórico suficiente"
        top_up_week = []
        top_down_week = []

        if week_csv:
            df_week = load_csv(week_csv)

            dfw = df_today.merge(
                df_week[["product_id", "price"]],
                on="product_id",
                suffixes=("_today", "_week")
            )

            dfw = dfw[dfw["price_week"] > 0]
            dfw["pct_change"] = (dfw["price_today"] - dfw["price_week"]) / dfw["price_week"] * 100

            ups = dfw[dfw["pct_change"] > 0]
            downs = dfw[dfw["pct_change"] < 0]

            weekly_text = (
                f"Última semana:\n"
                f"🔺 {len(ups)} productos suben\n"
                f"🔻 {len(downs)} productos bajan"
            )

            top_up_week = ups.sort_values("pct_change", ascending=False).head(TOP_N)
            top_down_week = downs.sort_values("pct_change").head(TOP_N)

        # ==============================
        # BUILD TWEET
        # ==============================

        lines = [
            "📊 Precios Mercadona · Madrid",
            "",
            "Desde inicio del seguimiento:",
            f"📈 Precio medio {avg_change:+.1f}%",
            "",
            "⬆️ Top subidas históricas:"
        ]

        for _, r in top_up_hist.iterrows():
            lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.append("⬇️ Top bajadas históricas:")

        for _, r in top_down_hist.iterrows():
            lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.append(weekly_text)

        if top_up_week is not None and len(top_up_week) > 0:
            lines.append("")
            lines.append("⬆️ Top subidas semanales:")
            for _, r in top_up_week.iterrows():
                lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        if top_down_week is not None and len(top_down_week) > 0:
            lines.append("")
            lines.append("⬇️ Top bajadas semanales:")
            for _, r in top_down_week.iterrows():
                lines.append(f"• {r['product_name']} ({r['pct_change']:+.1f}%)")

        lines.append("")
        lines.append("#Mercadona #Precios #Inflación")

        tweet = "\n".join(lines)

        # ==============================
        # SEND TWEET
        # ==============================

        auth = tweepy.OAuth1UserHandler(
            TWITTER_API_KEY,
            TWITTER_API_SECRET,
            TWITTER_ACCESS_TOKEN,
            TWITTER_ACCESS_TOKEN_SECRET
        )

        api = tweepy.API(auth)
        api.update_status(tweet)

        print("✅ Tweet publicado:")
        print(tweet)


if __name__ == "__main__":
    main()
