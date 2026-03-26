# scripts/generate_content_assets.py
import os
import tempfile
import pandas as pd

from .release_data_loader import (
    load_release_snapshots,
    load_csv,
    get_latest_snapshot,
    get_year_baseline_snapshot,
    get_snapshot_n_days_before,
)
from .price_analytics import (
    build_summary,
    build_price_index_series,
)
from chart_builder import (
    save_price_index_chart,
    save_top_changes_chart,
)


CCAA = os.environ.get("CCAA", "madrid")
TOP_N = int(os.environ.get("TOP_N", "5"))
DAYS_WEEK = int(os.environ.get("DAYS_WEEK", "7"))
OUTPUT_DIR = "outputs"
BASELINE_DATE = os.environ.get("BASELINE_DATE", "2026-01-04")

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fmt_pct(x: float) -> str:
    return f"{x:+.4f}%"


def fmt_eur(x: float) -> str:
    return f"{x:.2f}€"

def maybe_add_url(url: str | None) -> str:
    return f"\n\n{url}" if url else ""


def write_text_file(filename: str, text: str) -> str:
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.strip() + "\n")
    return path


def build_avg_tweet(summary, ccaa: str) -> str:
    return (
        f"📊 Precio medio Mercadona {ccaa.capitalize()} · {summary.latest_date}\n\n"
        f"Desde enero de 2026:\n"
        f"{fmt_pct(summary.avg_change)}"
    )


def build_top_up_tweet(summary, ccaa: str) -> str:
    if summary.top_up.empty:
        return "📈 No hay suficiente histórico para calcular el producto que más sube."

    r = summary.top_up.iloc[0]
    url = r.get("product_url")

    return (
        f"📈 Producto que más sube en Mercadona · {ccaa.capitalize()} · {summary.latest_date}\n\n"
        f"{r['product_name']}\n\n"
        f"{fmt_pct(r['pct_change'])}\n\n"
        f"{fmt_eur(r['price_base'])} → {fmt_eur(r['price_today'])}"
        f"{maybe_add_url(url)}"
    )

def build_top_down_tweet(summary, ccaa: str) -> str:
    if summary.top_down.empty:
        return "📉 No hay suficiente histórico para calcular el producto que más baja."

    r = summary.top_down.iloc[0]
    url = r.get("product_url")

    return (
        f"📉 Producto que más baja en Mercadona · {ccaa.capitalize()} · {summary.latest_date}\n\n"
        f"{r['product_name']}\n\n"
        f"{fmt_pct(r['pct_change'])}\n\n"
        f"{fmt_eur(r['price_base'])} → {fmt_eur(r['price_today'])}"
        f"{maybe_add_url(url)}"
    )

def main():
    ensure_output_dir()

    with tempfile.TemporaryDirectory() as tmpdir:
        snapshots = load_release_snapshots(CCAA, tmpdir)

        if not snapshots:
            raise RuntimeError(f"No se han encontrado snapshots para {CCAA}")

        latest_snap = get_latest_snapshot(snapshots)
        latest_dt = pd.to_datetime(latest_snap.date_str)
        latest_year = latest_dt.year

        base_snap = next(
            (s for s in snapshots if s.date_str == BASELINE_DATE),
            None
        )

        if base_snap is None:
            raise RuntimeError(f"No se ha encontrado snapshot para baseline {BASELINE_DATE}")
        
        week_snap = get_snapshot_n_days_before(snapshots, latest_dt, DAYS_WEEK)

        df_latest = load_csv(latest_snap.csv_path)
        df_base = load_csv(base_snap.csv_path)
        df_week = load_csv(week_snap.csv_path) if week_snap else None

        summary = build_summary(
            df_base=df_base,
            df_latest=df_latest,
            baseline_date=base_snap.date_str,
            latest_date=latest_snap.date_str,
            df_week=df_week,
            weekly_date=week_snap.date_str if week_snap else None,
            top_n=TOP_N
        )

        year_snapshots = [s for s in snapshots if s.date_str.startswith(f"{latest_year}-")]
        snapshots_with_df = [(s.date_str, load_csv(s.csv_path)) for s in year_snapshots]
        index_df = build_price_index_series(snapshots_with_df)

        avg_tweet = build_avg_tweet(summary, CCAA)
        top_up_tweet = build_top_up_tweet(summary, CCAA)
        top_down_tweet = build_top_down_tweet(summary, CCAA)

        avg_txt = write_text_file("tweet_avg_price.txt", avg_tweet)
        up_txt = write_text_file("tweet_top_up.txt", top_up_tweet)
        down_txt = write_text_file("tweet_top_down.txt", top_down_tweet)

        avg_png = save_price_index_chart(
            index_df,
            CCAA,
            out_name="tweet_avg_price.png"
        )

        up_png = save_top_changes_chart(
            summary.top_up.head(TOP_N),
            title=f"Productos que más suben · {CCAA.capitalize()}",
            out_name="tweet_top_up.png"
        )

        down_png = save_top_changes_chart(
            summary.top_down.head(TOP_N),
            title=f"Productos que más bajan · {CCAA.capitalize()}",
            out_name="tweet_top_down.png"
        )

        print("✅ Assets generados:")
        print(avg_txt)
        print(avg_png)
        print(up_txt)
        print(up_png)
        print(down_txt)
        print(down_png)


if __name__ == "__main__":
    main()