# scripts/generate_monthly_winners_assets.py
import os
import tempfile
import pandas as pd

from release_data_loader import load_release_snapshots, load_csv
from price_analytics import build_variation_df, get_top_risers, get_top_fallers
from chart_builder import save_top_changes_chart

CCAA = os.environ.get("CCAA", "madrid")
TOP_N = int(os.environ.get("TOP_N", "5"))
OUTPUT_DIR = "outputs"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fmt_pct(x: float) -> str:
    return f"{x:+.4f}%"


def fmt_eur(x: float) -> str:
    return f"{x:.2f}€"


def write_text_file(filename: str, text: str) -> str:
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.strip() + "\n")
    return path


def month_label_es(ts: pd.Timestamp) -> str:
    months = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }
    return f"{months[ts.month]} {ts.year}"


def get_month_end_snapshots(snapshots):
    rows = []
    for s in snapshots:
        dt = pd.to_datetime(s.date_str)
        rows.append({"date_str": s.date_str, "date": dt, "snapshot": s, "month": dt.to_period("M")})

    df = pd.DataFrame(rows).sort_values("date")
    if df.empty:
        raise RuntimeError("No hay snapshots disponibles")

    month_ends = (
        df.groupby("month", as_index=False)
          .tail(1)
          .sort_values("date")
          .reset_index(drop=True)
    )

    if len(month_ends) < 2:
        raise RuntimeError("No hay al menos dos meses cerrados para comparar")

    prev_row = month_ends.iloc[-2]
    last_row = month_ends.iloc[-1]

    return prev_row["snapshot"], last_row["snapshot"], prev_row["date"], last_row["date"]


def build_top_up_tweet(top_up: pd.DataFrame, ccaa: str, last_dt: pd.Timestamp) -> str:
    if top_up.empty:
        return f"📈 No hay datos suficientes para calcular la mayor subida mensual en {ccaa.capitalize()}."

    r = top_up.iloc[0]
    return (
        f"📈 El producto que más subió en Mercadona · {ccaa.capitalize()} · {month_label_es(last_dt)}\n\n"
        f"{r['product_name']}\n\n"
        f"{fmt_pct(r['pct_change'])}\n\n"
        f"{fmt_eur(r['price_prev'])} → {fmt_eur(r['price_today'])}"
    )


def build_top_down_tweet(top_down: pd.DataFrame, ccaa: str, last_dt: pd.Timestamp) -> str:
    if top_down.empty:
        return f"📉 No hay datos suficientes para calcular la mayor bajada mensual en {ccaa.capitalize()}."

    r = top_down.iloc[0]
    return (
        f"📉 El producto que más bajó en Mercadona · {ccaa.capitalize()} · {month_label_es(last_dt)}\n\n"
        f"{r['product_name']}\n\n"
        f"{fmt_pct(r['pct_change'])}\n\n"
        f"{fmt_eur(r['price_prev'])} → {fmt_eur(r['price_today'])}"
    )


def main():
    ensure_output_dir()

    with tempfile.TemporaryDirectory() as tmpdir:
        snapshots = load_release_snapshots(CCAA, tmpdir)
        if not snapshots:
            raise RuntimeError(f"No se han encontrado snapshots para {CCAA}")

        prev_snap, last_snap, prev_dt, last_dt = get_month_end_snapshots(snapshots)

        df_prev = load_csv(prev_snap.csv_path)
        df_last = load_csv(last_snap.csv_path)

        df_var = build_variation_df(df_last, df_prev, "prev")

        top_up = get_top_risers(df_var[df_var["pct_change"] > 0], top_n=TOP_N)
        top_down = get_top_fallers(df_var[df_var["pct_change"] < 0], top_n=TOP_N)

        up_txt = build_top_up_tweet(top_up, CCAA, last_dt)
        down_txt = build_top_down_tweet(top_down, CCAA, last_dt)

        up_txt_path = write_text_file("tweet_monthly_top_up.txt", up_txt)
        down_txt_path = write_text_file("tweet_monthly_top_down.txt", down_txt)

        up_png_path = save_top_changes_chart(
            top_up.head(TOP_N),
            title=f"Productos que más suben · {month_label_es(last_dt)} · {CCAA.capitalize()}",
            out_name="tweet_monthly_top_up.png"
        )

        down_png_path = save_top_changes_chart(
            top_down.head(TOP_N),
            title=f"Productos que más bajan · {month_label_es(last_dt)} · {CCAA.capitalize()}",
            out_name="tweet_monthly_top_down.png"
        )

        print("✅ Monthly winners assets generados:")
        print(up_txt_path)
        print(up_png_path)
        print(down_txt_path)
        print(down_png_path)
        print(f"Comparación mensual: {prev_snap.date_str} -> {last_snap.date_str}")


if __name__ == "__main__":
    main()