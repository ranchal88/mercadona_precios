# scripts/generate_product_churn_assets.py
import os
import tempfile
from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt

from release_data_loader import load_release_snapshots, load_csv

CCAA = os.environ.get("CCAA", "madrid")
TOP_N = int(os.environ.get("TOP_N", "12"))
OUTPUT_DIR = "outputs"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


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


def get_month_end_snapshots(snapshots) -> Tuple[object, object, pd.Timestamp, pd.Timestamp]:
    rows = []
    for s in snapshots:
        dt = pd.to_datetime(s.date_str)
        rows.append({
            "date_str": s.date_str,
            "date": dt,
            "snapshot": s,
            "month": dt.to_period("M"),
        })

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


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["product_id", "product_name", "price"]
    out = df[cols].copy()
    out = out.dropna(subset=["product_id", "product_name"])
    out = out.drop_duplicates(subset=["product_id"])
    return out


def build_new_and_removed(df_prev: pd.DataFrame, df_last: pd.DataFrame):
    prev = normalize_df(df_prev)
    last = normalize_df(df_last)

    prev_ids = set(prev["product_id"])
    last_ids = set(last["product_id"])

    new_ids = last_ids - prev_ids
    removed_ids = prev_ids - last_ids

    new_products = (
        last[last["product_id"].isin(new_ids)]
        .sort_values(["product_name", "product_id"])
        .reset_index(drop=True)
    )

    removed_products = (
        prev[prev["product_id"].isin(removed_ids)]
        .sort_values(["product_name", "product_id"])
        .reset_index(drop=True)
    )

    return new_products, removed_products


def build_new_products_tweet(new_products: pd.DataFrame, ccaa: str, month_dt: pd.Timestamp, top_n: int) -> str:
    title = f"🆕 Nuevos productos en Mercadona · {ccaa.capitalize()} · {month_label_es(month_dt)}"
    total = len(new_products)

    lines = [title, ""]

    if total == 0:
        lines.append("No aparecen productos nuevos este mes.")
        return "\n".join(lines)

    for _, row in new_products.head(top_n).iterrows():
        lines.append(f"• {row['product_name']}")

    extra = total - min(total, top_n)
    if extra > 0:
        lines += ["", f"+{extra} productos más"]

    return "\n".join(lines)


def build_removed_products_tweet(removed_products: pd.DataFrame, ccaa: str, month_dt: pd.Timestamp, top_n: int) -> str:
    title = f"🚫 Productos que ya no aparecen en Mercadona · {ccaa.capitalize()} · {month_label_es(month_dt)}"
    total = len(removed_products)

    lines = [title, ""]

    if total == 0:
        lines.append("No detectamos productos desaparecidos este mes.")
        return "\n".join(lines)

    for _, row in removed_products.head(top_n).iterrows():
        lines.append(f"• {row['product_name']}")

    extra = total - min(total, top_n)
    if extra > 0:
        lines += ["", f"+{extra} productos más"]

    return "\n".join(lines)


def save_list_card(df: pd.DataFrame, title: str, out_name: str, top_n: int) -> str:
    ensure_output_dir()
    out_path = os.path.join(OUTPUT_DIR, out_name)

    names = df["product_name"].head(top_n).tolist()
    if not names:
        names = ["Sin cambios este mes"]

    # Tarjeta visual simple con texto
    n_lines = len(names) + 2
    fig_height = max(5.5, 0.55 * n_lines + 1.8)

    fig, ax = plt.subplots(figsize=(10, fig_height))
    ax.axis("off")

    ax.text(
        0.02, 0.96, title,
        fontsize=18, fontweight="bold",
        va="top", ha="left",
        transform=ax.transAxes
    )

    y = 0.84
    step = 0.07 if len(names) <= 8 else 0.055

    for name in names:
        ax.text(
            0.04, y, f"• {name}",
            fontsize=13,
            va="top", ha="left",
            transform=ax.transAxes,
            wrap=True
        )
        y -= step

    plt.tight_layout()
    plt.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close()

    return out_path


def main():
    ensure_output_dir()

    with tempfile.TemporaryDirectory() as tmpdir:
        snapshots = load_release_snapshots(CCAA, tmpdir)
        if not snapshots:
            raise RuntimeError(f"No se han encontrado snapshots para {CCAA}")

        prev_snap, last_snap, prev_dt, last_dt = get_month_end_snapshots(snapshots)

        df_prev = load_csv(prev_snap.csv_path)
        df_last = load_csv(last_snap.csv_path)

        new_products, removed_products = build_new_and_removed(df_prev, df_last)

        new_txt = build_new_products_tweet(new_products, CCAA, last_dt, TOP_N)
        removed_txt = build_removed_products_tweet(removed_products, CCAA, last_dt, TOP_N)

        new_txt_path = write_text_file("tweet_new_products.txt", new_txt)
        removed_txt_path = write_text_file("tweet_removed_products.txt", removed_txt)

        new_png_path = save_list_card(
            new_products,
            title=f"🆕 Nuevos productos · {month_label_es(last_dt)} · {CCAA.capitalize()}",
            out_name="tweet_new_products.png",
            top_n=TOP_N,
        )

        removed_png_path = save_list_card(
            removed_products,
            title=f"🚫 Productos desaparecidos · {month_label_es(last_dt)} · {CCAA.capitalize()}",
            out_name="tweet_removed_products.png",
            top_n=TOP_N,
        )

        print("✅ Product churn assets generados:")
        print(new_txt_path)
        print(new_png_path)
        print(removed_txt_path)
        print(removed_png_path)
        print(f"Comparación mensual: {prev_snap.date_str} -> {last_snap.date_str}")
        print(f"Nuevos detectados: {len(new_products)}")
        print(f"Desaparecidos detectados: {len(removed_products)}")


if __name__ == "__main__":
    main()