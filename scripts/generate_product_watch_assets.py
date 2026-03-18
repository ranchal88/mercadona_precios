# scripts/generate_product_watch_assets.py
import os
import tempfile
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from release_data_loader import load_release_snapshots, load_csv

CCAA = os.environ.get("CCAA", "madrid")
PRODUCT_QUERY = os.environ.get("PRODUCT_QUERY", "Aceite de oliva virgen extra Hacendado")
OUTPUT_DIR = "outputs"
BASELINE_DATE = os.environ.get("BASELINE_DATE", "2026-01-04")


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fmt_eur(x: float) -> str:
    return f"{x:.2f}€"


def fmt_pct(x: float) -> str:
    return f"{x:+.4f}%"


def slugify(text: str) -> str:
    keep = []
    for ch in text.lower():
        if ch.isalnum():
            keep.append(ch)
        elif ch in (" ", "-", "_"):
            keep.append("_")
    out = "".join(keep)
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")[:80]


def find_product_rows(df: pd.DataFrame, query: str) -> pd.DataFrame:
    mask = df["product_name"].fillna("").str.contains(query, case=False, regex=False)
    out = df.loc[mask].copy()
    out = out.dropna(subset=["price"])
    return out


def pick_single_product_id(snapshots, query: str):
    # usar el último snapshot para decidir el product_id correcto
    latest = snapshots[-1]
    df_latest = load_csv(latest.csv_path)
    matches = find_product_rows(df_latest, query)

    if matches.empty:
        raise RuntimeError(f"No se ha encontrado ningún producto para query: {query}")

    # prioriza coincidencia exacta por nombre
    exact = matches[matches["product_name"].str.lower() == query.lower()]
    chosen = exact.iloc[0] if not exact.empty else matches.iloc[0]

    return int(chosen["product_id"]), str(chosen["product_name"])


def build_monthly_series(snapshots, product_id: int) -> pd.DataFrame:
    rows = []

    for snap in snapshots:
        df = load_csv(snap.csv_path)
        hit = df[df["product_id"] == product_id].copy()
        if hit.empty:
            continue

        price = pd.to_numeric(hit.iloc[0]["price"], errors="coerce")
        if pd.isna(price):
            continue

        date = pd.to_datetime(snap.date_str)
        rows.append({
            "snapshot_date": date,
            "month": date.to_period("M"),
            "price": float(price),
        })

    if not rows:
        raise RuntimeError("No hay histórico para ese product_id")

    out = pd.DataFrame(rows).sort_values("snapshot_date")

    # último snapshot de cada mes
    monthly = (
        out.sort_values("snapshot_date")
           .groupby("month", as_index=False)
           .tail(1)
           .sort_values("snapshot_date")
           .reset_index(drop=True)
    )

    return monthly


def get_baseline_price(snapshots, product_id: int, baseline_date: str) -> float:
    for snap in snapshots:
        if snap.date_str == baseline_date:
            df = load_csv(snap.csv_path)
            hit = df[df["product_id"] == product_id]
            if not hit.empty:
                return float(hit.iloc[0]["price"])
    raise RuntimeError(f"No se ha encontrado precio baseline para {baseline_date}")


def save_chart(monthly: pd.DataFrame, product_name: str, out_name: str) -> str:
    ensure_output_dir()
    out_path = os.path.join(OUTPUT_DIR, out_name)

    labels = monthly["snapshot_date"].dt.strftime("%d/%m/%Y")
    values = monthly["price"]

    plt.figure(figsize=(10, 6))
    plt.plot(labels, values, marker="o")
    plt.title(f"Precio mensual · {product_name} · Madrid")
    plt.ylabel("Precio (€)")
    plt.xlabel("")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close()

    return out_path


def save_text(text: str, out_name: str) -> str:
    ensure_output_dir()
    out_path = os.path.join(OUTPUT_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text.strip() + "\n")
    return out_path


def build_tweet(product_name: str, monthly: pd.DataFrame, baseline_price: float) -> str:
    latest_row = monthly.iloc[-1]
    latest_date = latest_row["snapshot_date"].strftime("%d/%m/%Y")
    latest_price = float(latest_row["price"])

    pct = (latest_price - baseline_price) / baseline_price * 100 if baseline_price > 0 else 0.0

    lines = [
        f"📊 Precio mensual {product_name} · Madrid · {latest_date}",
        "",
    ]

    for _, row in monthly.tail(3).iterrows():
        lines.append(f"{row['snapshot_date'].strftime('%d/%m/%Y')}: {fmt_eur(row['price'])}")

    lines += [
        "",
        fmt_pct(pct)
    ]

    return "\n".join(lines)


def main():
    ensure_output_dir()

    with tempfile.TemporaryDirectory() as tmpdir:
        snapshots = load_release_snapshots(CCAA, tmpdir)
        if not snapshots:
            raise RuntimeError(f"No hay snapshots para {CCAA}")

        product_id, product_name = pick_single_product_id(snapshots, PRODUCT_QUERY)
        monthly = build_monthly_series(snapshots, product_id)
        baseline_price = get_baseline_price(snapshots, product_id, BASELINE_DATE)

        slug = slugify(product_name)

        txt = build_tweet(product_name, monthly, baseline_price)
        txt_path = save_text(txt, f"tweet_product_{slug}.txt")
        png_path = save_chart(monthly, product_name, f"tweet_product_{slug}.png")

        print("✅ Product watch assets generados:")
        print(txt_path)
        print(png_path)


if __name__ == "__main__":
    main()