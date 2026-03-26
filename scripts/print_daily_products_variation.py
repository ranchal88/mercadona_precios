import os
import tempfile
from pathlib import Path

import pandas as pd

from .release_data_loader import load_release_snapshots, load_csv

CCAA = os.environ.get("CCAA", "madrid")
BASELINE_DATE = pd.to_datetime(os.environ.get("BASELINE_DATE", "2026-01-04")).date()

PRODUCT_IDS = [
    10380, 10382, 10384, 22313, 20559, 21581,
    13810, 82328, 5044, 6258, 29100,
    3724, 2788, 2869, 31540, 18071,
    69937, 3819, 3028, 68130, 69089, 69066,
    4740, 4040, 19897, 19731, 11172, 14102,
]


def fmt_pct(x: float) -> str:
    return f"{x:+.2f}%"


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        snapshot_list = load_release_snapshots(ccaa=CCAA, tmpdir=tmpdir)

        if not snapshot_list:
            raise SystemExit(f"No se encontraron releases para {CCAA}")

        # Load all data into a single DataFrame
        dfs = []
        for snap in snapshot_list:
            df = load_csv(snap.csv_path)
            df['date'] = pd.to_datetime(snap.date_str)
            dfs.append(df)

        snapshots = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if snapshots.empty:
        raise SystemExit(f"No se encontraron datos para {CCAA}")

    # Nos quedamos solo con los productos que quieres trackear
    df = snapshots[snapshots["product_id"].isin(PRODUCT_IDS)].copy()

    if df.empty:
        raise SystemExit("No hay datos para los product_id indicados")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])

    results = []

    for product_id, group in df.groupby("product_id"):
        group = group.sort_values("date").copy()

        baseline_rows = group[group["date"] >= BASELINE_DATE]
        if baseline_rows.empty:
            continue

        baseline_row = baseline_rows.iloc[0]
        latest_row = group.iloc[-1]

        baseline_price = float(baseline_row["price"])
        latest_price = float(latest_row["price"])

        if baseline_price <= 0:
            continue

        variation = ((latest_price - baseline_price) / baseline_price) * 100

        results.append(
            {
                "product_id": int(product_id),
                "product_name": str(latest_row["product_name"]),
                "baseline_date": baseline_row["date"],
                "baseline_price": baseline_price,
                "latest_date": latest_row["date"],
                "latest_price": latest_price,
                "variation": variation,
            }
        )

    if not results:
        raise SystemExit(
            f"No se pudieron calcular variaciones desde {BASELINE_DATE} para los product_id indicados"
        )

    results_df = pd.DataFrame(results).sort_values("variation", ascending=False)

    print(f"\n📊 Variación desde {BASELINE_DATE.strftime('%d/%m/%Y')} · {CCAA.title()}\n")

    for _, row in results_df.iterrows():
        print(
            f"{fmt_pct(row['variation']):>8} | "
            f"{row['product_name']} | "
            f"ID: {int(row['product_id'])} | "
            f"{row['baseline_price']:.2f}€ → {row['latest_price']:.2f}€ "
            f"({row['baseline_date'].strftime('%d/%m/%Y')} → {row['latest_date'].strftime('%d/%m/%Y')})"
        )


if __name__ == "__main__":
    main()