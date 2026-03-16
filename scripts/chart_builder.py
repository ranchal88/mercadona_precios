# scripts/chart_builder.py
import os
import matplotlib.pyplot as plt
import pandas as pd


OUTPUT_DIR = "outputs"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_price_index_chart(df_index: pd.DataFrame, ccaa: str, out_name: str) -> str:
    ensure_output_dir()
    out_path = os.path.join(OUTPUT_DIR, out_name)

    plt.figure(figsize=(10, 6))
    plt.plot(df_index["date"], df_index["avg_pct_change"], marker="o")
    plt.title(f"Precio medio Mercadona · {ccaa.capitalize()}")
    plt.ylabel("% vs inicio del año")
    plt.xlabel("")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close()

    return out_path


def save_top_changes_chart(df: pd.DataFrame, title: str, out_name: str) -> str:
    ensure_output_dir()
    out_path = os.path.join(OUTPUT_DIR, out_name)

    plot_df = df.copy()
    if plot_df.empty:
        raise RuntimeError("No hay datos para generar el gráfico")

    plot_df = plot_df.sort_values("pct_change", ascending=True)

    labels = plot_df["product_name"].tolist()
    values = plot_df["pct_change"].tolist()

    plt.figure(figsize=(10, 6))
    plt.barh(labels, values)
    plt.title(title)
    plt.xlabel("% variación")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close()

    return out_path