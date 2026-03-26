# scripts/price_analytics.py
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class AnalyticsSummary:
    baseline_date: str
    latest_date: str
    weekly_date: Optional[str]
    avg_change: float
    top_up: pd.DataFrame
    top_down: pd.DataFrame
    weekly_top_up: pd.DataFrame
    weekly_top_down: pd.DataFrame
    weekly_up_count: int
    weekly_down_count: int


def normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["product_id", "product_name", "price", "slug"]
    out = df[cols].copy()
    out = out.dropna(subset=["product_id", "price"])
    out = out.drop_duplicates(subset=["product_id"])
    return out

def build_product_url(product_id, slug) -> str | None:
    if pd.isna(product_id) or pd.isna(slug):
        return None

    try:
        product_id = int(product_id)
    except Exception:
        return None

    slug = str(slug).strip()
    if not slug:
        return None

    return f"https://tienda.mercadona.es/product/{product_id}/{slug}"

def build_variation_df(df_now: pd.DataFrame, df_ref: pd.DataFrame, ref_label: str) -> pd.DataFrame:
    now = normalize_snapshot_df(df_now).rename(
        columns={
            "price": "price_today",
            "slug": "slug",
        }
    )

    ref = normalize_snapshot_df(df_ref).rename(
        columns={
            "price": f"price_{ref_label}",
            "product_name": f"product_name_{ref_label}",
            "slug": f"slug_{ref_label}",
        }
    )

    df = now.merge(
        ref[["product_id", f"price_{ref_label}"]],
        on="product_id",
        how="inner"
    )

    df = df[df[f"price_{ref_label}"] > 0].copy()

    df["pct_change"] = (
        (df["price_today"] - df[f"price_{ref_label}"]) / df[f"price_{ref_label}"] * 100
    )

    df["product_url"] = df.apply(
        lambda r: build_product_url(r["product_id"], r["slug"]),
        axis=1
    )

    return df


def get_top_risers(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=df.columns)
    return df.sort_values("pct_change", ascending=False).head(top_n).copy()


def get_top_fallers(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=df.columns)
    return df.sort_values("pct_change", ascending=True).head(top_n).copy()


def get_average_change(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(df["pct_change"].mean())


def build_summary(
    df_base: pd.DataFrame,
    df_latest: pd.DataFrame,
    baseline_date: str,
    latest_date: str,
    df_week: Optional[pd.DataFrame] = None,
    weekly_date: Optional[str] = None,
    top_n: int = 5
) -> AnalyticsSummary:
    df_hist = build_variation_df(df_latest, df_base, "base")

    avg_change = get_average_change(df_hist)
    top_up = get_top_risers(df_hist, top_n=top_n)
    top_down = get_top_fallers(df_hist, top_n=top_n)

    weekly_top_up = pd.DataFrame()
    weekly_top_down = pd.DataFrame()
    weekly_up_count = 0
    weekly_down_count = 0

    if df_week is not None:
        df_week_var = build_variation_df(df_latest, df_week, "week")

        ups = df_week_var[df_week_var["pct_change"] > 0].copy()
        downs = df_week_var[df_week_var["pct_change"] < 0].copy()

        weekly_up_count = int(len(ups))
        weekly_down_count = int(len(downs))
        weekly_top_up = get_top_risers(ups, top_n=top_n) if not ups.empty else pd.DataFrame()
        weekly_top_down = get_top_fallers(downs, top_n=top_n) if not downs.empty else pd.DataFrame()

    return AnalyticsSummary(
        baseline_date=baseline_date,
        latest_date=latest_date,
        weekly_date=weekly_date,
        avg_change=avg_change,
        top_up=top_up,
        top_down=top_down,
        weekly_top_up=weekly_top_up,
        weekly_top_down=weekly_top_down,
        weekly_up_count=weekly_up_count,
        weekly_down_count=weekly_down_count,
    )


def build_price_index_series(snapshots_with_df: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    if not snapshots_with_df:
        return pd.DataFrame(columns=["date", "avg_pct_change"])

    baseline_date, df_base = snapshots_with_df[0]
    base = normalize_snapshot_df(df_base)[["product_id", "price"]].rename(columns={"price": "price_base"})

    rows = []
    for date_str, df_now in snapshots_with_df:
        now = normalize_snapshot_df(df_now)[["product_id", "price"]]
        df = now.merge(base, on="product_id", how="inner")
        df = df[df["price_base"] > 0].copy()
        df["pct_change"] = (df["price"] - df["price_base"]) / df["price_base"] * 100

        rows.append({
            "date": pd.to_datetime(date_str),
            "avg_pct_change": df["pct_change"].mean() if not df.empty else 0.0
        })

    return pd.DataFrame(rows).sort_values("date")

    