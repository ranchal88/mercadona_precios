import os
import requests
import pandas as pd
from datetime import datetime

# ==============================
# CONFIG
# ==============================

BASE_URL = "https://tienda.mercadona.es/api/categories/112/"
DATA_FOLDER = "data"
LANG = "es"

WAREHOUSES = {
    "andalucia": ["4694", "3968", "4544", "4354", "svq1"],
    "aragon": ["4665", "4389", "4493"],
    "asturias": ["4480"],
    "islas_baleares": ["3842"],
    "canarias": ["3280", "4701"],
    "cantabria": ["4522"],
    "castilla_y_leon": ["4077", "4471", "3880", "4673", "2316", "4346", "3681", "4735", "3683"],
    "castilla_la_mancha": ["4587", "4568", "4606", "4241"],
    "cataluna": ["3947", "2004", "4115", "bcn1"],
    "comunidad_valenciana": ["alc1", "vlc1", "4558"],
    "extremadura": ["4055", "3497"],
    "galicia": ["4450", "4655", "4166", "4592"],
    "madrid": ["mad1"],
    "murcia": ["alc1"],
    "navarra": ["4229"],
    "la_rioja": ["4375"],
    "pais_vasco": ["4391", "4331", "4697"]
}

# ==============================
# SCRAPING
# ==============================

def scrape_category(warehouse_id: str) -> dict | None:
    url = f"{BASE_URL}?lang={LANG}&wh={warehouse_id}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Error warehouse {warehouse_id}: {e}")
        return None

# ==============================
# NORMALIZATION
# ==============================

def extract_products(ccaa: str, warehouse: str, raw: dict, date: str) -> list[dict]:
    rows = []

    for cat in raw.get("categories", []):
        category_id = cat.get("id")
        category_name = cat.get("name")

        for product in cat.get("products", []):
            price_info = product.get("price_instructions", {})

            rows.append({
                "date": date,
                "ccaa": ccaa,
                "warehouse": warehouse,
                "category_id": category_id,
                "category_name": category_name,
                "product_id": product.get("id"),
                "product_name": product.get("name"),
                "price": price_info.get("unit_price"),
                "price_per_unit": price_info.get("bulk_price"),
                "unit_size": price_info.get("unit_size"),
                "unit_name": price_info.get("unit_name"),
                "is_pack": price_info.get("is_pack"),
                "pack_size": price_info.get("pack_size"),
                "iva": price_info.get("iva")
            })

    return rows

# ==============================
# SAVE CSV
# ==============================

def save_csv(ccaa: str, rows: list[dict], date: str):
    if not rows:
        return

    path = os.path.join(DATA_FOLDER, ccaa)
    os.makedirs(path, exist_ok=True)

    file = os.path.join(path, f"mercadona_{ccaa}_{date}.csv")
    df = pd.DataFrame(rows)

    df.to_csv(
        file,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )

    print(f"✅ {ccaa}: {len(df)} productos guardados")

# ==============================
# MAIN
# ==============================

def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for ccaa, warehouses in WAREHOUSES.items():
        print(f"\n🏴‍☠️ CCAA: {ccaa}")
        all_rows = []

        for wh in warehouses:
            print(f"  ↳ Warehouse {wh}")
            raw = scrape_category(wh)
            if raw:
                all_rows.extend(extract_products(ccaa, wh, raw, today))

        save_csv(ccaa, all_rows, today)

if __name__ == "__main__":
    main()
