import os
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict

# ==============================
# CONFIG
# ==============================

BASE_URL = "https://tienda.mercadona.es/api/categories"
LANG = "es"
DATA_FOLDER = "data"
REQUEST_TIMEOUT = 30

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
# HTTP HELPERS
# ==============================

def get_json(url: str) -> Dict | None:
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Error GET {url}: {e}")
        return None

# ==============================
# CATEGORY TREE
# ==============================

def get_all_category_ids(warehouse: str) -> List[int]:
    """
    Descarga el árbol completo de categorías y devuelve
    TODOS los IDs (padres + hojas)
    """
    url = f"{BASE_URL}/?lang={LANG}&wh={warehouse}"
    data = get_json(url)

    if not data:
        return []

    ids = set()

    def walk(categories):
        for c in categories:
            cid = c.get("id")
            if cid:
                ids.add(cid)
            if c.get("categories"):
                walk(c["categories"])

    walk(data.get("categories", []))
    return list(ids)

# ==============================
# CATEGORY SCRAPE
# ==============================

def scrape_category(category_id: int, warehouse: str) -> Dict | None:
    url = f"{BASE_URL}/{category_id}/?lang={LANG}&wh={warehouse}"
    return get_json(url)

# ==============================
# PRODUCT NORMALIZATION
# ==============================

def extract_products(
    ccaa: str,
    warehouse: str,
    raw: Dict,
    date: str
) -> List[Dict]:

    rows = []

    for cat in raw.get("categories", []):
        category_id = cat.get("id")
        category_name = cat.get("name")

        for product in cat.get("products", []):
            price = product.get("price_instructions", {})

            rows.append({
                "date": date,
                "ccaa": ccaa,
                "warehouse": warehouse,
                "category_id": category_id,
                "category_name": category_name,
                "product_id": product.get("id"),
                "product_name": product.get("name"),
                "price": price.get("unit_price"),
                "price_per_unit": price.get("bulk_price"),
                "unit_size": price.get("unit_size"),
                "unit_name": price.get("unit_name"),
                "is_pack": price.get("is_pack"),
                "pack_size": price.get("pack_size"),
                "iva": price.get("iva")
            })

    return rows

# ==============================
# SAVE CSV
# ==============================

def save_csv(ccaa: str, rows: List[Dict], date: str):
    if not rows:
        print(f"⚠️ {ccaa}: sin datos")
        return

    path = os.path.join(DATA_FOLDER, ccaa)
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, f"mercadona_{ccaa}_{date}.csv")
    df = pd.DataFrame(rows)

    # Deduplicación defensiva
    df = df.drop_duplicates(
        subset=["date", "ccaa", "warehouse", "product_id"]
    )

    df.to_csv(
        file_path,
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

            category_ids = get_all_category_ids(wh)
            print(f"    • {len(category_ids)} categorías")

            for cat_id in category_ids:
                raw = scrape_category(cat_id, wh)
                if raw:
                    all_rows.extend(
                        extract_products(ccaa, wh, raw, today)
                    )

        save_csv(ccaa, all_rows, today)
    if not all_rows:
    print("❌ No se han generado datos en ninguna CCAA")
    exit(1)


if __name__ == "__main__":
    main()
