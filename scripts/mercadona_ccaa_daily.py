import os
import time
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
SLEEP_BETWEEN_REQUESTS = 0.05  # 20 req/seg aprox

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://tienda.mercadona.es/"
}

# Warehouses por CCAA
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

# Categorías válidas (curadas, estables)
CATEGORY_IDS = [
    27, 28, 29, 31, 32, 34, 36, 37, 38, 40, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    51, 52, 53, 54, 56, 58, 59, 60, 62, 64, 65, 66, 68, 69, 71, 72, 75, 77, 78,
    79, 80, 81, 83, 84, 86, 88, 89, 90, 92, 95, 97, 98, 99, 100, 103, 104, 105,
    106, 107, 108, 109, 110, 111, 112, 115, 116, 117, 118, 120, 121, 122, 123,
    126, 127, 129, 130, 132, 133, 135, 138, 140, 142, 143, 145, 147, 148, 149,
    150, 151, 152, 154, 155, 156, 158, 159, 161, 162, 163, 164, 166, 168, 169,
    170, 171, 173, 174, 181, 185, 186, 187, 188, 189, 190, 191, 192, 194, 196,
    198, 199, 201, 202, 203, 206, 207, 208, 210, 212, 213, 214, 216, 217, 218,
    219, 221, 222, 225, 226, 229, 230, 231, 232, 233, 234, 235, 237, 238, 239,
    241, 243, 244
]

# ==============================
# HTTP
# ==============================

def get_json(url: str) -> Dict | None:
    try:
        r = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"⚠️ Error GET {url}: {e}")
        return None

# ==============================
# SCRAPING
# ==============================

def scrape_category(category_id: int, warehouse: str) -> Dict | None:
    url = f"{BASE_URL}/{category_id}/?lang={LANG}&wh={warehouse}"
    return get_json(url)

# ==============================
# NORMALIZATION
# ==============================

def extract_products(
    ccaa: str,
    warehouse: str,
    raw: Dict,
    date: str
) -> List[Dict]:

    rows = []

    for subcat in raw.get("categories", []):
        subcat_id = subcat.get("id")
        subcat_name = subcat.get("name")

        for p in subcat.get("products", []):
            price = p.get("price_instructions", {})

            rows.append({
                "date": date,
                "ccaa": ccaa,
                "warehouse": warehouse,
                "category_id": raw.get("id"),
                "subcategory_id": subcat_id,
                "subcategory_name": subcat_name,
                "product_id": p.get("id"),
                "product_name": p.get("display_name"),
                "slug": p.get("slug"),
                "packaging": p.get("packaging"),
                "published": p.get("published"),
                "price": price.get("unit_price"),
                "price_per_unit": price.get("bulk_price"),
                "unit_size": price.get("unit_size"),
                "size_format": price.get("size_format"),
                "selling_method": price.get("selling_method"),
                "is_new": price.get("is_new"),
                "price_decreased": price.get("price_decreased"),
                "iva": price.get("iva")
            })

    return rows

# ==============================
# SAVE CSV
# ==============================

def save_csv(ccaa: str, rows: List[Dict], date: str) -> int:
    if not rows:
        print(f"⚠️ {ccaa}: sin datos")
        return 0

    path = os.path.join(DATA_FOLDER, ccaa)
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, f"mercadona_{ccaa}_{date}.csv")
    df = pd.DataFrame(rows)

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
    return len(df)

# ==============================
# MAIN
# ==============================

def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    total_products = 0

    for ccaa, warehouses in WAREHOUSES.items():
        print(f"\n🏴‍☠️ CCAA: {ccaa}")
        all_rows = []

        for wh in warehouses:
            print(f"  ↳ Warehouse {wh}")
            for cat_id in CATEGORY_IDS:
                raw = scrape_category(cat_id, wh)
                if raw:
                    all_rows.extend(
                        extract_products(ccaa, wh, raw, today)
                    )
                time.sleep(SLEEP_BETWEEN_REQUESTS)

        total_products += save_csv(ccaa, all_rows, today)

    if total_products == 0:
        print("❌ No se han generado datos en ninguna CCAA")
        exit(1)

    print(f"\n✅ TOTAL productos generados: {total_products}")

if __name__ == "__main__":
    main()
