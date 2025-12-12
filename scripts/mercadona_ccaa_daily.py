import os
import requests
import pandas as pd
from datetime import datetime

# Definir las CCAA y sus warehouses
warehouses = {
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

# Directorio de datos
data_folder = "data"

# Función para realizar el scraping
def scrape_warehouse(ccaa, warehouse_id):
    url = f"https://tienda.mercadona.es/api/categories/112/?lang=es&wh={warehouse_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener datos para {ccaa} desde warehouse {warehouse_id}")
        return None

# Función para guardar los datos en un CSV
def save_data(ccaa, data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(data_folder, ccaa, f"mercadona_{ccaa}_{date_str}.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"Datos guardados en {file_path}")

# Scrapeo para cada CCAA
def scrape_all():
    for ccaa, warehouse_ids in warehouses.items():
        all_data = []
        for warehouse_id in warehouse_ids:
            print(f"🏴‍☠️ Scrapeando {ccaa} desde warehouse {warehouse_id}...")
            data = scrape_warehouse(ccaa, warehouse_id)
            if data:
                all_data.append(data)
        if all_data:
            save_data(ccaa, all_data)

if __name__ == "__main__":
    scrape_all()
