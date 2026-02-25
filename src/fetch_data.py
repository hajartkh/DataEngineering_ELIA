import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("ELIA_API_BASE")  # https://opendata.elia.be/api/explore/v2.1/catalog/datasets
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DATASETS = {
    "ods001": "total_load",
    "ods031": "wind_power",
    "ods032": "solar_pv_power",
}

# Optioneel: filter op datum om volume te beperken
# Zet op None om alle data op te halen
DATE_FILTER = {
    "start": "2024-01-01",
    "end":   "2025-12-31",
}


def build_where_clause(date_field: str = "datetime") -> str | None:
    """Bouw een where-clause op basis van de datumfilter in .env of DATE_FILTER."""
    start = os.getenv("FILTER_START", DATE_FILTER.get("start"))
    end   = os.getenv("FILTER_END",   DATE_FILTER.get("end"))
    if start and end:
        return f"{date_field} >= '{start}' AND {date_field} <= '{end}'"
    return None


def fetch_dataset_export(dataset_id: str) -> list[dict]:
    """
    Gebruik het /exports/json endpoint — geen 10.000-rijlimiet.
    Streamt de volledige dataset in één request.
    """
    url = f"{BASE_URL}/{dataset_id}/exports/json"

    params = {"limit": -1}  # -1 = alle records
    where = build_where_clause()
    if where:
        params["where"] = where

    print(f"  URL: {url}")
    print(f"  Params: {params}")

    response = requests.get(url, params=params, timeout=120, stream=True)
    response.raise_for_status()

    data = response.json()

    # Exports endpoint geeft een lijst terug (geen wrapper dict)
    if isinstance(data, list):
        return data
    # Sommige versies geven toch een dict terug
    return data.get("results", data)


def save_raw(label: str, records: list[dict]) -> None:
    path = DATA_DIR / f"{label}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  Opgeslagen: {path} ({len(records)} records)")


if __name__ == "__main__":
    for ds_id, label in DATASETS.items():
        print(f"\nOphalen dataset: {ds_id} ({label})")
        try:
            records = fetch_dataset_export(ds_id)
            save_raw(label, records)
        except requests.exceptions.Timeout:
            print(f"  TIMEOUT bij {ds_id} — dataset mogelijk te groot, voeg datumfilter toe")
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP FOUT bij {ds_id}: {e.response.status_code} — {e.response.text[:200]}")
        except Exception as e:
            print(f"  FOUT bij {ds_id}: {e}")
        time.sleep(1)  # Respecteer rate limits