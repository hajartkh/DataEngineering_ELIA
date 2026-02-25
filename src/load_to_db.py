import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

DATA_DIR = Path("data")

DATASETS = {
    "total_load": "elia_total_load",
    "total_generation_by_fuel": "elia_generation_by_fuel",
    "wind_power": "elia_wind_power",
    "solar_pv_power": "elia_solar_pv",
}


def load_json_to_df(label: str) -> pd.DataFrame:
    path = DATA_DIR / f"{label}.json"
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)
    df = pd.json_normalize(records)
    # Kolommen opschonen
    df.columns = (
        df.columns.str.lower()
        .str.replace(".", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )
    return df


def write_to_db(engine, df: pd.DataFrame, table_name: str) -> None:
    with engine.begin() as conn:
        df.to_sql(
            table_name,
            con=conn,
            if_exists="replace",   # gebruik 'append' voor incrementeel laden
            index=False,
        )
    print(f"  Geschreven naar tabel: {table_name} ({len(df)} rijen)")


if __name__ == "__main__":
    engine = create_engine(DB_URL)

    # Test verbinding
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Verbinding met database OK\n")

    for label, table in DATASETS.items():
        json_path = DATA_DIR / f"{label}.json"
        if not json_path.exists():
            print(f"  Bestand niet gevonden, overgeslagen: {json_path}")
            continue
        print(f"Laden: {label} → {table}")
        try:
            df = load_json_to_df(label)
            print(f"  DataFrame shape: {df.shape}")
            write_to_db(engine, df, table)
        except Exception as e:
            print(f"  FOUT bij {label}: {e}")
