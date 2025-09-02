import os
import glob
import math
import configparser
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

# ---- MonkDB client (your existing lib) ----
from monkdb import client as monk_client

# ---------------- Config -------------------
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(CURRENT_DIR, "config", "config.ini")
config = configparser.ConfigParser()
config.read(CONFIG_FILE_PATH, encoding="utf-8")

DB_HOST = config["database"]["DB_HOST"]
DB_PORT = config["database"]["DB_PORT"]
DB_USER = config["database"]["DB_USER"]
DB_PASSWORD = config["database"]["DB_PASSWORD"]
DB_SCHEMA = config["database"]["DB_SCHEMA"]
TABLE_NAME = config["database"]["TABLE_NAME"]

FOLDER_PATH = "./csv_folder"         # change if needed
BLOCKSIZE = "64MB"                 # CSV chunk size for Dask
N_WORKERS = min(os.cpu_count() or 4, 8)   # be gentle with DB; cap workers
THREADS_PER_W = 2
BATCH_SIZE = 5000                   # rows per executemany
# ------------------------------------------

INSERT_SQL = f"""
INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
(product_id, style_id, title, brand, price, mrp, discount_percent, rating, rating_total, img_primary, img_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _as_int(x):
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def _as_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _as_str(x):
    try:
        if pd.isna(x):
            return None
        s = str(x)
        return s if s.lower() != "nan" else None
    except Exception:
        return None


def _connect():
    """Create a fresh MonkDB connection for the current worker."""
    return monk_client.connect(
        f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
        username=DB_USER
    )


def _ingest_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Runs inside each Dask worker. Converts the Pandas partition to DB rows,
    writes in batches with executemany, and returns a 1-row DataFrame
    indicating rows inserted for monitoring.
    """
    if pdf.empty:
        return pd.DataFrame({"rows_inserted": [0]})

    # Normalize / enforce the expected columns
    needed = ["product_id", "style_id", "title", "brand", "price", "mrp",
              "discount_percent", "rating", "rating_total", "img_primary", "img_count"]
    missing = [c for c in needed if c not in pdf.columns]
    if missing:
        # Create missing with None/0
        for c in missing:
            pdf[c] = None

    # Build batch rows
    batch = []
    total = 0

    # Open a fresh connection in the worker
    conn = _connect()
    cur = conn.cursor()

    try:
        for _, row in pdf.iterrows():
            values = (
                _as_int(row["product_id"]),
                _as_int(row["style_id"]),
                _as_str(row["title"]),
                _as_str(row["brand"]),
                _as_float(row["price"]),
                _as_float(row["mrp"]),
                _as_float(row["discount_percent"]),
                _as_float(row["rating"]),
                _as_int(row["rating_total"]),
                _as_str(row["img_primary"]),
                _as_int(row["img_count"]),
            )
            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                cur.executemany(INSERT_SQL, batch)
                conn.commit()
                total += len(batch)
                batch.clear()

        if batch:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            total += len(batch)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return pd.DataFrame({"rows_inserted": [total]})


def main():
    # Spin up local Dask cluster
    cluster = LocalCluster(
        n_workers=N_WORKERS,
        threads_per_worker=THREADS_PER_W,
        processes=True,
        dashboard_address=None,  # set ":8787" to view dashboard locally
    )
    client = Client(cluster)
    print(f"✅ Dask cluster up: {N_WORKERS} workers x {THREADS_PER_W} threads")

    # Read all CSVs with Dask (parallel, out-of-core)
    pattern = os.path.join(FOLDER_PATH, "*.csv")
    ddf = dd.read_csv(
        pattern,
        blocksize=BLOCKSIZE,
        assume_missing=True,  # prevents overly strict dtypes
        dtype=str,            # read as strings; cast later safely
        encoding="utf-8",
        on_bad_lines="skip",
    )

    # Optional: basic column normalization to expected names if your CSVs vary
    # (Uncomment and adapt if needed)
    # rename_map = {"ratingTotal": "rating_total", "img": "img_primary"}  # example
    # ddf = ddf.rename(columns={k: v for k, v in rename_map.items() if k in ddf.columns})

    # Force known columns to exist (others will be ignored by _ingest_partition)
    for col in ["product_id", "style_id", "title", "brand", "price", "mrp",
                "discount_percent", "rating", "rating_total", "img_primary", "img_count"]:
        if col not in ddf.columns:
            ddf[col] = None

    # Partition-wise ingestion → returns a small DF per partition
    results = ddf.map_partitions(_ingest_partition, meta={
                                 "rows_inserted": "int64"}).compute()

    total_inserted = int(results["rows_inserted"].sum()
                         ) if not results.empty else 0
    print(f"✅ Inserted {total_inserted} records into {DB_SCHEMA}.{TABLE_NAME}")

    client.close()
    cluster.close()


if __name__ == "__main__":
    main()
