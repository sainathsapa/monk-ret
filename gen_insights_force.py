# gen_insights_force.py
import argparse
import json
import math
import sys
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from mcp_monkdb.mcp_server import run_select_query

TABLE = "trent.products"


def q(sql: str) -> pd.DataFrame:
    print(f"[DEBUG] SQL =>\n{sql}\n", flush=True)
    res = run_select_query(sql)
    if isinstance(res, dict) and res.get("status") == "error":
        raise RuntimeError(res["message"])
    return pd.DataFrame(res or [])


def _q(v: str) -> str:
    """Single-quote/escape for SQL literals."""
    return "'" + str(v).replace("'", "''") + "'"


def build_where(filters: Dict[str, Any], include_rating: bool) -> str:
    """
    When include_rating=False, omit rating/review predicates (for KPIs/brands/bands).
    When include_rating=True, add rating/review predicates (for 'top_discounted_rated').
    Supported filters:
      - brands: [str], exclude_brands: [str]
      - min_discount, max_discount: int
      - price_between: [min, max] (float)
      - mrp_between:   [min, max] (float)
      - img_count_between: [min, max] (int)
      - title_ilike: str (uses ILIKE; %/_ wildcards supported)
      - only_discounted: bool  (price < mrp)
      - only_no_discount: bool (price = mrp)
    Rated-only (only applied when include_rating=True):
      - min_rating, max_rating: float
      - min_reviews: int  (rating_total >= X)
    """
    clauses = ["1=1"]

    # brand includes/excludes
    brands = filters.get("brands") or []
    if brands:
        clauses.append(f"brand IN ({','.join(_q(b) for b in brands)})")

    excl = filters.get("exclude_brands") or []
    if excl:
        clauses.append(f"brand NOT IN ({','.join(_q(b) for b in excl)})")

    # discount range
    if (m := filters.get("min_discount")) is not None:
        clauses.append(f"discount_percent >= {int(m)}")
    if (M := filters.get("max_discount")) is not None:
        clauses.append(f"discount_percent <= {int(M)}")

    # price range
    pb = filters.get("price_between")
    if isinstance(pb, (list, tuple)) and len(pb) == 2:
        clauses.append(f"price BETWEEN {float(pb[0])} AND {float(pb[1])}")

    # mrp range
    mb = filters.get("mrp_between")
    if isinstance(mb, (list, tuple)) and len(mb) == 2:
        clauses.append(f"mrp BETWEEN {float(mb[0])} AND {float(mb[1])}")

    # image count range
    ib = filters.get("img_count_between")
    if isinstance(ib, (list, tuple)) and len(ib) == 2:
        clauses.append(f"img_count BETWEEN {int(ib[0])} AND {int(ib[1])}")

    # title search (ILIKE)
    pat = filters.get("title_ilike")
    if pat:
        like = pat if ("%" in pat or "_" in pat) else f"%{pat}%"
        clauses.append(f"title ILIKE {_q(like)}")

    # discount toggles (mutually exclusive; no-discount wins if both set)
    if filters.get("only_no_discount"):
        clauses.append("price = mrp")
    elif filters.get("only_discounted"):
        clauses.append("price < mrp")

    # rated-only
    if include_rating:
        if (rmin := filters.get("min_rating")) is not None:
            clauses.append(f"rating >= {float(rmin)}")
        if (rmax := filters.get("max_rating")) is not None:
            clauses.append(f"rating <= {float(rmax)}")
        if (rv := filters.get("min_reviews")) is not None:
            clauses.append(f"rating_total >= {int(rv)}")

    return " AND ".join(clauses)


# ------- safe casters -------
def sf(v):
    try:
        if v is None:
            return 0.0
        f = float(v)
        return 0.0 if (f != f) else f  # NaN guard
    except Exception:
        return 0.0


def si(v):
    try:
        if v is None:
            return 0
        return int(v)
    except Exception:
        return 0


# ------- queries -------
def core_kpis(where_no_rating: str) -> dict:
    # KPIs MUST NOT apply rating filter. Force-COALESCE so no NULLs.
    df = q(f"""
        SELECT
          COALESCE(ROUND(AVG(price),2), 0)                AS avg_price,
          COALESCE(ROUND(AVG(mrp),2), 0)                  AS avg_mrp,
          COALESCE(ROUND(AVG(discount_percent),2), 0)     AS avg_discount_pct,
          COALESCE(SUM(CASE WHEN price = mrp THEN 1 ELSE 0 END), 0) AS no_discount_items,
          COALESCE(COUNT(*), 0)                           AS products
        FROM {TABLE}
        WHERE {where_no_rating}
    """)
    if df.empty:
        return {
            "avg_price": 0,
            "avg_mrp": 0,
            "avg_discount_pct": 0,
            "no_discount_items": 0,
            "products": 0
        }
    return df.iloc[0].to_dict()


def brand_concentration(where_no_rating: str) -> list:
    df = q(f"""
        SELECT t.brand, t.items,
               CASE WHEN total.s > 0 THEN ROUND(100.0 * t.items / total.s, 2) ELSE 0 END AS share_pct
        FROM (
          SELECT brand, COUNT(*) AS items
          FROM {TABLE}
          WHERE {where_no_rating}
          GROUP BY brand
        ) t
        CROSS JOIN (
          SELECT COUNT(*) AS s
          FROM {TABLE}
          WHERE {where_no_rating}
        ) total
        ORDER BY t.items DESC
        LIMIT 10
    """)
    return df.to_dict(orient="records")


def discount_bands(where_no_rating: str) -> list:
    df = q(f"""
        SELECT band, COUNT(*) AS items FROM (
          SELECT CASE
            WHEN discount_percent = 0 THEN '0%'
            WHEN discount_percent < 20 THEN '0-20%'
            WHEN discount_percent < 40 THEN '20-40%'
            WHEN discount_percent < 60 THEN '40-60%'
            ELSE '60%+'
          END AS band
          FROM {TABLE}
          WHERE {where_no_rating}
        ) b
        GROUP BY band
        ORDER BY items DESC
    """)
    return df.to_dict(orient="records")


def top_discounted_rated(where_with_rating: str, limit_n: int) -> list:
    limit_n = max(1, min(int(limit_n), 1000))
    df = q(f"""
        SELECT product_id, title, brand, price, mrp, discount_percent, rating, rating_total
        FROM {TABLE}
        WHERE {where_with_rating} AND rating_total > 0
        ORDER BY discount_percent DESC, price ASC
        LIMIT {limit_n}
    """)
    return df.to_dict(orient="records")


def bullets(k, brands, bands) -> list:
    out = []
    ap, am, ad = sf(k.get("avg_price")), sf(
        k.get("avg_mrp")), sf(k.get("avg_discount_pct"))
    prod, nd = si(k.get("products")), si(k.get("no_discount_items"))

    if prod > 0:
        out.append(
            f"Avg price ₹{ap:,.2f}, avg MRP ₹{am:,.2f}, avg discount {ad:.2f}% across {prod:,} items.")
        out.append(f"{nd:,} items are at MRP (0% discount).")
    else:
        out.append("No items match the current filters.")
        return out

    if brands:
        b0 = brands[0]
        out.append(
            f"Top brand: {b0['brand']} with {si(b0.get('items')):,} items (~{sf(b0.get('share_pct')):.2f}%)."
        )

    if bands:
        total = sum(si(r.get("items")) for r in bands) or 1
        bmax = max(bands, key=lambda r: si(r.get("items")))
        out.append(
            f"Largest discount band: {bmax['band']} at {si(bmax.get('items')):,} items (~{100.0 * si(bmax.get('items'))/total:.1f}%)."
        )
    return out


def main():
    print(f"[DEBUG] running file: {__file__}", flush=True)
    print(f"[DEBUG] python exe : {sys.executable}", flush=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="analytics_out/insights_pack.json")
    ap.add_argument("--filters-json", default="{}")
    args = ap.parse_args()

    filters = json.loads(args.filters_json or "{}")

    # WHEREs: no-rating for aggregates; with-rating for rated list
    where_no_rating = build_where(filters, include_rating=False)
    where_with_rating = build_where(filters, include_rating=True)

    print(f"[DEBUG] where_no_rating   = {where_no_rating}", flush=True)
    print(f"[DEBUG] where_with_rating = {where_with_rating}", flush=True)

    # Optional: how many rows to show in the rated table
    top_limit = int(filters.get("top_limit", 10))

    k = core_kpis(where_no_rating)
    bc = brand_concentration(where_no_rating)
    db = discount_bands(where_no_rating)
    td = top_discounted_rated(where_with_rating, top_limit)

    print(f"[DEBUG] KPIs dict                 : {k}", flush=True)
    print(f"[DEBUG] Brand concentration rows  : {len(bc)}", flush=True)
    print(f"[DEBUG] Discount bands rows       : {len(db)}", flush=True)
    print(f"[DEBUG] Top discounted rated rows : {len(td)}", flush=True)

    pack = {
        "kpis": k,
        "tables": {
            "brand_concentration": bc,
            "discount_bands": db,
            "top_discounted_rated": td
        },
        "bullets": bullets(k, bc, db)
    }

    # out_path = Path(args.out)
    # out_path.parent.mkdir(parents=True, exist_ok=True)
    # out_path.write_text(json.dumps(
    #     pack, ensure_ascii=False, indent=2), encoding="utf-8")
    # print(f"wrote {args.out}")
    
    return pack


if __name__ == "__main__":
    main()
