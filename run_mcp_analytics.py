import os
import json
import time
import pandas as pd
from pathlib import Path
from mcp_monkdb.mcp_server import run_select_query

OUTDIR = Path("./analytics_out")
OUTDIR.mkdir(parents=True, exist_ok=True)


def q(name: str, sql: str, limit_csv_rows: int | None = None) -> pd.DataFrame:
    """Run a SELECT via MCP and return a DataFrame. Also write a CSV."""
    print(f"\n=== {name} ===")
    t0 = time.time()
    res = run_select_query(sql)
    if isinstance(res, dict) and res.get("status") == "error":
        print(f"❌ {name}: {res['message']}")
        return pd.DataFrame()

    df = pd.DataFrame(res or [])
    if df.empty:
        print(" (no rows)")
        return df

    print(df.head(20).to_string(index=False))
    csv_path = OUTDIR / f"{name}.csv"
    df_out = df if limit_csv_rows is None else df.head(limit_csv_rows)
    df_out.to_csv(csv_path, index=False)
    print(
        f"→ saved {csv_path} ({len(df_out)} rows; full rows: {len(df)}) in {time.time()-t0:.2f}s")
    return df


def main():
    # 0) sanity
    q("row_counts", """
        SELECT
          COUNT(*) AS products,
          COUNT(DISTINCT brand) AS brands
        FROM trent.products
    """)

    # 1) core KPIs
    q("core_kpis", """
        SELECT
          ROUND(AVG(price),2) AS avg_price,
          ROUND(AVG(mrp),2)   AS avg_mrp,
          ROUND(AVG(discount_percent),2) AS avg_discount_pct,
          SUM(CASE WHEN price = mrp THEN 1 ELSE 0 END) AS no_discount_items
        FROM trent.products
    """)

    # 2) discount bands
    q("discount_bands", """
    SELECT band, COUNT(*) AS items
    FROM (
      SELECT CASE
        WHEN discount_percent = 0 THEN '0%'
        WHEN discount_percent < 20 THEN '0-20%'
        WHEN discount_percent < 40 THEN '20-40%'
        WHEN discount_percent < 60 THEN '40-60%'
        ELSE '60%+'
      END AS band
      FROM trent.products
    ) b
    GROUP BY band
    ORDER BY items DESC
""")

    # 3) brand-wise avg discount (min 5 items)
    q("brand_avg_discount_top20", """
        SELECT brand,
               COUNT(*) AS items,
               ROUND(AVG(discount_percent),2) AS avg_discount_pct
        FROM trent.products
        GROUP BY brand
        HAVING COUNT(*) >= 5
        ORDER BY avg_discount_pct DESC
        LIMIT 20
    """)

    # 4) brand concentration (share of catalog)
    q("brand_concentration_top20", """
    SELECT t.brand,
           t.c AS items,
           ROUND(100.0 * t.c / total.s, 2) AS share_pct
    FROM (
      SELECT brand, COUNT(*) AS c
      FROM trent.products
      GROUP BY brand
    ) t
    CROSS JOIN (
      SELECT COUNT(*) AS s
      FROM trent.products
    ) total
    ORDER BY t.c DESC
    LIMIT 20
""")

    # 5) ratings coverage & quality
    q("ratings_coverage", """
        SELECT
          SUM(CASE WHEN rating_total > 0 THEN 1 ELSE 0 END) AS rated_items,
          SUM(CASE WHEN rating_total = 0 THEN 1 ELSE 0 END) AS unrated_items,
          ROUND(AVG(NULLIF(rating, 0)), 2) AS avg_rating_nonzero
        FROM trent.products
    """)

    # 6) rating distribution (bands)
    q("rating_distribution", """
        SELECT CASE
          WHEN rating = 0 THEN '0 (unrated)'
          WHEN rating < 2 THEN '1.0-1.9'
          WHEN rating < 3 THEN '2.0-2.9'
          WHEN rating < 4 THEN '3.0-3.9'
          WHEN rating < 4.5 THEN '4.0-4.49'
          ELSE '4.5-5.0'
        END AS rating_band,
        COUNT(*) AS items
        FROM trent.products
        GROUP BY rating_band
        ORDER BY items DESC
    """)

    # 7) top products by rating & social proof
    q("top_rated_by_volume", """
        SELECT product_id, title, brand, rating, rating_total, price, mrp, discount_percent
        FROM trent.products
        WHERE rating_total >= 100 AND rating >= 4
        ORDER BY rating DESC, rating_total DESC
        LIMIT 50
    """)

    # 8) highest discounts among rated items
    q("highest_discounts_rated", """
        SELECT product_id, title, brand, price, mrp, discount_percent, rating, rating_total
        FROM trent.products
        WHERE rating_total > 0
        ORDER BY discount_percent DESC, price ASC
        LIMIT 50
    """)

    # 9) rating vs discount band
    q("rating_by_discount_band", """
    SELECT band,
           ROUND(AVG(NULLIF(rating,0)), 2) AS avg_rating_nonzero,
           SUM(rating_total) AS total_ratings,
           COUNT(*) AS items
    FROM (
      SELECT CASE
        WHEN discount_percent = 0 THEN '0%'
        WHEN discount_percent < 20 THEN '0-20%'
        WHEN discount_percent < 40 THEN '20-40%'
        WHEN discount_percent < 60 THEN '40-60%'
        ELSE '60%+'
      END AS band,
      rating,
      rating_total
      FROM trent.products
    ) bands
    GROUP BY band
    ORDER BY band
""")

    # 10) price buckets with avg discount
    q("price_bucket_distribution", """
        SELECT CASE
          WHEN price < 500 THEN '<500'
          WHEN price < 1000 THEN '500-999'
          WHEN price < 2000 THEN '1000-1999'
          WHEN price < 5000 THEN '2000-4999'
          ELSE '5000+'
        END AS price_bucket,
        COUNT(*) AS items,
        ROUND(AVG(discount_percent),2) AS avg_discount_pct
        FROM trent.products
        GROUP BY price_bucket
        ORDER BY items DESC
    """)

    # 11) image count vs rating
    q("image_count_vs_rating", """
        SELECT CASE
          WHEN img_count IS NULL OR img_count = 0 THEN '0'
          WHEN img_count <= 2 THEN '1-2'
          WHEN img_count <= 4 THEN '3-4'
          ELSE '5+'
        END AS img_bucket,
        COUNT(*) AS items,
        ROUND(AVG(NULLIF(rating,0)), 2) AS avg_rating_nonzero
        FROM trent.products
        GROUP BY img_bucket
        ORDER BY items DESC
    """)

    # 12) total markdown value (mrp - price)
    q("total_markdown_value", """
        SELECT ROUND(SUM(GREATEST(mrp - price, 0)), 2) AS total_markdown_value
        FROM trent.products
    """)

    # 13) duplicates by title (possible catalog hygiene)
    q("duplicate_titles_top50", """
        SELECT title, COUNT(*) AS dupes
        FROM trent.products
        GROUP BY title
        HAVING COUNT(*) > 1
        ORDER BY dupes DESC
        LIMIT 50
    """)

    # 14) data quality checks
    q("data_quality_nulls", """
        SELECT
          SUM(CASE WHEN brand IS NULL OR brand = '' THEN 1 ELSE 0 END) AS null_brands,
          SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS null_titles,
          SUM(CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END) AS bad_price
        FROM trent.products
    """)

    # 15) sample for scatter (price vs mrp)
    q("sample_price_vs_mrp", """
        SELECT product_id, brand, price, mrp, discount_percent
        FROM trent.products
        WHERE price IS NOT NULL AND mrp IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 1000
    """)

    print("\nAll analytics complete. CSVs are in ./analytics_out/")


if __name__ == "__main__":
    main()
