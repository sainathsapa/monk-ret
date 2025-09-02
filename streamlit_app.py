import json
from functools import lru_cache
from pathlib import Path
import sys
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# MCP import (SELECT-only)
from mcp_monkdb.mcp_server import run_select_query

SCHEMA_TABLE = "trent.products"  # adjust if needed

# ---------- Query helper (SELECT via MCP) ----------

load_dotenv()

import plotly.graph_objects as go

def create_brand_price_discount_chart(df):
    """
    Create a bar chart showing brand vs average price and discount.
    Returns a Plotly Figure.
    """
    if 'brand' not in df.columns:
        return None
    # Aggregate by brand
    agg = df.groupby('brand').agg(
        avg_price=('price', 'mean'),
        avg_discount=('discount_percent', 'mean'),
        count=('price','count')
    ).reset_index()
    top_brands = agg.sort_values(by='count', ascending=False).head(10)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top_brands['brand'], y=top_brands['avg_price'],
        name='Avg Price', marker_color='steelblue'
    ))
    fig.add_trace(go.Bar(
        x=top_brands['brand'], y=top_brands['avg_discount'],
        name='Avg Discount %', marker_color='salmon', yaxis='y2'
    ))
    fig.update_layout(
        title="Top 10 Brands: Average Price and Average Discount",
        xaxis_tickangle=-45,
        yaxis=dict(title="Average Price (₹)"),
        yaxis2=dict(title="Average Discount (%)", overlaying='y', side='right'),
        legend=dict(x=0.5, y=1.1, orientation='h')
    )
    return fig

@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    res = run_select_query(sql)
    if isinstance(res, dict) and res.get("status") == "error":
        raise RuntimeError(res["message"])
    return pd.DataFrame(res or [])


# ---------- App ----------
st.set_page_config(page_title="Trent Agentic AI Demo", layout="wide")
st.title("Trends Agentic AI: Data → Deployment")

# ---------- Filters ----------


def sql_quote(val: str) -> str:
    # wrap in single quotes and escape internal single quotes by doubling
    return "'" + val.replace("'", "''") + "'"


brands_df = q(f"SELECT DISTINCT brand FROM {SCHEMA_TABLE} ORDER BY 1")
brands = brands_df["brand"].dropna().tolist() if not brands_df.empty else []
c1, c2, c3 = st.columns([1, 2, 2])  # Adjust ratios as needed
# brand_sel = c1.multiselect(
#     "Brand(s)", brands, default=brands[:3] if len(brands) >= 3 else brands)
min_disc = c1.slider("Min discount %", 0, 90, 0)
min_rating = c2.slider("Min rating", 0.0, 5.0, 0.0, 0.1)

where_clauses = ["1=1"]
# if brand_sel:
brand_csv = ",".join(sql_quote(b) for b in [])
where_clauses.append(f"brand IN ({brand_csv})")
# if min_disc > 0:
where_clauses.append(f"discount_percent >= {int(min_disc)}")
# if min_rating > 0:
where_clauses.append(f"rating >= {min_rating}")

# where_sql = " AND ".join(where_clauses)

# ---------- KPIs ----------
kpis_df = q(f"""
    SELECT
      COUNT(*) AS products,
      ROUND(AVG(price),2) AS avg_price,
      ROUND(AVG(mrp),2) AS avg_mrp,
      ROUND(AVG(discount_percent),2) AS avg_discount_pct,
      SUM(CASE WHEN price = mrp THEN 1 ELSE 0 END) AS no_discount_items
    FROM {SCHEMA_TABLE}
    
""")
# WHERE {where_sql}
kpis = kpis_df.iloc[0] if not kpis_df.empty else pd.Series(
    {"products": 0, "avg_price": 0, "avg_mrp": 0,
        "avg_discount_pct": 0, "no_discount_items": 0}
)


k1, k2, k3, k4, k5 = st.columns(5)

with k1.container(border=True):
    st.metric("Products", int(kpis["products"]))

with k2.container(border=True):
    st.metric("Avg Price", kpis["avg_price"])

with k3.container(border=True):
    st.metric("Avg MRP", kpis["avg_mrp"])

with k4.container(border=True):
    st.metric("Avg Discount %", kpis["avg_discount_pct"])

with k5.container(border=True):
    st.metric("No-discount Items", int(kpis["no_discount_items"]))
    
    
st.subheader("Top Brands")

t_brands = q(f"""
    SELECT brand,
           COUNT(*) AS product_count,
           AVG(mrp) AS mrp
    FROM {SCHEMA_TABLE}
    GROUP BY brand
    ORDER BY product_count DESC
    LIMIT 5;
""")

if not t_brands.empty:
    # Sort to ensure consistent ordering
    t_brands = t_brands.sort_values(by='product_count', ascending=False)

    fig = go.Figure(data=[
        go.Bar(
            name='Product Count',
            x=t_brands['brand'],
            y=t_brands['product_count'],
            marker_color='steelblue'
        ),
        go.Bar(
            name='Avg. MRP',
            x=t_brands['brand'],
            y=t_brands['mrp'],
            marker_color='orange'
        )
    ])

    # Update layout for grouped bars
    fig.update_layout(
        barmode='group',
        xaxis_title='Brand',
        yaxis_title='Value',
        title='Top 5 Brands: Product Count & Avg Discount',
        legend_title='Metric',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data for top brands with the current filters.")

# ---------- Discount bands (SELECT-wrapped, no WITH) ----------
bands = q(f"""
    SELECT band, COUNT(*) AS items
    FROM (
      SELECT CASE
        WHEN discount_percent = 0 THEN '0%'
        WHEN discount_percent < 20 THEN '0-20%'
        WHEN discount_percent < 40 THEN '20-40%'
        WHEN discount_percent < 60 THEN '40-60%'
        ELSE '60%+'
      END AS band
      FROM {SCHEMA_TABLE}
      
    ) b
    GROUP BY band
    ORDER BY items DESC
""")


print(bands.keys())
# WHERE {where_sql}
st.subheader("Discount bands")
if not bands.empty:
    bands_indexed = bands.set_index("band")
    st.bar_chart(bands_indexed['items'])
    # st.bar_chart(bands.set_index("band")['items'])
else:
    st.info("No data for discount bands with the current filters.")

# ---------- Price bucket distribution ----------
price_buckets = q(f"""
    SELECT CASE
      WHEN price < 500 THEN '<500'
      WHEN price < 1000 THEN '500-999'
      WHEN price < 2000 THEN '1000-1999'
      WHEN price < 5000 THEN '2000-4999'
      ELSE '5000+'
    END AS price_bucket,
    COUNT(*) AS items,
    ROUND(AVG(discount_percent),2) AS avg_discount_pct
    FROM {SCHEMA_TABLE}
    
    GROUP BY price_bucket
        ORDER BY items DESC
""")
c4, c5 = st.columns(2)
with c4:
    st.subheader("Price buckets")
    if not price_buckets.empty:
        st.bar_chart(price_buckets.set_index("price_bucket")["items"])
    else:
        st.info("No data for price buckets with the current filters.")
with c5:
    st.subheader("Avg discount by price bucket")
    if not price_buckets.empty:
        st.bar_chart(price_buckets.set_index(
            "price_bucket")["avg_discount_pct"])
    else:
        st.info("No data for avg discount by bucket with the current filters.")

# ---------- Top discounted & Top rated (SELECT-only) ----------
top_discounted = q(f"""
    SELECT product_id, title, brand, price, mrp, discount_percent, rating, rating_total
    FROM {SCHEMA_TABLE}
    
    ORDER BY discount_percent DESC, price ASC
    LIMIT 50
""")
top_rated = q(f"""
    SELECT product_id, title, brand, rating, rating_total, price, mrp, discount_percent
    FROM {SCHEMA_TABLE}
    WHERE rating_total >= 100 AND rating >= 4
    ORDER BY rating DESC, rating_total DESC
    LIMIT 50
""")



st.subheader("Top discounted (50)")
st.dataframe(top_discounted, use_container_width=True, hide_index=True)
st.subheader("Top rated by volume (50)")
st.dataframe(top_rated, use_container_width=True, hide_index=True)

st.caption(
    "Powered by MCP (SELECT-only). No writes, no schema changes from the app.")

# =====================================================================
# Multi-pack insights viewer (reads files generated by gen_insights.py)
# =====================================================================
PACKS_DIR = Path("analytics_out/packs")
PACKS_DIR.mkdir(parents=True, exist_ok=True)

st.divider()
st.subheader("Insights Packs")



from gen_insights_force import main as run_insights

st.title("Dynamic Insight Packs")

with st.form("filters_form"):
    st.subheader("Filter products")

    all_brands = brands_df["brand"].dropna().unique().tolist() if not brands_df.empty else []

    col1, col2 = st.columns(2)
    with col1:
        brands_inc = st.multiselect("Include Brands", options=all_brands, help="Select brands to include")
        exclude_brands = st.multiselect("Exclude Brands", options=all_brands, help="Select brands to exclude")
        title_ilike = st.text_input("Title contains (ILIKE)", "")

    with col2:
        min_discount = st.slider("Min Discount %", 0, 100, 0)
        max_discount = st.slider("Max Discount %", 0, 100, 100)
        price_range = st.slider("Price Between", 0.0, 5000.0, (0.0, 5000.0))
        mrp_range = st.slider("MRP Between", 0.0, 5000.0, (0.0, 5000.0))
        img_range = st.slider("Image Count Between", 0, 10, (0, 10))

    only_discounted = st.checkbox("Only Discounted Items")
    only_no_discount = st.checkbox("Only Non-Discounted Items (at MRP)")

    include_rating = st.checkbox("Include Rating Filters")
    if include_rating:
        with st.expander("Rating Filters", expanded=True):
            min_rating = st.slider("Min Rating", 0.0, 5.0, 0.0, 0.1)
            max_rating = st.slider("Max Rating", 0.0, 5.0, 5.0, 0.1)
            min_reviews = st.number_input("Min Reviews", 0, 100000, 0)
    else:
        min_rating = max_rating = min_reviews = None

    top_limit = st.number_input("Top Rated Limit", 1, 100, 10)

    submitted = st.form_submit_button("Run Insights")

if submitted:
    filters = {
        "brands": brands_inc,
        "exclude_brands": exclude_brands,
        "title_ilike": title_ilike.strip() or None,
        "min_discount": min_discount,
        "max_discount": max_discount,
        "price_between": list(price_range),
        "mrp_between": list(mrp_range),
        "img_count_between": list(img_range),
        "only_discounted": only_discounted,
        "only_no_discount": only_no_discount,
        "top_limit": top_limit
    }

    if include_rating:
        filters.update({
            "min_rating": min_rating,
            "max_rating": max_rating,
            "min_reviews": min_reviews,
        })

    filters = {k: v for k, v in filters.items() if v not in [None, [], ""]}

    try:
        sys.argv = ["gen_insights_force.py", "--filters-json", json.dumps(filters)]
        pack = run_insights()
    except Exception as e:
        st.error(f"Failed to generate insights: {e}")
        st.stop()

    st.success("Pack generated successfully!")

    st.markdown("### Summary Bullets")
    for b in pack.get("bullets", []):
        st.write(f"• {b}")

    # st.markdown("### KPIs")
    # st.json(pack.get("kpis", {}))

    st.markdown("### Tables")
    tables = pack.get("tables", {})
    cols = st.columns(max(1, min(3, len(tables))))
    for i, (name, rows) in enumerate(tables.items()):
        with cols[i % len(cols)]:
            st.markdown(f"**{name.replace('_', ' ').title()}**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True)