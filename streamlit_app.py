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
import plotly.express as px

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
        name='Avg Price', marker_color='#1f77b4'  # blue
    ))
    fig.add_trace(go.Bar(
        x=top_brands['brand'], y=top_brands['avg_discount'],
        name='Avg Discount %', marker_color='#ff7f0e', yaxis='y2'  # orange
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
st.image("logo.png", width=250)  # Adjust width as needed

st.set_page_config(page_title="Trent Agentic AI Demo", layout="wide")
st.title("Product Trends Agentic AI: Data → Deployment")

# ---------- Filters ----------

def sql_quote(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"

brands_df = q(f"SELECT DISTINCT brand FROM {SCHEMA_TABLE} ORDER BY 1")
brands = brands_df["brand"].dropna().tolist() if not brands_df.empty else []
c1, c2, c3 = st.columns([1, 2, 2])
min_disc = c1.slider("Min discount %", 0, 90, 0)
min_rating = c2.slider("Min rating", 0.0, 5.0, 0.0, 0.1)

where_clauses = ["1=1"]
brand_csv = ",".join(sql_quote(b) for b in [])
where_clauses.append(f"brand IN ({brand_csv})")
where_clauses.append(f"discount_percent >= {int(min_disc)}")
where_clauses.append(f"rating >= {min_rating}")

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

# ---------- Top discounted & rated ----------
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

col1, col2 = st.columns(2)
with col1:
    st.subheader("Top discounted (50)")
    st.dataframe(top_discounted, use_container_width=True, hide_index=True)
with col2:
    st.subheader("Top rated by volume (50)")
    st.dataframe(top_rated, use_container_width=True, hide_index=True)

st.caption("Powered by MCP (SELECT-only). No writes, no schema changes from the app.")

# ---------- Top Brands ----------
st.subheader("Top Brands (5)")
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
    t_brands = t_brands.sort_values(by='product_count', ascending=False)
    fig = go.Figure(data=[
        go.Bar(name='Product Count', x=t_brands['brand'], y=t_brands['product_count'], marker_color='#2ca02c'), # green
        go.Bar(name='Avg. MRP', x=t_brands['brand'], y=t_brands['mrp'], marker_color='#d62728') # red
    ])
    fig.update_layout(barmode='group', xaxis_title='Brand', yaxis_title='Value', legend_title='Metric', height=500)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data for top brands with the current filters.")
st.subheader("Brand: Avg Price vs Avg Discount")

brand_metrics_df = q(f"""
    SELECT 
        brand, 
        AVG(price) AS avg_price, 
        AVG(discount_percent) AS avg_discount_percent
    FROM {SCHEMA_TABLE}
    WHERE brand IS NOT NULL 
      AND price IS NOT NULL 
      AND discount_percent IS NOT NULL
    GROUP BY brand
""")

if not brand_metrics_df.empty:
    fig = px.bar(
        brand_metrics_df,
        x="brand",
        y=["avg_price", "avg_discount_percent"],
        barmode="group",
        title="Average Price vs Discount % by Brand",
        labels={"value": "Value", "brand": "Brand", "variable": "Metric"},
        color_discrete_sequence=px.colors.qualitative.Set2  # nice distinct colors
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        legend_title="Metric"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No brand metrics available.")

# ---------- Discount bands ----------
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

st.subheader("Discount bands")
if not bands.empty:
    fig = px.bar(
        bands, x="items", y="band", orientation="h",
        title="Items per Discount Band",
        color="band", color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig, use_container_width=True)
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
        fig1 = px.bar(
            price_buckets, x="price_bucket", y="items",
            color="price_bucket", color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No data for price buckets with the current filters.")
with c5:
    st.subheader("Avg discount by price bucket")
    if not price_buckets.empty:
        fig2 = px.bar(
            price_buckets, x="price_bucket", y="avg_discount_pct",
            color="price_bucket", color_discrete_sequence=px.colors.qualitative.Bold
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data for avg discount by bucket with the current filters.")

# =====================================================================
# Multi-pack insights viewer
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
        brands_inc = st.multiselect("Include Brands", options=all_brands)
        exclude_brands = st.multiselect("Exclude Brands", options=all_brands)
        title_ilike = st.text_input("Title contains (ILIKE)", "")
        top_limit = st.number_input("Top Rated Limit", 1, 100, 10)
    with col2:
        min_discount = st.slider("Min Discount %", 0, 100, 0)
        max_discount = st.slider("Max Discount %", 0, 100, 100)
        price_range = st.slider("Price Between", 0.0, 5000.0, (0.0, 5000.0))
        mrp_range = st.slider("MRP Between", 0.0, 5000.0, (0.0, 5000.0))
        
    
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
        "top_limit": top_limit
    }
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
    st.markdown("### Tables")
    tables = pack.get("tables", {})
    cols = st.columns(max(1, min(3, len(tables))))
    for i, (name, rows) in enumerate(tables.items()):
        with cols[i % len(cols)]:
            st.markdown(f"**{name.replace('_', ' ').title()}**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
