import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ecommerce_streaming")

if not MONGODB_URI:
    st.error("MONGODB_URI is not configured in .env")
    st.stop()


# --------------------------------------------------
# Page configuration
# --------------------------------------------------

st.set_page_config(
    page_title="E-Commerce Streaming Dashboard",
    page_icon="📊",
    layout="wide"
)
st_autorefresh(
    interval=30 * 1000,
    key="dashboard_refresh"
)
st.title("📊 E-Commerce Real-Time Streaming Dashboard")
st.caption(
    "Assignment 3 | Kafka → Consumer → MongoDB Atlas → Streamlit"
)


# --------------------------------------------------
# MongoDB connection
# --------------------------------------------------

@st.cache_resource
def get_collection():
    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=10000
    )
    client.admin.command("ping")
    db = client[MONGODB_DB]
    return db["orders"]


try:
    collection = get_collection()
except Exception as e:
    st.error(f"MongoDB connection failed: {e}")
    st.stop()


# --------------------------------------------------
# Load data
# --------------------------------------------------

@st.cache_data(ttl=30)
def load_data():
    documents = list(
        collection.find(
            {},
            {"_id": 0}
        )
    )

    if not documents:
        return pd.DataFrame()

    return pd.DataFrame(documents)


df = load_data()


if df.empty:
    st.warning("No order data found in MongoDB.")
    st.stop()


# --------------------------------------------------
# Data preparation
# --------------------------------------------------

df["Total_Amount"] = pd.to_numeric(
    df["Total_Amount"],
    errors="coerce"
)

df["Quantity"] = pd.to_numeric(
    df["Quantity"],
    errors="coerce"
)

df["Timestamp"] = pd.to_datetime(
    df["Timestamp"],
    errors="coerce"
)


# --------------------------------------------------
# KPI metrics
# --------------------------------------------------

total_orders = len(df)
total_revenue = df["Total_Amount"].sum()
average_order_value = df["Total_Amount"].mean()
successful_orders = (
    df["Payment_Status"]
    .astype(str)
    .str.lower()
    .eq("successful")
    .sum()
)


st.subheader("Key Business Metrics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Orders",
        f"{total_orders:,}"
    )

with col2:
    st.metric(
        "Total Order Value",
        f"₹{total_revenue:,.0f}"
    )

with col3:
    st.metric(
        "Average Order Value",
        f"₹{average_order_value:,.0f}"
    )

with col4:
    st.metric(
        "Successful Orders",
        f"{successful_orders:,}"
    )


st.divider()


# --------------------------------------------------
# Chart 1 — Revenue by Category
# --------------------------------------------------

st.subheader("1. Revenue by Category")

category_revenue = (
    df.groupby("Category", as_index=False)["Total_Amount"]
    .sum()
    .sort_values("Total_Amount", ascending=False)
)

fig_category = px.bar(
    category_revenue,
    x="Category",
    y="Total_Amount",
    text_auto=".2s",
    title="Total Order Value by Category"
)

fig_category.update_layout(
    xaxis_title="Category",
    yaxis_title="Order Value (₹)"
)

st.plotly_chart(
    fig_category,
    use_container_width=True
)


# --------------------------------------------------
# Chart 2 — Revenue by City
# --------------------------------------------------

st.subheader("2. Revenue by City")

city_revenue = (
    df.groupby("City", as_index=False)["Total_Amount"]
    .sum()
    .sort_values("Total_Amount", ascending=False)
)

fig_city = px.bar(
    city_revenue,
    x="City",
    y="Total_Amount",
    text_auto=".2s",
    title="Total Order Value by City"
)

fig_city.update_layout(
    xaxis_title="City",
    yaxis_title="Order Value (₹)"
)

st.plotly_chart(
    fig_city,
    use_container_width=True
)


# --------------------------------------------------
# Chart 3 — Payment Status Distribution
# --------------------------------------------------

st.subheader("3. Payment Status Distribution")

payment_status = (
    df["Payment_Status"]
    .value_counts()
    .reset_index()
)

payment_status.columns = [
    "Payment_Status",
    "Orders"
]

fig_payment = px.pie(
    payment_status,
    names="Payment_Status",
    values="Orders",
    hole=0.4,
    title="Orders by Payment Status"
)

st.plotly_chart(
    fig_payment,
    use_container_width=True
)


# --------------------------------------------------
# Chart 4 — Revenue Trend
# --------------------------------------------------

st.subheader("4. Order Value Trend")

if df["Timestamp"].notna().any():

    daily_revenue = (
        df.dropna(subset=["Timestamp"])
        .assign(Date=lambda x: x["Timestamp"].dt.date)
        .groupby("Date", as_index=False)["Total_Amount"]
        .sum()
    )

    fig_trend = px.line(
        daily_revenue,
        x="Date",
        y="Total_Amount",
        markers=True,
        title="Order Value Over Time"
    )

    fig_trend.update_layout(
        xaxis_title="Date",
        yaxis_title="Order Value (₹)"
    )

    st.plotly_chart(
        fig_trend,
        use_container_width=True
    )


# --------------------------------------------------
# Business insights
# --------------------------------------------------

st.divider()

st.subheader("Business Insights")

top_category = (
    category_revenue.iloc[0]["Category"]
    if not category_revenue.empty
    else "N/A"
)

top_city = (
    city_revenue.iloc[0]["City"]
    if not city_revenue.empty
    else "N/A"
)

top_payment_status = (
    payment_status.iloc[0]["Payment_Status"]
    if not payment_status.empty
    else "N/A"
)

st.markdown(
    f"""
- **Category concentration:** `{top_category}` has the highest total order value in the current dataset.
- **Geographic performance:** `{top_city}` contributes the highest total order value among the cities represented.
- **Payment behaviour:** `{top_payment_status}` is the most frequently observed payment status.
- **Management implication:** Category and city-level performance can be used to identify where inventory, promotions and sales efforts may deserve closer attention.
"""
)


# --------------------------------------------------
# Data table
# --------------------------------------------------

with st.expander("View Order Data"):

    st.dataframe(
        df.sort_values(
            "Timestamp",
            ascending=False
        ),
        use_container_width=True
    )


st.caption(
    "Data source: MongoDB Atlas collection populated by the Kafka streaming pipeline."
)
