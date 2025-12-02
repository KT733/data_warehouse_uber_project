import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.connector import connect
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="Uber Ride Analytics Dashboard",
    layout="wide",
    page_icon="🚕"
)

# -----------------------------
# LOAD PRIVATE KEY
# -----------------------------
with open("/Users/kattygeng/Desktop/airflow_proj/include/rsa_key.pem", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

# -----------------------------
# SNOWFLAKE CONNECTION
# -----------------------------
@st.cache_resource
def get_snowflake_connection():
    return connect(
        user="CHEETAH",
        account="azb79167",
        private_key=private_key,
        warehouse="TRAINING_WH",
        database="FIVETRAN_DATABASE",
        schema="GOOGLE_DRIVE"
    )

# -----------------------------
# QUERY HELPERS
# -----------------------------
@st.cache_data(ttl=1800)
def load_fact_rides():
    conn = get_snowflake_connection()
    query = "SELECT * FROM FACT_RIDES ORDER BY RIDE_TS DESC"
    return pd.read_sql(query, conn)

@st.cache_data(ttl=1800)
def load_daily_agg():
    conn = get_snowflake_connection()
    return pd.read_sql("SELECT * FROM AGG_DAILY_RIDES ORDER BY RIDE_DATE", conn)

@st.cache_data(ttl=1800)
def load_location_summary():
    conn = get_snowflake_connection()
    return pd.read_sql("SELECT * FROM AGG_LOCATION_SUMMARY ORDER BY TOTAL_PICKUPS DESC", conn)

# -----------------------------
# APP HEADER
# -----------------------------
st.title("🚕 Uber Ride Analytics Dashboard")
st.markdown("Explore ride trends, popular locations, and KPIs — powered by **Fivetran + dbt + Snowflake + Streamlit**.")

# -----------------------------
# LOAD ALL DATA
# -----------------------------
with st.spinner("Loading Snowflake data..."):
    fact = load_fact_rides()
    daily = load_daily_agg()
    loc_summary = load_location_summary()

# -----------------------------
# SIDEBAR FILTERS
# -----------------------------
st.sidebar.header("Filters")

# Date filter
min_date = fact["RIDE_DATE"].min()
max_date = fact["RIDE_DATE"].max()

selected_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Convert RIDE_DATE to date (not timestamp)
fact["RIDE_DATE"] = pd.to_datetime(fact["RIDE_DATE"]).dt.date

# Selected range is already (date, date)
start_date, end_date = selected_range

# Filter
fact_filtered = fact[
    (fact["RIDE_DATE"] >= start_date) &
    (fact["RIDE_DATE"] <= end_date)
]


# -----------------------------
# KPI METRICS
# -----------------------------
st.markdown("### 📈 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Rides", f"{len(fact_filtered):,}")

with col2:
    avg_dist = fact_filtered["RIDE_DISTANCE_KM"].mean()
    st.metric("Avg Distance (km)", f"{avg_dist:.2f}")

with col3:
    completed = (fact_filtered["BOOKING_STATUS"] == "Completed").mean() * 100
    st.metric("Completion Rate", f"{completed:.1f}%")

st.markdown("---")

# -----------------------------
# TABS
# -----------------------------
tab1, tab2, tab3 = st.tabs(["📊 Visualizations", "📍 Locations", "📋 Raw Data"])

# =============================
# TAB 1 — Visualizations
# =============================
with tab1:
    st.subheader("Daily Trends")

    # Total rides per day
    fig = px.line(
        daily,
        x="RIDE_DATE",
        y="TOTAL_RIDES",
        title="Total Rides per Day",
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

    # Only one chart below
    fig_dist = px.line(
        daily,
        x="RIDE_DATE",
        y="AVG_DISTANCE",
        title="Average Ride Distance Over Time",
        markers=True
    )
    st.plotly_chart(fig_dist, use_container_width=True)


# =============================
# TAB 2 — Locations
# =============================
with tab2:
    st.subheader("Top Pickup Locations")

    st.dataframe(loc_summary.head(20), use_container_width=True)

    fig_loc = px.bar(
        loc_summary.head(20),
        x="TOTAL_PICKUPS",
        y="PICKUP_LOCATION",
        orientation="h",
        title="Most Frequent Pickup Locations",
        color="TOTAL_PICKUPS",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_loc, use_container_width=True)

# =============================
# TAB 3 — Raw Data
# =============================
with tab3:
    st.subheader("Ride-Level Data")
    st.dataframe(fact_filtered, use_container_width=True)

    csv = fact_filtered.to_csv(index=False)
    st.download_button(
        "📥 Download Filtered Data",
        csv,
        "uber_rides_filtered.csv",
        "text/csv"
    )

# -----------------------------
# SIDEBAR INFO
# -----------------------------
st.sidebar.markdown("---")
st.sidebar.info("Modern Data Stack + Streamlit = 🚀")
st.sidebar.markdown("1. Fivetran → Raw ingestion")
st.sidebar.markdown("2. dbt → Clean transformations")
st.sidebar.markdown("3. Snowflake → Data warehouse")
st.sidebar.markdown("4. Streamlit → Analytics dashboard")

