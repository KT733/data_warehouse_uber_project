# Uber Ride Analytics – Modern Data Stack Project

- This project demonstrates a complete **end-to-end Modern Data Stack pipeline** using real Uber ride booking data.  
- The goal is to build a **production-grade analytics system** that ingests raw data, transforms it into analytics-ready models, and visualizes insights in an interactive dashboard.

---

## Project Objective

Build a fully operational data analytics pipeline using:

- **Fivetran** → Automated ingestion of CSV data from Google Drive  
- **Snowflake** → Cloud data warehouse storing raw + transformed tables  
- **dbt** → Transformations, models, tests, and analytics marts  
- **Streamlit** → Interactive dashboard for ride analytics  

This project simulates a real enterprise data workflow, applying:
- ELT modeling  
- Dimensional modeling (fact + dimensions)  
- Transformation logic  
- Data quality testing  
- Modern BI dashboard development  

---

## Final Dashboard (Streamlit)

**Features:**
- Total rides over time  
- Average ride distance trend  
- Most frequent pickup locations  
- Filters by date range  
- Raw ride data preview + CSV download  

---

## How to Run

Run streamlit:

```bash
pip install streamlit pandas plotly cryptography snowflake-connector-python
streamlit run app.py
```
Run dbt:

```bash
# setup
conda create -n dbt311 python=3.11
conda activate dbt311
pip install dbt-core dbt-snowflake
dbt init dw_project

#run
dbt debug       # test connection
dbt run         # build staging + marts
dbt test        # run data quality tests
dbt docs serve  # open documentation UI
```
