# 🛒 End-to-End E-Commerce Data Pipeline with dbt & PostgreSQL

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![dbt](https://img.shields.io/badge/dbt-1.10-orange)
![pandas](https://img.shields.io/badge/pandas-2.1-green)

> Complete data analytics pipeline: ETL with Python pandas, PostgreSQL star schema warehouse, dbt transformation layer, automated testing

**Data:** 100K+ Brazilian e-commerce transactions | **Period:** 2016-2018 | **Scale:** 241K+ warehouse rows

---

## 📋 Project Overview

Built a production-grade data pipeline that extracts, cleans, transforms, and analyzes real e-commerce data. Demonstrates end-to-end data engineering from raw CSVs to business-ready analytics using modern data stack tools.

### What This Project Does

- Cleans messy real-world data with pandas (handling 4,908 missing values, duplicates, type errors)
- Designs and implements PostgreSQL star schema (3 dimensions + 1 fact table)
- Builds dbt transformation layer with 6 models (staging → marts)
- Calculates business metrics (Customer LTV, product performance, revenue growth)
- Validates data quality with 21 automated tests
- Generates interactive documentation with lineage visualization

---

## 🏗️ Architecture
```
Raw CSV Files (9 files, 100K orders)
         ↓
Python pandas ETL Pipeline
    • Handle missing values
    • Convert data types
    • Remove duplicates
    • Standardize formats
         ↓
PostgreSQL Star Schema (241K rows)
    • dim_customers (99K)
    • dim_products (32K)
    • dim_date (1K)
    • fact_orders (108K)
         ↓
dbt Transformation Layer
    • Staging models (clean sources)
    • Mart models (business metrics)
    • 21 automated tests
         ↓
Analytics-Ready Tables
    • Customer lifetime value
    • Product performance
    • Revenue trends
```

---

## 🛠️ Tech Stack

| Category | Tools |
|----------|-------|
| **Languages** | Python 3.12, SQL |
| **Data Processing** | pandas 2.1, NumPy |
| **Database** | PostgreSQL 15 |
| **Transformations** | dbt 1.10 (Data Build Tool) |
| **Connectivity** | SQLAlchemy, psycopg2 |
| **Development** | Git/GitHub, Jupyter, VS Code |

---

## 📊 Data Pipeline Phases

### Phase 1: Data Extraction
- Source: Kaggle - Brazilian E-Commerce Public Dataset (Olist)
- 9 CSV files: customers, orders, products, payments, reviews
- 100K orders, 99K customers, 33K products

### Phase 2: Data Cleaning (pandas)
**Script:** `scripts/clean_data.py`

Cleaning operations:
- Handled 4,908 missing values (drop if <5%, fill with median/Unknown)
- Converted 5 date columns from text to datetime
- Removed duplicates based on ID columns
- Standardized text (lowercase, trimmed whitespace)
- Filtered out negative values and invalid records
- Removed 5,000+ invalid rows

Key pandas functions: `read_csv()`, `dropna()`, `fillna()`, `to_datetime()`, `drop_duplicates()`, `str.lower()`, `str.strip()`, `to_csv()`

### Phase 3: Data Warehouse (PostgreSQL)
**Schema:** `sql/schema.sql`

Star schema with dimensional modeling:

**Dimensions:**
- `dim_customers` (99,441 rows) - Customer location data
- `dim_products` (32,340 rows) - Product attributes
- `dim_date` (1,096 rows) - Calendar table

**Fact:**
- `fact_orders` (108,643 rows) - Transaction grain

Features: Foreign keys, indexes, surrogate keys (SERIAL)

**Loading:** `scripts/load_to_db.py` using pandas `.to_sql()`

### Phase 4: Transformations (dbt)
**Project:** `ecommerce_dbt/`

**Staging models** (clean and standardize):
- `stg_customers` - Add location flags, clean formatting
- `stg_products` - Calculate volume, categorize by weight/size
- `stg_orders` - Calculate delivery days, on-time status

**Mart models** (business analytics):
- `fct_customer_metrics` - Customer LTV, order frequency, segmentation
- `fct_product_performance` - Sales volume, revenue, performance tiers
- `fct_monthly_revenue` - Time-series with MoM growth (LAG function)

**Testing:** 21 automated tests for data quality

**Documentation:** Interactive site with lineage graph

---

## 💼 Business Metrics

### Customer Analytics
- Customer Lifetime Value (total revenue per customer)
- Segmentation: One-time / Occasional / Frequent buyers
- On-time delivery percentage
- Customer tenure and repeat purchase patterns

### Product Analytics
- Sales volume (times ordered)
- Performance categories: Best Seller → Never Sold
- Weight/size categorization for logistics
- Average delivery time by product

### Revenue Analytics
- Monthly revenue trends (24 months)
- Month-over-month growth rates
- Seasonal patterns
- Active customer counts per period

---

## 🚀 Quick Start

### Installation
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/ecommerce-sales-pipeline.git
cd ecommerce-sales-pipeline

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Download dataset from Kaggle and place in data/raw/

# Create PostgreSQL database
psql postgres -c "CREATE DATABASE ecommerce_dw;"
psql -d ecommerce_dw -f sql/schema.sql

# Install dbt packages
cd ecommerce_dbt
dbt deps
```

### Run Pipeline
```bash
# Step 1: Clean data
cd scripts
python3 clean_data.py

# Step 2: Load to warehouse
python3 load_to_db.py

# Step 3: Run dbt models
cd ../ecommerce_dbt
dbt run

# Step 4: Test data quality
dbt test

# Step 5: View documentation
dbt docs generate
dbt docs serve
```

---

## 📂 Project Structure
```
ecommerce-sales-pipeline/
├── data/
│   ├── raw/                    # Original CSV files
│   └── cleaned/                # Cleaned CSV files
├── scripts/
│   ├── clean_data.py          # pandas cleaning pipeline
│   └── load_to_db.py          # PostgreSQL loader
├── sql/
│   └── schema.sql             # Star schema DDL
├── ecommerce_dbt/             # dbt project
│   ├── models/
│   │   ├── staging/           # stg_* models
│   │   └── marts/core/        # fct_* models
│   ├── tests/                 # Custom tests
│   └── packages.yml           # dbt dependencies
├── notebooks/
│   └── data_exploration.ipynb # Data analysis
├── requirements.txt           # Python dependencies
└── README.md
```

---

## 🎯 Skills Demonstrated

**Data Engineering:**
- ETL/ELT pipeline development
- Star schema design
- Data warehouse implementation
- Data quality frameworks
- Pipeline automation

**Python:**
- pandas data manipulation (cleaning, transformations)
- Database connectivity (SQLAlchemy)
- Error handling
- Modular code organization

**SQL:**
- Complex queries (JOINs, subqueries, CTEs)
- Window functions (LAG, RANK, ROW_NUMBER)
- Aggregate functions
- Date/time arithmetic
- Performance optimization

**dbt:**
- Model development (staging → marts)
- Dependency management ({{ ref() }}, {{ source() }})
- Automated testing
- Documentation generation
- Modern analytics engineering

**Tools:**
- Git/GitHub version control
- PostgreSQL database administration
- Jupyter notebooks for exploration
- Virtual environment management

---

## 📈 Results

### Data Processing
- Cleaned 100K+ raw records
- Loaded 241K+ rows to warehouse
- Generated 340K+ transformed rows via dbt
- Pipeline execution: <2 minutes end-to-end

### Data Quality
- 21 automated tests: 100% passing
- Foreign key integrity maintained
- No null values in required fields
- Valid value ranges enforced

### Business Insights
- 42% of customers from São Paulo state
- 75% are one-time buyers (retention opportunity)
- Best sellers: 156 products with 100+ orders
- 40% of catalog never sold (inventory optimization needed)

---

## 🔮 Future Enhancements

- Apache Airflow orchestration for scheduled runs
- Tableau dashboard for visual analytics
- Incremental dbt models for production efficiency
- AWS cloud deployment (S3, RDS, Lambda)
- Machine learning models (churn prediction, recommendations)
- Real-time streaming pipeline

---
