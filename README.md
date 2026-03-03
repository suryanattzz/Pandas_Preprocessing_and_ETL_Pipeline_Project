# Pandas Preprocessing & ETL Pipeline Project

## Project Overview

This project processes **two data domains** using Pandas and modular ETL pipelines:

1. **Sales + Customer data**
2. **Orders + Products data**

Each domain has:
- preprocessing notebooks for analysis/cleaning
- an ETL script with `extract()`, `transform()`, `load()`, `main()`

---

## Data Domains

### 1) Sales & Customer
- Raw input: `data/raw/sales_data.csv`, `data/raw/customer_data.json`
- Processed files: `data/processed/processed_sales_data.csv`, `data/processed/processed_customer_data.csv`
- ETL output reports: `output/sales_report.csv`, `output/customer_report.csv`

### 2) Orders & Products
- Raw input: `data/raw/orders.csv`, `data/raw/products.json`
- Processed file: `data/processed/processed_orders_data.csv`
- ETL output reports: `output/orders_report.csv`, `output/products_report.csv`

---

## ETL Pipelines

### Sales & Customer ETL
- Script: `etl/etl_pipeline.py`
- Run from project root:

```bash
python etl/etl_pipeline.py
```

### Orders & Products ETL
- Script: `etl/etl_orders_products_pipeline.py`
- Run from project root:

```bash
python etl/etl_orders_products_pipeline.py
```

---

## Notebooks

### Sales & Customer notebooks
- `notebooks/sales_data_preprocessing.ipynb`
- `notebooks/customer_data_preprocessing.ipynb`
- `notebooks/bussiness_reports.ipynb`

### Orders & Products notebooks
- `notebooks/orders_data_preprocessing.ipynb`
- `notebooks/bussiness_reports_orders_product.ipynb`
- `notebooks/report_orders_products.ipynb`

---

## Folder Structure

```
Pandas - ZipMart Sales Intelligence Pipeline/
├── data/
│   ├── raw/
│   │   ├── customer_data.json
│   │   ├── orders.csv
│   │   ├── products.json
│   │   └── sales_data.csv
│   └── processed/
│       ├── processed_customer_data.csv
│       ├── processed_orders_data.csv
│       └── processed_sales_data.csv
├── etl/
│   ├── etl_pipeline.py
│   └── etl_orders_products_pipeline.py
├── notebooks/
├── output/
├── .gitignore
└── README.md
```

---

## Key Pandas Operations Used

- Null handling: `fillna`, `isnull`
- Date conversion: `pd.to_datetime(..., format='mixed')`
- Duplicate handling: `duplicated`, `drop_duplicates`
- String cleanup: `str.title`, regex-based replacement
- Aggregation/reporting: `groupby`, `sum`, `mean`, ranking/index methods

---

## Notes

- All paths in ETL scripts and notebooks are aligned to the new folder layout.
- Keep generated report files in `output/` and processed intermediate files in `data/processed/`.
