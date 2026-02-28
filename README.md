# Pandas Preprocessing & ETL Pipeline Project

**Customer Transaction and Sales - Data Cleaning & Automated ETL**

---

## 📋 Project Overview

This project demonstrates **Pandas data preprocessing** and **ETL pipeline creation** using real-world sales and customer data.

**Approach:**
- **Approach 1:** Interactive Jupyter notebooks for data exploration & cleaning
- **Approach 2:** Production Python script with modular ETL architecture

---

## 📊 Data & Cleaning Tasks

### Sales Data (`sales_data.csv` - 2,062 rows)
- Contains missing values
- Mixed date formats (YYYY-MM-DD, DD/MM/YYYY)

### Customer Data (`customer_data.json` - 2,002 rows)
- Contact validation: email & phone (regex)
- Missing values
- Mixed date formats

---

## 🔍 Approach 1: Interactive Notebooks

Three notebooks for exploratory analysis:

1. **sales_data_preprocessing.ipynb**
   - `.fillna()` - Handle nulls
   - `pd.to_datetime(format='mixed')` - Date conversion
   - `.duplicated()` / `.drop_duplicates()` - Duplicate removal
   - `.str.title()` - String formatting
   - `np.isclose()` - Validation

2. **customer_data_preprocessing.ipynb**
   - Regex validation (email, phone)
   - Date parsing for multiple columns
   - Currency symbol removal with regex
   - `.astype()` - Type conversion

3. **bussiness_reports.ipynb**
   - `.groupby()` aggregations
   - `.sum()`, `.mean()`, `.idxmax()`
   - Visualizations (matplotlib/plotly)

**Output:** `processed_sales_data.csv`, `processed_customer_data.csv`

---

## ⚙️ Approach 2: Production ETL Pipeline

**Script:** `etl_pipeline.py`

**Architecture** (4 Functions):

```python
extract()        # Load CSV & JSON files + error handling
transform()      # Apply all preprocessing logic
load()           # Save to output/ folder
main()           # Orchestrate pipeline
```

**Features:**
- ✅ Modular design
- ✅ Error handling (try/except)
- ✅ Progress logging (print statements)
- ✅ Data validation at each stage
- ✅ No hardcoded values

**Run:**
```bash
python etl_pipeline.py
```

**Output:**
```
output/
├── sales_report.csv (2,002 clean rows)
└── customer_report.csv (2,002 clean rows)
```

---

## 📁 Project Structure

```
Pandas - ZipMart Sales Intelligence Pipeline/
├── sales_data.csv
├── customer_data.json
│
├── sales_data_preprocessing.ipynb
├── customer_data_preprocessing.ipynb
├── bussiness_reports.ipynb
│
├── etl_pipeline.py          # Production ETL
├── output/                  # Generated reports
├── .gitignore
└── README.md
```

---

## 🎓 Pandas Concepts Covered

| Concept | Methods Used |
|---------|--------------|
| **Null Handling** | `.fillna()`, `.isnull()`, `.dropna()` |
| **Date Processing** | `pd.to_datetime()`, `.dt` accessors |
| **String Operations** | `.str.title()`, `.str.match()`, regex |
| **Duplicates** | `.duplicated()`, `.drop_duplicates()` |
| **Aggregations** | `.groupby()`, `.sum()`, `.mean()`, `.idxmax()` |
| **Validation** | `np.isclose()`, regex patterns |
| **Data Export** | `.to_csv()`, `.to_json()` |

---

## 📊 Cleaning Results

| Metric | Before | After |
|--------|--------|-------|
| Sales Rows | 2,062 | 2,002 |
| Null Values | 120+ | 0 |
| Duplicates | 60 | 0 |
| Date Format | Mixed | ISO 8601 |
