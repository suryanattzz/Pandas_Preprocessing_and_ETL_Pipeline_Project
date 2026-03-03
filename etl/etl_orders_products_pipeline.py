import pandas as pd
import numpy as np
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')


def extract():
    print("\n" + "="*60)
    print("EXTRACT PHASE - Reading Orders & Products Data")
    print("="*60)
    
    orders_file = "../data/raw/orders.csv"
    products_file = "../data/raw/products.json"
    
    try:
        print(f"\n📂 Loading {orders_file}...")
        df_orders = pd.read_csv(orders_file)
        print(f"✓ Orders data loaded successfully: {len(df_orders):,} rows")
        
        print(f"\n📂 Loading {products_file}...")
        df_products = pd.read_json(products_file)
        print(f"✓ Products data loaded successfully: {len(df_products):,} rows")
        
        return df_orders, df_products
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Required file not found!")
        print(f"   {str(e)}")
        print("\n   Please ensure both files are in the project directory:")
        print(f"   - {orders_file}")
        print(f"   - {products_file}")
        print("\n   Pipeline aborted. Fix the issue and run again.")
        exit(1)
        
    except Exception as e:
        print(f"\n❌ ERROR: Unexpected error while reading files!")
        print(f"   {str(e)}")
        print("\n   Pipeline aborted.")
        exit(1)


def transform(df_orders, df_products):
    
    # ==================== ORDERS DATA CLEANING ====================
    print("\n🔧 Cleaning Orders Data...")
    
    # 1. Strip and standardize order_id
    print("   → Standardizing order_id format")
    df_orders["order_id"] = df_orders["order_id"].astype("string").str.strip()
    
    # 2. Remove duplicate orders based on order_id (keep first occurrence)
    duplicates_before = df_orders["order_id"].duplicated().sum()
    df_orders = df_orders.drop_duplicates(subset=["order_id"], keep="first").copy()
    print(f"   → Removed {duplicates_before} duplicate order records")
    
    # 3. Standardize and format order_date
    print("   → Standardizing order_date format to YYYY-MM-DD")
    df_orders["order_date"] = pd.to_datetime(
        df_orders["order_date"], 
        errors="coerce", 
        dayfirst=True,
        format="mixed"
    )
    df_orders["order_date"] = df_orders["order_date"].dt.strftime("%Y-%m-%d")
    
    # 4. Standardize text fields (customer_city, payment_mode)
    print("   → Applying title case formatting to customer_city and payment_mode")
    for col in ["customer_city", "payment_mode"]:
        df_orders[col] = (
            df_orders[col]
            .astype("string")
            .str.strip()
            .str.replace("_", " ", regex=False)
            .str.title()
        )
    
    # 5. Fill null values with "Unknown" in key columns
    print("   → Filling null values with 'Unknown'")
    null_cols = ["order_date", "customer_city", "payment_mode"]
    for col in null_cols:
        df_orders[col] = df_orders[col].fillna("Unknown").replace({"": "Unknown", "Nan": "Unknown"})
    
    # 6. Ensure product_id is uppercase and stripped
    print("   → Standardizing product_id format")
    df_orders["product_id"] = df_orders["product_id"].astype("string").str.strip().str.upper()
    
    # 7. Convert quantity to numeric
    print("   → Converting quantity to numeric format")
    df_orders["quantity"] = pd.to_numeric(df_orders["quantity"], errors="coerce")
    df_orders["quantity"] = df_orders["quantity"].fillna(0).astype(int)
    
    print(f"✓ Orders data cleaned: {len(df_orders):,} rows remaining")
    
    
    # ==================== PRODUCTS DATA CLEANING ====================
    print("\n🔧 Cleaning Products Data...")
    
    # 1. Standardize product_id
    print("   → Standardizing product_id format")
    df_products["product_id"] = df_products["product_id"].astype("string").str.strip().str.upper()
    
    # 2. Remove duplicate products
    duplicates_before = df_products["product_id"].duplicated().sum()
    df_products = df_products.drop_duplicates(
        subset=["product_id"], 
        keep="first"
    ).reset_index(drop=True)
    print(f"   → Removed {duplicates_before} duplicate product records")
    
    # 3. Standardize product_name and category
    print("   → Applying title case formatting to product names and categories")
    df_products["product_name"] = df_products["product_name"].str.title()
    df_products["category"] = df_products["category"].str.title()
    
    # 4. Convert unit_price to numeric
    print("   → Converting unit_price to numeric format")
    df_products["unit_price"] = pd.to_numeric(df_products["unit_price"], errors="coerce")
    df_products["unit_price"] = df_products["unit_price"].fillna(0)
    
    print(f"✓ Products data cleaned: {len(df_products):,} rows remaining")
    
    return df_orders, df_products


def load(df_orders, df_products):
    # Create output folder if it doesn't exist
    output_folder = Path("../output")
    output_folder.mkdir(exist_ok=True)
    
    # Define output file paths
    orders_output = output_folder / "orders_report.csv"
    products_output = output_folder / "products_report.csv"
    
    try:
        # Save orders data
        print(f"\n💾 Saving orders report to: {orders_output}")
        df_orders.to_csv(orders_output, index=False)
        print(f"✓ Orders report saved successfully ({len(df_orders):,} rows)")
        
        # Save products data
        print(f"\n💾 Saving products report to: {products_output}")
        df_products.to_csv(products_output, index=False)
        print(f"✓ Products report saved successfully ({len(df_products):,} rows)")
        
        print(f"\n✓ All files saved to '{output_folder}' folder")
        
    except Exception as e:
        print(f"\n❌ ERROR: Failed to save output files!")
        print(f"   {str(e)}")
        print("\n   Pipeline aborted.")
        exit(1)

def main():
    """
    Main pipeline orchestrator.
    Calls Extract → Transform → Load → Business Insights in sequence.
    """
    print("\n" + "="*60)
    print("ORDERS & PRODUCTS ETL PIPELINE")
    print("="*60)
    
    # STEP 1: Extract
    df_orders, df_products = extract()
    
    # STEP 2: Transform
    df_orders_clean, df_products_clean = transform(df_orders, df_products)
    
    # STEP 3: Load
    load(df_orders_clean, df_products_clean)
    
    print("\n" + "="*60)
    print("✓ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    print("="*60)
    print("\n📁 Check the 'output' folder for your reports:")
    print("   - orders_report.csv")
    print("   - products_report.csv")
    print("\n")


if __name__ == "__main__":
    main()
