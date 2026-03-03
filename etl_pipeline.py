import pandas as pd
import numpy as np
import warnings
import os
from pathlib import Path

warnings.filterwarnings('ignore')


def extract():
    print("\n" + "="*60)
    print("EXTRACT PHASE - Reading Raw Data Files")
    print("="*60)
    
    # Define file paths
    sales_file = "sales_data.csv"
    customer_file = "customer_data.json"
    
    try:
        print(f"\n📂 Loading {sales_file}...")
        df_sales = pd.read_csv(sales_file)
        print(f"✓ Sales data loaded successfully: {len(df_sales):,} rows")
        
        print(f"\n📂 Loading {customer_file}...")
        df_customers = pd.read_json(customer_file)
        print(f"✓ Customer data loaded successfully: {len(df_customers):,} rows")
        
        return df_sales, df_customers
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Required file not found!")
        print(f"   {str(e)}")
        print("\n   Please ensure both files are in the project directory:")
        print(f"   - {sales_file}")
        print(f"   - {customer_file}")
        print("\n   Pipeline aborted. Fix the issue and run again.")
        exit(1)
        
    except Exception as e:
        print(f"\n❌ ERROR: Unexpected error while reading files!")
        print(f"   {str(e)}")
        print("\n   Pipeline aborted.")
        exit(1)


def transform(df_sales, df_customers):
    print("\n" + "="*60)
    print("TRANSFORM PHASE - Data Cleaning & Enrichment")
    print("="*60)
    
    # ==================== SALES DATA CLEANING ====================
    print("\n🔧 Cleaning Sales Data...")
    
    # 1. Handle null values in categorical columns
    print("   → Filling missing categorical values with 'unknown'")
    categorical_cols = ['store_id', 'product_name', 'payment_mode', 'sales_rep_id']
    df_sales[categorical_cols] = df_sales[categorical_cols].fillna("unknown")
    
    # 2. Calculate missing unit prices
    print("   → Calculating missing unit_price from total_amount/quantity")
    df_sales['unit_price'] = df_sales['unit_price'].fillna(
        df_sales['total_amount'] / df_sales['quantity']
    )
    
    # 3. Convert and standardize date format
    print("   → Standardizing sale_date format")
    df_sales['sale_date'] = pd.to_datetime(
        df_sales['sale_date'], 
        format='mixed', 
        errors='coerce'
    )
    
    # 4. Remove duplicate transactions
    duplicates_before = df_sales['transaction_id'].duplicated().sum()
    df_sales = df_sales.drop_duplicates(
        subset=['transaction_id'], 
        keep='first'
    ).reset_index(drop=True)
    print(f"   → Removed {duplicates_before} duplicate transactions")
    
    # 5. Standardize string formatting (Title Case)
    print("   → Applying title case formatting to text columns")
    text_cols = ['region', 'category', 'product_name']
    df_sales[text_cols] = df_sales[text_cols].apply(
        lambda x: x.str.title() if x.dtype == 'object' else x
    )
    
    # 6. Fix any negative prices
    df_sales['unit_price'] = df_sales['unit_price'].abs()
    
    print(f"✓ Sales data cleaned: {len(df_sales):,} rows remaining")
    
    
    # ==================== CUSTOMER DATA CLEANING ====================
    print("\n🔧 Cleaning Customer Data...")
    
    # 1. Handle null values
    print("   → Filling missing customer fields with 'none'")
    customer_null_cols = ['email', 'phone', 'city', 'occupation', 'last_purchase_date']
    df_customers[customer_null_cols] = df_customers[customer_null_cols].fillna('none')
    
    # 2. Convert date columns
    print("   → Converting date columns to datetime format")
    date_cols = ['date_of_birth', 'joined_date', 'last_purchase_date']
    df_customers[date_cols] = df_customers[date_cols].apply(
        lambda x: pd.to_datetime(x, format='mixed', errors='coerce')
    )
    
    # 3. Remove duplicate customers
    duplicates_before = df_customers['customer_id'].duplicated().sum()
    df_customers = df_customers.drop_duplicates(
        subset=['customer_id'], 
        keep='first'
    ).reset_index(drop=True)
    print(f"   → Removed {duplicates_before} duplicate customer records")
    
    # 4. Standardize text formatting
    print("   → Standardizing gender and membership_tier")
    df_customers[['gender', 'membership_tier']] = df_customers[['gender', 'membership_tier']].apply(
        lambda x: x.str.title()
    )
    
    # 5. Clean and convert lifetime_value
    print("   → Converting lifetime_value to numeric")
    df_customers['lifetime_value'] = (
        df_customers['lifetime_value']
        .astype(str)
        .str.replace(r'[^\d.]', '', regex=True)
    )
    df_customers['lifetime_value'] = pd.to_numeric(
        df_customers['lifetime_value'], 
        errors='coerce'
    )
    
    print(f"✓ Customer data cleaned: {len(df_customers):,} rows remaining")
    
    return df_sales, df_customers


def load(df_sales, df_customers):
    """
    LOAD PHASE
    Saves the cleaned and transformed data to the output folder.
    Creates the folder if it doesn't exist.
    """
    # Create output folder if it doesn't exist
    output_folder = Path("output")
    output_folder.mkdir(exist_ok=True)
    
    # Define output file paths
    sales_output = output_folder / "sales_report.csv"
    customer_output = output_folder / "customer_report.csv"
    
    try:
        # Save sales data
        print(f"\n💾 Saving sales report to: {sales_output}")
        df_sales.to_csv(sales_output, index=False)
        print(f"✓ Sales report saved successfully ({len(df_sales):,} rows)")
        
        # Save customer data
        print(f"\n💾 Saving customer report to: {customer_output}")
        df_customers.to_csv(customer_output, index=False)
        print(f"✓ Customer report saved successfully ({len(df_customers):,} rows)")
        
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
    # STEP 1: Extract
    df_sales, df_customers = extract()
    
    # STEP 2: Transform
    df_sales_clean, df_customers_clean = transform(df_sales, df_customers)
    
    # STEP 3: Load
    load(df_sales_clean, df_customers_clean)
    
    print("\n" + "="*60)
    print("✓ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    print("="*60)
    print("\n📁 Check the 'output' folder for your reports:")
    print("   - sales_report.csv")
    print("   - customer_report.csv")
    print("\n")


if __name__ == "__main__":
    main()
