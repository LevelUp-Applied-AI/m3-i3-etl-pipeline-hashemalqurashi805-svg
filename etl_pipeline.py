"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os

def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames."""
    print("--- Starting Extraction ---")
    tables = ["customers", "products", "orders", "order_items"]
    data_dict = {}
    
    for table in tables:
        data_dict[table] = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"Extracted {len(data_dict[table])} rows from {table}")
        
    return data_dict

def transform(data_dict):
    """Transform raw data into customer-level analytics summary."""
    print("--- Starting Transformation ---")
    
    # 1. Join orders with order_items and products and customers
    df = data_dict['order_items'].merge(data_dict['orders'], on='order_id')
    df = df.merge(data_dict['products'], on='product_id')
    df = df.merge(data_dict['customers'], on='customer_id')

    # 2. Compute line_total (quantity * unit_price)
    df['line_total'] = df['quantity'] * df['unit_price']

    # 3. Filter out cancelled orders (status = 'cancelled')
    df = df[df['status'] != 'cancelled']

    # 4. Filter out suspicious quantities (quantity > 100)
    df = df[df['quantity'] <= 100]

    # 5. Aggregate to customer level
    # حساب عدد الطلبات الفريدة وإجمالي الإيرادات
    customer_summary = df.groupby(['customer_id', 'customer_name', 'city']).agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    # حساب متوسط قيمة الطلب (Average Order Value)
    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']

    # (اختياري) تحديد الفئة الأكثر شراءً لكل عميل - سنكتفي بالبيانات الأساسية حالياً
    customer_summary['top_category'] = "General" 

    return customer_summary

def validate(df):
    """Run data quality checks on the transformed DataFrame."""
    print("--- Starting Validation ---")
    
    checks = {
        "no_nulls": df[['customer_id', 'customer_name']].notnull().all().all(),
        "positive_revenue": (df['total_revenue'] >= 0).all(),
        "no_duplicates": df['customer_id'].is_unique,
        "positive_orders": (df['total_orders'] > 0).all()
    }
    
    for check, passed in checks.items():
        if not passed:
            raise ValueError(f"Critical Data Quality Check Failed: {check}")
        print(f"Check '{check}': Passed")
        
    return checks

def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file."""
    print("--- Starting Loading ---")
    
    # 1. Load to PostgreSQL
    df.to_sql('customer_summary', engine, if_exists='replace', index=False)
    print("Loaded data to table 'customer_summary'")

    # 2. Load to CSV
    # إنشاء المجلد إذا لم يكن موجوداً
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Saved analytics to {csv_path}")

def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    # 1. Create engine
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/amman_market")
    engine = create_engine(DATABASE_URL)

    try:
        # 2. Extract
        data = extract(engine)

        # 3. Transform
        summary_df = transform(data)

        # 4. Validate
        validate(summary_df)

        # 5. Load
        load(summary_df, engine, "output/customer_analytics.csv")
        
        print("\n🎉 ETL Pipeline executed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")

if __name__ == "__main__":
    main()