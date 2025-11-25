import pandas as pd
import os

def check_actual_columns():
    print("🔍 CHECKING ACTUAL COLUMNS IN SOURCE A FILES...")
    print("=" * 60)
    
    source_a_files = {
        'Employees_A': 'data/sources/source_a/Employees.csv',
        'Customers_A': 'data/sources/source_a/Customers.csv',
        'Orders_A': 'data/sources/source_a/Orders.csv',
        'Order_Details_A': 'data/sources/source_a/Order_Details.csv'
    }
    
    for name, path in source_a_files.items():
        if os.path.exists(path):
            df = pd.read_csv(path)
            print(f"\n📋 {name}:")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Shape: {df.shape}")
            if len(df) > 0:
                print(f"   Sample data:")
                print(df.head(1).to_string())
        else:
            print(f"\n❌ {name}: File not found")

if __name__ == "__main__":
    check_actual_columns()
