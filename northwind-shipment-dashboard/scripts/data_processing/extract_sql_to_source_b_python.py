import pyodbc
import pandas as pd
import os

def extract_sql_tables_to_csv():
    """Extract essential tables from SQL Server to CSV files in Source B"""
    
    try:
        # Connection string
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Northwind;"
            "UID=SA;"
            "PWD=Northwind123!;"
        )
        
        print("🔗 Connecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        print("✅ Connected successfully!")
        
        # Create Source B directory
        source_b_dir = 'data/sources/source_b'
        os.makedirs(source_b_dir, exist_ok=True)
        
        # Essential tables for our project
        essential_tables = [
            'Employees',
            'EmployeeTerritories', 
            'Territories',
            'Region',
            'Customers',
            'Orders',
            'Order Details'  # Note: Space in name
        ]
        
        print("🚀 Extracting essential tables to Source B...")
        print("=" * 60)
        
        extracted_tables = {}
        
        for table_name in essential_tables:
            try:
                # Handle table names with spaces
                if ' ' in table_name:
                    query = f'SELECT * FROM [{table_name}]'
                else:
                    query = f'SELECT * FROM {table_name}'
                
                # Read table into DataFrame
                df = pd.read_sql(query, conn)
                
                # Save to CSV
                csv_filename = table_name.replace(' ', '_') + '.csv'
                csv_path = os.path.join(source_b_dir, csv_filename)
                df.to_csv(csv_path, index=False)
                
                extracted_tables[table_name] = df
                print(f"✅ {table_name:25} -> {len(df):4} rows -> {csv_filename}")
                
            except Exception as e:
                print(f"❌ Failed to extract {table_name}: {e}")
        
        conn.close()
        
        print("=" * 60)
        print(f"✅ Extraction completed! {len(extracted_tables)} tables saved to {source_b_dir}/")
        
        # Show summary
        print("\n📊 Extraction Summary:")
        for table_name, df in extracted_tables.items():
            print(f"   - {table_name}: {len(df)} rows, {len(df.columns)} columns")
            
    except pyodbc.Error as e:
        print(f"❌ SQL Server connection failed: {e}")
        print("\n💡 Please check:")
        print("   1. SQL Server is running")
        print("   2. Northwind database exists")
        print("   3. ODBC Driver 17 is installed")
        print("   4. Credentials are correct")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    extract_sql_tables_to_csv()
