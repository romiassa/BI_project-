import pyodbc
import pandas as pd
import os

def extract_phase2_tables():
    """Extract additional tables needed for Phase 2 (Product Dimension)"""
    
    try:
        # Connection string
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Northwind;"
            "UID=SA;"
            "PWD=Northwind123!;"
        )
        
        print("🔗 Connecting to SQL Server for Phase 2 tables...")
        conn = pyodbc.connect(connection_string)
        print("✅ Connected successfully!")
        
        # Create Source B directory (should already exist)
        source_b_dir = 'data/sources/source_b'
        os.makedirs(source_b_dir, exist_ok=True)
        
        # Phase 2 tables for Product Dimension
        phase2_tables = [
            'Products',
            'Categories', 
            'Suppliers'
        ]
        
        print("🚀 Extracting Phase 2 tables to Source B...")
        print("=" * 60)
        
        extracted_tables = {}
        
        for table_name in phase2_tables:
            try:
                # Read table into DataFrame
                query = f'SELECT * FROM {table_name}'
                df = pd.read_sql(query, conn)
                
                # Save to CSV
                csv_filename = table_name + '.csv'
                csv_path = os.path.join(source_b_dir, csv_filename)
                df.to_csv(csv_path, index=False)
                
                extracted_tables[table_name] = df
                print(f"✅ {table_name:25} -> {len(df):4} rows -> {csv_filename}")
                
            except Exception as e:
                print(f"❌ Failed to extract {table_name}: {e}")
        
        conn.close()
        
        print("=" * 60)
        print(f"✅ Phase 2 extraction completed! {len(extracted_tables)} tables saved to {source_b_dir}/")
        
        # Show summary
        print("\n📊 Phase 2 Extraction Summary:")
        for table_name, df in extracted_tables.items():
            print(f"   - {table_name}: {len(df)} rows, {len(df.columns)} columns")
            
        print("\n🎯 These tables will be used for Product Dimension in Phase 2")
            
    except pyodbc.Error as e:
        print(f"❌ SQL Server connection failed: {e}")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    extract_phase2_tables()
