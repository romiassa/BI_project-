import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import urllib.parse

# Database Configuration
DATABASE_CONFIG = {
    'server': 'localhost',
    'database': 'Northwind',
    'username': 'SA', 
    'password': 'Northwind123!',
    'driver': 'ODBC Driver 17 for SQL Server'
}

def create_sql_server_connection():
    connection_string = (
        f"mssql+pyodbc://{DATABASE_CONFIG['username']}:{urllib.parse.quote_plus(DATABASE_CONFIG['password'])}"
        f"@{DATABASE_CONFIG['server']}/{DATABASE_CONFIG['database']}?"
        f"driver={urllib.parse.quote_plus(DATABASE_CONFIG['driver'])}"
    )
    return create_engine(connection_string)

def inspect_table_structure(engine, table_name):
    try:
        # Get column information
        query_columns = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        columns_df = pd.read_sql(query_columns, engine)
        
        # Get sample data 
        query_sample = f"SELECT TOP 3 * FROM {table_name}"
        sample_df = pd.read_sql(query_sample, engine)
        
        return columns_df, sample_df
    except Exception as e:
        print(f"Error inspecting {table_name}: {e}")
        return None, None

def main():
    print("🔍 INSPECTING SQL SERVER TABLES...")
    
    # Create connection
    engine = create_sql_server_connection()
    
    # Get all table names
    query_tables = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    tables_df = pd.read_sql(query_tables, engine)
    
    print(f"📊 Found {len(tables_df)} tables in Northwind database:")
    print("=" * 80)
    
    # Inspect each table
    for table_name in tables_df['TABLE_NAME']:
        print(f"\n📋 TABLE: {table_name}")
        print("-" * 40)
        
        columns_df, sample_df = inspect_table_structure(engine, table_name)
        
        if columns_df is not None:
            print("COLUMNS:")
            for _, row in columns_df.iterrows():
                print(f"  - {row['COLUMN_NAME']} ({row['DATA_TYPE']}) {'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")
            
            if sample_df is not None and not sample_df.empty:
                print(f"\nSAMPLE DATA (first {len(sample_df)} rows):")
                print(sample_df.to_string(index=False))
        
        print("-" * 40)

if __name__ == "__main__":
    main()
