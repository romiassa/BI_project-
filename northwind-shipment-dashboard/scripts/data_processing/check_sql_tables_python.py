import pyodbc
import pandas as pd

def check_sql_server_connection():
    """Check if we can connect to SQL Server and list tables"""
    try:
        # Connection string for SQL Server on Linux
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Northwind;"
            "UID=SA;"
            "PWD=Northwind123!;"
        )
        
        print("🔗 Attempting to connect to SQL Server...")
        conn = pyodbc.connect(connection_string)
        print("✅ Connected to SQL Server successfully!")
        
        # Get all tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        print(f"📊 Found {len(tables)} tables in Northwind database:")
        print("=" * 50)
        
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table[0]}")
            
        # Also check the structure of key tables we need
        print("\n🔍 Checking structure of essential tables:")
        essential_tables = ['Employees', 'EmployeeTerritories', 'Territories', 'Region', 'Customers', 'Orders', 'Order Details']
        
        for table in essential_tables:
            try:
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table}' 
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cursor.fetchall()
                print(f"\n📋 {table} ({len(columns)} columns):")
                for col in columns:
                    print(f"   - {col[0]} ({col[1]})")
            except Exception as e:
                print(f"❌ Could not check {table}: {e}")
        
        conn.close()
        
    except pyodbc.Error as e:
        print(f"❌ SQL Server connection failed: {e}")
        print("\n💡 Troubleshooting tips:")
        print("1. Make sure SQL Server is running: sudo systemctl status mssql-server")
        print("2. Check if Northwind database exists")
        print("3. Verify credentials: SA / Northwind123!")
        print("4. Install ODBC driver if needed")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    check_sql_server_connection()
