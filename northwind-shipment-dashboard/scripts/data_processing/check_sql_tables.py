import subprocess
import sys

def get_sql_server_tables():
    """Get all table names from SQL Server"""
    try:
        # Command to get all table names
        cmd = [
            'sqlcmd', '-S', 'localhost', '-U', 'SA', '-P', 'Northwind123!',
            '-d', 'Northwind', 
            '-Q', "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ SQL Server Tables Found:")
            print("=" * 50)
            print(result.stdout)
        else:
            print("❌ Failed to get tables:")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    get_sql_server_tables()
