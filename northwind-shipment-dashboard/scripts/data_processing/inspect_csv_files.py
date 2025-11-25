import pandas as pd
import os

def inspect_csv_files():
    print("🔍 INSPECTING CSV FILES STRUCTURE...")
    print("=" * 80)
    
    source_a_dir = 'data/sources/source_a'
    
    if not os.path.exists(source_a_dir):
        print(f"❌ Directory {source_a_dir} does not exist!")
        return
    
    csv_files = [f for f in os.listdir(source_a_dir) if f.endswith('.csv')]
    print(f"📁 Found {len(csv_files)} CSV files:")
    
    for csv_file in sorted(csv_files):
        file_path = os.path.join(source_a_dir, csv_file)
        try:
            # Read just the first few rows to get structure
            df = pd.read_csv(file_path, nrows=5)
            print(f"\n📊 FILE: {csv_file}")
            print(f"   Shape: {df.shape} (rows x columns)")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Sample data:")
            print(df.head(2).to_string(index=False))
            print("-" * 60)
        except Exception as e:
            print(f"❌ Error reading {csv_file}: {e}")

if __name__ == "__main__":
    inspect_csv_files()
