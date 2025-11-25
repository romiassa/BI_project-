import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os

class UnifiedDataWarehouse:
    def __init__(self):
        self.dataframes = {}
        
    def load_both_sources(self):
        """Load data from both sources"""
        print("📂 LOADING DATA FROM BOTH SOURCES...")
        
        # Source A - Your original CSV files
        source_a_files = {
            'Employees_A': 'data/sources/source_a/Employees.csv',
            'Orders_A': 'data/sources/source_a/Orders.csv', 
            'Order_Details_A': 'data/sources/source_a/Order_Details.csv',
            'Customers_A': 'data/sources/source_a/Customers.csv'
        }
        
        # Source B - Extracted from SQL Server
        source_b_files = {
            'Employees_B': 'data/sources/source_b/Employees.csv',
            'Orders_B': 'data/sources/source_b/Orders.csv',
            'Order_Details_B': 'data/sources/source_b/Order_Details.csv', 
            'Customers_B': 'data/sources/source_b/Customers.csv',
            'EmployeeTerritories_B': 'data/sources/source_b/EmployeeTerritories.csv',
            'Territories_B': 'data/sources/source_b/Territories.csv',
            'Region_B': 'data/sources/source_b/Region.csv'
        }
        
        # Load Source A data
        print("📁 Source A:")
        for name, path in source_a_files.items():
            if os.path.exists(path):
                self.dataframes[name] = pd.read_csv(path)
                print(f"   ✅ {name}: {len(self.dataframes[name])} rows")
        
        # Load Source B data  
        print("📁 Source B:")
        for name, path in source_b_files.items():
            if os.path.exists(path):
                self.dataframes[name] = pd.read_csv(path)
                print(f"   ✅ {name}: {len(self.dataframes[name])} rows")
        
        return self.dataframes
    
    def create_employee_dimension(self):
        """Create employee dimension with ESSENTIAL attributes"""
        print("\n👥 CREATING EMPLOYEE DIMENSION...")
        
        # Use Source B for employees (complete data)
        employees_b = self.dataframes['Employees_B'].copy()
        emp_territories = self.dataframes['EmployeeTerritories_B'].copy()
        territories = self.dataframes['Territories_B'].copy()
        region = self.dataframes['Region_B'].copy()
        
        # Merge for territory information
        emp_terr = pd.merge(emp_territories, territories, on='TerritoryID', how='left')
        emp_terr = pd.merge(emp_terr, region, on='RegionID', how='left')
        
        # Aggregate territories per employee
        employee_territories_agg = emp_terr.groupby('EmployeeID').agg({
            'TerritoryDescription': lambda x: ', '.join([str(desc).strip() for desc in x if pd.notna(desc)]),
            'RegionDescription': lambda x: ', '.join([str(desc).strip() for desc in x.unique() if pd.notna(desc)])
        }).reset_index()
        
        employee_territories_agg.columns = ['EmployeeID', 'work_territories', 'work_regions']
        
        # ✅ EMPLOYEE DIMENSION - Essential attributes for analysis
        employee_dim = employees_b[[
            'EmployeeID', 'LastName', 'FirstName', 'Title',
            'City', 'Region', 'Country', 'HireDate'
        ]].copy()
        
        # Keep important fields for analysis
        employee_dim['full_name'] = employee_dim['FirstName'] + ' ' + employee_dim['LastName']
        employee_dim['location'] = employee_dim['City'] + ', ' + employee_dim['Country']
        
        # Convert HireDate for analysis
        employee_dim['HireDate'] = pd.to_datetime(employee_dim['HireDate'], errors='coerce')
        
        # Merge with territories (IMPORTANT for analysis)
        employee_dim = pd.merge(employee_dim, employee_territories_agg, on='EmployeeID', how='left')
        
        # Fill NaN values
        employee_dim['work_territories'] = employee_dim['work_territories'].fillna('No Territory')
        employee_dim['work_regions'] = employee_dim['work_regions'].fillna('No Region')
        employee_dim['Region'] = employee_dim['Region'].fillna('Unknown')
        
        self.dataframes['dim_employees'] = employee_dim
        print(f"✅ Employee Dimension: {len(employee_dim)} employees")
        print(f"   Essential attributes: ID, name, title, location, region, territories, hire_date")
        return employee_dim
    
    def create_customer_dimension(self):
        """Create customer dimension with ESSENTIAL attributes"""
        print("\n🏢 CREATING CUSTOMER DIMENSION...")
        
        # Use Source B for customers
        customers_b = self.dataframes['Customers_B'].copy()
        orders_b = self.dataframes['Orders_B'].copy()
        
        # Calculate customer order statistics
        orders_b['OrderDate'] = pd.to_datetime(orders_b['OrderDate'], errors='coerce')
        valid_orders = orders_b[orders_b['OrderDate'].notna()]
        
        customer_stats = valid_orders.groupby('CustomerID').agg({
            'OrderID': 'count',
            'OrderDate': ['min', 'max']
        }).reset_index()
        
        customer_stats.columns = ['CustomerID', 'total_orders', 'first_order_date', 'last_order_date']
        
        # ✅ CUSTOMER DIMENSION - Essential attributes for analysis
        customer_dim = customers_b[[
            'CustomerID', 'CompanyName', 'ContactName', 'ContactTitle',
            'City', 'Region', 'Country', 'PostalCode'
        ]].copy()
        
        # Keep important fields for analysis
        customer_dim['location'] = customer_dim['City'] + ', ' + customer_dim['Country']
        
        # Merge with order statistics
        customer_dim = pd.merge(customer_dim, customer_stats, on='CustomerID', how='left')
        customer_dim['total_orders'] = customer_dim['total_orders'].fillna(0)
        
        # Convert dates for analysis
        customer_dim['first_order_date'] = pd.to_datetime(customer_dim['first_order_date'], errors='coerce')
        customer_dim['last_order_date'] = pd.to_datetime(customer_dim['last_order_date'], errors='coerce')
        
        self.dataframes['dim_customers'] = customer_dim
        print(f"✅ Customer Dimension: {len(customer_dim)} customers")
        print(f"   Essential attributes: ID, company, contact, location, region, order_stats")
        return customer_dim
    
    def create_order_based_time_dimension(self, total_orders):
        """Create time dimension where EACH ORDER gets its OWN MONTH"""
        print(f"\n🕐 CREATING ORDER-BASED TIME DIMENSION...")
        print(f"   Total orders: {total_orders} = {total_orders} months")
        
        # Start from November 2025 (final month)
        final_month = datetime(2025, 11, 1)
        
        # Generate months going BACKWARDS from November 2025
        months = []
        for i in range(total_orders):
            month_date = final_month - pd.DateOffset(months=i)
            months.append(month_date)
        
        # Reverse to have chronological order
        months = list(reversed(months))
        
        # ✅ TIME DIMENSION - Essential attributes for analysis
        time_dimension = pd.DataFrame({
            'date_id': [date.strftime('%Y%m') for date in months],
            'full_date': months,
            'year': [date.year for date in months],
            'quarter': [date.quarter for date in months],
            'month': [date.month for date in months],
            'month_name': [date.strftime('%B') for date in months],
            'year_month': [date.strftime('%Y-%m') for date in months],
            'quarter_name': ['Q' + str(date.quarter) + ' ' + str(date.year) for date in months],
            'month_year': [date.strftime('%b %Y') for date in months],
            'order_sequence': range(1, total_orders + 1)
        })
        
        self.dataframes['dim_time'] = time_dimension
        print(f"✅ Time Dimension: {len(time_dimension)} months")
        print(f"   Essential attributes: date_id, year, quarter, month, year_month, sequence")
        return time_dimension
    
    def create_fact_orders(self):
        """Create fact table with COMPLETE analysis attributes"""
        print("\n📊 CREATING FACT ORDERS TABLE...")
        
        # Use Source B data
        orders_b = self.dataframes['Orders_B'].copy()
        order_details_b = self.dataframes['Order_Details_B'].copy()
        
        # Calculate order amounts
        order_totals = order_details_b.groupby('OrderID').agg({
            'UnitPrice': 'sum',
            'Quantity': 'sum',
            'Discount': 'mean'
        }).reset_index()
        
        order_totals = order_totals.rename(columns={
            'UnitPrice': 'total_amount',
            'Quantity': 'total_quantity',
            'Discount': 'avg_discount'
        })
        
        # ✅ FACT ORDERS - Complete attributes for analysis
        fact_orders = orders_b[[
            'OrderID', 'CustomerID', 'EmployeeID', 
            'OrderDate', 'RequiredDate', 'ShippedDate',
            'ShipCountry', 'ShipCity', 'ShipRegion', 
            'Freight', 'ShipName', 'ShipAddress'
        ]].copy()
        
        # Convert dates
        fact_orders['OrderDate'] = pd.to_datetime(fact_orders['OrderDate'], errors='coerce')
        fact_orders['RequiredDate'] = pd.to_datetime(fact_orders['RequiredDate'], errors='coerce')
        fact_orders['ShippedDate'] = pd.to_datetime(fact_orders['ShippedDate'], errors='coerce')
        
        # ✅ COMPLETE SHIPMENT KPIs for analysis
        fact_orders['is_shipped'] = fact_orders['ShippedDate'].notna()
        fact_orders['shipping_delay_days'] = (fact_orders['ShippedDate'] - fact_orders['OrderDate']).dt.days
        fact_orders['fulfillment_delay_days'] = (fact_orders['ShippedDate'] - fact_orders['RequiredDate']).dt.days
        fact_orders['on_time_status'] = fact_orders['shipping_delay_days'] <= 7
        fact_orders['is_fulfilled_on_time'] = fact_orders['fulfillment_delay_days'] <= 0
        
        # Merge with order totals
        fact_orders = pd.merge(fact_orders, order_totals, on='OrderID', how='left')
        
        # Sort orders by date for time sequence
        fact_orders_sorted = fact_orders.sort_values('OrderDate').reset_index(drop=True)
        fact_orders_sorted['order_sequence'] = range(1, len(fact_orders_sorted) + 1)
        
        # Final fact table
        fact_orders_final = fact_orders_sorted.rename(columns={
            'OrderID': 'order_id',
            'CustomerID': 'customer_id', 
            'EmployeeID': 'employee_id'
        })
        
        # Fill NaN values
        fact_orders_final['total_amount'] = fact_orders_final['total_amount'].fillna(0)
        fact_orders_final['total_quantity'] = fact_orders_final['total_quantity'].fillna(0)
        fact_orders_final['avg_discount'] = fact_orders_final['avg_discount'].fillna(0)
        fact_orders_final['shipping_delay_days'] = fact_orders_final['shipping_delay_days'].fillna(-1)
        fact_orders_final['fulfillment_delay_days'] = fact_orders_final['fulfillment_delay_days'].fillna(-1)
        fact_orders_final['Freight'] = fact_orders_final['Freight'].fillna(0)
        
        self.dataframes['fact_orders'] = fact_orders_final
        
        # Calculate KPIs
        shipped_count = fact_orders_final['is_shipped'].sum()
        total_orders = len(fact_orders_final)
        success_rate = (shipped_count / total_orders) * 100
        
        print(f"✅ Fact Orders Table: {len(fact_orders_final)} orders")
        print(f"🎯 COMPLETE KPIs for analysis:")
        print(f"   - is_shipped: 🟢 Livrées vs 🔴 Non Livrées")
        print(f"   - total_amount: Order value")
        print(f"   - total_quantity: Number of items")
        print(f"   - shipping_delay_days: Shipping performance")
        print(f"   - fulfillment_delay_days: Fulfillment performance")
        print(f"   - on_time_status: On-time shipping")
        print(f"   - avg_discount: Discount analysis")
        print(f"   - Success Rate: {success_rate:.1f}%")
        
        return fact_orders_final, total_orders
    
    def build_unified_warehouse(self):
        """Build COMPLETE unified data warehouse for analysis"""
        print("🏗️ BUILDING COMPLETE DATA WAREHOUSE FOR ANALYSIS...")
        print("=" * 60)
        
        # Load both sources
        self.load_both_sources()
        
        # Create COMPLETE dimensions
        self.create_employee_dimension()
        self.create_customer_dimension()
        
        # Create COMPLETE fact table
        fact_orders, total_orders = self.create_fact_orders()
        
        # Create ORDER-BASED time dimension
        self.create_order_based_time_dimension(total_orders)
        
        # Assign time dimension to fact orders
        time_dim = self.dataframes['dim_time']
        fact_orders_with_time = pd.merge(
            fact_orders, 
            time_dim[['order_sequence', 'date_id']], 
            on='order_sequence', 
            how='left'
        )
        
        self.dataframes['fact_orders'] = fact_orders_with_time
        
        # Save to SQLite database
        self.save_to_database()
        
        print("\n🎉 COMPLETE DATA WAREHOUSE READY FOR ANALYSIS!")
        print("📊 FINAL COMPLETE STRUCTURE:")
        print("   - dim_employees: ID, name, title, location, region, territories, hire_date")
        print("   - dim_customers: ID, company, contact, location, region, order_stats")
        print("   - dim_time: ID, year, quarter, month, year_month, sequence")
        print("   - fact_orders: Complete order facts with all KPIs")
        print("🚀 READY FOR 3D PLOT: Time × Employees × Customers")
    
    def save_to_database(self):
        """Save complete tables to SQLite database"""
        print("\n💾 SAVING COMPLETE TABLES TO DATABASE...")
        
        os.makedirs('data/warehouse', exist_ok=True)
        conn = sqlite3.connect('data/warehouse/northwind_bi.db')
        
        tables_to_save = ['dim_time', 'dim_employees', 'dim_customers', 'fact_orders']
        
        for table_name in tables_to_save:
            if table_name in self.dataframes:
                self.dataframes[table_name].to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"✅ Saved {table_name} to database")
        
        conn.close()

if __name__ == "__main__":
    warehouse = UnifiedDataWarehouse()
    warehouse.build_unified_warehouse()
