import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os

class NorthwindDataTransformer:
    def __init__(self):
        self.dataframes = {}
        
    def load_source_data(self):
        """Load data from CSV source files"""
        print("📂 Loading source data from CSV files...")
        
        source_a_files = {
            'Employees': 'data/sources/source_a/Employees.csv',
            'Orders': 'data/sources/source_a/Orders.csv', 
            'Order_Details': 'data/sources/source_a/Order Details.csv',
            'Customers': 'data/sources/source_a/Customers.csv',
            'Products': 'data/sources/source_a/Products.csv',
            'Categories': 'data/sources/source_a/Categories.csv',
            'Suppliers': 'data/sources/source_a/Suppliers.csv',
            'Shippers': 'data/sources/source_a/Shippers.csv',
            'Region': 'data/sources/source_a/Region.csv',
            'Territories': 'data/sources/source_a/Territories.csv',
            'EmployeeTerritories': 'data/sources/source_a/EmployeeTerritories.csv'
        }
        
        # Load each CSV file
        for table_name, file_path in source_a_files.items():
            if os.path.exists(file_path):
                self.dataframes[table_name] = pd.read_csv(file_path)
                print(f"✓ Loaded {table_name}: {len(self.dataframes[table_name])} rows")
            else:
                print(f"✗ Missing file: {file_path}")
                
        return self.dataframes
    
    def create_time_dimension(self):
        print("🕐 Creating time dimension...")
        
        # Get date range from orders
        orders_df = self.dataframes['Orders']
        orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'])
        
        min_date = orders_df['OrderDate'].min()
        max_date = orders_df['OrderDate'].max()
        
        # Add buffer period
        buffer_days = 30
        start_date = min_date - timedelta(days=buffer_days)
        end_date = max_date + timedelta(days=buffer_days)
        
        # Generate complete date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        time_dimension = pd.DataFrame({
            'date_id': date_range.strftime('%Y%m%d').astype(int),
            'full_date': date_range,
            'year': date_range.year,
            'quarter': date_range.quarter,
            'month': date_range.month,
            'month_name': date_range.strftime('%B'),
            'day': date_range.day,
            'day_name': date_range.strftime('%A'),
            'week_number': date_range.isocalendar().week,
            'year_month': date_range.strftime('%Y-%m'),
            'is_weekend': date_range.dayofweek >= 5
        })
        
        self.dataframes['dim_time'] = time_dimension
        print(f"✓ Created time dimension: {len(time_dimension)} days")
        return time_dimension
    
    def create_employee_dimension(self):
        print("👥 Creating employee dimension...")
        
        employees = self.dataframes['Employees'].copy()
        territories = self.dataframes['Territories'].copy()
        employee_territories = self.dataframes['EmployeeTerritories'].copy()
        region = self.dataframes['Region'].copy()
        
        # Merge employee territories
        emp_terr = pd.merge(employee_territories, territories, on='TerritoryID', how='left')
        emp_terr = pd.merge(emp_terr, region, on='RegionID', how='left')
        
        # Aggregate territories per employee
        employee_territories_agg = emp_terr.groupby('EmployeeID').agg({
            'TerritoryDescription': lambda x: ', '.join(x),
            'RegionDescription': lambda x: ', '.join(x.unique())
        }).reset_index()
        
        employee_territories_agg.columns = ['EmployeeID', 'work_territories', 'work_regions']
        
        # Create employee dimension
        employee_dim = employees[[
            'EmployeeID', 'LastName', 'FirstName', 'Title',
            'City', 'Region', 'Country'
        ]].copy()
        
        employee_dim['full_name'] = employee_dim['FirstName'] + ' ' + employee_dim['LastName']
        employee_dim = employee_dim.rename(columns={
            'City': 'personal_city',
            'Region': 'personal_region', 
            'Country': 'personal_country'
        })
        
        # Merge with territories
        employee_dim = pd.merge(employee_dim, employee_territories_agg, on='EmployeeID', how='left')
        
        self.dataframes['dim_employees'] = employee_dim
        print(f"✓ Created employee dimension: {len(employee_dim)} employees")
        return employee_dim
    
    def create_customer_dimension(self):
        """Create customer dimension"""
        print("🏢 Creating customer dimension...")
        
        customers = self.dataframes['Customers'].copy()
        orders = self.dataframes['Orders'].copy()
        
        # Calculate customer order statistics
        customer_stats = orders.groupby('CustomerID').agg({
            'OrderID': 'count',
            'OrderDate': ['min', 'max']
        }).reset_index()
        
        customer_stats.columns = ['CustomerID', 'total_orders', 'first_order_date', 'last_order_date']
        
        # Create customer dimension
        customer_dim = customers[[
            'CustomerID', 'CompanyName', 'ContactName', 'ContactTitle',
            'Address', 'City', 'Region', 'Country', 'PostalCode'
        ]].copy()
        
        # Merge with order statistics
        customer_dim = pd.merge(customer_dim, customer_stats, on='CustomerID', how='left')
        
        self.dataframes['dim_customers'] = customer_dim
        print(f"✓ Created customer dimension: {len(customer_dim)} customers")
        return customer_dim
    
    def create_fact_orders(self):
        """Create fact orders table for Phase 1 (3 dimensions)"""
        print("📊 Creating fact orders table...")
        
        orders = self.dataframes['Orders'].copy()
        order_details = self.dataframes['Order_Details'].copy()
        employees = self.dataframes['Employees'].copy()
        
        # Calculate order amounts from order details
        order_totals = order_details.groupby('OrderID').agg({
            'UnitPrice': 'sum',
            'Quantity': 'sum',
            'Discount': 'mean'
        }).reset_index()
        
        order_totals = order_totals.rename(columns={
            'UnitPrice': 'total_amount',
            'Quantity': 'total_quantity'
        })
        
        # Create fact orders
        fact_orders = orders[[
            'OrderID', 'CustomerID', 'EmployeeID', 'OrderDate', 
            'ShippedDate', 'ShipCountry', 'ShipCity', 'ShipRegion'
        ]].copy()
        
        # Convert dates
        fact_orders['OrderDate'] = pd.to_datetime(fact_orders['OrderDate'])
        fact_orders['ShippedDate'] = pd.to_datetime(fact_orders['ShippedDate'])
        
        # Calculate shipment metrics
        fact_orders['is_shipped'] = fact_orders['ShippedDate'].notna()
        fact_orders['shipping_delay_days'] = (fact_orders['ShippedDate'] - fact_orders['OrderDate']).dt.days
        fact_orders['on_time_status'] = fact_orders['shipping_delay_days'] <= 7  # Assuming 7 days is on-time
        
        # Merge with order totals
        fact_orders = pd.merge(fact_orders, order_totals, on='OrderID', how='left')
        
        # Add date_id for time dimension
        fact_orders['date_id'] = fact_orders['OrderDate'].dt.strftime('%Y%m%d').astype(int)
        
        self.dataframes['fact_orders'] = fact_orders
        print(f"✓ Created fact orders: {len(fact_orders)} orders")
        return fact_orders
    
    def build_data_warehouse(self):
        """Build complete data warehouse"""
        print("🏗️ Building data warehouse...")
        
        # Load source data
        self.load_source_data()
        
        # Create dimensions
        self.create_time_dimension()
        self.create_employee_dimension() 
        self.create_customer_dimension()
        
        # Create fact table
        self.create_fact_orders()
        
        # Save to SQLite database
        self.save_to_database()
        
        print("✅ Data warehouse built successfully!")
    
    def save_to_database(self):
        """Save all tables to SQLite database"""
        print("💾 Saving to SQLite database...")
        
       
        os.makedirs('data/warehouse', exist_ok=True)
        
        # Connect to SQLite
        conn = sqlite3.connect('data/warehouse/northwind_bi.db')
        
        # Save each table
        tables_to_save = ['dim_time', 'dim_employees', 'dim_customers', 'fact_orders']
        
        for table_name in tables_to_save:
            if table_name in self.dataframes:
                self.dataframes[table_name].to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"✓ Saved {table_name} to database")
        
        conn.close()
