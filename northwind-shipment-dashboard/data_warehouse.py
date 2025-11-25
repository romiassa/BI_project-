import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os

class UnifiedDataWarehouse:
    def __init__(self):
        self.dataframes = {}
        
    def load_both_sources(self):
        """Load data from both sources including PRODUCT DATA"""
        print("📂 LOADING DATA FROM BOTH SOURCES...")
        
        # Source A - Your original CSV files
        source_a_files = {
            'Employees_A': 'data/sources/source_a/Employees.csv',
            'Orders_A': 'data/sources/source_a/Orders.csv', 
            'Order_Details_A': 'data/sources/source_a/Order_Details.csv',
            'Customers_A': 'data/sources/source_a/Customers.csv'
        }
        
        # Source B - Extracted from SQL Server (INCLUDING PRODUCT DATA)
        source_b_files = {
            'Employees_B': 'data/sources/source_b/Employees.csv',
            'Orders_B': 'data/sources/source_b/Orders.csv',
            'Order_Details_B': 'data/sources/source_b/Order_Details.csv', 
            'Customers_B': 'data/sources/source_b/Customers.csv',
            'EmployeeTerritories_B': 'data/sources/source_b/EmployeeTerritories.csv',
            'Territories_B': 'data/sources/source_b/Territories.csv',
            'Region_B': 'data/sources/source_b/Region.csv',
            'Products_B': 'data/sources/source_b/Products.csv',
            'Categories_B': 'data/sources/source_b/Categories.csv',
            'Suppliers_B': 'data/sources/source_b/Suppliers.csv'
        }
        
        # Load Source A data
        print("📁 Source A:")
        for name, path in source_a_files.items():
            if os.path.exists(path):
                self.dataframes[name] = pd.read_csv(path)
                print(f"   ✅ {name}: {len(self.dataframes[name])} rows")
        
        # Load Source B data including PRODUCTS  
        print("📁 Source B:")
        for name, path in source_b_files.items():
            if os.path.exists(path):
                self.dataframes[name] = pd.read_csv(path)
                print(f"   ✅ {name}: {len(self.dataframes[name])} rows")
            else:
                print(f"   ❌ {name}: File not found at {path}")
        
        return self.dataframes
    
    def create_product_dimension(self):
        """Create COMPLETE product dimension for 4D analysis"""
        print("\n📦 CREATING PRODUCT DIMENSION...")
        
        # Check if product data exists
        if 'Products_B' not in self.dataframes or 'Categories_B' not in self.dataframes:
            print("❌ Product data not available - creating empty dimension")
            product_dim = pd.DataFrame({
                'ProductID': [],
                'ProductName': [],
                'CategoryID': [],
                'CategoryName': [],
                'SupplierID': [],
                'CompanyName': [],
                'UnitPrice': [],
                'UnitsInStock': [],
                'Discontinued': []
            })
            self.dataframes['dim_products'] = product_dim
            return product_dim
        
        products_b = self.dataframes['Products_B'].copy()
        categories_b = self.dataframes['Categories_B'].copy()
        suppliers_b = self.dataframes['Suppliers_B'].copy() if 'Suppliers_B' in self.dataframes else pd.DataFrame()
        
        # Merge products with categories
        products_with_categories = pd.merge(
            products_b, 
            categories_b[['CategoryID', 'CategoryName', 'Description']], 
            on='CategoryID', 
            how='left'
        )
        
        # Merge with suppliers if available
        if not suppliers_b.empty:
            products_with_categories = pd.merge(
                products_with_categories,
                suppliers_b[['SupplierID', 'CompanyName', 'Country']],
                on='SupplierID',
                how='left'
            )
            products_with_categories['SupplierCompany'] = products_with_categories['CompanyName']
            products_with_categories['SupplierCountry'] = products_with_categories['Country']
        else:
            products_with_categories['SupplierCompany'] = 'Unknown Supplier'
            products_with_categories['SupplierCountry'] = 'Unknown Country'
        
        # ✅ PRODUCT DIMENSION - Complete attributes for 4D analysis
        product_dim = products_with_categories[[
            'ProductID', 'ProductName', 'CategoryID', 'CategoryName',
            'SupplierID', 'SupplierCompany', 'SupplierCountry',
            'UnitPrice', 'UnitsInStock', 'UnitsOnOrder', 'ReorderLevel',
            'Discontinued', 'QuantityPerUnit'
        ]].copy()
        
        # Enhance with analysis attributes
        product_dim['price_range'] = pd.cut(
            product_dim['UnitPrice'], 
            bins=[0, 10, 25, 50, 100, float('inf')],
            labels=['Budget', 'Economy', 'Standard', 'Premium', 'Luxury']
        )
        
        product_dim['stock_status'] = np.where(
            product_dim['UnitsInStock'] == 0, 'Out of Stock',
            np.where(product_dim['UnitsInStock'] < product_dim['ReorderLevel'], 'Low Stock', 'In Stock')
        )
        
        product_dim['is_discontinued'] = product_dim['Discontinued'] == 1
        
        self.dataframes['dim_products'] = product_dim
        print(f"✅ Product Dimension: {len(product_dim)} products")
        print(f"   Categories: {product_dim['CategoryName'].nunique()} categories")
        print(f"   Price ranges: {product_dim['price_range'].nunique()} ranges")
        return product_dim
    
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
        return time_dimension
    
    def create_enhanced_fact_orders(self):
        """Create ENHANCED fact table with PRODUCT-LEVEL details for 4D analysis"""
        print("\n📊 CREATING ENHANCED FACT ORDERS TABLE...")
        
        # Use Source B data
        orders_b = self.dataframes['Orders_B'].copy()
        order_details_b = self.dataframes['Order_Details_B'].copy()
        
        # Check if product data is available for enhanced analysis
        has_product_data = 'Products_B' in self.dataframes and 'Categories_B' in self.dataframes
        
        if has_product_data:
            print("🎯 PRODUCT DATA AVAILABLE - Creating enhanced fact table with product details")
            # Merge order details with product information
            order_details_enhanced = pd.merge(
                order_details_b,
                self.dataframes['Products_B'][['ProductID', 'ProductName', 'CategoryID', 'UnitPrice']],
                on='ProductID',
                how='left'
            )
            
            # Merge with categories
            order_details_enhanced = pd.merge(
                order_details_enhanced,
                self.dataframes['Categories_B'][['CategoryID', 'CategoryName']],
                on='CategoryID',
                how='left'
            )
            
            # Calculate product-level metrics
            product_order_totals = order_details_enhanced.groupby('OrderID').agg({
                'UnitPrice_x': 'sum',  # Total order amount
                'Quantity': 'sum',     # Total quantity
                'Discount': 'mean',    # Average discount
                'CategoryName': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Unknown',  # Primary category
                'ProductID': 'nunique'  # Number of unique products
            }).reset_index()
            
            product_order_totals.columns = [
                'OrderID', 'total_amount', 'total_quantity', 'avg_discount', 
                'primary_category', 'unique_products_count'
            ]
        else:
            print("⚠️  Product data not available - using basic order totals")
            # Basic order totals without product details
            product_order_totals = order_details_b.groupby('OrderID').agg({
                'UnitPrice': 'sum',
                'Quantity': 'sum',
                'Discount': 'mean'
            }).reset_index()
            
            product_order_totals.columns = ['OrderID', 'total_amount', 'total_quantity', 'avg_discount']
            product_order_totals['primary_category'] = 'Unknown'
            product_order_totals['unique_products_count'] = 1
        
        # ✅ ENHANCED FACT ORDERS - Complete attributes for 4D analysis
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
        
        # Merge with enhanced order totals
        fact_orders = pd.merge(fact_orders, product_order_totals, on='OrderID', how='left')
        
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
        fact_orders_final['primary_category'] = fact_orders_final['primary_category'].fillna('Unknown')
        fact_orders_final['unique_products_count'] = fact_orders_final['unique_products_count'].fillna(1)
        
        self.dataframes['fact_orders'] = fact_orders_final
        
        # Calculate KPIs
        shipped_count = fact_orders_final['is_shipped'].sum()
        total_orders = len(fact_orders_final)
        success_rate = (shipped_count / total_orders) * 100
        
        print(f"✅ Enhanced Fact Orders Table: {len(fact_orders_final)} orders")
        print(f"🎯 4D ANALYSIS READY: Product dimension integrated")
        print(f"   - Product categories: {fact_orders_final['primary_category'].nunique()} categories")
        print(f"   - Enhanced metrics: primary_category, unique_products_count")
        print(f"   - Success Rate: {success_rate:.1f}%")
        
        return fact_orders_final, total_orders, has_product_data
    
    def build_enhanced_warehouse(self):
        """Build ENHANCED data warehouse with PRODUCT DIMENSION for 4D analysis"""
        print("🏗️ BUILDING ENHANCED 4D DATA WAREHOUSE...")
        print("=" * 60)
        
        # Load both sources including PRODUCT DATA
        self.load_both_sources()
        
        # Create COMPLETE dimensions including PRODUCTS
        self.create_employee_dimension()
        self.create_customer_dimension()
        self.create_product_dimension()  # NEW: Product dimension
        
        # Create ENHANCED fact table with product details
        fact_orders, total_orders, has_product_data = self.create_enhanced_fact_orders()
        
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
        
        print("\n🎉 ENHANCED 4D DATA WAREHOUSE READY!")
        print("📊 4D ANALYSIS STRUCTURE:")
        print("   - dim_employees: Employee performance analysis")
        print("   - dim_customers: Customer reliability analysis") 
        print("   - dim_products: Product category/supplier analysis")
        print("   - dim_time: Temporal trend analysis")
        print("   - fact_orders: Enhanced with product categories")
        print(f"🚀 4D VISUALIZATION READY: {has_product_data}")
        
        return has_product_data
    
    def save_to_database(self):
        """Save complete tables to SQLite database"""
        print("\n💾 SAVING ENHANCED TABLES TO DATABASE...")
        
        os.makedirs('data/warehouse', exist_ok=True)
        conn = sqlite3.connect('data/warehouse/northwind_bi.db')
        
        tables_to_save = ['dim_time', 'dim_employees', 'dim_customers', 'dim_products', 'fact_orders']
        
        for table_name in tables_to_save:
            if table_name in self.dataframes and not self.dataframes[table_name].empty:
                self.dataframes[table_name].to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"✅ Saved {table_name} to database")
        
        conn.close()

if __name__ == "__main__":
    warehouse = UnifiedDataWarehouse()
    has_product_data = warehouse.build_enhanced_warehouse()
    print(f"\n📦 PRODUCT DATA STATUS: {'AVAILABLE' if has_product_data else 'NOT AVAILABLE'}")
    print("🚀 Run the enhanced dashboard to see 4D visualization!")
