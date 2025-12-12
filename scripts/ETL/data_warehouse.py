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
        
        # Source A 
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

    def standardize_source_a_columns(self):
        print("\n🔄 STANDARDIZING SOURCE A COLUMN NAMES...")
        
        # Standardize Customers_A
        if 'Customers_A' in self.dataframes:
            customers_a = self.dataframes['Customers_A'].copy()
            # Map Source A column names to match Source B
            column_mapping = {
                'ID': 'CustomerID',
                'Company': 'CompanyName',
                'Last Name': 'LastName', 
                'First Name': 'FirstName',
                'E-mail Address': 'Email',
                'Job Title': 'ContactTitle',
                'Business Phone': 'Phone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }
            # Rename columns 
            existing_columns = {k: v for k, v in column_mapping.items() if k in customers_a.columns}
            customers_a = customers_a.rename(columns=existing_columns)
            self.dataframes['Customers_A'] = customers_a
            print(f"   ✅ Standardized Customers_A columns: {list(customers_a.columns)}")
        
        # Standardize Orders_A
        if 'Orders_A' in self.dataframes:
            orders_a = self.dataframes['Orders_A'].copy()
            column_mapping = {
                'Order ID': 'OrderID',
                'Employee ID': 'EmployeeID', 
                'Customer ID': 'CustomerID',
                'Order Date': 'OrderDate',
                'Shipped Date': 'ShippedDate',
                'Ship Name': 'ShipName',
                'Ship Address': 'ShipAddress',
                'Ship City': 'ShipCity',
                'Ship State/Province': 'ShipRegion',
                'Ship ZIP/Postal Code': 'ShipPostalCode',
                'Ship Country/Region': 'ShipCountry',
                'Shipping Fee': 'Freight'
            }
            existing_columns = {k: v for k, v in column_mapping.items() if k in orders_a.columns}
            orders_a = orders_a.rename(columns=existing_columns)
            self.dataframes['Orders_A'] = orders_a
            print(f"   ✅ Standardized Orders_A columns: {list(orders_a.columns)}")
        
        # Standardize Order_Details_A
        if 'Order_Details_A' in self.dataframes:
            details_a = self.dataframes['Order_Details_A'].copy()
            column_mapping = {
                'ID': 'OrderDetailID',
                'Order ID': 'OrderID',
                'Product ID': 'ProductID',
                'Quantity': 'Quantity',
                'Unit Price': 'UnitPrice',
                'Discount': 'Discount'
            }
            existing_columns = {k: v for k, v in column_mapping.items() if k in details_a.columns}
            details_a = details_a.rename(columns=existing_columns)
            self.dataframes['Order_Details_A'] = details_a
            print(f"   ✅ Standardized Order_Details_A columns: {list(details_a.columns)}")
        
        # Standardize Employees_A
        if 'Employees_A' in self.dataframes:
            employees_a = self.dataframes['Employees_A'].copy()
            column_mapping = {
                'ID': 'EmployeeID',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'E-mail Address': 'Email',
                'Job Title': 'Title',
                'Business Phone': 'HomePhone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }
            existing_columns = {k: v for k, v in column_mapping.items() if k in employees_a.columns}
            employees_a = employees_a.rename(columns=existing_columns)
            self.dataframes['Employees_A'] = employees_a
            print(f"   ✅ Standardized Employees_A columns: {list(employees_a.columns)}")

    def unify_customers(self):
        """UNION customers from both sources with Source B priority"""
        print("\n🏢 UNIFYING CUSTOMERS FROM BOTH SOURCES...")
        
        customers_a = self.dataframes.get('Customers_A', pd.DataFrame())
        customers_b = self.dataframes.get('Customers_B', pd.DataFrame())
        
        if customers_a.empty and customers_b.empty:
            print("❌ No customer data available")
            return pd.DataFrame()
        
        # Start with Source B customers
        unified_customers = customers_b.copy()
        
        # Add unique customers from Source A (not in Source B)
        if not customers_a.empty:
            # Check if CustomerID column exists in both
            if 'CustomerID' in customers_a.columns and 'CustomerID' in customers_b.columns:
                # Find customers in A that are not in B
                customers_a_unique = customers_a[~customers_a['CustomerID'].isin(customers_b['CustomerID'])]
                
                if not customers_a_unique.empty:
                    print(f"➕ Adding {len(customers_a_unique)} unique customers from Source A")
                    unified_customers = pd.concat([unified_customers, customers_a_unique], ignore_index=True)
            else:
                print("⚠️  CustomerID column not found in one of the datasets")
        
        print(f"✅ Unified Customers: {len(unified_customers)} total customers")
        print(f"   - From Source B: {len(customers_b)}")
        print(f"   - From Source A: {len(unified_customers) - len(customers_b)}")
        
        return unified_customers

    def unify_orders(self):
        """UNION orders from both sources with conflict resolution"""
        print("\n📦 UNIFYING ORDERS FROM BOTH SOURCES...")
        
        orders_a = self.dataframes.get('Orders_A', pd.DataFrame())
        orders_b = self.dataframes.get('Orders_B', pd.DataFrame())
        
        if orders_a.empty and orders_b.empty:
            print("❌ No order data available")
            return pd.DataFrame()
        
        # Start with Source B orders (complete set)
        unified_orders = orders_b.copy()
        
        # Add orders from Source A that are not in Source B
        if not orders_a.empty:
            # Check if OrderID column exists in both
            if 'OrderID' in orders_a.columns and 'OrderID' in orders_b.columns:
                # Find orders in A that are not in B
                orders_a_unique = orders_a[~orders_a['OrderID'].isin(orders_b['OrderID'])]
                
                if not orders_a_unique.empty:
                    print(f"➕ Adding {len(orders_a_unique)} unique orders from Source A")
                    unified_orders = pd.concat([unified_orders, orders_a_unique], ignore_index=True)
            else:
                print("⚠️  OrderID column not found in one of the datasets")
        
        print(f"✅ Unified Orders: {len(unified_orders)} total orders")
        print(f"   - From Source B: {len(orders_b)}")
        print(f"   - From Source A: {len(unified_orders) - len(orders_b)}")
        
        return unified_orders

    def unify_order_details(self):
        """UNION order details from both sources"""
        print("\n📋 UNIFYING ORDER DETAILS FROM BOTH SOURCES...")
        
        details_a = self.dataframes.get('Order_Details_A', pd.DataFrame())
        details_b = self.dataframes.get('Order_Details_B', pd.DataFrame())
        
        if details_a.empty and details_b.empty:
            print("❌ No order details available")
            return pd.DataFrame()
        
        # Start with Source B details
        unified_details = details_b.copy()
        
        # Add details from Source A
        if not details_a.empty:
            # Add all details from Source A (they might have different OrderIDs)
            print(f"➕ Adding {len(details_a)} order details from Source A")
            unified_details = pd.concat([unified_details, details_a], ignore_index=True)
        
        print(f"✅ Unified Order Details: {len(unified_details)} total details")
        print(f"   - From Source B: {len(details_b)}")
        print(f"   - From Source A: {len(unified_details) - len(details_b)}")
        
        return unified_details

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
        """Create customer dimension from UNIFIED data"""
        print("\n🏢 CREATING CUSTOMER DIMENSION FROM UNIFIED DATA...")
        
        # Get unified customers
        unified_customers = self.unify_customers()
        unified_orders = self.unify_orders()
        
        if unified_customers.empty:
            print("❌ No customer data available")
            return pd.DataFrame()
        
        # Calculate customer order statistics from unified orders
        if not unified_orders.empty:
            unified_orders['OrderDate'] = pd.to_datetime(unified_orders['OrderDate'], errors='coerce')
            valid_orders = unified_orders[unified_orders['OrderDate'].notna()]
            
            customer_stats = valid_orders.groupby('CustomerID').agg({
                'OrderID': 'count',
                'OrderDate': ['min', 'max']
            }).reset_index()
            
            customer_stats.columns = ['CustomerID', 'total_orders', 'first_order_date', 'last_order_date']
        else:
            customer_stats = pd.DataFrame({
                'CustomerID': [],
                'total_orders': [],
                'first_order_date': [],
                'last_order_date': []
            })
        
        # ✅ CUSTOMER DIMENSION - From unified data
        customer_dim = unified_customers[[
            'CustomerID', 'CompanyName', 'ContactName', 'ContactTitle',
            'City', 'Region', 'Country', 'PostalCode'
        ]].copy()
        
        # Keep important fields for analysis
        customer_dim['location'] = customer_dim['City'] + ', ' + customer_dim['Country']
        
        # Merge with order statistics
        if not customer_stats.empty:
            customer_dim = pd.merge(customer_dim, customer_stats, on='CustomerID', how='left')
        
        customer_dim['total_orders'] = customer_dim['total_orders'].fillna(0)
        
        # Convert dates for analysis
        customer_dim['first_order_date'] = pd.to_datetime(customer_dim['first_order_date'], errors='coerce')
        customer_dim['last_order_date'] = pd.to_datetime(customer_dim['last_order_date'], errors='coerce')
        
        self.dataframes['dim_customers'] = customer_dim
        print(f"✅ Customer Dimension: {len(customer_dim)} customers (UNIFIED)")
        return customer_dim
    
    def create_realistic_time_dimension(self, fact_orders_df):
        """Create REALISTIC time dimension based on ACTUAL order dates"""
        print(f"\n🕐 CREATING REALISTIC TIME DIMENSION FROM ORDER DATES...")
        
        if fact_orders_df.empty:
            print("❌ No order data available for time dimension")
            time_dimension = pd.DataFrame({
                'date_id': [],
                'full_date': [],
                'year': [],
                'quarter': [],
                'month': [],
                'month_name': [],
                'year_month': [],
                'quarter_name': [],
                'month_year': []
            })
            self.dataframes['dim_time'] = time_dimension
            return time_dimension
        
        # Extract year-month from order dates
        fact_orders_df['OrderDate'] = pd.to_datetime(fact_orders_df['OrderDate'], errors='coerce')
        
        # Get unique months from actual order dates
        valid_dates = fact_orders_df[fact_orders_df['OrderDate'].notna()].copy()
        
        if valid_dates.empty:
            print("❌ No valid order dates found")
            time_dimension = pd.DataFrame({
                'date_id': [],
                'full_date': [],
                'year': [],
                'quarter': [],
                'month': [],
                'month_name': [],
                'year_month': [],
                'quarter_name': [],
                'month_year': []
            })
            self.dataframes['dim_time'] = time_dimension
            return time_dimension
        
        # Create month-level dates (first day of each month)
        valid_dates['year_month'] = valid_dates['OrderDate'].dt.to_period('M')
        unique_months = valid_dates['year_month'].unique()
        
        # Convert periods to datetime (first day of month)
        month_dates = [period.to_timestamp() for period in unique_months]
        
        # ✅ TIME DIMENSION - Based on REAL order data
        time_dimension = pd.DataFrame({
            'date_id': [date.strftime('%Y%m') for date in month_dates],
            'full_date': month_dates,
            'year': [date.year for date in month_dates],
            'quarter': [date.quarter for date in month_dates],
            'month': [date.month for date in month_dates],
            'month_name': [date.strftime('%B') for date in month_dates],
            'year_month': [date.strftime('%Y-%m') for date in month_dates],
            'quarter_name': ['Q' + str(date.quarter) + ' ' + str(date.year) for date in month_dates],
            'month_year': [date.strftime('%b %Y') for date in month_dates]
        })
        
        # Sort by date (chronological order)
        time_dimension = time_dimension.sort_values('full_date').reset_index(drop=True)
        
        # Add sequence number
        time_dimension['order_sequence'] = range(1, len(time_dimension) + 1)
        
        # Add delivery statistics for each month
        monthly_stats = valid_dates.groupby('year_month').agg({
            'order_id': 'nunique',  # Number of orders
            'is_shipped': 'sum',    # Number of shipped orders
            'total_amount': 'sum'   # Total revenue
        }).reset_index()
        
        monthly_stats['year_month_str'] = monthly_stats['year_month'].dt.strftime('%Y-%m')
        
        # Merge with time dimension
        time_dimension = pd.merge(
            time_dimension,
            monthly_stats[['year_month_str', 'order_id', 'is_shipped', 'total_amount']],
            left_on='year_month',
            right_on='year_month_str',
            how='left'
        )
        
        # Rename columns for clarity
        time_dimension = time_dimension.rename(columns={
            'order_id': 'orders_count',
            'is_shipped': 'shipped_orders_count',
            'total_amount': 'monthly_revenue'
        })
        
        # Fill NaN values for months with no orders
        time_dimension['orders_count'] = time_dimension['orders_count'].fillna(0)
        time_dimension['shipped_orders_count'] = time_dimension['shipped_orders_count'].fillna(0)
        time_dimension['monthly_revenue'] = time_dimension['monthly_revenue'].fillna(0)
        time_dimension['delivery_rate'] = np.where(
            time_dimension['orders_count'] > 0,
            (time_dimension['shipped_orders_count'] / time_dimension['orders_count']) * 100,
            0  # No deliveries in this month
        )
        
        # Remove temporary column
        time_dimension = time_dimension.drop(columns=['year_month_str'])
        
        self.dataframes['dim_time'] = time_dimension
        
        print(f"✅ Realistic Time Dimension: {len(time_dimension)} unique months")
        print(f"   - Time range: {time_dimension['year_month'].min()} to {time_dimension['year_month'].max()}")
        print(f"   - Months with orders: {(time_dimension['orders_count'] > 0).sum()}")
        print(f"   - Months without orders: {(time_dimension['orders_count'] == 0).sum()}")
        
        return time_dimension
    
    def create_enhanced_fact_orders(self):
        """Create ENHANCED fact table from UNIFIED data"""
        print("\n📊 CREATING ENHANCED FACT ORDERS FROM UNIFIED DATA...")
        
        # Use UNIFIED data
        unified_orders = self.unify_orders()
        unified_order_details = self.unify_order_details()
        
        if unified_orders.empty:
            print("❌ No order data available")
            return pd.DataFrame(), 0, False
        
        # Check if product data is available for enhanced analysis
        has_product_data = 'Products_B' in self.dataframes and 'Categories_B' in self.dataframes
        
        if has_product_data and not unified_order_details.empty:
            print("🎯 PRODUCT DATA AVAILABLE - Creating enhanced fact table with product details")
            # Merge order details with product information
            order_details_enhanced = pd.merge(
                unified_order_details,
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
            if not unified_order_details.empty:
                product_order_totals = unified_order_details.groupby('OrderID').agg({
                    'UnitPrice': 'sum',
                    'Quantity': 'sum',
                    'Discount': 'mean'
                }).reset_index()
                
                product_order_totals.columns = ['OrderID', 'total_amount', 'total_quantity', 'avg_discount']
            else:
                product_order_totals = pd.DataFrame({
                    'OrderID': [],
                    'total_amount': [],
                    'total_quantity': [],
                    'avg_discount': []
                })
            
            product_order_totals['primary_category'] = 'Unknown'
            product_order_totals['unique_products_count'] = 1
        
        # ✅ ENHANCED FACT ORDERS - From unified data
        fact_orders = unified_orders[[
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
        success_rate = (shipped_count / total_orders) * 100 if total_orders > 0 else 0
        
        print(f"✅ Enhanced Fact Orders Table: {len(fact_orders_final)} orders (UNIFIED)")
        print(f"🎯 4D ANALYSIS READY: Product dimension integrated")
        print(f"   - Product categories: {fact_orders_final['primary_category'].nunique()} categories")
        print(f"   - Enhanced metrics: primary_category, unique_products_count")
        print(f"   - Success Rate: {success_rate:.1f}%")
        
        return fact_orders_final, total_orders, has_product_data

    def build_enhanced_warehouse(self):
        """Build ENHANCED data warehouse with UNIFIED data"""
        print("🏗️ BUILDING ENHANCED 4D DATA WAREHOUSE WITH UNIFIED DATA...")
        print("=" * 60)
        
        # Load both sources including PRODUCT DATA
        self.load_both_sources()
        
        # Standardize Source A column names to match Source B
        self.standardize_source_a_columns()
        
        # Create COMPLETE dimensions from UNIFIED data
        self.create_employee_dimension()  # Uses Source B (territories)
        self.create_customer_dimension()  # Uses UNIFIED customers
        self.create_product_dimension()   # Uses Source B (products)
        
        # Create ENHANCED fact table from UNIFIED data
        fact_orders, total_orders, has_product_data = self.create_enhanced_fact_orders()
        
        # Create REALISTIC time dimension based on actual order dates
        self.create_realistic_time_dimension(fact_orders)
        
        # Add time dimension ID to fact orders (year-month level)
        if not fact_orders.empty and 'dim_time' in self.dataframes:
            # Extract year-month from order date
            fact_orders['order_year_month'] = fact_orders['OrderDate'].dt.strftime('%Y-%m')
            
            # Merge with time dimension
            fact_orders_with_time = pd.merge(
                fact_orders,
                self.dataframes['dim_time'][['year_month', 'date_id']],
                left_on='order_year_month',
                right_on='year_month',
                how='left'
            )
            
            # Remove temporary columns
            fact_orders_with_time = fact_orders_with_time.drop(columns=['order_year_month', 'year_month'])
            
            self.dataframes['fact_orders'] = fact_orders_with_time
        
        # Save to SQLite database
        self.save_to_database()
        
        print("\n🎉 ENHANCED 4D DATA WAREHOUSE READY WITH UNIFIED DATA!")
        print("📊 4D ANALYSIS STRUCTURE:")
        print("   - dim_employees: Employee performance analysis")
        print("   - dim_customers: Customer reliability analysis (UNIFIED)") 
        print("   - dim_products: Product category/supplier analysis")
        print("   - dim_time: REALISTIC timeline based on actual orders")
        print("   - fact_orders: Enhanced with product categories (UNIFIED)")
        print(f"🚀 4D VISUALIZATION READY: {has_product_data}")
        print("\n🕐 TIME DIMENSION FEATURES:")
        print("   - Real dates from order data")
        print("   - Months can have 0, 1, or multiple orders")
        print("   - Monthly delivery statistics included")
        print("   - No redundancy - unique months only")
        
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
    print("🚀 STARTING UNIFIED DATA WAREHOUSE BUILD...")
    
    warehouse = UnifiedDataWarehouse()
    
    has_product_data = warehouse.build_enhanced_warehouse()
    
    print("\n" + "="*60)
    print("📊 UNIFIED DATA WAREHOUSE SUMMARY:")
    print("="*60)
  
    for table_name, df in warehouse.dataframes.items():
        if table_name.startswith(('dim_', 'fact_')):
            if table_name == 'dim_time':
                time_info = f" | Range: {df['year_month'].min()} to {df['year_month'].max()}"
                print(f"✅ {table_name}: {len(df)} months{time_info}")
            else:
                print(f"✅ {table_name}: {len(df)} rows")
    
    print(f"\n📦 PRODUCT DATA: {'✅ AVAILABLE' if has_product_data else '❌ NOT AVAILABLE'}")
    print("🎯 4D ANALYSIS: READY!")
    print("🚀 You can now run the dashboard with: python dashboard.py")