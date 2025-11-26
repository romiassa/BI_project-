import dash
from dash import dcc, html, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import os

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
app.title = "Northwind - Ultimate Shipment Analytics Dashboard"

# Load data with CORRECT PATH
def load_data():
    try:
        # CORRECT PATH - go up one level from dashboard folder
        conn = sqlite3.connect('../data/warehouse/northwind_bi.db')
        
        tables = {}
        table_names = ['fact_orders', 'dim_employees', 'dim_customers', 'dim_time', 'dim_products']
        
        for table in table_names:
            try:
                tables[table] = pd.read_sql(f'SELECT * FROM {table}', conn)
                print(f"✅ Loaded {table}: {len(tables[table])} rows")
            except Exception as e:
                print(f"❌ Error loading {table}: {e}")
                tables[table] = pd.DataFrame()
        
        conn.close()
        
        # Merge data
        if all(table in tables for table in ['fact_orders', 'dim_employees', 'dim_customers', 'dim_time']):
            try:
                merged_data = tables['fact_orders'].merge(
                    tables['dim_employees'], left_on='employee_id', right_on='EmployeeID', how='left'
                ).merge(
                    tables['dim_customers'], left_on='customer_id', right_on='CustomerID', how='left'
                ).merge(
                    tables['dim_time'], left_on='date_id', right_on='date_id', how='left'
                )
                
                # Add product data if available
                if not tables['dim_products'].empty and 'primary_category' in tables['fact_orders'].columns:
                    merged_data = merged_data.merge(
                        tables['dim_products'][['CategoryName', 'price_range']].drop_duplicates(),
                        left_on='primary_category', 
                        right_on='CategoryName', 
                        how='left',
                        suffixes=('', '_product')
                    )
                
                print("✅ Successfully merged all tables")
            except Exception as e:
                print(f"❌ Error merging tables: {e}")
                merged_data = tables['fact_orders'].copy()
        else:
            print("❌ Missing some tables, using fact_orders only")
            merged_data = tables['fact_orders'].copy()
        
        return merged_data, tables['fact_orders'], tables['dim_employees'], tables['dim_customers'], tables['dim_time']
    
    except Exception as e:
        print(f"❌ Critical error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

print("📊 Loading data...")
data, fact_orders, dim_employees, dim_customers, dim_time = load_data()

# Calculate KPIs
if not fact_orders.empty:
    total_orders = len(fact_orders)
    shipped_orders = fact_orders['is_shipped'].sum() if 'is_shipped' in fact_orders.columns else 0
    not_shipped_orders = total_orders - shipped_orders
    success_rate = (shipped_orders / total_orders * 100) if total_orders > 0 else 0
else:
    total_orders = shipped_orders = not_shipped_orders = success_rate = 0

print(f"✅ Data loaded: {total_orders} orders, {shipped_orders} shipped, {not_shipped_orders} not shipped")

# App layout
app.layout = dbc.Container([
    # Header
    dbc.Row([dbc.Col([html.H1("🚢 Northwind - Ultimate Shipment Analytics", className="text-center mb-4", style={'color': '#00ff00'})])]),
    
    # KPI Cards
    dbc.Row([
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("📦 Total Orders", className="card-title"), html.H3(f"{total_orders:,}", className="card-text text-info")])])], width=2),
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("✅ Livrées", className="card-title"), html.H3(f"{shipped_orders:,}", className="card-text text-success")])])], width=2),
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("❌ Non Livrées", className="card-title"), html.H3(f"{not_shipped_orders:,}", className="card-text text-danger")])])], width=2),
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("📊 Success Rate", className="card-title"), html.H3(f"{success_rate:.1f}%", className="card-text text-warning")])])], width=2),
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("👥 Employees", className="card-title"), html.H3(f"{len(dim_employees):,}", className="card-text text-info")])])], width=2),
        dbc.Col([dbc.Card([dbc.CardBody([html.H4("🏢 Customers", className="card-title"), html.H3(f"{len(dim_customers):,}", className="card-text text-success")])])], width=2),
    ], className="mb-4"),
    
    # Tabs
    dbc.Tabs([
        # TAB 1: 3D Analysis
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🎛️ 3D Analysis Filters", className="card-title"),
                    
                    html.Label("👥 Employee:"), 
                    dcc.Dropdown(
                        id='employee-filter-3d', 
                        options=[{'label': 'All Employees', 'value': 'all'}] + [{'label': f"{emp}", 'value': emp} for emp in sorted(data['full_name'].unique()) if pd.notna(emp)] if not data.empty else [],
                        value='all'
                    ),
                ])], color="secondary")], width=3),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🎯 3D Analysis: Time × Employee × Customer", className="card-title text-center"),
                    dcc.Graph(id='3d-plot', style={'height': '700px'}),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-3 text-warning"),
                        html.P("3D visualization shows order distribution. 🟢=Shipped, 🔴=Not Shipped. Each point represents an order!", className="text-light")
                    ])
                ])], color="dark")], width=9),
            ]),
        ], label='🎯 3D Analysis'),
        
        # TAB 2: 4D Product Analysis
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📦 4D Product Analysis", className="card-title"),
                    html.P("Product dimension analysis with categories and price ranges", className="text-light"),
                ])], color="secondary")], width=3),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📦 4D Analysis: Product × Time × Performance", className="card-title text-center"),
                    dcc.Graph(id='4d-product-plot', style={'height': '700px'}),
                ])], color="dark")], width=9),
            ]),
        ], label='📦 4D Product Analysis'),
    ]),
], fluid=True, style={'padding': '20px'})

# 3D Plot with GREEN and RED colors
@app.callback(Output('3d-plot', 'figure'), [Input('employee-filter-3d', 'value')])
def update_3d_plot(employee):
    if data.empty:
        fig = go.Figure()
        fig.update_layout(title="No data available", paper_bgcolor='black', font=dict(color='white'))
        return fig
    
    filtered_data = data.copy()
    if employee != 'all': 
        filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    try:
        fig = px.scatter_3d(
            filtered_data, 
            x='year_month', 
            y='full_name', 
            z='CompanyName',
            color='is_shipped',
            color_discrete_map={True: '#00ff00', False: '#ff0000'},  # GREEN for shipped, RED for not shipped
            size='total_amount',
            size_max=20,
            hover_data={
                'order_id': True, 
                'total_amount': ':.2f',
                'primary_category': True,
            },
            title="3D Analysis: Time × Employee × Customer - 🟢=Shipped 🔴=Not Shipped"
        )
        
        fig.update_layout(
            scene=dict(
                xaxis=dict(title='<b>TIME</b> 📅', backgroundcolor="black", gridcolor="gray"),
                yaxis=dict(title='<b>EMPLOYEE</b> 👥', backgroundcolor="black", gridcolor="gray"),
                zaxis=dict(title='<b>CUSTOMER</b> 🏢', backgroundcolor="black", gridcolor="gray"),
                bgcolor='black'
            ),
            paper_bgcolor='black',
            font=dict(color='white', size=12),
            height=700
        )
        return fig
    except Exception as e:
        print(f"❌ Error creating 3D plot: {e}")
        fig = go.Figure()
        fig.update_layout(title="Error creating 3D visualization", paper_bgcolor='black', font=dict(color='white'))
        return fig
    
colors = ['green' if shipped else 'red' for shipped in shipment_status_list]
ax.scatter(x_list, y_list, z_list, c=colors, marker='o')

# 4D Product Analysis
@app.callback(Output('4d-product-plot', 'figure'), [Input('employee-filter-3d', 'value')])
def update_4d_plot(employee):
    if data.empty:
        fig = go.Figure()
        fig.update_layout(title="No data available for 4D analysis", paper_bgcolor='black', font=dict(color='white'))
        return fig
    
    filtered_data = data.copy()
    if employee != 'all': 
        filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    try:
        # Use category as the 4th dimension (color)
        color_column = 'CategoryName' if 'CategoryName' in filtered_data.columns else 'primary_category'
        
        fig = px.scatter_3d(
            filtered_data, 
            x='year_month', 
            y='full_name', 
            z='CompanyName',
            color=color_column,
            size='total_amount',
            size_max=20,
            hover_data={
                'order_id': True, 
                'total_amount': ':.2f',
                'primary_category': True,
                'is_shipped': True
            },
            title="4D Analysis: Time × Employee × Customer × Product Category"
        )
        
        fig.update_layout(
            scene=dict(
                xaxis=dict(title='<b>TIME</b> 📅', backgroundcolor="black", gridcolor="gray"),
                yaxis=dict(title='<b>EMPLOYEE</b> 👥', backgroundcolor="black", gridcolor="gray"),
                zaxis=dict(title='<b>CUSTOMER</b> 🏢', backgroundcolor="black", gridcolor="gray"),
                bgcolor='black'
            ),
            paper_bgcolor='black',
            font=dict(color='white', size=12),
            height=700
        )
        return fig
    except Exception as e:
        print(f"❌ Error creating 4D plot: {e}")
        fig = go.Figure()
        fig.update_layout(title="Error creating 4D visualization", paper_bgcolor='black', font=dict(color='white'))
        return fig

if __name__ == '__main__':
    print("🚀 Starting FIXED DASHBOARD at: http://localhost:8050")
    print("✅ 3D Colors: GREEN for shipped, RED for not shipped!")
    print("✅ 4D Analysis: Product dimension integrated!")
    app.run(debug=True, host='0.0.0.0', port=8050)
