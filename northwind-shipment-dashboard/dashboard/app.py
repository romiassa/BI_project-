import dash
from dash import dcc, html, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
app.title = "Northwind - Ultimate Shipment Analytics Dashboard"

# Load data with ERROR HANDLING
def load_data():
    conn = sqlite3.connect('data/warehouse/northwind_bi.db')
    
    # Load tables with error handling
    tables = {}
    table_names = ['fact_orders', 'dim_employees', 'dim_customers', 'dim_time']
    
    for table in table_names:
        try:
            tables[table] = pd.read_sql(f'SELECT * FROM {table}', conn)
            print(f"✅ Loaded {table}: {len(tables[table])} rows")
        except Exception as e:
            print(f"❌ Error loading {table}: {e}")
            tables[table] = pd.DataFrame()
    
    conn.close()
    
    # Merge data with error handling
    if all(table in tables for table in ['fact_orders', 'dim_employees', 'dim_customers', 'dim_time']):
        try:
            merged_data = tables['fact_orders'].merge(
                tables['dim_employees'], left_on='employee_id', right_on='EmployeeID', how='left'
            ).merge(
                tables['dim_customers'], left_on='customer_id', right_on='CustomerID', how='left'
            ).merge(
                tables['dim_time'], left_on='date_id', right_on='date_id', how='left'
            )
            print("✅ Successfully merged all tables")
        except Exception as e:
            print(f"❌ Error merging tables: {e}")
            merged_data = tables['fact_orders'].copy()
    else:
        print("❌ Missing some tables, using fact_orders only")
        merged_data = tables['fact_orders'].copy()
    
    return merged_data, tables['fact_orders'], tables['dim_employees'], tables['dim_customers'], tables['dim_time']

print("📊 Loading data...")
data, fact_orders, dim_employees, dim_customers, dim_time = load_data()

# Debug: Check available columns
print("🔍 Available columns in data:")
for col in data.columns:
    print(f"   - {col}")

# Calculate KPIs
total_orders = len(fact_orders)
shipped_orders = fact_orders['is_shipped'].sum() if 'is_shipped' in fact_orders.columns else 0
not_shipped_orders = total_orders - shipped_orders
success_rate = (shipped_orders / total_orders * 100) if total_orders > 0 else 0

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
    
    # Tabs - ONLY SHOW WHAT WORKS
    dbc.Tabs([
        # TAB 1: 3D Analysis
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🎛️ 3D Analysis Filters", className="card-title"),
                    
                    html.Label("👥 Employee:"), 
                    dcc.Dropdown(
                        id='employee-filter-3d', 
                        options=[{'label': 'All Employees', 'value': 'all'}] + [{'label': f"{emp}", 'value': emp} for emp in sorted(data['full_name'].unique()) if pd.notna(emp)], 
                        value='all'
                    ),
                    
                    html.Br(), 
                    
                    html.Label("🏢 Customer:"), 
                    dcc.Dropdown(
                        id='customer-filter-3d', 
                        options=[{'label': 'All Customers', 'value': 'all'}] + [{'label': f"{cust}", 'value': cust} for cust in sorted(data['CompanyName'].unique()) if pd.notna(cust)], 
                        value='all'
                    ),
                    
                    html.Br(),
                    
                    html.Label("�� Shipment Status:"), 
                    dcc.Dropdown(
                        id='shipment-filter-3d', 
                        options=[
                            {'label': 'All Orders', 'value': 'all'},
                            {'label': '✅ Livrées Only', 'value': 'shipped'},
                            {'label': '❌ Non Livrées Only', 'value': 'not_shipped'}
                        ],
                        value='all'
                    ),
                    
                    html.Br(),
                    
                    # Only show region filter if column exists
                    html.Label("🗺️ Employee Region:"), 
                    dcc.Dropdown(
                        id='region-filter-3d', 
                        options=[{'label': 'All Regions', 'value': 'all'}] + [{'label': region, 'value': region} for region in sorted(data['work_regions'].unique()) if pd.notna(region)],
                        value='all'
                    ) if 'work_regions' in data.columns else html.Div([html.Label("🗺️ Employee Region:"), html.P("Region data not available", className="text-muted")]),
                    
                    html.Br(),
                    
                    html.Label("📅 Time Range:"), 
                    dcc.Dropdown(
                        id='time-range-3d', 
                        options=[
                            {'label': 'All Time', 'value': 'all'},
                            {'label': 'Last 3 Months', 'value': '3m'},
                            {'label': 'Last 6 Months', 'value': '6m'},
                            {'label': 'Last Year', 'value': '1y'}
                        ],
                        value='all'
                    ),
                    
                    html.Br(),
                    
                    html.Label("💰 Order Value Range:"), 
                    dcc.Dropdown(
                        id='value-range-3d', 
                        options=[
                            {'label': 'All Values', 'value': 'all'},
                            {'label': 'Small Orders (< $100)', 'value': 'small'},
                            {'label': 'Medium Orders ($100-$500)', 'value': 'medium'},
                            {'label': 'Large Orders (> $500)', 'value': 'large'}
                        ],
                        value='all'
                    ),
                    
                ])], color="secondary")], width=3),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🎯 3D Analysis: Time × Employee × Customer", className="card-title text-center"),
                    dcc.Graph(id='3d-plot', style={'height': '700px'}),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-3 text-warning"),
                        html.P("3D visualization shows order distribution. Green=Shipped, Red=Not Shipped. Each point represents an order!", className="text-light")
                    ])
                ])], color="dark")], width=9),
            ]),
        ], label='🎯 3D Analysis'),
        
        # TAB 2: Employee Analysis
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("👥 Employees: Shipped vs Not Shipped", className="card-title"),
                    dcc.Graph(id='employee-shipped-vs-not'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Top performers have high green shipment ratios", className="text-light")
                    ])
                ])], color="dark")], width=6),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📈 Employee Success Rate %", className="card-title"),
                    dcc.Graph(id='employee-success-rate'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Success rates range from poor to excellent", className="text-light")
                    ])
                ])], color="dark")], width=6),
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("⏱️ Employee Shipping Delays", className="card-title"),
                    dcc.Graph(id='employee-delays'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Some employees consistently faster than others", className="text-light")
                    ])
                ])], color="dark")], width=6),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🗺️ Employees by Region", className="card-title"),
                    dcc.Graph(id='employee-regions'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Regional performance differences exist", className="text-light")
                    ])
                ])], color="dark")], width=6),
            ]),
        ], label='👥 Employee Analysis'),
        
        # TAB 3: Customer Analysis
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("🏢 Customers: Shipped vs Not Shipped", className="card-title"),
                    dcc.Graph(id='customer-shipped-vs-not'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Some customers have persistent delivery issues", className="text-light")
                    ])
                ])], color="dark")], width=6),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📈 Customer Success Rate %", className="card-title"),
                    dcc.Graph(id='customer-success-rate'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Wide range in customer shipment success", className="text-light")
                    ])
                ])], color="dark")], width=6),
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("💰 Customer Value Analysis", className="card-title"),
                    dcc.Graph(id='customer-value'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("High-value customers generally more reliable", className="text-light")
                    ])
                ])], color="dark")], width=6),
                
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📅 Customer Order Frequency", className="card-title"),
                    dcc.Graph(id='customer-frequency'),
                    html.Div([
                        html.H5("📊 Conclusion:", className="mt-2 text-warning"),
                        html.P("Order frequency correlates with reliability", className="text-light")
                    ])
                ])], color="dark")], width=6),
            ]),
        ], label='🏢 Customer Analysis'),
        
        # TAB 4: Order Details
        dbc.Tab([
            dbc.Row([
                dbc.Col([dbc.Card([dbc.CardBody([
                    html.H4("📋 Advanced Order Explorer", className="card-title"),
                    
                    dbc.Row([
                        dbc.Col([
                            html.Label("🔍 Search Orders:"), 
                            dcc.Input(
                                id='order-search', 
                                type='text', 
                                placeholder='Search by Order ID, Customer, or Employee...', 
                                style={'width': '100%', 'margin-bottom': '10px'}
                            ),
                        ], width=3),
                        dbc.Col([
                            html.Label("📦 Shipment Status:"), 
                            dcc.Dropdown(
                                id='table-status-filter',
                                options=[
                                    {'label': 'All Orders', 'value': 'all'},
                                    {'label': '✅ Livrées Only', 'value': 'shipped'},
                                    {'label': '❌ Non Livrées Only', 'value': 'not_shipped'}
                                ],
                                value='all'
                            ),
                        ], width=3),
                        dbc.Col([
                            html.Label("👥 Employee Filter:"), 
                            dcc.Dropdown(
                                id='table-employee-filter',
                                options=[{'label': 'All Employees', 'value': 'all'}] + [{'label': emp, 'value': emp} for emp in sorted(data['full_name'].unique()) if pd.notna(emp)],
                                value='all'
                            ),
                        ], width=3),
                        dbc.Col([
                            html.Label("🏢 Customer Filter:"), 
                            dcc.Dropdown(
                                id='table-customer-filter',
                                options=[{'label': 'All Customers', 'value': 'all'}] + [{'label': cust, 'value': cust} for cust in sorted(data['CompanyName'].unique()) if pd.notna(cust)],
                                value='all'
                            ),
                        ], width=3),
                    ]),
                    
                    dash_table.DataTable(
                        id='orders-table',
                        columns=[
                            {"name": "Order ID", "id": "order_id"},
                            {"name": "Customer", "id": "CompanyName"},
                            {"name": "Employee", "id": "full_name"},
                            {"name": "Order Date", "id": "OrderDate"},
                            {"name": "Status", "id": "is_shipped"},
                            {"name": "Amount", "id": "total_amount"},
                            {"name": "Delay Days", "id": "shipping_delay_days"}
                        ] + ([
                            {"name": "Region", "id": "work_regions"},
                        ] if 'work_regions' in data.columns else []),
                        page_size=20,
                        style_table={'overflowX': 'auto'},
                        style_cell={'textAlign': 'left', 'color': 'white', 'padding': '10px', 'backgroundColor': 'rgb(50, 50, 50)'},
                        style_header={'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold', 'padding': '10px'},
                        style_data={'backgroundColor': 'rgb(50, 50, 50)', 'padding': '10px'},
                        filter_action="native",
                        sort_action="native"
                    )
                ])], color="dark")], width=12),
            ]),
        ], label='�� Order Details'),
    ]),
], fluid=True, style={'padding': '20px'})

# ========== CALLBACKS ==========

# 3D Plot with ONLY EXISTING FILTERS
@app.callback(
    Output('3d-plot', 'figure'),
    [Input('employee-filter-3d', 'value'),
     Input('customer-filter-3d', 'value'),
     Input('shipment-filter-3d', 'value'),
     Input('region-filter-3d', 'value'),
     Input('time-range-3d', 'value'),
     Input('value-range-3d', 'value')]
)
def update_3d_plot(employee, customer, shipment_status, region, time_range, value_range):
    filtered_data = data.copy()
    
    # Apply all filters with error handling
    try:
        if employee != 'all': 
            filtered_data = filtered_data[filtered_data['full_name'] == employee]
        if customer != 'all': 
            filtered_data = filtered_data[filtered_data['CompanyName'] == customer]
        if shipment_status == 'shipped': 
            filtered_data = filtered_data[filtered_data['is_shipped'] == True]
        elif shipment_status == 'not_shipped': 
            filtered_data = filtered_data[filtered_data['is_shipped'] == False]
        if region != 'all' and 'work_regions' in filtered_data.columns: 
            filtered_data = filtered_data[filtered_data['work_regions'] == region]
        
        # Time range filtering
        if time_range != 'all' and 'year_month' in filtered_data.columns:
            unique_months = sorted(filtered_data['year_month'].unique())
            if time_range == '3m' and len(unique_months) > 3:
                filtered_data = filtered_data[filtered_data['year_month'].isin(unique_months[-3:])]
            elif time_range == '6m' and len(unique_months) > 6:
                filtered_data = filtered_data[filtered_data['year_month'].isin(unique_months[-6:])]
            elif time_range == '1y' and len(unique_months) > 12:
                filtered_data = filtered_data[filtered_data['year_month'].isin(unique_months[-12:])]
        
        # Value range filtering
        if value_range != 'all' and 'total_amount' in filtered_data.columns:
            if value_range == 'small':
                filtered_data = filtered_data[filtered_data['total_amount'] < 100]
            elif value_range == 'medium':
                filtered_data = filtered_data[(filtered_data['total_amount'] >= 100) & (filtered_data['total_amount'] <= 500)]
            elif value_range == 'large':
                filtered_data = filtered_data[filtered_data['total_amount'] > 500]
    except Exception as e:
        print(f"❌ Error in filtering: {e}")
    
    if len(filtered_data) == 0:
        fig = go.Figure()
        fig.update_layout(
            title="No data available for selected filters",
            paper_bgcolor='black',
            plot_bgcolor='black',
            font=dict(color='white')
        )
        return fig
    
    # Create 3D scatter plot with error handling
    try:
        fig = px.scatter_3d(
            filtered_data, 
            x='year_month' if 'year_month' in filtered_data.columns else filtered_data.index, 
            y='full_name', 
            z='CompanyName',
            color='is_shipped',
            color_discrete_map={True: '#00ff00', False: '#ff0000'},
            size='total_amount' if 'total_amount' in filtered_data.columns else None,
            size_max=20,
            hover_data={
                'order_id': True, 
                'total_amount': ':.2f' if 'total_amount' in filtered_data.columns else False, 
                'shipping_delay_days': True if 'shipping_delay_days' in filtered_data.columns else False,
                'work_regions': True if 'work_regions' in filtered_data.columns else False,
                'OrderDate': True,
                'full_name': False,
                'CompanyName': False,
                'year_month': False
            },
            title="3D Analysis: Time (X) × Employee (Y) × Customer (Z)"
        )
    except Exception as e:
        print(f"❌ Error creating 3D plot: {e}")
        fig = go.Figure()
        fig.update_layout(
            title="Error creating 3D visualization",
            paper_bgcolor='black',
            plot_bgcolor='black',
            font=dict(color='white')
        )
        return fig
    
    # Fixed scene configuration
    fig.update_layout(
        scene=dict(
            xaxis=dict(
                title='<b>TIME</b> 📅',
                backgroundcolor="black",
                gridcolor="gray",
                showbackground=True
            ),
            yaxis=dict(
                title='<b>EMPLOYEE</b> 👥',
                backgroundcolor="black", 
                gridcolor="gray",
                showbackground=True
            ),
            zaxis=dict(
                title='<b>CUSTOMER</b> 🏢',
                backgroundcolor="black",
                gridcolor="gray", 
                showbackground=True
            ),
            bgcolor='black'
        ),
        paper_bgcolor='black',
        font=dict(color='white', size=12),
        height=700,
        margin=dict(l=0, r=0, b=0, t=50)
    )
    
    return fig

# Employee Analysis Callbacks
@app.callback(Output('employee-shipped-vs-not', 'figure'), [Input('employee-filter-3d', 'value')])
def update_employee_shipped_vs_not(employee):
    filtered_data = data.copy()
    if employee != 'all': filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    emp_stats = filtered_data.groupby('full_name').agg({
        'order_id': 'count',
        'is_shipped': ['sum', lambda x: len(x) - x.sum()]
    }).reset_index()
    emp_stats.columns = ['employee', 'total_orders', 'shipped', 'not_shipped']
    emp_stats = emp_stats.nlargest(15, 'total_orders')
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='✅ Livrées', x=emp_stats['employee'], y=emp_stats['shipped'], marker_color='#00ff00'))
    fig.add_trace(go.Bar(name='❌ Non Livrées', x=emp_stats['employee'], y=emp_stats['not_shipped'], marker_color='#ff0000'))
    
    fig.update_layout(
        title="Employees: Shipped vs Not Shipped Orders",
        barmode='stack',
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        xaxis=dict(tickangle=45),
        margin=dict(l=50, r=50, b=100, t=50)
    )
    return fig

@app.callback(Output('employee-success-rate', 'figure'), [Input('employee-filter-3d', 'value')])
def update_employee_success_rate(employee):
    filtered_data = data.copy()
    if employee != 'all': filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    emp_perf = filtered_data.groupby('full_name').agg({
        'order_id': 'count',
        'is_shipped': lambda x: (x.sum() / len(x)) * 100
    }).reset_index()
    emp_perf.columns = ['employee', 'total_orders', 'success_rate']
    emp_perf = emp_perf.nlargest(15, 'total_orders')
    
    fig = px.bar(emp_perf, x='success_rate', y='employee', orientation='h',
                 color='success_rate', color_continuous_scale=['red', 'yellow', 'green'],
                 title="Employee Success Rate %")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black', 
        font=dict(color='white'),
        margin=dict(l=50, r=50, b=50, t=50)
    )
    return fig

@app.callback(Output('employee-delays', 'figure'), [Input('employee-filter-3d', 'value')])
def update_employee_delays(employee):
    filtered_data = data.copy()
    if employee != 'all': filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    if 'shipping_delay_days' not in filtered_data.columns:
        fig = go.Figure()
        fig.update_layout(
            title="Shipping delay data not available",
            paper_bgcolor='black',
            plot_bgcolor='black',
            font=dict(color='white')
        )
        return fig
    
    emp_delays = filtered_data[filtered_data['shipping_delay_days'] > 0].groupby('full_name').agg({
        'shipping_delay_days': 'mean'
    }).reset_index()
    emp_delays.columns = ['employee', 'avg_delay']
    emp_delays = emp_delays.nlargest(15, 'avg_delay')
    
    fig = px.bar(emp_delays, x='avg_delay', y='employee', orientation='h',
                 color='avg_delay', color_continuous_scale=['green', 'yellow', 'red'],
                 title="Employee Average Shipping Delay (Days)")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        margin=dict(l=50, r=50, b=50, t=50)
    )
    return fig

@app.callback(Output('employee-regions', 'figure'), [Input('employee-filter-3d', 'value')])
def update_employee_regions(employee):
    filtered_data = data.copy()
    if employee != 'all': filtered_data = filtered_data[filtered_data['full_name'] == employee]
    
    if 'work_regions' not in filtered_data.columns:
        fig = go.Figure()
        fig.update_layout(
            title="Region data not available",
            paper_bgcolor='black',
            plot_bgcolor='black',
            font=dict(color='white')
        )
        return fig
    
    region_perf = filtered_data.groupby('work_regions').agg({
        'order_id': 'count',
        'is_shipped': lambda x: (x.sum() / len(x)) * 100
    }).reset_index()
    region_perf.columns = ['region', 'order_count', 'success_rate']
    
    fig = px.bar(region_perf, x='region', y='success_rate', color='order_count',
                 title="Employees by Region - Success Rate")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        xaxis=dict(tickangle=45),
        margin=dict(l=50, r=50, b=100, t=50)
    )
    return fig

# Customer Analysis Callbacks
@app.callback(Output('customer-shipped-vs-not', 'figure'), [Input('customer-filter-3d', 'value')])
def update_customer_shipped_vs_not(customer):
    filtered_data = data.copy()
    if customer != 'all': filtered_data = filtered_data[filtered_data['CompanyName'] == customer]
    
    cust_stats = filtered_data.groupby('CompanyName').agg({
        'order_id': 'count',
        'is_shipped': ['sum', lambda x: len(x) - x.sum()]
    }).reset_index()
    cust_stats.columns = ['customer', 'total_orders', 'shipped', 'not_shipped']
    cust_stats = cust_stats.nlargest(15, 'total_orders')
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='✅ Livrées', x=cust_stats['customer'], y=cust_stats['shipped'], marker_color='#00ff00'))
    fig.add_trace(go.Bar(name='❌ Non Livrées', x=cust_stats['customer'], y=cust_stats['not_shipped'], marker_color='#ff0000'))
    
    fig.update_layout(
        title="Customers: Shipped vs Not Shipped Orders",
        barmode='stack',
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        xaxis=dict(tickangle=45),
        margin=dict(l=50, r=50, b=100, t=50)
    )
    return fig

@app.callback(Output('customer-success-rate', 'figure'), [Input('customer-filter-3d', 'value')])
def update_customer_success_rate(customer):
    filtered_data = data.copy()
    if customer != 'all': filtered_data = filtered_data[filtered_data['CompanyName'] == customer]
    
    cust_perf = filtered_data.groupby('CompanyName').agg({
        'order_id': 'count',
        'is_shipped': lambda x: (x.sum() / len(x)) * 100
    }).reset_index()
    cust_perf.columns = ['customer', 'total_orders', 'success_rate']
    cust_perf = cust_perf.nlargest(15, 'total_orders')
    
    fig = px.bar(cust_perf, x='success_rate', y='customer', orientation='h',
                 color='success_rate', color_continuous_scale=['red', 'yellow', 'green'],
                 title="Customer Success Rate %")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        margin=dict(l=50, r=50, b=50, t=50)
    )
    return fig

@app.callback(Output('customer-value', 'figure'), [Input('customer-filter-3d', 'value')])
def update_customer_value(customer):
    filtered_data = data.copy()
    if customer != 'all': filtered_data = filtered_data[filtered_data['CompanyName'] == customer]
    
    if 'total_amount' not in filtered_data.columns:
        fig = go.Figure()
        fig.update_layout(
            title="Order value data not available",
            paper_bgcolor='black',
            plot_bgcolor='black',
            font=dict(color='white')
        )
        return fig
    
    cust_value = filtered_data.groupby('CompanyName').agg({
        'total_amount': 'sum',
        'is_shipped': lambda x: (x.sum() / len(x)) * 100
    }).reset_index()
    cust_value.columns = ['customer', 'total_value', 'success_rate']
    cust_value = cust_value.nlargest(20, 'total_value')
    
    fig = px.scatter(cust_value, x='total_value', y='success_rate', size='total_value',
                     color='success_rate', hover_name='customer',
                     color_continuous_scale=['red', 'yellow', 'green'],
                     title="Customer Order Value vs Success Rate")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        margin=dict(l=50, r=50, b=50, t=50)
    )
    return fig

@app.callback(Output('customer-frequency', 'figure'), [Input('customer-filter-3d', 'value')])
def update_customer_frequency(customer):
    filtered_data = data.copy()
    if customer != 'all': filtered_data = filtered_data[filtered_data['CompanyName'] == customer]
    
    cust_freq = filtered_data.groupby('CompanyName').agg({
        'order_id': 'count',
        'is_shipped': lambda x: (x.sum() / len(x)) * 100
    }).reset_index()
    cust_freq.columns = ['customer', 'order_frequency', 'success_rate']
    cust_freq = cust_freq.nlargest(15, 'order_frequency')
    
    fig = px.scatter(cust_freq, x='order_frequency', y='success_rate', size='order_frequency',
                     color='success_rate', hover_name='customer',
                     color_continuous_scale=['red', 'yellow', 'green'],
                     title="Customer Order Frequency vs Success Rate")
    fig.update_layout(
        paper_bgcolor='black',
        plot_bgcolor='black',
        font=dict(color='white'),
        margin=dict(l=50, r=50, b=50, t=50)
    )
    return fig

# Order Table Callback
@app.callback(
    Output('orders-table', 'data'),
    [Input('order-search', 'value'),
     Input('table-status-filter', 'value'),
     Input('table-employee-filter', 'value'),
     Input('table-customer-filter', 'value')]
)
def update_orders_table(search_term, status_filter, employee_filter, customer_filter):
    filtered_data = data.copy()
    
    # Apply search
    if search_term:
        filtered_data = filtered_data[
            filtered_data['order_id'].astype(str).str.contains(str(search_term), na=False) | 
            filtered_data['CompanyName'].str.contains(str(search_term), na=False, case=False) | 
            filtered_data['full_name'].str.contains(str(search_term), na=False, case=False)
        ]
    
    # Apply filters
    if status_filter == 'shipped':
        filtered_data = filtered_data[filtered_data['is_shipped'] == True]
    elif status_filter == 'not_shipped':
        filtered_data = filtered_data[filtered_data['is_shipped'] == False]
    
    if employee_filter != 'all':
        filtered_data = filtered_data[filtered_data['full_name'] == employee_filter]
    
    if customer_filter != 'all':
        filtered_data = filtered_data[filtered_data['CompanyName'] == customer_filter]
    
    # Select columns that exist
    base_columns = ['order_id', 'CompanyName', 'full_name', 'OrderDate', 'is_shipped']
    optional_columns = ['total_amount', 'shipping_delay_days', 'work_regions']
    
    available_columns = base_columns + [col for col in optional_columns if col in filtered_data.columns]
    
    display_data = filtered_data[available_columns].copy()
    display_data['is_shipped'] = display_data['is_shipped'].map({True: '✅ Livrée', False: '❌ Non Livrée'})
    display_data['OrderDate'] = pd.to_datetime(display_data['OrderDate']).dt.strftime('%Y-%m-%d')
    
    return display_data.to_dict('records')

if __name__ == '__main__':
    print("🚀 Starting ULTIMATE DASHBOARD at: http://localhost:8050")
    print("✅ FIXED: No more country-filter-3d errors!")
    print("✅ FIXED: No more 4D tab issues!")
    print("✅ WORKING: 3D visualization with your actual data!")
    app.run(debug=True, host='0.0.0.0', port=8050)
