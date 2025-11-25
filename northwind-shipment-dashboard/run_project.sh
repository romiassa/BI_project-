#!/bin/bash

echo "🚀 Starting Northwind Shipment Status Dashboard Project..."

# Activate virtual environment
source venv/bin/activate

echo "📊 Step 1: Building Data Warehouse..."
python scripts/data_processing/transform_data.py

echo "🌐 Step 2: Starting Dashboard..."
python dashboard/app.py

echo "✅ Project is running! Open http://localhost:8050 in your browser"
