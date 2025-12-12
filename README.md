# 🚢 Northwind Shipment Analytics Dashboard

<div align="center">

![Dashboard Preview](https://img.shields.io/badge/Dashboard-Interactive-blue)
![Python](https://img.shields.io/badge/Python-3.8+-green)
![Dash](https://img.shields.io/badge/Plotly%20Dash-Framework-orange)
![License](https://img.shields.io/badge/License-MIT-yellow)

**Advanced 3D & 4D Business Intelligence Platform**

[Features](#-features) • [Installation](#-installation) • [Usage](#-usage) • [Screenshots](#-screenshots)

</div>

## 📋 Overview

Northwind Shipment Analytics Dashboard is a comprehensive Business Intelligence platform that transforms shipment data into actionable insights through advanced 3D and 4D visualizations. Built for the Northwind trading database, this dashboard enables logistics managers, operations teams, and executives to monitor, analyze, and optimize their global shipment operations in real-time.

## ✨ Features

### 🎯 **Multi-Dimensional Analytics**
- **3D Analysis**: Time × Employee × Customer visualization
- **4D Product Analysis**: Time × Employee × Customer × Product
- **Interactive Filters**: Region, product category, price range
- **Real-time KPIs**: Success rates, delays, order volumes

### 📊 **Advanced Visualizations**
- **3D Scatter Plots** with color-coded delivery status (🟢=Delivered, 🔴=Not Delivered)
- **Product Category Analysis** with price segmentation
- **Employee Performance Metrics** across regions
- **Customer Value Analysis** with success correlation

### 🔍 **Smart Filtering System**
- **Employee/Customer** segmentation
- **Time Range** selection (Last 3M, 6M, Year)
- **Order Value** filtering (Small/Medium/Large orders)
- **Delivery Status** filtering (Shipped/Pending)

### 📱 **Dashboard Components**
- **6 KPI Cards** with live metrics
- **Interactive Data Tables** with search and sorting
- **Dark Theme UI** optimized for analytics
- **Mobile-responsive** design

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Frontend** | Plotly Dash, Dash Bootstrap | Interactive visualizations & UI |
| **Backend** | Python 3.8+ | Data processing & API |
| **Database** | SQLite (Northwind BI) | Data storage & retrieval |
| **Visualization** | Plotly, Plotly 3D | Charts & 3D/4D plots |
| **Styling** | CSS3, Darkly Theme | Dark mode UI |

##📦 Installation

### Prerequisites
- Python 3.8 or higher
- Git
- SQLite (included with Python)

### Quick Setup
```bash
# Clone repository
git clone https://github.com/romiassa/BI_project-.git
cd BI_project-

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Linux/Mac:
source venv/bin/activate
# Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Ensure database exists
# The dashboard expects: data/warehouse/northwind_bi.db

# Run dashboard
python app.py

# Open browser: http://localhost:8050
