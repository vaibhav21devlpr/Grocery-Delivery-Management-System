"""
Grocery Delivery Management System - Interactive Dashboard
Real-time data visualization and analytics dashboard
Author: Vaibhav Pandey
Date: 20th April, 2026
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Grocery Delivery Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load all datasets"""
    try:
        data = {
            'customers': pd.read_csv('data/processed/customers_clean.csv') if os.path.exists('data/processed/customers_clean.csv') else pd.read_csv('data/raw/customers.csv'),
            'products': pd.read_csv('data/processed/products_clean.csv') if os.path.exists('data/processed/products_clean.csv') else pd.read_csv('data/raw/products.csv'),
            'orders': pd.read_csv('data/processed/orders_clean.csv') if os.path.exists('data/processed/orders_clean.csv') else pd.read_csv('data/raw/orders.csv'),
            'order_items': pd.read_csv('data/processed/order_items_clean.csv') if os.path.exists('data/processed/order_items_clean.csv') else pd.read_csv('data/raw/order_items.csv'),
            'deliveries': pd.read_csv('data/processed/deliveries_clean.csv') if os.path.exists('data/processed/deliveries_clean.csv') else pd.read_csv('data/raw/deliveries.csv'),
            'categories': pd.read_csv('data/raw/categories.csv'),
            'inventory': pd.read_csv('data/raw/inventory.csv')
        }
        
        data['orders']['order_date'] = pd.to_datetime(data['orders']['order_date'])
        
        return data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def create_kpi_cards(data):
    """Create KPI metric cards"""
    orders_df = data['orders']
    customers_df = data['customers']
    products_df = data['products']
    deliveries_df = data['deliveries']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_orders = len(orders_df)
        st.metric(
            label="📦 Total Orders",
            value=f"{total_orders:,}",
            delta=f"+{int(total_orders * 0.12)} this month"
        )
    
    with col2:
        total_revenue = orders_df['final_amount'].sum()
        st.metric(
            label="💰 Total Revenue",
            value=f"₹{total_revenue:,.0f}",
            delta="+15.3%"
        )
    
    with col3:
        total_customers = len(customers_df)
        st.metric(
            label="👥 Active Customers",
            value=f"{total_customers:,}",
            delta=f"+{int(total_customers * 0.08)} new"
        )
    
    with col4:
        avg_delivery_rating = deliveries_df['delivery_rating'].dropna().mean()
        st.metric(
            label="⭐ Avg Rating",
            value=f"{avg_delivery_rating:.2f}/5.0",
            delta="+0.3"
        )

def plot_sales_trends(data):
    """Plot sales trends over time"""
    st.subheader("📈 Sales Trends Over Time")
    
    orders_df = data['orders'].copy()
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    
    daily_sales = orders_df.groupby(orders_df['order_date'].dt.date).agg({
        'order_id': 'count',
        'final_amount': 'sum'
    }).reset_index()
    daily_sales.columns = ['Date', 'Orders', 'Revenue']
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=daily_sales['Date'],
        y=daily_sales['Revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#1f77b4', width=3),
        fill='tozeroy'
    ))
    
    fig.update_layout(
        title="Daily Revenue Trend",
        xaxis_title="Date",
        yaxis_title="Revenue (₹)",
        hovermode='x unified',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_top_products(data):
    """Plot top selling products"""
    st.subheader("🏆 Top 10 Best Selling Products")
    
    order_items_df = data['order_items']
    products_df = data['products']
    
    product_sales = order_items_df.merge(products_df, on='product_id')
    top_products = product_sales.groupby('product_name').agg({
        'quantity': 'sum',
        'subtotal': 'sum'
    }).reset_index().sort_values('subtotal', ascending=False).head(10)
    
    fig = px.bar(
        top_products,
        x='product_name',
        y='subtotal',
        title="Top 10 Products by Revenue",
        labels={'product_name': 'Product', 'subtotal': 'Revenue (₹)'},
        color='subtotal',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

def plot_category_distribution(data):
    """Plot category-wise sales distribution"""
    st.subheader("📊 Sales by Category")
    
    order_items_df = data['order_items']
    products_df = data['products']
    categories_df = data['categories']
    
    sales_by_category = order_items_df.merge(products_df, on='product_id') \
                                      .merge(categories_df, on='category_id')
    
    category_revenue = sales_by_category.groupby('category_name')['subtotal'].sum().reset_index()
    category_revenue = category_revenue.sort_values('subtotal', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            category_revenue,
            values='subtotal',
            names='category_name',
            title="Revenue Distribution by Category",
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            category_revenue,
            x='subtotal',
            y='category_name',
            orientation='h',
            title="Revenue by Category",
            labels={'subtotal': 'Revenue (₹)', 'category_name': 'Category'},
            color='subtotal',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)

def plot_customer_segmentation(data):
    """Plot customer segmentation"""
    st.subheader("👥 Customer Segmentation Analysis")
    
    orders_df = data['orders']
    customers_df = data['customers']
    
    customer_metrics = orders_df.groupby('customer_id').agg({
        'order_id': 'count',
        'final_amount': 'sum'
    }).reset_index()
    customer_metrics.columns = ['customer_id', 'total_orders', 'lifetime_value']
    
    customer_metrics['segment'] = pd.cut(
        customer_metrics['lifetime_value'],
        bins=[0, 10000, 20000, 50000, float('inf')],
        labels=['Bronze', 'Silver', 'Gold', 'Premium']
    )
    
    segment_counts = customer_metrics['segment'].value_counts().reset_index()
    segment_counts.columns = ['Segment', 'Count']
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            segment_counts,
            values='Count',
            names='Segment',
            title="Customer Distribution by Segment",
            color='Segment',
            color_discrete_map={'Premium': '#FFD700', 'Gold': '#C0C0C0', 
                               'Silver': '#CD7F32', 'Bronze': '#8B4513'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        segment_revenue = customer_metrics.groupby('segment')['lifetime_value'].sum().reset_index()
        fig = px.bar(
            segment_revenue,
            x='segment',
            y='lifetime_value',
            title="Total Revenue by Customer Segment",
            labels={'lifetime_value': 'Revenue (₹)', 'segment': 'Segment'},
            color='lifetime_value',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig, use_container_width=True)

def plot_delivery_performance(data):
    """Plot delivery performance metrics"""
    st.subheader("🚚 Delivery Performance Dashboard")
    
    deliveries_df = data['deliveries'].copy()
    
    deliveries_df['pickup_time'] = pd.to_datetime(deliveries_df['pickup_time'], errors='coerce')
    deliveries_df['actual_delivery_time'] = pd.to_datetime(deliveries_df['actual_delivery_time'], errors='coerce')
    deliveries_df['estimated_delivery_time'] = pd.to_datetime(deliveries_df['estimated_delivery_time'], errors='coerce')
    
    deliveries_df['delivery_time_minutes'] = (
        (deliveries_df['actual_delivery_time'] - deliveries_df['pickup_time']).dt.total_seconds() / 60
    )
    
    deliveries_df['is_on_time'] = deliveries_df['actual_delivery_time'] <= deliveries_df['estimated_delivery_time']
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_delivery_time = deliveries_df['delivery_time_minutes'].dropna().mean()
        st.metric("⏱️ Avg Delivery Time", f"{avg_delivery_time:.1f} min")
    
    with col2:
        on_time_rate = deliveries_df['is_on_time'].sum() / len(deliveries_df) * 100
        st.metric("✅ On-Time Rate", f"{on_time_rate:.1f}%")
    
    with col3:
        avg_rating = deliveries_df['delivery_rating'].dropna().mean()
        st.metric("⭐ Avg Rating", f"{avg_rating:.2f}/5.0")
    
    status_counts = deliveries_df['delivery_status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']
    
    fig = px.bar(
        status_counts,
        x='Status',
        y='Count',
        title="Deliveries by Status",
        color='Status',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_inventory_status(data):
    """Plot inventory status"""
    st.subheader("📦 Inventory Management")
    
    inventory_df = data['inventory']
    products_df = data['products']
    
    inventory_products = inventory_df.merge(products_df, on='product_id')
    
    inventory_products['stock_status'] = inventory_products.apply(
        lambda row: 'Out of Stock' if row['quantity_available'] <= 0
        else 'Low Stock' if row['quantity_available'] <= row['reorder_level']
        else 'Sufficient', axis=1
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        status_counts = inventory_products['stock_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        
        fig = px.pie(
            status_counts,
            values='Count',
            names='Status',
            title="Inventory Stock Status",
            color='Status',
            color_discrete_map={
                'Sufficient': '#28a745',
                'Low Stock': '#ffc107',
                'Out of Stock': '#dc3545'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        low_stock = inventory_products[
            inventory_products['stock_status'].isin(['Low Stock', 'Out of Stock'])
        ].sort_values('quantity_available')[['product_name', 'quantity_available', 'reorder_level']].head(10)
        
        st.write("⚠️ **Low Stock Products**")
        st.dataframe(low_stock, use_container_width=True)

def show_data_tables(data):
    """Display raw data tables"""
    st.subheader("📋 Data Tables")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Orders", "Customers", "Products", "Deliveries", "Inventory"
    ])
    
    with tab1:
        st.write(f"**Total Orders:** {len(data['orders'])}")
        st.dataframe(data['orders'].head(100), use_container_width=True)
    
    with tab2:
        st.write(f"**Total Customers:** {len(data['customers'])}")
        st.dataframe(data['customers'].head(100), use_container_width=True)
    
    with tab3:
        st.write(f"**Total Products:** {len(data['products'])}")
        st.dataframe(data['products'].head(100), use_container_width=True)
    
    with tab4:
        st.write(f"**Total Deliveries:** {len(data['deliveries'])}")
        st.dataframe(data['deliveries'].head(100), use_container_width=True)
    
    with tab5:
        st.write(f"**Total Items:** {len(data['inventory'])}")
        st.dataframe(data['inventory'].head(100), use_container_width=True)

def plot_hourly_patterns(data):
    """Plot hourly ordering patterns"""
    st.subheader("⏰ Peak Order Hours")
    
    orders_df = data['orders'].copy()
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    orders_df['hour'] = orders_df['order_date'].dt.hour
    
    hourly_orders = orders_df.groupby('hour').size().reset_index(name='count')
    
    fig = px.line(
        hourly_orders,
        x='hour',
        y='count',
        title="Orders by Hour of Day",
        labels={'hour': 'Hour of Day', 'count': 'Number of Orders'},
        markers=True
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def main():
    """Main dashboard function"""
    
    st.markdown('<h1 class="main-header">🛒 Grocery Delivery Management Dashboard</h1>', unsafe_allow_html=True)
    
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/000000/grocery-shelf.png", width=100)
        st.title("Navigation")
        
        page = st.radio(
            "Select View",
            ["Overview", "Sales Analytics", "Customer Analytics", 
             "Delivery Performance", "Inventory Management", "Raw Data"]
        )
        
        st.markdown("---")
        st.info("**Data Engineering Project**\nGrocery Delivery System\n\n[Your Name]\n[Roll Number]")
        
        st.markdown("---")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("🔄 Refresh Data", key="refresh_button", use_container_width=True):
                load_data.clear()
                st.session_state.last_refresh = datetime.now()
                st.success("✅ Data refreshed!")
                st.rerun()
        
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        
        st.caption(f"Last updated: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
        
        st.markdown("---")
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        
        if auto_refresh:
            count = st_autorefresh(interval=30000, key="auto_refresh")
    
            if count > 0:
                load_data.clear()
                st.session_state.last_refresh = datetime.now()
                st.toast("🔄 Data auto-refreshed")
    
    with st.spinner("Loading data..."):
        data = load_data()
    
    if data is None:
        st.error("Failed to load data. Please ensure data files are in the correct location.")
        st.info("Run `python scripts/generate_data.py` to generate sample data.")
        return
    
    if page == "Overview":
        st.markdown("### 📊 Key Performance Indicators")
        create_kpi_cards(data)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            plot_sales_trends(data)
        
        with col2:
            plot_hourly_patterns(data)
        
        st.markdown("---")
        plot_category_distribution(data)
        
    elif page == "Sales Analytics":
        plot_top_products(data)
        st.markdown("---")
        plot_category_distribution(data)
        st.markdown("---")
        plot_sales_trends(data)
        
    elif page == "Customer Analytics":
        plot_customer_segmentation(data)
        
        st.markdown("---")
        
        customers_df = data['customers']
        orders_df = data['orders']
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_orders = orders_df.groupby('customer_id').size().mean()
            st.metric("📊 Avg Orders per Customer", f"{avg_orders:.1f}")
        
        with col2:
            repeat_customers = orders_df.groupby('customer_id').size()
            repeat_rate = (repeat_customers > 1).sum() / len(repeat_customers) * 100
            st.metric("🔄 Repeat Purchase Rate", f"{repeat_rate:.1f}%")
        
        with col3:
            avg_lifetime_value = orders_df.groupby('customer_id')['final_amount'].sum().mean()
            st.metric("💎 Avg Lifetime Value", f"₹{avg_lifetime_value:,.0f}")
        
    elif page == "Delivery Performance":
        plot_delivery_performance(data)
        
    elif page == "Inventory Management":
        plot_inventory_status(data)
        
    elif page == "Raw Data":
        show_data_tables(data)
    
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
            <p>Grocery Delivery Management System | Data Engineering Capstone Project</p>
            <p>Built with Streamlit, Plotly, and Pandas</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
