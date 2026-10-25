import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
import time
import threading
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Global variables
latest_rates = {}
data_lock = threading.Lock()
update_count = 0

# Page configuration
st.set_page_config(
    page_title="Currency Rates Dashboard",
    page_icon="💱",
    layout="wide"
)

# CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #F8F9FA;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #3B82F6;
        margin-bottom: 1rem;
    }
    .currency-card {
        background-color: white;
        padding: 0.75rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Function to consume Kafka messages
def consume_kafka_messages():
    global latest_rates, update_count
    
    KAFKA_TOPIC = "currency-rates"
    KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='streamlit-consumer'
    )
    
    for message in consumer:
        with data_lock:
            latest_rates = message.value
            update_count += 1
            print(f"Data updated: {len(latest_rates)} currencies")

# Start Kafka consumer in background thread
def start_kafka_consumer():
    thread = threading.Thread(target=consume_kafka_messages, daemon=True)
    thread.start()

# Initialize Kafka consumer
start_kafka_consumer()

# Dashboard Header
st.markdown('<h1 class="main-header">💱 Real-Time Currency Rates Dashboard</h1>', unsafe_allow_html=True)

# Sidebar for controls
with st.sidebar:
    st.header("Dashboard Controls")
    
    # Refresh interval
    refresh_interval = st.slider(
        "Auto-refresh interval (seconds)",
        min_value=5,
        max_value=60,
        value=10,
        step=5
    )
    
    # Currency selection
    st.subheader("Filter Currencies")
    show_all = st.checkbox("Show All Currencies", value=True)
    
    if not show_all:
        top_currencies = st.multiselect(
            "Select Currencies to Display",
            options=['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'AED', 'SAR', 'EGP'],
            default=['USD', 'EUR', 'GBP', 'JPY', 'AED']
        )
    else:
        top_currencies = None
    
    # Display options
    st.subheader("Display Options")
    show_chart = st.checkbox("Show Chart", value=True)
    show_table = st.checkbox("Show Data Table", value=True)
    
    # Stats
    st.markdown("---")
    st.subheader("System Information")
    st.write(f"**Kafka Server:** {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    st.write(f"**Update Count:** {update_count}")

# Main content area
col1, col2, col3 = st.columns(3)

with col1:
    with data_lock:
        currency_count = len(latest_rates) if latest_rates else 0
    st.metric("Total Currencies", currency_count)

with col2:
    st.metric("Last Update", time.strftime("%H:%M:%S"))

with col3:
    st.metric("Data Stream", "Active" if latest_rates else "Waiting")

# Display warning if no data
if not latest_rates:
    st.warning("Waiting for data from Kafka producer... Make sure producer.py is running.")
    st.info("Start the producer in a separate terminal: python producer.py")
    
    # Display sample data for demo
    st.subheader("Sample Data (for demonstration)")
    sample_data = {
        'USD': 1.0,
        'EUR': 0.92,
        'GBP': 0.79,
        'JPY': 148.5,
        'AED': 3.67
    }
    
    for currency, rate in sample_data.items():
        cols = st.columns([1, 2])
        cols[0].write(f"**{currency}**")
        cols[1].write(f"`{rate:.4f}`")
    
    st.stop()

# Main data display
st.markdown("---")

# Filter currencies if needed
with data_lock:
    if top_currencies and not show_all:
        display_rates = {k: v for k, v in latest_rates.items() if k in top_currencies}
    else:
        display_rates = latest_rates.copy()

# Convert to DataFrame for easier manipulation
df = pd.DataFrame(list(display_rates.items()), columns=['Currency', 'Rate'])

# Display in two columns
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Currency Rates")
    
    # Search box
    search_term = st.text_input("Search currency:", placeholder="Type currency code...")
    
    # Filter by search
    if search_term:
        filtered_df = df[df['Currency'].str.contains(search_term, case=False)]
    else:
        filtered_df = df
    
    # Display currency cards
    for _, row in filtered_df.head(20).iterrows():
        with st.container():
            cols = st.columns([1, 3, 2])
            cols[0].metric("", row['Currency'])
            cols[1].write(f"1 USD =")
            cols[2].write(f"**{row['Rate']:.6f}**")
            st.markdown("---")

with col_right:
    if show_chart:
        st.subheader("Exchange Rate Visualization")
        
        # Prepare data for chart
        chart_df = df.sort_values('Rate', ascending=False).head(15)
        
        # Create bar chart
        fig = px.bar(
            chart_df,
            x='Currency',
            y='Rate',
            title='Top 15 Currency Exchange Rates vs USD',
            labels={'Rate': 'Exchange Rate', 'Currency': 'Currency Code'},
            color='Rate',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            height=400,
            xaxis_tickangle=-45,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    if show_table:
        st.subheader("Data Table")
        
        # Display data table
        st.dataframe(
            df.sort_values('Currency'),
            column_config={
                "Currency": st.column_config.TextColumn("Currency Code", width="medium"),
                "Rate": st.column_config.NumberColumn("Exchange Rate", format="%.6f")
            },
            hide_index=True,
            use_container_width=True
        )

# Bottom statistics
st.markdown("---")
col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)

with data_lock:
    if latest_rates:
        rates_list = list(latest_rates.values())
        
        with col_stats1:
            st.metric("Highest Rate", f"{max(rates_list):.4f}")
        
        with col_stats2:
            st.metric("Lowest Rate", f"{min(rates_list):.4f}")
        
        with col_stats3:
            avg_rate = sum(rates_list) / len(rates_list)
            st.metric("Average Rate", f"{avg_rate:.4f}")
        
        with col_stats4:
            st.metric("Data Points", len(latest_rates))

# Auto-refresh
st.markdown("---")
if st.button("Manual Refresh"):
    st.rerun()

# Add auto-refresh notice
st.caption(f"Auto-refreshing every {refresh_interval} seconds. Last update: {time.strftime('%H:%M:%S')}")

# Auto-refresh logic
time.sleep(refresh_interval)
st.rerun()