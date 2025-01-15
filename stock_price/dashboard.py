import psycopg2
import pandas as pd
import time
import streamlit as st

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="stock_data",
    user="postgres",
    password="password",
    host="localhost"
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id SERIAL PRIMARY KEY,
        symbol TEXT,
        price NUMERIC,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

# Dashboard
st.title("Real-Time Stock Price Tracker")

st.sidebar.header("Filter Options")
stock_symbol = st.sidebar.selectbox("Select Stock", ["AAPL", "GOOGL", "MSFT"])

while True:
    # Query data
    cursor.execute("""
        SELECT price, timestamp FROM prices WHERE symbol = %s ORDER BY timestamp DESC LIMIT 50
    """, (stock_symbol,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["price", "timestamp"])

    # Display data
    st.subheader(f"Latest Prices for {stock_symbol}")
    st.dataframe(df)

    st.subheader(f"Price Trends for {stock_symbol}")
    st.line_chart(df.set_index("timestamp")["price"])

    time.sleep(5)
