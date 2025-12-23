
import pandas as pd
from sqlalchemy import create_engine, text
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DB_URI = 'postgresql://postgres:your_password@127.0.0.1:5433/logistic_db'
engine = create_engine(DB_URI)


def load_logistic_data():
    """Extracts raw CSV data and loads it into a Dockerized PostgreSQL database."""
    with engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS logistics_master_table CASCADE;"))
        conn.commit()

    csv_files = {
        'orders': 'data/olist_orders_dataset.csv',
        'customer': 'data/olist_customers_dataset.csv',
        'items': 'data/olist_order_items_dataset.csv',
        'geo': 'data/olist_geolocation_dataset.csv',
        'sellers': 'data/olist_sellers_dataset.csv'
    }

    print("\n--- STARTING DATA LOAD ---")
    for table_name, file_path in csv_files.items():
        if not os.path.exists(file_path):
            print(f" File missing: {file_path}")
            continue

        print(f" Loading {table_name}...")
        df = pd.read_csv(file_path)

        if table_name == 'orders':

            date_cols = [c for c in df.columns if 'date' in c or 'timestamp' in c]
            for col in date_cols:

                df[col] = pd.to_datetime(df[col])
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f" Table '{table_name}' synchronized successfully.")

if __name__ == "__main__":
    print("=== Loading data ===")

    load_logistic_data()


def create_logistics_view():
    """Drops the old view and creates a new one with the updated structure."""

    drop_query = "DROP VIEW IF EXISTS logistics_master_table CASCADE;"

    create_query = """
    CREATE OR REPLACE VIEW logistics_master_table AS
    SELECT 
        o.order_id,
        o.order_purchase_timestamp,
        o.order_status,
        c.customer_city,
        c.customer_state,
        s.seller_city,
        s.seller_state,
        i.price,
        i.freight_value,
        EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)) AS delay_days
    FROM orders o
    JOIN items i ON o.order_id = i.order_id
    JOIN customer c ON o.customer_id = c.customer_id
    JOIN sellers s ON i.seller_id = s.seller_id;
    """

    with engine.connect() as conn:
        conn.execute(text(drop_query))
        conn.execute(text(create_query))
        conn.commit()
        print(" Analytics Layer (SQL View) has been reset and is ready.")


def analyze_state_delays():
    """Report: Average shipping delay per customer state."""
    query = """
    SELECT 
        c.customer_state, 
        ROUND(AVG(EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)))::numeric, 2) AS avg_delay
    FROM orders o
    JOIN customer c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered' 
      AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    GROUP BY c.customer_state
    ORDER BY avg_delay DESC
    LIMIT 10;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn)
        print("\n--- TOP 10 STATES WITH HIGHEST DELAYS ---")
        print(result)


def analyze_worst_sellers():
    """Report: Identify specific sellers causing the most delays."""
    query = """
    SELECT 
        i.seller_id, 
        COUNT(o.order_id) as delayed_orders_count,
        ROUND(AVG(EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)))::numeric, 2) AS avg_delay_days
    FROM orders o
    JOIN items i ON o.order_id = i.order_id
    WHERE o.order_status = 'delivered' 
      AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    GROUP BY i.seller_id
    HAVING COUNT(o.order_id) > 10
    ORDER BY avg_delay_days DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn)
        print("\n--- SELLER BLACKLIST (TOP 5 OFFENDERS) ---")
        print(result)


def analyze_financial_impact():
    """Report: Total money lost/stuck in delayed shipments by state."""
    query = """
    SELECT 
        c.customer_state,
        COUNT(o.order_id) as delayed_orders,
        ROUND(SUM(i.freight_value)::numeric, 2) as wasted_shipping_cost,
        ROUND(SUM(i.price)::numeric, 2) as impacted_revenue
    FROM orders o
    JOIN customer c ON o.customer_id = c.customer_id
    JOIN items i ON o.order_id = i.order_id
    WHERE o.order_status = 'delivered' 
      AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    GROUP BY c.customer_state
    ORDER BY wasted_shipping_cost DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn)
        print("\n--- FINANCIAL LOSS REPORT (BY STATE) ---")
        print(result)


def analyze_logistics_routes():
    """Report: Worst performing shipping routes (Origin -> Destination)."""
    query = """
    SELECT 
        s.seller_state as origin, 
        c.customer_state as destination, 
        COUNT(o.order_id) as parcel_count,
        ROUND(AVG(EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)))::numeric, 2) AS avg_delay
    FROM orders o
    JOIN items i ON o.order_id = i.order_id
    JOIN customer c ON o.customer_id = c.customer_id
    JOIN sellers s ON i.seller_id = s.seller_id
    WHERE o.order_status = 'delivered' 
      AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    GROUP BY s.seller_state, c.customer_state
    HAVING COUNT(o.order_id) > 20
    ORDER BY avg_delay DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn)
        print("\n--- WORST SHIPPING ROUTES (ORIGIN -> DEST) ---")
        print(result)


def analyze_city_bottlenecks():
    """Report: Specific seller cities causing delays using the Master View."""
    query = """
    SELECT 
        seller_city,
        COUNT(order_id) as total_orders,
        ROUND(AVG(delay_days)::numeric, 2) as avg_delay
    FROM logistics_master_table
    WHERE delay_days > 0
    GROUP BY seller_city
    HAVING COUNT(order_id) > 10
    ORDER BY avg_delay DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn)
        print("\n--- CITY BOTTLENECKS (SELLER LOCATIONS) ---")
        print(result)


def generate_visual_dashboard():
    """Generates a professional 2x2 Full-Page Interactive Dashboard using Plotly."""
    print("\n🎨 Generating Interactive Dashboard...")
    with engine.connect() as conn:
        # 1. Spatial Analysis
        df_states = pd.read_sql(text("""
            SELECT customer_state, ROUND(AVG(delay_days)::numeric, 2) as avg_delay
            FROM logistics_master_table WHERE delay_days > 0
            GROUP BY 1 ORDER BY avg_delay DESC;
        """), conn)

        df_size = pd.read_sql(text("""
            SELECT 
                CASE 
                    WHEN freight_value < 15 THEN 'Small/Light'
                    WHEN freight_value BETWEEN 15 AND 40 THEN 'Medium'
                    ELSE 'Large/Heavy'
                END AS package_type,
                ROUND(AVG(delay_days)::numeric, 2) AS avg_delay
            FROM logistics_master_table WHERE delay_days > 0
            GROUP BY 1 ORDER BY avg_delay DESC;
        """), conn)

        df_trend = pd.read_sql(text("""
            SELECT TO_CHAR(order_purchase_timestamp, 'YYYY-MM') as month,
                   ROUND(AVG(delay_days)::numeric, 2) as avg_delay
            FROM logistics_master_table 
            WHERE delay_days IS NOT NULL
            GROUP BY 1 ORDER BY 1;
        """), conn)

        df_finance = pd.read_sql(text("""
            SELECT 
                CASE WHEN delay_days > 0 THEN 'Delayed Orders' ELSE 'On-time Orders' END as status,
                SUM(price) as total_value
            FROM logistics_master_table
            GROUP BY 1;
        """), conn)

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Average Delivery Delay by State (Days)",
            "Logistics Bottleneck: Package Size vs Delay",
            "Historical Performance Trend (Monthly)",
            "Revenue Distribution: Delivery Status"
        ),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "scatter"}, {"type": "pie"}]],
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )

    fig.add_trace(go.Bar(x=df_states['customer_state'], y=df_states['avg_delay'],
                         marker=dict(color=df_states['avg_delay'], colorscale='Reds'), name='State'), 1, 1)
    fig.add_trace(go.Bar(x=df_size['package_type'], y=df_size['avg_delay'],
                         marker=dict(color=df_size['avg_delay'], colorscale='Viridis'), name='Size'), 1, 2)
    fig.add_trace(go.Scatter(x=df_trend['month'], y=df_trend['avg_delay'], mode='lines+markers',
                             line=dict(color='#00CC96', width=4), name='Trend'), 2, 1)
    fig.add_trace(go.Pie(labels=df_finance['status'], values=df_finance['total_value'], hole=.4,
                         marker=dict(colors=['#EF553B', '#636EFA'])), 2, 2)

    fig.update_layout(template='plotly_dark', title_text="Olist Logistics Intelligence Dashboard", title_x=0.5,
                      height=1000, showlegend=False)
    fig.show()


if __name__ == "__main__":
    print("=== OLIST DATA ENGINEERING PROJECT START ===")

    # load_logistic_data()
    create_logistics_view()
    analyze_state_delays()
    analyze_worst_sellers()
    analyze_financial_impact()
    analyze_logistics_routes()
    analyze_city_bottlenecks()
    generate_visual_dashboard()

