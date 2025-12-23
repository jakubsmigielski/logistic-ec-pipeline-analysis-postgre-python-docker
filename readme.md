## 🚚   Logistics E-C Pipeline - Python - Postgre - Docker

Basic Data Engineering project that analyzes logistics performance for Brazilian e-commerce.
This project implements an automated ETL pipeline, creates a specialized Data Warehouse layer (SQL Views), and delivers actionable business insights via an interactive **Dashboard**.

### Project Overview & Dashboard
The system identifies logistics bottlenecks, calculates financial losses due to delays, and visualizes shipping trends across 27 states.

![Dashboard Preview](plots/plot1.png)



![Dashboard Preview](plots/plot2.png)


* Database: PostgreSQL 15 
* ETL & Data Manipulation: Pandas, SQLAlchemy
* Visualization: Plotly 
* Containerization: Docker & Docker Compose

##  Key Features


1.  **Automated ETL Pipeline**:
    * Ingests 5 raw CSV datasets (~100k records).
    * Performs data cleaning and type casting (Temporal data handling).
    * Loads data into a normalized PostgreSQL schema.

2.  **Analytical Layer (SQL Views)**:
    * Instead of complex joins in Python, a persistent logistics_master_table View was created in the database.
    * This acts as a "Single Source of Truth" for all downstream analytics.

3.  **Business Intelligence**:
    * **Route Analysis**: Identifies worst-performing origin-destination pairs.
    * **Financial Impact**: Calculates revenue at risk due to shipping delays.
    * **Interactive Dashboard**: A 2x2 Plotly grid showing spatial, temporal, and financial metrics.

### Data downloaded: [Kaggle: Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Data Setup

The project relies on 5 CSV files from the Olist dataset (are too big to upload). You have two ways to get them:

### Option A: Automated 
Run the included helper script. It will **automatically create the `data/` folder** and download the necessary files for you.
(Requires Kaggle API key).

### Option B: Manual 
Create a folder named `data` in the main project directory.

Download the [Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 

Unzip the files and place the 5 required CSVs inside the data folder.

```text
Project/
│
├── data/                       
│   ├── olist_orders_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   └── olist_sellers_dataset.csv

```

###  Start the Database
Spin up the PostgreSQL container using Docker:
```bash
docker-compose up -d