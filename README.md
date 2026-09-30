# E-Commerce Real-Time Streaming Analytics

This repository contains the implementation of a real-time e-commerce data streaming and analytics project using Apache Kafka, Python, MongoDB Atlas, Streamlit, Plotly, and Grafana Cloud.

## Project Overview

The project demonstrates how e-commerce order data can be streamed through Kafka, consumed using Python, stored in MongoDB Atlas, and visualized through interactive dashboards.

The overall data flow is:

CSV Data → Kafka Producer → Kafka Topic → Kafka Consumer → MongoDB Atlas → Streamlit / Grafana Cloud

## Technology Stack

- Python
- Apache Kafka
- Docker
- MongoDB Atlas
- PyMongo
- Streamlit
- Plotly
- Flask
- Grafana Cloud
- GitHub

## Project Files

- `producer.py` — Publishes e-commerce order events to the Kafka topic.
- `consumer.py` — Consumes order events from Kafka and stores them in MongoDB Atlas.
- `dashboard.py` — Streamlit dashboard connected to MongoDB Atlas for interactive analytics.
- `grafana_api.py` — Flask API that reads aggregated data from MongoDB and provides endpoints for Grafana.
- `ecommerce_orders_sample.csv` — Sample e-commerce order dataset.
- `requirements.txt` — Python dependencies.
- `.gitignore` — Prevents sensitive files such as `.env` from being committed.

## Kafka Streaming

The Kafka topic used in the project is:

`ecommerce_orders`

The producer publishes individual e-commerce order records as JSON messages. The consumer subscribes to the topic and processes the incoming events.

Each order contains fields such as:

- Order ID
- Timestamp
- Customer ID
- Product ID
- Product Name
- Category
- Quantity
- Unit Price
- Total Amount
- Payment Method
- Payment Status
- City

## MongoDB Storage

Consumed Kafka messages are stored in MongoDB Atlas.

Database:

`ecommerce_streaming`

Collection:

`orders`

The consumer uses `Order_ID` to identify orders and uses upsert logic to avoid unnecessary duplicate records.

## Dashboard

The project contains dashboards built using Streamlit and Grafana Cloud.

### Grafana Cloud Visualizations

The Grafana dashboard contains four visualizations:

1. **Revenue by Category** — compares order value across product categories.
2. **Revenue by City** — compares order value across different cities.
3. **Payment Status Distribution** — shows successful, pending, and failed orders.
4. **Total Order Value** — KPI panel showing total order value, total orders, and average order value.

### Streamlit Dashboard

The Streamlit dashboard provides:

- Total Orders
- Total Order Value
- Average Order Value
- Successful Orders
- Revenue by Category
- Revenue by City
- Payment Status Distribution
- Order Value Trend
- Business Insights
- Order-level data

## Current Dashboard Results

Based on the current 100-order dataset:

- Total Orders: 100
- Total Order Value: ₹361,225
- Average Order Value: ₹3,612
- Successful Orders: 58

The current dashboard shows Electronics as the highest-value category and Bengaluru as the highest-value city.

Payment status distribution is:

- Successful: 58%
- Pending: 22%
- Failed: 20%

## Business Insights

The dashboard provides several useful business insights from the e-commerce order data. Electronics generates the highest order value among the product categories, indicating that it is the largest contributor to overall revenue in the current dataset. Home & Kitchen and Fashion are the next major contributors. At the city level, Bengaluru records the highest order value, followed by Jaipur and Mumbai, showing that these locations contribute significantly to sales. Payment status analysis shows that 58% of orders are successful, while 22% are pending and 20% are failed. The relatively large proportion of pending and failed payments indicates an area that can be monitored to reduce potential revenue leakage. Overall, the dashboard enables management to compare category performance, identify high-value markets, monitor payment outcomes, and track key revenue metrics from the streaming data.

## Dashboard Links

### Streamlit Dashboard

https://sda-assignment2-hl4ayg8rh8yua6qdwfyo5b.streamlit.app/

### Grafana Cloud Dashboard

https://neathickory1336.grafana.net/public-dashboards/79631ae3f9d24410b55770113e66043d

## Data Flow Architecture

```text
E-Commerce CSV
      |
      v
Kafka Producer
      |
      v
Kafka Topic: ecommerce_orders
      |
      v
consumer.py
      |
      v
MongoDB Atlas
      |
      +--------------------+
      |                    |
      v                    v
Streamlit Dashboard    Flask API
                           |
                           v
                    Grafana Cloud
