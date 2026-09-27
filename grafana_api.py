import os

from dotenv import load_dotenv
from flask import Flask, jsonify
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ecommerce_streaming")

if not MONGODB_URI:
    raise ValueError("MONGODB_URI is not configured in .env")

client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=10000
)

client.admin.command("ping")

db = client[MONGODB_DB]
orders = db["orders"]

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "database": MONGODB_DB,
        "collection": "orders"
    })


@app.route("/api/orders")
def all_orders():
    data = list(
        orders.find(
            {},
            {"_id": 0}
        )
    )

    for order in data:
        if "Timestamp" in order:
            order["Timestamp"] = str(order["Timestamp"])

    return jsonify(data)


@app.route("/api/revenue-by-category")
def revenue_by_category():
    pipeline = [
        {
            "$group": {
                "_id": "$Category",
                "revenue": {"$sum": "$Total_Amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "Category": "$_id",
                "revenue": 1
            }
        },
        {
            "$sort": {
                "revenue": -1
            }
        }
    ]

    return jsonify(list(orders.aggregate(pipeline)))


@app.route("/api/revenue-by-city")
def revenue_by_city():
    pipeline = [
        {
            "$group": {
                "_id": "$City",
                "revenue": {"$sum": "$Total_Amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "City": "$_id",
                "revenue": 1
            }
        },
        {
            "$sort": {
                "revenue": -1
            }
        }
    ]

    return jsonify(list(orders.aggregate(pipeline)))


@app.route("/api/payment-status")
def payment_status():
    pipeline = [
        {
            "$group": {
                "_id": "$Payment_Status",
                "orders": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "Payment_Status": "$_id",
                "orders": 1
            }
        },
        {
            "$sort": {
                "orders": -1
            }
        }
    ]

    return jsonify(list(orders.aggregate(pipeline)))


@app.route("/api/kpis")
def kpis():
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_orders": {"$sum": 1},
                "total_order_value": {"$sum": "$Total_Amount"},
                "average_order_value": {"$avg": "$Total_Amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_orders": 1,
                "total_order_value": 1,
                "average_order_value": 1
            }
        }
    ]

    result = list(orders.aggregate(pipeline))

    if not result:
        return jsonify({
            "total_orders": 0,
            "total_order_value": 0,
            "average_order_value": 0
        })

    return jsonify(result[0])


if __name__ == "__main__":
    print("MongoDB → Grafana API")
    print("Database:", MONGODB_DB)
    print("API running on port 8001")

    app.run(
        host="0.0.0.0",
        port=8001,
        debug=False
    )
