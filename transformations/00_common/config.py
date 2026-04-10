INCREMENTAL_TABLES = [
    "orders", "order_items", "payments", "products",
    "reviews", "risk_support", "sessions", "shipping"
]

APPEND_TABLES = ["reviews", "order_items", "sessions"]

UPSERT_TABLES = ["orders", "payments", "shipping"]

SCD2_TABLES = ["products", "risk_support"]

KEYS = {
    "reviews": ["review_id"],
    "order_items": ["order_item_id"],
    "sessions": ["session_id"],
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "shipping": ["shipping_id"],
    "products": ["product_id"],
    "risk_support": ["risk_id"]
}