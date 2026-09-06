import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# ============================================================
# CONFIGURATION
# ============================================================

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "student"
DB_PASSWORD = "student"
DB_NAME = "ecommerce"

CUSTOMERS_COUNT = 100_000
PRODUCTS_COUNT = 20_000
ORDERS_COUNT = 500_000
ORDER_ITEMS_COUNT = 2_000_000
PAYMENTS_COUNT = 500_000

BATCH_SIZE = 5_000

fake = Faker()

# ============================================================
# DATABASE CONNECTION
# ============================================================

connection = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = connection.cursor()

cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {DB_NAME}
""")

connection.database = DB_NAME

# ============================================================
# DROP TABLES
# ============================================================

cursor.execute("DROP TABLE IF EXISTS payments")
cursor.execute("DROP TABLE IF EXISTS order_items")
cursor.execute("DROP TABLE IF EXISTS orders")
cursor.execute("DROP TABLE IF EXISTS products")
cursor.execute("DROP TABLE IF EXISTS customers")

connection.commit()

# ============================================================
# CREATE CUSTOMERS
# ============================================================

print("Creating customers table...")

cursor.execute("""
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(100),
    city VARCHAR(100),
    signup_date DATE
)
""")

# ============================================================
# CREATE PRODUCTS
# ============================================================

print("Creating products table...")

cursor.execute("""
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT
)
""")

# ============================================================
# CREATE ORDERS
# ============================================================

print("Creating orders table...")

cursor.execute("""
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    order_date DATETIME,
    status VARCHAR(30),
    shipping_country VARCHAR(100)
)
""")

# ============================================================
# CREATE ORDER ITEMS
# ============================================================

print("Creating order_items table...")

cursor.execute("""
CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DECIMAL(10,2)
)
""")

# ============================================================
# CREATE PAYMENTS
# ============================================================

print("Creating payments table...")

cursor.execute("""
CREATE TABLE payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    payment_method VARCHAR(50),
    payment_status VARCHAR(30),
    payment_date DATETIME,
    amount DECIMAL(10,2)
)
""")

connection.commit()

# ============================================================
# 1. GENERATE CUSTOMERS
# ============================================================

print("Generating customers...")

countries = [
    "Egypt",
    "Saudi Arabia",
    "UAE",
    "Jordan",
    "Kuwait",
    "Qatar"
]

customers = []

for customer_id in range(1, CUSTOMERS_COUNT + 1):
    first_name = fake.first_name()
    last_name = fake.last_name()

    customers.append((
        customer_id,
        first_name,
        last_name,
        f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com",
        random.choice(countries),
        fake.city(),
        fake.date_between(start_date="-5y", end_date="today")
    ))

    if len(customers) >= BATCH_SIZE:
        cursor.executemany("""
        INSERT INTO customers
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, customers)
        connection.commit()
        customers = []
        print(f"Customers inserted: {customer_id:,}")

if customers:
    cursor.executemany("""
    INSERT INTO customers
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, customers)
    connection.commit()

# ============================================================
# 2. GENERATE PRODUCTS
# ============================================================

print("Generating products...")

categories = [
    "Electronics", "Clothing", "Shoes", "Home", "Beauty",
    "Sports", "Books", "Gaming", "Accessories", "Furniture"
]

products = []

for product_id in range(1, PRODUCTS_COUNT + 1):
    products.append((
        product_id,
        fake.catch_phrase(),
        random.choice(categories),
        round(random.uniform(5, 2000), 2),
        random.randint(0, 1000)
    ))

    if len(products) >= BATCH_SIZE:
        cursor.executemany("""
        INSERT INTO products
        VALUES (%s,%s,%s,%s,%s)
        """, products)
        connection.commit()
        products = []
        print(f"Products inserted: {product_id:,}")

if products:
    cursor.executemany("""
    INSERT INTO products
    VALUES (%s,%s,%s,%s,%s)
    """, products)
    connection.commit()

# ============================================================
# 3. GENERATE ORDERS
# ============================================================

print("Generating orders...")

statuses = ["completed", "completed", "completed", "pending", "cancelled", "shipped"]

orders = []
start_date = datetime.now() - timedelta(days=730)

for order_id in range(1, ORDERS_COUNT + 1):
    customer_id = random.randint(1, CUSTOMERS_COUNT)

    order_date = start_date + timedelta(
        seconds=random.randint(0, 730 * 24 * 60 * 60)
    )

    orders.append((
        order_id,
        customer_id,
        order_date,
        random.choice(statuses),
        random.choice(countries)
    ))

    if len(orders) >= BATCH_SIZE:
        cursor.executemany("""
        INSERT INTO orders
        VALUES (%s,%s,%s,%s,%s)
        """, orders)
        connection.commit()
        orders = []
        print(f"Orders inserted: {order_id:,}")

if orders:
    cursor.executemany("""
    INSERT INTO orders
    VALUES (%s,%s,%s,%s,%s)
    """, orders)
    connection.commit()

# ============================================================
# 4. GENERATE ORDER ITEMS
# ============================================================

print("Generating order items...")

order_items = []

for order_item_id in range(1, ORDER_ITEMS_COUNT + 1):
    order_id = random.randint(1, ORDERS_COUNT)
    product_id = random.randint(1, PRODUCTS_COUNT)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5, 2000), 2)

    order_items.append((
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price
    ))

    if len(order_items) >= BATCH_SIZE:
        cursor.executemany("""
        INSERT INTO order_items
        VALUES (%s,%s,%s,%s,%s)
        """, order_items)
        connection.commit()
        order_items = []
        print(f"Order items inserted: {order_item_id:,}")

if order_items:
    cursor.executemany("""
    INSERT INTO order_items
    VALUES (%s,%s,%s,%s,%s)
    """, order_items)
    connection.commit()

# ============================================================
# 5. GENERATE PAYMENTS
# ============================================================

print("Generating payments...")

payment_methods = ["credit_card", "debit_card", "cash", "paypal", "bank_transfer"]
payment_statuses = ["paid", "paid", "paid", "failed", "refunded"]

payments = []

for payment_id in range(1, PAYMENTS_COUNT + 1):
    order_id = payment_id

    payment_date = start_date + timedelta(
        seconds=random.randint(0, 730 * 24 * 60 * 60)
    )

    payments.append((
        payment_id,
        order_id,
        random.choice(payment_methods),
        random.choice(payment_statuses),
        payment_date,
        round(random.uniform(10, 5000), 2)
    ))

    if len(payments) >= BATCH_SIZE:
        cursor.executemany("""
        INSERT INTO payments
        VALUES (%s,%s,%s,%s,%s,%s)
        """, payments)
        connection.commit()
        payments = []
        print(f"Payments inserted: {payment_id:,}")

if payments:
    cursor.executemany("""
    INSERT INTO payments
    VALUES (%s,%s,%s,%s,%s,%s)
    """, payments)
    connection.commit()

# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
connection.close()

print("====================================")
print("DATA GENERATION COMPLETED")
print("====================================")
