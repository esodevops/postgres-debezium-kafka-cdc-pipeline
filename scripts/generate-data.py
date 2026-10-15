#!/usr/bin/env python3
"""CDC demo data generator reconstructed from the supplied screenshots."""
import psycopg2
import random
import time
import argparse
import signal
import sys
from faker import Faker
from decimal import Decimal

# Initialize Faker
fake = Faker()

# Database connection settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'enterprise_db',
    'user': 'cdc_user',
    'password': 'cdc_password'
}

# Control flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    global running
    print('\nStopping data generator...')
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def create_customer(conn):
    """Create a new customer"""
    with conn.cursor() as cursor:
        tier = random.choices(
            ['bronze', 'silver', 'gold', 'platinum'],
            weights=[50, 30, 15, 5]
        )[0]

        cursor.execute("""
            INSERT INTO ecommerce.customers (email, first_name, last_name, phone, tier, total_spent)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, first_name, last_name
        """, (
            fake.unique.email(),
            fake.first_name(),
            fake.last_name(),
            fake.phone_number()[:20],  # Match the schema's VARCHAR(20).
            tier,
            round(random.uniform(0, 5000), 2)
        ))
        result = cursor.fetchone()
    conn.commit()
    return result[0]


def update_customer(conn):
    """Update a random customer"""
    with conn.cursor() as cursor:
        # Get a random customer
        cursor.execute('SELECT id, first_name, last_name FROM ecommerce.customers ORDER BY RANDOM() LIMIT 1')
        customer = cursor.fetchone()
        if not customer:
            return

        # Update tier or total spent
        if random.random() > 0.5:
            new_tier = random.choice(['bronze', 'silver', 'gold', 'platinum'])
            cursor.execute("""
                UPDATE ecommerce.customers
                SET tier = %s
                WHERE id = %s
            """, (new_tier, customer[0]))
        else:
            additional_spent = round(random.uniform(50, 500), 2)
            cursor.execute("""
                UPDATE ecommerce.customers
                SET total_spent = total_spent + %s
                WHERE id = %s
            """, (additional_spent, customer[0]))
    conn.commit()


def create_order(conn):
    """Create a new order with items"""
    with conn.cursor() as cursor:
        # Get a random customer
        cursor.execute('SELECT id FROM ecommerce.customers WHERE is_active = true ORDER BY RANDOM() LIMIT 1')
        customer = cursor.fetchone()
        if not customer:
            return

        # Get random products
        cursor.execute('SELECT id, price FROM ecommerce.products WHERE is_available = true ORDER BY RANDOM() LIMIT %s',
                       (random.randint(1, 4),))
        products = cursor.fetchall()
        if not products:
            return

        # Completed missing section: calculate totals and create the order.
        order_items = []
        subtotal = Decimal('0.00')
        for product_id, unit_price in products:
            quantity = random.randint(1, 3)
            total_price = unit_price * quantity
            order_items.append((product_id, quantity, unit_price, total_price))
            subtotal += total_price

        tax = (subtotal * Decimal('0.10')).quantize(Decimal('0.01'))
        shipping = Decimal('0.00') if subtotal >= 100 else Decimal('9.99')
        total = subtotal + tax + shipping
        # The database trigger generates order_number for an empty string.
        cursor.execute("""
            INSERT INTO ecommerce.orders (
                order_number, customer_id, status, subtotal, tax,
                shipping, total, shipping_address, payment_method
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, order_number
        """, (
            '', customer[0], 'pending', subtotal, tax, shipping, total,
            fake.address(), random.choice(['credit_card', 'debit_card', 'paypal'])
        ))
        order = cursor.fetchone()

        # Create order items
        for product_id, quantity, unit_price, total_price in order_items:
            cursor.execute("""
                INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price, total_price)
                VALUES (%s, %s, %s, %s, %s)
            """, (order[0], product_id, quantity, unit_price, total_price))
    conn.commit()
    return order[0]


def update_order_status(conn):
    """Update order status (simulating order lifecycle)"""
    with conn.cursor() as cursor:
        status_flow = {
            'pending': 'confirmed',
            'confirmed': 'processing',
            'processing': 'shipped',
            'shipped': 'delivered'
        }

        # Get an order that can be updated
        cursor.execute("""
            SELECT id, order_number, status
            FROM ecommerce.orders
            WHERE status IN ('pending', 'confirmed', 'processing', 'shipped')
            ORDER BY RANDOM()
            LIMIT 1
        """)
        # Completed missing section: advance to the next lifecycle status.
        order = cursor.fetchone()
        if not order:
            return
        new_status = status_flow[order[2]]
        cursor.execute("""
            UPDATE ecommerce.orders
            SET status = %s
            WHERE id = %s
        """, (new_status, order[0]))
    conn.commit()


def update_inventory(conn):
    """Adjust stock to simulate sales and restocking."""
    # Completed missing section: select stock and calculate its new quantity.
    with conn.cursor() as cursor:
        cursor.execute('SELECT id, quantity, reserved FROM ecommerce.inventory ORDER BY RANDOM() LIMIT 1')
        inventory = cursor.fetchone()
        if not inventory:
            return
        change = random.randint(-10, 50)
        new_qty = max(inventory[2], inventory[1] + change)
        cursor.execute("""
            UPDATE ecommerce.inventory
            SET quantity = %s,
                available = %s,
                last_restocked_at = CASE WHEN %s > 0 THEN CURRENT_TIMESTAMP ELSE last_restocked_at END
            WHERE id = %s
        """, (new_qty, new_qty - inventory[2], change, inventory[0]))
    conn.commit()


def update_product_price(conn):
    """Update product price"""
    with conn.cursor() as cursor:
        cursor.execute('SELECT id, name, price FROM ecommerce.products ORDER BY RANDOM() LIMIT 1')
        product = cursor.fetchone()
        if not product:
            return

        # Adjust price by -10% to +15%
        adjustment = random.uniform(0.9, 1.15)
        new_price = round(float(product[2]) * adjustment, 2)
        cursor.execute("""
            UPDATE ecommerce.products
            SET price = %s
            WHERE id = %s
        """, (new_price, product[0]))
    conn.commit()


# Completed missing section: run randomly selected operations until stopped.
def run_demo(interval=2, duration=None):
    conn = get_connection()
    operations = [
        create_customer,
        update_customer,
        create_order,
        update_order_status,
        update_inventory,
        update_product_price
    ]
    start_time = time.monotonic()
    deadline = start_time + duration if duration is not None else None
    completed = 0
    print(f'Connected to {DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')
    print('Generating CDC demo data. Press Ctrl+C to stop.')
    try:
        while running and (deadline is None or time.monotonic() < deadline):
            operation = random.choice(operations)
            try:
                operation(conn)
                # Also end read-only transactions when no eligible row exists.
                conn.commit()
                completed += 1
                print(f'[{completed}] {operation.__name__}', flush=True)
            except psycopg2.Error as e:
                conn.rollback()
                print(f'{operation.__name__} failed: {e}', file=sys.stderr)
                if conn.closed or isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
                    raise

            wake_at = time.monotonic() + interval
            if deadline is not None:
                wake_at = min(wake_at, deadline)
            while running and time.monotonic() < wake_at:
                time.sleep(min(0.2, max(0, wake_at - time.monotonic())))
    finally:
        conn.close()
        print(f'Data generator stopped after {completed} operations.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDC Demo Data Generator')
    parser.add_argument('-i', '--interval', type=float, default=2,
                        help='Interval between operations in seconds (default: 2)')
    parser.add_argument('-d', '--duration', type=int, default=None,
                        help='Duration to run in seconds (default: unlimited)')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')

    args = parser.parse_args()
    if args.interval < 0:
        parser.error('--interval must be non-negative')
    if args.duration is not None and args.duration < 0:
        parser.error('--duration must be non-negative')

    DB_CONFIG['host'] = args.host
    DB_CONFIG['port'] = args.port

    run_demo(interval=args.interval, duration=args.duration)
