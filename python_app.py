import websocket
import json
import pytz
import mysql.connector
import os
import datetime
import time

socket = "wss://fstream.binance.com/ws/!forceOrder@arr"

# MySQL database configuration from environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Table name
table_name = os.getenv("DB_LIQ_TABLENAME")

# Global connection object
global_conn = None
global_cursor = None

# Create table if it doesn't exist
def create_table_if_not_exists(cursor):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(255),
        side VARCHAR(255),
        order_type VARCHAR(255),
        time_in_force VARCHAR(255),
        original_quantity DECIMAL(30, 8),
        price DECIMAL(30, 8),
        average_price DECIMAL(30, 8),
        order_status VARCHAR(255),
        order_last_filled_quantity DECIMAL(30, 8),
        order_filled_accumulated_quantity DECIMAL(30, 8),
        order_trade_time BIGINT,
        usd_size DECIMAL(30, 8),
        timestamp TIMESTAMP
    )
    """
    cursor.execute(create_table_query)


# Insert data into the table
def insert_data(cursor, data):
    insert_query = f"""
    INSERT INTO {table_name} (
        symbol, side, order_type, time_in_force, original_quantity, price, average_price,
        order_status, order_last_filled_quantity, order_filled_accumulated_quantity,
        order_trade_time, usd_size, timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    cursor.execute(insert_query, data)


def get_db_connection():
    global global_conn, global_cursor
    if not global_conn or not global_cursor:
        global_conn = mysql.connector.connect(**db_config)
        global_cursor = global_conn.cursor()
    return global_conn, global_cursor

def init_app():
    global global_conn, global_cursor
    global_conn, global_cursor = get_db_connection()
    create_table_if_not_exists(global_cursor)
    global_conn.commit()


def on_message(ws, message):
    data = json.loads(message)
    order_data = data["o"]
    timestamp = int(order_data["T"])
    filled_quantity = float(order_data["z"])
    price = float(order_data["p"])
    usd_size = filled_quantity * price
    time_est = datetime.datetime.fromtimestamp(
        timestamp / 1000, pytz.timezone("Europe/Berlin")
    ).strftime("%H:%M:%S")

    # Prepare data for insertion
    data_to_insert = [
        order_data.get(key)
        for key in [
            "s",
            "S",
            "o",
            "f",
            "q",
            "p",
            "ap",
            "X",
            "l",
            "z",
            "T",
        ]
    ] + [usd_size] + [datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')]

    # Insert data immediately
    global_conn, global_cursor = get_db_connection()
    insert_data(global_cursor, data_to_insert)
    global_conn.commit()


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed with status code: {close_status_code}, message: {close_msg}")
    # Reinitialize the WebSocket connection
    init_app()
    ws = websocket.WebSocketApp(
        socket, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()


def on_open(ws):
    print("Connection opened")


init_app()

ws = websocket.WebSocketApp(
    socket, on_message=on_message, on_error=on_error, on_close=on_close
)
ws.on_open = on_open
ws.run_forever()
