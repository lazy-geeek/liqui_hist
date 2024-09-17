from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import asyncio
import threading
import time
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint
import mysql.connector

app = Flask(__name__)
socketio = SocketIO(app)

# Global variables to store the output
output_data = []

# MySQL database configuration from environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

# Table name
table_name = "binance_liqs"

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
        usd_size DECIMAL(30, 8)
    )
    """
    cursor.execute(create_table_query)


# Insert data into the table
def insert_data(cursor, data):
    insert_query = f"""
    INSERT INTO {table_name} (
        symbol, side, order_type, time_in_force, original_quantity, price, average_price,
        order_status, order_last_filled_quantity, order_filled_accumulated_quantity,
        order_trade_time, usd_size
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    cursor.execute(insert_query, data)


async def binance_liquidation(uri):
    global global_conn, global_cursor
    if not global_conn or not global_cursor:
        global_conn, global_cursor = get_db_connection()
    async with connect(uri) as websocket:
        while True:
            try:
                msg = await websocket.recv()
                order_data = json.loads(msg)["o"]
                symbol = order_data["s"].replace("USDT", "")
                side = order_data["S"]
                timestamp = int(order_data["T"])
                filled_quantity = float(order_data["z"])
                price = float(order_data["p"])
                usd_size = filled_quantity * price
                time_est = datetime.fromtimestamp(
                    timestamp / 1000, pytz.timezone("Europe/Berlin")
                ).strftime("%H:%M:%S")
                if usd_size > 0:
                    liquidation_type = "L LIQ" if side == "SELL" else "S LIQ"
                    symbol = symbol[:4]
                    output = f"{liquidation_type} {symbol} {time_est} {usd_size:,.0f}"
                    color = "green" if side == "SELL" else "red"
                    attrs = ["bold"] if usd_size > 10000 else []

                    if usd_size > 250000:
                        stars = "*" * 3
                        attrs.append("blink")
                        output = f"{stars}{output}"
                        for _ in range(4):
                            output_data.append(output)
                    elif usd_size > 100000:
                        stars = "*" * 1
                        attrs.append("blink")
                        output = f"{stars}{output}"
                        for _ in range(2):
                            output_data.append(output)
                    elif usd_size > 25000:
                        output_data.append(output)
                    else:
                        output_data.append(output)

                    msg_values = [
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
                    ]
                    msg_values.append(usd_size)

                    # Add the data to the buffer
                    buffer.append(msg_values)

                    # Emit the new liquidation data to all clients
                    socketio.emit('new_liquidation', output)

            except Exception as e:
                await asyncio.sleep(5)


def get_db_connection():
    global global_conn, global_cursor
    if not global_conn or not global_cursor:
        global_conn = mysql.connector.connect(**db_config)
        global_cursor = global_conn.cursor()
        create_table_if_not_exists(global_cursor)
        global_conn.commit()
    return global_conn, global_cursor


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/liquidations")
def liquidations():
    return jsonify(output_data)


def run_flask():
    socketio.run(app, host="0.0.0.0", port=5000)


buffer = []

def write_buffer_to_db():
    global buffer
    if buffer:
        global_conn, global_cursor = get_db_connection()
        for data in buffer:
            insert_data(global_cursor, data)
        global_conn.commit()
        buffer = []

def run_binance_liquidation():
    asyncio.run(binance_liquidation("wss://fstream.binance.com/ws/!forceOrder@arr"))

def periodic_write_to_db():
    while True:
        time.sleep(60)
        write_buffer_to_db()

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    binance_thread = threading.Thread(target=run_binance_liquidation)
    db_write_thread = threading.Thread(target=periodic_write_to_db)

    flask_thread.start()
    binance_thread.start()
    db_write_thread.start()

    flask_thread.join()
    binance_thread.join()
    db_write_thread.join()


if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    binance_thread = threading.Thread(target=run_binance_liquidation)

    flask_thread.start()
    binance_thread.start()

    flask_thread.join()
    binance_thread.join()
