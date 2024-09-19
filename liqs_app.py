from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import mysql.connector
import os
from datetime import datetime, timedelta
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

# Buffer to store liquidation data before writing to the database
buffer = []

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
    global global_conn, global_cursor, buffer
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

                    # Append the data to the buffer
                    buffer.append(msg_values)

                # Emit the new liquidation data to all clients
                socketio.emit("new_liquidation", output)

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


@app.route("/api/liquidations", methods=["GET", "POST"])
def get_liquidations():
    if request.method == "POST":
        try:
            data = request.get_json()
            symbol = data.get("symbol").lower()
            timeframe = data.get("timeframe")
            try:
                start_datetime_str = data.get("start_timestamp")
                end_datetime_str = data.get("end_timestamp")
                start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
                end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
                start_timestamp = int(start_datetime.timestamp())
                end_timestamp = int(end_datetime.timestamp())
            except (TypeError, ValueError):
                return (
                    jsonify(
                        {
                            "error": "start_timestamp and end_timestamp must be valid datetime strings in the format 'YYYY-MM-DD hh:mm'"
                        }
                    ),
                    400,
                )
        except Exception as e:
            return jsonify({"error": "Invalid JSON request body"}), 400
    else:
        symbol = request.args.get("symbol").lower()
        timeframe = request.args.get("timeframe")
        try:
            start_datetime_str = request.args.get("start_timestamp")
            end_datetime_str = request.args.get("end_timestamp")
            start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())
        except (TypeError, ValueError):
            return (
                jsonify(
                    {
                        "error": "start_timestamp and end_timestamp must be valid datetime strings in the format 'YYYY-MM-DD hh:mm'"
                    }
                ),
                400,
            )

    timeframe_seconds = convert_timeframe_to_seconds(timeframe)
    if start_timestamp < 0 or end_timestamp < 0:
        return (
            jsonify(
                {
                    "error": "start_timestamp and end_timestamp must be non-negative integers"
                }
            ),
            400,
        )
    try:
        start_time = datetime.fromtimestamp(start_timestamp)
        end_time = datetime.fromtimestamp(end_timestamp)
    except (ValueError, OverflowError):
        return (
            jsonify(
                {"error": "start_timestamp and end_timestamp are out of valid range"}
            ),
            400,
        )

    # Ensure the start_time and end_time are within a reasonable range
    if start_time > end_time:
        return jsonify({"error": "start_timestamp must be before end_timestamp"}), 400

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    results = []
    current_start = start_time
    while current_start < end_time:
        current_end = current_start + timedelta(seconds=timeframe_seconds)
        query = f"""
        SELECT symbol, {timeframe_seconds} AS timeframe, {int(current_start.timestamp() * 1000)} AS start_timestamp, {int(current_end.timestamp() * 1000)} AS end_timestamp, SUM(usd_size) AS cumulated_usd_size
        FROM binance_liqs
        WHERE LOWER(symbol) = %s AND order_trade_time >= %s AND order_trade_time < %s
        GROUP BY symbol, timeframe, start_timestamp, end_timestamp
        """

        cursor.execute(
            query,
            (
                symbol,
                int(current_start.timestamp() * 1000),
                int(current_end.timestamp() * 1000),
            ),
        )
        result = cursor.fetchone()
        if result:
            results.append(
                {
                    "symbol": result[0],
                    "timeframe": timeframe,
                    "start_timestamp": datetime.fromtimestamp(
                        result[2] / 1000
                    ).isoformat(),
                    "end_timestamp": datetime.fromtimestamp(
                        result[3] / 1000
                    ).isoformat(),
                    "cumulated_usd_size": float(result[4]),
                }
            )
        current_start = current_end

    if not results:
        return jsonify({"message": "No data found for the given parameters"}), 404

    cursor.close()
    conn.close()

    return jsonify(results)


def convert_timeframe_to_seconds(timeframe: str) -> int:
    timeframe = timeframe.lower()
    if timeframe.endswith("m"):
        return int(timeframe[:-1]) * 60
    elif timeframe.endswith("h"):
        return int(timeframe[:-1]) * 3600
    elif timeframe.endswith("d"):
        return int(timeframe[:-1]) * 86400
    else:
        raise ValueError("Invalid timeframe format")


def write_buffer_to_db():
    global buffer, global_conn, global_cursor
    if buffer:
        if not global_conn or not global_cursor:
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
    # Start the Binance liquidation thread
    binance_thread = threading.Thread(target=run_binance_liquidation)
    binance_thread.start()

    # Start the periodic database write thread
    db_write_thread = threading.Thread(target=periodic_write_to_db)
    db_write_thread.start()

    # Run the Flask app
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)

    # Join the threads to ensure they run until the app is stopped
    binance_thread.join()
    db_write_thread.join()
