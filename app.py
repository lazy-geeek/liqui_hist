from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import os
from datetime import datetime, timedelta
from db_utils import get_db_connection
from binance_liquidation import run_binance_liquidation
from utils import convert_timeframe_to_seconds

app = Flask(__name__)
socketio = SocketIO(app)

output_data = []

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/liquidations")
def liquidations():
    return jsonify(output_data)

@app.route("/api/liquidations", methods=["GET", "POST"])
def get_liquidations():
    if request.method == "POST":
        data = request.get_json()
        symbol = data.get("symbol").lower()
        timeframe = data.get("timeframe")
        start_datetime_str = data.get("start_timestamp")
        end_datetime_str = data.get("end_timestamp")
    else:
        symbol = request.args.get("symbol").lower()
        timeframe = request.args.get("timeframe")
        start_datetime_str = request.args.get("start_timestamp")
        end_datetime_str = request.args.get("end_timestamp")

    try:
        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
        end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
        start_timestamp = int(start_datetime.timestamp())
        end_timestamp = int(end_datetime.timestamp())
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid datetime format"}), 400

    timeframe_seconds = convert_timeframe_to_seconds(timeframe)
    if start_timestamp < 0 or end_timestamp < 0:
        return jsonify({"error": "Timestamps must be non-negative"}), 400

    if start_time > end_time:
        return jsonify({"error": "start_timestamp must be before end_timestamp"}), 400

    conn = get_db_connection()
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
        return jsonify({"message": "No data found"}), 404

    cursor.close()
    conn.close()

    return jsonify(results)

def run_flask():
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    socketio.run(app, host="0.0.0.0", port=5000, debug=debug_mode)

if __name__ == "__main__":
    run_flask()
