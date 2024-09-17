from fastapi import FastAPI, Query
from typing import List
import mysql.connector
import os
from datetime import datetime, timedelta

app = FastAPI()

# MySQL database configuration from environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}


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


@app.get("/liquidations")
async def get_liquidations(
    symbol: str, timeframe: str, start_timestamp: int, end_timestamp: int
):
    timeframe_seconds = convert_timeframe_to_seconds(timeframe)
    start_time = datetime.fromtimestamp(start_timestamp)
    end_time = datetime.fromtimestamp(end_timestamp)

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    results = []
    current_start = start_time
    while current_start < end_time:
        current_end = current_start + timedelta(seconds=timeframe_seconds)
        query = f"""
        SELECT symbol, {timeframe_seconds} AS timeframe, {int(current_start.timestamp())} AS start_timestamp, {int(current_end.timestamp())} AS end_timestamp, SUM(usd_size) AS cumulated_usd_size
        FROM binance_liqs
        WHERE symbol = %s AND order_trade_time >= %s AND order_trade_time < %s
        GROUP BY symbol, timeframe, start_timestamp, end_timestamp
        """
        cursor.execute(
            query,
            (symbol, int(current_start.timestamp()), int(current_end.timestamp())),
        )
        result = cursor.fetchone()
        if result:
            results.append(
                {
                    "symbol": result[0],
                    "timeframe": result[1],
                    "start_timestamp": result[2],
                    "end_timestamp": result[3],
                    "cumulated_usd_size": float(result[4]),
                }
            )
        current_start = current_end

    cursor.close()
    conn.close()

    return results
