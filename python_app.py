import websocket
import json
import os
from datetime import datetime
import uuid
from azure.cosmos import CosmosClient
import pytz
from urllib.parse import unquote
import codecs

socket = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Cosmos DB configuration from environment variables
cosmos_endpoint = os.getenv("COSMOS_ENDPOINT")
cosmos_key = os.getenv("COSMOS_KEY")

if not cosmos_endpoint or not cosmos_key:
    raise ValueError("Missing Cosmos DB configuration - check environment variables")

print(f"Raw COSMOS_ENDPOINT from env: {cosmos_endpoint}")  # Debug output

# Fix URL encoding if needed
# First decode backslash-escaped sequences (like \x3a)
cosmos_endpoint = codecs.decode(cosmos_endpoint, "unicode-escape")

# Then unquote standard URL-encoded characters
cosmos_endpoint = unquote(cosmos_endpoint)

print(f"Final Cosmos URL: {cosmos_endpoint}")  # Debug output

try:
    cosmos_client = CosmosClient(url=cosmos_endpoint, credential=cosmos_key)
except Exception as e:
    raise ValueError(f"Failed to initialize CosmosClient: {str(e)}")
database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE_ID"))
container = database.get_container_client(os.getenv("COSMOS_CONTAINER_ID"))


def on_message(ws, message):
    data = json.loads(message)
    order_data = data["o"]
    timestamp = int(order_data["T"])
    filled_quantity = float(order_data["z"])
    price = float(order_data["p"])
    usd_size = filled_quantity * price
    # Prepare document for Cosmos DB
    document = {
        "id": str(uuid.uuid4()),
        "symbol": order_data.get("s"),
        "side": order_data.get("S"),
        "order_type": order_data.get("o"),
        "time_in_force": order_data.get("f"),
        "original_quantity": order_data.get("q"),
        "price": order_data.get("p"),
        "average_price": order_data.get("ap"),
        "order_status": order_data.get("X"),
        "order_last_filled_quantity": order_data.get("l"),
        "order_filled_accumulated_quantity": order_data.get("z"),
        "order_trade_time": order_data.get("T"),
        "usd_size": usd_size,
        "timestamp": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Insert document into Cosmos DB
    container.upsert_item(body=document)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print(
        f"Connection closed with status code: {close_status_code}, message: {close_msg}"
    )
    # Reinitialize the WebSocket connection
    ws = websocket.WebSocketApp(
        socket, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()


def on_open(ws):
    print("Connection opened")


ws = websocket.WebSocketApp(
    socket, on_message=on_message, on_error=on_error, on_close=on_close
)
ws.on_open = on_open
ws.run_forever()
