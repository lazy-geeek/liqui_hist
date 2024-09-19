import asyncio
import json
import pytz
from datetime import datetime
from websockets import connect
from termcolor import cprint
from db_utils import get_db_connection, insert_data

async def binance_liquidation(uri):
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
                    socketio.emit("new_liquidation", output)

            except Exception as e:
                await asyncio.sleep(5)

def run_binance_liquidation():
    asyncio.run(binance_liquidation("wss://fstream.binance.com/ws/!forceOrder@arr"))
