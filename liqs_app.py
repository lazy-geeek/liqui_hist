from app import run_flask
from binance_liquidation import run_binance_liquidation
import threading
import time

buffer = []

def write_buffer_to_db():
    global buffer
    if buffer:
        global_conn, global_cursor = get_db_connection()
        for data in buffer:
            insert_data(global_cursor, data)
        global_conn.commit()
        buffer = []

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
