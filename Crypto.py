import json
import websocket
import requests
import sqlite3
from datetime import datetime, timedelta
import os

# Define the symbol and initialize the order book
symbol = 'BTCUSDT'
order_book = {'bids': {}, 'asks': {}}
total_volume = 0
last_price = None
print_flag = True
total_bid_qty = 0
total_ask_qty = 0

# Set database path relative to script location
DB_PATH = os.path.join(os.path.dirname(__file__), 'crypto.db')
table_name = "crypto"

def initialize_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_price REAL,
            total_volume REAL,
            total_bid_qty REAL,
            total_ask_qty REAL
        )
    """)
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()
    conn.close()

def update_db(last_price, total_volume, total_bid_qty, total_ask_qty):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        INSERT INTO {table_name} (last_price, total_volume, total_bid_qty, total_ask_qty)
        VALUES (?, ?, ?, ?)
    """, (last_price, total_volume, total_bid_qty, total_ask_qty))
    conn.commit()
    conn.close()

def get_depth_snapshot(symbol, limit=50):
    url = f"https://api.binance.com/api/v3/depth"
    params = {'symbol': symbol, 'limit': limit}
    response = requests.get(url, params=params)
    return response.json()

initialize_db()
snapshot = get_depth_snapshot(symbol, limit=100)

for bid in snapshot['bids']:
    price = float(bid[0])
    quantity = float(bid[1])
    order_book['bids'][price] = quantity

for ask in snapshot['asks']:
    price = float(ask[0])
    quantity = float(ask[1])
    order_book['asks'][price] = quantity

def on_message(ws, message):
    global total_volume, last_price, print_flag, total_bid_qty, total_ask_qty
    message = json.loads(message)

    if 'data' not in message:
        return

    update = message['data']

    if 'b' in update and 'a' in update:
        u = update['u']
        last_update_id = snapshot['lastUpdateId']

        if u <= last_update_id:
            return

        for bid in update['b']:
            price, quantity = float(bid[0]), float(bid[1])
            if quantity == 0:
                order_book['bids'].pop(price, None)
            else:
                order_book['bids'][price] = quantity

        for ask in update['a']:
            price, quantity = float(ask[0]), float(ask[1])
            if quantity == 0:
                order_book['asks'].pop(price, None)
            else:
                order_book['asks'][price] = quantity

        total_bid_qty = sum(order_book['bids'].values())
        total_ask_qty = sum(order_book['asks'].values())
        print_flag = True

    if 'p' in update and 'q' in update:
        trade_price = float(update['p'])
        trade_quantity = float(update['q'])
        total_volume += trade_quantity
        last_price = trade_price

        if print_flag:
            current_time = datetime.now().strftime("%H:%M:%S")
            print([current_time, last_price, total_volume, total_bid_qty, total_ask_qty])
            update_db(last_price, total_volume, total_bid_qty, total_ask_qty)
            print_flag = False

def on_open(ws):
    print("Connection opened")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

depth_stream = f"{symbol.lower()}@depth"
trade_stream = f"{symbol.lower()}@aggTrade"
socket = f"wss://stream.binance.com:9443/stream?streams={depth_stream}/{trade_stream}"

ws = websocket.WebSocketApp(socket, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)
ws.run_forever()
