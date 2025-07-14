# 📊 Crypto Bitcoin Realtime Price Stream + SQLite DB Logger

A lightweight Python script to stream real-time Bitcoin (BTCUSDT) order book and trade data from the Binance WebSocket API and store key metrics into a local SQLite database.

---

## 📌 Features

- Streams **live BTCUSDT market depth (order book)** and **aggregate trade data** via WebSocket.
- Maintains a running total of:
  - Last traded price
  - Total traded volume
  - Total bid quantity
  - Total ask quantity
- Writes these values along with a timestamp into a local `crypto.db` SQLite database.
- Simple, clean, and production-ready Python implementation.

---
