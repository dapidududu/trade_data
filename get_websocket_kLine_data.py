import websocket
import threading
import time
import json
import pytz
import requests
from datetime import datetime

# 设置全局时区
shanghai_tz = pytz.timezone("Asia/Shanghai")

# Binance 基础地址
REST_BASE_URL = "https://fapi.binance.com"
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt"]
INTERVAL = "1s"
LAST_KLINE_END = {}

def get_historical_kline(symbol, limit=1):
    """用于补偿丢失的K线"""
    url = f"{REST_BASE_URL}/fapi/v1/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": INTERVAL,
        "limit": limit
    }
    resp = requests.get(url, params=params)
    return resp.json()

def on_message(ws, message, symbol):
    print(f"Raw message for {symbol}: {message}")
    data = json.loads(message)
    kline = data['k']
    end_time = kline['T']
    start_time_str = datetime.fromtimestamp(kline['t'] / 1000, tz=shanghai_tz).strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = datetime.fromtimestamp(kline['T'] / 1000, tz=shanghai_tz).strftime('%Y-%m-%d %H:%M:%S')

    # 判断是否丢数据（K线收盘时间相隔超过预期）
    last_end = LAST_KLINE_END.get(symbol)
    if last_end and (end_time - last_end) > 1000:
        missed = get_historical_kline(symbol, limit=2)
        print(f"[补偿] {symbol.upper()} - 补回 {len(missed)} 根K线")
        for m in missed:
            ts = datetime.fromtimestamp(m[0]/1000, tz=shanghai_tz)
            print(f"  补 => {symbol.upper()} @ {ts} | 开: {m[1]} 收: {m[4]}")

    LAST_KLINE_END[symbol] = end_time

    # print(f"[{symbol.upper()}] 收: {kline['c']} @ {dt_str}")
    print(
        f"[{kline['s']}] {kline['i']} | 开始: {start_time_str} | 结束: {end_time_str} | 开: {kline['o']} 高: {kline['h']} 低: {kline['l']} 收: {kline['c']} 量: {kline['v']}"
    )
def on_open(ws, symbol):
    print(f"[{symbol.upper()}] 连接成功，开始订阅")
    # 可自动重连
    def ping():
        while True:
            time.sleep(60)
            try:
                ws.send("pong")
            except:
                break
    threading.Thread(target=ping, daemon=True).start()

def run_ws(symbol):
    # url = f"wss://fstream.binance.com/ws/{symbol}@kline_{INTERVAL}"
    url = f"wss://stream.binance.com:9443/ws/{symbol}@kline_{INTERVAL}"

    def _on_message(ws, message): on_message(ws, message, symbol)
    def _on_open(ws): on_open(ws, symbol)
    ws = websocket.WebSocketApp(
        url,
        on_open=_on_open,
        on_message=_on_message,
        on_error=lambda ws, e: print(f"[{symbol.upper()}] 错误：{e}"),
        on_close=lambda ws, code, msg: print(f"[{symbol.upper()}] 连接关闭")
    )

    # 自动重连机制
    while True:
        try:
            ws.run_forever()
        except Exception as e:
            print(f"[{symbol.upper()}] WebSocket异常：{e}")
        print(f"[{symbol.upper()}] 10秒后重连中...")
        time.sleep(10)

def main():
    threads = []
    for symbol in SYMBOLS:
        t = threading.Thread(target=run_ws, args=(symbol,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
