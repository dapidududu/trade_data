import os

import pandas as pd
import requests

# 用 last_day_of_last_month 作为 start
day_date_range = pd.date_range(start="2022-01-01", end=pd.Timestamp.today(), freq="D")

day_date_list = day_date_range.strftime("%Y-%m-%d").tolist()
print(day_date_list)


day_base_url = "https://data.binance.vision/data/option/daily/EOHSummary/"
coin_list = ["BNBUSDT",
"BTCUSDT",
"DOGEUSDT",
"ETHUSDT",
"XRPUSDT"]

file_list = []
if coin_list:
    for coin in coin_list:
        os.makedirs(coin, exist_ok=True)
        for date in day_date_list:
            coin_file_url = day_base_url + f"{coin}/"
            file_url = coin_file_url + f"{coin}-EOHSummary-{date}.zip"
            file_path = os.path.join(coin, f"{coin}-EOHSummary-{date}.zip")
            file_list.append([file_url, file_path])


for file_data in file_list:
    # 下载文件
    if not os.path.exists(file_data[1]):
        print(file_data[0])
        response = requests.get(file_data[0], stream=True)
        if response.status_code == 200:
            with open(file_data[1], "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Saved: {file_data[1]}")
        else:
            print(f"Failed to download: {file_data[0]} (Status: {response.status_code})")

