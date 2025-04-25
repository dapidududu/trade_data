import os

import pandas as pd
import requests

# date_range = pd.date_range(start="2022-01", end=pd.Timestamp.today(), freq="D")
date_range = pd.date_range(start="2022-01", end="2022-03", freq="MS")

# 转换为 "YYYY-MM" 格式的列表
date_list = date_range.strftime("%Y-%m").tolist()

print(date_list)

base_url = "https://data.binance.vision/data/futures/um/monthly/trades/"

coin_list = ['BTC', "ETH", "XRP", "BNB", "SOL", "TRUMP", "DOGE", "ADA", "1000PEPE"]
# coin_list = ['ETH']
for coin in coin_list:
    os.makedirs(coin, exist_ok=True)
    coin_file_url = base_url + f"{coin}USDT/"
    for date in date_list:
        file_url = coin_file_url + f"{coin}USDT-trades-{date}.zip"
        file_path = os.path.join(coin, f"{coin}USDT-trades-{date}.zip")
        print(file_url)

        # 下载文件
        if not os.path.exists(file_path):
            response = requests.get(file_url, stream=True)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                print(f"Saved: {file_path}")
            else:
                print(f"Failed to download: {file_url} (Status: {response.status_code})")

        break
    break