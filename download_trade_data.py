import os

import pandas as pd
import requests

month_date_range = pd.date_range(start="2022-01", end=pd.Timestamp.today(), freq="MS")
# 获取本月的第一天
first_day_of_this_month = pd.Timestamp.today().replace(day=1)
# 上个月最后一天 = 本月第一天减一天
last_day_of_last_month = first_day_of_this_month - pd.Timedelta(days=1)

# 用 last_day_of_last_month 作为 start
day_date_range = pd.date_range(start=last_day_of_last_month, end=pd.Timestamp.today(), freq="D")

month_date_list = month_date_range.strftime("%Y-%m").tolist()
day_date_list = day_date_range.strftime("%Y-%m-%d").tolist()
print(month_date_list)
print(day_date_list)


month_base_url = "https://data.binance.vision/data/futures/um/monthly/trades/"
day_base_url = "https://data.binance.vision/data/futures/um/daily/trades/"
spot_month_base_url = "https://data.binance.vision/data/spot/monthly/trades/"
spot_day_base_url = "https://data.binance.vision/data/spot/daily/trades/"

coin_list = []
USDC_futures_coin_list = ["SOLUSDC",
    "XRPUSDC",
    "DOGEUSDC",
    "1000PEPEUSDC"]
spot_coin_list = ['BTCFDUSD']

file_list = []
if coin_list:
    for coin in coin_list:
        os.makedirs(coin, exist_ok=True)
        for date in month_date_list:
            coin_file_url = month_base_url + f"{coin}USDT/"
            file_url = coin_file_url + f"{coin}USDT-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}USDT-trades-{date}.zip")
            file_list.append([file_url, file_path])
        for date in day_date_list:
            coin_file_url = day_base_url + f"{coin}USDT/"
            file_url = coin_file_url + f"{coin}USDT-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}USDT-trades-{date}.zip")
            file_list.append([file_url, file_path])

if USDC_futures_coin_list:
    for coin in USDC_futures_coin_list:
        os.makedirs(coin, exist_ok=True)
        for date in month_date_list:
            coin_file_url = month_base_url + f"{coin}/"
            file_url = coin_file_url + f"{coin}-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}-trades-{date}.zip")
            file_list.append([file_url, file_path])
        for date in day_date_list:
            coin_file_url = day_base_url + f"{coin}/"
            file_url = coin_file_url + f"{coin}-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}-trades-{date}.zip")
            file_list.append([file_url, file_path])

if spot_coin_list:
    for coin in spot_coin_list:
        os.makedirs(coin, exist_ok=True)
        for date in month_date_list:
            coin_file_url = spot_month_base_url + f"{coin}/"
            file_url = coin_file_url + f"{coin}-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}-trades-{date}.zip")
            file_list.append([file_url, file_path])
        for date in day_date_list:
            coin_file_url = spot_day_base_url + f"{coin}/"
            file_url = coin_file_url + f"{coin}-trades-{date}.zip"
            file_path = os.path.join(coin, f"{coin}-trades-{date}.zip")
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

