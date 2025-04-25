from datetime import datetime, timedelta

import pytz
from binance.client import Client
shanghai_tz = pytz.timezone("Asia/Shanghai")


def time_res(client):
    time_res = client.get_server_time()
    print(time_res)


def get_exchange_info(client):
    info = client.get_exchange_info()
    quote_asset_list = ["USDT", "FDUSD"]
    trade_rules = {'USDT': [], 'FDUSD': []}
    for symbol in info['symbols']:
        for quote_asset in quote_asset_list:
            if symbol['quoteAsset'] == quote_asset:
                symbol_data = {"symbol": symbol['symbol']}
                for filter_dict in symbol['filters']:
                    if filter_dict['filterType'] == "LOT_SIZE":
                        symbol_data['minQty'] = filter_dict['minQty']
                        symbol_data['stepSize'] = filter_dict['stepSize']
                    elif filter_dict['filterType'] == "PRICE_FILTER":
                        symbol_data['tickSize'] = filter_dict['tickSize']
                    elif filter_dict['filterType'] == "NOTIONAL":
                        symbol_data['minNotional'] = filter_dict['minNotional']
                    elif filter_dict['filterType'] == "MARKET_LOT_SIZE":
                        symbol_data['maxQty'] = filter_dict['maxQty']
                    elif filter_dict['filterType'] == "MAX_NUM_ORDERS":
                        symbol_data['maxNumOrders'] = filter_dict['maxNumOrders']
                    elif filter_dict['filterType'] == "MAX_NUM_ALGO_ORDERS":
                        symbol_data['maxNumAlgoOrders'] = filter_dict['maxNumAlgoOrders']
                trade_rules[quote_asset].append(symbol_data)
    print(trade_rules)


def get_trades_info(client):
    info = client.get_recent_trades(symbol='BTCUSDT', limit=1000)
    print(len(info))
    print(info)

def parse_timestamp(ts):
    """
    自动识别时间戳单位（秒、毫秒、微秒、纳秒），并转换为上海时间。

    :param ts: 整数或字符串形式的时间戳
    :return: dict，包含单位、UTC时间、上海时间
    """
    if isinstance(ts, str):
        ts = int(ts)

    length = len(str(ts))
    epoch = datetime(1970, 1, 1)

    if length == 13:
        dt = epoch + timedelta(milliseconds=ts)
        unit = "毫秒"
    elif length == 16:
        dt = epoch + timedelta(microseconds=ts)
        unit = "微秒"
    elif length == 19:
        dt = epoch + timedelta(microseconds=ts // 1000)
        unit = "纳秒（已转为微秒）"
    else:
        dt = epoch + timedelta(seconds=ts)
        unit = "秒"

    dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_shanghai = dt_utc.astimezone(shanghai_tz)

    return {
        "UTC": dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
        "Shanghai": dt_shanghai.strftime("%Y-%m-%d")
    }


if __name__ == '__main__':
    api_key = "VilqADd2OseILGt8bcj8JMlO5hNgQmMQx6mZrRrrkex4czr5bjGUIFo4LVdSIlZE"
    api_secret = "zFUeZEU4HTf4vTCynKZxuQLVSVnxV1qymiHCYdRDQgHaL9pllu1LPj0XZSkWctPQ"
    client = Client(api_key, api_secret)
    # time_res(client)
    get_exchange_info(client)
    get_trades_info(client)
    # print(parse_timestamp(1744156799980))