import schedule
import time
import datetime
import subprocess

# def yesterday_trade_job():
#     print("Running download script at", datetime.datetime.now())
#     subprocess.run([
#         "/home/anaconda3/envs/trade_data/bin/python",
#         "/home/workplace/trade_data/send_trade_data_yesterday.py"
#     ])
#
# schedule.every().day.at("15:00").do(yesterday_trade_job)

def test():
    print("test ok")

schedule.every().minute.do(test)


print("Scheduler started")
while True:
    schedule.run_pending()
    time.sleep(30)