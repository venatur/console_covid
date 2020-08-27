from dao_covid import DaoCovid
import schedule
import time
from logger import Logger
from datetime import datetime, date


def job():
    today = str(date.today())
    current_time = time.process_time()
    obj.log('saved at ' + today, current_time)
    DaoCovid()


obj = Logger()
schedule.every(15).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

