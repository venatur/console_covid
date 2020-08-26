from config import Config
from datetime import date, timedelta, datetime


class Logger(Config):

    def __init__(self):
        self.path = Config.LOGGER

    def log(self, msg, time):
        with open(self.path, 'a+') as fd:
            now = datetime.now()
            string_header = "Log is due on " + now.strftime("%d/%m/%Y %H:%M:%S") + '\n'
            fd.write('\n' + string_header + '\n' + msg + '\n' + 'Time:' + str(timedelta(seconds=time)) + '\n')
            fd.close()
