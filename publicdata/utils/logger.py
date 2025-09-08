import csv
from dateutil.relativedelta import relativedelta
import logging
import sys
from datetime import datetime
import os
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


def create_logger(log_dir, name, backupCount=30):
  log_path = Path(log_dir)
  log_path.mkdir(parents=True, exist_ok=True)
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  formatter = logging.Formatter('[%(asctime)s] %(levelname)s:%(message)s', '%Y/%m/%d %I:%M:%S %p')

  logger = logging.getLogger()

  # file_handler = logging.FileHandler('naverCatCollector.log'
  #                                    , encoding='utf-8')
  filename = (log_path / f'{name}.log')
  file_handler = TimedRotatingFileHandler(filename=filename,
                                          when='D', interval=1, backupCount=backupCount, delay=False,
                                          encoding='utf-8')
  # file_handler = RotatingFileHandler(filename=(log_path / f'{name}.log'), maxBytes=10*1024*1024, backupCount=5)

  file_handler.setFormatter(formatter)
  file_handler.setLevel(logging.DEBUG)
  logger.addHandler(file_handler)

  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)

  now = datetime.now().date()
  file_date = datetime.fromtimestamp(os.path.getmtime(filename)).date()
  if file_date != now:
    logger.debug("Date changed with no events - rotating the log...")
    file_handler.doRollover()

  return logger