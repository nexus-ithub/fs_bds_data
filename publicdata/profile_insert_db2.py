import json

import mysql.connector
import pathlib

from utils.logger import create_logger
from utils.files import read_csv



CUR_PATH = pathlib.Path(__file__).parent.resolve()
logger = create_logger(f'{CUR_PATH}/../logs', f'test_insert2', backupCount=10)

EXECUTE_SIZE = 60000
logger.info(f"START {EXECUTE_SIZE}")
count = 0

try:
    file_db = open(f"{CUR_PATH}/../config/hansung_db.json", 'r')
    db = json.load(file_db)


    mysql_con = mysql.connector.connect(host=db["DB_HOST"], port=db["DB_PORT"], database=db["DB_NAME"], user=db["DB_USER"],
                                        password=db["DB_PASS"])
    mysql_con.autocommit = True
    cursor = mysql_con.cursor(dictionary=True)

    sql =("INSERT INTO land_usage_info_test ("
          "id, leg_dong_code, leg_dong_name, div_code, div_name, "
          "jibun, drawing_number, conflict_code, conflict, usage_code, "
          "usage_name, register_date, create_date, sigungu_code, etc) "
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    data_list = []
    for line in read_csv("../data/AL_D155_11_20240504.csv", delimiter=','):
        if line[0] == "고유번호":
            continue

        data_list.append(tuple(line))

        if len(data_list) == EXECUTE_SIZE:
            cursor.executemany(sql, data_list)
            data_list = []

        count += 1

    if len(data_list) > 0:
        cursor.executemany(sql, data_list)

    mysql_con.commit()
    mysql_con.close()

except Exception as e:
    logger.error(f"ERROR {e}")


logger.info(f"END {count}")