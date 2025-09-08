import json

import mysql.connector
import pathlib

from utils.logger import create_logger


DEBUG = False

ONLY_SEOUL_ADDRESS = True

CUR_PATH = pathlib.Path(__file__).parent.resolve()


logger = create_logger(f'{CUR_PATH}/../logs', f'tuning_db{"_debug" if DEBUG else ""}', backupCount=10)

file_db = open(f"{CUR_PATH}/../config/hansung_db.json", 'r')
db = json.load(file_db)

mysql_con = mysql.connector.connect(host=db["DB_HOST"], port=db["DB_PORT"], database=db["DB_NAME"], user=db["DB_USER"],
                                    password=db["DB_PASS"])


cursor = mysql_con.cursor(dictionary=True)



lastKey = 0
while True:
    cursor.execute(f"SELECT * FROM individual_announced_price WHERE `key` > {lastKey} limit 1000")
    results = cursor.fetchall()
    print(results[0])
    for result in results:
        fields = []
        values = []
        for key, value in result.items():
            if key == "key":
                continue
            fields.append(key)
            values.append(str(value))
            # print(f"key: {key}, value: {value}")

        sql = (f"INSERT INTO individual_announced_price_new "
               f"({', '.join(fields)}) "
               f"VALUES ({', '.join(values)})")

        print(sql)
    break


mysql_con.close()
