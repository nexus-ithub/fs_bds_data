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


mysql_cursor = mysql_con.cursor(dictionary=True)

# create unique index for land_info
sql_list = [
    # "create table fs_bds.building_search_new (primary key (building_id)) "
    # "AS SELECT building_id, site_loc, sigungu_code, bun, ji, leg_dong_code_val, use_approval_date, total_floor_area, land_area "
    # "FROM fs_bds.building_leg_headline",
    # "CREATE INDEX `idx_sigungu_code` ON `fs_bds`.`building_search_new` (sigungu_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_land_area` ON `fs_bds`.`building_search_new` (land_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_total_floor_area` ON `fs_bds`.`building_search_new` (total_floor_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_use_approval_date` ON `fs_bds`.`building_search_new` (use_approval_date) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_bun` ON `fs_bds`.`building_search_new` (bun) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_ji` ON `fs_bds`.`building_search_new` (ji) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE INDEX `idx_leg_dong_code_val` ON `fs_bds`.`building_search_new` (leg_dong_code_val) COMMENT '' ALGORITHM DEFAULT LOCK NONE"
    # "CREATE INDEX `idx_site_loc` ON `fs_bds`.`building_search_new` (site_loc) COMMENT '' ALGORITHM DEFAULT LOCK NONE"

# "CREATE UNIQUE INDEX `idx_id_year_month`  ON `fs_bds`.`land_char_info` (id, year, month) COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT",
    # "CREATE INDEX `idx_create_date` ON `fs_bds`.`land_char_info` (create_date desc) COMMENT '' ALGORITHM DEFAULT LOCK NONE",
    # "CREATE UNIQUE INDEX `idx_id_year_month`  ON `fs_bds`.`individual_announced_price` (id, year, month) COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT"
]

for sql in sql_list:
    try:
        logger.info(f"{sql}")
        logger.info(f"START")
        mysql_cursor.execute(sql)
        logger.info(f"END")
    except Exception as e:
        logger.error(f"error to tuning {e}")


mysql_con.commit()
mysql_con.close()
