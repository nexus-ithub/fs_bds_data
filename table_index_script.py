import argparse
import datetime
import time

import numpy as np
import requests
import xmltodict
import pandas as pd
import json
import mysql.connector
import csv
from util import get_last_months, create_logger

def main(args):
    logger = create_logger(args.logdir, f'table_index_script', backupCount=10)
    file_db = open(f"{args.db}", 'r')
    db = json.load(file_db)
    mysql_con = mysql.connector.connect(host=db["DB_HOST"], port=db["DB_PORT"], database=db["DB_NAME"],
                                        user=db["DB_USER"],
                                        password=db["DB_PASS"])
    cursor = mysql_con.cursor(dictionary=True)



    logger.info(f"create idx_site_loc...")
    cursor.execute(
        "CREATE INDEX `idx_site_loc` ON `fs_bds`.`building_leg_headline` (site_loc) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_sigungu_code...")
    cursor.execute(
        "CREATE INDEX `idx_sigungu_code` ON `fs_bds`.`building_leg_headline` (sigungu_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_land_area...")
    cursor.execute(
        "CREATE INDEX `idx_land_area` ON `fs_bds`.`building_leg_headline` (land_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_total_floor_area...")
    cursor.execute(
        "CREATE INDEX `idx_total_floor_area` ON `fs_bds`.`building_leg_headline` (total_floor_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_use_approval_date...")
    cursor.execute(
        "CREATE INDEX `idx_use_approval_date` ON `fs_bds`.`building_leg_headline` (use_approval_date) COMMENT '' ALGORITHM DEFAULT LOCK NONE")


    logger.info(f"END")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--logdir",
        type=str,
        default='./',
        help="log directory",
    )

    parser.add_argument(
        "--db",
        type=str,
        help="db config file path",
    )

    args = parser.parse_args()

    main(args)