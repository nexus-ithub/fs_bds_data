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



    # logger.info(f"create idx_site_loc...")
    # cursor.execute(
    #     "CREATE INDEX `idx_site_loc` ON `fs_bds`.`building_leg_headline` (site_loc) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_sigungu_code...")
    # cursor.execute(
    #     "CREATE INDEX `idx_sigungu_code` ON `fs_bds`.`building_leg_headline` (sigungu_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_land_area...")
    # cursor.execute(
    #     "CREATE INDEX `idx_land_area` ON `fs_bds`.`building_leg_headline` (land_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_total_floor_area...")
    # cursor.execute(
    #     "CREATE INDEX `idx_total_floor_area` ON `fs_bds`.`building_leg_headline` (total_floor_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_use_approval_date...")
    # cursor.execute(
    #     "CREATE INDEX `idx_use_approval_date` ON `fs_bds`.`building_leg_headline` (use_approval_date) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info("drop search info table")
    # cursor.execute("DROP TABLE `fs_bds`.`address_search_info`")

    # logger.info("create search info table")
    # cursor.execute("""
    #     CREATE TABLE `address_search` (
    #     `id` varchar(19) NOT NULL,
    #     `jibun` varchar(150) NOT NULL,
    #     `road` varchar(150) NOT NULL,
    #     `building_name` varchar(128) NOT NULL,
    #     `key_jibun`      VARCHAR(256)
    #         GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(jibun),' ',''),'-','')) STORED,
    #     `key_road`       VARCHAR(256)
    #         GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(road),' ',''),'-','')) STORED,
    #     `key_building`   VARCHAR(256)
    #         GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(building_name),' ',''),'-','')) STORED,
    #     `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
    #     `created_at` datetime DEFAULT current_timestamp(),
    #     PRIMARY KEY (`id`),
    #     KEY `idx_jibun` (`key_jibun`),
    #     KEY `idx_road` (`key_road`),
    #     KEY `idx_building` (`key_building`)
    #     );
    # """)
    
    # logger.info(f"create idx_leg_dong_code...")
    # cursor.execute(
        # "CREATE INDEX `idx_leg_dong_code` ON `fs_bds`.`jibun_info` (leg_dong_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_search...")
    # cursor.execute("CREATE INDEX `idx_search` ON `fs_bds`.`jibun_info` (leg_dong_code, jibun_main_num, jibun_sub_num) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"update search table")
    cursor.execute(
        """
          INSERT INTO address_search (
            id,
            jibun,
            road,
            building_name
          )
          SELECT
            li.id AS id,
            /* 지번 주소: 동리명 + 공백 + 지번 */
            CONCAT(COALESCE(li.leg_dong_name, ''), ' ', li.jibun) AS jibun,
            /* 도로명 주소: road_name 없으면 공란, 있으면 "시도 시군구 도로명 건물주번[-부번]" */
            CASE
              WHEN ri.road_name IS NULL OR ri.road_name = '' THEN ''
              ELSE
                CASE
                  WHEN addr.building_main_num IS NULL THEN
                    /* 건물번호가 없으면 번호 없이 앞부분만 */
                    CONCAT_WS(' ', jb.sido_name, jb.sigungu_name, ri.road_name)
                  ELSE
                    /* 건물번호가 있으면 주번[-부번]까지 */
                    CONCAT_WS(
                      ' ',
                      jb.sido_name,
                      jb.sigungu_name,
                      ri.road_name,
                      CONCAT(
                        addr.building_main_num,
                        CASE
                          WHEN COALESCE(addr.building_sub_num, 0) > 0
                            THEN CONCAT('-', addr.building_sub_num)
                          ELSE ''
                        END
                      )
                    )
                END
            END AS road,
            /* 빌딩명: localBuildingName 우선, 없으면 buildingLegName, 그래도 없으면 공란 */
            COALESCE(inf.local_building_name, inf.building_leg_name, '') AS building_name
          FROM land_info AS li
          LEFT JOIN jibun_info AS jb
            ON jb.leg_dong_code = li.leg_dong_code
          AND jb.jibun_main_num = SUBSTRING_INDEX(li.jibun, '-', 1)
          AND jb.jibun_sub_num = CASE
              WHEN li.jibun LIKE '%-%' THEN SUBSTRING_INDEX(li.jibun, '-', -1)
              ELSE '0'
            END
          LEFT JOIN address_info AS addr
            ON addr.address_id = jb.address_id
          LEFT JOIN additional_info AS inf
            ON inf.address_id = addr.address_id
          LEFT JOIN road_code_info AS ri
            ON addr.road_name_code = ri.road_name_code
          AND addr.eupmyeondong_serial_num = ri.eupmyeondong_serial_num
          GROUP BY li.id 
          ON DUPLICATE KEY UPDATE
            jibun         = VALUES(jibun),
            road          = VALUES(road),
            building_name = VALUES(building_name);        
        """
    )

    logger.info(f"create idx_jibun...")
    cursor.execute("CREATE INDEX `idx_jibun` ON `fs_bds`.`address_search` (key_jibun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    logger.info(f"create idx_road...")
    cursor.execute("CREATE INDEX `idx_road` ON `fs_bds`.`address_search` (key_road) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    logger.info(f"create idx_building...")
    cursor.execute("CREATE INDEX `idx_building` ON `fs_bds`.`address_search` (key_building) COMMENT '' ALGORITHM DEFAULT LOCK NONE")


    logger.info(f"create sub_leg_dong_code_val...")
    cursor.execute(
        "UPDATE fs_bds.building_sub_addr_new SET sub_leg_dong_code_val = concat(sub_sigungu_code, sub_leg_dong_code)")

    logger.info(f"create idx_sub_bunji...")
    cursor.execute(
        "CREATE INDEX `idx_sub_bunji` ON `fs_bds`.`building_sub_addr_new` (sub_bun, sub_ji, sub_leg_dong_code_val) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

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