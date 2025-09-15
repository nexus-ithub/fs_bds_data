import json
import shutil
import zipfile
import argparse

import mysql.connector
import os
import pathlib
import datetime

from data_utils import get_dataname_by_type
from queries.search import INSERT_SEARCH_INFO, UPDATE_SEARCH_INFO_QUERY
from utils.telegramBot import TelegramBot
from queries.create_sql import CREATE_NEW_INDIVIDUAL_ANNOUNCED_PRICE, CREATE_NEW_LAND_CHAR_INFO, \
    CREATE_NEW_LAND_USAGE_INFO, CREATE_NEW_BUILDING_LEG_HEADLINE, CREATE_NEW_BUILDING_FLOOR_INFO
from queries.address import update_address_info_query
from utils.logger import create_logger
from utils.files import read_csv
import geopandas as gpd
import csv
import asyncio

DEBUG = False

BURST_LIMIT = 10000
TEST_LIMIT = 1000000

ONLY_SEOUL_ADDRESS = True

CUR_PATH = pathlib.Path(__file__).parent.resolve()

strNullProc = lambda x: 'NULL' if x is None else f"'{x}'"

print(f"start ", CUR_PATH)

logger = create_logger(f'{CUR_PATH}/../logs', f'update_data{"_debug" if DEBUG else ""}', backupCount=10)

file_db = open(f"{CUR_PATH}/../config/config.json", 'r')
config = json.load(file_db)
config_name = config["NAME"]
mysql_con = mysql.connector.connect(host=config["DB_HOST"], port=config["DB_PORT"], database=config["DB_NAME"], user=config["DB_USER"],
                                    password=config["DB_PASS"])
mysql_con.autocommit = True

mysql_cursor = mysql_con.cursor(dictionary=True)

bot = TelegramBot(config["TG_TOKEN"], config["TG_CID"])


def get_schema(cursor, type):
    cursor.execute(f'show columns FROM {type};')
    return cursor.fetchall()


def updateDataToTable(cursor, path, type, known_length, header_key, delimeter=',', encoding='cp949', quoting=0):
    logger.info(f"updateToTable {type}")
    update_count = 0
    schema = get_schema(cursor, type)

    datalist = []
    sql = None

    for data in read_csv(path, delimiter=delimeter, encoding=encoding, quoting=quoting):
        values = []
        fields = []
        formats = []
        update_values = []

        if len(data) != known_length:
            raise Exception(f'csv data length is not equal to previous known length known_length : {known_length} , data length : {len(data)}')

        # print (schema[0])
        if schema[0]['Field'] == 'key' and schema[0]['Extra'] == 'auto_increment':
            logger.info(f'remove auto increment key field {schema[0]}')
            schema.pop(0)

        if data[0] == header_key:
            logger.info(f'is header {data}')
            continue

        for i, column_data in enumerate(zip(data, schema)):

            column_value, column_info = column_data
            field = column_info['Field']
            fields.append(field)

            # if 'decimal' in column_info['Type'] or 'int' in column_info['Type']:
            #     value = column_value if len(column_value) > 0 else 'NULL'
            # else:
            #     value = f"'{get_valid_str_value(column_value)}'"
            if 'decimal' in column_info['Type'] or 'int' in column_info['Type']:
                value = column_value if len(column_value) > 0 else 'NULL'
            else:
                value = get_valid_str_value(column_value)

            values.append(value)

            formats.append("%s")
            update_values.append(f"{column_info['Field']} = VALUES({field})")

        datalist.append(tuple(values))

        if DEBUG:
            logger.info(sql)
            break
        else:
            try:
                sql = (f"INSERT INTO {type} "
                       f"({', '.join(fields)}) "
                       f"VALUES ({', '.join(formats)}) "
                       f"ON DUPLICATE KEY UPDATE "
                       f"{', '.join(update_values)};")

                update_count += 1

                if len(datalist) == BURST_LIMIT:
                    logger.info(f'write data update_count = {update_count}')
                    cursor.executemany(sql, datalist)
                    logger.info(f'write data end')
                    datalist = []

                if update_count >= TEST_LIMIT:
                    break

            except Exception as e:
                logger.error(sql)
                raise Exception(f'{e}')

    if sql and len(datalist) > 0:
        logger.info(f'write extra data len = {len(datalist)}')
        cursor.executemany(sql, datalist)
        logger.info(f'write extra data end')

    logger.info(f"update ended {update_count}")
    return update_count


def insertDataToNewTable(cursor, path, type, known_length, header_key, delimiter=',', encoding='cp949', quoting=0):
    logger.info(f"insertDataToNewTable {type}")
    update_count = 0
    schema = get_schema(cursor, type)

    datalist = []
    sql = None

    for data in read_csv(path, delimiter=delimiter, encoding=encoding, quoting=quoting):
        values = []
        fields = []
        formats = []

        if len(data) != known_length:
            logger.info(f'csv data length is not equal to previous known length {len(data)} data : {str(data)}')
            raise Exception(f'csv data length is not equal to previous known length {len(data)}')

        # print (schema[0])
        if schema[0]['Field'] == 'key' and schema[0]['Extra'] == 'auto_increment':
            logger.info(f'remove auto increment key field {schema[0]}')
            schema.pop(0)

        if data[0] == header_key:
            logger.info(f'is header {data}')
            continue

        for i, column_data in enumerate(zip(data, schema)):

            column_value, column_info = column_data
            # column_value = column_value.strip()
            # logger.info(f"column_info = {column_info} , column_value = {column_value}")
            field = column_info['Field']
            fields.append(field)

            if 'decimal' in column_info['Type'] or 'int' in column_info['Type']:
                if type == 'building_floor_info':
                    value = column_value if len(column_value) > 0 else 0
                else:
                    value = column_value if len(column_value) > 0 else None
            else:
                value = get_valid_str_value(column_value)

            values.append(value)
            formats.append("%s")

        datalist.append(tuple(values))

        if DEBUG:
            logger.info(sql)
            break
        else:
            try:
                sql = (f"INSERT INTO {type}_new "
                       f"({', '.join(fields)}) "
                       f"VALUES ({', '.join(formats)})")

                update_count += 1

                if len(datalist) == BURST_LIMIT:
                    logger.info(f'write data update_count = {update_count}')
                    cursor.executemany(sql, datalist)
                    logger.info(f'write data end')
                    datalist = []

                # if update_count >= TEST_LIMIT:
                #     break

            except Exception as e:
                logger.error(sql)
                raise Exception(f'{e}')

    if sql and len(datalist) > 0:
        logger.info(f'write extra data len = {len(datalist)}')
        cursor.executemany(sql, datalist)
        logger.info(f'write extra data end')

    logger.info(f"update ended {update_count}")
    return update_count


def exist_table(cursor, table_name):
    cursor.execute(
        f"SELECT count(*) as count FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'fs_bds') AND (TABLE_NAME = '{table_name}')")
    result_exist = cursor.fetchone()
    return result_exist['count'] > 0


def drop_table_if_exist(cursor, table_name):
    exist = exist_table(cursor, table_name)

    if exist:
        logger.info(f"drop prev table start {table_name}")
        cursor.execute(f"drop table {table_name}")
        logger.info(f"drop prev table end {table_name}")


def change_as_new_table(cursor, table_name):
    logger.info(f"table {table_name} change start")

    drop_table_if_exist(cursor, f"{table_name}_old")

    cursor.execute(
        f"RENAME TABLE {table_name} to {table_name}_old, {table_name}_new to {table_name}")
    logger.info(f"table {table_name} change end")


def create_new_table(cursor, type):
    new_table_name = f"{type}_new"

    drop_table_if_exist(cursor, new_table_name)

    logger.info("create new table")
    create_sql = None

    if type == "individual_announced_price":
        create_sql = CREATE_NEW_INDIVIDUAL_ANNOUNCED_PRICE
    elif type == "land_char_info":
        create_sql = CREATE_NEW_LAND_CHAR_INFO
    elif type == "land_usage_info":
        create_sql = CREATE_NEW_LAND_USAGE_INFO
    elif type == "building_leg_headline":
        create_sql = CREATE_NEW_BUILDING_LEG_HEADLINE
    elif type == "building_floor_info":
        create_sql = CREATE_NEW_BUILDING_FLOOR_INFO

    if create_sql:
        logger.info("create new table start")
        cursor.execute(create_sql)
        logger.info("create new table end")
    else:
        raise Exception(f'{type} not implemented table!!')


def get_valid_str_value(value):
    return value.replace("\\", "").replace("'", "\\'")


def update_address_data(cursor, path, type):
    logger.info(f"updating ... ({type}) {path}")
    update_count = 0
    filename = os.path.basename(path)

    for data in read_csv(path):
        for i, item in enumerate(data):
            data[i] = item.replace("'", "\\'")

        # print(f"data {data}")
        if ONLY_SEOUL_ADDRESS and not data[0].startswith('11'):
            # print(f"not seoul ", data)
            continue

        query = update_address_info_query(filename, data)

        if query:
            # print(f"query {query}")
            cursor.execute(query)
            update_count += 1

    logger.info(f"update ended {update_count}")
    return update_count


def update_land_char_data(cursor, path, type):
    logger.info(f"updating land char info data")

    file_size = os.stat(path).st_size
    file_size_in_gigabytes = file_size / (1024 * 1024 * 1024)
    logger.info(f"file_size_in_gigabytes {file_size_in_gigabytes}")
    if file_size_in_gigabytes >= 1.5:
        create_new_table(cursor, type)
        update_count = insertDataToNewTable(cursor, path, type, 26, "고유번호")

        # indexing
        logger.info(f"create idx_id_year_month...")
        cursor.execute("CREATE UNIQUE INDEX `idx_id_year_month`  ON `fs_bds`.`land_char_info_new` (id, year, month) COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT")
        logger.info(f"create idx_leg_dong_code...")
        cursor.execute("CREATE INDEX `idx_leg_dong_code` ON `fs_bds`.`land_char_info_new` (leg_dong_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_leg_dong_name...")
        cursor.execute("CREATE INDEX `idx_leg_dong_name` ON `fs_bds`.`land_char_info_new` (leg_dong_name) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_jibun...")
        cursor.execute("CREATE INDEX `idx_jibun` ON `fs_bds`.`land_char_info_new` (jibun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_jimok_name...")
        cursor.execute("CREATE INDEX `idx_jimok_name` ON `fs_bds`.`land_char_info_new` (jimok_name) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_area...")
        cursor.execute("CREATE INDEX `idx_area` ON `fs_bds`.`land_char_info_new` (area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_usage1_name...")
        cursor.execute("CREATE INDEX `idx_usage1_name` ON `fs_bds`.`land_char_info_new` (usage1_name) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_create_date...")
        cursor.execute("CREATE INDEX `idx_create_date` ON `fs_bds`.`land_char_info_new` (create_date desc) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

        change_as_new_table(cursor, "land_char_info")

    else:
        update_count = updateDataToTable(cursor, path, type, 26, "고유번호")

    logger.info(f"update land char info end")
    return update_count


def update_land_usage_data(cursor, path, type):
    logger.info(f"updating land usage info data")

    file_size = os.stat(path).st_size
    file_size_in_gigabytes = file_size / (1024 * 1024 * 1024)
    logger.info(f"file_size_in_gigabytes {file_size_in_gigabytes}")
    if file_size_in_gigabytes >= 1.0:
        create_new_table(cursor, type)
        update_count = insertDataToNewTable(cursor, path, type, 15, "고유번호")

        logger.info(f"create idx_id...")
        cursor.execute("CREATE INDEX `idx_id` ON `fs_bds`.`land_usage_info_new` (id) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_leg_dong_code...")
        cursor.execute("CREATE INDEX `idx_leg_dong_code` ON `fs_bds`.`land_usage_info_new` (leg_dong_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_jibun...")
        cursor.execute("CREATE INDEX `idx_jibun` ON `fs_bds`.`land_usage_info_new` (jibun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

        change_as_new_table(cursor, "land_usage_info")

    else:
        update_count = updateDataToTable(cursor, path, type, 15, "고유번호")

    logger.info(f"update land usage info end")

    return update_count


def update_building_leg_headline(cursor, path, type):
    logger.info(f"updating {type} data")

    file_size = os.stat(path).st_size
    file_size_in_gigabytes = file_size / (1024 * 1024 * 1024)
    logger.info(f"file_size_in_gigabytes {file_size_in_gigabytes}")
    create_new_table(cursor, type)
    update_count = insertDataToNewTable(cursor, path, type, 77, "", delimiter='|', encoding='utf-8', quoting=csv.QUOTE_NONE)

    logger.info(f"make leg_dong_code_val...")
    cursor.execute(
        "UPDATE fs_bds.building_leg_headline_new SET leg_dong_code_val = concat(sigungu_code, leg_dong_code)")

    logger.info(f"create idx_bun...")
    cursor.execute(
        "CREATE INDEX `idx_bun` ON `fs_bds`.`building_leg_headline_new` (bun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    logger.info(f"create idx_ji...")
    cursor.execute(
        "CREATE INDEX `idx_ji` ON `fs_bds`.`building_leg_headline_new` (ji) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    logger.info(f"create idx_leg_dong_code...")
    cursor.execute(
        "CREATE INDEX `idx_leg_dong_code_val` ON `fs_bds`.`building_leg_headline_new` (leg_dong_code_val) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # make building search table
    # drop_table_if_exist(cursor, "building_search_new")

    # logger.info(f"create building_search_new...")
    # cursor.execute(
    #     "create table fs_bds.building_search_new (primary key (building_id)) "
    #     "AS SELECT building_id, site_loc, sigungu_code, bun, ji, leg_dong_code_val, use_approval_date, total_floor_area, land_area "
    #     "FROM fs_bds.building_leg_headline")

    # logger.info(f"create idx_site_loc...")
    # cursor.execute(
    #     "CREATE INDEX `idx_site_loc` ON `fs_bds`.`building_search_new` (site_loc) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_sigungu_code...")
    # cursor.execute(
    #     "CREATE INDEX `idx_sigungu_code` ON `fs_bds`.`building_search_new` (sigungu_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_land_area...")
    # cursor.execute(
    #     "CREATE INDEX `idx_land_area` ON `fs_bds`.`building_search_new` (land_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_total_floor_area...")
    # cursor.execute(
    #     "CREATE INDEX `idx_total_floor_area` ON `fs_bds`.`building_search_new` (total_floor_area) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # logger.info(f"create idx_use_approval_date...")
    # cursor.execute(
    #     "CREATE INDEX `idx_use_approval_date` ON `fs_bds`.`building_search_new` (use_approval_date) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    # logger.info(f"create idx_bun...")
    # cursor.execute(
    #     "CREATE INDEX `idx_bun` ON `fs_bds`.`building_search_new` (bun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    # logger.info(f"create idx_ji...")
    # cursor.execute(
    #     "CREATE INDEX `idx_ji` ON `fs_bds`.`building_search_new` (ji) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    # logger.info(f"create idx_leg_dong_code...")
    # cursor.execute(
    #     "CREATE INDEX `idx_leg_dong_code_val` ON `fs_bds`.`building_search_new` (leg_dong_code_val) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    # change_as_new_table(cursor, "building_search")

    change_as_new_table(cursor, "building_leg_headline")

    logger.info(f"update {type} end")

    return update_count


def update_building_floor_info(cursor, path, type):
    logger.info(f"updating {type} data")

    file_size = os.stat(path).st_size
    file_size_in_gigabytes = file_size / (1024 * 1024 * 1024)
    logger.info(f"file_size_in_gigabytes {file_size_in_gigabytes}")
    create_new_table(cursor, type)
    update_count = insertDataToNewTable(cursor, path, type, 33, "", delimiter='|', encoding='utf-8', quoting=csv.QUOTE_NONE)

    logger.info(f"make leg_dong_code_val...")
    cursor.execute(
        "UPDATE fs_bds.building_floor_info_new SET leg_dong_code_val = concat(sigungu_code, leg_dong_code)")

    logger.info(f"create idx_building_id...")
    cursor.execute(
        "CREATE INDEX `idx_building_id` ON `fs_bds`.`building_floor_info_new` (building_id) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_bun...")
    cursor.execute(
        "CREATE INDEX `idx_bun` ON `fs_bds`.`building_floor_info_new` (bun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
    logger.info(f"create idx_ji...")
    cursor.execute(
        "CREATE INDEX `idx_ji` ON `fs_bds`.`building_floor_info_new` (ji) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    logger.info(f"create idx_leg_dong_code...")
    cursor.execute(
        "CREATE INDEX `idx_leg_dong_code_val` ON `fs_bds`.`building_floor_info_new` (leg_dong_code_val) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

    change_as_new_table(cursor, "building_floor_info")

    logger.info(f"update {type} end")

    return update_count


def update_individual_announce_price_data(cursor, path, type):
    logger.info(f"updating announced price data")

    file_size = os.stat(path).st_size
    file_size_in_gigabytes = file_size / (1024 * 1024 * 1024)
    logger.info(f"file_size_in_gigabytes {file_size_in_gigabytes}")
    if file_size_in_gigabytes >= 1.5:
        create_new_table(cursor, type)
        update_count = insertDataToNewTable(cursor, path, type, 13, "고유번호")

        # indexing
        logger.info(f"create idx_id_year_month...")
        cursor.execute("CREATE UNIQUE INDEX `idx_id_year_month`  ON `fs_bds`.`individual_announced_price_new` (id, year, month) COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT")
        logger.info(f"create idx_leg_dong_code...")
        cursor.execute("CREATE INDEX `idx_leg_dong_code` ON `fs_bds`.`individual_announced_price_new` (leg_dong_code) COMMENT '' ALGORITHM DEFAULT LOCK NONE")
        logger.info(f"create idx_jibun...")
        cursor.execute("CREATE INDEX `idx_jibun` ON `fs_bds`.`individual_announced_price_new` (jibun) COMMENT '' ALGORITHM DEFAULT LOCK NONE")

        change_as_new_table(cursor, "individual_announced_price")

    else:
        update_count = updateDataToTable(cursor, path, type, 13, "고유번호")

    return update_count


def update_address_polygon_data(cursor, path):
    if pathlib.Path(path).suffix.lower() != '.shp':
        logger.info(f"not shp file... skipping {path}")
        return 0

    logger.info(f"updating polygon data {path}")

    update_count = 0

    data = gpd.read_file(path, encoding='euc-kr')
    if DEBUG:
        logger.info(data.head())

    data['center_point'] = data['geometry'].geometry.centroid
    data['geometry'] = data['geometry'].to_crs(epsg=4326)
    data['center_point'] = data['center_point'].to_crs(epsg=4326)
    data['lng'] = data['center_point'].map(lambda x: x.xy[0][0])
    data['lat'] = data['center_point'].map(lambda x: x.xy[1][0])

    datalist = []
    sql = None

    for item in data.iterrows():

        row = item[1]

        if len(row) != 12:
            raise Exception(f'data length is not equal to previous known length {len(data)}')
        # if DEBUG:
        #     logger.info(f"{len(row)} , {row}")

        sql = (f"INSERT INTO fs_bds.address_polygon ( "
               f"id, leg_dong_code, leg_dong_name, jibun, lat, lng, polygon) "
               f"VALUES ( "
               f"%s, %s, %s, %s,"
               f"%s, %s, "
               f"st_geomfromtext(%s) "
               f") on duplicate key update "
               f"leg_dong_code = VALUES(leg_dong_code), leg_dong_name = VALUES(leg_dong_name), jibun = VALUES(jibun),"
               f" lat=VALUES(lat), lng=VALUES(lng), polygon=VALUES(polygon);")
        if DEBUG:
            logger.info(sql)
            break
        else:
            try:
                datalist.append((
                    row['A1'],
                    row['A2'],
                    row['A3'],
                    row['A4'],
                    row['lat'],
                    row['lng'],
                    f"{row['geometry']}",
                ))

                update_count += 1
                if len(datalist) == BURST_LIMIT:
                    logger.info(f'write data update_count = {update_count}')
                    cursor.executemany(sql, datalist)
                    logger.info(f'write data end')
                    datalist = []

            except Exception as e:
                logger.error(sql)
                raise Exception(f'{e}')

    if sql and len(datalist) > 0:
        logger.info(f'write extra data len = {len(datalist)}')
        cursor.executemany(sql, datalist)
        logger.info(f'write extra data end')

    logger.info(f"update ended {update_count}")
    return update_count


def update_distinct_polygon_data(cursor, path):
    if pathlib.Path(path).suffix.lower() != '.shp':
        logger.info(f"not shp file... skipping {path}")
        return 0

    logger.info(f"updating distinct polygon data {path}")

    update_count = 0

    data = gpd.read_file(path, encoding='utf-8')
    if DEBUG:
        logger.info(data.head())

    data['center_point'] = data['geometry'].geometry.centroid
    data['geometry'] = data['geometry'].to_crs(epsg=4326)
    data['center_point'] = data['center_point'].to_crs(epsg=4326)
    data['lng'] = data['center_point'].map(lambda x: x.xy[0][0])
    data['lat'] = data['center_point'].map(lambda x: x.xy[1][0])

    datalist = []
    sql = None

    for item in data.iterrows():

        row = item[1]

        if len(row) != 15:
            raise Exception(f'data length is not equal to previous known length {len(data)}')
        if DEBUG:
            logger.info(f"{len(row)} , {row}")

        sql = (f"INSERT INTO fs_bds.district_polygon ( "
               f"code, code_name, div_code, div_code_name, "
               f"sigungu_code, sigungu_name, leg_dong_code, leg_dong_name, "
               f"area, lat, lng, "
               f"polygon) "
               f"VALUES ( "
               f"%s, %s, %s, %s,"
               f"%s, %s, %s, %s,"
               f"%s, %s, %s, "
               f"st_geomfromtext(%s) "
               f") on duplicate key update code_name=VALUES(code_name),div_code=VALUES(div_code),div_code_name=VALUES(div_code_name),"
               f"sigungu_code=VALUES(sigungu_code),sigungu_name=VALUES(sigungu_name),leg_dong_code=VALUES(leg_dong_code),leg_dong_name=VALUES(leg_dong_name),"
               f"area=VALUES(area),lat=VALUES(lat),lng=VALUES(lng),"
               f"polygon=VALUES(polygon);")

        if DEBUG:
            logger.info(sql)
            break
        else:
            try:
                datalist.append((
                    row['TRDAR_CD'],
                    row['TRDAR_CD_N'],
                    row['TRDAR_SE_C'],
                    row['TRDAR_SE_1'],
                    row['SIGNGU_CD'],
                    row['SIGNGU_CD_'],
                    row['ADSTRD_CD'],
                    row['ADSTRD_CD_'],
                    row['RELM_AR'],
                    row['lat'],
                    row['lng'],
                    f"{row['geometry']}",
                ))

                update_count += 1
                if len(datalist) == BURST_LIMIT:
                    logger.info(f'write data update_count = {update_count}')
                    cursor.executemany(sql, datalist)
                    logger.info(f'write data end')
                    datalist = []

            except Exception as e:
                logger.error(sql)
                raise Exception(f'{e}')

    if sql and len(datalist) > 0:
        logger.info(f'write extra data len = {len(datalist)}')
        cursor.executemany(sql, datalist)
        logger.info(f'write extra data end')

    logger.info(f"update ended {update_count}")
    return update_count


def update_data(cursor, path, type):
    if type == 'address' or type == 'building_addr':
        return update_address_data(cursor, path, type)
    elif type == 'building_leg_headline':
        return update_building_leg_headline(cursor, path, type)
    elif type == 'building_floor_info':
        return update_building_floor_info(cursor, path, type)
    elif type == 'land_info':
        return updateDataToTable(cursor, path, type, 16, "고유번호")
    elif type == 'land_char_info':
        return update_land_char_data(cursor, path, type)
    elif type == 'land_usage_info':
        return update_land_usage_data(cursor, path, type)
    elif type == 'individual_announced_price':
        return update_individual_announce_price_data(cursor, path, type)
    elif type == 'address_polygon':
        return update_address_polygon_data(cursor, path)
    elif type == 'district_polygon':
        return update_distinct_polygon_data(cursor, path)
    elif type == 'district_foot_traffic':
        return updateDataToTable(cursor, path, type, 27, "기준_년분기_코드")
    elif type == 'district_office_workers':
        return updateDataToTable(cursor, path, type, 26, "기준_년분기_코드")
    elif type == 'district_resident':
        return updateDataToTable(cursor, path, type, 29, "기준_년분기_코드")
    elif type == 'district_resident_alltime':
        return updateDataToTable(cursor, path, type, 29, "기준_년분기_코드")
    elif type == 'district_foot_traffic_seoul':
        return updateDataToTable(cursor, path, type, 25, "기준_년분기_코드")
    elif type == 'leg_dong_codes':
        return updateDataToTable(cursor, path, type, 3, "법정동코드", delimeter="\t")


def update_search_table(cursor):
    logger.info(f'INSERT_SEARCH_INFO ...')
    cursor.execute(INSERT_SEARCH_INFO)
    logger.info(f'UPDATE_SEARCH_INFO_QUERY ...')
    cursor.execute(UPDATE_SEARCH_INFO_QUERY)


async def process_single_file(file_path: str, file_type: str, file_id: str = None, memo: str = None):
    """단일 파일을 처리하는 함수"""
    update_failed = False
    update_count = 0
    tmp_dest = None

    try:
        if file_id and not DEBUG:
            sql_update = f"UPDATE fs_bds.public_data_files SET update_status = 'R', update_start_date = now() WHERE id = '{file_id}'"
            logger.info(f'sql update {sql_update}')
            mysql_cursor.execute(sql_update)

        if not DEBUG and memo:
            await bot.send_message(f"[{config_name}] 데이터 업데이트 시작 > {get_dataname_by_type(file_type)}, memo : {memo}")

        filename = os.path.basename(file_path)
        if pathlib.Path(filename).suffix.lower() == '.zip':
            tmp_dest = f'{CUR_PATH}/data/tmp_{datetime.datetime.today().strftime("%Y-%m-%d_%H_%M_%S")}'
            logger.info(f"unzip file -> {filename} to {tmp_dest}")

            with zipfile.ZipFile(file_path, 'r') as zf:
                logger.info('open success')
                zipinfo = zf.infolist()
                logger.info(f'zipinfo {zipinfo}')
                for info in zipinfo:
                    info.filename = info.filename.encode('cp437', errors='replace').decode('euc-kr', errors='replace')
                    logger.info(f"unzip ... {info.filename}")
                    zf.extract(info, path=tmp_dest)

                for info in zipinfo:
                    update_count += update_data(mysql_cursor, f"{tmp_dest}/{info.filename}", file_type)

                zf.close()
        else:
            logger.info(f"update data -> {file_path}")
            update_count = update_data(mysql_cursor, file_path, file_type)

    except Exception as e:
        logger.error(f'update failed {e}')
        if not DEBUG and memo:
            await bot.send_message(f"[{config_name}] 데이터 업데이트 실패 > {get_dataname_by_type(file_type)}, memo : {memo}, error : {e}")
        update_failed = True

    finally:
        logger.info(f'end {update_count}')
        if not update_failed:
            if not DEBUG and memo:
                await bot.send_message(f"[{config_name}] 데이터 업데이트 완료!! > {get_dataname_by_type(file_type)}, memo : {memo}, update_count : {update_count}")

        if file_id and not DEBUG:
            sql_update = f"UPDATE fs_bds.public_data_files SET update_status = '{'F' if update_failed else 'Y'}', update_end_date = now(), update_count = {update_count} WHERE id = '{file_id}'"
            logger.info(f'sql update {sql_update}')
            mysql_cursor.execute(sql_update)

        if tmp_dest is not None:
            shutil.rmtree(tmp_dest)

        # 수동 모드에서는 파일을 삭제하지 않음
        if not update_failed and file_path is not None and file_id:
            os.remove(file_path)

    return update_count, update_failed


async def main_db_mode():
    """DB 기반 처리 모드"""
    sql = "SELECT * FROM fs_bds.public_data_files WHERE cancel_yn = 'N' and update_status != 'Y' and update_status != 'R' order by created_at asc;"

    mysql_cursor.execute(sql)
    files = mysql_cursor.fetchall()

    logger.info(f"fetch.. files : {len(files)}")

    need_search_db_update = False

    for file in files:
        file_id = file['id']
        file_type = file['type']
        file_path = file['path']
        memo = file['memo']

        update_count, update_failed = await process_single_file(file_path, file_type, file_id, memo)

        if not update_failed:
            if file_type == 'building_addr' or file_type == 'land_info':
                need_search_db_update = True

    if need_search_db_update:
        try:
            await bot.send_message(f"[{config_name}] 검색DB 업데이트 시작")
            update_search_table(mysql_cursor)
            await bot.send_message(f"[{config_name}] 검색DB 업데이트 완료")
        except Exception as e:
            await bot.send_message(f"[{config_name}] 검색DB 업데이트 실패")

    mysql_con.commit()


async def main_manual_mode(file_path: str, file_type: str):
    """수동 파일 처리 모드"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    logger.info(f"수동 모드: 파일 {file_path}, 타입 {file_type} 처리 시작")

    update_count, update_failed = await process_single_file(file_path, file_type)

    if update_failed:
        logger.error(f"파일 처리 실패: {file_path}")
        return False
    else:
        logger.info(f"파일 처리 완료: {file_path}, 업데이트 건수: {update_count}")

        # 검색 DB 업데이트가 필요한 타입인지 확인
        if file_type == 'building_addr' or file_type == 'land_info':
            try:
                logger.info("검색DB 업데이트 시작")
                update_search_table(mysql_cursor)
                logger.info("검색DB 업데이트 완료")
            except Exception as e:
                logger.error(f"검색DB 업데이트 실패: {e}")

        mysql_con.commit()
        return True


async def main():
    parser = argparse.ArgumentParser(description="공공데이터 업데이트 스크립트")
    parser.add_argument("--file_path", help="처리할 파일 경로 (수동 모드)")
    parser.add_argument("--type", help="데이터 타입 (land_info, land_char, building_addr, address 등)")
    parser.add_argument("--mode", choices=["db", "manual"], default="db",
                       help="실행 모드: db (DB 기반), manual (수동 파일 처리)")

    args = parser.parse_args()

    if args.mode == "manual":
        if not args.file_path or not args.type:
            parser.error("수동 모드에서는 --file-path와 --type이 필요합니다")

        success = await main_manual_mode(args.file_path, args.type)
        if not success:
            exit(1)
    else:
        await main_db_mode()

    mysql_con.close()


if __name__ == '__main__':
    asyncio.run(main())
