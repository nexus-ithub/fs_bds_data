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
import ssl
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        try:
            ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        except Exception:
            pass
        try:
            ctx.options |= ssl.OP_NO_COMPRESSION
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        try:
            ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        except Exception:
            pass
        try:
            ctx.options |= ssl.OP_NO_COMPRESSION
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().proxy_manager_for(*args, **kwargs)

session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = TLS12Adapter(max_retries=retries)
session.mount("https://", adapter)
session.verify = certifi.where()


# 국토교통부_상업업무용 부동산 매매 신고 자료
# http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc
# http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade


integer_columns = ['dealYear', 'dealMonth', 'dealDay', 'floor', 'buildYear',
                        'dealAmount', '보증금액', '보증금', '월세금액', '월세', '종전계약보증금', '종전계약월세']
float_columns = ['전용면적', '대지권면적',
                      'plottageAr', '연면적', '계약면적', 'buildingAr', 'dealArea']

url = 'https://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade'
# columns = ['지역코드', '시군구', '법정동', '지번', '용도지역', '지목', '거래면적', '거래금액', '구분', '년', '월', '일', '거래유형', '중개사소재지', '해제사유발생일',
#            '해제여부']
columns = [
    'sggCd',          # 지역코드
    'sggNm',          # 시군구
    'umdNm',          # 법정동
    'jibun',          # 지번
    'jimok',          # 지목
    'landUse',        # 용도지역
    'dealYear',       # 계약년도
    'dealMonth',      # 계약월
    'dealDay',        # 계약일
    'dealArea',       # 거래면적
    'dealAmount',     # 거래금액(만원)
    'shareDealingType', # 지분거래구분
    'cdealType',      # 해제여부
    'cdealDay',       # 해제사유발생일
    'dealingGbn',     # 거래유형(중개및직거래여부)
    'estateAgentSggNm' # 중개사소재지(시군구단위)
]

end_month = '200601'


fail_count = 0
total_count = 0
useKeyIndex = 0
insert_count = 0
request_count = 0

def collect(logger, mysql_con, mysql_cursor, region_code, date, public_keys):
    global useKeyIndex
    global total_count
    global fail_count
    global insert_count
    global request_count


    logger.info(f"public_keys {public_keys}")
    logger.info(f"###############################################")
    logger.info(f"#################START {region_code} {date} #################")

    for i in range(len(public_keys)):
        try:
            df = pd.DataFrame(columns=columns)

            params = {'serviceKey': public_keys[i],
                      'LAWD_CD': region_code,
                      'DEAL_YMD': date,
                      'numOfRows': 99999
                      }
            request_count += 1
            # response = requests.get(url, params=params, verify=False)
            response = session.get(url, params=params, timeout=10)

            res_json = xmltodict.parse(response.text)
            if res_json['response']['header']['resultCode'] != '000':
                error_message = res_json['response']['header']['resultMsg']
                raise Exception(error_message)
            items = res_json['response']['body']['items']
            if not items:
                return True

            data = items['item']
            if isinstance(data, list):
                sub = pd.DataFrame(data)
            elif isinstance(data, dict):
                sub = pd.DataFrame([data])
            df = pd.concat([df, sub], axis=0, ignore_index=True)

            try:
                for col in integer_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].apply(
                            lambda x: x.strip().replace(",", "") if x is not None and not pd.isnull(x) else x)).astype("Int64")
                for col in float_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col])
            except Exception as e:
                raise Exception(e)

            df = df.replace({np.nan: None})

            strNullProc = lambda x: 'NULL' if x is None else f"'{x}'"
            for row in df.iterrows():
                # logger.info(row[1]['지역코드'])
                total_count += 1

                col = row[1]

                # deal_date = f"{str(col['년']).zfill(4)}/{str(col['월']).zfill(2)}/{str(col['일']).zfill(2)} 00:00:00"
                # region_code = col['지역코드']
                # area = col['거래면적']
                # usage_name = strNullProc(col['용도지역'])
                # jibun = col['지번']
                # jimok = strNullProc(col['지목'])
                # leg_dong_name = col['법정동']
                region_code = col['sggCd'] #지역코드 
                sigungu_name = col['sggNm'] #시군구
                leg_dong_name = col['umdNm'] #법정동
                area = col['dealArea'] #거래면적
                usage_name = col['landUse'] #용도지역
                jibun = col['jibun'] #지번
                jimok = col['jimok'] #지목
                deal_year = col['dealYear'] #계약년도
                deal_month = col['dealMonth'] #계약월
                deal_day = col['dealDay'] #계약일
                deal_amount = col['dealAmount'] #거래금액(만원)
                share_dealing_type = col['shareDealingType'] #지분거래구분
                cdeal_type = col['cdealType'] #해제여부
                cdeal_day = col['cdealDay'] #해제사유발생일
                dealing_gbn = col['dealingGbn'] #거래유형(중개및직거래여부)
                estate_agent_sgg_nm = col['estateAgentSggNm'] #중개사소재지(시군구단위)

                deal_date = f"{str(deal_year).zfill(4)}/{str(deal_month).zfill(2)}/{str(deal_day).zfill(2)} 00:00:00"
                    

                sql = (
                    f"SELECT DISTINCT(land_char.id), land_char.leg_dong_code, land_char.leg_dong_name, land_char.jibun, land_char.jimok_name, land_char.usage1_name, "
                    f"polygon.lat, polygon.lng "
                    f"FROM land_char_info as land_char "
                    f"left join address_polygon as polygon on polygon.leg_dong_code = land_char.leg_dong_code and polygon.jibun = land_char.jibun "
                    f"where land_char.leg_dong_code like '{region_code}%' and land_char.jibun like '{jibun.replace('*', '')}%' and land_char.area = {area} "
                    f"and land_char.usage1_name = {strNullProc(usage_name)} and land_char.leg_dong_name like '%{leg_dong_name}' and land_char.jimok_name = {strNullProc(jimok)}"
                )
                logger.info(sql)
                mysql_cursor.execute(sql)

                result = mysql_cursor.fetchall()
                # logger.info(result)

                if result and len(result) == 1 and result[0]['lat'] and result[0]['lng']:
                    sql = (f"INSERT INTO land_deal_list ( "
                           f"id, region_code, sigungu_name, "
                           f"leg_dong_name, leg_dong_code, jibun, jibun_hint, usage_name, "
                           f"jimok, area, "
                           f"price, div_name, deal_date, "
                           f"deal_type, broker_loc, "
                           f"cancel_reason_date, cancel_yn, position) "
                           f"VALUES ( "
                           f"{strNullProc(result[0]['id'])}, {strNullProc(region_code)}, {strNullProc(sigungu_name)}, "
                           f"{strNullProc(result[0]['leg_dong_name'])}, {strNullProc(result[0]['leg_dong_code'])}, {strNullProc(result[0]['jibun'])}, {strNullProc(jibun)}, {strNullProc(usage_name)}, "
                           f"{strNullProc(jimok)}, {strNullProc(area)}, "
                           f"{strNullProc(deal_amount)}, {strNullProc(share_dealing_type)}, '{deal_date}', "
                           f"{strNullProc(dealing_gbn)}, {strNullProc(estate_agent_sgg_nm)}, "
                           f"{strNullProc(cdeal_day)}, {strNullProc(cdeal_type)}, GEOMFROMTEXT('POINT({result[0]['lng']} {result[0]['lat']})')) "
                           f"on duplicate key update "
                           f"price={strNullProc(deal_amount)};")

                    logger.info(sql)
                    mysql_cursor.execute(sql)
                    mysql_con.commit()
                    insert_count += 1
                    logger.info(f"insert ! {insert_count}")
                else:
                    logger.info(f"could not find landinfo {result}")
                    logger.info(f"==> DATA {col}")
                    fail_count += 1
            return True
        except Exception as e:
            useKeyIndex += 1
            if useKeyIndex >= len(public_keys):
                useKeyIndex = 0
            logger.error(f"error {e} {useKeyIndex}")

    logger.error ("failed!")

    return False


def main(args):

    logger = create_logger(args.logdir, f'write_land_deal_{args.range}', backupCount=10)
    months = 3 if args.range == 'update' else 999999

    prev_code = ''
    # file_codes = open("data/leg_dong_code.txt", 'r', encoding='cp949')
    # file_codes = open(f"{args.codes}", 'r', encoding='cp949')
    # codes = csv.reader(file_codes, delimiter='\t')
    file_db = open(f"{args.db}", 'r')
    db = json.load(file_db)
    mysql_con = mysql.connector.connect(host=db["DB_HOST"], port=db["DB_PORT"], database=db["DB_NAME"],
                                        user=db["DB_USER"],
                                        password=db["DB_PASS"])
    mysql_cursor = mysql_con.cursor(dictionary=True)

    mysql_cursor.execute("SELECT * FROM leg_dong_codes")
    codes = mysql_cursor.fetchall()

    #TEST CODE
    # collect(logger, mysql_con, mysql_cursor, '11680', '202311')
    # return

    for code in codes:
        # if code[0] == '법정동코드' or code[1] == '서울특별시':
        #     logger.info(f"skip {code}")
        #     continue
        if code['deleted'] == '폐지':
            logger.info (f"skip {code}")
            continue
        if code['leg_dong_code'] == '1100000000': # 서울특별시 전체
            logger.info (f"skip {code}")
            continue        
        if '서울특별시' not in code["leg_dong_name"]:
            logger.info (f"skip {code}")
            continue

        logger.info(code)

        region_code = code["leg_dong_code"][0:5]
        logger.info(region_code)
        if region_code == prev_code:
          logger.info("same code")
          continue

        result = True
        prev_code = region_code
        for month in get_last_months(datetime.datetime.today(), months):
            logger.info(month)

            result = collect(logger, mysql_con, mysql_cursor, region_code, month, db['PUBLIC_DATA_KEY'])
            if not result:
              logger.info(f"failed")
              break

            if month == end_month:
                logger.info('end month')
                break
        if not result:
            logger.info(f"failed")
            break


    mysql_cursor.close()
    mysql_con.close()

    logger.info(f"total_count : {total_count}, insert_count : {insert_count}, fail_count {fail_count}, request_count {request_count}")
    logger.info(f"END")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--range",
        type=str,
        default='all',
        help="all - (이번달 ~ 201101), update - (이번달 ~ 3개월전)",
    )

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
    # parser.add_argument(
    #     "--codes",
    #     type=str,
    #     help="법정동 코드 파일",
    # )


    args = parser.parse_args()

    main(args)