import argparse
import time

import requests
import json
import mysql.connector
from utils.logger import create_logger
from typing import Dict, Tuple, Iterator
from utils.telegramBot import TelegramBot
import asyncio

RETRY_DURATION = 60 * 60 * 1 # 1 hours
MAX_RETRY = 16

REQUEST_DELAY = 3 # 3sec
NEXT_BOUND_DELAY = 60 # 1min


SQL_BOUNDS = """
SELECT
  CAST(MIN(lng) AS DOUBLE) AS left_top_lng,
  CAST(MAX(lat) AS DOUBLE) AS left_top_lat,
  CAST(MAX(lng) AS DOUBLE) AS right_top_lng,
  CAST(MAX(lat) AS DOUBLE) AS right_top_lat,
  CAST(MIN(lng) AS DOUBLE) AS left_bottom_lng,
  CAST(MIN(lat) AS DOUBLE) AS left_bottom_lat,
  CAST(MAX(lng) AS DOUBLE) AS right_bottom_lng,
  CAST(MIN(lat) AS DOUBLE) AS right_bottom_lat
FROM address_polygon
WHERE lat IS NOT NULL AND lng IS NOT NULL;
"""

REQUEST_INTERVAL = {
    "lat": "37.571759",     # (참고용) 중심 lat 예시 — step 계산에는 아래 4개만 사용
    "lng": "127.03191995",  # (참고용) 중심 lng 예시
    "bottom_lat": "37.5676858",
    "top_lat":    "37.5758322",
    "left_lng":   "127.0235515",
    "right_lng":  "127.0402884",
    "z": 17,  # 질문 값 기준. 필요 시 변경 가능
}

REQUEST_FLOOR = [
    {"sort": "highPrc", "tag": "ONEFLOOR",   "page": 1},
    {"sort": "highPrc", "tag": "ONEFLOOR",   "page": 2},
    {"sort": "highPrc", "tag": "ONEFLOOR",   "page": 3},
    {"sort": "highPrc", "tag": "UPPERFLOOR", "page": 1},
    {"sort": "highPrc", "tag": "UPPERFLOOR", "page": 2},
    {"sort": "highPrc", "tag": "UPPERFLOOR", "page": 3},
    {"sort": "highPrc", "tag": "UNDERFLOOR", "page": 1},
    {"sort": "highPrc", "tag": "UNDERFLOOR", "page": 2},
    {"sort": "highPrc", "tag": "UNDERFLOOR", "page": 3},
]

def compute_steps(interval_cfg: Dict[str, str]) -> Tuple[float, float]:
    """요청 간격(top/bottom/left/right 차이)에서 step(lat,lng) 계산."""
    top = float(interval_cfg["top_lat"])
    bottom = float(interval_cfg["bottom_lat"])
    left = float(interval_cfg["left_lng"])
    right = float(interval_cfg["right_lng"])
    step_lat = abs(top - bottom)  # 북-남
    step_lng = abs(right - left)  # 서-동
    if step_lat <= 0 or step_lng <= 0:
        raise ValueError("Invalid REQUEST_INTERVAL: step must be positive.")
    return step_lat, step_lng


def frange(start: float, stop: float, step: float) -> Iterator[float]:
    """float range. start에서 stop을 지나치지 않는 범위까지 step으로 생성."""
    # 증가
    if step > 0 and start <= stop:
        v = start
        # float 누적오차 보호를 위해 여유분 추가
        while v < stop - step * 0.5:
            yield v
            v += step
        yield min(stop, v)
    # 감소 (여기서는 lat top->bottom에 사용)
    elif step > 0 and start >= stop:
        v = start
        while v > stop + step * 0.5:
            yield v
            v -= step
        yield max(stop, v)
    else:
        raise ValueError("Invalid frange parameters.")


def center_of_tile(left: float, right: float, bottom: float, top: float) -> Tuple[float, float]:
    return (top + bottom) / 2.0, (left + right) / 2.0  # (lat, lon)

def build_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": "https://m.land.naver.com/",
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Mobile Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
    }


def collect(logger,  mysql_con, mysql_cursor, session: requests.Session,
               left: float, right: float, bottom: float, top: float, zoom: int,):

    lat_center, lon_center = center_of_tile(left=left, right=right, bottom=bottom, top=top)

    retry_count = 0

    while True:
        try:
            for combo in REQUEST_FLOOR:
                page = int(combo.get("page", 1))
                sort = combo["sort"]
                tag = combo["tag"]
                url = (
                    "https://m.land.naver.com/cluster/ajax/articleList"
                    "?view=atcl"
                    "&rletTpCd=TJ:SG:SMS:GJCG:APTHGJ:GM"
                    "&tradTpCd=B2"
                    f"&lat={lat_center}&lon={lon_center}"
                    f"&btm={bottom}&lft={left}&top={top}&rgt={right}"
                    f"&z={zoom}"
                    "&showR0=&cortarNo="
                    f"&sort={sort}"
                    f"&page={page}"
                    f"&tag={tag}"
                )
                logger.info(f"[REQ] {url}")

                resp = session.get(url, headers=build_headers(), timeout=30)
                data = resp.json()
                # logger.info(data)
                items = data['body']
                floor_type = '1' if tag == 'ONEFLOOR' else '2' if tag == 'UPPERFLOOR' else '3'
                logger.info(f'item count {len(items)}')
                
                # for item in items:
                #     atcl_no = item['atclNo']
                #     atcl_nm = item['atclNm']
                #     rlet_tp_nm = item['rletTpNm']
                #     trad_tp_nm = item['tradTpNm']
                #     floor_info = item['floorInfo']
                #     price = item['prc']
                #     rent_price = item['rentPrc']
                #     area = item['spc1']
                #     excl_area = item['spc2']
                #     lat = item['lat']
                #     lng = item['lng']
                    
                #     mysql_cursor.execute("""
                #         INSERT INTO naver_rent_info (
                #             atcl_no, atcl_nm, rlet_tp_nm, trad_tp_nm, floor_info,
                #             price, rent_price, area, excl_area, lat, lng
                #         )
                #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                #         ON DUPLICATE KEY UPDATE
                #             atcl_nm = VALUES(atcl_nm),
                #             rlet_tp_nm = VALUES(rlet_tp_nm),
                #             trad_tp_nm = VALUES(trad_tp_nm),
                #             floor_info = VALUES(floor_info),
                #             price = VALUES(price),
                #             rent_price = VALUES(rent_price),
                #             area = VALUES(area),
                #             excl_area = VALUES(excl_area),
                #             lat = VALUES(lat),
                #             lng = VALUES(lng)
                #     """, (atcl_no, atcl_nm, rlet_tp_nm, trad_tp_nm, floor_info,
                #         price, rent_price, area, excl_area, lat, lng))
                values = []
                for item in items:
                    values.append((
                        item['atclNo'],
                        item['atclNm'],
                        item['rletTpNm'],
                        item['tradTpNm'],
                        item['flrInfo'],
                        floor_type,
                        item['prc'],
                        item['rentPrc'],
                        item['spc1'],
                        item['spc2'],
                        item['lat'],
                        item['lng']
                    ))

                mysql_cursor.executemany("""
                    INSERT INTO naver_rent_info (
                        atcl_no, atcl_nm, rlet_tp_nm, trad_tp_nm, floor_info,
                        floor_type, price, rent_price, area, excl_area, lat, lng
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        atcl_nm = VALUES(atcl_nm),
                        rlet_tp_nm = VALUES(rlet_tp_nm),
                        trad_tp_nm = VALUES(trad_tp_nm),
                        floor_info = VALUES(floor_info),
                        floor_type = VALUES(floor_type),
                        price = VALUES(price),
                        rent_price = VALUES(rent_price),
                        area = VALUES(area),
                        excl_area = VALUES(excl_area),
                        lat = VALUES(lat),
                        lng = VALUES(lng)
                """, values)

                mysql_con.commit()
                logger.info(f"inserted {len(values)} items, wait {REQUEST_DELAY} sec...")
                
                time.sleep(REQUEST_DELAY)
                
            return True;    
        except Exception as e:
            logger.error(f"error {e}")
            retry_count += 1
            if retry_count > MAX_RETRY:
                logger.error(f"retry count exceeded {MAX_RETRY}")
                break
            logger.error(f"retry after {RETRY_DURATION} seconds ....")
            time.sleep(RETRY_DURATION)
        
    

    logger.error("collect failed!")

    return False


async def main(args):
    logger = create_logger(args.logdir, f'collect_rent_info', backupCount=10)
 
    file_db = open(f"{args.db}", 'r')
    db = json.load(file_db)
    mysql_con = mysql.connector.connect(host=db["DB_HOST"], port=db["DB_PORT"], database=db["DB_NAME"],
                                        user=db["DB_USER"],
                                        password=db["DB_PASS"])

    config_name = db["NAME"]

    bot = TelegramBot(db["TG_TOKEN"], db["TG_CID"])

    await bot.send_message(f"[{config_name}] 네이버 임대정보 수집시작")
    

    try:
        mysql_cursor = mysql_con.cursor(dictionary=True)

        mysql_cursor.execute(SQL_BOUNDS)
        position = mysql_cursor.fetchall()[0]
        
        left_top_lng = position["left_top_lng"]
        left_top_lat = position["left_top_lat"]
        right_bottom_lng = position["right_bottom_lng"]
        right_bottom_lat = position["right_bottom_lat"]

    
        step_lat, step_lng = compute_steps(REQUEST_INTERVAL)
        zoom = int(REQUEST_INTERVAL.get("z", 17))

        logger.info(f"[INFO] bounds="
            f"({left_top_lng:.6f},{left_top_lat:.6f}) → ({right_bottom_lng:.6f},{right_bottom_lat:.6f})")
        logger.info(f"[INFO] step_lat={step_lat:.7f}, step_lng={step_lng:.7f}, zoom={zoom}")

        
        row = 0
        total_count = 0
        session = requests.Session()
        started = True
        start_left = None
        start_top = None
        
        if args.start_left is not None:
            start_left = args.start_left
            started = False
        if args.start_top is not None:
            start_top = args.start_top
            started = False

        for top in frange(left_top_lat, right_bottom_lat, step_lat):  # 감소 방향
            bottom = max(top - step_lat, right_bottom_lat)
            col = 0
            for left in frange(left_top_lng, right_bottom_lng, step_lng):  # 증가 방향
                right = min(left + step_lng, right_bottom_lng)
                logger.info(f"\n[TILE r{row} c{col}] "
                        f"top={top:.6f}, bottom={bottom:.6f}, left={left:.6f}, right={right:.6f}")

                if not started:
                    if (start_left is not None and left == start_left) and (start_top is not None and top == start_top):
                        started = True
                    else:
                        continue   

                total_count += 1        
                collect(logger, mysql_con, mysql_cursor, session, left=left, right=right, bottom=bottom, top=top, zoom=zoom)
                
                logger.info(f"collect NEXT bounds after {NEXT_BOUND_DELAY} seconds...")
                if not started:
                    time.sleep(NEXT_BOUND_DELAY)
                
                col += 1
            row += 1


        mysql_cursor.close()
        mysql_con.close()

        await bot.send_message(f"[{config_name}] 네이버 임대정보 수집완료 - 총갯수 : {total_count}")
        logger.info(f"total_count : {total_count}")
        logger.info(f"END")        
    except Exception as e:
        logger.error(f"error in main: {e}")
        await bot.send_message(f"[{config_name}] 네이버 임대정보 수집 중 오류 발생: {str(e)[:100]}")
        return False
    
   


if __name__ == '__main__':
    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--logdir",
        type=str,
        default='../logs/',
        help="log directory",
    )

    parser.add_argument(
        "--db",
        type=str,
        help="db config file path",
    )

    parser.add_argument(
        "--start_left",
        type=float,
        help="start left",
    )

    parser.add_argument(
        "--start_top",
        type=float,
        help="start top",
    )

    args = parser.parse_args()


    asyncio.run(main(args))