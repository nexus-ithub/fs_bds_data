
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import pathlib
from typing import List, Tuple

import mysql.connector

from utils.files import read_csv
from utils.logger import create_logger


CUR_PATH = pathlib.Path(__file__).parent.resolve()
ROOT_PATH = CUR_PATH.parent.resolve()
CONFIG_PATH = ROOT_PATH / "config" / "config.json"
LOGS_PATH = ROOT_PATH / "logs"


def parse_address_row_to_tuple(row: List[str]) -> Tuple:
    """
    주소 CSV 행을 address_info 테이블 삽입용 튜플로 변환
    11개 컬럼: address_id ~ detail_address_avail
    """
    if len(row) != 11:
        raise ValueError(f"주소 데이터는 11개 컬럼이 필요합니다. 실제: {len(row)}개, 데이터: {row}")

    def to_int_or_none(v: str):
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return int(v)
        except ValueError:
            return None

    address_id = row[0].strip()
    road_name_code = row[1].strip() or None
    eupmyeondong_serial_num = row[2].strip() or None
    is_underground = row[3].strip() or None
    building_main_num = to_int_or_none(row[4])
    building_sub_num = to_int_or_none(row[5])
    basic_local_num = row[6].strip() or None
    change_reason_code = row[7].strip() or None
    noti_date = row[8].strip() or None
    prev_road_addr = row[9].strip() or None
    detail_address_avail = row[10].strip() or None

    return (
        address_id,
        road_name_code,
        eupmyeondong_serial_num,
        is_underground,
        building_main_num,
        building_sub_num,
        basic_local_num,
        change_reason_code,
        noti_date,
        prev_road_addr,
        detail_address_avail,
    )


def parse_jibun_row_to_tuple(row: List[str]) -> Tuple:
    """
    지번 CSV 행을 jibun_info 테이블 삽입용 튜플로 변환
    11개 컬럼: address_id, serial_num, leg_dong_code, sido_name, sigungu_name, 
              leg_eupmyeondong_name, leg_li_name, is_mountain, jibun_main_num, jibun_sub_num, is_main_jibun
    """
    if len(row) != 11:
        raise ValueError(f"지번 데이터는 11개 컬럼이 필요합니다. 실제: {len(row)}개, 데이터: {row}")

    def to_int_or_none(v: str):
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return int(v)
        except ValueError:
            return None

    address_id = row[0].strip()
    serial_num = row[1].strip()
    leg_dong_code = row[2].strip() or None
    sido_name = row[3].strip() or None
    sigungu_name = row[4].strip() or None
    leg_eupmyeondong_name = row[5].strip() or None
    leg_li_name = row[6].strip() or None
    is_mountain = row[7].strip() or None
    jibun_main_num = to_int_or_none(row[8])
    jibun_sub_num = to_int_or_none(row[9])
    is_main_jibun = row[10].strip() or None

    return (
        address_id,
        serial_num,
        leg_dong_code,
        sido_name,
        sigungu_name,
        leg_eupmyeondong_name,
        leg_li_name,
        is_mountain,
        jibun_main_num,
        jibun_sub_num,
        is_main_jibun,
    )


def parse_additional_row_to_tuple(row: List[str]) -> Tuple:
    """
    부가정보 CSV 행을 additional_info 테이블 삽입용 튜플로 변환
    9개 컬럼: address_id, admin_dong_code, admin_dong_name, zip_code, zip_serial_num,
             huge_delivery_name, building_leg_name, local_building_name, is_apartment_house
    """
    if len(row) != 9:
        raise ValueError(f"부가정보 데이터는 9개 컬럼이 필요합니다. 실제: {len(row)}개, 데이터: {row}")

    address_id = row[0].strip()
    admin_dong_code = row[1].strip() or None
    admin_dong_name = row[2].strip() or None
    zip_code = row[3].strip() or None
    zip_serial_num = row[4].strip() or None
    huge_delivery_name = row[5].strip() or None
    building_leg_name = row[6].strip() or None
    local_building_name = row[7].strip() or None
    is_apartment_house = row[8].strip() or None

    return (
        address_id,
        admin_dong_code,
        admin_dong_name,
        zip_code,
        zip_serial_num,
        huge_delivery_name,
        building_leg_name,
        local_building_name,
        is_apartment_house,
    )


def parse_road_code_row_to_tuple(row: List[str]) -> Tuple:
    """
    도로명코드 CSV 행을 road_code_info 테이블 삽입용 튜플로 변환
    17개 컬럼: road_name_code, road_name, road_name_roman, eupmyeondong_serial_num,
              sido_name, sido_name_roman, sigungu_name, sigungu_name_roman,
              eupmyeondong_name, eupmyeondong_name_roman, eupmyeondong_val, eupmyeondong_code,
              in_use, change_reason, chage_history, noti_date, cancel_date
    """
    if len(row) != 17:
        raise ValueError(f"도로명코드 데이터는 17개 컬럼이 필요합니다. 실제: {len(row)}개, 데이터: {row}")

    road_name_code = row[0].strip()
    road_name = row[1].strip() or None
    road_name_roman = row[2].strip() or None
    eupmyeondong_serial_num = row[3].strip()
    sido_name = row[4].strip() or None
    sido_name_roman = row[5].strip() or None
    sigungu_name = row[6].strip() or None
    sigungu_name_roman = row[7].strip() or None
    eupmyeondong_name = row[8].strip() or None
    eupmyeondong_name_roman = row[9].strip() or None
    eupmyeondong_val = row[10].strip() or None
    eupmyeondong_code = row[11].strip() or None
    in_use = row[12].strip() or None
    change_reason = row[13].strip() or None
    chage_history = row[14].strip() or None
    noti_date = row[15].strip() or None
    cancel_date = row[16].strip() or None

    return (
        road_name_code,
        road_name,
        road_name_roman,
        eupmyeondong_serial_num,
        sido_name,
        sido_name_roman,
        sigungu_name,
        sigungu_name_roman,
        eupmyeondong_name,
        eupmyeondong_name_roman,
        eupmyeondong_val,
        eupmyeondong_code,
        in_use,
        change_reason,
        chage_history,
        noti_date,
        cancel_date,
    )


def parse_building_addr_row_to_tuple(row: List[str]) -> Tuple:
    """
    건물주소 CSV 행을 building_addr 테이블 삽입용 튜플로 변환
    30개 컬럼: leg_dong_code, sido_name, sigungu_name, leg_eupmyeondong_name, leg_li_name,
              is_mountain, jibun_main_num, jibun_sub_num, road_name_code, road_name,
              is_underground, building_main_num, building_sub_num, building_leg_name, build_detail_name,
              building_id, eupmyeondong_serial_num, admin_dong_code, admin_dong_name, zip_code,
              zip_serial_num, huge_delivery_name, move_reason_code, release_date, prev_road_addr,
              local_building_name, is_apartment_house, basic_local_num, detail_address_avail, note1, note2
    """
    if len(row) != 30:
        raise ValueError(f"건물주소 데이터는 30개 컬럼이 필요합니다. 실제: {len(row)}개, 데이터: {row}")

    def to_int_or_none(v: str):
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return int(v)
        except ValueError:
            return None

    leg_dong_code = row[0].strip() or None
    sido_name = row[1].strip() or None
    sigungu_name = row[2].strip() or None
    leg_eupmyeondong_name = row[3].strip() or None
    leg_li_name = row[4].strip() or None
    is_mountain = row[5].strip() or None
    jibun_main_num = to_int_or_none(row[6])
    jibun_sub_num = to_int_or_none(row[7])
    road_name_code = row[8].strip() or None
    road_name = row[9].strip() or None
    is_underground = row[10].strip() or None
    building_main_num = to_int_or_none(row[11])
    building_sub_num = to_int_or_none(row[12])
    building_leg_name = row[13].strip() or None
    build_detail_name = row[14].strip() or None
    building_id = row[15].strip()
    eupmyeondong_serial_num = row[16].strip() or None
    admin_dong_code = row[17].strip() or None
    admin_dong_name = row[18].strip() or None
    zip_code = row[19].strip() or None
    zip_serial_num = row[20].strip() or None
    huge_delivery_name = row[21].strip() or None
    move_reason_code = row[22].strip() or None
    release_date = row[23].strip() or None
    prev_road_addr = row[24].strip() or None
    local_building_name = row[25].strip() or None
    is_apartment_house = row[26].strip() or None
    basic_local_num = row[27].strip() or None
    detail_address_avail = row[28].strip() or None
    note1 = row[29].strip() or None 
    note2 = row[30].strip() or None 

    return (
        leg_dong_code,
        sido_name,
        sigungu_name,
        leg_eupmyeondong_name,
        leg_li_name,
        is_mountain,
        jibun_main_num,
        jibun_sub_num,
        road_name_code,
        road_name,
        is_underground,
        building_main_num,
        building_sub_num,
        building_leg_name,
        build_detail_name,
        building_id,
        eupmyeondong_serial_num,
        admin_dong_code,
        admin_dong_name,
        zip_code,
        zip_serial_num,
        huge_delivery_name,
        move_reason_code,
        release_date,
        prev_road_addr,
        local_building_name,
        is_apartment_house,
        basic_local_num,
        detail_address_avail,
        note1,
        note2,
        note3,
    )


def build_address_insert_sql() -> str:
    """address_info INSERT ... ON DUPLICATE KEY UPDATE SQL 생성"""
    return """
        INSERT INTO address_info (
            address_id, road_name_code, eupmyeondong_serial_num, is_underground,
            building_main_num, building_sub_num, basic_local_num,
            change_reason_code, noti_date, prev_road_addr, detail_address_avail
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            road_name_code = VALUES(road_name_code),
            eupmyeondong_serial_num = VALUES(eupmyeondong_serial_num),
            is_underground = VALUES(is_underground),
            building_main_num = VALUES(building_main_num),
            building_sub_num = VALUES(building_sub_num),
            basic_local_num = VALUES(basic_local_num),
            change_reason_code = VALUES(change_reason_code),
            noti_date = VALUES(noti_date),
            prev_road_addr = VALUES(prev_road_addr),
            detail_address_avail = VALUES(detail_address_avail)
    """.strip()


def build_jibun_insert_sql() -> str:
    """jibun_info INSERT ... ON DUPLICATE KEY UPDATE SQL 생성"""
    return """
        INSERT INTO jibun_info (
            address_id, serial_num, leg_dong_code, sido_name, sigungu_name,
            leg_eupmyeondong_name, leg_li_name, is_mountain, jibun_main_num, jibun_sub_num, is_main_jibun
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            leg_dong_code = VALUES(leg_dong_code),
            sido_name = VALUES(sido_name),
            sigungu_name = VALUES(sigungu_name),
            leg_eupmyeondong_name = VALUES(leg_eupmyeondong_name),
            leg_li_name = VALUES(leg_li_name),
            is_mountain = VALUES(is_mountain),
            jibun_main_num = VALUES(jibun_main_num),
            jibun_sub_num = VALUES(jibun_sub_num),
            is_main_jibun = VALUES(is_main_jibun)
    """.strip()


def build_additional_insert_sql() -> str:
    """additional_info INSERT ... ON DUPLICATE KEY UPDATE SQL 생성"""
    return """
        INSERT INTO additional_info (
            address_id, admin_dong_code, admin_dong_name, zip_code, zip_serial_num,
            huge_delivery_name, building_leg_name, local_building_name, is_apartment_house
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            admin_dong_code = VALUES(admin_dong_code),
            admin_dong_name = VALUES(admin_dong_name),
            zip_code = VALUES(zip_code),
            zip_serial_num = VALUES(zip_serial_num),
            huge_delivery_name = VALUES(huge_delivery_name),
            building_leg_name = VALUES(building_leg_name),
            local_building_name = VALUES(local_building_name),
            is_apartment_house = VALUES(is_apartment_house)
    """.strip()


def build_road_code_insert_sql() -> str:
    """road_code_info INSERT ... ON DUPLICATE KEY UPDATE SQL 생성"""
    return """
        INSERT INTO road_code_info (
            road_name_code, road_name, road_name_roman, eupmyeondong_serial_num,
            sido_name, sido_name_roman, sigungu_name, sigungu_name_roman,
            eupmyeondong_name, eupmyeondong_name_roman, eupmyeondong_val, eupmyeondong_code,
            in_use, change_reason, chage_history, noti_date, cancel_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            road_name = VALUES(road_name),
            road_name_roman = VALUES(road_name_roman),
            sido_name = VALUES(sido_name),
            sido_name_roman = VALUES(sido_name_roman),
            sigungu_name = VALUES(sigungu_name),
            sigungu_name_roman = VALUES(sigungu_name_roman),
            eupmyeondong_name = VALUES(eupmyeondong_name),
            eupmyeondong_name_roman = VALUES(eupmyeondong_name_roman),
            eupmyeondong_val = VALUES(eupmyeondong_val),
            eupmyeondong_code = VALUES(eupmyeondong_code),
            in_use = VALUES(in_use),
            change_reason = VALUES(change_reason),
            chage_history = VALUES(chage_history),
            noti_date = VALUES(noti_date),
            cancel_date = VALUES(cancel_date)
    """.strip()


def build_building_addr_insert_sql() -> str:
    """building_addr INSERT ... ON DUPLICATE KEY UPDATE SQL 생성"""
    return """
        INSERT INTO building_addr (
            leg_dong_code, sido_name, sigungu_name, leg_eupmyeondong_name, leg_li_name,
            is_mountain, jibun_main_num, jibun_sub_num, road_name_code, road_name,
            is_underground, building_main_num, building_sub_num, building_leg_name, build_detail_name,
            building_id, eupmyeondong_serial_num, admin_dong_code, admin_dong_name, zip_code,
            zip_serial_num, huge_delivery_name, move_reason_code, release_date, prev_road_addr,
            local_building_name, is_apartment_house, basic_local_num, detail_address_avail, note1, note2
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            leg_dong_code = VALUES(leg_dong_code),
            sido_name = VALUES(sido_name),
            sigungu_name = VALUES(sigungu_name),
            leg_eupmyeondong_name = VALUES(leg_eupmyeondong_name),
            leg_li_name = VALUES(leg_li_name),
            is_mountain = VALUES(is_mountain),
            jibun_main_num = VALUES(jibun_main_num),
            jibun_sub_num = VALUES(jibun_sub_num),
            road_name_code = VALUES(road_name_code),
            road_name = VALUES(road_name),
            is_underground = VALUES(is_underground),
            building_main_num = VALUES(building_main_num),
            building_sub_num = VALUES(building_sub_num),
            building_leg_name = VALUES(building_leg_name),
            build_detail_name = VALUES(build_detail_name),
            eupmyeondong_serial_num = VALUES(eupmyeondong_serial_num),
            admin_dong_code = VALUES(admin_dong_code),
            admin_dong_name = VALUES(admin_dong_name),
            zip_code = VALUES(zip_code),
            zip_serial_num = VALUES(zip_serial_num),
            huge_delivery_name = VALUES(huge_delivery_name),
            move_reason_code = VALUES(move_reason_code),
            release_date = VALUES(release_date),
            prev_road_addr = VALUES(prev_road_addr),
            local_building_name = VALUES(local_building_name),
            is_apartment_house = VALUES(is_apartment_house),
            basic_local_num = VALUES(basic_local_num),
            detail_address_avail = VALUES(detail_address_avail),
            note1 = VALUES(note1),
            note2 = VALUES(note2)
    """.strip()


def connect_db():
    """config.json에서 DB 설정을 읽어 MySQL 연결"""
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
    
    conn = mysql.connector.connect(
        host=cfg["DB_HOST"],
        port=cfg["DB_PORT"],
        database=cfg["DB_NAME"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASS"],
        autocommit=False,
    )
    return conn


def find_target_files(input_dir: str) -> List[Tuple[str, str]]:
    """
    대상 파일들을 찾고 (파일경로, 테이블타입) 튜플 리스트 반환
    테이블타입: 'address', 'jibun', 'additional', 'road_code', 'building_addr'
    """
    files = []
    for name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, name)
        
        if name.startswith("주소_") and name.lower().endswith(".txt"):
            files.append((file_path, "address"))
        elif name.startswith("지번_") and name.lower().endswith(".txt"):
            files.append((file_path, "jibun"))
        elif name.startswith("부가정보_") and name.lower().endswith(".txt"):
            files.append((file_path, "additional"))
        elif "도로명코드_전체분" in name and name.lower().endswith(".txt"):
            files.append((file_path, "road_code"))
        elif "build_" in name and name.lower().endswith(".txt"):
            files.append((file_path, "building_addr"))
    
    files.sort(key=lambda x: x[0])  # 파일경로로 정렬
    return files


def process_file(cursor, file_path: str, table_type: str, batch_size: int = 1000) -> int:
    """파일 하나를 처리하여 DB에 삽입. 처리된 행 수를 반환"""
    
    if table_type == "address":
        insert_sql = build_address_insert_sql()
        parse_func = parse_address_row_to_tuple
    elif table_type == "jibun":
        insert_sql = build_jibun_insert_sql()
        parse_func = parse_jibun_row_to_tuple
    elif table_type == "additional":
        insert_sql = build_additional_insert_sql()
        parse_func = parse_additional_row_to_tuple
    elif table_type == "road_code":
        insert_sql = build_road_code_insert_sql()
        parse_func = parse_road_code_row_to_tuple
    elif table_type == "building_addr":
        insert_sql = build_building_addr_insert_sql()
        parse_func = parse_building_addr_row_to_tuple
    else:
        raise ValueError(f"지원하지 않는 테이블 타입: {table_type}")
    
    batch: List[Tuple] = []
    total = 0

    for row in read_csv(file_path, delimiter='|', encoding='cp949', quoting=0):
        if not row:
            continue
        
        values = parse_func(row)
        # 첫 번째 컬럼이 없으면 스킵 (building_addr의 경우 building_id가 16번째이므로 별도 처리)
        if table_type == "building_addr":
            if not values[15]:  # building_id
                continue
        else:
            if not values[0]:
                continue
            
        batch.append(values)
        
        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            total += len(batch)
            batch.clear()

    # 남은 배치 처리
    if batch:
        cursor.executemany(insert_sql, batch)
        total += len(batch)

    return total


def main():
    parser = argparse.ArgumentParser(description="주소/지번/부가정보/도로명코드/건물주소 txt 파일들을 읽어 해당 테이블에 삽입")
    parser.add_argument("input_dir", help="'주소_', '지번_', '부가정보_', '도로명코드_전체분', 'build_'가 포함된 txt 파일들이 있는 디렉토리")
    parser.add_argument("--batch-size", type=int, default=1000, help="배치 삽입 크기 (기본값: 1000)")
    parser.add_argument("--table-type", choices=["address", "jibun", "additional", "road_code", "building_addr", "all"], default="all", 
                       help="처리할 테이블 타입 (address: 주소만, jibun: 지번만, additional: 부가정보만, road_code: 도로명코드만, building_addr: 건물주소만, all: 모두, 기본값: all)")
    args = parser.parse_args()

    # 로거 설정
    logger = create_logger(str(LOGS_PATH), "write_address_data", backupCount=10)

    input_dir = args.input_dir
    if not os.path.isdir(input_dir):
        raise FileNotFoundError(f"입력 디렉토리를 찾을 수 없습니다: {input_dir}")

    # 대상 파일들 찾기
    all_files = find_target_files(input_dir)
    
    # 테이블 타입에 따라 필터링
    if args.table_type == "address":
        files = [(f, t) for f, t in all_files if t == "address"]
    elif args.table_type == "jibun":
        files = [(f, t) for f, t in all_files if t == "jibun"]
    elif args.table_type == "additional":
        files = [(f, t) for f, t in all_files if t == "additional"]
    elif args.table_type == "road_code":
        files = [(f, t) for f, t in all_files if t == "road_code"]
    elif args.table_type == "building_addr":
        files = [(f, t) for f, t in all_files if t == "building_addr"]
    else:  # all
        files = all_files
    
    if not files:
        logger.warning(f"{input_dir}에서 대상 파일을 찾을 수 없습니다")
        return

    logger.info(f"처리할 파일 {len(files)}개를 찾았습니다")
    
    # 파일 타입별 개수 출력
    address_count = sum(1 for _, t in files if t == "address")
    jibun_count = sum(1 for _, t in files if t == "jibun")
    additional_count = sum(1 for _, t in files if t == "additional")
    road_code_count = sum(1 for _, t in files if t == "road_code")
    building_addr_count = sum(1 for _, t in files if t == "building_addr")
    logger.info(f"- 주소 파일: {address_count}개")
    logger.info(f"- 지번 파일: {jibun_count}개")
    logger.info(f"- 부가정보 파일: {additional_count}개")
    logger.info(f"- 도로명코드 파일: {road_code_count}개")
    logger.info(f"- 건물주소 파일: {building_addr_count}개")

    # DB 연결
    conn = connect_db()
    cursor = conn.cursor()

    total_rows = 0
    try:
        for i, (file_path, table_type) in enumerate(files, start=1):
            table_name_map = {
                "address": "address_info",
                "jibun": "jibun_info", 
                "additional": "additional_info",
                "road_code": "road_code_info",
                "building_addr": "building_addr"
            }
            table_name = table_name_map[table_type]
            logger.info(f"[{i}/{len(files)}] {os.path.basename(file_path)} -> {table_name} 처리 중...")
            
            rows = process_file(cursor, file_path, table_type, batch_size=args.batch_size)
            conn.commit()
            total_rows += rows
            logger.info(f"{os.path.basename(file_path)}에서 {rows}행 삽입/업데이트 완료")
            
    except Exception as e:
        conn.rollback()
        logger.exception(f"오류 발생, 트랜잭션 롤백: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"완료. 총 처리된 행 수: {total_rows}")


if __name__ == "__main__":
    main()