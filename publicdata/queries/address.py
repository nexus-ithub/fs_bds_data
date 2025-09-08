# -*- coding: utf-8 -*-
import unicodedata


def get_deleted(code):
    if code == '63':
        return 'Y'
    return 'N'


def update_address_info_query(filename, data):
    # print(type(filename), filename)

    if unicodedata.normalize('NFC', "주소_변동분") in unicodedata.normalize('NFC', filename):
        return (f"INSERT INTO address_info ("
                f"address_id, road_name_code, eupmyeondong_serial_num, is_underground, "
                f"building_main_num, building_sub_num, basic_local_num, "
                f"change_reason_code, noti_date, prev_road_addr, detail_address_avail"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}','{data[3]}',"
                f"{data[4]}, {data[5]},'{data[6]}',"
                f"'{data[7]}','{data[8]}','{data[9]}','{data[10]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"address_id = '{data[0]}', road_name_code = '{data[1]}', eupmyeondong_serial_num = '{data[2]}', is_underground = '{data[3]}', "
                f"building_main_num = {data[4]}, building_sub_num = {data[5]}, basic_local_num = '{data[6]}', "
                f"change_reason_code = '{data[7]}', noti_date = '{data[8]}', prev_road_addr = '{data[9]}', detail_address_avail = '{data[10]}', "
                f"deleted_yn = '{get_deleted(data[7])}', update_date = now()"
                )
    elif unicodedata.normalize('NFC', "관련지번_변동분") in unicodedata.normalize('NFC', filename):
        return (f"INSERT INTO jibun_info ("
                f"address_id, serial_num, leg_dong_code, "
                f"sido_name, sigungu_name, leg_eupmyeondong_name, leg_li_name, "
                f"is_mountain, jibun_main_num, jibun_sub_num, is_main_jibun"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}',"
                f"'{data[3]}', '{data[4]}', '{data[5]}','{data[6]}',"
                f"'{data[7]}',{data[8]},{data[9]},'{data[10]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"address_id = '{data[0]}', serial_num = '{data[1]}', leg_dong_code = '{data[2]}',"
                f"sido_name = '{data[3]}', sigungu_name = '{data[4]}', leg_eupmyeondong_name = '{data[5]}', leg_li_name = '{data[6]}',"
                f"is_mountain = '{data[7]}', jibun_main_num = {data[8]}, jibun_sub_num = {data[9]}, is_main_jibun = '{data[10]}',"
                f"deleted_yn = '{get_deleted(data[11])}', update_date = now()"
                )
    elif unicodedata.normalize('NFC', "지번_변동분") in unicodedata.normalize('NFC', filename):
        return (f"INSERT INTO jibun_info ("
                f"address_id, serial_num, leg_dong_code, "
                f"sido_name, sigungu_name, leg_eupmyeondong_name, leg_li_name, "
                f"is_mountain, jibun_main_num, jibun_sub_num, is_main_jibun"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}',"
                f"'{data[3]}', '{data[4]}', '{data[5]}','{data[6]}',"
                f"'{data[7]}',{data[8]},{data[9]},'{data[10]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"address_id = '{data[0]}', serial_num = '{data[1]}', leg_dong_code = '{data[2]}',"
                f"sido_name = '{data[3]}', sigungu_name = '{data[4]}', leg_eupmyeondong_name = '{data[5]}', leg_li_name = '{data[6]}',"
                f"is_mountain = '{data[7]}', jibun_main_num = {data[8]}, jibun_sub_num = {data[9]}, is_main_jibun = '{data[10]}', update_date = now()"
                )
    elif unicodedata.normalize('NFC', "부가정보_변동분") in unicodedata.normalize('NFC', filename):
        return (f"INSERT INTO additional_info ("
                f"address_id, admin_dong_code, admin_dong_name, "
                f"zip_code, zip_serial_num, huge_delivery_name, "
                f"building_leg_name, local_building_name, is_apartment_house"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}',"
                f"'{data[3]}', '{data[4]}','{data[5]}',"
                f"'{data[6]}','{data[7]}','{data[8]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"address_id = '{data[0]}', admin_dong_code = '{data[1]}', admin_dong_name = '{data[2]}',"
                f"zip_code = '{data[3]}', zip_serial_num = '{data[4]}', huge_delivery_name = '{data[5]}',"
                f"building_leg_name = '{data[6]}', local_building_name = '{data[7]}', is_apartment_house ='{data[8]}', update_date = now()"
                )
    elif unicodedata.normalize('NFC', "도로명코드_변경분") in unicodedata.normalize('NFC', filename):
        return (f"INSERT INTO road_code_info ("
                f"road_name_code, road_name, road_name_roman, eupmyeondong_serial_num, "
                f"sido_name, sido_name_roman, sigungu_name, sigungu_name_roman, "
                f"eupmyeondong_name, eupmyeondong_name_roman, eupmyeondong_val, eupmyeondong_code, "
                f"in_use, change_reason, chage_history, noti_date, cancel_date"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}','{data[3]}',"
                f"'{data[4]}', '{data[5]}','{data[6]}','{data[7]}',"
                f"'{data[8]}', '{data[9]}','{data[10]}','{data[11]}',"
                f"'{data[12]}', '{data[13]}','{data[14]}','{data[15]}','{data[16]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"road_name_code = '{data[0]}', road_name = '{data[1]}', road_name_roman = '{data[2]}', eupmyeondong_serial_num = '{data[3]}',"
                f"sido_name = '{data[4]}', sido_name_roman = '{data[5]}', sigungu_name = '{data[6]}', sigungu_name_roman = '{data[7]}',"
                f"eupmyeondong_name = '{data[8]}', eupmyeondong_name_roman = '{data[9]}', eupmyeondong_val = '{data[10]}', eupmyeondong_code = '{data[11]}',"
                f"in_use = '{data[12]}', change_reason = '{data[13]}', chage_history ='{data[14]}', noti_date = '{data[15]}', cancel_date ='{data[16]}', update_date = now()"
                )
    elif "build_mod" in filename:
        return (f"INSERT INTO building_addr ("
                f"leg_dong_code, sido_name, sigungu_name, leg_eupmyeondong_name, "
                f"leg_li_name, is_mountain, jibun_main_num, jibun_sub_num, "
                f"road_name_code, road_name, is_underground, building_main_num, "
                f"building_sub_num, building_leg_name, build_detail_name, building_id, "
                f"eupmyeondong_serial_num, admin_dong_code, admin_dong_name, zip_code, "
                f"zip_serial_num, huge_delivery_name, move_reason_code, release_date, "
                f"prev_road_addr, local_building_name, is_apartment_house, basic_local_num, "
                f"detail_address_avail, note1, note2"
                f") VALUES ('{data[0]}','{data[1]}','{data[2]}','{data[3]}',"
                f"'{data[4]}', '{data[5]}',{data[6]},{data[7]},"
                f"'{data[8]}', '{data[9]}','{data[10]}',{data[11]},"
                f"{data[12]}, '{data[13]}','{data[14]}','{data[15]}',"
                f"'{data[16]}', '{data[17]}','{data[18]}','{data[19]}',"
                f"'{data[20]}', '{data[21]}','{data[22]}','{data[23]}',"
                f"'{data[24]}', '{data[25]}','{data[26]}','{data[27]}',"
                f"'{data[28]}', '{data[29]}','{data[30]}') "
                f"ON DUPLICATE KEY UPDATE "
                f"leg_dong_code = '{data[0]}', sido_name = '{data[1]}', sigungu_name ='{data[2]}', leg_eupmyeondong_name = '{data[3]}',"
                f"leg_li_name = '{data[4]}', is_mountain = '{data[5]}', jibun_main_num ={data[6]}, jibun_sub_num = {data[7]},"
                f"road_name_code = '{data[8]}', road_name = '{data[9]}', is_underground ='{data[10]}', building_main_num = {data[11]},"
                f"building_sub_num = {data[12]}, building_leg_name = '{data[13]}', build_detail_name ='{data[14]}', building_id = '{data[15]}',"
                f"eupmyeondong_serial_num = '{data[16]}', admin_dong_code = '{data[17]}', admin_dong_name ='{data[18]}', zip_code = '{data[19]}',"
                f"zip_serial_num = '{data[20]}', huge_delivery_name = '{data[21]}', move_reason_code ='{data[22]}', release_date = '{data[23]}',"
                f"prev_road_addr = '{data[24]}', local_building_name = '{data[25]}', is_apartment_house ='{data[26]}', basic_local_num = '{data[27]}',"
                f"detail_address_avail = '{data[28]}', note1 = '{data[29]}', note2 ='{data[30]}', "
                f"deleted_yn = '{get_deleted(data[22])}', update_date = now()"
                )
