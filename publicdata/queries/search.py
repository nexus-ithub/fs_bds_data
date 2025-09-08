

INSERT_SEARCH_INFO = \
    "INSERT INTO fs_building.address_search_info_by_fs (id, leg_dong_code, sido_name, sigungu_name, leg_eupmyeondong_name, jibun_main_num, jibun_sub_num, update_date)\
    SELECT\
    id,\
    leg_dong_code,\
    SUBSTRING_INDEX(TRIM(leg_dong_name), ' ', 1) AS si,\
    SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(leg_dong_name), ' ', 2), ' ', -1) as gu,\
    SUBSTRING_INDEX(TRIM(leg_dong_name), ' ', -1) as dong,\
    IF(INSTR(jibun, '-') = 0, CAST(jibun AS UNSIGNED), CAST(SUBSTRING_INDEX(jibun, '-', 1) AS UNSIGNED)) AS jibun_main_num,\
    IF(INSTR(jibun, '-') = 0, 0, CAST(SUBSTRING_INDEX(jibun, '-', -1) AS UNSIGNED)) AS jibun_sub_num,\
    NOW()\
    FROM fs_building.land_info LINFO\
    ON DUPLICATE KEY UPDATE\
     id = LINFO.id,\
     leg_dong_code = LINFO.leg_dong_code,\
     sido_name = SUBSTRING_INDEX(TRIM(LINFO.leg_dong_name), ' ', 1),\
      sigungu_name = SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(leg_dong_name), ' ', 2), ' ', -1),\
     leg_eupmyeondong_name = SUBSTRING_INDEX(TRIM(leg_dong_name), ' ', -1),\
     jibun_main_num=IF(INSTR(LINFO.jibun, '-') = 0, CAST(LINFO.jibun AS UNSIGNED), CAST(SUBSTRING_INDEX(LINFO.jibun, '-', 1) AS UNSIGNED)),\
     jibun_sub_num=IF(INSTR(LINFO.jibun, '-') = 0, 0, CAST(SUBSTRING_INDEX(LINFO.jibun, '-', -1) AS UNSIGNED)),\
     update_date=NOW();"\

UPDATE_SEARCH_INFO_QUERY = \
    "UPDATE fs_building.address_search_info_by_fs FS\
    INNER JOIN building_addr BLDG\
    ON FS.sido_name = BLDG.sido_name\
    AND FS.sigungu_name = BLDG.sigungu_name\
    AND FS.leg_eupmyeondong_name = BLDG.leg_eupmyeondong_name\
    AND FS.jibun_main_num = BLDG.jibun_main_num\
    AND FS.jibun_sub_num = BLDG.jibun_sub_num\
    SET\
    FS.local_building_name = COALESCE(NULLIF(BLDG.local_building_name, ''), BLDG.building_leg_name),\
    FS.local_building_name_no_space = REPLACE(COALESCE(NULLIF(BLDG.local_building_name, ''), BLDG.building_leg_name), ' ', ''),\
    FS.update_date=now();"


