CREATE TABLE `public_data_files` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` varchar(45) NOT NULL,
  `user` int(11) DEFAULT NULL,
  `memo` varchar(45) NOT NULL,
  `path` varchar(256) NOT NULL,
  `size` int(11) DEFAULT 0,
  `auto_yn` char(1) DEFAULT 'N',
  `update_start_date` datetime DEFAULT NULL,
  `update_end_date` datetime DEFAULT NULL,
  `update_status` char(1) DEFAULT 'P' COMMENT 'P : 대기중 \\nR : 업데이트중 \\nY : 성공 \\nF : 실패   ',
  `update_count` int(11) DEFAULT NULL,
  `delete_yn` char(1) DEFAULT 'N',
  `cancel_yn` char(1) DEFAULT 'N',
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_type` (`type`),
  KEY `idx_cancel_yn` (`cancel_yn`),
  KEY `idx_create_date` (`created_at`)
);


CREATE TABLE `road_width` (
  `id` int(10) unsigned NOT NULL,
  `road_name` varchar(45) DEFAULT NULL,
  `road_type` varchar(45) DEFAULT NULL,
  `road_func` varchar(45) DEFAULT NULL,
  `road_scale` varchar(45) DEFAULT NULL,
  `road_width` varchar(45) DEFAULT NULL,
  `road_owner` varchar(45) DEFAULT NULL,
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_name` (`road_name`)
);



CREATE TABLE `leg_land_usage_ratio` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) NOT NULL,
  `far` decimal(5,0) NOT NULL COMMENT '용적률',
  `bcr` decimal(5,0) NOT NULL COMMENT '건폐율',
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`)
);
  

CREATE TABLE `naver_rent_info` (
  `atcl_no` varchar(14) NOT NULL,
  `atcl_nm` varchar(14) NOT NULL COMMENT '일반상가/단지내상가/복합상가 등등',
  `rlet_tp_nm` varchar(14) NOT NULL COMMENT '상가/사무실 등등',
  `trad_tp_nm` varchar(14) NOT NULL COMMENT '전세/월세',
  `floor_info` varchar(14) NOT NULL COMMENT '해당층수/총층수',
  `floor_type` char(1) NOT NULL COMMENT '1:1층, 2:상층, 3:하층',  
  `price` INT(10) NOT NULL COMMENT '임대료',
  `rent_price` INT(10) NOT NULL COMMENT '월세',
  `area` INT(10) NOT NULL COMMENT '면적',
  `excl_area` varchar(10) NOT NULL COMMENT '전용면적',
  `lat` decimal(18,14) NOT NULL,
  `lng` decimal(18,14) NOT NULL,
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`atcl_no`)
);

CREATE TABLE `address_search` (
  `id` varchar(19) NOT NULL,
  `jibun` varchar(150) NOT NULL,
  `road` varchar(150) NOT NULL,
  `building_name` varchar(128) NOT NULL,
  `key_jibun`      VARCHAR(150)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(jibun),' ',''),'-','')) STORED,
  `key_road`       VARCHAR(150)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(road),' ',''),'-','')) STORED,
  `key_building`   VARCHAR(128)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(building_name),' ',''),'-','')) STORED,
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_jibun` (`key_jibun`),
  KEY `idx_road` (`key_road`),
  KEY `idx_building` (`key_building`)
);




CREATE TABLE `address_search` (
  `id` varchar(19) NOT NULL,
  `jibun` varchar(150) NOT NULL,
  `road` varchar(150) NOT NULL,
  `building_name` varchar(128) NOT NULL,
  `key_jibun`      VARCHAR(150)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(jibun),' ',''),'-','')) STORED,
  `key_road`       VARCHAR(150)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(road),' ',''),'-','')) STORED,
  `key_building`   VARCHAR(128)
    GENERATED ALWAYS AS (REPLACE(REPLACE(LOWER(building_name),' ',''),'-','')) STORED,
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
);


-- INSERT INTO address_search_info (
--   id,
--   jibun,
--   road,
--   building_name
-- )
-- SELECT
--   li.id AS id,
--   /* 지번 주소: 동리명 + 공백 + 지번 */
--   CONCAT(COALESCE(li.leg_dong_name, ''), ' ', li.jibun) AS jibun,
--   /* 도로명 주소: road_name 없으면 공란, 있으면 "시도 시군구 도로명 건물주번[-부번]" */
--   CASE
--     WHEN ri.road_name IS NULL OR ri.road_name = '' THEN ''
--     ELSE
--       CASE
--         WHEN addr.building_main_num IS NULL THEN
--           /* 건물번호가 없으면 번호 없이 앞부분만 */
--           CONCAT_WS(' ', jb.sido_name, jb.sigungu_name, ri.road_name)
--         ELSE
--           /* 건물번호가 있으면 주번[-부번]까지 */
--           CONCAT_WS(
--             ' ',
--             jb.sido_name,
--             jb.sigungu_name,
--             ri.road_name,
--             CONCAT(
--               addr.building_main_num,
--               CASE
--                 WHEN COALESCE(addr.building_sub_num, 0) > 0
--                   THEN CONCAT('-', addr.building_sub_num)
--                 ELSE ''
--               END
--             )
--           )
--       END
--   END AS road,
--   /* 빌딩명: localBuildingName 우선, 없으면 buildingLegName, 그래도 없으면 공란 */
--   COALESCE(inf.local_building_name, inf.building_leg_name, '') AS building_name
-- FROM land_info AS li
-- LEFT JOIN jibun_info AS jb
--   ON jb.leg_dong_code = li.leg_dong_code
--  AND jb.jibun_main_num = SUBSTRING_INDEX(li.jibun, '-', 1)
--  AND jb.jibun_sub_num = CASE
--     WHEN li.jibun LIKE '%-%' THEN SUBSTRING_INDEX(li.jibun, '-', -1)
--     ELSE '0'
--   END
-- LEFT JOIN address_info AS addr
--   ON addr.address_id = jb.address_id
-- LEFT JOIN additional_info AS inf
--   ON inf.address_id = addr.address_id
-- LEFT JOIN road_code_info AS ri
--   ON addr.road_name_code = ri.road_name_code
--  AND addr.eupmyeondong_serial_num = ri.eupmyeondong_serial_num
-- /* -- (옵션) 특정 필지만 갱신하고 싶다면 아래 주석을 해제하고 id 바인딩
-- WHERE li.id = ? */
-- ON DUPLICATE KEY UPDATE
--   jibun         = VALUES(jibun),
--   road          = VALUES(road),
--   building_name = VALUES(building_name),
--   updated_at    = CURRENT_TIMESTAMP;


INSERT INTO address_search (id, jibun, road, building_name)
SELECT id, jibun, road, building_name
FROM (
  SELECT
    li.id,
    CONCAT(COALESCE(li.leg_dong_name, ''), ' ', li.jibun) AS jibun,
    CASE
      WHEN ri.road_name IS NULL OR ri.road_name = '' THEN ''
      ELSE
        CASE
          WHEN addr.building_main_num IS NULL THEN
            CONCAT_WS(' ', jb.sido_name, jb.sigungu_name, ri.road_name)
          ELSE
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
    COALESCE(inf.local_building_name, inf.building_leg_name, '') AS building_name,
    ROW_NUMBER() OVER (PARTITION BY li.id ORDER BY jb.address_id ASC) AS rn
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
) t
WHERE rn = 1
ON DUPLICATE KEY UPDATE
  jibun         = VALUES(jibun),
  road          = VALUES(road),
  building_name = VALUES(building_name),
  updated_at    = CURRENT_TIMESTAMP;




INSERT INTO leg_land_usage_ratio (name, bcr, far)
VALUES
('제1종전용주거지역', 50.0, 100.0),
('제2종전용주거지역', 50.0, 150.0),
('제1종일반주거지역', 60.0, 200.0),
('제2종일반주거지역', 60.0, 250.0),
('제3종일반주거지역', 50.0, 300.0),
('준주거지역', 70.0, 500.0),
('중심상업지역', 90.0, 1500.0),
('일반상업지역', 80.0, 1300.0),
('근린상업지역', 70.0, 900.0),
('유통상업지역', 80.0, 1100.0),
('전용공업지역', 70.0, 300.0),
('일반공업지역', 70.0, 350.0),
('준공업지역', 70.0, 400.0),
('보전녹지지역', 20.0, 80.0),
('생산녹지지역', 20.0, 100.0),
('자연녹지지역', 20.0, 100.0),
('보전관리지역', 20.0, 80.0),
('생산관리지역', 20.0, 80.0),
('계획관리지역', 40.0, 100.0),
('농림지역', 20.0, 80.0),
('자연환경보전지역', 20.0, 80.0);



CREATE TABLE `consult_request` (
  `key` int(11) NOT NULL AUTO_INCREMENT,
  `land_id` varchar(19) NOT NULL,
  `user_id` varchar(12) NOT NULL,
  `content` varchar(1000) NOT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`key`)
);
