

CREATE_NEW_INDIVIDUAL_ANNOUNCED_PRICE \
    = "create table individual_announced_price_new\
(\
    `key`            int auto_increment primary key,\
    id               varchar(19)                          null comment '고유번호',\
    leg_dong_code    varchar(10)                          null comment '법정동코드',\
    leg_dong_name    varchar(300)                         null comment '법정동명',\
    special_div_code varchar(1)                           null comment '특수지구분코드',\
    special_div_name varchar(300)                         null comment '특수지구분명',\
    jibun            varchar(9)                           null comment '지번',\
    year             varchar(4)                           null comment '기준연도',\
    month            varchar(2)                           null comment '기준월',\
    price            varchar(15)                          null comment '개별공시지가',\
    announce_date    varchar(10)                          null comment '공시일자',\
    is_standard      char                                 null comment '표준지여부',\
    data_date        varchar(10)                          null comment '데이터 기준일자',\
    sigungu_code     varchar(5)                           null,\
    update_date      datetime                             null,\
    insert_date      datetime default current_timestamp() null\
);"


CREATE_NEW_LAND_CHAR_INFO \
    = "create table land_char_info_new\
(\
    `key`              int auto_increment primary key,\
    id                 varchar(19)                          null comment '고유번호',\
    leg_dong_code      varchar(10)                          null comment '법정동코드',\
    leg_dong_name      varchar(300)                         null comment '법정동명',\
    div_code           char                                 null comment '대장구분코드',\
    div_name           varchar(300)                         null comment '대장구분명',\
    jibun              varchar(9)                           null comment '지번',\
    land_serial_number varchar(10)                          null comment '토지일련번호',\
    year               varchar(4)                           null comment '기준연도',\
    month              varchar(2)                           null comment '기준월',\
    jimok_code         varchar(2)                           null comment '지목코드',\
    jimok_name         varchar(20)                          null comment '지목명',\
    area               decimal(28, 9)                       null comment '면적',\
    usage1_code        varchar(2)                           null comment '용도지역코드1',\
    usage1_name        varchar(300)                         null comment '용도지역명1',\
    usage2_code        varchar(2)                           null comment '용도지역코드2',\
    usage2_name        varchar(300)                         null comment '용도지역명2',\
    cur_use_code       varchar(3)                           null comment '토지이용상황코드',\
    cur_use            varchar(300)                         null comment '토지이용상황',\
    height_code        varchar(3)                           null comment '지형높이코드',\
    height             varchar(300)                         null comment '지형높이',\
    shape_code         varchar(2)                           null comment '지형형상코드',\
    shape              varchar(300)                         null comment '지형형상',\
    road_contact_code  varchar(2)                           null comment '도로접면코드',\
    road_contact       varchar(300)                         null comment '도로접면',\
    price              varchar(300)                         null comment '공시지가',\
    create_date        varchar(10)                          null comment '데이터기준일자',\
    update_date        datetime                             null,\
    insert_date        datetime default current_timestamp() null\
);"


CREATE_NEW_LAND_USAGE_INFO \
    = "create table land_usage_info_new\
(\
    `key`          int auto_increment primary key,\
    id             varchar(19)                          null comment '고유번호',\
    leg_dong_code  varchar(10)                          null comment '법정동코드',\
    leg_dong_name  varchar(300)                         null comment '법정동명',\
    div_code       char                                 null comment '대장구분코드',\
    div_name       varchar(300)                         null comment '대장구분명',\
    jibun          varchar(9)                           null comment '지번',\
    drawing_number varchar(33)                          null comment '도면번호',\
    conflict_code  varchar(200)                         null comment '저촉여부코드',\
    conflict       varchar(300)                         null comment '저촉여부',\
    usage_code     varchar(300)                         null comment '용도지역지구코드목록',\
    usage_name     varchar(300)                         null comment '용도지역지구명목록',\
    register_date  varchar(10)                          null comment '등록일자',\
    create_date    varchar(10)                          null comment '데이터기준일자',\
    sigungu_code   varchar(5)                           null,\
    etc            varchar(100)                         null,\
    update_date    datetime                             null,\
    insert_date    datetime default current_timestamp() null\
);"


CREATE_NEW_BUILDING_LEG_HEADLINE = "create table building_leg_headline_new\
(\
    building_id                 varchar(33)                            not null comment '관리_건축물대장_PK'\
        primary key,\
    leg_div_code                varchar(1)                             null comment '대장_구분_코드',\
    leg_div_code_name           varchar(100)                           null comment '대장_구분_코드_명',\
    leg_type_code               varchar(1)                             null comment '대장_종류_코드',\
    leg_type_code_name          varchar(100)                           null comment '대장_종류_코드_명',\
    site_loc                    varchar(500)                           null comment '대지_위치',\
    road_name_site_loc          varchar(400)                           null comment '도로명_대지_위치',\
    building_name               varchar(100)                           null comment '건물_명',\
    sigungu_code                varchar(5)                             null comment '시군구_코드',\
    leg_dong_code               varchar(5)                             null comment '법정동_코드',\
    site_div_code               varchar(1)                             null comment '대지_구분_코드',\
    bun                         varchar(4)                             null comment '번',\
    ji                          varchar(4)                             null comment '지',\
    specialji_name              varchar(200)                           null comment '특수지_명',\
    block                       varchar(20)                            null comment '블록',\
    lot                         varchar(20)                            null comment '로트',\
    rest_lot_count              decimal(5)                             null comment '외필지_수',\
    new_road_code               varchar(12)                            null comment '새주소_도로_코드',\
    new_leg_dong_code           varchar(5)                             null comment '새주소_법정동_코드',\
    is_underground              varchar(1)                             null comment '새주소_지상지하_코드',\
    new_building_main_num       decimal(5)                             null comment '새주소_본_번',\
    new_building_sub_num        decimal(5) default 0                   null comment '새주소_부_번',\
    dong_name                   varchar(100)                           null comment '동_명',\
    main_sub_sep_code           char                                   null comment '주_부속_구분_코드',\
    main_sub_sep_code_name      varchar(100)                           null comment '주_부속_구분_코드_명',\
    land_area                   decimal(19, 9)                         null comment '대지_면적(㎡)',\
    arch_area                   decimal(19, 9)                         null comment '건축_면적(㎡)',\
    arch_land_ratio             decimal(19, 9)                         null comment '건폐_율(%)',\
    total_floor_area            decimal(19, 9)                         null comment '연면적(㎡)',\
    total_floor_area_ratio      decimal(19, 9)                         null comment '용적_률_산정_연면적(㎡)',\
    floor_area_ratio            decimal(19, 9)                         null comment '용적_률(%)',\
    structure_code              varchar(2)                             null comment '구조_코드',\
    structure_code_name         varchar(100)                           null comment '구조_코드_명',\
    structure_etc               varchar(500)                           null comment '기타_구조',\
    main_usage_code             varchar(5)                             null comment '주_용도_코드',\
    main_usage_code_name        varchar(100)                           null comment '주_용도_코드_명',\
    etc_usage                   varchar(500)                           null comment '기타_용도',\
    roof_code                   varchar(2)                             null comment '지붕_코드',\
    roof_code_name              varchar(100)                           null comment '지붕_코드_명',\
    etc_roof                    varchar(500)                           null comment '기타_지붕',\
    household_number            varchar(5)                             null comment '세대_수(세대)',\
    family_number               varchar(5)                             null comment '가구_수(가구)',\
    height                      decimal(19, 9)                         null comment '높이(m)',\
    gnd_floor_number            varchar(5)                             null comment '지상_층_수',\
    base_floor_number           varchar(5)                             null comment '지하_층_수',\
    lift_number                 varchar(5)                             null comment '승용_승강기_수',\
    e_lift_number               varchar(5)                             null comment '비상용_승강기_수',\
    sub_arch_number             varchar(5)                             null comment '부속_건축물_수',\
    sub_arch_area               decimal(19, 9)                         null comment '부속_건축물_면적(㎡)',\
    total_dong_area             decimal(19, 9)                         null comment '총_동_연면적(㎡)',\
    indoor_mech_parking_number  varchar(6)                             null comment '옥내_기계식_대수(대)',\
    indoor_mech_parking_area    decimal(19, 9)                         null comment '옥내_기계식_면적(㎡)',\
    outdoor_mech_parking_number varchar(6)                             null comment '옥외_기계식_대수(대)',\
    outdoor_mech_parking_area   decimal(19, 9)                         null comment '옥외_기계식_면적(㎡)',\
    indoor_self_parking_number  varchar(6)                             null comment '옥내_자주식_대수(대)',\
    indoor_self_parking_area    decimal(19, 9)                         null comment '옥내_자주식_면적(㎡)',\
    outdoor_self_parking_number varchar(6)                             null comment '옥외_자주식_대수(대)',\
    outdoor_self_parking_area   decimal(19, 9)                         null comment '옥외_자주식_면적(㎡)',\
    permit_date                 varchar(8)                             null comment '허가_일',\
    start_date                  varchar(8)                             null comment '착공_일',\
    use_approval_date           varchar(8)                             null comment '사용승인_일',\
    license_year                varchar(4)                             null comment '허가번호_년',\
    license_agency_code         char(7)                                null comment '허가번호_기관_코드',\
    license_agency_code_name    varchar(100)                           null comment '허가번호_기관_코드_명',\
    license_div_code            varchar(4)                             null comment '허가번호_구분_코드',\
    license_div_code_name       varchar(100)                           null comment '허가번호_구분_코드_명',\
    unit_number                 decimal(5)                             null comment '호_수(호)',\
    energy_eff_grade            varchar(4)                             null comment '에너지효율_등급',\
    energy_reduct_ratio         decimal(19, 9)                         null comment '에너지절감_율',\
    energy_epi_score            decimal(5)                             null comment '에너지_EPI점수',\
    eco_arch_grade              char                                   null comment '친환경_건축물_등급',\
    eco_arch_score              decimal(5)                             null comment '친환경_건축물_인증점수',\
    intelligent_arch_grade      char                                   null comment '지능형_건축물_등급',\
    intelligent_arch_score      decimal(5)                             null comment '지능형_건축물_인증점수',\
    create_date                 varchar(8)                             null comment '생성_일자',\
    is_eq_resist                varchar(1)                             null comment '내진_설계_적용_여부',\
    eq_resist_ability           varchar(200)                           null comment '내진_능력',\
    leg_dong_code_val           varchar(10)                            null comment 'concat(sigungu_code + leg_dong_code)',\
    update_date                 datetime                               null,\
    insert_date                 datetime   default current_timestamp() null\
);"


CREATE_NEW_BUILDING_FLOOR_INFO = "create table building_floor_info_new\
(\
    `key`                  int auto_increment\
        primary key,\
    building_id            varchar(33)                            null comment '관리_건축물대장_PK',\
    site_loc               varchar(500)                           null comment '대지_위치',\
    road_name_site_loc     varchar(400)                           null comment '도로명_대지_위치',\
    building_name          varchar(100)                           null comment '건물_명',\
    sigungu_code           varchar(5)                             null comment '시군구_코드',\
    leg_dong_code          varchar(5)                             null comment '법정동_코드',\
    site_div_code          varchar(1)                             null comment '대지_구분_코드',\
    bun                    varchar(4)                             null comment '번',\
    ji                     varchar(4)                             null comment '지',\
    specialji_name         varchar(200)                           null comment '특수지_명',\
    block                  varchar(20)                            null comment '블록',\
    lot                    varchar(20)                            null comment '로트',\
    new_road_code          varchar(12)                            null comment '새주소_도로_코드',\
    new_leg_dong_code      varchar(5)                             null comment '새주소_법정동_코드',\
    is_underground         varchar(1)                             null comment '새주소_지상지하_코드',\
    new_building_main_num  decimal(5)                             null comment '새주소_본_번',\
    new_building_sub_num   decimal(5) default 0                   null comment '새주소_부_번',\
    dong_name              varchar(100)                           null comment '동_명',\
    floor_div_code         varchar(2)                             null comment '층_구분_코드',\
    floor_div_code_name    varchar(100)                           null comment '층_구분_코드명',\
    floor_number           decimal(4)                             null comment '층_번호',\
    floor_number_name      varchar(100)                           null comment '층_번호_명',\
    structure_code         varchar(2)                             null comment '구조_코드',\
    structure_code_name    varchar(100)                           null comment '구조_코드_명',\
    structure_etc          varchar(500)                           null comment '기타_구조',\
    main_usage_code        varchar(5)                             null comment '주_용도_코드',\
    main_usage_code_name   varchar(100)                           null comment '주_용도_코드_명',\
    etc_usage              varchar(500)                           null comment '기타_용도',\
    area                   decimal(19, 9)                         null comment '면적',\
    main_sub_sep_code      char                                   null comment '주_부속_구분_코드',\
    main_sub_sep_code_name varchar(100)                           null comment '주_부속_구분_코드_명',\
    is_except_area         varchar(1)                             null comment '면적_제외_여부',\
    create_date            varchar(8)                             null comment '생성_일자',\
    leg_dong_code_val      varchar(10)                            null,\
    update_date            datetime                               null,\
    insert_date            datetime   default current_timestamp() null\
);"