


def get_dataname_by_type(type):
    if type == "address":
        return "주소DB(월변동자료)"
    elif type == "building_addr":
        return "건물DB(월변동자료)"
    elif type == "building_leg_headline":
        return "건축물대장(표제부)"
    elif type == "building_floor_info":
        return "건축물대장(층별개요)"
    elif type == "land_info":
        return "토지임야정보"
    elif type == "land_char_info":
        return "토지특성정보"
    elif type == "land_usage_info":
        return "토지이용계획정보"
    elif type == "address_polygon":
        return "연속지적도형정보"
    elif type == "individual_announced_price":
        return "개별공시지가정보"
    elif type == "district_polygon":
        return "서울시 상권분석서비스(영역-상권)"
    elif type == "district_foot_traffic":
        return "서울시 상권분석서비스(길단위인구-상권)"
    elif type == "district_office_workers":
        return "서울시 상권분석서비스(직장인구-상권)"
    elif type == "district_resident":
        return "서울시 상권분석서비스(상주인구-상권)"
    elif type == "district_resident_alltime":
        return "서울시 상권분석서비스(상주인구-상권배후지)"
    elif type == "district_foot_traffic_seoul":
        return "서울시 상권분석서비스(길단위인구-서울시)"
    elif type == "leg_dong_codes":
        return "법정동코드 전체자료"


