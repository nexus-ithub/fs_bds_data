import argparse
import json
import os
import time

import mysql.connector
import pathlib
import shutil
import zipfile

import requests
from dateutil.relativedelta import relativedelta

from data_utils import get_dataname_by_type
from utils.telegramBot import TelegramBot
from utils.files import merge_split_zip_files
from utils.logger import create_logger
import datetime
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager




CUR_PATH = pathlib.Path(__file__).parent.resolve()
TMP_DOWNLOAD_PATH = f"{CUR_PATH}/data"

logger = create_logger(f'{CUR_PATH}/../logs', f'data_download', backupCount=10)


def deleteFileIfExist(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def download_file_from_url(url, save_path, headers=None, cookies=None, data=None, method="GET"):
    logger.info(f"download start : {save_path}")
    if method == "GET":
        response = requests.get(url, headers=headers, cookies=cookies, data=data, stream=True)
    elif method == "POST":
        response = requests.post(url, headers=headers, cookies=cookies, data=data, stream=True)

    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
    else:
        raise Exception(f"download error : {response.status_code}")

    logger.info(f"download end : {save_path}")


def unzip_file(file_path, dest):
    # tmp_dest = f'{CUR_PATH}/data/tmp_{datetime.datetime.today().strftime("%Y-%m-%d_%H_%M_%S")}'
    logger.info(f"unzip file -> {file_path} to {dest}")

    try:
        with zipfile.ZipFile(file_path, 'r') as zf:
            logger.info('open success')
            zipinfo = zf.infolist()
            logger.info(f'zipinfo {zipinfo}')
            for info in zipinfo:
                info.filename = info.filename.encode('cp437').decode('euc-kr')
                logger.info(f"unzip ... {info.filename}")
                zf.extract(info, path=dest)

            zf.close()

        return True
    except Exception as e:
        logger.info('unzip error : {}'.format(e))

    return False


def download_address(type):
    prev_month_date = datetime.today() + relativedelta(months=-1)

    YEAR = prev_month_date.year
    PREV_MONTH = format(prev_month_date.month, '02')
    YEAR_MONTH = f"{YEAR}{PREV_MONTH}"
    PATH = f"{CUR_PATH}/data"
    TMP_NAME = f"{PATH}/{type}_{datetime.today().strftime('%Y-%m-%d')}"
    DOWNLOAD_FILE_PATH = TMP_NAME + ".zip"

    os.makedirs(PATH, exist_ok=True)

    deleteFileIfExist(TMP_NAME)


    if type == "building_addr":
        URL = f"https://business.juso.go.kr/addrlink/download.do?reqType=ALLRDNM&regYmd={YEAR}&ctprvnCd=01&stdde={YEAR_MONTH}&fileName={YEAR_MONTH}_%EA%B1%B4%EB%AC%BCDB_%EB%B3%80%EB%8F%99%EB%B6%84.zip&intNum=undefined&intFileNo=undefined&realFileName={YEAR_MONTH}ALLRDNM01.zip"
    else:
        URL = f"https://business.juso.go.kr/addrlink/download.do?reqType=ALLMTCHG&regYmd={YEAR}&ctprvnCd=01&stdde={YEAR_MONTH}&fileName={YEAR_MONTH}_%EC%A3%BC%EC%86%8CDB_%EB%B3%80%EB%8F%99%EB%B6%84.zip&intNum=undefined&intFileNo=undefined&realFileName={YEAR_MONTH}ALLMTCHG01.zip"

    download_file_from_url(URL, DOWNLOAD_FILE_PATH)

    # 다운로드 완료 후 zip 파일이 이상한 경우가 있어서, 압축이 해제되는 것까지 확인
    success = unzip_file(DOWNLOAD_FILE_PATH, TMP_NAME)

    # 압축 해제된 디렉토리 삭제
    deleteFileIfExist(TMP_NAME)

    if not success:
        return None, None

    return DOWNLOAD_FILE_PATH, f"{YEAR_MONTH}"


def get_district_data_by_type(type):
    if type == "district_polygon":
        return {
            'infId': 'OA-15560',
            'seqNo': '',
            'seq': '5',
            'infSeq': '3',
        }
    elif type == "district_foot_traffic":
        return {
            'srvType': 'S',
            'infId': 'OA-15568',
            'serviceKind': '1',
            'pageNo': '1',
            'gridTotalCnt': '32994',
            'ssUserId': 'SAMPLE_VIEW',
            'strWhere': '',
            'strOrderby': 'STDR_YYQU_CD DESC',
            'filterCol': '',
            'txtFilter': '',
        }
    elif type == "district_office_workers":
        return {
            'srvType': 'S',
            'infId': 'OA-15569',
            'serviceKind': '1',
            'pageNo': '1',
            'gridTotalCnt': '29235',
            'ssUserId': 'SAMPLE_VIEW',
            'strWhere': '',
            'strOrderby': '',
            'filterCol': '',
            'txtFilter': '',
        }
    elif type == "district_resident":
        return {
            'srvType': 'S',
            'infId': 'OA-15584',
            'serviceKind': '1',
            'pageNo': '1',
            'gridTotalCnt': '27736',
            'ssUserId': 'SAMPLE_VIEW',
            'strWhere': '',
            'strOrderby': '',
            'filterCol': '',
            'txtFilter': '',
        }
    elif type == "district_resident_alltime":
        return {
            'srvType': 'S',
            'infId': 'OA-15583',
            'serviceKind': '1',
            'pageNo': '1',
            'gridTotalCnt': '18530',
            'ssUserId': 'SAMPLE_VIEW',
            'strWhere': '',
            'strOrderby': '',
            'filterCol': '',
            'txtFilter': '',
        }
    elif type == "district_foot_traffic_seoul":
        return {
            'srvType': 'S',
            'infId': 'OA-22180',
            'serviceKind': '1',
            'pageNo': '1',
            'gridTotalCnt': '20',
            'ssUserId': 'SAMPLE_VIEW',
            'strWhere': '',
            'strOrderby': '',
            'filterCol': '',
            'txtFilter': '',
        }


def download_building_data(type):

    DOWNLOAD_PATH = f"{TMP_DOWNLOAD_PATH}/tmp_{type}"
    deleteFileIfExist(DOWNLOAD_PATH)

    find_title = "표제부" if type == "building_leg_headline" else "층별개요"


    logger.info(f"download_building_data {type} {find_title}")

    # ChromeOptions를 생성하여 다운로드 옵션을 설정
    options = Options()
    # 다운로드 경로 설정
    # options.add_argument(f"download.default_directory=/path/to/download/directory")
    options.add_argument(f"download.default_directory={DOWNLOAD_PATH}")
    options.add_argument(f"savefile.default_directory={DOWNLOAD_PATH}")
    prefs = {
        "download.default_directory": DOWNLOAD_PATH,
        "savefile.default_directory": DOWNLOAD_PATH
    }
    options.add_experimental_option('prefs', prefs)

    # 다운로드 시 프롬프트 표시 안 함
    options.add_argument("download.prompt_for_download=false")
    # 다운로드 경로 자동 업그레이드
    options.add_argument("download.directory_upgrade=true")
    # 안전한 다운로드 사용 안 함
    options.add_argument("safebrowsing.enabled=false")
    options.add_argument("profile.default_content_setting_values.automatic_downloads: 1")
    # options.add_argument("download_restrictions: 3")
    options.add_argument('--headless')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        url = "https://open.eais.go.kr/main/main.do"
        driver.get(url)

        time.sleep(5)

        wait = WebDriverWait(driver, 10)

        more_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@alt='공지사항더보기']")))
        more_link.click()

        title_link = wait.until(EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{find_title}')]")))
        title_link.click()

        # title_content = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='content']")))

        # logger.info(title_content.text)

        date = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div[5]/div/div[2]/form/div[3]/table/tbody/tr[2]/td[1]")))

        logger.info(f'date {date.text}')
        date_text = date.text

        download_links = driver.find_elements(By.XPATH, '//a[starts-with(@href, "javascript:download(")]')
        for link in download_links:
            driver.execute_script(f"arguments[0].click();", link)

        MAX_TRY = 1 * 60 * 12
        count = 0
        while True:
            file_list = os.listdir(DOWNLOAD_PATH)
            file_list_zip = [file for file in file_list if not file.endswith(".crdownload")]

            if len(download_links) == len(file_list_zip):
                logger.info("completed")
                break
            elif count >= MAX_TRY:
                logger.info("give up")
                driver.quit()
                return None, None

            logger.info(f"not completed {file_list_zip}")
            time.sleep(5)
            count += 1


        OUTPUT_FILE_PATH = f"{TMP_DOWNLOAD_PATH}/{type}_output"
        logger.info(f"merge seperated zip files...{DOWNLOAD_PATH} -> {OUTPUT_FILE_PATH}")
        merge_split_zip_files(DOWNLOAD_PATH, OUTPUT_FILE_PATH)
        logger.info(f"merge end")


        output_list = os.listdir(OUTPUT_FILE_PATH)
        output_zip = [file for file in output_list if file.endswith(".zip")]
        logger.info(f"output zip files... {output_zip}")
        # deleteFileIfExist(DOWNLOAD_PATH)
        driver.quit()
        return f"{OUTPUT_FILE_PATH}/{output_zip[0]}", f"{date_text}등록"
    except Exception as e:
        print(f"error {e}")
        driver.quit()
        return None, None


def get_land_download_param_by_type(type):
    if type == 'land_info':
        return 29, "토지임야정보"
    elif type == 'land_char_info':
        return 4, "토지특성정보"
    elif type == 'land_usage_info':
        return 14, "토지이용계획정보"
    elif type == 'individual_announced_price':
        return 6, "개별공시지가정보"
    elif type == 'address_polygon':
        return 23, "연속지적도형정보"
def download_land_data(type):
    DOWNLOAD_PATH = f"{TMP_DOWNLOAD_PATH}/tmp_{type}"
    deleteFileIfExist(DOWNLOAD_PATH)

    data_id, data_name = get_land_download_param_by_type(type)


    logger.info(f"download_land_data {type} {data_id} {data_name}")

    # ChromeOptions를 생성하여 다운로드 옵션을 설정
    options = Options()
    # 다운로드 경로 설정
    options.add_argument(f"download.default_directory={DOWNLOAD_PATH}")
    options.add_argument(f"savefile.default_directory={DOWNLOAD_PATH}")
    prefs = {
        "download.default_directory": DOWNLOAD_PATH,
        "savefile.default_directory": DOWNLOAD_PATH
    }
    options.add_experimental_option('prefs', prefs)

    # 다운로드 시 프롬프트 표시 안 함
    options.add_argument("download.prompt_for_download=false")
    # 다운로드 경로 자동 업그레이드
    options.add_argument("download.directory_upgrade=true")
    # 안전한 다운로드 사용 안 함
    options.add_argument("safebrowsing.enabled=false")
    options.add_argument("profile.default_content_setting_values.automatic_downloads: 1")
    # options.add_argument("download_restrictions: 3")
    options.add_argument('--headless')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    WebDriverWait(driver, 5)

    try:
        url = f"https://www.vworld.kr/dtna/dtna_fileDataView_s001.do?dataSetSeq={data_id}"
        driver.get(url)
        driver.implicitly_wait(5)

        # Select Seoul
        sido_cd_element = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidoCd option[value='11']"))
        )
        sido_cd_element.click()
        driver.implicitly_wait(5)
        # select all
        if type == 'land_info' or type == 'address_polygon':
            file_gbn_cd_element = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#fileGbnCd option[value='AL']"))
            )
            file_gbn_cd_element.click()
            driver.implicitly_wait(5)

        search_button = WebDriverWait(driver, 40).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn_search[href='javascript:fnSearch();']"))
        )
        search_button.click()
        driver.implicitly_wait(5)

        page_size_element = WebDriverWait(driver, 0).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#pageSize option[value='100']"))
        )
        page_size_element.click()
        driver.implicitly_wait(5)

        rows = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, f"//tr[td[contains(text(), '{data_name}')]]"))
        )

        logger.info(f'all text {rows[0].text}')
        date_text = rows[0].find_element(By.XPATH, ".//td[5]").text
        logger.info(f'date_text {date_text}')

        # 첫번째만 선택
        checkbox = rows[0].find_element(By.XPATH, ".//input[@type='checkbox']")
        if not checkbox.is_selected():
            logger.info("select!")
            checkbox.click()
            driver.implicitly_wait(5)

        try:
            confirm_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".confirm_button_selector"))  # Update this selector
            )
            confirm_button.click()
            driver.implicitly_wait(20)
        except Exception as e:
            logger.info(f"No confirmation dialog appeared:{e}")

        select_download_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='javascript:fnMultiDownload();']"))
        )
        select_download_button.click()
        driver.implicitly_wait(5)

        MAX_TRY = 60 * 12
        count = 0
        while True:
            file_list = None
            try:
                file_list = os.listdir(DOWNLOAD_PATH)
                file_list_zip = [file for file in file_list if not file.endswith(".crdownload")]
                if 1 == len(file_list_zip):
                    logger.info(f"completed {file_list}")
                    break

            except Exception as e:
                logger.warn(e)

            logger.info(f"not completed {file_list}")
            time.sleep(5)
            count += 1

            if count >= MAX_TRY:
                logger.info("give up")
                driver.quit()
                return None, None

        output_list = os.listdir(DOWNLOAD_PATH)
        output_zip = [file for file in output_list if file.endswith(".zip")]
        logger.info(f"output zip files... {output_zip}")

        return f"{DOWNLOAD_PATH}/{output_zip[0]}", f"{date_text}등록"
    finally:
        driver.quit()


def download_distinct(type):
    cookies = {
        '_ga': 'GA1.1.1257076746.1716180082',
        'WL_PCID': '17161800818288195833047',
        'WMONID': 'RtZGZZmX5l3',
        '_ga_0T3XG23CN7': 'GS1.1.1716428826.10.1.1716430278.60.0.0',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': '_ga=GA1.1.1257076746.1716180082; WL_PCID=17161800818288195833047; WMONID=RtZGZZmX5l3; _ga_0T3XG23CN7=GS1.1.1716428826.10.1.1716430278.60.0.0',
        'Origin': 'https://data.seoul.go.kr',
        'Referer': 'https://data.seoul.go.kr/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    data = get_district_data_by_type(type)

    ext = "zip" if type == "district_polygon" else "csv"
    url = "http://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do" if type == "district_polygon" else "http://datafile.seoul.go.kr/bigfile/iot/sheet/csv/download.do"
    DOWNLOAD_FILE_PATH = f"{TMP_DOWNLOAD_PATH}/{type}_{datetime.today().strftime('%Y-%m-%d')}.{ext}"

    download_file_from_url(url,
                           DOWNLOAD_FILE_PATH, headers, cookies, data, method="POST")
    return DOWNLOAD_FILE_PATH, f"{datetime.today().strftime('%Y%m')}"

def download_leg_dong_codes():
    cookies = {
        'SCOUTER': 'z44lbdh4einsot',
        'JSESSIONID': 'Pj5ucfhukRTeMkiUoworR1Cu.CODROOTserver2',
        'clientid': '020038134702',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.code.go.kr',
        'Referer': 'https://www.code.go.kr/stdcode/regCodeL.do',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    data = 'cPage=1&regionCd_pk=&chkWantCnt=0&reqSggCd=*&reqUmdCd=*&reqRiCd=*&searchOk=0&codeseId=%EB%B2%95%EC%A0%95%EB%8F%99%EC%BD%94%EB%93%9C&pageSize=10&regionCd=&locataddNm=&sidoCd=*&sggCd=*&umdCd=*&riCd=*&disuseAt=0&stdate='

    DOWNLOAD_FILE_PATH = f"{TMP_DOWNLOAD_PATH}/leg_dong_codes_{datetime.today().strftime('%Y-%m-%d')}.zip"

    download_file_from_url("https://www.code.go.kr/etc/codeFullDown.do",
                           DOWNLOAD_FILE_PATH,
                           method="POST",
                           headers=headers, cookies=cookies, data=data)

    return DOWNLOAD_FILE_PATH, f"{datetime.today().strftime('%Y%m')}"


def downloadFile(type):
    if type == 'address' or type == 'building_addr':
        return download_address(type)
    elif type == 'building_leg_headline' or type == 'building_floor_info':
        return download_building_data(type)
    elif type == 'district_polygon' or type == 'district_foot_traffic' or\
        type == 'district_office_workers' or type == 'district_resident' or type == 'district_resident_alltime' or\
        type == 'district_foot_traffic_seoul':
        return download_distinct(type)
    elif type == 'land_info' or type == 'land_char_info' or type == 'land_usage_info'\
        or type == 'individual_announced_price' or type == 'address_polygon':
        return download_land_data(type)
    elif type == 'leg_dong_codes':
        return download_leg_dong_codes()

    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--type",
        type=str,
        default=None,
        help="data type",
    )
    args = parser.parse_args()
    type = args.type

    logger.info(f"START {type}")

    file_db = open(f"{CUR_PATH}/../config/config.json", 'r')
    config = json.load(file_db)
    config_name = config["NAME"]
    public_data_path = config['PUBLIC_DATA_PATH']

    mysql_con = mysql.connector.connect(host=config["DB_HOST"], port=config["DB_PORT"], database=config["DB_NAME"],
                                        user=config["DB_USER"],
                                        password=config["DB_PASS"])
    mysql_con.autocommit = True
    cursor = mysql_con.cursor(dictionary=True)

    bot = TelegramBot(config["TG_TOKEN"], config["TG_CID"])


    try:
        # bot.send_message(f"데이터 다운로드 시작 >  {get_dataname_by_type(type)}")

        cursor.execute("SELECT * FROM public_data_files WHERE type=%s and cancel_yn = 'N' order by created_at desc limit 2", (type,))
        results = cursor.fetchall()
        logger.info(f"prev results: {results}")
        prev_filesize = 0
        if results and len(results) > 0:
            prev_filesize = results[0]["size"]

        logger.info(f"prev file size : {prev_filesize}")
        path, memo = downloadFile(type)
        if path is not None:
            file_size = os.path.getsize(path)
            logger.info(f"file size prev : {prev_filesize}, cur : {file_size}")
            if prev_filesize != file_size:

                logger.info(f"need update")
                file_ext = pathlib.Path(path).suffix.lower()
                dest_dir = f"{public_data_path}{datetime.today().strftime('%Y%m')}"

                os.makedirs(dest_dir, exist_ok=True)

                dest_file_path = f"{dest_dir}/{type}_{datetime.today().strftime('%Y-%m-%d_%H_%M_%S')}{file_ext}"
                logger.info(f"file_ext : {file_ext}")
                logger.info(f"file_move : {path} -> {dest_file_path}")
                shutil.move(path, dest_file_path)
                cursor.execute("INSERT INTO public_data_files (type, memo, path, size, auto_yn) VALUES (%s, %s, %s, %s, %s)", (type, memo, dest_file_path, file_size, "Y"))

                bot.send_message(f"[{config_name}] 새로운 데이터 다운로드 완료 > {get_dataname_by_type(type)}, memo : {memo}")

            else:
                # bot.send_message(f"데이터 다운로드 완료 > {get_dataname_by_type(type)}, 데이터 업데이트 필요없음")
                logger.info(f"same file size")
                if os.path.exists(path):
                    os.remove(path)
        else:
            logger.info(f"failed to download {type}")
            bot.send_message(f"[{config_name}] 데이터 다운로드 실패!! > {get_dataname_by_type(type)}")

        mysql_con.close()

    except Exception as e:
        bot.send_message(f"[{config_name}] 데이터 다운로드 실패!! > {get_dataname_by_type(type)}, {e}")

        logger.error(f"ERROR {e}")


    logger.info(f"END")




if __name__ == '__main__':
    main()