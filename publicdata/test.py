import requests

from utils.files import merge_split_zip_files


# merge_split_zip_files("/data/sungkim/fs_bds_data/publicdata/data/tmp_building_leg_headline", "/data/sungkim/fs_bds_data/publicdata/data/tmp_building_leg_headline/output/")
# cookies = {
#     'SCOUTER': 'z44lbdh4einsot',
#     'JSESSIONID': 'Pj5ucfhukRTeMkiUoworR1Cu.CODROOTserver2',
#     'clientid': '020038134702',
# }
#
# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
#     'Cache-Control': 'max-age=0',
#     'Connection': 'keep-alive',
#     'Content-Type': 'application/x-www-form-urlencoded',
#     # 'Cookie': 'SCOUTER=z44lbdh4einsot; JSESSIONID=Pj5ucfhukRTeMkiUoworR1Cu.CODROOTserver2; clientid=020038134702',
#     'Origin': 'https://www.code.go.kr',
#     'Referer': 'https://www.code.go.kr/stdcode/regCodeL.do',
#     'Sec-Fetch-Dest': 'iframe',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-Site': 'same-origin',
#     'Sec-Fetch-User': '?1',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
#     'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"macOS"',
# }
#
# data = 'cPage=1&regionCd_pk=&chkWantCnt=0&reqSggCd=*&reqUmdCd=*&reqRiCd=*&searchOk=0&codeseId=%EB%B2%95%EC%A0%95%EB%8F%99%EC%BD%94%EB%93%9C&pageSize=10&regionCd=&locataddNm=&sidoCd=*&sggCd=*&umdCd=*&riCd=*&disuseAt=0&stdate='
#
# try:
#     response = requests.post('https://www.code.go.kr/etc/codeFullDown.do', cookies=cookies, headers=headers, data=data)
#     if response.status_code == 200:
#         # 파일 이름 추출
#         content_disposition = response.headers.get('Content-Disposition')
#         if content_disposition:
#             filename = content_disposition.split('filename=')[-1].strip('";')
#         else:
#             filename = 'downloaded_file.zip'  # 기본 파일 이름 설정
#         # 파일 저장
#         with open(filename, 'wb') as file:
#             file.write(response.content)
#         print(f"{filename} 파일을 성공적으로 다운로드했습니다.")
#     else:
#         print(f"다운로드 실패. 상태 코드: {response.status_code}")
# except requests.exceptions.RequestException as e:
#     print(f"요청 중 오류 발생: {e}")


import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta

options = Options()
options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "download.default_directory": os.path.expanduser('~/Downloads')
})
options.add_argument('--headless')

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

wait = WebDriverWait(driver, 20)

def wait_for_downloads(download_dir, timeout=300):
    """Wait for downloads to complete"""
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        for fname in os.listdir(download_dir):
            if fname.endswith('.crdownload'):
                dl_wait = True
        seconds += 1
    return not dl_wait

def is_within_last_3_months(date_string):
    """Check if the date is within the last 3 months"""
    date_format = "%Y-%m-%d"
    file_date = datetime.strptime(date_string, date_format)
    three_months_ago = datetime.now() - timedelta(days=90)
    return file_date >= three_months_ago

try:
    url = "https://www.vworld.kr/dtna/dtna_fileDataView_s001.do?dataSetSeq=14"
    driver.get(url)
    driver.implicitly_wait(5)

    sido_cd_element = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidoCd option[value='11']"))
    )
    sido_cd_element.click()

    search_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn_search[href='javascript:fnSearch();']"))
    )
    search_button.click()

    page_size_element = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#pageSize option[value='100']"))
    )
    page_size_element.click()

    try:
        rows = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, "//tr[td[contains(text(), '토지이용계획정보')]]"))
        )

        for row in rows:
            date_text = row.find_element(By.XPATH, ".//td[5]").text
            if is_within_last_3_months(date_text):
                checkbox = row.find_element(By.XPATH, ".//input[@type='checkbox']")
                if not checkbox.is_selected():
                    checkbox.click()
                    driver.implicitly_wait(5)
    except Exception as e:
        print("Error occurred while selecting rows:", e)

    try:
        confirm_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".confirm_button_selector"))
        )
        confirm_button.click()
    except Exception as e:
        print("No confirmation dialog appeared:", e)

    select_download_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='javascript:fnMultiDownload();']"))
    )
    select_download_button.click()

    download_dir = os.path.expanduser('~/Downloads')
    if wait_for_downloads(download_dir):
        print("Downloads completed successfully.")
    else:
        print("Downloads did not complete within the timeout period.")

except Exception as e:
    print(f"An error occurred: {e}")
