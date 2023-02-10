"""
@auther daumit
@since 2022/12/22
@contents google news 크롤링
"""

# -*- coding: utf-8 -*-

# from data_crawling_analysis.env import env_common -> 현 상황 불필요
import os   # 시스템관련
import pandas as pd  # 데이터 분석
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium import webdriver # By MIN
import concurrent.futures
import tqdm  # for 문의 진행상태 파악
from tqdm.notebook import tqdm
import datetime
import numpy
import time  # 시간 지연
from time import sleep
import multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
from urllib.request import urlopen
from urllib import parse
from functools import partial
import random
import glob

from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
# 서비스 환경별 구분 -> 현 상황 불필요
# service_type = env_common.service_type()
# crawling_json_url = service_type['crawling_json_url']
# crawling_path = service_type['crawling_path']
# chromedriver = service_type['chromedriver']
# directory_bar = service_type['directory_bar']

crawling_path = 'data/' # By MIN
directory_bar = '/' # By MIN

def get_url_link(url: str, keywordstorageNm):
    url_link = []
    # 403 Error 방지를 위한 추가 헤더 설정
    headers = {'User-Agent':
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'}
    try:
        req =urllib.request.Request(url, headers=headers)
        response = urlopen(req)

        if response.status == 200:
            url_link.append(url)
            sleep(1)
            return url_link, keywordstorageNm

    except Exception as e:
        print(e)
        print("[%s] Error for URL : %s" % (datetime.datetime.now(), url))
        #return None

def do_html_crawl(url: str, keywordstorageNm: str):
    url_list = []
    title_list = []

    #프로세스 확인하기
    ps = url.split("ps=")
    ps1 = ps[1]

    # driver = env_common.chrom_type(crawling_path, chromedriver, 'site', url)

    # 크롬 드라이버 불러오기
    options = Options()
    userAgent_name = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
    options.add_argument(f'user-agent={userAgent_name}')
    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    driver.implicitly_wait(7)
    driver.get(url)

    time.sleep(1)
    
    # URL, Title 크롤링 시작
    for i in range(1, 11):
        try:
            article_raw = driver.find_elements(By.XPATH, f'//*[@id="rso"]/div/div/div[{i}]/div/div/a')

            for article in article_raw:
                url_each = article.get_attribute('href')
                url_list.append(url_each)
            time.sleep(1)

            # 원인은 알 수 없으나, 낮은 확률로 구글의 검색 결과 Title의 위치가 다르게 적용된 것이 있어, 해당 에러를 방지하기 위해 2가지 경우로 나눔
            try :
                title_each = driver.find_elements(By.XPATH, f'//*[@id="rso"]/div/div/div[{i}]/div/div/a/div/div[2]/div[2]')[0].text
            except:
                title_each = driver.find_elements(By.XPATH, f'// *[ @ id = "rso"] / div / div / div[{i}] / div / div / a / div / div / div[2]')[0].text
            title_list.append(title_each)
            time.sleep(1)

        except:
            break

    # 수집된 정보를 재료로 새로운 DataFrame 생성
    df = pd.DataFrame({'url': url_list, 'title': title_list, 'ps' : ps1})


    driver.close()
    driver.quit()

    return df

def do_process_with_thread_crawl(keywordstorageNm, urls:str):  
    if urls is not None:
        urls, keywordstorageNm = get_url_link(urls, keywordstorageNm)
        do_thread_crawl(urls, keywordstorageNm)

def do_thread_crawl(urls: list, keywordstorageNm):

    thread_list = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for url in urls:
            future = executor.submit(do_html_crawl, url, keywordstorageNm)

            # 스케쥴링
            thread_list.append(future)

        for future in as_completed(thread_list):
            result = future.result()
            done = future.done()
            cancelled = future.cancelled

            if result.size > 0:
                ps = result.iat[0,2]
            else:
                ps = 1  #변경하지 말것
                print('array has a size of 0') 

            # 최초 생성 이후 mode는 append(a)
            # if not os.path.exists(crawling_path+keywordstorageNm+directory_bar+'_news_url_'+str(ps)+'.csv'):
            result.to_csv(crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_news_url_'+str(ps)+'.csv', index=False, mode='a', encoding='utf-8-sig', header=False)

            # else:
            #     result.to_csv(crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_news_url_'+str(ps)+'.csv', index=False, mode='a', encoding='utf-8-sig', header=False)


        for execution in concurrent.futures.as_completed(thread_list):
            execution.result()    
            

def crawling(keyword, keywordstorageNm, crawlingSdate, crawlingEdate):
    # 구글의 날짜 형식에 맞춰 Start Date, End Date 재구성
    start_year = crawlingSdate[:4]
    start_month = crawlingSdate[4:6]
    start_day = crawlingSdate[6:]

    end_year = crawlingEdate[:4]
    end_month = crawlingEdate[4:6]
    end_day = crawlingEdate[6:]

    # Keyword, Date에 맞춰 구글의 뉴스 검색 결과 URL 생성
    url = f'https://www.google.com/search?q={keyword}&tbs=cdr:1,cd_min:{start_month}/{start_day}/{start_year},cd_max:{end_month}/{end_day}/{end_year}&tbm=nws&start=0'

    # Step 1. "구글" 사이트 열기
    # driver = env_common.chrom_type(crawling_path, chromedriver, 'channel','google')
    # driver = webdriver.Chrome('./chromedriver_win32/chromedriver')

    # 크롬 드라이버 불러오기
    options = Options()
    userAgent_name = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
    options.add_argument(f'user-agent={userAgent_name}')

    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    driver.implicitly_wait(7)
    driver.get(url)

    url_list = []
    title_list = []

    process = multiprocessing.cpu_count() #프로세서 개수 확인
    print('프로세스개수:'+str(process))
    #프로세스갯수에 맞게 아래 조절
    code_list = [i+1 for i in range(process)]

    # 몇개의 페이지를 크롤링 할지 선택
    # 전체 게시물 갯수 확인
    urls = []

    # 전체 검색 결과 갯수 파악이 어렵기 때문에, 모든 검색 페이지를 한번씩 접근하여 각 Title, URL 수집하는 방향으로
    # 다음 페이지가 없을 경우, 반복문 종료
    while True:
        try:
            choicelist = random.choice(code_list)
            url = driver.current_url.strip()
            urls.append(url+f'&ps={choicelist}')            # 각 프로세스 번호 부여
            print(url+f'&ps={choicelist}')
            driver.find_element(By.LINK_TEXT, "다음").click()# 다음 검색 결과 페이지 접근
            time.sleep(1)
        except:
            break

    start = time.time() # 실행 시간 측정
                
    #폴더 생성
    if not os.path.exists(crawling_path+keywordstorageNm):
        os.makedirs(crawling_path+keywordstorageNm)
    
    pool = Pool(processes=process)
    pool.map(partial(do_process_with_thread_crawl, keywordstorageNm), urls)

    # 모든 프로세스 종료까지 기다림
    pool.close()
    pool.join()
    
    print("--- elapsed time %s seconds ---" % (time.time() - start))

    input_path = crawling_path+keywordstorageNm+directory_bar # csv파일들이 있는 디렉토리 위치
    output_file = crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_news_url.csv' # 저장 파일명
    file_list = glob.glob(os.path.join(input_path, '*.csv')) # 모든 csv파일 선택

    totDf = pd.DataFrame(columns= ['url','title','ps'])
    for file_name in file_list:
        df = pd.read_csv(file_name, sep=',', encoding='utf-8-sig', names = ['url', 'title', 'ps'], header = None)
        totDf = pd.concat([totDf,df])

    totDf.reset_index(drop=True, inplace=True)
    totDf.to_csv(output_file, sep=',', index=False, encoding='utf-8-sig',header= True)

    print('File Mergin Succeed..!')
    sleep(2)
    
    #개별 파일 삭제            
    file_list = glob.glob(f"{input_path}/{keywordstorageNm}_news_url_*.csv")

    for f in file_list:
        os.remove(f)
    
    driver.close()     
    driver.quit()
    
    return keywordstorageNm+"_news_url.csv"


if __name__ == "__main__":

    keyword = "카드"
    keywordstorageNm = "admin_13_카드_20230201"
    crawlingSdate = "20230201"
    crawlingEdate = "20230203"
    crawlingFile = crawling(keyword, keywordstorageNm, crawlingSdate, crawlingEdate)
