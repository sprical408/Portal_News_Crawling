from selenium import webdriver
import os  # 시스템관련
import pandas as pd  # 데이터 분석
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium import webdriver
from fake_useragent import UserAgent
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
import requests
from dateutil.relativedelta import relativedelta

"""
@auther daumit
@since 2022/12/22
@contents daum 뉴스 크롤링
"""

crawling_path = './data/'
directory_bar = './'


def get_url_link(url: str, keywordstorageNm):
    url_link = []
    try:
        response = urlopen(url)
        print(f'EACH URL ::: {url}')
        if response.status == 200:
            url_link.append(url)
            sleep(1)
            return url_link, keywordstorageNm
    except Exception as e:
        print(e)
        time.sleep(2)
        print("[%s] Error for URL : %s" % (datetime.datetime.now(), url))
        # return None


def do_html_crawl(url: str, keywordstorageNm: str):
    url_list = []
    title_list = []

    # 프로세스 확인하기
    ps = url.split("ps=")
    ps1 = ps[1]

    # driver = env_common.chrom_type(crawling_path, chromedriver, 'site', url)
    options = Options()
    userAgent_name = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
    options.add_argument(f'user-agent={userAgent_name}')
    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    driver.implicitly_wait(7)
    driver.get(url)

    time.sleep(1)

    # URL 및 Title 크롤링 시작
    for i in range(1, 11):
        try:
            article_raw = driver.find_elements(By.XPATH, f'//*[@id="newsColl"]/div[1]/ul/li[{i}]/div/a')

            for article in article_raw:
                url_list.append(article.get_attribute('href'))

            url_list = list(set(url_list))

            try:
                title_each = driver.find_elements(By.XPATH, f'//*[@id="newsColl"]/div[1]/ul/li[{i}]/div[2]/a')[0].text
            except:
                title_each = driver.find_elements(By.XPATH, f'//*[@id="newsColl"]/div[1]/ul/li[{i}]/div/a')[0].text
            title_list.append(title_each)

        except:
            break


    df = pd.DataFrame({'url': url_list, 'title': title_list, 'ps': ps1})

    driver.close()
    driver.quit()

    return df


def do_process_with_thread_crawl(keywordstorageNm, urls: str):
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

            # print('size', result.size)
            if result.size > 0:
                ps = result.iat[0, 2]
            else:
                ps = 1  # 변경하지 말것
                print('array has a size of 0')

                # 최초 생성 이후 mode는 append(a)
            # if not os.path.exists(crawling_path + keywordstorageNm + directory_bar + '_news_url_' + str(ps) + '.csv'):
            result.to_csv(crawling_path + keywordstorageNm + directory_bar + keywordstorageNm + '_news_url_' + str(ps) + '.csv', index=False, mode='a', encoding='utf-8-sig', header=False)

            # else:
            #     result.to_csv(crawling_path + keywordstorageNm + directory_bar + keywordstorageNm + '_news_url_' +

            #                   str(ps) + '.csv', index=False, mode='a', encoding='utf-8-sig', header=False)

        for execution in concurrent.futures.as_completed(thread_list):
            execution.result()


def crawling(keyword, keywordstorageNm, crawlingSdate, crawlingEdate):
    # Step 1. "다음" 사이트 열기
    # driver = env_common.chrom_type(crawling_path, chromedriver, 'channel','naver')

    url = f'https://search.daum.net/search?w=news&DA=STC&enc=utf8&cluster=n&cluster_page=1&q={keyword}&period=u&sd={crawlingSdate}000000&ed={crawlingEdate}235959&p=1'

    options = Options()
    ua = UserAgent()
    userAgent = ua.random
    print(userAgent)
    options.add_argument(f'user-agent={userAgent}')
    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    driver.implicitly_wait(3)
    driver.get(url)

    '''
    # Step 2 . 검색창에서 "검색어" 검색
    element = driver.find_element(By.NAME, "q")
    element.send_keys(keyword)
    driver.find_element(By.CLASS_NAME, "btn_search").click()
    time.sleep(1)

    # Step 3. "뉴스" 카테고리 선택
    driver.find_element(By.LINK_TEXT, "뉴스").click()
    time.sleep(1)
    

    # Step 4. 선택된 기간에 대한 검색 결과 클릭
    search_url = (
        "https://search.daum.net/search?w=news&DA=STC&enc=utf8&cluster=n&cluster_page=1&q={0}&period=u&sd={1}000000&ed={2}235959&p=1".format(
            keyword, crawlingSdate, crawlingEdate))
    driver.get(search_url)
    
    # 관련뉴스 닫기 (더 많은 기사를 보기 위해서)
    # driver.find_element(By.LINK_TEXT, "관련뉴스 닫기").click()
    # time.sleep(1)
    '''

    # 전체 게시물 갯수 확인
    article_cnt = driver.find_element(By.CLASS_NAME, "txt_info").text.split(' ')[-1].replace('건', '').replace(',','').strip()

    total_page = int(article_cnt)
    total_cnt = int(article_cnt)
    print(total_cnt)

    # 한페이지에 10개의 게시물
    total_page, remainder = divmod(total_page, 10)

    if remainder > 0:
        total_page = total_page + 1

    process = multiprocessing.cpu_count()  # 프로세서 개수 확인
    print('프로세스개수:' + str(process))
    # 프로세스갯수에 맞게 아래 조절
    code_list = [i+1 for i in range(process)]

    # 검색키워드, 크롤링 시작 날짜, 크롤링 끝나는 날짜 정해서 크롤링하기
    urls = []
    for i in tqdm(numpy.arange(1, total_page + 1)):  # 페이지 번호 (arange 는 0 부터 시작하므로 +1 해준다)
        choicelist = random.choice(code_list)
        url = "https://search.daum.net/search?w=news&DA=PGD&enc=utf8&cluster=n&cluster_page=1&q={0}&period=u&sd={1}000000&ed={2}235959&p={3}&ps={4}" \
            .format(parse.quote(keyword), crawlingSdate, crawlingEdate, i, choicelist)
        url = url.strip()
        urls.append(url)

    start = time.time()  # 실행 시간 측정

    # 폴더 생성
    if not os.path.exists(crawling_path + keywordstorageNm):
        os.makedirs(crawling_path + keywordstorageNm)

    pool = Pool(processes=process)
    pool.map(partial(do_process_with_thread_crawl, keywordstorageNm), urls)

    # 모든 프로세스 종료까지 기다림
    pool.close()
    pool.join()

    print("--- elapsed time %s seconds ---" % (time.time() - start))

    input_path = crawling_path + keywordstorageNm + directory_bar  # csv파일들이 있는 디렉토리 위치
    output_file = crawling_path + keywordstorageNm + directory_bar + keywordstorageNm + '_news_url.csv'  # 저장 파일명
    file_list = glob.glob(os.path.join(input_path, '*.csv'))  # 모든 csv파일 선택

    totDf = pd.DataFrame(columns= ['url','title','ps'])
    for file_name in file_list:
        df = pd.read_csv(file_name, sep=',', encoding='utf-8-sig', names = ['url', 'title', 'ps'], header = None)
        totDf = pd.concat([totDf,df])

    totDf.reset_index(drop=True, inplace=True)
    totDf.to_csv(output_file, sep=',', index=False, encoding='utf-8-sig',header= True)

    print('File Mergin Succeed..!')
    sleep(2)

    # 개별 파일 삭제
    file_list = glob.glob(f"{input_path}/{keywordstorageNm}_news_url_*.csv")

    for f in file_list:
        os.remove(f)

    driver.close()
    driver.quit()

    return keywordstorageNm + "_news_url.csv"


if __name__ == "__main__":
    keyword = "메가커피"
    keywordstorageNm = "admin_13_메가커피_2023"
    crawlingSdate = "20230201"
    crawlingEdate = "202302010"
    crawlingFile = crawling(keyword, keywordstorageNm, crawlingSdate, crawlingEdate)
    # url = "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=%EC%9C%A4%EC%84%9D%EC%97%B4&sort=0&photo=0&field=0&pd=3&ds=2022.12.30&de=2022.12.30&cluster_rank=18&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:r,p:from20221230to20221230,a:all"
    # do_html_crawl(url, keywordstorageNm)