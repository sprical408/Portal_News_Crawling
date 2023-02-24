# -*- coding: utf-8 -*-

import os   # 시스템관련
from asyncio import events
# from data_crawling_analysis.env import env_common
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from os.path import getsize
import pandas as pd  # 데이터 분석 
import numpy as np  # 행렬 연산
import numpy
import urllib
import requests  # http 요청
from bs4 import BeautifulSoup  # 웹 데이터 크롤링 또는 스크래핑
from selenium import webdriver # By MIN
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import concurrent.futures
import time  # 시간 지연
import tqdm  # for 문의 진행상태 파악
from tqdm.notebook import tqdm
import io
import json
# import db_module
# from data_crawling_analysis.dbcon import mongodb_module  # 몽고 DB 연결
# from data_crawling_analysis.dbcon import db_module
import datetime
import math
import multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from urllib.request import urlopen
import requests
import time  # 시간 지연
from time import sleep
import random
import glob
from selenium.common.exceptions import NoAlertPresentException, NoSuchElementException, TimeoutException, ElementNotInteractableException,NoSuchWindowException, NoSuchFrameException

from selenium.webdriver.chrome.options import Options

from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
import lxml
import re

from fake_useragent import UserAgent
# 서비스 환경별 구분
# service_type = env_common.service_type()
# crawling_json_url = service_type['crawling_json_url']
# crawling_path = service_type['crawling_path']
# chromedriver = service_type['chromedriver']
# directory_bar = service_type['directory_bar']

crawling_path = 'data/' # By MIN
directory_bar = '/' # By MIN

def get_url_link(url: str):
    url_link = []
    # 403 Error 방지를 위한 추가 헤더 설정
    headers = {'User-Agent':
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'}
    try:
        req = urllib.request.Request(url, headers=headers)
        response = urlopen(req)

        if response.status == 200:
            url_link.append(url)
            sleep(1)
            return url_link
    except Exception as e:
        print(e)
        print("[%s] Error for URL : %s" % (datetime.datetime.now(), url))
        #return None

def work_crawling(url,title, ps):
    dict = {}  # 전체 크롤링 데이터를 담을 그릇
    # driver = env_common.chrom_type(crawling_path, chromedriver, 'site', url)

    # userAgent_name = UserAgent().random
    # options.add_argument(f'user-agent={userAgent_name}')
    options = Options()
    userAgent_name = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
    options.add_argument(f'user-agent={userAgent_name}')

    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    driver.implicitly_wait(7)
    driver.get(url)
    time.sleep(4)

    html = driver.page_source

    soup = BeautifulSoup(html, 'html.parser')

    # html soup로부터 text 추출
    text = soup.get_text()
    text = re.sub(r'[\n\t]+', '', text)  # 개행문자, 탭 문자 제거
    text = re.sub(r'\[[^\]]+\]', '', text)  # 대괄호로 둘러싸인 내용 제거
    text = re.sub(r'\s\s+', ' ', text)  # 여러 개의 공백 문자를 하나의 공백 문자로 대체

    # title = soup.find('title').get_text()
    target_info = {}
    # iframe 접근
    target_info['title'] = title
    target_info['content'] = text
    target_info['url'] = url
    target_info['ps'] = ps

    # 각각의 글은 dict 라는 딕셔너리에 담음
    dict[int(ps)] = target_info
    time.sleep(2)

    # 판다스로 만들기
    result_df = pd.DataFrame({'index': dict})
    # print(result_df)
    return result_df



def do_html_crawl(url: str, title, keywordstorageNm: str, ps: str):

    result_df = work_crawling(url,title, ps)
        
    if result_df.empty:
        z = random.randint(1,9)
        ps = 10+z
        result_df = work_crawling(url,title, ps)

    return result_df


def do_process_with_thread_crawl(keywordstorageNm, urls:str, titles : list):
    if urls is not None:
        urls = get_url_link(urls)
        do_thread_crawl(urls,titles, keywordstorageNm)


def do_thread_crawl(urls: list,titles : list, keywordstorageNm):
    thread_list = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        for url in urls:
            addinfo = url.split("_")
            crawlingChannel = addinfo[1]
            ps = addinfo[2]
            try:
                future = executor.submit(do_html_crawl, url, titles, keywordstorageNm, ps)
                print('=========',future)
            except:
                print('********',future)
                time.sleep(1)
                continue
            # 스케쥴링
            thread_list.append(future)

        for future in as_completed(thread_list):
            result = future.result()
            done = future.done()
            cancelled = future.cancelled

            # 최초 생성 이후 mode는 append(a)
            if not os.path.exists(crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_'+crawlingChannel+'_'+str(ps)+'.csv'):
                result.to_csv(crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_'+crawlingChannel+'_'+str(ps)+'.csv', index=False, mode='w', encoding='utf-8-sig', header=True)
            else:
                result.to_csv(crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_'+crawlingChannel+'_'+str(ps)+'.csv', index=False, mode='a', encoding='utf-8-sig', header=False)

            #print('size', result.size)
            if result.size > 0:
                resultDic = result.iat[0,0]
                # DB 저장
                # mongodb_module.save_record(resultDic['title'], resultDic['nickname'], resultDic['datetime'], resultDic['content'], keywordstorageNm, crawlingChannel, url)
            else:
                print('array has a size of 0') 
        
        for execution in concurrent.futures.as_completed(thread_list):
            execution.result()   

  
def crawling_record(keyword, crawlingIdx, crawlingFile, keywordstorageNm, crawlingChannel):

    now = datetime.datetime.now()
    nowDate = now.strftime('%Y%m%d') 

    urls = []
    titles = []
    process = multiprocessing.cpu_count() #프로세서 개수 확인
    print('프로세스개수:'+str(process))
    #프로세스갯수에 맞게 아래 조절
    code_list = [i+1 for i in range(process)]

    # "url_list.csv" 불러오기
    print(crawling_path+keywordstorageNm+directory_bar+crawlingFile)
    url_load = pd.read_csv(crawling_path+keywordstorageNm+directory_bar+crawlingFile, encoding='utf-8-sig')
    num_list = len(url_load)
    # 수집할 글 갯수
    number = num_list

    for i in tqdm(numpy.arange(0, number)): 
        choicelist = random.choice(code_list)
        addinfo = crawlingChannel+'_'+str(choicelist)

        titles.append(url_load['title'][i])
        url = url_load['url'][i]
        url = url.strip()
        url = url+'?_'+addinfo

        urls.append(url)
        print(i,url)
            
    start = time.time() # 실행 시간 측정

    pool = Pool(processes=process)
    do_process_with_thread_crawl_partial = partial(do_process_with_thread_crawl)
    pool.starmap(do_process_with_thread_crawl_partial, [(keywordstorageNm, url, title) for url, title in zip(urls, titles)])

    # 모든 프로세스 종료까지 기다림
    pool.close()
    pool.join()
    
    # # mongodb 저장 완료 시 MysqlDB 수집 설정 상세 테이블에 입력된 MonogDB 컬렉션명과 수집정보를 저장하고 수집설정 테이블에 완료 flag 을 업데이트 해준다.
    # fileSize = getsize(crawling_path+keywordstorageNm+directory_bar+crawlingFile)
    # db_module.crawling_record(keywordstorageNm, crawlingIdx, fileSize, number, crawlingChannel)

    print("--- elapsed time %s seconds ---" % (time.time() - start))
    
    input_path = crawling_path+keywordstorageNm # csv파일들이 있는 디렉토리 위치
    output_file = crawling_path+keywordstorageNm+directory_bar+keywordstorageNm+'_'+crawlingChannel+'.csv' # 저장 파일명
    file_list = glob.glob(os.path.join(input_path, '*_'+crawlingChannel+'_*.csv')) # 모든 csv파일 선택

    tmpList = []
    for file_name in file_list:
        df = pd.read_csv(file_name, sep=',', encoding='utf-8-sig')
        tmpList.append(df)
    totDf = pd.concat(tmpList)
    totDf.reset_index(drop=True, inplace=True)
    totDf.to_csv(output_file, sep=',', index=False, encoding='utf-8-sig')

    print('File Mergin Succeed..!')
    sleep(2)
    
    #개별 파일 삭제            
    file_list = glob.glob(f"{input_path}/{keywordstorageNm}_"+crawlingChannel+"_*.csv")

    for f in file_list:
        os.remove(f)

if __name__ == "__main__":

    keyword = "ai최근근황"
    crawlingIdx = 13
    crawlingFile = 'admin_13_ai최근근황_2023_ddoc_url.csv'
    keywordstorageNm = 'admin_13_ai최근근황_2023'
    crawlingChannel = 'ddoc'

    crawlingResult = crawling_record(keyword, crawlingIdx, crawlingFile, keywordstorageNm, crawlingChannel)