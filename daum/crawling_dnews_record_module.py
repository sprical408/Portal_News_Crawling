# -*- coding: utf-8 -*-

import os  # 시스템관련
from asyncio import events
# from data_crawling_analysis.env import env_common
from fake_useragent import UserAgent
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from os.path import getsize
import pandas as pd  # 데이터 분석
import numpy as np  # 행렬 연산
import numpy

import requests  # http 요청
from bs4 import BeautifulSoup  # 웹 데이터 크롤링 또는 스크래핑
from selenium import webdriver  # By MIN
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import concurrent.futures
import time  # 시간 지연
import tqdm  # for 문의 진행상태 파악
from tqdm.notebook import tqdm
from selenium.webdriver.chrome.options import Options
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
from selenium.common.exceptions import NoAlertPresentException, NoSuchElementException, TimeoutException, \
    ElementNotInteractableException, NoSuchWindowException, NoSuchFrameException

import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
import time

crawling_path = 'data/dnews/'
directory_bar = '/blog/'


def crawl_and_save_text(keyword):

    # 경로 지정 및 폴더 내에 있는 csv파일 불러오기
    folder_path = 'C:/Users/user/Desktop/albastella/MC_crawling_nblog_record_module/data/dnews/admin_13_' + keyword + '_2023/'
    file_name = 'admin_13_' + keyword + '_2023_news_url.csv'
    file_path = folder_path + file_name

    options = Options()
    userAgent_name = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
    options.add_argument(f'user-agent={userAgent_name}')
    driver = webdriver.Chrome(chrome_options=options, executable_path='./chromedriver_win32/chromedriver')

    #driver = webdriver.Chrome(r"C:\Users\user\Desktop\albastella\MC_crawling_nblog_record_module\chromedriver_win32\chromedriver")
    data = pd.read_csv(file_path)

    title_list = []
    text_list = []

    # URL 별로 크롤링하여 본문내용을 가져오기
    for url in tqdm(data['url']):
        driver.get(url)
        time.sleep(4)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # html soup로부터 text 추출
        text = soup.get_text()
        text = re.sub(r'[\n\t]+', '', text) # 개행문자, 탭 문자 제거
        text = re.sub(r'\[[^\]]+\]', '', text) # 대괄호로 둘러싸인 내용 제거
        text = re.sub(r'\s\s+', ' ', text) # 여러 개의 공백 문자를 하나의 공백 문자로 대체
        text_list.append(text)

        #url title 추출
        title = soup.find('title').get_text()
        title_list.append(title)

    # 데이터프레임에 본문내용 열을 추가합니다.
    data['text_list'] = text_list
    data['title_list'] = title_list

    # 지정된 경로로 csv 파일 저장
    data.to_csv(crawling_path + keywordstorageNm + "/" + keyword + "_record.csv", index=False)
    driver.close()

    print('File Mergin Succeed..!')
    sleep(2)

if __name__ == "__main__":
    keyword = "메가커피"
    keywordstorageNm = "admin_13_메가커피_2023"
    crawlingFile = crawl_and_save_text(keyword)
