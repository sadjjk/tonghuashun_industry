#coding:utf-8
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
from multiprocessing import Pool 
import datetime
import pymysql
import requests
import pandas as pd 
import time 
import random
import re 


#获取动态cookies
def get_cookie():
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver=webdriver.Chrome(chrome_options=options)
    url="http://q.10jqka.com.cn/thshy/"
    driver.get(url)
    # 获取cookie列表
    cookie=driver.get_cookies()
    driver.close()
    return cookie[0]['value']

#获取网页详情页
def get_page_detail(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36', 
                'Referer':'http://q.10jqka.com.cn/thshy/detail',
                'Cookie':'v={}'.format(get_cookie())
                }
    try:
        response = requests.get(url,headers =headers)
        if response.status_code == 200:
            return response.content
        return None
    except RequestException:
        print('请求页面失败',url)
        return None

#获取行业列表 名称title、代码code、链接url
def get_industry_list(url):
    html = get_page_detail(url).decode('gbk')
    soup = BeautifulSoup(html,'lxml')
    industry_list = soup.select('.cate_items > a')
    
    for industry in industry_list:

        yield {
            'title':industry.get_text(),
            'code':industry.get('href').split('/')[-2],
            'url':industry.get('href')
        }


def get_classify_url(code):
    url  = 'http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/1/ajax/1/code/{}'.format(code)
    html = get_page_detail(url).decode('gbk')
    soup = BeautifulSoup(html,'lxml')
    try:
        page = soup.select('.page_info')[0].get_text().split('/')[-1]
        for i in range(1,int(page)+1):
            url = 'http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/{}/ajax/1/code/{}'.format(i,code)
            yield url
    except: 
        url = 'http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/1/ajax/1/code/{}'.format(code)
        yield url 

def get_classify_info(url):
    part_date = datetime.date.today().strftime("%Y%m%d")
    code = url.split('/')[-1]
    html = get_page_detail(url).decode('gbk')
    soup = BeautifulSoup(html,'lxml')
    stocks = soup.select('tr > td:nth-of-type(3) > a  ')
    for stock in stocks:
        yield {
            'industry_code':code,
            'stock':stock.get_text(),
            'stock_code':stock.get('href').split('/')[-1],
            'part_date':part_date
        }


def save_to_mysql(url):
    info = get_classify_info(url)
    info = pd.DataFrame(info)
    print(info.head(5))
    engine = create_engine('mysql://liangzhi:liangzhi123@192.168.2.52/financial_data?charset=utf8')
    info.to_sql('industry_classify', engine, if_exists='append')

def main():

    pool = Pool(processes=8)
    instury_index_url = 'http://q.10jqka.com.cn/thshy/'
    industry_index_info = get_industry_list(instury_index_url)
    for i in industry_index_info:
        code = i['code']
        urls = get_classify_url(code)

        pool.map(save_to_mysql,[url for url in urls])

        time.sleep(random.randint(0,2))


if __name__ == '__main__':
    main()
    