#coding:utf-8
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
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


#获取行业历史2017年至2018年
def get_instury_history(code,year):
    url = 'http://d.10jqka.com.cn/v4/line/bk_{}/01/{}.js'.format(code,year)
    html = get_page_detail(url).decode('gbk')
    pattern = re.compile('{"data":"(.*?)"}',re.S)
    instury = re.findall(pattern,html)[0].replace(';','').split(',')
    for i in range(0,len(instury)-7,7): 
        yield {
            'code':code,
            'date':instury[i],
            'open_price':instury[i+1],
            'high_price':instury[i+2],
            'low_price':instury[i+3],
            'close_price':instury[i+4],
            'volume':instury[i+5],
            'amount':instury[i+6]
        }

def save_to_mysql(code,year):
    industry_history_info = get_instury_history(code,year)
    industry_history_df = pd.DataFrame(industry_history_info)
    print(industry_history_df.head(5))
    engine = create_engine('mysql://liangzhi:liangzhi123@192.168.2.52/financial_data?charset=utf8')
    industry_history_df.to_sql('industry_history', engine, if_exists='append')

def main():
    instury_index_url = 'http://q.10jqka.com.cn/thshy/'
    industry_index_info = get_industry_list(instury_index_url)
    for i in industry_index_info:
        code = i['code']
    #获取2017年和2018年的历史行情数据（只有2017年和2018年）
        for year in (2017,2018):
            save_to_mysql(code, year)
        
        time.sleep(random.randint(0,5))
    # break
        

if __name__ == '__main__':
    main()








