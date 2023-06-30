import requests
import os
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import EM_TOOL

# get last days list of last 8 quareters(2 years)
def get_lastday_of_quarter():
    last_days = ['12-31', '09-30', '06-30', '03-31']
    current_date = datetime.now().date()
    current_year = current_date.year
    dates = ''
    number = 8
    count = 1
    index = 0
    while (count <= number) :
        date = str(current_year) + '-' + last_days[index]
        if (date < str(current_date)):
            count += 1
            dates = dates + date + ','
        index += 1
        if (index == 4):
            current_year -= 1
            index = 0
    return dates

def make_text_file(filename, data):
    text = json.dumps(data)
    script_dir = os.path.dirname(__file__)
    rel_path = "../download/CW/" + filename + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, "w", encoding='utf-8') as f:
        f.write(text)
    print(filename + " file was made successfully!")

def get_type(js):
    ide = js['IDE']
    siteURL = 'https://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/Index'
    para = {
            'type': 'web',
            'code': EM_TOOL.IDE2SID(ide)
    }
    _url = requests.Request('GET', url=siteURL, params=para).prepare().url
    try:
        r = requests.get(_url, timeout=10,
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                                'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
            }
        )
        r.encoding = 'utf8'
        if (r.status_code != 200): return False
        allParse = BeautifulSoup(r.content, 'html.parser')
        hidctype = allParse.find("input", {"id": "hidctype"})
        ctype = hidctype['value']
        return ctype
    except: return

def get_ZCFZB2(ide='0000012', companyType=4, dates='2023-03-31'):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    r = requests.get(url,
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    js = json.loads(r.content)
    return js
    # print(r.content)

def get_XJLLB2(ide='0000012',companyType=4,dates="2023-03-31"):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/xjllbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    r = requests.get(url,
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    js = json.loads(r.content)
    return js

def get_LRB2(ide='0000012',companyType=4,dates="2000-01-01"):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/lrbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    r = requests.get(url,
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    js = json.loads(r.content)
    return js

def get_ZYZ2(ide='0000012'):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew'
    para = {
        'type': 0,
        'code': EM_TOOL.IDE2SID(ide)
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    r = requests.get(url,
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    js = json.loads(r.content)
    return js

def get_DBFX2(ide='0000012'):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/DBFXAjaxNew'
    para = {
        'code': EM_TOOL.IDE2SID(ide)
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    r = requests.get(url,
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    js = json.loads(r.content)
    return js

if __name__ == "__main__":
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/CW"):
        os.mkdir(os.path.dirname(__file__) + "/../download/CW")

    with open("../download/IDELIST/CALIST.txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    for index in list:
        # get 8 last days of quarters
        dates = get_lastday_of_quarter()
        ctype = get_type(index)
        ZCFZB2_result = get_ZCFZB2(ide=index['IDE'], companyType=ctype, dates=dates)
        make_text_file("ZCFZB2_" + index['IDE'], ZCFZB2_result)
        XJLLB2_result = get_XJLLB2(ide=index['IDE'], companyType=ctype, dates=dates)
        make_text_file("XJLLB2_" + index['IDE'], XJLLB2_result)
        LRB2_result = get_LRB2(ide=index['IDE'], companyType=ctype, dates=dates)
        make_text_file("LRB2_" + index['IDE'], LRB2_result)
        ZYZ2_result = get_ZYZ2(ide=index['IDE'])
        make_text_file("ZYZ2_" + index['IDE'], ZYZ2_result)
        DBFX2_result = get_DBFX2(ide=index['IDE'])
        make_text_file("DBFX2_" + index['IDE'], DBFX2_result)