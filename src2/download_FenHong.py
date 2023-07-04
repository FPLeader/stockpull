import os
import json, requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_list(CQCXR='2000-01-01',pn=1,pz=200):
    siteURL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    para = {
        'reportName' : 'RPT_DATE_SHAREBONUS_DET',
        'columns' : 'ALL',
        'quoteColumns' : '',
        'pageNumber' : '1',
        'sortColumns' : 'REPORT_DATE',
        'sortTypes' : '-1',
        'source' : 'WEB',
        'client' : 'WEB',
        '_' : '1686592227740'
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    return fetch_push(url)

def get_data(report_date):
    siteURL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    para = {
        'sortColumns' : 'PLAN_NOTICE_DATE',
        'sortTypes' : '-1',
        'pageNumber' : '1',
        'reportName' : 'RPT_SHAREBONUS_DET',
        'columns' : 'ALL',
        'quoteColumns' : '',
        'js' : '{"data":(x),"pages":(tp)}',
        'source' : 'WEB',
        'client' : 'WEB',
        'filter' : "(REPORT_DATE='" + report_date + "')"
    }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    return fetch_push(url)

def fetch_push(url): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=1000
    )
    # print(r.content)
    js = json.loads(r.content)
    return js

def main_FenHong():
    print("Starting FenHong download...")
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/FENHONG"):
        os.mkdir(os.path.dirname(__file__) + "/../download/FENHONG")

    one_yrs_ago = datetime.now() - relativedelta(years=10)
    last_date_limit = one_yrs_ago.strftime('%Y-%m-%d')
    response = get_list()
    list = response['result']['data']
    # downloading FenHong database for only recent 3 years.
    count = 1
    for x in list:
        report_date = datetime.strptime(x['REPORT_DATE'], "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        prefix_filename = datetime.strptime(x['REPORT_DATE'], "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d')
        print("downloading and writing text file for " + report_date + " report")
        data_response = get_data(report_date)
        text = json.dumps(data_response)
        script_dir = os.path.dirname(__file__)
        rel_path = "../download/FENHONG/FENHONG_" + prefix_filename + ".txt"
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, "w") as f:
            f.write(text)
        if (count < 6):
            count += 1
        else:
            break
    print("Finished FenHong download successfully!")
    print("---------------------------------------------")

if __name__ == '__main__':
    main_FenHong()
        

