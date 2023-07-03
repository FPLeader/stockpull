import os
import json
import requests
import math
from os.path import exists
from datetime import datetime, timedelta, date

def dl_PDF(jsItem, url_fc='', path='', filename=''):
    os.makedirs(path, exist_ok = True)
    print("downloading " + filename + "...")
    try:
        r = requests.get(url_fc,
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
            },
            timeout=1000
        )
        fname = os.path.join(path + "/",filename)
        with open(fname, 'wb') as fd:
            fd.write(r.content)
        return True
    except:
        return False

def get_url_for_report_dg(pn=1,pz=100,dt='2020-01-01'):
    siteURL = 'http://reportapi.eastmoney.com/report/dg'
    para = {
        'pageNo': pn,
        'pageSize': pz,
        'endTime': '2050-12-31',
        'beginTime': dt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def get_data_for_report_dg(pn=1, pz=100, dt='2020-01-01'):
    r = requests.get(get_url_for_report_dg(pn=pn, pz=pz, dt=dt),
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        },
        timeout=1000
    ).json()
    return r['data']

def get_total_page(pn=1, pz=100, dt='2020-01-01'):
    r = requests.get(get_url_for_report_dg(pn=pn, pz=pz, dt=dt),
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        },
        timeout=1000
    ).json()
    return r['hits']

def main_DG():
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/CW"):
        os.mkdir(os.path.dirname(__file__) + "/../download/DG")

    start_date = '2023-06-01'
    pz = 100

    if not os.path.isdir("../download/PDG"):
        os.mkdir(os.path.dirname(__file__) + "/../download/PDG")
    
    if exists("../download/PDG/start_date.txt"):
        with open("../download/PDG/start_date.txt", "r") as f:
            file_date = f.read()
            start_date = file_date


    # get total report count and pages
    total_count = get_total_page(pn=1, pz=100, dt=start_date)
    total_pages = int(total_count/pz) + 1

    # make hyyb report list file
    print("writing hyyb.txt file...")
    print("total pages is " + str(total_pages))

    with open("../download/PDG/hyyb.txt", "w") as f:
        f.write('[')
        for i in range(total_pages):
            page_number = i + 1
            print("writing page" + str(page_number) + "...")
            list_response = get_data_for_report_dg(pn=page_number, pz=100, dt=start_date)
            count = 1
            list_length = len(list_response)
            for item in list_response:
                f.write(json.dumps(item))
                if (page_number != total_pages):
                    f.write(',')
                else:
                    if (count != list_length):
                        f.write(',')
                count += 1
        f.write(']')
    
    print('successfully text file made.')

    with open("../download/PDG/start_date.txt", "w") as f:
        yesterday = datetime.now() - timedelta(1)
        yesterday = datetime.strftime(yesterday, "%Y-%m-%d")
        f.write(str(yesterday))

    #download report pdf files
    print("start downloading pdf files...")
    with open("../download/PDG/hyyb.txt", "r") as f:
        all_list = f.read()
    
    json_data = json.loads(all_list)
    path = "../download/PDG/hyyb"
    os.makedirs(path, exist_ok = True)
    for item in json_data:
        # url_fc= 'http://pdf.dfcfw.com/pdf/H3_'+item['infoCode']+'_1.pdf'
        # print(url_fc)
        dl_PDF(
            item,
            url_fc= 'http://pdf.dfcfw.com/pdf/H3_'+item['infoCode']+'_1.pdf',
            path = os.path.join(path,item['infoCode'].replace('AP','')[0:8]+'/'+item['columnType']),
            filename = item['infoCode']+'.pdf'
        )

if __name__ == "__main__":
    main_DG()