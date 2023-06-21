
import sys,requests,json,math
sys.path.insert(0,'..')
# from MDB import col_CW_FH
from datetime import datetime
from dateutil.relativedelta import relativedelta
one_yrs_ago = datetime.now() - relativedelta(years=10)
lastdatelimit = one_yrs_ago.strftime('%Y-%m-%d')
# print(lastdatelimit)

def get_list(CQCXR='2000-01-01',pn=1,pz=100):
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
    # siteURL = 'http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get'
    # para = {
    #     'type':'DCSOBS',
    #     'token': '70f12f2f4f091e459a279469fe49eca5',
    #     'st': 'YAGGR',
    #     'sr': -1,
    #     'p': pn,
    #     'ps': pz,
    #     'filter':"(YAGGR>='" + CQCXR + "')",
    #     'js':'{"data":(x),"pages":(tp)}'
    # }
    url = requests.Request('GET', url=siteURL, params=para).prepare().url
    return fetch_push(url)
# print(url_FenHong())

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

def parser(content):
    js = json.loads(content)
    try:
        pages = int(js['pages'])
    except:
        return 0
    js = js['data']
    request = []
    for ele in js:
        request.append(pymongo.UpdateOne({"RowNum":ele['RowNum']},{"$set":ele},upsert=True))
    if request: col_CW_FH.bulk_write(request)
    return pages    


def fetch_push(url): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    print(r.content)
    js = json.loads(r.content)
    # try:
    #     pages = int(js['pages'])
    # except:
    #     return 0
    # js = js['data']
    # request = []
    # for ele in js:
    #     request.append(pymongo.UpdateOne({"RowNum":ele['RowNum']},{"$set":ele},upsert=True))
    # if request: col_CW_FH.bulk_write(request)
    # return pages
    return js
# print(fetch_push(url_FenHong()))


def Do(pn):
    try:
        allpages = fetch_push(url_FenHong(CQCXR=lastdatelimit,pn=pn,pz=100))
    except:
        pass