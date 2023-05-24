
import sys,requests,json,pymongo,math
sys.path.insert(0,'..')
from MDB import col_CW_FH
from datetime import datetime
from dateutil.relativedelta import relativedelta
one_yrs_ago = datetime.now() - relativedelta(years=10)
lastdatelimit = one_yrs_ago.strftime('%Y-%m-%d')
# print(lastdatelimit)

def url_FenHong(CQCXR='2000-01-01',pn=1,pz=100):
    siteURL = 'http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get'
    para = {
        'type':'DCSOBS',
        'token': '70f12f2f4f091e459a279469fe49eca5',
        'st': 'YAGGR',
        'sr': -1,
        'p': pn,
        'ps': pz,
        'filter':"(YAGGR>='" + CQCXR + "')",
        'js':'{"data":(x),"pages":(tp)}'
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url
# print(url_FenHong())

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
    js = json.loads(r.content)
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
# print(fetch_push(url_FenHong()))


def Do(pn):
    try:
        allpages = fetch_push(url_FenHong(CQCXR=lastdatelimit,pn=pn,pz=100))
    except:
        pass