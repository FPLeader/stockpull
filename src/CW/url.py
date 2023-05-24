import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB, datetime

def url_ZCFZB2(ide='0000012',companyType=4,dates="2000-01-01"):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def url_LRB2(ide='0000012',companyType=4,dates="2000-01-01"):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/lrbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def url_XJLLB2(ide='0000012',companyType=4,dates="2000-01-01"):
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewFinanceAnalysis/xjllbAjaxNew'
    para = {
            'companyType': companyType,
            'reportDateType': 0,
            'reportType':1,
            'code': EM_TOOL.IDE2SID(ide),
            'dates': dates,
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def parseContent_zcfzb2(content,ide):
    try:   
        js = json.loads(content) 
        js = js['data'] 
    except: return True
    request = []
    for e in js:
        e['REPORTDATE'] = datetime.datetime.strptime(e['REPORT_DATE'],'%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
        request.append(pymongo.UpdateOne({"IDE":ide,"REPORTDATE":e['REPORTDATE']},{"$set":e},upsert=True ))
    if request: MDB.col_CW2.bulk_write(request)
    return True


def fetch_push(ide,ctype,dates): 
    r = requests.get(url_ZCFZB2(ide=ide,companyType=ctype,dates=dates), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    print(r.url)
    if parseContent_zcfzb2(r.content,ide) == False: return False
    

    r = requests.get(url_LRB2(ide=ide,companyType=ctype,dates=dates), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=30
    )
    # print(r.url)
    if parseContent_zcfzb2(r.content,ide) == False: return False

    r = requests.get(url_XJLLB2(ide=ide,companyType=ctype,dates=dates), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    # print(r.url)
    if parseContent_zcfzb2(r.content,ide) == False: return False
    return True
# print(fetch_push("0000012",4,"2021-09-30,2021-12-31"))