
import sys,requests,json,math
sys.path.insert(0,'..')
# import EM_TOOL, MDB

siteURL = 'http://push2.eastmoney.com/api/qt/clist/get'
# siteURL = 'ttp://13.push2.eastmoney.com/api/qt/clist/get'


def url_LIST(fs='b:MK0300',fields='f12,f13,f14',pn=1,pz=100):
    para = {'pn': pn,'pz': pz,'fs': fs,'fields': fields,'po': 0, 'np': 1,'fltt': 2,'invt': 2,'fid': 'f3'}
    return requests.Request('GET', url=siteURL, params=para).prepare().url


def url_LIST_RMB(fields='f12,f13,f14',pn=1,pz=100):
    return url_LIST(fs='m:121+t:1',fields=fields,pn=pn,pz=pz)


def fetch_push_init(url,TYPE): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=20
    )
    js = json.loads(r.content)
    try:
        pages = math.ceil(int(js['data']['total'])/100)+1
    except:
        return 0
    js = js['data']['diff']
    request = []
    for ele in js:
        if 'f13' in ele:
            IDE = ele['f12']+{1:'1', 0:'2'}.get(ele['f13'], '')
            js = {"TYPE":TYPE,   "IDE":IDE, "IDS":ele['f14'],   "ID6":ele['f12'],   "SEC":ele['f13']}
        else:
            IDE = ele['f12']
            js = {"TYPE":TYPE,   "IDE":IDE,  "IDS":ele['f14'],   "ID6":ele['f12']}
        # print(js)
        request.append(js)
    # if request: MDB.col_IDLIST.bulk_write(request)fetch_push_init
    return request
# for test:
# allpages = fetch_push_init(url_LIST(pn=1,pz=1000),"forex")
# allpages = fetch_push_init(url_LIST_RMB(pn=1,pz=1000),"forex")


allfields='f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f24,f25,f152,f297'    
columnsNameSpace_Detail = {
    "f1": "f1", #?
    "f2": "C", #最新价
    "f3": "CP", #涨跌幅
    "f4": "CV", #涨跌额
    "f5": "V", #成交量
    "f6": "A", #成交额
    "f7": "CPW", #振幅
    "f8": "T", #换手率
    "f9": "PE", #市盈率
    "f10": "VB", #量比
    "f11": "CP_M5", #5分钟涨跌幅
    "f12":"ID6", #代码
    "f13":"SEC", #market 0:sh 1:sz
    "f14":"IDS", #名称
    "f15":"H", #最高价
    "f16":"L", #最低价
    "f17":"O", #今开
    "f18":"PC", #昨收
    "f24":"CP_60", #60日涨跌幅
    "f25":"CP_Y",  #年初至今涨跌幅
    "f152":"f152",  #?
    "f297":"DT",     
}
colnames_Detail = [columnsNameSpace_Detail.get(x,'') for x in allfields.split(",")]

dellist_Quate = ["ID6","IDS"]


def parseContent_Detail(content):
    js = json.loads(content)
    try:
        js = js['data']['diff']
    except:
        return True
    request = []
    for ele in js:
        record = {}
        # print(ele)
        for key in ele.keys():  record[columnsNameSpace_Detail.get(key)] = ele[key]
        # record['IDE'] = record['ID6'] + str(record['SEC'])
        record['IDE'] = record['ID6']
        DT = str(record['DT']); DT = DT[0:4]+'-'+DT[4:6]+'-'+DT[6:8]; record['DT'] = DT
        for key in dellist_Quate: del record[key]
        request.append(pymongo.UpdateOne({"IDE":record['IDE'],"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_FOREX.bulk_write(request)
    return True



fields2='f51,f52,f53,f54,f55,f56,f57,f59,f61'
columnsNameSpace_KLINE = {
    "f51": "DT", 
    "f52": "O",
    "f53": "C",
    "f54": "H",
    "f55": "L",
    "f56": "V",
    "f57": "A",
    "f58": "ZP", 
    "f59": "CP", 
    "f60": "CPE", 
    "f61": "T",   
}
colnames = [columnsNameSpace_KLINE.get(x,'') for x in fields2.split(",")]
# print(colnames)

# colnames = ','.join([columnsNameSpace_KLINE.get(x,'') for x in fields2.split(",")])


def url_DK(id6='CADUSD', sec="119",lmt=700, fields2='f51,f52,f53,f54,f55,f56,f57,f59,f61'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
    para = {
        'secid': str(sec) +'.'+str(id6),
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
        'fields1': 'f0',
        'fields2': fields2,
        'klt': 101,
        'fqt': 0,
        'end': 20500101,
        'lmt': lmt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url
# print(url_DK())


def parser_DK(content,ide):
    js = json.loads(content)
    request = []
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames[index]] = key
        request.append(pymongo.UpdateOne({"IDE":ide,"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_FOREX.bulk_write(request)
    return True