import sys, requests, json, math
sys.path.insert(0,'..')
import EM_TOOL

siteURL = 'http://push2.eastmoney.com/api/qt/clist/get'
def url_CALIST(fs='m:0 t:6,m:0 t:13,m:0 t:80,m:1 t:2,m:1 t:23',fields='f12,f13,f14',pn=1,pz=100):
    para = {'pn': pn,'pz': pz,'fs': fs,'fields': fields,'po': 0, 'np': 1,'fltt': 2,'invt': 2,'fid': 'f12'}
    return requests.Request('GET', url=siteURL, params=para).prepare().url


def url_CILIST(fields = 'f12,f13,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:0 t:5,m:1 s:2',fields = fields, pz=pz, pn=pn)

def url_DQLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:1',fields = fields, pz=pz, pn=pn)

def url_GNLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:3',fields = fields, pz=pz, pn=pn)

def url_HYLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:2',fields = fields, pz=pz, pn=pn)
    

def parseContent(content): 
    js = json.loads(content)
    try:
        pages = math.ceil(int(js['data']['total'])/100)+1
    except:
        return 0
    js = js['data']['diff']
    request = []
    for ele in js:
        request.append(
            pymongo.UpdateOne(
                {"IDE":ele['f12']+{1:'1', 0:'2'}.get(ele['f13'], '')},
                {"$set":{"TYPE":"stock",    "IDS":ele['f14'],   "ID6":ele['f12'],   "SEC":ele['f13']}},
                upsert=True
            )
        )
    if request: MDB.col_IDLIST.bulk_write(request)
    return pages
    
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
    # request = []
    # for ele in js:
    #     if 'f13' in ele:
    #         IDE = ele['f12']+{1:'1', 0:'2'}.get(ele['f13'], '')
    #         js = {"TYPE":TYPE,    "IDS":ele['f14'],   "ID6":ele['f12'],   "SEC":ele['f13']}
    #     else:
    #         IDE = ele['f12']
    #         js = {"TYPE":TYPE,    "IDS":ele['f14'],   "ID6":ele['f12']}
        # request.append(pymongo.UpdateOne({"IDE":IDE},{"$set": js},upsert=True))
    # if request: MDB.col_IDLIST.bulk_write(request)
    return js


allfields='f2,f3,f5,f6,f8,f9,f10,f12,f13,f14,f15,f16,f17,f20,f21,f23,f24,f25,f26,f35,f37,f38,f39,f62,f66,f72,f78,f84,f100,f102,f103,f114,f265,f297'    
columnsNameSpace_Detail = {
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
    ##    "f19":"PC", #6
    "f20":"ZONG_VALUE", #总市值
    "f21":"LT_VALUE", #流通市值
    "f22":"ZHANGSU", #涨速
    "f23":"PB", #市净率
    "f24":"CP_60", #60日涨跌幅
    "f25":"CP_Y",  #年初至今涨跌幅
    "f26":"StartDT", #上市日
    ##f27 0
    ##f28 -
    # "f29":"ctype",
    ##f30 -12621
    "f33":"WeiBi", #委比
    "f34":"WaiPan", #外盘
    "f35":"NeiPan", #内盘
    "f37":"ROE", #ROE
    "f38":"ZGB", #总股本
    "f39":"LTG", #流通股
    #     "f40":"WeiBi", #总营收
    #     "f41":"WeiBi", #同比
    # ##    "f42":"WeiBi" #委比
    # ##    "f43":"WeiBi" #委比
    # ##    "f44":"WeiBi" #委比
    #     "f45":"WeiBi", #净利润
    #     "f46":"WeiBi", #同比
    # ##    "f47":"WeiBi" #委比
    #     "f48":"WeiBi", #每股未分配利润
    #     "f49":"WeiBi", #委比
    #     "f50":"WeiBi", #委比
    #     "f51":"WeiBi", #委比
    #     "f52":"WeiBi", #委比
    #     "f53":"WeiBi", #委比
    #     "f54":"WeiBi", #委比
    #     "f55":"WeiBi", #委比
    "f62":"F_ZHU", #今日主力净流入 净额
    "f66":"F_C", #今日超大单净流入 净额
    "f69":"ZJLX_CHAODA_P", #今日超大单净流入 净占比
    "f72":"F_D", #今日大单净流入 净额
    "f75":"ZJLX_DA_P", #今日大单净流入 净占比
    "f78":"F_Z", #今日中单净流入 净额
    "f81":"ZJLX_ZHONG_P", #今日中单净流入 净占比
    "f84":"F_X", #今日小单净流入 净额
    "f87":"ZJLX_XIAO_P", #今日小单净流入 净占比
    # "f99": "ctype",
    "f100":"HY_BK_IDS", #行业板块
    "f102":"DQ_BK_IDS", #地区板块
    "f103":"GN_BK_IDS", #概念板块
    "f109":"CP_5", #5日涨跌幅
    "f114":"PEd", #PE(动)
    "f160":"CP_10",#10日涨跌幅
    "f127":"CP_3",#3日涨跌幅
    "f164":"ZJLX_ZHULI_5",      #5日主力净流入 净额
    "f165":"ZJLX_ZHULI_5_P",    #5日主力净流入 净占比
    "f166":"ZJLX_CHAODA_5",     #5日超大单净流入 净额
    "f167":"ZJLX_CHAODA_5_P",   #5日超大单净流入 净占比
    "f168":"ZJLX_DA_5",         #5日大单净流入 净额
    "f169":"ZJLX_DA_5_P",       #5日大单净流入 净占比
    "f170":"ZJLX_ZHONG_5",      #5日中单净流入 净额
    "f171":"ZJLX_ZHONG_5_P",    #5日中单净流入 净占比
    "f172":"ZJLX_XIAO_5",       #5日小单净流入 净额
    "f173":"ZJLX_XIAO_5_P",     #5日小单净流入 净占比
    "f174":"ZJLX_ZHULI_10",      #10日主力净流入 净额
    "f175":"ZJLX_ZHULI_10_P",    #10日主力净流入 净占比
    "f176":"ZJLX_CHAODA_10",     #10日超大单净流入 净额
    "f177":"ZJLX_CHAODA_10_P",   #10日超大单净流入 净占比
    "f178":"ZJLX_DA_10",         #10日大单净流入 净额
    "f179":"ZJLX_DA_10_P",       #10日大单净流入 净占比
    "f180":"ZJLX_ZHONG_10",      #10日中单净流入 净额
    "f181":"ZJLX_ZHONG_10_P",    #10日中单净流入 净占比
    "f182":"ZJLX_XIAO_10",       #10日小单净流入 净额
    "f183":"ZJLX_XIAO_10_P",     #10日小单净流入 净占比
    "f184":"ZJLX_XIAO_P",        #今日主力净流入 净占比
    "f265":"HY_BK_ID6", #行业代码
    "f267":"ZJLX_ZHULI_3",      #3日主力净流入 净额
    "f268":"ZJLX_ZHULI_3_P",    #3日主力净流入 净占比
    "f269":"ZJLX_CHAODA_3",     #3日超大单净流入 净额
    "f270":"ZJLX_CHAODA_3_P",   #3日超大单净流入 净占比
    "f271":"ZJLX_DA_3",         #3日大单净流入 净额
    "f272":"ZJLX_DA_3_P",       #3日大单净流入 净占比
    "f273":"ZJLX_ZHONG_3",      #3日中单净流入 净额
    "f274":"ZJLX_ZHONG_3_P",    #3日中单净流入 净占比
    "f275":"ZJLX_XIAO_3",       #3日小单净流入 净额
    "f276":"ZJLX_XIAO_3_P",     #3日小单净流入 净占比
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
    request_IDLIST = []; request = []
    for ele in js:
        record = {}
        for key in ele.keys():  record[columnsNameSpace_Detail.get(key)] = ele[key]
        record['IDE'] = ele['f12'] + {1:'1', 0:'2'}.get(ele['f13'], '')
        DT = str(record['DT']); DT = DT[0:4]+'-'+DT[4:6]+'-'+DT[6:8]; record['DT'] = DT
        request_IDLIST.append(pymongo.UpdateOne({"IDE":record['IDE']},{"$set":record},upsert=True))
        for key in dellist_Quate: del record[key]
        request.append(pymongo.UpdateOne({"IDE":record['IDE'],"DT":record['DT']},{"$set":record},upsert=True))
    if request_IDLIST:  MDB.col_IDLIST.bulk_write(request_IDLIST)
    if request:         MDB.col_QUATE.bulk_write(request)
    return True


def parseContent_Detail_IDX(content):
    dellist = ["DQ_BK_IDS","GN_BK_IDS","HY_BK_ID6","HY_BK_IDS","StartDT","PB","PEd","ROE"]
    js = json.loads(content)
    try:
        js = js['data']['diff']
    except:
        return True
    request_IDLIST = []; request = []
    for ele in js:
        record = {}
        for key in ele.keys():  record[columnsNameSpace_Detail.get(key)] = ele[key]
        record['IDE'] = ele['f12'] + {1:'1', 0:'2'}.get(ele['f13'], '')
        DT = str(record['DT']); DT = DT[0:4]+'-'+DT[4:6]+'-'+DT[6:8]; record['DT'] = DT
        for key in dellist: del record[key]
        request_IDLIST.append(pymongo.UpdateOne({"IDE":record['IDE']},{"$set":record},upsert=True))
        for key in dellist_Quate: del record[key]
        request.append(pymongo.UpdateOne({"IDE":record['IDE'],"DT":record['DT']},{"$set":record},upsert=True))
    if request_IDLIST:  MDB.col_IDLIST.bulk_write(request_IDLIST)
    if request:         MDB.col_QUATEIDX.bulk_write(request)
    return True

def fetch_push(url): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    return parseContent_Detail(r.content)

# print(url_CALIST(fields = allfields, pz = 100))
# fetch_push(url_CALIST(fields = allfields, pz = 100))

# s = ['f'+str(y+1) for y in range(1000)];print(url_CALIST(fields = ",".join(s), pz = 5))


def url_CW_html(js):
    ide = js['IDE']
    from bs4 import BeautifulSoup
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
        MDB.col_IDLIST.bulk_write([pymongo.UpdateOne({"IDE":ide},{"$set":{"ctype":hidctype['value']}},upsert=True)])        
    except: return


    # print({"IDE":ide},{"ctype":hidctype['value']})
    # MDB.col_IDLIST.update_one({"IDE":ide,"ctype":hidctype['value']})




