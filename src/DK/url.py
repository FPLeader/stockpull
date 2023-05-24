
import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB



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


def url_DK(ide='0000012', lmt=700, fields2='f51,f52,f53,f54,f55,f56,f57,f59,f61'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
    para = {
        'secid': EM_TOOL.IDE2SECID(ide),
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

def parser(content,ide):
    js = json.loads(content)
    request = []
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames[index]] = key
        request.append(pymongo.UpdateOne({"IDE":ide,"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_QUATE.bulk_write(request)
    return True

def parser_IDX(content,ide):
    js = json.loads(content)
    request = []
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames[index]] = key
        request.append(pymongo.UpdateOne({"IDE":ide,"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_QUATEIDX.bulk_write(request)
    return True



# KFLOW +++++++++++++++++++++++++++++++
fields2_FLOW = 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62'
columnsNameSpace_KFLOW = {
    "f51": "DT", 
    "f52": "F_ZHU",
    "f53": "F_X",
    "f54": "F_Z",
    "f55": "F_D",
    "f56": "F_C",
    "f57": "F_ZHU_B",
    "f58": "F_X_B", 
    "f59": "F_Z_B", 
    "f60": "F_D_B", 
    "f61": "F_C_B", 
    "f62": "CP", 
}
colnames_FLOW = [columnsNameSpace_KFLOW.get(x,'') for x in fields2_FLOW.split(",")]


def url_FLOW(ide='0000012', lmt=700, fields2='f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get'
    para = {
        'secid': EM_TOOL.IDE2SECID(ide),
        'ut': 'b2884a393a59ad64002292a3e90d46a5',
        'fields1': 'f0',
        'fields2': fields2,
        'klt': 101,
        'lmt': lmt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url
# print(url_DK())

def parser_FLOW(content,ide):
    js = json.loads(content)
    request = []
    if js['data'] == None: return
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames_FLOW[index]] = key
        request.append(pymongo.UpdateOne({"IDE":ide,"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_QUATE.bulk_write(request)
    return True

def parser_IDX_FLOW(content,ide):
    js = json.loads(content)
    if js['data'] == None: return
    request = []
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames_FLOW[index]] = key
        request.append(pymongo.UpdateOne({"IDE":ide,"DT":record['DT']},{"$set":record},upsert=True))
    if request: MDB.col_QUATEIDX.bulk_write(request)
    return True


def fetch_push(ide): 
    # r = requests.get(url_DK(ide=ide), 
    # print(url_FLOW(ide=ide))
    r = requests.get(url_FLOW(ide=ide), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    # return parser(r.content,ide)
    return parser_IDX_FLOW(r.content,ide)
# print(fetch_push("BK1026"))

   



