
import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB

fields2='f51,f53,f54,f55,f56,f57,f58'
columnsNameSpace_M1KLINE = {
    "f51": "DTHM", 
    "f53": "C",
    "f54": "H",
    "f55": "L",
    "f56": "V",
    "f57": "A",
    "f58": "CC", 
}
colnames = [columnsNameSpace_M1KLINE.get(x,'') for x in fields2.split(",")]




def url_DK(ide='0000012', fields2='f51,f53,f54,f55,f56,f57,f58'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/trends2/get'
    para = {
        'secid': EM_TOOL.IDE2SECID(ide),
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
        'fields1': 'f0',
        'fields2': fields2,
        'ndays': 1,
        'iscr': 0
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url
# print(url_DK())

def parser(content,ide):
    js = json.loads(content)
    if js['data'] == None: return True
    if 'klines' not in js['data']: return True
    request = []
    M1 = [];MDT = '1900-01-01'
    for line in js['data']['trends']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames[index]] = key
        DT = record['DTHM'][0:10];  HM = record['DTHM'][11:16]; record['HM'] = HM; del record['DTHM']
        if DT != MDT:
            if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'M1':M1}},upsert=True ))
            M1 = [];    MDT = DT
        M1.append(record)
    if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'M1':M1}},upsert=True ))
    if request: MDB.col_QUATE.bulk_write(request)
    return True
        

def parser_IDX(content,ide):
    js = json.loads(content)
    if js['data'] == None: return True
    if 'klines' not in js['data']: return True
    request = []
    M1 = [];MDT = '1900-01-01';DT=False
    for line in js['data']['trends']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames[index]] = key
        DT = record['DTHM'][0:10];  HM = record['DTHM'][11:16]; record['HM'] = HM; del record['DTHM']
        if DT != MDT:
            if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'M1':M1}},upsert=True ))
            M1 = [];    MDT = DT
        M1.append(record)
    if M1: request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'M1':M1}},upsert=True ))
    if request: MDB.col_QUATEIDX.bulk_write(request)
    return True




fields2_FLOW='f51,f52,f53,f54,f55,f56'
columnsNameSpace_M1FLOW = {
    "f51": "DTHM", 
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
colnames_FLOW = [columnsNameSpace_M1FLOW.get(x,'') for x in fields2_FLOW.split(",")]

def url_FLOW(ide='0000012', fields2='f51,f52,f53,f54,f55,f56'):
    siteURL = 'http://push2.eastmoney.com/api/qt/stock/fflow/kline/get'
    para = {
        'secid': EM_TOOL.IDE2SECID(ide),
        'ut': 'b2884a393a59ad64002292a3e90d46a5',
        'fields1': 'f0',
        'fields2': fields2,
        'klt': 1,
        'lmt': 0
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url
# print(url_DK())



def parser_FLOW(content,ide):
    js = json.loads(content)
    if js['data'] == None: return True
    if 'klines' not in js['data']: return True
    request = []
    M1 = [];MDT = '1900-01-01';DT=False
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames_FLOW[index]] = key
        DT = record['DTHM'][0:10];  HM = record['DTHM'][11:16]; record['HM'] = HM; del record['DTHM']
        if DT != MDT:
            if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'FM1':M1}},upsert=True ))
            M1 = [];    MDT = DT
        M1.append(record)
    if DT:
        if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'FM1':M1}},upsert=True ))
    if request: MDB.col_QUATE.bulk_write(request)
    return True
        



def parser_IDX_FLOW(content,ide):
    js = json.loads(content)
    if js['data'] == None: return True
    if 'klines' not in js['data']: return True
    request = []
    M1 = [];MDT = '1900-01-01';DT=False
    for line in js['data']['klines']:
        record = {}
        for index, key in enumerate(line.split(",")): record[colnames_FLOW[index]] = key
        DT = record['DTHM'][0:10];  HM = record['DTHM'][11:16]; record['HM'] = HM; del record['DTHM']
        if DT != MDT:
            if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'FM1':M1}},upsert=True ))
            M1 = [];    MDT = DT
        M1.append(record)
    if DT:
        if M1:  request.append(pymongo.UpdateOne({"IDE":ide,"DT":DT},{"$set":{'FM1':M1}},upsert=True ))
    if request: MDB.col_QUATEIDX.bulk_write(request)
    return True




def fetch_push(ide): 
    r = requests.get(url_FLOW(ide=ide), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    # return parser(r.content,ide)
    return parser_IDX_FLOW(r.content,ide)
# print(fetch_push("0000011"))
# print(fetch_push("BK1026"))

   



