import sys,json
sys.path.insert(0,'..')
import MDB,pymongo
from Export.yield_rows import yield_rows

def export(DB=MDB.col_DG , path="", batch_size=1000):
    delitem = {
        "_id":0,
        "infoCode":1,
        "columnType":1,
        "industryCode":1,
        "industryName":1,
        "reportType":1,
        "stockCode":1,
        "stockName":1,
        "title":1,
    }
    cursor = DB.find({}, delitem, batch_size=batch_size).sort([('infocode',-1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                ele['tmp_tit'] = [ord(c) for c in ele['title']];del ele['title']
                ele['tmp_columnType'] = [ord(c) for c in ele['columnType']];del ele['columnType']
                
            with open(path + f'/DG_{str(pn).zfill(5)}.txt', 'w',encoding='utf8') as out:
                json.dump(js,out,ensure_ascii=False,indent=4)
                print(f'DG_{str(pn).zfill(5)}.txt')

def upload(DB=MDB.col_DG , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\DG_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                convert_tmp2ori('tmp_tit','title',record) 
                convert_tmp2ori('tmp_columnType','columnType',record) 
                request.append(pymongo.UpdateOne({
                        "infoCode":record["infoCode"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)

def move(DB=MDB.col_DG,olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['HYPAPER']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "infoCode":1,
        "columnType":1,
        "industryCode":1,
        "industryName":1,
        "reportType":1,
        "stockCode":1,
        "stockName":1,
        "title":1,
    }
    cursor = olddb.find({}, delitem, batch_size=batch_size).sort([('infocode',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            ele['columnType'] = '行业研报'
            request.append(pymongo.UpdateOne(
                {"infoCode":ele['infoCode']},
                {"$set":ele},
                upsert=True
            ))
        
        if request: DB.bulk_write(request)
        print(pn)

def move_HGYJ(DB=MDB.col_DG,olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['CELVEBAOGAO']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "infoCode":1,
        "columnType":1,
        "industryCode":1,
        "industryName":1,
        "reportType":1,
        "stockCode":1,
        "stockName":1,
        "title":1,
    }
    cursor = olddb.find({}, delitem, batch_size=batch_size).sort([('infocode',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            # ele['columnType'] = '行业研报'
            request.append(pymongo.UpdateOne(
                {"infoCode":ele['infoCode']},
                {"$set":ele},
                upsert=True
            ))
        
        if request: DB.bulk_write(request)
        print(pn)

if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\EM_DG/', batch_size=10000)

    # upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\EM_DG")
    move_HGYJ()
