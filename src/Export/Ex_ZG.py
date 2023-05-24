import sys,json
sys.path.insert(0,'..')
import MDB,pymongo
from Export.yield_rows import yield_rows

def export(DB=MDB.col_NOTICE , path="", DT="2020-01-01",batch_size=1000):
    # print(DT.replace('-',''))
    cursor = DB.find({'code': {'$gte':DT.replace('-','')}},{"_id":0,"IDE":1,"infoCode":1,'title':1,'summary':1,'content':1}, batch_size=batch_size).sort([('IDE',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                # del ele['uniqueUrl'],ele['url'],ele['publishDate'],ele['code'],ele['showDateTime'],ele['updateTime'],ele['recordId'],ele['sRatingName'],ele['source']
                ele['s'] = [ord(c) for c in ele['summary']];del ele['summary']
                ele['t'] = [ord(c) for c in ele['title']];del ele['title']
                if 'content' in ele:
                    ele['c'] = [ord(c) for c in ele['content']];del ele['content']
            with open(path + f'/ZX_{str(pn).zfill(3)}.txt', 'w') as out:
                # out.write(json.dumps(js,indent=4))
                out.write(json.dumps(js))
                print(f'/ZX_{str(pn).zfill(3)}.txt')

def export_GG(DB=MDB.col_NOTICE , path="", DT="2020-01-01", batch_size=1000):
    # print(DT.replace('-',''))
    cursor = DB.find({'infoCode': {'$gte':'AN'+ DT.replace('-','')}},{"_id":0,"IDE":1,"infoCode":1,'title':1,'content':1}, batch_size=batch_size)
    # .sort([('IDE',1),('infoCode',-1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                # del ele['uniqueUrl'],ele['url']
                # ele['s'] = [ord(c) for c in ele['summary']];del ele['summary']
                ele['t'] = [ord(c) for c in ele['title']];del ele['title']
                if 'content' in ele:
                    try:
                        ele['c'] = [ord(c) for c in ele['content']];del ele['content']
                    except: pass
            with open(path + f'/GG_{str(pn).zfill(3)}.txt', 'w') as out:
                out.write(json.dumps(js))
                print(f'/GG_{str(pn).zfill(3)}.txt')

def upload(DB=MDB.col_NOTICE , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\ZX_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                record['type'] = "资讯"
                convert_tmp2ori('t','title',record)
                # convert_ori('content',record)
                convert_tmp2ori('s','summary',record)
                convert_tmp2ori('c','content',record)  
                request.append(pymongo.UpdateOne({
                        "IDE":record["IDE"],
                        "infoCode":record["infoCode"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)

def upload_GG(DB=MDB.col_NOTICE , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\GG_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                record['type'] = "公告"
                convert_tmp2ori('t','title',record)
                # convert_ori('content',record)
                convert_tmp2ori('s','summary',record)
                convert_tmp2ori('c','content',record)  
                request.append(pymongo.UpdateOne({
                        "IDE":record["IDE"],
                        "infoCode":record["infoCode"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)


def check_content(DB=MDB.col_NOTICE):
    batch_size = 1000
    cursor = DB.find({"type":"资讯"}, {},batch_size=batch_size)
    chunk = yield_rows(cursor, batch_size)
    request = []
    for pn, js in enumerate(chunk):
        print(pn)
        if js:
            for record in js:
                if 'content' in record:
                    request.append(pymongo.UpdateOne({"_id":record["_id"]},
                    {"$set":{"txt":True}},upsert=True)
                )
            if request: DB.bulk_write(request)

if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # # export(path=parser['export']['export_path']+'\EM_ZIX/',DT="1990-01-01", batch_size=2000)
    # export_GG(path=parser['export']['export_path']+'\EM_GON/',DT="1990-01-01", batch_size=50000)

    # upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\EM_ZIX")
    # upload_GG(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\EM_GON")
    check_content()