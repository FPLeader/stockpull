import sys,json
sys.path.insert(0,'..')
import MDB, pymongo
from Export.yield_rows import yield_rows

def move_PAPER_DG(DB=MDB.col_DG,olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['PAPER']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "infoCode":1,
        "sRatingName":1,
        "columnType":1,
        "industryCode":1,
        "industryName":1,
        "reportType":1,
        "stockCode":1,
        "stockName":1,
        "title":1,
        "newIssuePrice": 1,
        "newPeIssueA": 1,
        "predictLastYearEps": 1,
        "predictLastYearPe": 1,
        "predictNextTwoYearEps": 1,
        "predictNextTwoYearPe": 1,
        "predictNextYearEps": 1,
        "predictNextYearPe": 1,
        "predictThisYearEps": 1,
        "predictThisYearPe": 1,
    }
    cursor = olddb.find({}, delitem, batch_size=batch_size).sort([('infocode',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            ele['columnType'] = '个股研报'
            request.append(pymongo.UpdateOne(
                {"infoCode":ele['infoCode']},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)


def move_GG_NOTICE(DB=MDB.col_NOTICE, olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['NOTICE_GONGGAO']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "IDE":1,
        "infoCode":1,
        "title":1,
    }
    cursor = olddb.find({}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            ele['type'] = '公告'
            request.append(pymongo.UpdateOne(
                {"IDE":ele['IDE'],"infoCode":ele['infoCode']},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)


def move_ZI_NOTICE(DB=MDB.col_NOTICE, olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['NOTICE_ZIXUN']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "IDE":1,
        "infoCode":1,
        "title":1,
        "content":1,
        "summary":1,
        "uniqueUrl":1
    }
    cursor = olddb.find({}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            ele['type'] = '资讯'
            request.append(pymongo.UpdateOne(
                {"IDE":ele['IDE'],"infoCode":ele['infoCode']},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)

def move_NWDFGD_CJ(DB=MDB.col_CJ, olddb=pymongo.MongoClient("mongodb://localhost:27017/")['StockDB']['NW_DFGD']):
    batch_size = 1000
    delitem = {
        "_id":0,
        "title":1,
        # "content":1,
        # "summary":1,
        "url":1
    }
    cursor = olddb.find({"Type":"GNJJ"}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            # ele['type'] = '国内经济'
            # ele['info'] = ele['content'];del ele['content']
            request.append(pymongo.UpdateOne(
                {"url":ele['url'],"type":'国内经济'},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)

    cursor = olddb.find({"Type":"GJJJ"}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            # ele['type'] = '国际经济'
            # ele['info'] = ele['content'];del ele['content']
            request.append(pymongo.UpdateOne(
                {"url":ele['url'],"type":'国际经济'},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)        

    cursor = olddb.find({"Type":"ZQYW"}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            # ele['info'] = ele['content'];del ele['content']
            request.append(pymongo.UpdateOne(
                {"url":ele['url'],"type":'证券聚焦'},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)   

    cursor = olddb.find({"Type":"DFGD"}, delitem, batch_size=batch_size).sort([('_id',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        request = []
        for ele in js:
            # ele['info'] = ele['content'];del ele['content']
            request.append(pymongo.UpdateOne(
                {"url":ele['url'],"type":'资讯精华'},
                {"$set":ele},
                upsert=True
            ))
        if request: DB.bulk_write(request)
        print(pn)  


if __name__ == "__main__":
    move_NWDFGD_CJ()
