import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

# def yield_rows(cursor, chunk_size):
#     chunk = []
#     for i, row in enumerate(cursor):
#         if i % chunk_size == 0 and i > 0:
#             yield chunk; del chunk[:]
#         chunk.append(row)
#     yield chunk


def export(DB=MDB.col_QUATE, path="", DT="2020-01-01",batch_size=20000):
    cursor = DB.find({'DT': DT},{"_id":0,"M1":0,"FM1":0}, batch_size=batch_size).sort([('IDE',1),('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path +'/QUATE_'+str(DT)+'_'+str(pn).zfill(3)+".txt", 'w') as out:
                out.write(json.dumps(js))



def export_IDX(DB=MDB.col_QUATEIDX, path="", DT="2020-01-01",batch_size=20000):
    cursor = DB.find({'DT': DT},{"_id":0,"M1":0,"FM1":0}, batch_size=batch_size).sort([('IDE',1),('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path +'/QUATEIDX_'+str(DT)+'_'+str(pn).zfill(3)+".txt", 'w') as out:
                out.write(json.dumps(js))
                

def export_All(DB=MDB.col_QUATE, path="", DT="2020-01-01",batch_size=20000):
    cursor = DB.find({'DT': {'$gte':DT},'C':{'$ne':'-'},'C':{'$ne':''}},{"_id":0,"M1":0,"FM1":0}, batch_size=batch_size).sort([('IDE',1),('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path +'/QUATE_' + str(pn).zfill(5)+".txt", 'w') as out:
                out.write(json.dumps(js))
                print(f'/QUATE_{str(pn).zfill(5)}.txt')

def export_on_DT(DB=MDB.col_QUATE, path="", DT="2020-01-01", batch_size=20000): 
    cursor = MDB.col_QUATE.find({'DT': {'$eq':DT},'C':{'$exists':True},'C':{'$ne':"-"},'C':{'$ne':''}},{"_id":0,"M1":0,"FM1":0,"GN_BK_IDS":0,"DQ_BK_IDS":0,"HY_BK_IDS":0}, batch_size=batch_size).sort([('IDE',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path +'/QUATE_' + DT +'_'+ str(pn).zfill(5)+".txt", 'w') as out:
                out.write(json.dumps(js))
                # print(f'/QUATE_{str(pn).zfill(5)}.txt')               

def export_by_IDE(DB=MDB.col_QUATE, path="", IDE="0000012", batch_size=20000): 
    delitem = {
        "_id":0,
        "M1":0,
        "FM1":0,
        "GN_BK_IDS":0,
        "DQ_BK_IDS":0,
        "HY_BK_IDS":0,
        "Unnamed: 10":0,
        "Unnamed: 6":0,
        "Unnamed: 7":0,
        "Unnamed: 8":0,
        "Unnamed: 9":0,
    }
    cursor = DB.find({'IDE': {'$eq':IDE},'C':{'$exists':True},'C':{'$ne':"-"},'C':{'$ne':''}},delitem, batch_size=batch_size).sort([('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            newjs = []
            for ele in js:
                if ele['DT'].find('-') > -1: newjs.append(ele)

            with open(path +'/QUATE_' + IDE +'_'+ str(pn).zfill(5)+".txt", 'w') as out:
                out.write(json.dumps(newjs,indent=4))
                print('QUATE_' + IDE +'_'+ str(pn).zfill(5)+".txt")


def upload_IDX(DB=MDB.col_QUATEIDX , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\QUATE_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                request.append(pymongo.UpdateOne({
                        "IDE":record["IDE"],
                        "DT":record["DT"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)

def upload(DB=MDB.col_QUATE, path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\QUATE_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                request.append(pymongo.UpdateOne({
                        "IDE":record["IDE"],
                        "DT":record["DT"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)




if __name__ == "__main__":
    import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # from datetime import datetime
    # from dateutil.relativedelta import relativedelta
    # today = datetime.now()
    # for i in range(365*40):
    #     DT = (today - relativedelta(days=i)).strftime('%Y-%m-%d')
    #     print(DT)
    #     export_on_DT(DT=DT,path=parser['export']['export_path']+'\QUATE/')

    # CA = [x for x in MDB.col_IDLIST.find({"TYPE":"stock"},{"IDE":1,"ctype":1,"_id":0}).sort([("IDE",1)])]
    # for stock in CA:
    #     export_by_IDE(IDE=stock["IDE"], path=parser['export']['export_path']+'\QUATE/')

    # CI = [x for x in MDB.col_IDLIST.find({"TYPE":"index"},{"IDE":1,"_id":0}).sort([("IDE",1)])]
    # for stock in CI:    export_by_IDE(DB=MDB.col_QUATEIDX, IDE=stock["IDE"], path=parser['export']['export_path']+'\QUATE_IDX/')
        
    # HY = [x for x in MDB.col_IDLIST.find({"TYPE":"BK_HY"},{"IDE":1,"_id":0}).sort([("IDE",1)])]
    # for stock in HY:    export_by_IDE(DB=MDB.col_QUATEIDX, IDE=stock["IDE"], path=parser['export']['export_path']+'\QUATE_IDX/')

    # DQ = [x for x in MDB.col_IDLIST.find({"TYPE":"BK_DQ"},{"IDE":1,"_id":0}).sort([("IDE",1)])]
    # for stock in DQ:    export_by_IDE(DB=MDB.col_QUATEIDX, IDE=stock["IDE"], path=parser['export']['export_path']+'\QUATE_IDX/')

    # GN = [x for x in MDB.col_IDLIST.find({"TYPE":"BK_GN"},{"IDE":1,"_id":0}).sort([("IDE",1)])]
    # for stock in GN:    export_by_IDE(DB=MDB.col_QUATEIDX, IDE=stock["IDE"], path=parser['export']['export_path']+'\QUATE_IDX/')




    # upload_IDX(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\QUATE_IDX")
    upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\QUATE")








