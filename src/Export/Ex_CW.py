import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_CW2, path="", DT="2020-01-01", batch_size=1000):
    cursor = DB.find({'REPORTDATE': {'$gte':DT}},{"_id":0}, batch_size=batch_size).sort([('IDE',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path + f'/CW_{str(pn).zfill(3)}.txt', 'w') as out:
                out.write(json.dumps(js,indent=4))
                print(f'/CW_{str(pn).zfill(3)}.txt')

def upload(DB=MDB.col_CW2 , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    for nf in glob.glob(path + '\CW_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                request.append(pymongo.UpdateOne({"IDE":record["IDE"],"REPORTDATE":record["REPORTDATE"]},{"$set":record},upsert=True))
            if request: DB.bulk_write(request)

if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # batch_size = 1000
    # CA = [x for x in MDB.col_IDLIST.find({"TYPE":"stock"},{"IDE":1,"ctype":1,"_id":0}).sort([("IDE",1)])]
    # for stock in CA:
    #     IDE = stock['IDE']
    #     cursor = MDB.col_CW2.find({'IDE': IDE},{"_id":0}, batch_size=batch_size).sort([('IDE',1)])
    #     chunk = yield_rows(cursor, batch_size)
    #     for pn, js in enumerate(chunk):
    #         if js:
    #             with open(parser['export']['export_path'] + f'/CW2/CW_{IDE}_{str(pn).zfill(3)}.txt', 'w') as out:
    #                 out.write(json.dumps(js,indent=4))
    #     print(IDE)

    upload(path=r"E:\01 System Program Data\MDB27018\CW2")