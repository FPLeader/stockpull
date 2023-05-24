import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_CW_FH, path="", DT="2020-01-01",batch_size=20000):
    cursor = DB.find({'NOTICEDATE': {'$gt':DT}},{"_id":0}, batch_size=batch_size).sort([('NOTICEDATE',-1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path + f'/FH_{str(pn).zfill(3)}.txt', 'w') as out:
                out.write(json.dumps(js,indent=4))


def upload(DB=MDB.col_CW_FH , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    for nf in glob.glob(path + '\FH_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                request.append(pymongo.UpdateOne({
                        "RowNum":record["RowNum"],
                        # "type":record["type"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)
                

if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\CW_FH/', DT="1990-01-01",batch_size=100000)

    upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\CW_FH")













