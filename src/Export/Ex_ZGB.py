import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_ZGB, path="",batch_size=10000):
    cursor = DB.find({},{"_id":0}, batch_size=batch_size).sort([('IDE',1),('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            with open(path + f'/ZGB_{str(pn).zfill(3)}.txt', 'w') as out:
                out.write(json.dumps(js))
                print(f'/ZGB_{str(pn).zfill(3)}.txt')

def upload(DB=MDB.col_ZGB , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    for nf in glob.glob(path + '\ZGB_*.txt'):
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
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\CW_ZGB/',batch_size=100000)

    upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\CW_ZGB")
