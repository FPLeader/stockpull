import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_FOREX , path="", DT="2020-01-01",batch_size=1000):
    cursor = DB.find({'DT': {'$gte':DT}},{"_id":0}, batch_size=batch_size).sort([('IDE',1),('DT',1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                ele['IDE'] = [ord(c) for c in ele['IDE']]
            with open(path + f'/QUATEFOR_{str(pn).zfill(5)}.txt', 'w') as out:
                # out.write(json.dumps(js,indent=4))
                out.write(json.dumps(js))
                print(f'/ZX_{str(pn).zfill(5)}.txt')


def upload(DB=MDB.col_FOREX , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\QUATEFOR_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                convert_ori('IDE',record) 
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
    # export(path=parser['export']['export_path']+'\QUATEFOR/',DT="1990-01-01", batch_size=500000)

    upload(path=r"F:\Source\StockStydy_2021_12_20\MDB27018\QUATEFOR")
