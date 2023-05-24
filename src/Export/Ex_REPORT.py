import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_REPORT , path="", batch_size=1000):
    cursor = DB.find({},{"_id":0,"art_code":1,"codes":1,"notice_date":1,"title":1}, batch_size=batch_size).sort([('_id',-1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                ele['tmp_tit'] = [ord(c) for c in ele['title']];del ele['title']
            with open(path + f'/REP_{str(pn).zfill(5)}.txt', 'w') as out:
                out.write(json.dumps(js))
                print(f'REP_{str(pn).zfill(5)}.txt')
                
def upload(DB=MDB.col_REPORT , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    def convert_ori(OriName,ele):
        if OriName in ele:  ele[OriName] = "".join([chr(c) for c in ele[OriName]])
    for nf in glob.glob(path + '\REP_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                convert_tmp2ori('tmp_tit','title',record) 
                record['stock_code'] = record['codes'][0]['stock_code']
                record['IDS'] = record['codes'][0]['short_name']
                del record['notice_date'],record['codes']
                request.append(pymongo.UpdateOne({
                        "art_code":record["art_code"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)




if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\EM_REP/', batch_size=50000)

    upload(path=r"F:\\Source\\StockStydy_2021_12_20\\MDB27018\\EM_REP")