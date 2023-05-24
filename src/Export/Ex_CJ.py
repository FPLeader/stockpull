import sys,json
sys.path.insert(0,'..')
import MDB
from Export.yield_rows import yield_rows

def export(DB=MDB.col_CJ , path="", batch_size=1000):
    cursor = DB.find({},{"_id":0}, batch_size=batch_size).sort([('_id',-1)])
    chunk = yield_rows(cursor, batch_size)
    for pn, js in enumerate(chunk):
        if js:
            for ele in js:
                ele['tmp_type'] = [ord(c) for c in ele['type']];del ele['type']
                ele['tmp_ur'] = [ord(c) for c in ele['url']];del ele['url']
                ele['tmp_info'] = [ord(c) for c in ele['info']];del ele['info']
                ele['tmp_tit'] = [ord(c) for c in ele['title']];del ele['title']
                del ele['time']
            with open(path + f'/CJ_{str(pn).zfill(5)}.txt', 'w') as out:
                out.write(json.dumps(js))
                print(f'CJ_{str(pn).zfill(5)}.txt')

def upload(DB=MDB.col_CJ , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    for nf in glob.glob(path + '\CJ_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                convert_tmp2ori('tmp_type','type',record)
                convert_tmp2ori('tmp_ur','url',record)
                convert_tmp2ori('tmp_info','info',record)
                convert_tmp2ori('tmp_tit','title',record)
                request.append(pymongo.UpdateOne({
                        "info":record["info"],
                        "type":record["type"]
                    },
                    {"$set":record},upsert=True)
                )
            if request: DB.bulk_write(request)
                
if __name__ == "__main__":
    # # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\EM_CJ/', batch_size=3000)

    upload(path=r"E:\01 System Program Data\MDB27018\EM_CJ")