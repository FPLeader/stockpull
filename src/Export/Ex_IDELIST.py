import sys,json
sys.path.insert(0,'..')
import MDB,pymongo
from Export.yield_rows import yield_rows

# def export(DB=MDB.col_IDLIST , path="", batch_size=1000):
#     cursor = DB.find({},{"_id":0}, batch_size=batch_size).sort([('IDE',1)])
#     chunk = yield_rows(cursor, batch_size)
#     for pn, js in enumerate(chunk):
#         if js:
#             for ele in js:
#                 if 'IDS' in ele:    ele['tmp_IDS'] = [ord(c) for c in ele['IDS']];del ele['IDS']
#                 if 'GN_BK_IDS' in ele:    ele['tmp_GN_BK_IDS'] = [ord(c) for c in ele['GN_BK_IDS']];del ele['GN_BK_IDS']
#                 if 'DQ_BK_IDS' in ele:    ele['tmp_DQ_BK_IDS'] = [ord(c) for c in ele['DQ_BK_IDS']];del ele['DQ_BK_IDS']
#                 if 'HY_BK_IDS' in ele:    ele['tmp_HY_BK_IDS'] = [ord(c) for c in ele['HY_BK_IDS']];del ele['HY_BK_IDS']
                    
#             with open(path + f"/REP_{str(pn).zfill(5)}.txt", 'w',encoding='utf8') as out:
#                 json.dump(js,out,ensure_ascii=False,indent=4)
#                 print(f'REP_{str(pn).zfill(5)}.txt')

def upload(DB=MDB.col_IDLIST , path=""):
    import glob,pymongo
    def convert_tmp2ori(tmpName,OriName,ele):
        if tmpName in ele:  ele[OriName] = "".join([chr(c) for c in ele[tmpName]]);del ele[tmpName]
    for nf in glob.glob(path + '\REP_*.txt'):
        print(nf)
        with open(nf,"r", encoding="utf8") as json_file:
            js = json.load(json_file)
            request = []
            for record in js:
                convert_tmp2ori('tmp_IDS','IDS',record)
                convert_tmp2ori('tmp_GN_BK_IDS','GN_BK_IDS',record)
                convert_tmp2ori('tmp_DQ_BK_IDS','DQ_BK_IDS',record)
                convert_tmp2ori('tmp_HY_BK_IDS','HY_BK_IDS',record)
                request.append(pymongo.UpdateOne({"IDE":record["IDE"]},{"$set":record},upsert=True))
            if request: DB.bulk_write(request)
                


if __name__ == "__main__":
    # import tqdm
    # import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
    # export(path=parser['export']['export_path']+'\IDELIST/', batch_size=1000)


    upload(DB=MDB.col_IDLIST , path=r"E:\01 System Program Data\MDB27018\IDELIST")