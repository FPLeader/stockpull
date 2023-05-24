import sys,json
sys.path.insert(0,'..')

root = r"E:/01 System Program Data/MongoDB_Repo_New/PDF"

import glob, pymongo, MDB


def upload(DB=MDB.col_DG , path = root + "\\"):
    request = []
    matching_Count = 0
    for nf in glob.glob(path + '*.pdf'):
        infoCode = nf.replace(path,"").replace(".pdf","")
        print(infoCode)
        js = DB.find({'infoCode': infoCode},{"_id":1}).limit(1)
        for ele in js:
            request.append(pymongo.UpdateOne({"_id":ele["_id"]},{"$set":{"pdf":True}},upsert=True))
            matching_Count = matching_Count + 1
            print(matching_Count)
    if request: DB.bulk_write(request)

upload()