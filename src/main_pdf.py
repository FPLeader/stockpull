import sys
sys.tracebacklimit = 0

if __name__ == "__main__":
    import configparser; parser = configparser.ConfigParser(); parser.read('config.ini')

    print("path = ",parser['main_pdf']['path'])
    path = parser['main_pdf']['path']
    gtstr = 'AP'+ parser['main_pdf']['StartDate']
    print(gtstr)
    import tqdm, multiprocessing
    multiprocessing.freeze_support(); pool = multiprocessing.Pool(processes=20) 
    
    import MDB,EM_DG
    js= [x for x in MDB.col_DG.find({'columnType':'宏观研究', 'infoCode':{'$gt':gtstr} ,'dl':{'$ne':True}})]
    js = js + [x for x in MDB.col_DG.find({'columnType':'行业研报', 'infoCode':{'$gt':gtstr} ,'dl':{'$ne':True}}).sort([('infoCode',-1)])]
    # js = js + [x for x in MDB.col_DG.find({'columnType':'券商晨会', 'infoCode':{'$gt':gtstr} ,'dl':{'$ne':True}}).sort([('infoCode',-1)])]
    # js = js + [x for x in MDB.col_DG.find({'columnType':'个股研报', 'infoCode':{'$gt':gtstr} ,'dl':{'$ne':True}}).sort([('infoCode',-1)])]
    # js = js + [x for x in MDB.col_DG.find({'columnType':'策略报告', 'infoCode':{'$gt':gtstr} ,'dl':{'$ne':True}}).sort([('infoCode',-1)])]
    # print(js)
    for ele in js:  ele['path'] = path
    for _ in tqdm.tqdm(pool.imap_unordered(EM_DG.dl_H3, js), total=len(js)): pass


