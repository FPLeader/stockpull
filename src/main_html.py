import sys
sys.tracebacklimit = 0
import MDB
import EM_NES.url
import configparser
import tqdm, multiprocessing


if __name__ == "__main__":
    parser = configparser.ConfigParser(); parser.read('config.ini')
    print("path = ",parser['main_pdf']['path'])
    path = parser['main_pdf']['path']
    gtstr = 'NW'+ parser['main_pdf']['StartDate']
    print(gtstr)
    multiprocessing.freeze_support(); pool = multiprocessing.Pool(processes=20) 
    js= [x for x in MDB.col_NOTICE_ZIXUN.find({'infoCode':{'$gt':gtstr},'content':{'$exists':False}}).limit(0)]
    for _ in tqdm.tqdm(pool.imap_unordered(EM_NES.url.down_ZIXUN, js), total=len(js)): pass
