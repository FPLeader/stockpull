import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB, datetime
import CW.url
import configparser; parser = configparser.ConfigParser(); parser.read('../config.ini')
dates = parser['download']['CaiWu_Dates']

def do(js):
    CW.url.fetch_push(js["IDE"],js['ctype'],dates)

if __name__ == "__main__":
    


    print(">> CaiWu -> LIST")
    dates = parser['download']['CaiWu_Dates']
    print("CaiWu_Dates",dates)


    
    dllist = []
    import tqdm, multiprocessing
    multiprocessing.freeze_support(); pool = multiprocessing.Pool(processes=20)  
    import IDELIST.url

    CA = [x for x in MDB.col_IDLIST.find({"TYPE":"stock"},{"IDE":1,"ctype":1,"_id":0}).sort([("IDE",1)])]
    for _ in tqdm.tqdm(pool.imap_unordered(IDELIST.url.url_CW_html,  CA),  total=len(CA)): pass
