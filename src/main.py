import configparser
import getopt
import multiprocessing
import requests
import sys
import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
import CW.url
import EM_TOOL
import MDB
import pymongo
import IDELIST.url
import math
import EM_FOREX.url
import DK.url
import FenHong.url
import MK.url
import CW.url
import EM_NES.url
import IDELIST.url
import FenHong.url
import DK.url
import MK.url
import CW.url
import EM_NES.url
import EM_YaoWen
import EM_DG

# setting----------------------------------------------------------------------
sys.tracebacklimit = 0
n_config = EM_TOOL.Configuration(**{'process_count': 20, 'restart': False})

try:
    opts, root = getopt.getopt(sys.argv[1:], "p:r", ["process_count=", "help"])
except getopt.GetoptError:
    err = sys.exc_info()[1]
    sys.exit("usage error: %s" % err)
for k, v in opts:
    if k in "-p"  "--process_count":
        n_config.process_count = int(v)
    elif k in "-r":
        n_config.restart = True
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  


col_DL = MDB.col_DL


def fetch(jsItem):
    try:
        re = False
        if jsItem['url'][0:4] == "pass":
            if jsItem['module'] == 'CW':
                re = CW.url.fetch_push(jsItem['IDE'], jsItem['ctype'], jsItem['dates'])
                if re:
                    col_DL.remove({'url': jsItem['url']})
                return False
        try:
            r = requests.get(
                jsItem['url'],
                timeout=10,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                                  'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
                })
            r.encoding = 'utf8'
            if r.status_code != 200:
                return False
        except Exception as e:
            print(e)
            return False
        if jsItem['module'] == 'TODAY':
            re = IDELIST.url.parseContent_Detail(r.content)
        elif jsItem['module'] == 'TODAY_IDX':
            re = IDELIST.url.parseContent_Detail_IDX(r.content)
        elif jsItem['module'] == 'TODAY_FOREX':
            re = EM_FOREX.url.parseContent_Detail(r.content)
        elif jsItem['module'] == 'FH':
            re = FenHong.url.parser(r.content)
        elif jsItem['module'] == 'DK_LINE':
            re = DK.url.parser(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'DK_FLOW':
            re = DK.url.parser_FLOW(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'DK_LINE_IDX':
            re = DK.url.parser_IDX(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'DK_FLOW_IDX':
            re = DK.url.parser_IDX_FLOW(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'DK_FOREX':
            re = EM_FOREX.url.parser_DK(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'MK_LINE':
            re = MK.url.parser(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'MK_LINE_IDX':
            re = MK.url.parser_IDX(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'MK_FLOW':
            re = MK.url.parser_FLOW(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'MK_FLOW_IDX':
            re = MK.url.parser_IDX_FLOW(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'CW':
            re = CW.url.parseContent_zcfzb2(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'EM_NES':
            re = EM_NES.url.parser(r.content, jsItem['IDE'])
        elif jsItem['module'] == 'EM_YaoWen':
            re = EM_YaoWen.parser(r.content, jsItem['type'])
        elif jsItem['module'] == 'EM_DG':
            re = EM_DG.parseContent(r.content)
        if re:
            col_DL.remove({'url': jsItem['url']})
    except Exception as e:
        print(e)
        return False


def download_ide_lists():
    all_pages = IDELIST.url.fetch_push_init(IDELIST.url.url_CALIST(pn=1, pz=10000), "stock")
    print("load index list from Internet...")
    all_pages += IDELIST.url.fetch_push_init(IDELIST.url.url_CILIST(pn=1, pz=10000), "index")
    print("load HY list from Internet...")
    all_pages += IDELIST.url.fetch_push_init(IDELIST.url.url_HYLIST(pn=1, pz=10000), "BK_HY")
    print("load DQ list from Internet...")
    all_pages += IDELIST.url.fetch_push_init(IDELIST.url.url_DQLIST(pn=1, pz=10000), "BK_DQ")
    print("load GN list from Internet...")
    all_pages += IDELIST.url.fetch_push_init(IDELIST.url.url_GNLIST(pn=1, pz=10000), "BK_GN")
    print("load Forex list from Internet...")
    all_pages += EM_FOREX.url.fetch_push_init(EM_FOREX.url.url_LIST(pn=1, pz=1000), "forex")
    print("All of list has pushed!")
    return all_pages


def download_stock_data(dl_list, stock, module_normal='DK_LINE', module_flow='DK_FLOW'):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    fetch_param = {'url': DK.url.url_DK(ide=stock['IDE'], lmt=int(parser['download']['FETCH_LIMIT'])), 'module': module_normal, 'IDE': stock['IDE']}
    dl_list.append(pymongo.UpdateOne({'url': fetch_param['url']}, {"$set": fetch_param}, upsert=True))
    fetch_param = {'url': DK.url.url_FLOW(ide=stock['IDE'], lmt=int(parser['download']['FETCH_LIMIT'])), 'module': module_flow, 'IDE': stock['IDE']}
    dl_list.append(pymongo.UpdateOne({'url': fetch_param['url']}, {"$set": fetch_param}, upsert=True))
    return dl_list

if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    # _________prepare__________________
    CA = [x for x in MDB.col_IDLIST.find({"TYPE": "stock"}, {"IDE": 1, "ctype": 1, "_id": 0}).sort([("IDE", 1)])]
    CI = [x for x in MDB.col_IDLIST.find({"TYPE": "index"}, {"IDE": 1, "_id": 0}).sort([("IDE", 1)])]
    HY = [x for x in MDB.col_IDLIST.find({"TYPE": "BK_HY"}, {"IDE": 1, "_id": 0}).sort([("IDE", 1)])]
    DQ = [x for x in MDB.col_IDLIST.find({"TYPE": "BK_DQ"}, {"IDE": 1, "_id": 0}).sort([("IDE", 1)])]
    GN = [x for x in MDB.col_IDLIST.find({"TYPE": "BK_GN"}, {"IDE": 1, "_id": 0}).sort([("IDE", 1)])]
    FX = [x for x in MDB.col_IDLIST.find({"TYPE": "forex"}, {"IDE": 1, "_id": 0}).sort([("IDE", 1)])]
    # print(CA)
    # __________PHASE 0___________________
    if parser['download']['IDELIST'] == 'yes':
        download_ide_lists()
    # __________LIST___________________

    if n_config.restart:
        MDB.col_DL.delete_many({})
        download_list = []
        if parser['download']['Today'] == 'yes':
            print(">> Today -> LIST")
            pz = 100
            for page in range(math.ceil(len(CA) / 100) + 1):
                one = {'url': IDELIST.url.url_CALIST(fields=IDELIST.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for page in range(math.ceil(len(CI) / 100) + 1):
                one = {'url': IDELIST.url.url_CILIST(fields=IDELIST.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY_IDX'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for page in range(math.ceil(len(HY) / 100) + 1):
                one = {'url': IDELIST.url.url_HYLIST(fields=IDELIST.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY_IDX'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for page in range(math.ceil(len(DQ) / 100) + 1):
                one = {'url': IDELIST.url.url_DQLIST(fields=IDELIST.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY_IDX'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for page in range(math.ceil(len(GN) / 100) + 1):
                one = {'url': IDELIST.url.url_GNLIST(fields=IDELIST.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY_IDX'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))

            for page in range(math.ceil(len(FX) / 100) + 1):
                one = {'url': EM_FOREX.url.url_LIST(fields=EM_FOREX.url.allfields, pz=100, pn=page + 1),
                       'module': 'TODAY_FOREX'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            one = {'url': EM_FOREX.url.url_LIST_RMB(fields=EM_FOREX.url.allfields, pz=1000, pn=1),
                   'module': 'TODAY_FOREX'}
            download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['FenHong'] == 'yes':
            print(">> FenHong -> LIST")
            one_yrs_ago = datetime.now() - relativedelta(years=10)
            last_date_limit = one_yrs_ago.strftime('%Y-%m-%d')
            pns = [x + 2 for x in range(10)]
            for pn in pns:
                one = {'url': FenHong.url.url_FenHong(CQCXR=last_date_limit, pn=pn, pz=100), 'module': 'FH'}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['DK'] == 'yes':
            print(">> DK OCHLVAT -> LIST")            
            for index in CI:
                download_list = download_stock_data(download_list, index, module_normal='DK_LINE_IDX',
                                                    module_flow='DK_FLOW_IDX')
            for index in HY:
                download_list = download_stock_data(download_list, index, module_normal='DK_LINE_IDX',
                                                    module_flow='DK_FLOW_IDX')
            for index in DQ:
                download_list = download_stock_data(download_list, index, module_normal='DK_LINE_IDX',
                                                    module_flow='DK_FLOW_IDX')
            for index in GN:
                download_list = download_stock_data(download_list, index, module_normal='DK_LINE_IDX',
                                                    module_flow='DK_FLOW_IDX')
            for index in FX:
                one = {'url': EM_FOREX.url.url_DK(id6=index['IDE'], sec=index['SEC'] if 'SEC' in index else '', lmt=int(parser['download']['FETCH_LIMIT'])),
                       'module': 'DK_FOREX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for index in CA:
                download_list = download_stock_data(download_list, index)
        if parser['download']['MK'] == 'yes':
            print(">> MK OCHLVAT -> LIST")
            for index in CA:
                one = {'url': MK.url.url_DK(ide=index['IDE']), 'module': 'MK_LINE', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {'url': MK.url.url_FLOW(ide=index['IDE']), 'module': 'MK_FLOW', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for index in CI:
                one = {'url': MK.url.url_DK(ide=index['IDE']), 'module': 'MK_LINE_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {'url': MK.url.url_FLOW(ide=index['IDE']), 'module': 'MK_FLOW_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for index in HY:
                one = {'url': MK.url.url_DK(ide=index['IDE']), 'module': 'MK_LINE_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {'url': MK.url.url_FLOW(ide=index['IDE']), 'module': 'MK_FLOW_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for index in DQ:
                one = {'url': MK.url.url_DK(ide=index['IDE']), 'module': 'MK_LINE_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {'url': MK.url.url_FLOW(ide=index['IDE']), 'module': 'MK_FLOW_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
            for index in GN:
                one = {'url': MK.url.url_DK(ide=index['IDE']), 'module': 'MK_LINE_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {'url': MK.url.url_FLOW(ide=index['IDE']), 'module': 'MK_FLOW_IDX', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['CaiWu'] == 'yes':
            print(">> CaiWu -> LIST")
            dates = parser['download']['CaiWu_Dates']
            for index in CA:
                one = {
                    'url': CW.url.url_ZCFZB2(
                        ide=index['IDE'],
                        companyType=index['ctype'] if 'ctype' in index else '',
                        dates=dates),
                    'module': 'CW',
                    'IDE': index['IDE'],
                    'ctype': index['ctype'] if 'ctype' in index else '',
                    'dates': dates
                }
                download_list.append(pymongo.UpdateOne(
                    {'url': one['url']},
                    {"$set": one},
                    upsert=True))
                one = {'url': CW.url.url_LRB2(ide=index['IDE'],
                                              companyType=index['ctype'] if 'ctype' in index else '',
                                              dates=dates),
                       'module': 'CW',
                       'IDE': index['IDE'],
                       'ctype': index['ctype'] if 'ctype' in index else '',
                       'dates': dates}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
                one = {
                    'url': CW.url.url_XJLLB2(ide=index['IDE'], companyType=index['ctype'] if 'ctype' in index else '',
                                             dates=dates), 'module': 'CW', 'IDE': index['IDE'],
                    'ctype': index['ctype'] if 'ctype' in index else '', 'dates': dates}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['EM_NES'] == 'yes':
            print(">> EM_NEWS -> LIST")
            for index in CA:
                one = {'url': EM_NES.url.url(ide=index['IDE']), 'module': 'EM_NES', 'IDE': index['IDE']}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['EM_YaoWen'] == 'yes':
            print(">> EM_YaoWen -> LIST")
            pages = 3000
            for i in range(pages):
                one = {'url': EM_YaoWen.url_zxjh(pn=i + 1), 'module': 'EM_YaoWen', 'type': "资讯精华"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))

                one = {'url': EM_YaoWen.url_cgnjj(pn=i + 1), 'module': 'EM_YaoWen', 'type': "国内经济"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))

                one = {'url': EM_YaoWen.url_cgjjj(pn=i + 1), 'module': 'EM_YaoWen', 'type': "国际经济"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))

                one = {'url': EM_YaoWen.url_czqyw(pn=i + 1), 'module': 'EM_YaoWen', 'type': "证券聚焦"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))

                one = {'url': EM_YaoWen.url_cgsxw(pn=i + 1), 'module': 'EM_YaoWen', 'type': "公司资讯"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if parser['download']['EM_Report'] == 'yes':
            print(">> EM_Report -> LIST")
            ten_days_ago = datetime.now() - relativedelta(days=int(parser['download']['REPORT_DAYS_AGO']))
            last_date_limit = ten_days_ago.strftime('%Y-%m-%d')
            tp, all_data = EM_DG.get_data_for_report_dg(dt=last_date_limit)
            print(f'Total Pages: {tp} {last_date_limit} ')
            for i in range(1, tp + 1):
                one = {'url': EM_DG.get_url_for_report_dg(pn=i, pz=100, dt=last_date_limit), "module": "EM_DG"}
                download_list.append(pymongo.UpdateOne({'url': one['url']}, {"$set": one}, upsert=True))
        if download_list:
            MDB.col_DL.bulk_write(download_list)
    # __________DownLoad All___________________
    multiprocessing.freeze_support()
    pool = multiprocessing.Pool(processes=n_config.process_count)
    for x in range(5):
        js = [x for x in MDB.col_DL.find({})]
        for _ in tqdm.tqdm(pool.imap_unordered(fetch, js), total=len(js)):
            pass
