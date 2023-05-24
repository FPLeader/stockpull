import sys,requests,json,pymongo,os
sys.path.insert(0,'..')
import EM_TOOL, MDB
from bs4 import BeautifulSoup


def get_url_for_report_dg(pn=1,pz=100,dt='2020-01-01'):
    siteURL = 'http://reportapi.eastmoney.com/report/dg'
    para = {
        'pageSize': pz,
        'pageNo': pn,
        'endTime': '2050-12-31',
        'beginTime': dt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url


def get_data_for_report_dg(pn=1,pz=100,dt='20200101'):
    r = requests.get(get_url_for_report_dg(pn=pn, pz=pz,dt=dt),
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        },
        timeout=20
    ).json()
    return r['TotalPage'], r['data']


def parseContent(content):
    try:
        js = json.loads(content)
        data = js['data']
    except:
        return True
    request = []
    for item in data:
        request.append(pymongo.UpdateOne(
            {"infoCode":item['infoCode']},
            {"$set":item},
            upsert=True
        ))
    if request: MDB.col_DG.bulk_write(request)
    return True


def dl_PDF(jsItem, url_fc='', path='', filename=''):
    os.makedirs(path, exist_ok = True)
    try:
        r = requests.get(url_fc,
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
            },
            timeout=20
        )
        fname = os.path.join(path,filename)
        with open(fname, 'wb') as fd:
            fd.write(r.content)
            MDB.col_DG.update_one({"infoCode":jsItem['infoCode']},{"$set":{"dl":True}},)
        return True
    except:
        return False


def dl_H3(jsItem):
    # file_path = os.path.join(jsItem['path'],
    #                          jsItem['infoCode'].replace('AP','')[0:8],
    #                          jsItem['columnType'], 
    #                          jsItem['infoCode']+'.pdf')
    # print(file_path if os.path.isfile(file_path) else '')
    # return
    dl_PDF(
        jsItem,
        url_fc= 'http://pdf.dfcfw.com/pdf/H3_'+jsItem['infoCode']+'_1.pdf',
        path = os.path.join(jsItem['path'],jsItem['infoCode'].replace('AP','')[0:8]+'/'+jsItem['columnType']),
        filename = jsItem['infoCode']+'.pdf'
    )


