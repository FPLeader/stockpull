import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB

def url(ide='0000012'):
    # siteURL = 'http://f10.eastmoney.com/NewsBulletin/NewsBulletinAjax'
    siteURL = 'http://emweb.eastmoney.com/PC_HSF10/NewsBulletin/PageAjax'
    para = {
        'code': EM_TOOL.IDE2SID(ide)
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def parser(content,ide):
    js = json.loads(content)
    xx = js['gszx']['data']['items']
    request = []
    for record in xx:
        # del record['url'], record['code'], record['recordId'], record['source'], record['updateTime']
        # del record['publishDate'], record['sRatingName']
        request.append(pymongo.UpdateOne(
            {"IDE":ide,"infoCode":record['infoCode']},
            {"$set":record},
            upsert=True 
        ))
    if request: MDB.col_NOTICE_ZIXUN.bulk_write(request)


    # gg = js['gsgg']['data']['items']
    gg = js['gsgg']
    request = []
    for record in gg:
        # del record['uniqueUrl'],record['url'], record['code'], record['recordId'], record['source']
        # del record['updateTime'],record['showDateTime'], record['sRatingName'], record['summary']
        request.append(pymongo.UpdateOne(
            {"IDE":ide,"infoCode":record['art_code']},
            {"$set":record},
            upsert=True 
        ))
    if request: MDB.col_NOTICE_GONGGAO.bulk_write(request)
    return True

def down_ZIXUN(item):
    from bs4 import BeautifulSoup
    try:
        try:
            r = requests.get(
                item['uniqueUrl'], 
                headers ={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
                },
                allow_redirects=False
            )
        except:
            r = requests.get(
                item['url'], 
                headers ={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
                },
                allow_redirects=True,
                timeout=1
            )            
        if (r.status_code == 200):
            ContentBody = BeautifulSoup(r.content, 'html.parser').find(id='ContentBody')   
            ps = []
            for p in ContentBody.find_all('p'): 
                ps.append(p.text)
            ps = '\n'.join(ps)
            # print(ps)
            MDB.col_NOTICE_ZIXUN.update_one({"IDE":item['IDE'],"infoCode":item['infoCode']},{"$set":{"content":ps}},)
    except:
        pass





def fetch_push(ide): 
    r = requests.get(url(ide=ide), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=10
    )
    return parser(r.content,ide)
# print(fetch_push("0000012"))