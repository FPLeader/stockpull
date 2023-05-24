import sys,requests,json,pymongo,os,math
sys.path.insert(0,'..')
import EM_TOOL, MDB

def url_全部(pn=1,pz=100,ann_type="A",f_node=1,s_node=1):
    siteURL = 'https://np-anotice-stock.eastmoney.com/api/security/ann'
    para = {
        'sr':-1,
        'page_size': pz,
        'page_index': pn,
        'client_source': 'web',
        'ann_type': ann_type,
        'f_node': f_node,
        's_node': s_node,
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def url_财务报告(pn=1,pz=100,ann_type="A",ID6='000001',f_node=1,s_node=1):
    siteURL = 'https://np-anotice-stock.eastmoney.com/api/security/ann'
    para = {
        'sr':-1,
        'page_size': pz,
        'page_index': pn,
        'client_source': 'web',
        'ann_type': ann_type,
        'f_node': f_node,
        's_node': s_node,
        'stock_list': ID6
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

print(url_财务报告())

def parseContent(content):
    try:
        js = json.loads(content)
        data = js['data']
    except:
        return True
    request = []
    for item in data['list']:
        request.append(pymongo.UpdateOne(
            {"art_code":item['art_code']},
            {"$set":item},
            upsert=True
        ))
    if request: MDB.col_REPORT.bulk_write(request)
    return math.ceil(data['total_hits']/data['page_size'])


# for pn in range(500):
#     u = url_全部(pn=pn+1,pz=100,ann_type="A",f_node=1,s_node=1)
#     print(pn)
#     print(u)
#     try:
#         r = requests.get(u, timeout=10,
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
#                                 'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
#             }
#         )
#         r.encoding = 'utf8'
#     except: pass
#     parseContent(r.content)
