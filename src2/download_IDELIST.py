import sys, requests, math
import os
import json

def url_CALIST(fs='m:0 t:6,m:0 t:13,m:0 t:80,m:1 t:2,m:1 t:23',fields='f12,f13,f14',pn=1,pz=100):
    siteURL = 'http://push2.eastmoney.com/api/qt/clist/get'
    para = {'pn': pn,'pz': pz,'fs': fs,'fields': fields,'po': 0, 'np': 1,'fltt': 2,'invt': 2,'fid': 'f12'}
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def url_CILIST(fields = 'f12,f13,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:0 t:5,m:1 s:2',fields = fields, pz=pz, pn=pn)

def url_DQLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:1',fields = fields, pz=pz, pn=pn)

def url_GNLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:3',fields = fields, pz=pz, pn=pn)

def url_HYLIST(fields = 'f12,f14',pz=10000,pn=1): 
    return url_CALIST(fs = 'm:90+t:2',fields = fields, pz=pz, pn=pn)

def fetch_idelist(url,TYPE): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=1000
    )
    js = json.loads(r.content)
    try:
        pages = math.ceil(int(js['data']['total'])/100)+1
    except:
        return 0
    js = js['data']['diff']
    request = []
    for ele in js:
        if 'f13' in ele:
            IDE = ele['f12']+{1:'1', 0:'2'}.get(ele['f13'], '')
            js = {"TYPE":TYPE, "IDE": IDE, "IDS":ele['f14'],   "ID6":ele['f12'],   "SEC":ele['f13']}
        else:
            IDE = ele['f12']
            js = {"TYPE":TYPE,  "IDE": IDE,  "IDS":ele['f14'],   "ID6":ele['f12']}
        request.append(js)
    # if request: MDB.col_IDLIST.bulk_write(request)
    return request

def url_LIST(fs='b:MK0300',fields='f12,f13,f14',pn=1,pz=100):
    em_forex_siteURL = 'http://push2.eastmoney.com/api/qt/clist/get'
    para = {'pn': pn,'pz': pz,'fs': fs,'fields': fields,'po': 0, 'np': 1,'fltt': 2,'invt': 2,'fid': 'f3'}
    return requests.Request('GET', url=em_forex_siteURL, params=para).prepare().url

def em_forex_fetch(url,TYPE): 
    r = requests.get(url, 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=1000
    )
    js = json.loads(r.content)
    try:
        pages = math.ceil(int(js['data']['total'])/100)+1
    except:
        return 0
    js = js['data']['diff']
    request = []
    for ele in js:
        if 'f13' in ele:
            IDE = ele['f12']+{1:'1', 0:'2'}.get(ele['f13'], '')
            js = {"TYPE":TYPE,   "IDE":IDE, "IDS":ele['f14'],   "ID6":ele['f12'],   "SEC":ele['f13']}
        else:
            IDE = ele['f12']
            js = {"TYPE":TYPE,   "IDE":IDE,  "IDS":ele['f14'],   "ID6":ele['f12']}
        # print(js)
        request.append(js)
    # if request: MDB.col_IDLIST.bulk_write(request)fetch_idelist
    return request


def make_idelist_text_file(filename, data):
    print("make text file for " + filename + "...")
    text = json.dumps(data)
    script_dir = os.path.dirname(__file__)
    rel_path = "../download/IDELIST/" + filename + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, "w") as f:
        f.write(text)
    print(filename + " file was made successfully!")

def main_IDELIST():
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/IDELIST"):
        os.mkdir(os.path.dirname(__file__) + "/../download/IDELIST")
    
    # stock list
    print("load stock list from Internet...")
    response = fetch_idelist(url_CALIST(pn=1, pz=10000), "stock")
    print("downloaded stock list!")
    make_idelist_text_file("CALIST", response)

    #index list
    print("load index list from Internet...")
    response = fetch_idelist(url_CILIST(pn=1, pz=10000), "index")
    print("downloaded index list!")
    make_idelist_text_file("CILIST", response)

    #HY list
    print("load HY list from Internet...")
    response = fetch_idelist(url_HYLIST(pn=1, pz=10000), "BK_HY")
    print("downloaded HY list!")
    make_idelist_text_file("HYLIST", response)

    # DQ list
    print("load DQ list from Internet...")
    response = fetch_idelist(url_DQLIST(pn=1, pz=10000), "BK_DQ")
    print("downloaded DQ list!")
    make_idelist_text_file("DQLIST", response)

    #GN list
    print("load GN list from Internet...")
    response = fetch_idelist(url_GNLIST(pn=1, pz=10000), "BK_GN")
    print("downloaded GN list!")
    make_idelist_text_file("GNLIST", response)

    #FOREXLIST
    print("load Forex list from Internet...")
    response = em_forex_fetch(url_LIST(pn=1, pz=1000), "forex")
    print("downloaded Forex list!")
    make_idelist_text_file("FOREXLIST", response)

if __name__ == "__main__":
    main_IDELIST()