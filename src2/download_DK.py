import configparser
import os
import tqdm, multiprocessing
import requests, json
import EM_TOOL

def fetch_dk_push(ide): 
    # r = requests.get(url_DK(ide=ide), 
    # print(url_FLOW(ide=ide))
    r = requests.get(url_FLOW(ide=ide), 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML  '
                            'like Gecko) Chrome/81.0.4044.138 Safari/537.36 Edg/81.0.416.77'
        }, 
        timeout=1000
    )
    js = json.loads(r.content)
    return js

def url_FLOW(ide='0000012', lmt=700, fields2='f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get'
    para = {
        'secid': EM_TOOL.IDE2SECID(ide),
        'ut': 'b2884a393a59ad64002292a3e90d46a5',
        'fields1': 'f0',
        'fields2': fields2,
        'klt': 101,
        'lmt': lmt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def url_DK(id6='CADUSD', sec="119",lmt=700, fields2='f51,f52,f53,f54,f55,f56,f57,f59,f61'):
    siteURL = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
    para = {
        'secid': str(sec) +'.'+str(id6),
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
        'fields1': 'f0',
        'fields2': fields2,
        'klt': 101,
        'fqt': 0,
        'end': 20500101,
        'lmt': lmt
    }
    return requests.Request('GET', url=siteURL, params=para).prepare().url

def make_dk_text_file(filename, data):
    # print("make text file for QUATE_" + filename + "...")
    text = json.dumps(data)
    script_dir = os.path.dirname(__file__)
    rel_path = "../download/QUATE/QUATE_" + filename + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, "w") as f:
        f.write(text)
    # print("QUATE_" + filename + " file was made successfully!")

def download_stock_data(stock):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    response = fetch_dk_push(stock["IDE"])
    make_dk_text_file(stock["IDE"], response)

def handle_dk_list_file(filename):
    with open ("../download/IDELIST/" + filename + ".txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    multiprocessing.freeze_support();
    pool = multiprocessing.Pool(processes=20)
    for _ in tqdm.tqdm(pool.imap_unordered(download_stock_data, list), total=len(list)): pass
    # for index in list:
    #     download_stock_data(index)

def handle_forex_list(index):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    response = {'url': url_DK(id6=index['IDE'], sec=index['SEC'] if 'SEC' in index else '', lmt=1), 'module': 'DK_FOREX', 'IDE': index['IDE']}
    make_dk_text_file(index["IDE"], response)

def main_DK():
    print("Starting DK download...")
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/QUATE"):
        os.mkdir(os.path.dirname(__file__) + "/../download/QUATE")

    print("Starting CALIST download...")
    handle_dk_list_file("CALIST")
    print("Finished CALIST download successfully!")
    
    print("Starting CILIST download...")
    handle_dk_list_file("CILIST")
    print("Finished CILIST download successfully!")
    
    print("Starting HYLIST download...")
    handle_dk_list_file("HYLIST")
    print("Finished HYLIST download successfully!")
    
    print("Starting DQLIST download...")
    handle_dk_list_file("DQLIST")
    print("Finished DQLIST download successfully!")
    
    print("Starting GNLIST download...")
    handle_dk_list_file("GNLIST")
    print("Finished GNLIST download successfully!")
    
    print("Starting FOREXLIST download...")
    
    # FOREX LIST
    with open ("../download/IDELIST/FOREXLIST.txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    # lmt = int(parser['download']['FETCH_LIMIT'])
    lmt = 1000
    multiprocessing.freeze_support();
    pool = multiprocessing.Pool(processes=20)
    for _ in tqdm.tqdm(pool.imap_unordered(handle_forex_list, list), total=len(list)): pass
    print("Finished FOREXLIST download successfully!")
    print("Finished all DK list download successfully!")
    print("------------------------------------------------")

if __name__ == "__main__":
    main_DK()

