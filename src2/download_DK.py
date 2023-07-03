import configparser
import os
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
    print("QUATE_" + filename + " file was made successfully!")

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
    for index in list:
        download_stock_data(index)

def main_DK():
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/QUATE"):
        os.mkdir(os.path.dirname(__file__) + "/../download/QUATE")

    handle_dk_list_file("CALIST")
    handle_dk_list_file("CILIST")
    handle_dk_list_file("HYLIST")
    handle_dk_list_file("DQLIST")
    handle_dk_list_file("GNLIST")
    
    # FOREX LIST
    with open ("../download/IDELIST/FOREXLIST.txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    # lmt = int(parser['download']['FETCH_LIMIT'])
    lmt = 1000
    for index in list:
        parser = configparser.ConfigParser()
        parser.read('config.ini')
        response = {'url': url_DK(id6=index['IDE'], sec=index['SEC'] if 'SEC' in index else '', lmt=1), 'module': 'DK_FOREX', 'IDE': index['IDE']}
        make_dk_text_file(index["IDE"], response)

if __name__ == "__main__":
    main_DK()

