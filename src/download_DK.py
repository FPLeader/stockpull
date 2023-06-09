import configparser
import os
import json
import DK.url
import EM_FOREX.url

def make_text_file(filename, data):
    print("make text file for QUATE_" + filename + "...")
    text = json.dumps(data)
    script_dir = os.path.dirname(__file__)
    rel_path = "../download/QUATE/QUATE_" + filename + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, "w") as f:
        f.write(text)
    print("QUATE_" + filename + " file was made successfully!")

def download_stock_data(stock, module_normal="DK_LINE", module_flow="DK_FLOW"):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    fetch_param1 = {'url': DK.url.url_DK(stock["IDE"], lmt=int(parser['download']['FETCH_LIMIT'])), 'module': module_normal, 'IDE': stock["IDE"]}
    fetch_param2 = {'url': DK.url.url_FLOW(ide=stock['IDE'], lmt=int(parser['download']['FETCH_LIMIT'])), 'module': module_flow, 'IDE': stock['IDE']}
    response = {'url1': fetch_param1['url'], 'module1': fetch_param1['module'], 'url2': fetch_param2['url'], 'module2': fetch_param2['module'], 'IDE': stock['IDE']}
    make_text_file(stock["IDE"], response)

def handle_list_file(filename, module_normal="DK_LINE", module_flow="DK_FLOW"):
    with open ("../download/IDELIST/" + filename + ".txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    for index in list:
        download_stock_data(index, module_normal, module_flow)

if __name__ == "__main__":
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/QUATE"):
        os.mkdir(os.path.dirname(__file__) + "/../download/QUATE")

    handle_list_file("CALIST")
    handle_list_file("CILIST", module_normal='DK_LINE_IDX', module_flow='DK_FLOW_IDX')
    handle_list_file("HYLIST", module_normal='DK_LINE_IDX', module_flow='DK_FLOW_IDX')
    handle_list_file("DQLIST", module_normal='DK_LINE_IDX', module_flow='DK_FLOW_IDX')
    handle_list_file("GNLIST", module_normal='DK_LINE_IDX', module_flow='DK_FLOW_IDX')
    
    # FOREX LIST
    with open ("../download/IDELIST/FOREXLIST.txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    for index in list:
        parser = configparser.ConfigParser()
        parser.read('config.ini')
        response = {'url': EM_FOREX.url.url_DK(id6=index['IDE'], sec=index['SEC'] if 'SEC' in index else '', lmt=int(parser['download']['FETCH_LIMIT'])), 'module': 'DK_FOREX', 'IDE': index['IDE']}
        make_text_file(index["IDE"], response)


