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

def download_stock_data(stock):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    response = DK.url.fetch_push(stock["IDE"])
    make_text_file(stock["IDE"], response)

def handle_list_file(filename):
    with open ("../download/IDELIST/" + filename + ".txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    list = [x for x in json_object]
    for index in list:
        download_stock_data(index)

if __name__ == "__main__":
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/QUATE"):
        os.mkdir(os.path.dirname(__file__) + "/../download/QUATE")

    handle_list_file("CALIST")
    handle_list_file("CILIST")
    handle_list_file("HYLIST")
    handle_list_file("DQLIST")
    handle_list_file("GNLIST")
    
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


