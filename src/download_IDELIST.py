import os
import json
import numpy
import IDELIST.url
import EM_FOREX.url

def make_text_file(filename, data):
    print("make text file for " + filename + "...")
    text = json.dumps(data)
    script_dir = os.path.dirname(__file__)
    rel_path = "../download/" + filename + ".txt"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, "w") as f:
        f.write(text)
    print(filename + " file was made successfully!")

if __name__ == "__main__":
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    
    # stock list
    print("load stock list from Internet...")
    response = IDELIST.url.fetch_push_init(IDELIST.url.url_CALIST(pn=1, pz=10000), "stock")
    print("downloaded stock list!")
    make_text_file("CALIST", response)

    #index list
    print("load index list from Internet...")
    response = IDELIST.url.fetch_push_init(IDELIST.url.url_CILIST(pn=1, pz=10000), "index")
    print("downloaded index list!")
    make_text_file("CILIST", response)

    #HY list
    print("load HY list from Internet...")
    response = IDELIST.url.fetch_push_init(IDELIST.url.url_HYLIST(pn=1, pz=10000), "BK_HY")
    print("downloaded HY list!")
    make_text_file("HYLIST", response)

    # DQ list
    print("load DQ list from Internet...")
    response = IDELIST.url.fetch_push_init(IDELIST.url.url_DQLIST(pn=1, pz=10000), "BK_DQ")
    print("downloaded DQ list!")
    make_text_file("DQLIST", response)

    #GN list
    print("load GN list from Internet...")
    response = IDELIST.url.fetch_push_init(IDELIST.url.url_DQLIST(pn=1, pz=10000), "BK_DQ")
    print("downloaded GN list!")
    make_text_file("GNLIST", response)

    #FOREXLIST
    print("load Forex list from Internet...")
    response = EM_FOREX.url.fetch_push_init(EM_FOREX.url.url_LIST(pn=1, pz=1000), "forex")
    print("downloaded Forex list!")
    make_text_file("FOREXLIST", response)