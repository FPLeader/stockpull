import os, sys, requests, math, configparser, json
from os.path import exists
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import tqdm, multiprocessing
import EM_TOOL
import download_IDELIST, download_DK, download_FenHong, download_CW, download_DG

if __name__ == "__main__":
    # part of IDELIST download 
    download_IDELIST.main_IDELIST()

    # part of download DK
    download_DK.main_DK()

    # part of download FenHong
    download_FenHong.main_FenHong()

    # part of download CW
    download_CW.main_CW()

    # part of download DG
    download_DG.main_DG()

