import sys,requests,json,pymongo
sys.path.insert(0,'..')
import EM_TOOL, MDB
from bs4 import BeautifulSoup

def url_zxjh(pn=1): #资讯精华
    return "http://finance.eastmoney.com/a/cywjh_{}.html".format(pn)
def url_cgnjj(pn=1): #国内经济
    return "http://finance.eastmoney.com/a/cgnjj_{}.html".format(pn)
def url_cgjjj(pn=1): #国际经济
    return "http://finance.eastmoney.com/a/cgjjj_{}.html".format(pn)
def url_czqyw(pn=1): #证券聚焦
    return "http://finance.eastmoney.com/a/czqyw_{}.html".format(pn)
def url_cgsxw(pn=1): #公司资讯
    return "http://finance.eastmoney.com/a/cgsxw_{}.html".format(pn)

def parser(content,type):
    request = []
    newsList = BeautifulSoup(content,'html.parser').find(id = "newsListContent").findAll("li")
    for itm in newsList:
        ptitle = itm.find("p",  {"class": "title"}).find("a")
        pinfo = itm.find("p",  {"class": "info"})
        ptime = itm.find("p",  {"class": "time"})
        # pimg = itm.find("img")
        re = {
            'title':    ptitle.text.replace(" ",  "").replace("\r\n",  "").encode('utf-8', 'ignore').decode("utf-8", "ignore"), 
            'info':     pinfo.text.replace(" ",  "").replace("\r\n",  ""),
            'url':      ptitle.get('href'),
            'time':     ptime.text.replace(" ", "").replace("\r\n",  ""),
            'type':     type
        }
        # print(re)
        # try:
        #     response = requests.get('http:'+ itm.find("img")['src'])
        #     re['img'] = (
        #         "data:" + response.headers['Content-Type'] + ";" 
        #         + "base64 " + base64.b64encode(response.content).decode("utf-8")   
        #     )
        # except: pass
        request.append(pymongo.UpdateOne({"url":re['url'],'type':re['type']},{"$set":re},upsert=True)) 
    if request: MDB.col_CJ.bulk_write(request)
    return True

