import pymongo
import configparser
parser = configparser.ConfigParser()
parser.read('config.ini')


remoteMDB_client = pymongo.MongoClient(parser['database']['DB_URL'])
col_QUATE = remoteMDB_client['StockDB']['QUATE']
col_QUATE.create_index([('IDE', pymongo.ASCENDING),  ('DT',  pymongo.ASCENDING)],  unique=True)
col_QUATE.create_index([('DT', pymongo.ASCENDING), ('IDE', pymongo.ASCENDING)],  unique=True)

col_FOREX = remoteMDB_client['StockDB']['FOREX']
col_FOREX.create_index([('IDE', pymongo.ASCENDING), ('DT', pymongo.ASCENDING)],  unique=True)
col_FOREX.create_index([('DT', pymongo.ASCENDING), ('IDE', pymongo.ASCENDING)],  unique=True)

col_QUATEIDX = remoteMDB_client['StockDB']['QUATE_IDX']
col_QUATEIDX.create_index([('IDE', pymongo.ASCENDING), ('DT', pymongo.ASCENDING)],  unique=True)
col_QUATEIDX.create_index([('DT', pymongo.ASCENDING), ('IDE', pymongo.ASCENDING)],  unique=True)

col_IDLIST = remoteMDB_client['StockDB']['IDLIST']
col_IDLIST.create_index([('IDE',  pymongo.ASCENDING)],  unique=True)

col_CW_FH = remoteMDB_client['StockDB']['CW_FH']
col_CW_FH.create_index([('RowNum', pymongo.ASCENDING)],  unique=True)

col_NOTICE = remoteMDB_client['StockDB']['NOTICE']
col_NOTICE.create_index([('IDE', pymongo.ASCENDING), ('infoCode', pymongo.ASCENDING)],  unique=True)

col_DG = remoteMDB_client['StockDB']['DG']
col_DG.create_index([('infoCode', pymongo.ASCENDING)], unique=True)

col_REPORT = remoteMDB_client['StockDB']['REPORT']
col_REPORT.create_index([('art_code', pymongo.ASCENDING)], unique=True)


col_ZGB = remoteMDB_client['StockDB']['ZGB']
col_ZGB.create_index([("IDE", pymongo.ASCENDING), ("DT", pymongo.ASCENDING)], unique=True)

col_CW2 = remoteMDB_client['StockDB']['CW']
col_CW2.create_index([("IDE", pymongo.ASCENDING), ("REPORTDATE", pymongo.ASCENDING)], unique=True)

col_CJ = remoteMDB_client['StockDB']['CJ']
col_CJ.create_index([("url", pymongo.ASCENDING), ("type", pymongo.ASCENDING)], unique=True)

col_USER = remoteMDB_client['StockDB']['USER']
col_USER.create_index([("name", pymongo.ASCENDING)], unique=True)

col_NOTICE_ZIXUN = remoteMDB_client['StockDB']['NOTICE_ZIXUN']
# col_NOTICE_ZIXUN.create_index([("name", pymongo.ASCENDING)], unique=True)

col_NOTICE_GONGGAO = remoteMDB_client['StockDB']['NOTICE_GONGGAO']
# col_NOTICE_GONGGAO.create_index([("name", pymongo.ASCENDING)], unique=True)

DB_DL = remoteMDB_client['dl']
col_DL = DB_DL['dl_all_new']
col_DL.create_index([('url', pymongo.ASCENDING)],  unique=True)
