import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import tqdm
import configparser
from src import MDB
import Export.Ex_QUATE
import Export.Ex_FH
import Export.Ex_CW
import Export.Ex_ZG
import Export.Ex_ZGB
# sys.path.insert(0, '..')


def run_export():
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    dates = [(datetime.now() - relativedelta(days=x)).strftime('%Y-%m-%d') for x in
             range(int(parser['export']['duration']))]
    print("Exporting QUATE table...")
    for date in tqdm.tqdm(dates):
        Export.Ex_QUATE.export(DB=MDB.col_QUATE, path=parser['export']['export_path'], DT=date, batch_size=30000)
    print("Exporting QUATE_IDX table...")
    for date in tqdm.tqdm(dates):
        Export.Ex_QUATE.export_IDX(DB=MDB.col_QUATEIDX, path=parser['export']['export_path'], DT=date, batch_size=30000)
    print("Exporting FenHong table...")
    Export.Ex_FH.export(DB=MDB.col_CW_FH, path=parser['export']['export_path'],
                        DT=(datetime.now() - relativedelta(years=int(parser['download']['FETCH_LIMIT']))).strftime('%Y-%m-%d'), batch_size=30000)
    print("Exporting CaiWu table...")
    Export.Ex_CW.export(DB=MDB.col_CW2, path=parser['export']['export_path'],
                        DT=(datetime.now() - relativedelta(months=int(parser['download']['FETCH_LIMIT_EXPORT_CW_MONTHS']))).strftime('%Y-%m-%d'), batch_size=1000)
    print("Exporting ZiXun table...")
    # ss="".join([chr(c) for c in s])
    Export.Ex_ZG.export(DB=MDB.col_NOTICE_ZIXUN, path=parser['export']['export_path'], DT=dates[-1], batch_size=5000)
    print("Exporting GongGao table...")
    Export.Ex_ZG.export_GG(DB=MDB.col_NOTICE_GONGGAO, path=parser['export']['export_path'], DT=dates[-1],
                           batch_size=5000)
    print("Exporting ZGB table...")
    Export.Ex_ZGB.export(DB=MDB.col_ZGB, path=parser['export']['export_path'], batch_size=50000)


if __name__ == "__main__":
    run_export()

