import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import FenHong.url


if __name__ == '__main__':
    # checking if download folder exists or not.
    if not os.path.isdir("../download"):
        os.mkdir(os.path.dirname(__file__) + "/../download")
    if not os.path.isdir("../download/FENHONG"):
        os.mkdir(os.path.dirname(__file__) + "/../download/FENHONG")

    one_yrs_ago = datetime.now() - relativedelta(years=10)
    last_date_limit = one_yrs_ago.strftime('%Y-%m-%d')
    response = FenHong.url.get_list()
    list = response['result']['data']
    for x in list:
        report_date = datetime.strptime(x['REPORT_DATE'], "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        prefix_filename = datetime.strptime(x['REPORT_DATE'], "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d')
        print("downloading and writing text file for " + report_date + " report")
        data_response = FenHong.url.get_data(report_date)
        for y in data_response['result']['data']:
            security_code = y['SECURITY_CODE']
            text = json.dumps(y)
            script_dir = os.path.dirname(__file__)
            rel_path = "../download/FENHONG/FENHONG_" + prefix_filename + "_" + security_code +".txt"
            abs_file_path = os.path.join(script_dir, rel_path)
            with open(abs_file_path, "w") as f:
                f.write(text)
        

