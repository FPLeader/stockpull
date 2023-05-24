import os
import configparser
import MDB
import shutil
from tqdm import tqdm


def extract_pdf(data):
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    path = parser['main_pdf']['path']
    extract_path = parser['main_pdf']['extract_path']
    src_path = os.path.join(
        path,
        data['infoCode'].replace('AP', '')[0:8],
        data['columnType'], 
        data['infoCode']+'.pdf')
    
    os.makedirs(path, exist_ok = True)
    dst_path = os.path.join(
        extract_path, 
        data['orgSName'], 
        f"{data['infoCode']}.pdf"
        )
    os.makedirs(os.path.join(extract_path, data['orgSName']), exist_ok=True)
    if os.path.isfile(dst_path):
        return True
    if os.path.isfile(src_path) is True:
        shutil.copyfile(src_path, dst_path)
        return True
    

if __name__ == "__main__":    
    report_data = [x for x in MDB.col_DG.find({
        "$or": [
            {"orgSName": {"$regex": "头豹"}},
            {"orgSName": {"$regex": "五矿"}},
            {"orgSName": {"$regex": "华安"}}
        ]})]
    for index in tqdm(range(len(report_data))):    
        extract_pdf(report_data[index])
        
    
    


