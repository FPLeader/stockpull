<!-- @format -->

# **Stock Pull**

## _Getting started_

To make it easy for you to get started with this project, here's a list of recommended next steps.

## _Installation_

- [Download and install python (recommenedation version 3.8.10)](https://www.python.org/ftp/python/3.8.10/python-3.8.10-amd64.exe)

- Make your enviroment on local and install dependence

```
$ cd /your_project_root_path
$ python -m venv /path/your/env_path
$ .\env\Scripts\activate
$ pip install -r requirement
```

- [Install MongoDB](https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-5.0.6-signed.msi) and run service mongodb

Please open `MongoDB\Server\5.0\bin\mongod.cfg` at your path that it installed and config as follow

```cfg
storage:
  dbPath: your_database_diretory_path
  journal:
    enabled: true

systemLog:
  destination: file
  logAppend: true
  path:  your_DB_log_path
net:
  port: 27017
  bindIp: 127.0.0.1
```

## _Run script_

- Fetch all data of stocks and store to database

Please configure `project_root/src/config.ini`

```ini
[download]
IDELIST = yes
Today = no
FenHong = no
DK = yes
MK = no
CaiWu = no
EM_NES = no
EM_YaoWen = no
EM_Report = no
FETCH_LIMIT = 10
CaiWu_Dates = 2020-01-01
REPORT_DAYS_AGO = 10
FETCH_LIMIT_EXPORT_CW_MONTHS = 10

[main_pdf]
StartDate = 20200101
path = your_exported_pdf_file_path
extract_path = your_extracted_pdf_file_path
[export]
export_path = your_exported_files_path
duration = 10
[database]
DB_URL = mongodb://localhost:27017/
```

And run script for fetch all database data at your project root path on cmd

```
cd src
python main.py -r
```

Fetch with config.ini 
```
cd src
python main.py
``` 

- Export research reports from database

```
python main_export.py
```

- Export report as pdf

```
python main_pdf.py
```

- Fetch news with html to database

```
python main_html.py
```

- Extract report to specific path

```
python extract_pdf.py
```
