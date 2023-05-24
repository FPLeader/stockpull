@ECHO OFF
SET MONGO_SERVICE="C:\Program Files\MongoDB\Server\5.0\bin\mongod.exe"
ECHO --------Stop and Remove service---------
REM 1. Stop your mongodb service
Net start StockMDB
REM 2. Remove mongodb service
%MONGO_SERVICE% --remove --serviceName "StockMDB"
ECHO --------Done---------
PAUSE
