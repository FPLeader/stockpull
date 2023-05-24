@ECHO off
SET MONGO_SERVICE="C:\Program Files\MongoDB\Server\5.0\bin\mongod.exe"
ECHO --------Starting service---------
REM 1. Install the service into your windows service
%MONGO_SERVICE% --config "E:\work\StockPull2.0\data\mongod.cfg" --install --serviceName "MyMongoDB"
REM 2. Start installed mongodb service
net start MyMongoDB
ECHO --------Done---------
PAUSE

