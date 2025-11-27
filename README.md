# flume-to-sqlserver
flume sink collect json.log to sql

## 📂source 
json file
## 📜channel
local file 
## 🔗sink 
sql server

those java file are used by sink

# introduction 
### 🔍function:  
1. verify json format, if data in channel is not json format,print to metrics log and skip this data
2. skip error data,just like "
                if (content.contains("\u0000")) {
                    return false;
                }"
3. sql server connection pool   
4. not auto commit transation
# How to use 
### ✅pack jar
you need maven project 
mvn package and get like json-to-sql.jar
### ✅config
flume config : agent.sinks.k1.type = flume.sqlserver.sink.SQLSink
