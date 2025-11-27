# flume-to-sqlserver
flume json.log to sql

## source 
json file
## channel
local file 
## sink 
sql server

this java code is used by sink

function:  
1. verify json format, if data in channel is not json format,print to metrics log and skip this data
2. skip error data,just like "
                if (content.contains("\u0000")) {
                    return false;
                }"
3. sql server connection pool   
4. not auto commit transation  
