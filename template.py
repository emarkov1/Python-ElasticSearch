##importation des libs
import sys
from elasticsearch import Elasticsearch
from datetime import timedelta
import csv
from openpyxl import load_workbook
import openpyxl
from openpyxl import Workbook
scriptpath = "/home/data-mining/getPersoData/indicesES"
sys.path.append(scriptpath)
import index
import time
import datetime
 
#define variable
siteCode = "ca4kfioskh"
expId = 69762
es=Elasticsearch("formenos.kameleoon.net")

dateBegin = "07/10/2018"
timeBeginIncluded = time.mktime(datetime.datetime.strptime(dateBegin, "%d/%m/%Y").timetuple())*1000

dateNow = "06/01/2017"
timeEndExcluded = time.mktime(datetime.datetime.strptime(dateNow, "%d/%m/%Y").timetuple())*1000

matching = index.getIndicesWithDate( siteCode , timeBeginIncluded , timeEndExcluded )
query={ "query": {
        "bool": {
            "must": {
                "match_all":{}
            },
            "filter": [
                {"range": {
                    "timeStarted": {
                        "gte":timeBeginIncluded,
                        "lt" :timeEndExcluded
                        }
                    }
                },
                {
                "nested": {
                    "path": "experiments",
                    "query": {
                        "bool": {
                            "must":
                                {"match" : {"experiments.id":expId}
                            }
                        }
                    }
                }
                },
                {"nested": {
                    "path": "conversions",
                    "query": {
                        "bool": {
                            "must":{"term" : {"conversions.goalId":76362}}
                        }
                    }
                }},
                
            ]
        }
    }
}

if(len(matching)):
    scanResp = es.search(index=matching, body=query, sort = "_doc",  scroll="1m")

    scrollId = scanResp['_scroll_id']

    scroll_size = scanResp["hits"]["total"]
    print scroll_size

    print "end"
else:
        print 'no matching'


 