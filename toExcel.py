import time
import datetime
import sys
sys.getdefaultencoding()
# sys.setdefaultencoding("utf8")
# import index
import json
import csv
import os
import os.path
from datetime import date
from elasticsearch import Elasticsearch
#Import pour excel
# import dataToExcel
import openpyxl
import glob
# from pyexcel.cookbook import merge_all_to_a_book
from datetime import date
from openpyxl import load_workbook

scriptpath = "/home/data-mining/getPersoData/indicesES"
sys.path.append(scriptpath)
import index
import pandas as pd

from openpyxl import Workbook
siteCode="mnpvb2hj6w"


timeInterval=86400000.0 #Interval de temps

es=Elasticsearch("formenos.kameleoon.net", timeout = 30)
import json

dateBegin = "10/09/2018 16:00"
dateEnd = "31/09/2018 00:00"
timeBegin = time.mktime(datetime.datetime.strptime(dateBegin, "%d/%m/%Y %H:%M").timetuple())*1000
timeEnd = time.mktime(datetime.datetime.strptime(dateEnd, "%d/%m/%Y %H:%M").timetuple())*1000
timeBeginIncluded = timeBegin
timeEndExcluded = timeEnd


if True :
# while timeNow>timeEndExcluded:
    # pass
    data = []

    # print time.strftime("%d-%m-%Y", time.localtime(timeBeginIncluded/1000))
    matching = index.getIndicesWithDate( siteCode , timeBeginIncluded , timeEndExcluded )
    print matching
    query={"query": {
            "bool" :{
                "must" :{
                    "match_all": {},
                },
                "filter": [
                    {"range": {
                        "timeStarted": {
                            "gte":timeBeginIncluded,
                            "lt" :timeEndExcluded
                        }
                    }
                },
                {"nested":{"path" : "experiments" , "query":{"bool":{"must" : [{"term" :{"experiments.id":64383}}, {"term" :{"experiments.variationId":194592}}]}}}},
                ]
                }
                }
        }

    if(len(matching)):
        scanResp = es.search(index=matching, body=query, sort="_doc", scroll="10m", _source=["visitorCode", "pageURLs", "conversions", "deviceType","browser","os", "customData" ])
        scrollId = scanResp['_scroll_id']
        scroll_size = scanResp["hits"]["total"]
        print scroll_size

        i=10
        while scroll_size>0:
            # line =""

            response= es.scroll(scroll_id=scrollId, scroll="10m")
            for hit in response["hits"]["hits"]:
                h={}
                h["accees_lpb"] = 0
                h["visitorCode"] = hit["_source"]["visitorCode"]
                h["pages"] = hit["_source"]["pageURLs"]
                h["device"] = hit["_source"]["deviceType"]
                h["browser"] = hit["_source"]["browser"]
                h["os"] = hit["_source"]["os"]
                for conv in hit["_source"]["conversions"] :
                    if conv["goalId"] ==  92023 :
                        h["accees_lpb"] = 1
                for cd in hit["_source"]["customData"] :
                    if cd["id"] == 0 :
                        h["URLRed"] = cd["value"]
                    if cd["id"] == 1 :
                        h["CD2"] = cd["value"]
                data.append(h)
            scroll_size = len(response['hits']['hits'])
        # timeEndExcluded+=timeInterval
        # timeBeginIncluded+=timeInterval
    else:
        print 'no matching'
data = pd.DataFrame(data)
data.to_excel("seLoger.xlsx")
