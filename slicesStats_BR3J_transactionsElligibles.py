#Import des librairies
import time
from datetime import datetime
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
# from openpyxl import load_workbook
from openpyxl import Workbook
import copy

scriptpath = "/home/data-mining/getPersoData/indicesES"
sys.path.append(scriptpath)
import index
import pandas as pd
import numpy as np



def pagesExpo(r) :
    e = r["cdValue"].split("|")
    m = 0
    for i in e :
        p = i.split(":")[-1]
        t = i.split(":")[0]
        m+=1
        # print p
        try :
            if float(p) > 0.1 and float(p)< 0.9 :
                return p,t,m,r["revenue"]
        except :
            print("Problem in parsing custom data "+str(e))

def parseCDGetMax(r) :
    barList = r.split("|")
    scoreList = []
    for page in barList :
        score = page.split(":")[-1]
        scoreList.append(copy.copy(float(score)))
    return max(scoreList)



def getExtractBeginAndEndTimes(datePairList):
    outputList = []
    currentPair = []
    for datePair in datePairList:
        dateBegin = datePair[0]
        timeBeginIncluded = (time.mktime(datetime.datetime.strptime(dateBegin, "%d-%m-%Y %H:%M").timetuple()) * 1000)
        dateEnd = datePair[1]
        timeEndExcluded = (time.mktime(datetime.datetime.strptime(dateEnd, "%d-%m-%Y %H:%M").timetuple()) * 1000)
        currentPair.append(copy.copy(timeBeginIncluded))
        currentPair.append(copy.copy(timeEndExcluded))
        outputList.append(copy.copy(currentPair))
        currentPair = []
    return outputList



if __name__ == "__main__" :

    # CONSTANTS
    SITE_CODE="5ctkqcpb29"
    # PERSO_ID = 23140
    PERSO_ID = 23386
    GOAL_ID = 93444 # transactions eligibles
    # CUSTOM_DATA_ID = 22
    CUSTOM_DATA_ID = 9
    MAX_NUM_VISITORS = 1000000

    dateStartPerso = "22-05-2018 12:00"
    timeStartPerso = (time.mktime(datetime.strptime(dateStartPerso, "%d-%m-%Y %H:%M").timetuple()) * 1000)

    dateBegin = "11-06-2018 00:00"
    timeBeginIncluded = (time.mktime(datetime.strptime(dateBegin, "%d-%m-%Y %H:%M").timetuple()) * 1000)
    dateNow = "14-06-2018 00:00"
    timeEndExcluded = (time.mktime(datetime.strptime(dateNow, "%d-%m-%Y %H:%M").timetuple()) * 1000)
    dateString = "11Juin14Juin"

    # # Use current time
    # dt = datetime.fromtimestamp(time.mktime(time.localtime()))
    #
    # a = dt.replace(hour=0, minute=0, second=0, day=dt.day-1)
    # timeBeginIncluded = (time.mktime(datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S").timetuple())*1000)
    #
    # b = dt.replace(hour=23, minute=59, second=59, day=dt.day-1)
    # timeEndExcluded = (time.mktime(datetime.strptime(str(b), "%Y-%m-%d %H:%M:%S").timetuple())*1000)
    #
    # dt_bef = dt.replace(day=dt.day-1)
    # dateString = str(dt_bef.date())
    #
    # print(a)
    # print(b)
    # print(dateString)

    es=Elasticsearch("formenos.kameleoon.net", timeout = 30)
    matching = index.getIndicesWithDate( SITE_CODE , timeBeginIncluded , timeEndExcluded )
    if len(matching) == 0:
        print("EMPTY INDICES EXITING BEFORE QUERY")
        sys.exit()

    # Create slices
    listeBornesTranches = np.arange(0,0.1,0.01).tolist()
    listeBornesTranches += np.arange(0.1,1.0,0.1).astype(np.float64).tolist()
    listeBornesTranches.append(1.01)

    ranges=[]
    h = {}
    for i in range(len(listeBornesTranches)-1) :
        h = {}
        h = {"Tranche": "[" + str(listeBornesTranches[i]) + ";" + str(listeBornesTranches[i+1]) + "[",
         "Visiteurs Exposes": 0, "Visiteurs Exposes Convertis": 0, "Nombre Conversions Visiteurs Exposes" : 0, "Revenu Visiteurs Exposes" : 0,
         "Visiteurs Non-Exposes": 0, "Visiteurs Non-Exposes Convertis": 0, "Nombre Conversions Visiteurs Non-Exposes" : 0, "Revenu Visiteurs Non-Exposes" : 0,
         "min": listeBornesTranches[i], "max": listeBornesTranches[i+1]}
        ranges.append(copy.copy(h))

    h = {"Tranche": "Total",
         "Visiteurs Exposes": 0, "Visiteurs Exposes Convertis": 0, "Nombre Conversions Visiteurs Exposes" : 0, "Revenu Visiteurs Exposes" : 0,
         "Visiteurs Non-Exposes": 0, "Visiteurs Non-Exposes Convertis": 0, "Nombre Conversions Visiteurs Non-Exposes" : 0, "Revenu Visiteurs Non-Exposes" : 0,
         "min": 0, "max": 1.01}
    ranges.append(copy.copy(h))

    variationIDList = [0, 1]
    variationIDString = ["nonExposed", "exposed"]

    # create workbook
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    wbPath = "/home/data-mining/getPersoData/calum/cdiscount/2018_05_exportScoreScliceStats/results_BR3J/cdiscount_"+dateString+"compteur3J.xlsx"
    writer = pd.ExcelWriter(wbPath, engine='xlsxwriter')
    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book

    countVisitsInPersoWithoutCD22 = 0

    for variation in variationIDList:
        # if True:
        # wbPath = "cdiscount_" + str(variationIDString[variation]) + "_.xlsx"
        # wb = Workbook()
        # ws = wb.active

        query_visitors = {"query": {
            "bool": {
                "must": {
                    "match_all": {},
                },
                "filter": [
                    {"range": {"timeStarted": {"gte": timeBeginIncluded, "lt": timeEndExcluded}}},
                    {"nested": {"path": "personalizations", "query": {"bool": {
                        "must": [{"match": {"personalizations.id": PERSO_ID}},
                                 {"match": {"personalizations.variationId": variation}}]}}}},
                    # {"nested" : {"path" : "customData", "query" : {"bool" : { "must" : [ {"match" : {"customData.id" : CUSTOM_DATA_ID}}]}}}}

                ]
            }
        },

            "aggs": {
                "visitorCode": {
                    "terms": {"field": "visitorCode", "size": MAX_NUM_VISITORS
                              },

                },
                # "to_conversion": { "nested": { "path": "conversions"},
                # "aggs" : { "filtered_by_goalID" : {"filter" : {"term" : {"conversions.goalID" : GOAL_ID} }},
                # "aggs" : { "total_conv" : { "sum" : {"field" : "conversions.count"}},
                #  "total_rev" : {"sum" : {"field" : "conversions.revenue"}}}
                # }
                # }
                }
        }

        # scanResp = es.search(index=matching, body=query_visitors, sort="_doc", scroll="10m", _source=["customData", "VisitNumber", "conversions", "visitorCode", "timeStarted"])
        scanResp = es.search(index=matching, body=query_visitors, sort="_doc", scroll="20m")
        scrollId = scanResp['_scroll_id']
        scroll_size = scanResp["hits"]["total"]
        print(scroll_size)
        print(len(scanResp["aggregations"]["visitorCode"]["buckets"]))


        bucket_num = 0
        for bucket in scanResp["aggregations"]["visitorCode"]["buckets"]:

            bucket_num+=1
            if bucket_num%1000 == 0:
                print(bucket_num)


            ############################## Get statistics for current day ###########################

            visitor_revenue = 0
            visitor_conversions = 0
            visitorScoreSlice = 0

            query_bucket = {"query": {
                "bool" :{
                    "must" :{
                        "match_all": {},
                    },
                    "filter": [
                        {"range": {"timeStarted": {"gte":timeBeginIncluded, "lt" :timeEndExcluded}}},
                        {"bool" : {"must" : {"term" : {"visitorCode" : bucket["key"]}}}},
                        {"nested": {"path": "personalizations", "query": {"bool": {
                            "must": [{"match": {"personalizations.id": PERSO_ID}},
                                     {"match": {"personalizations.variationId": variation}}]}}}},
                        {"nested": {"path": "customData",
                                    "query": {"bool": {"must": [{"match": {"customData.id": CUSTOM_DATA_ID}}]}}}}
                    ]
                    }
                },
                "aggs" : {
                    "to_conversion": {
                        "nested": { "path": "conversions"},
                    "aggs" : {
                        "filtered_by_goalID" : {"filter" : {"term" : {"conversions.goalId" : GOAL_ID} },
                    "aggs" : { "total_conv" : { "sum" : {"field" : "conversions.count"}},
                     "total_rev" : {"sum" : {"field" : "conversions.revenue"}}}
                    }
                    }
                    }
                }
            }

            # scanRespV = es.search(index=matching, body=query_bucket, sort="_doc", scroll="10m",_source=["customData", "VisitNumber", "conversions", "visitorCode", "timeStarted"])
            scanRespV = es.search(index=matching, body=query_bucket, sort="_doc", scroll="15m")
            scroll_size = scanRespV["hits"]["total"]
            current_scroll = len(scanRespV["hits"]["hits"])
            scroll_id = scanRespV['_scroll_id']
            if scroll_size>10:
                print(" WARNING MORE THAN 10 VISITS TO DAY WITH VISITOR "+str(scroll_size))
                print(current_scroll)




            visitor_revenue = scanRespV["aggregations"]["to_conversion"]["filtered_by_goalID"]["total_rev"]["value"]
            visitor_conversions = scanRespV["aggregations"]["to_conversion"]["filtered_by_goalID"]["total_conv"]["value"]

            # GET visitor max score of custom_data_9
            maxScore=0
            currMax=0
            for hit in scanRespV["hits"]["hits"]:
                for cd in hit["_source"]["customData"] :
                    if cd["id"] == CUSTOM_DATA_ID:
                        # print(str(cd["value"][0]))
                        try:
                            currMax = parseCDGetMax(str(cd["value"][0]))
                        except:
                            print("EXCEPTION")
                            print(cd["value"])
                        if currMax>maxScore:
                            maxScore=copy.copy(currMax)

            while current_scroll == 10:
                response = es.scroll(scroll_id=scroll_id, scroll="10m")
                scroll_id = response['_scroll_id']
                current_scroll = len(response["hits"]["hits"])
                if current_scroll<10:
                    print("LAST SCROLL SIZE "+str(current_scroll))
                for hit in response["hits"]["hits"]:
                    for cd in hit["_source"]["customData"]:
                        if cd["id"] == CUSTOM_DATA_ID:
                            # print(str(cd["value"][0]))
                            try:
                                currMax = parseCDGetMax(str(cd["value"][0]))
                            except:
                                print("EXCEPTION")
                                print(cd["value"])
                            if currMax > maxScore:
                                maxScore = copy.copy(currMax)


            visitor_score = copy.copy(maxScore)


            # get score slice
            for slice in range(len(ranges)) :
                if visitor_score < ranges[slice]["max"]:
                    visitorScoreSlice = slice
                    break


            if variation == 0:
                ranges[visitorScoreSlice]["Visiteurs Non-Exposes"] +=1
                ranges[visitorScoreSlice]["Nombre Conversions Visiteurs Non-Exposes"] += visitor_conversions
                ranges[visitorScoreSlice]["Visiteurs Non-Exposes Convertis"] += (1 if (visitor_conversions>0) else 0)
                ranges[visitorScoreSlice]["Revenu Visiteurs Non-Exposes"] += visitor_revenue
                ranges[-1]["Visiteurs Non-Exposes"] += 1
                ranges[-1]["Nombre Conversions Visiteurs Non-Exposes"] += visitor_conversions
                ranges[-1]["Visiteurs Non-Exposes Convertis"] += (1 if (visitor_conversions > 0) else 0)
                ranges[-1]["Revenu Visiteurs Non-Exposes"] += visitor_revenue

            elif variation == 1 :
                ranges[visitorScoreSlice]["Visiteurs Exposes"] +=1
                ranges[visitorScoreSlice]["Nombre Conversions Visiteurs Exposes"] += visitor_conversions
                ranges[visitorScoreSlice]["Visiteurs Exposes Convertis"] += (1 if (visitor_conversions>0) else 0)
                ranges[visitorScoreSlice]["Revenu Visiteurs Exposes"] += visitor_revenue
                ranges[-1]["Visiteurs Exposes"] += 1
                ranges[-1]["Nombre Conversions Visiteurs Exposes"] += visitor_conversions
                ranges[-1]["Visiteurs Exposes Convertis"] += (1 if (visitor_conversions > 0) else 0)
                ranges[-1]["Revenu Visiteurs Exposes"] += visitor_revenue




    # print(" Number of visits in perso without CD22 "+str(countVisitsInPersoWithoutCD22))

    data = pd.DataFrame(ranges)
    print(data)

    data.drop(['min','max'],axis=1)
    data["taux de conversions E"]= data["Visiteurs Exposes Convertis"]/data["Visiteurs Exposes"]
    data["taux de conversions NE"] = data["Visiteurs Non-Exposes Convertis"]/data["Visiteurs Non-Exposes"]
    data["panier moyen E"] = data["Revenu Visiteurs Exposes"]/data["Nombre Conversions Visiteurs Exposes"]
    data["panier moyen NE"] = data["Revenu Visiteurs Non-Exposes"]/data["Nombre Conversions Visiteurs Non-Exposes"]
    data = data[["Tranche","Visiteurs Exposes","Visiteurs Exposes Convertis","Nombre Conversions Visiteurs Exposes","Revenu Visiteurs Exposes", "taux de conversions E","panier moyen E","Visiteurs Non-Exposes","Visiteurs Non-Exposes Convertis","Nombre Conversions Visiteurs Non-Exposes","Revenu Visiteurs Non-Exposes","taux de conversions NE","panier moyen NE"]]

    ##############
    data.to_excel(writer, sheet_name='Sheet1')
    worksheet = writer.sheets['Sheet1']

    # Add some cell formats.
    format1 = workbook.add_format({'num_format': '0.00'})
    format2 = workbook.add_format({'num_format': '0%'})
    format3 = workbook.add_format({'num_format': '#,##0'})

    # Note: It isn't possible to format any cells that already have a format such
    # as the index or headers or any cells that contain dates or datetimes.

    # # Set the column width and format.
    # # worksheet.set_column('B', 30, format1)
    # worksheet.set_column('C:E', 30, format3)
    # # worksheet.set_column('F', 30, format1)
    # worksheet.set_column('G:I', 30, format3)
    # # worksheet.set_column('J', 30, format1)
    # worksheet.set_column('G:G', 30, format2)
    # worksheet.set_column('M:M', 30, format2)

    for i in range(0,20):
        worksheet.set_column(i,i, 25, format1)
    worksheet.set_column('G:G', 30, format2)
    worksheet.set_column('M:M', 30, format2)


    writer.save()




