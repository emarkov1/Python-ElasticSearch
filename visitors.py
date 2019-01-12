if(len(matching)):
        visitorCodes =[]
        scanResp = es.search(index=matching, body=query, sort="_doc", scroll="1m", _source=["visitorCode", "pageURLs", "conversions", "deviceType","browser","os", "customData", "experiments" ])
        scrollId = scanResp['_scroll_id']
        scroll_size = scanResp["hits"]["total"]
        print scroll_size
        scroll_size1 = scroll_size
        i=10
        count = 0
        while count < scroll_size1-10:
            response= es.scroll(scroll_id=scrollId, scroll= "10m")
            for hit in response["hits"]["hits"]: 
			
                visitorCodes.append(hit["_source"]["visitorCode"])  
                count += 1         
				





        scanResp = es.search(index=matching, body=query, sort="_doc", scroll="1m", _source=["visitorCode", "pageURLs", "conversions", "deviceType","browser","os", "customData" ])
        scrollId = scanResp['_scroll_id']
        scroll_size = scanResp["hits"]["total"]
        scroll_size1 = scroll_size
        i=10
        count1 = scroll_size1 - count
        response = scanResp
        while count1 > 0:

            for hit in response["hits"]["hits"]:
                visitorCodes.append(hit["_source"]["visitorCode"])           
                count1 -= 1
                count += 1
                response= es.scroll(scroll_id=scrollId, scroll="10m")
    else:
        print 'no matching'


print str(len(list(set(visitorCodes))))
