import redis
userMap = {}
tsize = 0
gtsize = 0
r = redis.StrictRedis(host='xxxxxxx', port=xxxxxxxx, db=0, password='xxxxxxxx')
print r
userSet = r.keys("*user_agl")
for key in userSet :
	print key
        val = r.hgetall(key)
        if "user_name" in val :
		print val['user_name']
		userMap[key] = val['user_name']
	else :
		print "Empty User name"
print userMap

#For Every User, Extract the Request Details from all the Pid and
#Hostname 
for user in userMap:
	#print user
        #print userMap[user]  
	user = user.strip("_user_agl")
        #print user
        statsuser = r.keys("*"+user+"stats_agl")
        putcount = 0
        getcount = 0
        delcount = 0 
        postcount = 0 
        headcount = 0 
        putsize = 0
        getsize = 0
        count_2xx = 0
        count_3xx = 0
        count_4xx = 0
        count_5xx = 0
        print "--Starting User Stats-----------------"
        print userMap[user+"_user_agl"]
        print user
        for key in statsuser:
                #print "Host Name --> " + key
        	val = r.hgetall(key)
                """
                for v in val :
                        print val[v]
			print v
                """
                if "PUT" in val :
                	putcount = putcount + int(val["PUT"])
                if "GET" in val :
                	getcount = getcount + int(val["GET"])
                if "DELETE" in val :
                	delcount = delcount + int(val["DELETE"])
                if "POST" in val :
                	postcount = postcount + int(val["POST"])
                if "HEAD" in val :
                	headcount = headcount + int(val["HEAD"])
                if "200" in val :
                	count_2xx = count_2xx + int(val["200"])
                if "403" in val :
                	count_4xx = count_4xx + int(val["403"])
                if "404" in val :
                	count_4xx = count_4xx + int(val["404"])
                if "503" in val :
                	count_5xx = count_5xx + int(val["503"])
                if "put_size" in val :
                	putsize = putsize + int(val["put_size"])
                if "get_size" in val :
                	getsize = getsize + int(val["get_size"])

        print "PUT Count  -->"  + str(putcount)
        print "POST Count -->"  + str(postcount)
        print "DELETE Count -->"  + str(delcount)
        print "GET Count -->"  + str(getcount)
        print "2xx Count -->"  + str(count_2xx)
        print "4xx Count -->"  + str(count_4xx)
        print "5xx Count -->"  + str(count_5xx)
        print "PUT Size -->"  + str(putsize)
        print "GET Size -->"  + str(getsize)
        tsize = tsize + putsize
        gtsize = gtsize + getsize
        print key 
        print tsize
        print gtsize
        print "================="


##Global Stats Which are accumlated across all users
print "Tsize is  ---->" + str(tsize)
print "GTsize is ---->" + str(gtsize)
"""
result = r.keys("*stats_agl")
for key in result :
	val = r.hgetall(key)
        print key 
        print val
"""
