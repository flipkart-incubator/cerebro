import redis
userMap = {}
tsize = 0
gtsize = 0
r = redis.StrictRedis(host='xxxxxxxx', port=6380, db=0, password='xxxxxxx')
print r
res = r.keys("xxxxx")
for itr in res:
	print itr
	res = r.delete(itr)
        print res

        

