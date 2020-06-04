import redis
r = redis.StrictRedis(host='xxxxx', port=6380, db=0, password='xxxxxxxxxxx')
print r
if __name__ == "__main__":
	key = "xxxxxxxx_buck_agl"
	bucket_hash_val = {
				"active" : "xxxx",
				"active_sync" : "xxx",
				"state" : "201",
				"opmode" : "100",
				"version": "1.0",
				"access_key" : "xxxxxxxx",
                                "replication_enabled" : "0"
			  }

res = r.hmset(key, bucket_hash_val)
print "Updating User Result-->", res
res = r.hincrby(key, "epoch", 1)
print "Updating User Result-->", res
print "Exiting"


        

