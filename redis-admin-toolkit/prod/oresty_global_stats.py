import redis
userMap = {}
tsize = 0
gtsize = 0
r = redis.StrictRedis(host='xxxxxxx', port=6380, db=0, password='xxxxx')
print r
keyset = r.keys("*global_stats_agl")
anon_read = 0
anon_write = 0
fbcount = 0
replfailed = 0
for key in keyset :
        val = r.hgetall(key) 
        if "anon_read" in val :
        	anon_read = anon_read + int(val["anon_read"])
        if "anon_write" in val :
        	anon_write = anon_write + int(val["anon_write"])
        if "fallback_count" in val :
        	fbcount = fbcount + int(val["fallback_count"])
        if "repl_failed_response" in val :
                print key
        	replfailed = replfailed + int(val["repl_failed_response"])

print "Anonymous Read -->" + str(anon_read)
print "Anonymous Write -->" + str(anon_write)
print "Fall Back Count is -->" + str(fbcount)
print "Repl Failed Count is -->" + str(replfailed)

