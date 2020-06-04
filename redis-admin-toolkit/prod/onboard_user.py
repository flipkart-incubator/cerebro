import redis
userMap = {}
tsize = 0
gtsize = 0
r = redis.StrictRedis(host='X.X.X.X', port=XXXX, db=0, password='xxxxxxxxx')
print r
#Add verdification of acccess_key and user and cluster to be onboarded
access_key = ""
user = ""
target_cluster = ""
userkey = access_key + "_user_agl"
userhash = {
             "epoch": "21",
             "version": "1.0",
             "target_write_cluster": target_cluster,
             "user_name" : user,
             "placement_policy" : "Default",
             "bootstrap_cluster" : target_cluster
            }
print "Updating User -->", user
print "Updating User Key as -->", userkey
print "Updating hash as -->"
print userhash

res = r.hmset(userkey, userhash)
print "Updating User Result-->", res
print "Exiting"


        

