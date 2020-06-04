#Parse the USer Name 
#Get the access key
#Parse The Server Where User Exists
#Parse Where the Repication Needs to be Done
#Sync any Buckets Before starting the replication
#Parse all Existing Bucekts and Update repl_mode to 1
#Enable the User Replication Mode To 1


import sys
from rgwadmin import RGWAdmin
import boto
import boto.s3.connection
import redis

#Global Cluster  info Storing the RGW Endpoint Details for Various CLusters
cluster_info = {
}

class Replication:
    
    def __init__(self,user,repl_mode, src_cluster, repl_cluster, hostname, port):
        self.user = user
        self.repl_mode = repl_mode
	self.src_cluster = src_cluster
	self.repl_cluster = repl_cluster
	self.hostname = hostname
	self.port = port
        self.redis = redis.StrictRedis(host=self.hostname, port=self.port, db=0, password='XXXXX')


    def get_rgw_conn(self, access_key, secret_key, ip_addr):
        return RGWAdmin(access_key=access_key, secret_key=secret_key, server=ip_addr, secure=False)

    def get_cluster_endpoint(self, cluster_name):
    	if cluster_info[cluster_name]:
        	print "Cluster info for Cluster Name -->" + cluster_name
                print cluster_info[cluster_name]["access_key"]
                print cluster_info[cluster_name]["secret_key"]
                print cluster_info[cluster_name]["host"]
                akey = cluster_info[cluster_name]["access_key"]
                skey = cluster_info[cluster_name]["secret_key"]
                ipaddr = cluster_info[cluster_name]["host"]
                return self.get_rgw_conn(akey,skey,ipaddr)

    def update_user_replication_info(self):
        print "User is  --> " +self.user
        print "Repl Mode is --> "  + self.repl_mode
        print "Src Cluster is --> " + self.src_cluster
        print "Replication Cluster is --> " + self.repl_cluster
        print "Replication Cluster is --> " + self.hostname
        print "Replication Cluster is --> " + self.port

	rgw_conn = self.get_cluster_endpoint(self.src_cluster)

	user_details = rgw_conn.get_user(self.user)
        print user_details
        print user_details["keys"][0]["access_key"]
        print user_details["keys"][0]["secret_key"]
        print user_details["keys"][0]["user"]

	#Prepare the User Key For Replication
        user_key = user_details["keys"][0]["access_key"] + "_repl_agl"
	replhash = {
             "version": "1.0",
             "repl_mode": self.repl_mode,
             "read_cluster" : self.repl_cluster,
             "write_cluster" : self.repl_cluster,
            }
	print "Updating User Key as -->", user_key
	print "Updating hash as -->"
	print replhash
	res = self.redis.hmset(user_key, replhash)
	print "Updating User Result-->", res
        res = self.redis.hincrby(user_key, "epoch", 1)
        print res
	res = self.redis.hgetall(user_key)
        print res
        
        #Now Get all the Buckets and Enable there replication mode to 1
        for bucket in rgw_conn.get_bucket(uid = user_details["keys"][0]["user"]):
                bucketkey =  bucket + "_buck_agl"   
		buckethash = self.redis.hgetall(bucketkey)
                print "Original Bucket Hash is -->"
                print buckethash
                buckethash["replication_enabled"] = "1"
                r = self.redis.hmset(bucketkey, buckethash)
                print r
                res = self.redis.hincrby(bucketkey, "epoch", 1)
                print res
                updatedhash = self.redis.hgetall(bucketkey)
                print updatedhash
                       
        


if __name__  == "__main__":
    #Get the User name, the replication mode and the source and destination clusters
    #The SRC Cluster is where the actual traffic is played down.
    #The DST Cluster is where the replicated traffic is replayed
    #The Replication mode is used to toggle the replication on or off for this particular user
    user = sys.argv[1]
    repl_mode = sys.argv[2]
    src_cluster = sys.argv[3]
    repl_cluster = sys.argv[4] 
    hostname = sys.argv[5]
    port = sys.argv[6]

    
    repl_update_obj = Replication(user, repl_mode, src_cluster, repl_cluster, hostname, port)
    repl_update_obj.update_user_replication_info()

