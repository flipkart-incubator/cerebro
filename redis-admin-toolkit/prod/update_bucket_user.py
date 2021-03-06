import redis
from rgwadmin import RGWAdmin
import boto
import boto.s3.connection
import sys

cluster_info = {
}


class Update:
	def __init__(self,hostname,port,user, target_cluster):
		self.hostname = hostname
		self.port = port
		self.user = user
                self.target_cluster = target_cluster
		self.redis = redis.StrictRedis(host=self.hostname, port= self.port, db=0, password='XXXXXXXXX')
		print self.redis
	
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
	
	def updatebuckets(self):
		rgw_conn = self.get_cluster_endpoint(self.target_cluster)
                user_details = rgw_conn.get_user(self.user)
		ctr = 0
		print user_details
                print user_details["keys"][0]["access_key"]
                print user_details["keys"][0]["secret_key"]
                print user_details["keys"][0]["user"]
		for bucket in rgw_conn.get_bucket(uid = user_details["keys"][0]["user"]):
			ctr = ctr + 1
			print bucket
			#Update the Buckets in the DB, with the Active Cluster and Active Sync Cluster to the Target Cluster
			key = bucket + "_buck_agl"
			bucket_hash_val = {
					"active" : self.target_cluster,
					"active_sync" : self.target_cluster,
					"state" : "200",
					"opmode" : "105",
					"version": "1.0",
					"access_key" : user_details["keys"][0]["access_key"],
					"replication_enabled" : "0"
			}
			r = self.redis.hmset(key, bucket_hash_val)
			print r
			r = self.redis.hincrby(key, "epoch", 1)
			print r
                        bkt_hash_val = self.redis.hgetall(key)
                        print bkt_hash_val
			print ctr


if __name__ == "__main__":

	redis_host = sys.argv[1]
	redis_port = sys.argv[2]
	user = sys.argv[3]
	target_cluster = sys.argv[4]

	update_user_bucket_obj = Update(redis_host, redis_port, user, target_cluster)
	#Get the User Buckets from the Target Cluster, and Update the Redis DB
	update_user_bucket_obj.updatebuckets()
