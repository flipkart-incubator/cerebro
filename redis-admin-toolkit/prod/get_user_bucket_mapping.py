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
		self.redis = redis.StrictRedis(host=self.hostname, port= self.port, db=0, password='XXXXXXX')
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
		print user_details
                print user_details["keys"][0]["access_key"]
                print user_details["keys"][0]["secret_key"]
                print user_details["keys"][0]["user"]
		for bucket in rgw_conn.get_bucket(uid = user_details["keys"][0]["user"]):
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


if __name__ == "__main__":
	target_cluster = sys.argv[1]
	update_user_bucket_obj = Update(redis_host, redis_port, user, target_cluster)
	update_user_bucket_obj.updatebuckets()
