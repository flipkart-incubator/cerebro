# Init Script for Populating the Cache for all the User on the Passed  Cluster "psa"
# This Script Populates Both the User Details as well as all the possible buckets
# The Buckets store the access_key mapping as well

import json
from rgwadmin import RGWAdmin
import boto
import boto.s3.connection
import redis

class RedisUpdate:

    def __init__(self,hostname,port):
        self.hostname = hostname
        self.port = port
        self.redis = redis.StrictRedis(host=self.hostname, port=self.port, db=0, password='XXXXX')

    def get_rgw_conn(self, access_key,secret_key,ip_addr):
        return RGWAdmin(access_key=access_key, secret_key=secret_key,
            server=ip_addr, secure=False)

    def get_clusters(self):
        # cluster info had rgwadmin access key secret key and rgw host details,
        # add entry to dictionary for any new cluster
        clusters_info = {
        }
        return clusters_info
    
    def set_key(self,keys_list):
        # This function updates the redis with the hash values
        for key in keys_list:
           self.redis.hmset(key, keys_list[key])
    
#Given a User Name, Return all the buckets for that user across all the clusters
    def get_user_bucket(self,connHandle, user):
        # returns the list of buckets for user across all cluster
        # pass a list to get buckets for specific users else it returns for all users
        user_bucket_map = {}
        if user not in user_bucket_map:
        	user_bucket_map[user] = []
	#Append the Per User Buckets 
	for bucket in connHandle.get_bucket(uid=user):
        	if bucket not in user_bucket_map[user]:
                	user_bucket_map[user].append(bucket)
        return user_bucket_map
    
#API Called for Updating the Users info into the Redis    
    def get_user_info(self, connHandle, userName):
    	userinfo = connHandle.get_user(userName)
	return userinfo
    
    def get_users(self,rgw_conn,user_list_update=None):
        user_info = {}
        if user_list_update is not None:
        	users_list = list(set(user_list_update).intersection(set(rgw_conn.get_users())))
        else:
                users_list = rgw_conn.get_users()
        for user in users_list:
        	if user not in user_info:
                    user_detail = self.get_user_info(rgw_conn,user)
                    user_info[user] = {"access_key": user_detail["keys"][0]["access_key"]}
        return user_info
   
    def update_bucket_info(self,connHandle, target_cluster,user):
   	user_bucket_map = self.get_user_bucket(connHandle, user)
        userinfo = self.get_user_info(connHandle, user)
        print userinfo
        for bucket in user_bucket_map[user]:
       		key = bucket + "_buck_agl"
                hash_val = {
                	"active": target_cluster,
                        "active_sync": target_cluster,
                        "state": "200",
                        "opmode": "105",
                        "epoch": "3",
                        "version": "1.0",
                        "access_key" : userinfo["keys"][0]["access_key"],
                        "replication_enabled" : "1"
                } 
		self.redis.hmset(key, hash_val)
			
    def update_user_info(self,target_cluster,user_list=None):
	clusters = self.get_clusters()
        rgw_conn = None
	for cluster in clusters:
            cluster_info = clusters[cluster]
            rgw_conn = self.get_rgw_conn(cluster_info["access_key"],cluster_info["secret_key"],cluster_info["host"])
       
	print "Fetching the changes to be made"
        user_info = self.get_users(rgw_conn, user_list_update=user_list)
        for user in user_info:
            userhash = {
                "epoch": "3",
                "version": "1.0",
                "target_write_cluster": target_cluster,
                "user_name" : user,
                "placement_policy" : "Default",
                "bootstrap_cluster" : target_cluster
            }
            userkey = user_info[user]["access_key"]+"_user_agl"
            print "Updating User -->", user
            print "Updating User Key as -->", userkey
            r = self.redis.hmset(userkey, userhash)
            print "Updating User Result-->", r
            self.update_bucket_info(rgw_conn, target_cluster,user)

        print "Exiting"


        
