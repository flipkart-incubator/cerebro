import boto
import boto.s3.connection
from rgwadmin import RGWAdmin
import datetime
import logging
import schedule
import time

logger = logging.getLogger('sync_log')
hdlr = logging.FileHandler('sync.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

user='user-name'


#Define the RGW Super User Connections

rgw_src= RGWAdmin(access_key='XXXXXXX', secret_key='XXXXXXXXX',
               server='XXXXX', secure=False) #Add keys here of rgwadmin admin account
rgw_dst = RGWAdmin(access_key='XXXXXXX', secret_key='XXXXXXXXXX',
               server='XXXXXXXx', secure=False) #Add keys here of rgwadmin admin account

def reconcile():

    
    bucket_src_dict = {}
    bucket_dst_dict = {}
 
    dBuckets = rgw_src.get_bucket(uid=user, stats=True)
    for bucket in dBuckets:
        #print bucket["usage"]["rgw.main"]["num_objects"]
        if 0:
            print bucket["bucket"] 
        
        bucket_src_dict[bucket["bucket"]] = bucket["usage"]["rgw.main"]["num_objects"]
    
    dBuckets = {}
 
    dBuckets = rgw_dst.get_bucket(uid=user, stats=True)
    for bucket in dBuckets:
        #print bucket["usage"]["rgw.main"]["num_objects"]
        
        if 0:
            print bucket["bucket"] 
        
        if len(bucket["usage"]) == 0:
            continue
        if bucket["usage"]["rgw.main"]:
            bucket_src_dict[bucket["bucket"]] = bucket["usage"]["rgw.main"]["num_objects"]

    
    for entry in bucket_src_dict:
         src_objs = 0 
         dst_objs = 0
         
         # If Migration is in Progress, some buckets may not have been synced
         if entry in bucket_src_dict:
             src_objs = bucket_src_dict[entry]
             dst_objs = bucket_dst_dict[entry]
             if src_objs == dst_objs:
                  if 0:
                      print entry + "    RECONCILE SUCCESS"
             else:
                 print entry + "    RECONCILE FAILED" + "  SRC : " + str(src_objs) + "  DST : " + str(dst_objs) + "  DIFF : " + str(src_objs - dst_objs)
         else:
             print "Bucket Not Found At Target -->" + entry
reconcile()


