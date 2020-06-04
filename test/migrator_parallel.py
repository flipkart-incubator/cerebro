import boto
import boto.s3.connection
import random
from boto.s3.key import Key
import logging
import time
import os
import math
import string
import random
import copy
import hashlib
from Queue import Queue
from threading import Thread
from boto.s3.cors import CORSConfiguration
import json
import threading
import sys
reload(sys)
sys.setdefaultencoding('utf8')

#Boto Stream Debugger Used Dump the Stream request and Response
#Uncomment the next line for debug purposes or use lower logging level
#boto.set_stream_logger('boto3.resources', logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#Create a  file handler
handler = logging.FileHandler('multiclient_parallel.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


#Due to Pip related issues
#from rgwadmin import RGWAdmin
from filechunkio import FileChunkIO

#Class Encapsulating the Task Queue, where the Task Will be Queued via Bucket Listing
class TaskQueue(object):
    def __init__(self):
         self._q=Queue(1024)
         #self._q = queue


    def enq(self, task):
        try:
            self._q.put(task)
        except Exception as e:
            logger.error(e)

    def deq(self):
        taskObject = self._q.get()
        return taskObject


#Object Tasks That Will be Parsed
class ObjectTask(object):
    def __init__(self,task_id):
        self._task_id=task_id
        self._key_marker_start=""
        self._key_marker_end=""
        self._key_marker_next_itr=""
        self._bucket_name=""
        self._num_keys=0
        self._num_keys_pass=0
        self._num_keys_failed=0

class PoolWorker(Thread):
    def __init__(self,queue):
        super(PoolWorker,self).__init__()
        self._q=queue
        
        # Start as a BackGround Thread
        self.daemon=True
        self.start()

    #Overridden Run Method
    def run(self):
        logger.info('Starting Thread')
        #Run Forever
        while True:
            #Block on the Queue for Some Task, Queue is Synchronized (thread safe)
            func,arg=self._q.get()
            
            try:
                #Execute the Task
                func(*arg)
            except Exception as e:
               logger.error(e)

"""
   Class Defining a Simple ThreadPool. 
   The Queue Size is init at the time of creation of the pool
   The Enqueue Thread Will Block if the Queue if Full,
   automatically backpressuring the system
   Current Queue Size = 4, could be changed when system is tested 
   across various conigurations
"""
     
class ThreadPool(object):
    def __init__(self,num_th):
        self._q=Queue(num_th)
        for _ in range(num_th):
            PoolWorker(self._q)

    def add_task(self,func,args):
        try:
            #Queue the Task 
            self._q.put((func,args))
        except Exception as e:
            logger.error(e)

def listObjects_and_enq_task(s3_conn_src, s3_conn_dst, bucket_name,tq):
    logger.info("Starting the Bucket Listing for Bucket Name -->" + str(bucket_name))
    buckethandle = s3_conn_src.get_bucket(bucket_name)
    ctr = 0
    marker=""
    try:
         while True:
             time.sleep(1)
             keys = buckethandle.get_all_keys(max_keys=1000,marker = marker)
             #logger.info("Type is --> " + str(type(keys)))
             keys_count = len(keys)
             start_marker = keys[0].name
             end_marker = keys[keys_count-1].name
             
             #Global Counter for Incrementing the Task ID
             ctr = ctr + 1             
             
             #Populated the Task
             objtask = ObjectTask(ctr)
             objtask.bucket_name = bucket_name
             objtask.key_marker_start = start_marker
             objtask.key_marker_end = end_marker
             objtask.key_marker_next_itr = marker
             objtask.num_keys = keys_count
             objtask.key_marker_next_itr = marker
             
             #Update the Marker for the Next Cycle 
             marker = end_marker

             try:
                  logger.info("Bucket Name -->\t" + str(bucket_name) + "\tStart -->\t" + str(start_marker) + "\tEnd-->\t" + str(end_marker) + "\tCount-->\t" + str(keys_count))                
                  logger.info("Pushing Task to Global Task Queue")
                  #handle_thPool.add_task(copyObject,[s3_conn_src,s3_conn_dst,bkt_name,])
                  tq.enq(objtask)
             except Exception, e:
                 logger.error("Error While Downloading and Uploading file\t" +k.name)
                 logger.error(e)
             
             if keys.is_truncated is False:
                 break
    except Exception, e:
         logger.error("Outer Exception for Bucket Name" + bucket_name)
         logger.error(e)

#Inteanl API, which uses objtask to copy the objects
def copyObjectInternal(objtask):
     #Decompose the Object Task Handle into its consistutents
     logger.info("COPY Object INternal")
     bucket_name = objtask.bucket_name
     bucket_start_marker = objtask.key_marker_start
     bucket_end_marker = objtask.key_marker_end
     bucket_key_marker_next_itr = objtask.key_marker_next_itr
     key_count=0 
     failed_keys=0
     skipped_keys=0
     logger.info("Starting Copy of Bucket -->" + bucket_name)
     try:
         buckethandle = s3_conn_src.get_bucket(bucket_name)
         buckethandledst = s3_conn_dst.get_bucket(bucket_name)

         logger.info("Bucket Handle Source -->" + str(buckethandle))
         logger.info("Bucket Handle Destination -->" + str(buckethandledst))
     except  Exception, e:
         logger.error("Exception in Copying Getting Bucket" + bucket_name)
         logger.error(e)
         return
     marker=bucket_key_marker_next_itr
     try:
         thread_name = threading.currentThread().getName() 
         #while True:
         keys = buckethandle.get_all_keys(max_keys=1000,marker = marker)
         for k in keys:
             #print "Keys is -->" + str(k.name)
             try:
                 #Src Object Handle
                 key_count = key_count + 1
                 objhandle = buckethandle.get_key(k.name)
                 mdata = objhandle.metadata
                 #Copying to a File and Reading Back Again to a Object
                 #objhandle.get_contents_to_filename("tmpdata"+k.name)
                 
                 #For > 3 GB MB use MPU
                 if k.size > 3769882852 :
                     logger.info(thread_name + "\tVery Large Object Found-->\t" +k.name + "\tFor Bucket -->\t" + bucket_name + "\tSize\t" + str(k.size))
                     marker = k.name
                     continue

                 #Update the Marker
                 marker = k.name
                 #Get tHe Dst Ket
                 dst_key=buckethandledst.get_key(k.name)

                 if dst_key == None:
                     #Copy the Object to a Local Buffer
                     data_str = objhandle.get_contents_as_string()
                     
                     #Destrination Bucket and Object Handle
                     objhandleupload=buckethandledst.new_key(k.name)
                     #logger.info(threading.currentThread().getName() + "Copy Object ---> " + k.name + str(k.size))
                     #Pass Header Oprion of 'rgwx-copy-if-newer':'true''
                     #This option causeses the RGW not to Override the object (if name same), if it is newer
                     res = objhandleupload.set_contents_from_string(data_str, headers = {'rgwx-copy-if-newer': 'true'})
                 
                     try:
                         #Update the CORS and ACL as well for the object
                         #CORS can only be Updated on the Bucket
                        objhandleupload.set_acl(objhandle.get_acl())
                     except Exception, e:
                         logger.error("Error While Setting ACL \t"+k.name)
                         logger.error(e)
                 else:      
                     skipped_keys = skipped_keys + 1
                     logger.info("Key Collision Detected\t" + "DST\t" + dst_key.name + "\t"+ dst_key.etag + "\t" +dst_key.last_modified + "\t" +str(dst_key.size) + "\t" + "SRC\t" + objhandle.name + "\t" + objhandle.etag + "\t" + objhandle.last_modified + "\t" + str(objhandle.size))

                     #Remove the File if created using get_contents_to_file
                     #os.remove("tmpdata"+k.name)
             except Exception, e:
                 logger.error("Error While Downloading and Uploading file\t" +k.name)
                 logger.error(e)
                 failed_keys = failed_keys + 1
         """
         if keys.is_truncated is False:
             logger.info("Finished Copied Objects -->\t" +k.name + "\tFor Bucket -->\t" + bucket_name + "\tObject Count -->\t" + str(key_count)) 
             break
         """
         logger.info(thread_name + "\tCopied Objects Till -->\t" +k.name + "\tFor Bucket -->\t" + bucket_name + "\tObject Count -->\t" + str(key_count)) 
         time.sleep(1)
     except Exception, e:
         logger.error("Outer Exception for Bucket Name" + bucket_name)
         logger.error(e)
     logger.info(thread_name + "\tSummary : Bucket Name -->\t" + bucket_name + "\tTotal Keys -->\t" + str(key_count) + "\tFailed Keys -->\t" + str(failed_keys))

def copyObject(tq):
     while True:
         try:
             logger.info("Waiting to Deqeue")
             objtask = tq.deq()
             logger.info("Deqeued")
             copyObjectInternal(objtask)
         except Exception,e :
             logger.error("Error While Calling copy Object INternal")
             logger.error(e)


def createBucket(s3_conn_src, src_conn_dst, bucket_name):
     print ("Copy Bucket" + bucket_name)
     try:
         buckethandledst = s3_conn_dst.create_bucket(bucket_name)
         print buckethandledst
     except  Exception, e:
         print "Exception in Creating Bucket"
         print "Exception in Creating Bucket"

if __name__ == "__main__": 
    #bucketname=sys.argv[1]
    #bucketname=str(bucketname)
    try:
        
        s3_conn_src = boto.connect_s3(aws_access_key_id="XXXXXXXXX",
                                 aws_secret_access_key="XXXXXXXx", 
                                 host="XXXXXXXXxx", 
                                 is_secure=False, 
                                 calling_format=boto.s3.connection.OrdinaryCallingFormat())
        s3_conn_dst = boto.connect_s3(aws_access_key_id="XXXXXXXXXXX",
                                 aws_secret_access_key="XXXXXXXXXXXXXxx", 
                                 host="XXXXXXXX", 
                                 is_secure=False, 
                                 calling_format=boto.s3.connection.OrdinaryCallingFormat())
        print s3_conn_src
        print s3_conn_dst
        handle_thPool=ThreadPool(1)
        tq=TaskQueue()
        taskPool=ThreadPool(5)
        file1 = open('bkt.txt', 'r')
        Lines = file1.readlines() 
        
        #Init The Task pool which will, copy the buckets
        taskPool.add_task(copyObject,[tq,])
        taskPool.add_task(copyObject,[tq,])
        taskPool.add_task(copyObject,[tq,])
        taskPool.add_task(copyObject,[tq,])
        taskPool.add_task(copyObject,[tq,])
        """
        taskPool.add_task(copyObject,[])
        taskPool.add_task(copyObject,[])
        """
        for line in Lines:
            bkt_name = line.strip()
            print bkt_name
            #Do Forever or Until Conditions Breaks
            #Get the Listing of the Buckets and Enqueue to Global Queue
            handle_thPool.add_task(listObjects_and_enq_task,[s3_conn_src,s3_conn_dst,bkt_name,tq,])

        time.sleep(8640000)
    except Exception, e:
        print("Caught Exception in Main")
        print e
