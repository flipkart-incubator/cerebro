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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#Create a  file handler
handler = logging.FileHandler('multiclient.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


#Due to Pip related issues
#from rgwadmin import RGWAdmin
from filechunkio import FileChunkIO


#Initial Set of Tests
#Create a Bucket
#Write 10000 Small Objects   < 100 KB (Random)
#Write 10000 Medium Objects   < 1 - 10 MB (Random)
#Write 10000 Large Objects    < 10 - 200 MB (Random)
#Get Objetcs
#Do MD5SUM
#Delete Obejects
#Bucket Listing Should Be zero
#Extend Test Case (Create Bucket Every 30 Mins)
#Repeat the Steps
#How to Catch Error
#Intially log all the errors
#Then Start Logging all the error codes as well

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

ctr=0
mpuctr=0
ctr=0
s3_conn=0

def startMPUTest(s3_conn, s3_conn_1, size):
    global mpuctr
    logger.info("Running MPU Test for Iteration -> "+str(mpuctr))
    mpuctr = mpuctr + 1
    
    #Create The Bucket
    chars = string.ascii_lowercase + string.digits  
    pwdSize = 24
    bucket_name = ''.join((random.choice(chars)) for x in range(pwdSize))
    logger.info("Bucket Name -> "+bucket_name)
    try:
        s3_conn.create_bucket(bucket_name)
    except Exception, e:
        print e
        return
    logger.info("Bucket Created -> "+bucket_name)
    
    try:    
    	bucket = s3_conn.get_bucket(bucket_name)
    	bucket1 = s3_conn_1.get_bucket(bucket_name)
    except Exception, e:
    	logger.error("Caught Exception in Get Bucket")
    	logger.error(e)
    
    try:
        #Start Multippart Upload 
        filesize=200000000
        with open("mpuobj.data", "wb") as fout:
                     fout.write(os.urandom(200000000))
        chunksize=12000000
        chunkcount = int(math.ceil(filesize/chunksize))
        
        header = {
            'x-archive-queue-derive': '0'
        }
        
        mp = bucket.initiate_multipart_upload(os.path.basename("mpuobj.data"),headers=header)
        for i in range (chunkcount + 1):
            offset = chunksize * i
            bytes = min(chunksize, filesize - offset)
            with FileChunkIO( "mpuobj.data", 'r', offset=offset, bytes=bytes ) as fp:
                mp.upload_part_from_file( fp, part_num=i + 1, headers=header )
            logger.info("MultiPart Upload in progress for Iteration "+str(i))
        mp.complete_upload()
    except Exception, e:
        logger.error("Caught Exception in Uploading MPU Object")
        logger.error(e)
    logger.info("MultiPart Upload Done")
    
    #Get the File From Cluster
    try:
        objkey = bucket.get_key("mpuobj.data")
        toFileName = 'getobjs/'+"mpuobj.data"
        #print toFileName
        objkey.get_contents_to_filename(toFileName)
        logger.info("Got MPU File")
    except Exception, e:
        logger.error("Caught Exception in Getting file from Cluster ->"+"mpuobj.data")
    
    try:
        objkey = bucket1.get_key("mpuobj.data")
        toFileName = 'getobjs1/'+"mpuobj.data"
        #print toFileName
        objkey.get_contents_to_filename(toFileName)
        logger.info("Got MPU File 1")
    except Exception, e:
        logger.error("Caught Exception in Getting file from Cluster 1->"+"mpuobj.data")
    time.sleep(90)
    
    try:
        bucket.delete_key("mpuobj.data")
        time.sleep(30)
        s3_conn.delete_bucket(bucket)
    except Exception, e:
        logger.error("Error In Deleting MPU Object")


def startSpecialObjectTest(s3_conn, s3_conn_1, sizeObj):
    global ctr
    longstrpath = "/some/file/long/dir/dcdcdcdxxxsxs/ccxc/"
    logger.info("Start Special Running Test for Iteration -> "+str(ctr))
    ctr = ctr + 1
    objectDict = {}
    bucket=0
    bucket1=0
    for i in range (128):
        chars = string.letters + string.digits 
        pwdSize = 32
        fileName = ''.join((random.choice(chars)) for x in range(pwdSize))
        fileName = fileName + ".data"
        if i%2 == 0 :
        	fileName = fileName + "!*$ "
        else :
        	fileName = fileName + "  " + "!*&%" + "fileName"
        
        objectDict[i]=fileName

    
    #Print the Dict
    #for key in objectDict.keys():
         #print key, objectDict[key]
    
    #Create The Bucket
    chars = string.ascii_lowercase + string.digits  
    pwdSize = 24
    bucket_name = ''.join((random.choice(chars)) for x in range(pwdSize))
    logger.info("Bucket Name -> "+bucket_name)
    try:
        s3_conn.create_bucket(bucket_name)
    except Exception, e:
        logger.error("Bucket Name Create Exception -> "+bucket_name)
        logger.error(e)
        return
    logger.info("Bucket Created -> "+bucket_name)
    time.sleep(10) 
    #Create the Object Locally
    for key in objectDict.keys():
        try:
            size = random.randint(1024, 900000) 
            with open(objectDict[key], "wb") as fout:
                 fout.write(os.urandom(size))
        except Exception, e:
            logger.error("Caught Exception in Creating Data Objects")
            logger.error(e)
    try:    
    	bucket = s3_conn.get_bucket(bucket_name)
    	bucket1 = s3_conn_1.get_bucket(bucket_name)
    except Exception, e:
    	logger.error("Caught Exception in Get Bucket")
    	logger.error(e)

    
    #Upload the File to Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            obj_name = objectDict[key]
            objkey = bucket.new_key(longstrpath+obj_name)
            objkey.set_contents_from_filename(obj_name)
            logger.info("Uplaoded Special Char-->"+obj_name)
        except Exception, e:
            logger.error("Caught Exception in Uploading file to Cluster ->"+objectDict[key])
            logger.error(e)
    
    #List Bucket
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.debug(objinfo)
    
    
    #Get the File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            objkey = bucket.get_key(longstrpath+objectDict[key])
            toFileName = 'getobjs/'+objectDict[key]
            #print toFileName
            objkey.get_contents_to_filename(toFileName)
        except Exception, e:
            logger.error("Caught Exception in Getting file from Cluster ->"+objectDict[key])
            logger.error(e)
    
    #Get the File From Cluster 1
    for key in objectDict.keys():
        time.sleep(1)
        try:
            objkey = bucket1.get_key(longstrpath+objectDict[key])
            toFileName = 'getobjs1/'+objectDict[key]
            #print toFileName
            objkey.get_contents_to_filename(toFileName)
        except Exception, e:
            logger.error("Caught Exception in Getting file from Cluster ->"+objectDict[key])
            logger.error(e)
    
    #Perfom MD5sum Local and Copied File
    for key in objectDict.keys():
        try:
            #MD5 of Local Objects
            hash_md5_local = hashlib.md5()
            with open(objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_local.update(chunk)
            hash_md5_local_digest = hash_md5_local.hexdigest()
            
            #Md5 of Remote Objects
            hash_md5_remote = hashlib.md5()
            with open('getobjs/'+objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_remote.update(chunk)
            hash_md5_remote_digest = hash_md5_remote.hexdigest()

            if hash_md5_local_digest == hash_md5_remote_digest :
                logger.debug("MD5 Matches for Special")
            else:
                logger.error("MD5 MISMATCH for Object-> "+objectDict[key])
                logger.error("Local MD5-> "+ hash_md5_local_digest)
                logger.error("Remote MD5-> "+ hash_md5_remote_digest)

        except Exception, e:
            logger.error("Exception While Handling Md5SUM")
            logger.error(e)
    
    #Perfom MD5sum Local and Copied File on 1
    for key in objectDict.keys():
        try:
            #MD5 of Local Objects
            hash_md5_local = hashlib.md5()
            with open(objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_local.update(chunk)
            hash_md5_local_digest = hash_md5_local.hexdigest()
            
            #Md5 of Remote Objects
            hash_md5_remote = hashlib.md5()
            with open('getobjs1/'+objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_remote.update(chunk)
            hash_md5_remote_digest = hash_md5_remote.hexdigest()

            if hash_md5_local_digest == hash_md5_remote_digest :
                logger.debug("MD5 Matches for Special 1")
            else:
                logger.error("MD5 MISMATCH for Object 1-> "+objectDict[key])
                logger.error("Local MD5 1 -> "+ hash_md5_local_digest)
                logger.error("Remote MD5 1-> "+ hash_md5_remote_digest)

        except Exception, e:
            logger.error("Exception While Handling Md5SUM")
            logger.error(e)

    
    #Delete File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            bucket.delete_key(longstrpath+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Deleting Object")
            logger.error(e)
    

    #Remove the File Locally
    for key in objectDict.keys():
        try:
            os.remove(objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object")
            logger.error(e)
    
    #Remove the File From GetObj Dir
    for key in objectDict.keys():
        try:
            os.remove('getobjs/'+objectDict[key])
            os.remove('getobjs1/'+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object from getObjs")
            logger.error(e)
    
    #List Bucket
    logger.debug("Listing Buckets Should Be Empty for -> " + bucket_name)
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.error("ERRORED: Object Found" + objinfo)
    
    #Finally Remove the Bucket
    try:
        s3_conn.delete_bucket(bucket_name)
        logger.error("Bucket Deleted -> " + bucket_name)
    except Exception, e:
        logger.error("Exception while Deleting bucker ->" + bucket_name)
        logger.error(e)


def startObjectTest(s3_conn, s3_conn_1, sizeObj):
    global ctr
    logger.info("Start Object TEst Running Test for Iteration -> "+str(ctr))
    ctr = ctr + 1
    objectDict = {}
    bucket=0
    bucket1=0
    for i in range (128):
        chars = string.letters + string.digits 
        pwdSize = 32
        fileName = ''.join((random.choice(chars)) for x in range(pwdSize))
        fileName = fileName + ".data"
        objectDict[i]=fileName

    
    #Print the Dict
    #for key in objectDict.keys():
         #print key, objectDict[key]
    
    #Create The Bucket
    chars = string.ascii_lowercase + string.digits  
    pwdSize = 24
    bucket_name = ''.join((random.choice(chars)) for x in range(pwdSize))
    logger.info("Bucket Name -> "+bucket_name)
    try:
        s3_conn.create_bucket(bucket_name)
    except Exception, e:
        logger.error("Bucket Name Create Exception -> "+bucket_name)
        logger.error(e)
        return
    logger.info("Bucket Created -> "+bucket_name)
    
    #Create the Object Locally
    for key in objectDict.keys():
        try:
            size = random.randint(1024, 900000) 
            with open(objectDict[key], "wb") as fout:
                 fout.write(os.urandom(size))
        except Exception, e:
            logger.error("Caught Exception in Creating Data Objects")
            logger.error(e)
    try:    
    	bucket = s3_conn.get_bucket(bucket_name)
    	bucket1 = s3_conn_1.get_bucket(bucket_name)
    except Exception, e:
    	logger.error("Caught Exception in Get Bucket")
    	logger.error(e)

    
    #Upload the File to Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            obj_name = objectDict[key]
            objkey = bucket.new_key(obj_name)
            objkey.set_contents_from_filename(obj_name)
            logger.info("Uploaded Object "+obj_name)
        except Exception, e:
            logger.error("Caught Exception in Uploading file to Cluster ->"+objectDict[key])
            logger.error(e)
    
    #List Bucket
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.debug(objinfo)
    
    
    #Get the File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            objkey = bucket.get_key(objectDict[key])
            toFileName = 'getobjs/'+objectDict[key]
            #print toFileName
            objkey.get_contents_to_filename(toFileName)
            logger.info("GOt Object from  "+obj_name)
        except Exception, e:
            logger.error("Caught Exception in Getting file from Cluster ->"+objectDict[key])
            logger.error(e)
    
    #Perfom MD5sum Local and Copied File
    for key in objectDict.keys():
        try:
            #MD5 of Local Objects
            hash_md5_local = hashlib.md5()
            with open(objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_local.update(chunk)
            hash_md5_local_digest = hash_md5_local.hexdigest()
            
            #Md5 of Remote Objects
            hash_md5_remote = hashlib.md5()
            with open('getobjs/'+objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_remote.update(chunk)
            hash_md5_remote_digest = hash_md5_remote.hexdigest()

            if hash_md5_local_digest == hash_md5_remote_digest :
                logger.info("MD5 Matches for ->" + objectDict[key])
            else:
                logger.error("MD5 MISMATCH for Object-> "+objectDict[key])
                logger.error("Local MD5-> "+ hash_md5_local_digest)
                logger.error("Remote MD5-> "+ hash_md5_remote_digest)

        except Exception, e:
            logger.error("Exception While Handling Md5SUM")
            logger.error(e)

    
    #Delete File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            bucket.delete_key(objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Deleting Object")
            logger.error(e)
    

    #Remove the File Locally
    for key in objectDict.keys():
        try:
            os.remove(objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object")
            logger.error(e)
    
    #Remove the File From GetObj Dir
    for key in objectDict.keys():
        try:
            os.remove('getobjs/'+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object from getObjs")
            logger.error(e)
    
    #List Bucket
    logger.debug("Listing Buckets Should Be Empty for -> " + bucket_name)
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.error("ERRORED: Object Found" + objinfo)
    
    #Finally Remove the Bucket
    try:
        s3_conn.delete_bucket(bucket_name)
        logger.info("Bucket Deleted -> " + bucket_name)
    except Exception, e:
        logger.error("Exception while Deleting bucker ->" + bucket_name)
        logger.error(e)

def startaclcorstest(s3_conn, s3_conn_1, size):
    
    global ctr
    logger.info("Running Test for Iteration -> "+str(ctr))
    ctr = ctr + 1
    size = 16345 
    objectDict = {}

    for i in range (16):
        chars = string.letters + string.digits 
        pwdSize = 32
        fileName = ''.join((random.choice(chars)) for x in range(pwdSize))
        fileName = fileName + ".data"
        objectDict[i]=fileName

    
    #Print the Dict
    #for key in objectDict.keys():
         #print key, objectDict[key]
    
    #Create The Bucket
    chars = string.ascii_lowercase + string.digits  
    pwdSize = 24
    bucket_name = ''.join((random.choice(chars)) for x in range(pwdSize))
    logger.info("Bucket Name -> "+bucket_name)
    try:
        s3_conn.create_bucket(bucket_name)
    except Exception, e:
        logger.error("Bucket Name Create Exception -> "+bucket_name)
        logger.error(e)
        return
    logger.info("Bucket Created -> "+bucket_name)
    
    #Create the Object Locally
    for key in objectDict.keys():
        try:
            with open(objectDict[key], "wb") as fout:
                 fout.write(os.urandom(size))
        except Exception, e:
            logger.error("Caught Exception in Creating Data Objects")
            logger.error(e)
    
    bucket = s3_conn.get_bucket(bucket_name)
    bucket1 = s3_conn_1.get_bucket(bucket_name)
    
    logger.info("Now Setting the CORS CONFIG")
    try:
         cors_cfg = CORSConfiguration()
         cors_cfg.add_rule(['PUT', 'POST', 'DELETE'], 'https://www.example.com', allowed_header='*', max_age_seconds=3000, expose_header='x-amz-server-side-encryption')
         cors_cfg.add_rule('GET', '*')
         bucket.set_cors(cors_cfg)
         logger.info("Set Config CORS For Bucket Sucess ->"+bucket_name)
    except Exception, e:
         logger.error("Error in Setting Cors Config")

    logger.info("Lets Get the CORS CONFIG")
    try:
        l_cors_cfg = bucket.get_cors_xml()
        logger.info("got Cors Cfg")
        logger.info(l_cors_cfg)
    except Exception, e:
        logger.error("Error In Getting CORS CONFIG FOR BUCKET ->"+bucket_name)
    
    logger.info("Lets Get the CORS CONFIG 1")
    try:
        l_cors_cfg = bucket1.get_cors_xml()
        logger.info("got Cors Cfg1")
        logger.info(l_cors_cfg)
    except Exception, e:
        logger.error("Error In Getting CORS CONFIG FOR BUCKET ->"+bucket_name)
    
    #Upload the File to Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            obj_name = objectDict[key]
            objkey = bucket.new_key(obj_name)
            objkey.set_contents_from_filename(obj_name)
            logger.info("Suucess Uploading file to Cluster ->"+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Uploading file to Cluster ->"+objectDict[key])
            logger.error(e)
    
    #List Bucket
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.debug(objinfo)
    
    #Get the File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            objkey = bucket.get_key(objectDict[key])
            toFileName = 'getobjs/'+objectDict[key]
            #print toFileName
            objkey.get_contents_to_filename(toFileName)
            logger.info("Success  Getting file from Cluster ->"+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Getting file from Cluster ->"+objectDict[key])
            logger.error(e)
    
    #Get the File From Cluster1
    for key in objectDict.keys():
        time.sleep(1)
        try:
            objkey = bucket1.get_key(objectDict[key])
            toFileName = 'getobjs/'+objectDict[key]
            #print toFileName
            objkey.get_contents_to_filename(toFileName)
            logger.info("Success  Getting file from Cluster 1->"+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Getting file from Cluster ->"+objectDict[key])
            logger.error(e)
    
    #Peform Set Acl Canned
    ctr=0
    for key in objectDict.keys():
        try:
            if ctr%2 == 0:
                objkey = bucket.get_key(objectDict[key])
                bucket.set_acl('public-read', objkey)
                logger.info("Set PUBLIC Canned ACl for -->"+objectDict[key])
                logger.info("Begin Lookup")
                getacl = objkey.get_acl()
                ctr = ctr + 1
            #
            #for grant in getacl.acl.grants:
                #logger.info("Gpermission ->" + grant.permission)
                #logger.info("G Display Name ->" + grant.display_name)
                #logger.info("GEMail ID ->" + grant.email_address)
                #logger.info("GID ->" + grant.id)
        except Exception, e:
            logger.error("Error In Setting Canned Acls")

    logger.info("Waiting for 3")
    time.sleep(3)
    
    #Peform Set Acl Canned 1
    ctr=0
    for key in objectDict.keys():
        try:
            if ctr%2 == 0:
                objkey = bucket1.get_key(objectDict[key])
                bucket1.set_acl('public-read', objkey)
                logger.info("Set PUBLIC Canned ACl for 1 -->"+objectDict[key])
                logger.info("Begin Lookup")
                getacl = objkey.get_acl()
                logger.info("Link is-->"  + bucket_name +"/" + objectDict[key])
                ctr = ctr + 1
            #
            #for grant in getacl.acl.grants:
                #logger.info("Gpermission ->" + grant.permission)
                #logger.info("G Display Name ->" + grant.display_name)
                #logger.info("GEMail ID ->" + grant.email_address)
                #logger.info("GID ->" + grant.id)
        except Exception, e:
            logger.error("Error In Setting Canned Acls 1")
    
    time.sleep(120)

    #Perfom MD5sum Local and Copied File
    for key in objectDict.keys():
        try:
            #MD5 of Local Objects
            hash_md5_local = hashlib.md5()
            with open(objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_local.update(chunk)
            hash_md5_local_digest = hash_md5_local.hexdigest()
            
            #Md5 of Remote Objects
            hash_md5_remote = hashlib.md5()
            with open('getobjs/'+objectDict[key], "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5_remote.update(chunk)
            hash_md5_remote_digest = hash_md5_remote.hexdigest()

            if hash_md5_local_digest == hash_md5_remote_digest :
                logger.info("MD5 Matches for ->" +objectDict[key])
            else:
                logger.error("MD5 MISMATCH for Object-> "+objectDict[key])
                logger.error("Local MD5-> "+ hash_md5_local_digest)
                logger.error("Remote MD5-> "+ hash_md5_remote_digest)

        except Exception, e:
            logger.error("Exception While Handling Md5SUM")
            logger.error(e)

    
    #Delete File From Cluster
    for key in objectDict.keys():
        time.sleep(1)
        try:
            bucket.delete_key(objectDict[key])
            logger.info("Success in Deleting Object from -> " +objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Deleting Object")
            logger.error(e)
    

    #Remove the File Locally
    for key in objectDict.keys():
        try:
            os.remove(objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object")
            logger.error(e)
    
    #Remove the File From GetObj Dir
    for key in objectDict.keys():
        try:
            os.remove('getobjs/'+objectDict[key])
        except Exception, e:
            logger.error("Caught Exception in Removing Object from getObjs")
            logger.error(e)
    
    #List Bucket
    logger.debug("Listing Buckets Should Be Empty for -> " + bucket_name)
    for objkey in bucket.list():
        objinfo = "{name}\t{size}\t{modified}".format(name = objkey.name,
                                                  size = objkey.size,
                                                  modified = objkey.last_modified,
                                                 )
        logger.error("ERRORED: Object Found" + objinfo)
    """   
    try:
        logger.info("Now Trying to make the Bucket Public")
        bucket.set_acl('public-read')
        logger.info("Set the Bucket as Public")
    except Exception, e:
        logger.error("Exception in Setting Public Acl to the Bucket")
        logger.error(e)
    
    logger.info("Sleeping For Some Time Beofre Deleteing")
    """
    time.sleep(10)

    #Finally Remove the Bucket
    try:
        s3_conn.delete_bucket(bucket_name)
        logger.error("Bucket Deleted -> " + bucket_name)
    except Exception, e:
        logger.error("Exception while Deleting bucker ->" + bucket_name)
        logger.error(e)

if __name__ == "__main__": 
    try:
    
        src_conn = boto.connect_s3(aws_access_key_id='XXXXXXXXXXXX',
                                   aws_secret_access_key='XXXXXXXXXx',  host="XXXXXXXx", 
                                   is_secure=False, 
                                   calling_format=boto.s3.connection.OrdinaryCallingFormat())
        print src_conn
        
        dst_conn_1 = boto.connect_s3(aws_access_key_id='XXXXXXXXxx',
                                   aws_secret_access_key='XXXXXXXXx',  host="XXXXXXXXX", 
                                   is_secure=False, 
                                   calling_format=boto.s3.connection.OrdinaryCallingFormat())
        print dst_conn
        handle_thPool=ThreadPool(1)
        while True:
             #Do Forever or Until Conditions Breaks
             handle_thPool.add_task(startObjectTest,[s3_conn,s3_conn_1,"4000",])
             #handle_thPool.add_task(startObjectTest,[s3_conn,s3_conn_1,"4000",])
             #handle_thPool.add_task(startSpecialObjectTest,[s3_conn,s3_conn_1,"200000",])
             #handle_thPool.add_task(startMPUTest,[s3_conn,s3_conn_1,"200000",])
             #handle_thPool.add_task(startaclcorstest,[s3_conn,s3_conn_1,"200000",])
    except Exception, e:
        print("Caught Exception in Main")
        print e
