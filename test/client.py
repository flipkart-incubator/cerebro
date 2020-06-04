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
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#Create a  file handler
handler = logging.FileHandler('pyclient.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

#Due to Pip related issues
#from rgwadmin import RGWAdmin
#from filechunkio import FileChunkIO


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


ctr=0
def startObjectTest(s3_conn,size):
    global ctr
    logger.info("Running Test for Iteration -> "+str(ctr))
    ctr = ctr + 1
    
    objectDict = {}

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
            with open(objectDict[key], "wb") as fout:
                 fout.write(os.urandom(size))
        except Exception, e:
            logger.error("Caught Exception in Creating Data Objects")
            logger.error(e)
    
    bucket = s3_conn.get_bucket(bucket_name)
    
    #Upload the File to Cluster
    for key in objectDict.keys():
        time.sleep(3)
        try:
            obj_name = objectDict[key]
            objkey = bucket.new_key(obj_name)
            objkey.set_contents_from_filename(obj_name)
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
        time.sleep(4)
        try:
            objkey = bucket.get_key(objectDict[key])
            toFileName = 'getobjs/'+objectDict[key]
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
                logger.debug("MD5 Matches")
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
        logger.error("Bucket Deleted -> " + bucket_name)
    except Exception, e:
        logger.error("Exception while Deleting bucker ->" + bucket_name)
        logger.error(e)


if __name__ == "__main__": 
    try:
         s3_conn = boto.connect_s3(aws_access_key_id='XXXXXXXXXXXXXXXXXXXXX',
                                   aws_secret_access_key='XXXXXXXXXXXx', 
                                   host="XXXXXXXXXX", 
                                   is_secure=False, 
                                   calling_format=boto.s3.connection.OrdinaryCallingFormat())
         print s3_conn 
         while True:
             #Do Forever or Until Conditions Breaks
             startObjectTest(s3_conn,random.randint(1000,100000))
             startObjectTest(s3_conn,random.randint(1000000,10000000))
             #startObjectTest(s3_conn,randomint(10000000,200000000))
             #break
    except Exception, e:
        print("Caught Exception in Main")
        print e
