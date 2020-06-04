import sys
from redis_update import RedisUpdate

# usage e.g. python run_bucket_update.py <ip> <port> <user>
# pass user list to update_keys_redis for specifi users update

redis_update_obj = RedisUpdate(sys.argv[1],sys.argv[2])
# redis_update_obj.update_keys_redis(target_cluster=sys.argv[3]) 
# print redis_update_obj.get_users()
redis_update_obj.update_user_info(target_cluster=sys.argv[3])
