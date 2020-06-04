import redis
r = redis.StrictRedis(host='XXXXXX', port=6380, db=0, password='XXXXXXXX')

#ssa_a
cluster={}
cluster['epoch'] = 1
cluster['version'] = '1.0'
cluster['cluster_read_endpoint'] = 'XXXXXXXXXXX'
cluster['cluster_write_endpoint'] = 'XXXXXXX'
cluster['cluster_state'] = '600'
cluster['fallback_cluster'] = 'cluster_name_fallback'
key = "cluster_a_cluster_agl"
res = r.hmset(key,cluster)
print key 
print cluster
print res













