simple_config_template = '''# simple config for single ob instance
ob1 = OBI()
'''
config_template = '''## Config File for 'deploy.py', this file is just a valid Python file.
## Note: the servers' port will be determined by the 'OBI's definition order in this config file,
##       unless your explict configure them. (that means Don't change 'OBI's definition order if not necessary)

## comment line below if you need to run './deploy.py ob1.random_test ...'
# load_file('monitor.py', 'fault_test.py')
data_dir = '/data/'         # $data_dir/{1..10} should exist
## comment line below if you want to provide custom schema and server conf template(see ob5's definition for another way)
## run: './deploy.py tpl.gensvrcfg' to generate a 'tpl' dir, edit tpl/rootserver.template... as you wish
# tpl = load_dir_vars('tpl')  # template of config file such as rootserver.conf/updateserver.conf...

# list ip of test machines
ObCfg.default_hosts = '10.232.36.31 10.232.36.32 10.232.36.33'.split()

ob1 = OBI(ct=CT('mixed_test'))
ob2 = OBI(masters = ObCfg.default_hosts[:2], ct=CT('syschecker'))
ob3 = OBI('OBI_MASTER', max_sstable_size=4194304, masters = ObCfg.default_hosts[:2], ct=CT('bigquery', hosts = ObCfg.default_hosts[:1]))
mysql = {
"ip" : "10.232.36.187",
"port" : "3306",
"user" : "sqltest",
"pass" : "sqltest",
"db" : ObCfg.usr.replace('.', '_')
}
ob4 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:2], ct=CT('sqltest', mysql=mysql, hosts = ObCfg.default_hosts[:2]))

## explict config each server and their attributes
rs0_ip = '10.232.36.31'
ups0_ip = '10.232.36.29'
cs0_ip = '10.232.36.31'
ms0_ip = '10.232.36.31'
cli0_ip = '10.232.36.33'

## config 'port', 'version' and 'schema' etc, no 'client'
## Note: if you set 'ver', updateserver/rootserver/... should be in dir 'bin.$ver'
ob5 = OBI('OBI_MASTER', ver='.30',
          ## comment lines below to explit configure default servers' ports using by this 'OBI'
          # rs_port = 2544,
          # ups_port = 2644,
          # ups_inner_port = 2744,
          # cs_port = 2844,
          # ms_port = 2944,
		    tpl = dict(schema_template=read('yubai.schema')),
                    rs0 = RootServer(rs0_ip),
                    ups0 = UpdateServer(ups0_ip, replay_checksum_flag=0),
                    cs0 = ChunkServer(cs0_ip),
                    ms0 = MergeServer(ms0_ip),
                    ## comment line below to explict configure 'ms1' port
                    # ms1 = ChunkServer(ms0_ip),
)

## config two OBI, UPS sync using lsync
ob6 = OBI('OBI_SLAVE', ver='.21',
                    rs0 = RootServer(rs0_ip),
                    ups0 = UpdateServer(ups0_ip, lsync_ip=ups0_ip, lsync_port=3046),
                    cs0 = ChunkServer(cs0_ip),
                    ms0 = MergeServer(ms0_ip),
		    lsync0 = LsyncServer(ups0_ip, port=3045, lsync_retry_wait_time_us=100000, log_level='debug'),
		    ct = CT('client_simulator', hosts = [cli0_ip]))
ob7 = OBI('OBI_MASTER', ver='.30',
		    lsyc_port = 3046,
                    rs0 = RootServer(rs0_ip),
                    ups0 = UpdateServer(ups0_ip, lsync_ip= ups0_ip, lsync_port = 3045),
		    lsync0 = LsyncServer(ups0_ip, port=3046, convert_switch_log=1, lsync_retry_wait_time_us=100050, log_level='debug'),
                    cs0 = ChunkServer(cs0_ip),
                    ms0 = MergeServer(ms0_ip),
		    ct = CT('client_simulator', hosts = [cli0_ip]))
'''

detail_config_template = '''# just see 'config' dumped by running: ./deploy.py tpl.gencfg'''

schema_template = '''
[app_name]
name=${app_name}
max_table_id=1999

'''

rootserver_template = '''
${{
inst1 = sub2('$inst1', locals())
inst2 = sub2('$inst2', locals())
if inst1 != '$inst1' and inst2 != '$inst2':
    _ob_instances_cfg = """obi_count=2
obi0_rs_vip=${%(inst1)s.rs0.ip}
obi0_rs_port=${%(inst1)s.rs0.port}
obi0_read_percentage=30
obi0_random_ms = 0
obi1_rs_vip=${%(inst2)s.rs0.ip}
obi1_rs_port=${%(inst2)s.rs0.port}
obi1_read_percentage=70
obi1_random_ms = 0""" % dict(inst1=inst1, inst2=inst2)
else:
    _ob_instances_cfg = """obi_count=1
obi0_rs_vip=${rs0.ip}
obi0_rs_port=${rs0.port}
obi0_read_percentage=30
obi0_random_ms = 0"""
}}
[ob_instances]
${_ob_instances_cfg}
[root_server]
pid_file=log/rootserver.pid.$ip:$port
log_file=log/rootserver.log.$ip:$port
data_dir=data/rs
log_level=$log_level
trace_log_level=$trace_log_level
dev_name=$dev
vip=${rs0.ip}
port=${rs0.port}

thread_count=20
read_queue_size=500
write_queue_size=50

log_queue_size=50
network_timeout=1000000
migrate_wait_seconds=1

log_dir_path=data/rs_commitlog
log_size=64

state_check_period_us=500000
replay_wait_time_us=5000
log_sync_limit_kb=51200
log_sync_type=1
register_times=10
register_timeout_us=2000000

lease_on=1
lease_interval_us=15000000
lease_reserved_time_us=10000000
cs_command_interval_us=1000000
__create_table_in_init=1
__safe_copy_count_in_init=1

[update_server]
vip=${ups0.ip}
port=${ups0.port}
ups_inner_port=${ups0.inner_port}
lease_us=9000000
lease_reserved_us=8500000
ups_renew_reserved_us=7770000
waiting_reigster_duration_us=2000000

[schema]
file_name=${schema}

[chunk_server]
lease=10000000
switch_hour=23
disk_high_level=85
disk_trigger_level=75
shared_adjacnet=10
safe_lost_one_duration=28800
wait_init_duration=1
max_merge_duration=14400
cs_probation_period=1
max_concurrent_migrate_per_cs=2
'''

updateserver_template = '''
[public]
log_file = log/updateserver.log.$ip:$port
pid_file = log/updateserver.pid.$ip:$port
log_level = $log_level
trace_log_level=$trace_log_level

[update_server]
vip = $ip
port = $port
ups_inner_port = $inner_port
dev_name = $dev
instance_master_ip = $master_ip
instance_master_port = $master_port
lsync_ip = $lsync_ip
lsync_port = $lsync_port
app_name = $app_name
log_size_mb = $log_size
log_dir_path = data/ups_commitlog
fetch_schema_times = 10
fetch_schema_timeout_us = 3000000
resp_root_times = 10
resp_root_timeout_us = 1000000
log_sync_type = $log_sync_type
# 0.2 without us, 0.3 has
lsync_fetch_timeout = 1000000
lsync_fetch_timeout_us = 1000000
read_thread_count = 14
store_thread_count = 4
read_task_queue_size = 1000
write_task_queue_size = 1000
log_task_queue_size = 1000

write_group_commit_num = 1024

replay_checksum_flag = ${replay_checksum_flag}
log_sync_limit_kb = 51200
replay_wait_time_us = 5000
state_check_period_us = 500000
log_sync_delay_warn_time_threshold_us=500001
max_n_lagged_log_allowed=10000
log_sync_delay_warn_time_report_interval_us=10000000
replay_wait_time_us = 50000
fetch_log_wait_time_us=500000

using_hash_index=1
#total_memory_limit = 9
total_memory_limit = ${total_memory_limit}
#table_memory_limit = 4
table_memory_limit = ${table_memory_limit}
lease_interval_us = 15000000
lease_reserved_time_us = 10000000
lease_on = 1
log_sync_timeout_us = 500000
log_sync_retry_times = 2
max_thread_memblock_num = 10
packet_max_timewait = 1000000
transaction_process_time_warning_us = 1000000
store_root = data/ups_data
raid_regex = ^raid[0-9]+$
dir_regex = ^store[0-9]+$

warm_up_time_s = ${warm_up_time_s}

#blockcache_size_mb = 512
blockcache_size_mb = ${blockcache_size_mb}
blockindex_cache_size_mb = ${blockindex_cache_size_mb}

#active_mem_limit_gb = 1
active_mem_limit_gb = ${active_mem_limit_gb}
minor_num_limit = ${minor_num_limit}
#frozen_mem_limit_gb = 1
#frozen_mem_limit_gb = ${frozen_mem_limit_gb}
table_available_warn_size_gb = 3
table_available_error_size_gb = 2
sstable_time_limit_s = 604800
major_freeze_duty_time = -1
min_major_freeze_interval_s = 1

sstable_compressor_name = none
sstable_block_size = 4096

[root_server]
vip = ${rs0.ip}
port = ${rs0.port}
'''

mergeserver_template = '''
[merge_server]
port=$port
dev_name=$dev
task_queue_size=512
task_thread_count=50
network_timeout_us=3000000
location_cache_size_mb=32
location_cache_timeout_us=600000000
log_file=log/mergeserver.log.$ip:$port
pid_file=log/mergeserver.pid.$ip:$port
log_level=$log_level
trace_log_level=$trace_log_level
max_log_file_size=1024
intermediate_buffer_size_mbyte=8
memory_size_limit_percent=15
upslist_interval_us=1000000
max_req_process_time_us = 10000000

[obmysql]
obmysql_port=$mysql_port

[root_server]
vip=${rs0.ip}
port=${rs0.port}
'''

chunkserver_template = '''
[public]
pid_file = log/chunkserver.pid.$ip:$port
log_file = log/chunkserver.log.$ip:$port
log_level = $log_level
trace_log_level=$trace_log_level

[chunk_server]
dev_name = $dev
port = $port
task_queue_size = 1000
task_thread_count = 50
max_migrate_task_count=4
max_sstable_size = $max_sstable_size

datadir_path = $dir/data/cs
application_name = $app_name
network_timeout= 1000000
merge_timeout=10000000

lease_check_interval=5000000
retry_times=5
migrate_band_limit_kbps=51200

merge_mem_limit=67108864
merge_thread_per_disk=$merge_thread_per_disk
#reserve_sstable_copy=2
merge_load_threshold_high=20
merge_threshold_request_high=3000
max_merge_thread_num=10

max_version_gap = 100
#max_version_gap = 3

write_sstable_io_type = 1
merge_delay_interval_minutes = 0
merge_delay_for_lsync_second = 10
min_merge_interval_second = 10
min_drop_cache_wait_second = 10

upslist_interval_us=1000000

each_tablet_sync_meta = 0
lazy_load_sstable = 1
unmerge_if_unchanged = 1
join_batch_count = 3000

[memory_cache]
block_cache_memsize_mb = 512
file_info_cache_max_cache_num = 8192
block_index_cache_memsize_mb = 1024
join_cache_memsize_mb=4096
sstable_row_cache_memsize_mb = 512

[root_server]
vip = ${rs0.ip}
port = ${rs0.port}
'''

lsyncserver_template = '''
[public]
log_file = ./log/lsyncserver.log.$ip:$port
pid_file = ./log/lsyncserver.pid.$ip:$port
log_level = $log_level

[lsync_server]
convert_switch_log=$convert_switch_log
dev_name=$dev
port=$port
ups_commit_log_dir= data/ups_commitlog
ups_commit_log_file_start_id=$start_file_id
timeout=1000
lsync_retry_wait_time_us=${lsync_retry_wait_time_us}
'''
proxyserver_template = '''
'''
monitorserver_template = '''
ip=$ip
port=$port
log_file=$role.log.$ip.$port
'''
new_table_template = '''
[table$table_id]
table_id=$table_id
table_type=1
max_column_id=128

column_info=1,4,column_2,varchar,32
column_info=1,5,column_3,varchar,32
column_info=1,6,column_4,varchar,32
column_info=1,7,column_5,int
column_info=1,8,column_6,int
column_info=1,9,column_7,int

column_info=1,2,c_time,create_time
column_info=1,3,m_time,modify_time
column_info=1,48,seed_column,int
column_info=1,49,rowkey_info,int
column_info=1,50,cell_num,int
column_info=1,51,suffix_length,int
column_info=1,52,suffix_num,int
column_info=1,53,prefix_end,int

rowkey_split=20
rowkey_max_length=40
rowkey_is_fixed_length=0
'''

def gensvrcfg(dir='tpl', **kw):
    err = sh('mkdir -p %s'%(dir))
    err_list = [write('%s/%s'%(dir, k.replace('_', '.')), v) for k, v in ObCfg.tpl.items() if re.search('_template', k)]
    return dir

def gencfg(path=None, detail=False, **kw):
    global config_template, detail_config_template
    template = detail and detail_config_template or config_template
    if type(path) == str:
        write(path, template)
    else:
        print '#--------------------begin config template--------------------'
        print template
        print '#---------------------end config template---------------------'
    return path
