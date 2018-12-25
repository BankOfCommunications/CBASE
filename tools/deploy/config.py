# -*- coding: utf-8 -*-
## Config File for 'deploy.py', this file is just a valid Python file.
## Note: the servers' port will be determined by the 'OBI's definition order in this config file,
##       unless your explict configure them. (that means Don't change 'OBI's definition order if not necessary)

## comment line below if you need to run './deploy.py ob1.random_test ...'
#load_file('monitor.py', 'fault_test.py')
data_dir = '/home/xiaojun.chengxj/ob1/data/'         # $data_dir/{1..10} should exist
## comment line below if you want to provide custom schema and server conf template(see ob5's definition for another way)
## run: './deploy.py tpl.gensvrcfg' to generate a 'tpl' dir, edit tpl/rootserver.template... as you wish
tpl = load_dir_vars('tpl.4x')  # template of config file such as rootserver.conf/updateserver.conf...

ObCfg.all_hosts = '''10.232.36.29 10.232.36.30 10.232.36.31 10.232.23.18  10.232.36.33
 10.232.36.42 10.232.36.171 10.232.36.175 10.232.36.176 10.232.36.177'''.split()
# list ip of test machines
ObCfg.default_hosts = '10.232.23.17 '.split()

# Use HostPool(2) to alloc 2 host from ObCfg.all_hosts, You need to call ob1.reconfigure to select host
# ob1 = OBI(hosts=HostPool(2))
ob1 = OBI(ct=CT('syschecker'))
#ob1 = OBI(local_sstable_dir='./sstable')
#ob2 = OBI('OBI_MASTER', max_sstable_size=4194304, masters = ObCfg.default_hosts[:2], ct=CT('bigquery', hosts = ObCfg.default_hosts[:1]))
#ob3 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:1], ct=CT('syschecker', hosts = ObCfg.default_hosts[:1]))
mysql = {
"ip" : "10.232.36.187",
"port" : "3306",
"user" : "sqltest",
"pass" : "sqltest",
"db" : ObCfg.usr.replace('.', '_')
}
#ob4 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:2], ct=CT('sqltest', mysql=mysql, hosts = ObCfg.default_hosts[:2]))

# rpm21_url = 'http://upload.yum.corp.taobao.com/taobao/5/x86_64/test/oceanbase/oceanbase-0.2.1-655.el5.x86_64.rpm'
# rpm30_url = 'http://upload.yum.corp.taobao.com/taobao/5/x86_64/test/oceanbase/oceanbase-0.3.0-674.el5.x86_64.rpm'
# svn30_url = 'http://svn.app.taobao.net/repos/oceanbase/branches/rev_0_3_0_ii_dev/oceanbase'
# local30_dir = '~/ob.30'
# ob1 = OBI('OBI_MASTER', src=rpm30_url, ver='.30', ct=CT('mixed_test'))
# tr0 = TR('ob1', case_src_pat='test/A0*.py')
# tr1 = TR('ob1', case_src_pat='test/A1*.py', case_pat='one_cluster')

# ob2 = OBI('OBI_MASTER', src=rpm30_url, ver='.30', ct=CT('mixed_test'))

# ## explict config each server and their attributes
# rs0_ip = '10.232.36.31'
# rs1_ip = '10.232.36.33'
# ups0_ip = '10.232.36.29'
# cs0_ip = '10.232.36.31'
# ms0_ip = '10.232.36.31'
# cli0_ip = '10.232.36.33'
rs0_ip = '182.119.80.55'
ups1_ip = '182.119.80.55'
cs0_ip = '182.119.80.55'
ms0_ip = '182.119.80.55'

ob1=OBI()
ob1=OBI( rs_port=9500,ups_port=9700,ms_port=9800,cs_port=9600,ups_inner_port=9701,mysql_port=9880,
rs0  = RootServer(rs0_ip,paxos_num=2,cluster_num=1,cluster_id=0),
ups1 = UpdateServer(ups1_ip,paxos_id=1,cluster_id=0),
cs0  = ChunkServer(cs0_ip,cluster_id=0),
ms0  = MergeServer(ms0_ip,cluster_id=0))

# for duyr

#ob6=OBI()
#ob6=OBI( rs_port=2500,ups_port=2700,ms_port=2800,cs_port=2600,ups_inner_port=2770,mysql_port=2880,
#rs0 = RootServer(rs0_ip),
#                    ups1 = UpdateServer(ups1_ip),
#                     cs0 = ChunkServer(cs0_ip),
#                    cs1 = ChunkServer(cs1_ip),
#                   cs2 = ChunkServer(cs2_ip),
#                  ms0 = MergeServer(ms0_ip),
#                 ms1 = MergeServer(ms1_ip),)



# # config 'port', 'version' and 'schema' etc, no 'client'
# ## Note: if you set 'ver', updateserver/rootserver/... should be in dir 'bin.$ver'
# ob5 = OBI('OBI_MASTER', ver='.30',
#           ## comment lines below to explit configure default servers' ports using by this 'OBI'
#           # rs_port = 2544,
#           # ups_port = 2644,
#           # ups_inner_port = 2744,
#           # cs_port = 2844,
#           # ms_port = 2944,
# 	  # tpl = dict(schema_template=read('yubai.schema')), # Note: client will reset schema
#                     rs0 = RootServer(rs0_ip),
#                     rs1 = RootServer(rs1_ip),
#                     ups0 = UpdateServer(ups0_ip, replay_checksum_flag=1),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    ct = CT('syschecker', hosts=[cli0_ip]),
#                     ## comment line below to explict configure 'ms1' port
#                     # ms1 = ChunkServer(ms0_ip),
# )

# ## config two OBI, UPS sync using lsync
# ob6 = OBI('OBI_MASTER', ver='.21',
#                     rs0 = RootServer(rs0_ip),
#                     ups0 = UpdateServer(ups0_ip, lsync_ip='', lsync_port=3046),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    lsync0 = LsyncServer(ups0_ip, port=3045, lsync_retry_wait_time_us=100000, log_level='debug'),
# 		    ct = CT('client_simulator', hosts = [cli0_ip]))
# ob7 = OBI('OBI_MASTER', ver='.30',
# 		    lsyc_port = 3046,
#                     rs0 = RootServer(rs0_ip),
#                     ups0 = UpdateServer(ups0_ip, lsync_ip= ups0_ip, lsync_port = 3045),
# 		    lsync0 = LsyncServer(ups0_ip, port=3046, convert_switch_log=1, lsync_retry_wait_time_us=100050, log_level='debug'),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    ct = CT('client_simulator', hosts = [cli0_ip]))

# ob8 = OBI('OBI_MASTER', ver='.dev-0.3', ct=CT('mixed_test'))
# ob2 = OBI(tpl = dict(schema_template=read('ob_new_sql_test.schema')), data_dir="/home/admin/ob2/data")
# ob1 = OBI(tpl=tpl, data_dir="/home/admin/ob1/data")
# ob1 = OBI(tpl=tpl, data_dir="/home/admin/ob1/data")

# multi_cluster example
# load_file('multi_cluster.py')
# mc_hosts = '10.232.36.171'.split()
# mc = MultiCluster(ob3=OBI(hosts=mc_hosts), ob4=OBI(hosts=mc_hosts))

# load_file('b/extra.py')
# ob3 = OBI('OBI_MASTER', ver='.31', tool_ver='.31', src='~/ob.31', hosts=HostsPool(1, '10.232.36.17.*'),
#          tpl = load_dir_vars('tpl.3x'),  new_style_config='False',
#          ct=CT('simple_client.31', write_type='mutator', write=100, write_size=20, hosts=HostsPool(1, '.*')))

#load_file('multi_cluster.py')
#mc = MultiCluster(ob9=OBI(hosts=['182.119.80.46']), ob10=OBI(hosts=['182.119.80.45']))
