## comment line below if you need to run './deploy.py ob1.random_test ...'
load_file('monitor.py', 'fault_test.py')
#data_dir = '/home/yuanqi.xhf/data'         # $data_dir/{1..10} should exist
data_dir = '/data/'
## comment line below if you want to provide custom schema and server conf template(see ob5's definition for another way)
## run: './deploy.py tpl.gensvrcfg' to generate a 'tpl' dir, edit tpl/rootserver.template... as you wish 
tpl = load_dir_vars('tpl.4x2')  # template of config file such as rootserver.conf/updateserver.conf...

ObCfg.usr = 'admin'
ObCfg.home = '/home/admin'
data_dir='/data'
sub_dir_list='{1..6}'
dev='bond0'
total_memory_limit = 140
table_memory_limit = 128
log_sync_type = 1
trace_log_level = 'debug'
clog_dir = '/data/log1'

load_file('profile.py', 'b/extra.py')
#profile_spec = ' '.join(expand('c01x001_[mget,mutator,phyplan]01_i01xi01 c08x100_[mget,mutator,phyplan][01,20]_[i01xi01,i04xi04,s01xs01] c08x100_[mutator,phyplan]80_i01xi01'))
profile_spec = ' '.join(expand('c01x001_[mget,mutator,phyplan]01_i01xi01_dev c08x[010,100]_mutator[01,20]_[i01xi01,i04xi04,s01xs01]_dev c08x[010,100]_mutator01_i01xi01_dev-opt'))
collected_log_dir = 'L/%s.3' %(popen('date +%y%j').strip())
def make_profiler(server_hosts, client_hosts, profile_spec):
    def make_profile_obi(spec, hosts, clients):
        def expand_host(spec):
            return list_merge(map(multiple_expand, spec.split()))
        online_common_attr = dict(
            exception_on_bootstrap_fail=1,
            rs_admin_cmd='ssh $gateway tools$tool_ver/rs_admin',
            ups_admin_cmd='ssh $gateway tools$tool_ver/ups_admin',
            client_cmd='ssh $gateway tools$tool_ver/client',
            gateway='login1.cm4.taobao.org',
            need_lsync=False, new_style_config='True', tpl=load_dir_vars('tpl.4x2'),
            hosts=hosts, replay_checksum_flag = 1,
            clog_src='~/log.dev', wait_slave_sync_time_us=100004, boot_strap_timeout=60000000, profile_duration='600',
            )
        pat = 'c(\d+)x(\d+)_([a-z]+)(\d+)_(.+)_(.+)'
        m = re.match(pat, spec)
        if not m: raise Fail('Not Valid Obi Spec for Profile', spec)
        client_num, thread_num, write_type, trans_size, table_name, ver = m.groups()
        client_num, thread_num, trans_size = int(client_num), int(thread_num), int(trans_size)
        return OBI(ct=CT('simple_client.dev', hosts=clients[:client_num], n_transport=4, table=table_name,
                         write=thread_num, write_type=write_type, write_size=trans_size), run_id=spec,
                   src='~/ob.%s'%(ver), ver='.%s'%(ver), tool_ver='.dev',
                   **online_common_attr)
    return Profiler(report_file='report.html', **dict([(spec, make_profile_obi(spec, server_hosts, client_hosts)) for spec in profile_spec]))
online_hosts = expand('172.24.131.[194,193]')
online_clients = expand('10.246.164.[31,32,33,34,35,36,37,38,39,40]')
p0 = make_profiler(online_hosts[:1], online_clients, profile_spec.split())
