ob_user='admin'
ob_password='admin'

simon_host = '10.232.23.15'
simon_port = 7466
simon_cluster = 'histest_monitor'

cs_rows=10000
ups_rows=10000

test_threads=2
max_seconds=60
freeze_seconds=60
freeze_repeat=2
max_requests=100000000

test_mode='read'

range_size = 100

point_query = 0
range_query = 0
range_sum_query = 0
range_order_query = 0
range_distinct_query = 0

point_join_query = 0
range_join_query = 0
range_sum_join_query = 0
range_order_join_query = 0
range_distinct_join_query = 0

check_local_file = 'sh: ls histest/{sysbench_mt,stress.sh,histest.conf.template,ups_admin,table.sql,fin.sql} # ExceptionOnFail'
client_start_args = 'histest.conf.$ip'
client_env_vars = 'LD_LIBRARY_PATH=/usr/lib64/mysql'

ps_mode = 'disable'


def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for histest')
    tpl = obi.get('tpl', {})
    obi.update(tpl=tpl)
    return 'configure_obi by histest'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for histest')
    return True

def gen_client_conf(**self):
    path, content = sub2('histest/histest.conf.$ip', self), sub2(read('histest/histest.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
