ob_user='admin'
ob_password='admin'

simon_host = '10.253.2.72'
simon_port = 7466
simon_cluster = 'daily_perf_regression'

cs_rows=10000
ups_rows=10000

test_threads=2
max_seconds=60
max_requests=100000000

test_mode='read'

range_size = 0

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

deletes = 0
update_query = 0
replace_query = 0

check_local_file = 'sh: ls benchmark/{sysbench,stress.sh,benchmark.conf.template,ups_admin,oltp.sql} # ExceptionOnFail'
client_start_args = 'benchmark.conf.$ip'
client_env_vars = 'LD_LIBRARY_PATH=/usr/lib64/mysql'

ps_mode = 'disable'


def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for benchmark')
    tpl = obi.get('tpl', {})
    obi.update(tpl=tpl)
    return 'configure_obi by benchmark'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for benchmark')
    return True

def gen_client_conf(**self):
    path, content = sub2('benchmark/benchmark.conf.$ip', self), sub2(read('benchmark/benchmark.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
