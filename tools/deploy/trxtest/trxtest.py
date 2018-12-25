ob_user='admin'
ob_password='admin'

simon_host = '10.232.23.15'
simon_port = 7466
simon_cluster = 'trxtest_daily'

cs_rows=10000
ups_rows=10000

test_threads=2
max_seconds=60
max_requests=100000000

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

int_update = 1
str_update = 1

check_local_file = 'sh: ls trxtest/{trxtest,stress.sh,trxtest.conf.template,ups_admin} # ExceptionOnFail'
client_start_args = 'trxtest.conf.$ip'
client_env_vars = 'LD_LIBRARY_PATH=/usr/lib64/mysql'

ps_mode = 'disable'


def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for trxtest')
    tpl = obi.get('tpl', {})
    obi.update(tpl=tpl)
    return 'configure_obi by trxtest'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for trxtest')
    return True

def gen_client_conf(**self):
    path, content = sub2('trxtest/trxtest.conf.$ip', self), sub2(read('trxtest/trxtest.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
