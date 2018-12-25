simon_host = '10.232.23.15'
simon_port = 7466
simon_cluster = 'tpcc'


username='admin'
password='admin'
warehouses=10
database='test'
connections=4
warmup_time=600
running_time=3600
report_interval=10
report_file='./report_file.log'
trx_file='./trx_file.log'

# -h server_host -P port -d database_name -u mysql_user -p mysql_password -w warehouses -c connections -r warmup_time -l running_time -i report_interval -f report_file -t trx_file


check_local_file = 'sh: ls tpcc/{load.sh,add_fkey_idx.sql,count.sql,create_table.sql,drop_cons.sql,stress.sh,tpcc.conf.template,tpcc_load,tpcc_start} # ExceptionOnFail'
client_start_args = 'tpcc.conf.$ip'
client_env_vars = 'LD_LIBRARY_PATH=/usr/lib64/mysql'

def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for tpcc')
    tpl = obi.get('tpl', {})
    obi.update(tpl=tpl)
    return 'configure_obi by tpcc'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for tpcc')
    return True

def gen_client_conf(**self):
    path, content = sub2('tpcc/tpcc.conf.$ip', self), sub2(read('tpcc/tpcc.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
