check_local_file = 'sh: ls mixed_test/{stress.sh,mixed_test.schema,launcher,multi_write,random_read,total_scan} # ExceptionOnFail'
is_read_consistency = "true"
client_env_vars = 'is_read_consistency=${is_read_consistency}'
client_start_args = '${nthreads} ${obi.rs0.ip} ${obi.rs0.port}'
def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for mixed_test')
    obi.update(tpl=dict(schema_template = read('mixed_test/mixed_test.schema')))
    return 'configure_obi by mixed_test'

def prepare(**self):
    pass

def conf(**self):
    pass
