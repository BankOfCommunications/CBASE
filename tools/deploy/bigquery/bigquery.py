write_thread_count = 1
read_thread_count = 10

check_local_file = 'sh: ls bigquery/{bigquery,stress.sh,bigquery.schema,bigquery.conf.template} # ExceptionOnFail'
client_start_args = 'bigquery.conf.$ip'
def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for bigquery')
    obi.update(tpl=dict(schema_template = read('bigquery/bigquery.schema')))
    print 'configure_obi by bigquery'

def gen_client_conf(**self):
    path, content = sub2('bigquery/bigquery.conf.$ip', self), sub2(read('bigquery/bigquery.conf.template'), self)
    write(path, content)
    return path
client_custom_attr = dict(conf=gen_client_conf)
