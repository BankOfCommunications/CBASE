write_thread_count = 1
read_thread_count = 5

check_local_file = 'sh: ls sqltest/{sqltest,stress.sh,sqltest.schema,sqltest.conf.template,sqlpattern.txt} # ExceptionOnFail'
client_start_args = 'sqltest.conf.$ip'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for syschecker')
    content = sub2(read('sqltest/mysql.sql.template'), self)
    write('sqltest/mysql.sql', content)
    return sh('sqltest/mysql -h 10.232.36.187 -P3306 -u sqltest -psqltest <sqltest/mysql.sql')

def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for sqltest')
    obi.update(tpl=dict(schema_template = read('sqltest/sqltest.schema')))
    print 'configure_obi by sqltest'

def gen_client_conf(**self):
    path, content = sub2('sqltest/sqltest.conf.$ip', self), sub2(read('sqltest/sqltest.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
