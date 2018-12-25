import os

config_server= '10.232.4.35:9000'
data_id='obmonster_junyue_perf'
core_jar_version='1.0.0'
core_jar_md5='a481e5321f16ec901f3187d1630376db'

case    =  'replace'
ps =  'disable'
threads = 32

cs_rows  = 100000
ups_rows = 10000
total_rows = cs_rows + ups_rows

replace_batch = 1000
skip_trx='on'
max_time=60
max_trx=100000000

check_local_file = 'sh: ls obdatasource/{obmonster.jar,obmonster,ups_admin} # ExceptionOnFail'
client_start_args = 'obdatasource.conf.$ip post_schema.xml.$ip prev_schema.xml.$ip obmonster.properties.$ip '
client_env_vars = 'LD_LIBRARY_PATH=/usr/lib64/mysql'

def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for obdatasource')
    tpl = obi.get('tpl', {})
    obi.update(tpl=tpl)
    return 'configure_obi by obdatasource'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for obdatasource')
    return True

def gen_client_conf(**self):

    ret = []
    path, content = sub2('obdatasource/obdatasource.conf.$ip', self), sub2(read('obdatasource/obdatasource.conf.template'), self)
    write(path, content)
    ret.append(path)

    path, content = sub2('obdatasource/prev_schema.xml.$ip', self), sub2(read('obdatasource/prev_schema.xml.template'), self)
    write(path, content)
    ret.append(path)
    path, content = sub2('obdatasource/post_schema.xml.$ip', self), sub2(read('obdatasource/post_schema.xml.template'), self)
    write(path, content)
    ret.append(path)
    path, content = sub2('obdatasource/obmonster.properties.$ip', self), sub2(read('obdatasource/obmonster.properties.template'), self)
    write(path, content)
    ret.append(path)

    flist = os.listdir(os.getcwd() + '/obdatasource') 
    for f in flist:
        if( '.svn'!= f and 'conf' != f and os.path.isdir(os.getcwd() + '/obdatasource/' + f)):
            path, content = sub2('obdatasource/' + f + '/transaction.xml.$ip', self), sub2(read('obdatasource/'+f+'/transaction.xml.template'), self)
            write(path, content)
            ret.append(path)

    return ret


client_custom_attr = dict(conf=gen_client_conf)
