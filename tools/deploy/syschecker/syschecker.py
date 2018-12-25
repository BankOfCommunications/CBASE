write_thread_count = 1
read_thread_count = 10

check_local_file = 'sh: ls syschecker/{syschecker,stress.sh,syschecker.schema,syschecker.conf.template,gen.sh,gen_sstable} # ExceptionOnFail'
client_start_args = 'syschecker.conf.$ip'
def configure_obi(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Exception('no obi defined for syschecker')
    tpl = obi.get('tpl', {})
    tpl.update(schema_template = read('syschecker/syschecker.schema'))
    obi.update(tpl=tpl)
    return 'configure_obi by syschecker'

def prepare(**self):
    obi = find_attr(self, 'obi')
    if not obi: raise Fail('no obi defined for syschecker')
    rsync_ret = par_do(obi, 'chunkserver', 'rsync2', 'syschecker')
    gen_ret = par_do(obi, 'chunkserver', 'ssh_no_tty', '$dir/syschecker/gen.sh ${app_name}.schema $data_dir $sub_data_dir # ExceptionOnFail', timeout=300)
    return rsync_ret, gen_ret

def gen_client_conf(**self):
    path, content = sub2('syschecker/syschecker.conf.$ip', self), sub2(read('syschecker/syschecker.conf.template'), self)
    write(path, content)
    return path

client_custom_attr = dict(conf=gen_client_conf)
