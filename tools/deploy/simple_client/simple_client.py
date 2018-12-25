check_local_file = 'sh: ls simple_client/{stress.sh,client} # ExceptionOnFail'
n_transport =10
duration = -1
write_type = 'mutator'
cfg_update_interval=60*1000000
client_env_vars = 'log_level=INFO n_transport=${n_transport} write_type=$write_type cfg_update_interval=$cfg_update_interval '
write=200
scan=0
mget=0
write_size=20
cond_size=0
scan_size=100
mget_size=1
server='ups'
client_start_args = '''${{
write_start = int(sub2('$idx', locals())) * 10000000000 + 1024
}} ${obi.rs0.ip}:${obi.rs0.port} server=$server duration=$duration start=$write_start write=${write} scan=${scan} mget=${mget} write_size=$write_size cond_size=$cond_size scan_size=$scan_size mget_size=$mget_size'''

def configure_obi(**self):
    return 'configure_obi by simple_client'

def prepare(obi=None, **self):
    pass

def gen_client_conf(**self):
    pass
client_custom_attr = dict(conf=gen_client_conf)
