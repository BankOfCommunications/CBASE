def extra_role_attrs(role):
    dd_clog_pool = '''sh: ssh $usr@$ip 'for i in {1..30}; do dd if=/dev/zero of=$dir/data/ups_commitlog/pool/$$i bs=1024 count=65536; done; sync;' '''
    return locals()
def extra_obi_attrs(obi):
    write_type = 'mutator'
    mutate = '''sh: log_level=INFO n_transport=2 keep_goging_on_err=true write_type=$write_type $client_cmd stress rs=${rs0.ip}:${rs0.port} start=$start_key end=$end_key server=ups write=10 scan=0 mget=0 write_size=10 cond_size=0'''
    return locals()

def extra_install():
    role_vars.update(dict_filter_out_special_attrs(extra_role_attrs(role_vars)))
    obi_vars.update(dict_filter_out_special_attrs(extra_obi_attrs(Env(obi_vars))))
extra_install()
