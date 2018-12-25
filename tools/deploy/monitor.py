def _get_ups_state(**role):
    cmd = '''ssh -t $usr@$ip "(cd $dir; gdb $exe --silent --batch --eval-command='p __ups->ups_master_' --eval-command='p __ups->ups_inst_master_' --eval-command='p *__obi_role' --eval-command='p *__role_mgr' --eval-command='p __log_mgr->replayed_cursor_' --eval-command='p __log_mgr->master_log_id_' \`pgrep -u $usr -f '^$exe'\` |sed '/New Thread/d' )"'''
    pat = 'port_ = (?P<master_port>\w+).*v4_ = (?P<master_ip>\w+).*port_ = (?P<inst_master_port>\w+).*v4_ = (?P<inst_master_ip>\w+).*role_ = oceanbase::common::ObiRole::(?P<obi>\w+).*role_ = oceanbase::updateserver::ObUpsRoleMgr::(?P<role>\w+),.*state_ = oceanbase::updateserver::ObUpsRoleMgr::(?P<state>\w+).*file_id_ = (?P<file_id>\w+).*log_id_ = (?P<log_id>\w+).*offset_ = (?P<offset>\w+).* = (?P<master_id>[-0-9]+)'
    if role.get('_dryrun_', False):
        return sub2(cmd, role)
    role_output = popen(sub2(cmd, role))
    if role.get('_verbose_'):
        print role_output
    match = re.search(pat, role_output, re.S)
    state = match and match.groupdict() or {}
    state.update(master_ip=ip2str(int(state.get('master_ip', 0))), inst_master_ip=ip2str(int(state.get('inst_master_ip', 0))))
    return state

def get_ups_state(**role):
    state = _get_ups_state(**role)
    return state.get('obi') and '%(obi)s:%(role)s:%(state)s, %(log_id)s@%(file_id)s:%(offset)s->%(master_id)s, master=%(master_ip)s:%(master_port)s, inst_master=%(inst_master_ip)s:%(inst_master_port)s'% state or 'Unknown'

def get_rs_state(**role):
    cmd = '''ssh -t $usr@$ip "(cd $dir; gdb $exe --silent --batch --eval-command='p *__obi_role' --eval-command='p *__role_mgr'  \`pgrep -u $usr -f '^$exe'\` |sed '/New Thread/d' )"'''
    pat = 'role_ = oceanbase::common::ObiRole::(?P<obi>\w+).*role_ = oceanbase::common::ObRoleMgr::(?P<role>\w+),.*state_ = oceanbase::common::ObRoleMgr::(?P<state>\w+)'
    if role.get('_dryrun_', False):
        return sub2(cmd, role)
    role_output = popen(sub2(cmd, role))
    if role.get('_verbose_'):
        print role_output
    match = re.search(pat, role_output, re.S)
    return match and '%(obi)s:%(role)s:%(state)s'% match.groupdict() or 'Unknown'

def monitor_role_attrs(ob):
    def get_role(**role):
        if role.get('role', None) == 'updateserver':
            return get_ups_state(**role)
        elif role.get('role', None) == 'rootserver':
            return get_rs_state(**role)
        else:
            return 'Unknown'
    def get_written_clog_cursor(**ups):
        cmd = sub2('$ups_admin_cmd -a $ip -p $port -t get_clog_cursor', ups)
        match = re.search('log_file_id=([0-9]+), log_seq_id=([0-9]+), log_offset=([0-9]+)', popen(cmd))
        if match: return int(match.group(1)), int(match.group(2)), int(match.group(3))
        else: return 0, 0, -1
    def get_written_log_id(**ups):
        ver = find_attr(ups, 'ver')
        file_id, seq_id, offset = get_written_clog_cursor(**ups)
        if re.search('\.21', ver): return seq_id + file_id - 1
        else: return seq_id
    #watch = '''sh: ssh -t $usr@$ip "(cd $dir; watch ls -lL $_rest_ $role_data_dir)"'''
    ls = '''sh: ssh $usr@$ip "(cd $dir; ls -lL $_rest_ $role_data_dir)"'''
    version = '''popen: ssh $usr@$ip "(cd $dir; $exe -V || true)"'''
    ping = '''sh: $dir/bin/ob_ping -i $ip -p $port -t 200000 '''
    return locals()

def monitor_obi_attrs(ob):
    client_simulator = '''sh: $localdir/tools/client_simulator -i ${rs.ip} -p ${rs.port} -o WRITE -c ${rs.schema} -t 20 $_rest_ -l /dev/null'''
    def get_role(**ob):
        rs_role = role_op(ob, 'rootserver', 'get_role')
        ups_role = role_op(ob, 'updateserver', 'get_role')
        return rs_role, ups_role
    def get_ups_list(**ob):
        return [k for k, v in sorted(ob.items(), key=lambda x: x[0]) if type(v) == dict and re.match('updateserver', v.get('role', ''))]
    def is_log_sync(**ob):
        state_list = [dict_updated(_get_ups_state(**ob.get(ups)), name=ups) for ups in get_ups_list(**ob)]
        if [state for state in state_list if state.get('obi') == None]: raise Fail("invalid state: server stoped!?")
        print ['%s:%s:%s:%s<-%s:%s<-%s:%s'% tuple(state.get(key) for key in "name obi role log_id master_ip master_port inst_master_ip inst_master_port".split()) for state in state_list]
        log_id_list = [state.get('log_id') for state in state_list]
        sys.stdout.flush()
        return len(set(log_id_list)) == 1
    def get_max_log_id_list(ups_list, timeout=3):
        def get_max_log_seq(ups):
            cmd = sub2('$ups_admin_cmd -a $ip -p $port -t get_max_log_seq', ups)
            match = re.search('max_log_seq=([-0-9]+)', popen(cmd))
            return match and int(match.group(1)) or -1
        return async_get(async_map(get_max_log_seq, ups_list), timeout)
    def get_master_written_log_id(**ob):
        ups = call_(ob, 'get_master_ups_name')
        if not ups: return -1
        else: return call_(ob, '%s.get_written_log_id'%(ups))
    def get_obi_log_id_list(timeout=3, **ob):
        inst1, inst2 = ob.get('inst1'), ob.get('inst2')
        if not inst1 or not inst2: raise Fail("get_obi_log_id_list: no inst1/inst2 specified!")
        return async_get(async_map(lambda obi: call_(ob, '%s.get_master_written_log_id'%(obi)), (inst1, inst2)), timeout)
    def get_sync_status(**ob):
        ups_name_list = get_ups_list(**ob)
        ups_list = [ob.get(ups) for ups in ups_name_list]
        return ups_name_list, get_max_log_id_list(ups_list), get_max_log_id_list(ups_list)
    def is_ups_alive(**ob):
        return [(ups, is_server_alive(ob.get(ups))) for ups in get_ups_list(**ob)]
    return locals()

def monitor_install():
    role_vars.update(dict_filter_out_special_attrs(monitor_role_attrs(role_vars)))
    obi_vars.update(dict_filter_out_special_attrs(monitor_obi_attrs(obi_vars)))
monitor_install()
