def fault_test_role_attrs(role):
    iof = '''sh: ssh $ip 'cd $dir; iof_ctrl=$dir/log/iof.$ip:$port $dir/tools/iof $_rest_' '''
    iof_disk_eio = '''sh: ssh $ip 'cd $dir; iof_ctrl=$dir/log/iof.$ip:$port $dir/tools/iof set /data/1 $iof_disk_fault_prob EIO' # ExceptionOnFail'''
    iof_disk_timeout = '''sh: ssh $ip 'cd $dir; iof_ctrl=$dir/log/iof.$ip:$port $dir/tools/iof set /data/1 $iof_disk_fault_prob EDELAY $iof_disk_timeout_us' # ExceptionOnFail'''
    iof_net_timeout = '''sh: ssh $ip 'cd $dir; iof_ctrl=$dir/log/iof.$ip:$port $dir/tools/iof set tcp:$port $iof_net_fault_prob EDELAY $iof_net_timeout_us' # ExceptionOnFail'''
    iof_clear = '''sh: ssh $ip 'cd $dir; iof_ctrl=$dir/log/iof.$ip:$port $dir/tools/iof clear' # ExceptionOnFail'''
    def get_vip(**attr):
        if attr.get('role') != 'rootserver': raise Fail('only rootserver has VIP')
        m = re.search('\$1 = ([0-9]+)', call_(attr, 'gdb_p', "p __rs->worker_->check_thread_.vip_"))
        if not m: return 'unknown'
        return ip2str(int(m.group(1)))
    def set_vip(vip='', **attr):
        if attr.get('role') != 'rootserver': raise Fail('only rootserver can reset VIP')
        if not vip: raise Fail('need to specify "vip"')
        return call_(attr, 'bgdb', "p __rs->worker_->check_thread_.reset_vip(%s)"%(str2ip(vip)))
    def add_table_to_schema(**attr):
        new_table_id = attr.get('new_table_id', None)
        if not new_table_id: raise Fail('need specify table_id!')
        tpl = find_attr(attr, 'tpl.new_table_template')
        if not tpl: raise Fail('no new_table_template found!')
        new_table = sub(tpl, dict(table_id=new_table_id))
        ret = append(sub2('$schema', attr), new_table)
        if not ret: raise Fail('append_new_table_to_schema fail')
        return 'new_table(%s)'%str(new_table_id)
    add_table = 'seq: add_table_to_schema switch_schema'
    return locals()

def fault_test_obi_attrs(obi):
    def random_test_(change_history=[], type='any', n='10', wait_time='1', **ob):
        for i in range(int(n)):
            if gcfg.get('stop', False): return True
            print "### %s iter %d ###"%(time.strftime('%Y-%m-%d %X'), i)
            random_change(change_history, op=type, **ob)
            sleep = 1.0*random.randint(0,int(wait_time)*1000)/1000
            print "sleep %fs" % (sleep)
            sys.stdout.flush()
            time.sleep(sleep)
        return change_history
    def random_test(*args, **ob):
        succ = True
        change_history = [(time.time(), 'init', 'ob', 'OK')]
        try:
            result = random_test_(change_history, *args, **ob)
        except Fail as e:
            succ = False
            print '--------------------Fail--------------------'
            print e
            print traceback.format_exc()
            #print call(ob, 'ct.stop')
            result = change_history + [(time.time(), 'fail', 'ob', e)]
        return succ, result

    def random_fault_test(*args, **ob):
        if len(get_match_child(ob, 'updateserver').keys()) < 2: raise Fail('need at least 2 ups!')
        succ, result = random_test(*args, **ob)
        if not succ: raise Fail('test fail!', result)
        return result

    def random_change(change_history, op='check', cli='ct', alive_check_timeout=30, **ob):
        timeout = 30
        def _check_basic(**ob):
            return call_(ob, "check_basic", cli=cli, _quiet_=True) and IterEnd() or 'TryAgain.'
        if not ob.get('_dryrun_') and not check_until_timeout(lambda :_check_basic(**ob), int(alive_check_timeout), interval=1):
                raise Fail('not pass basic check before timeout[%ds]!'%(int(alive_check_timeout)))
        if op == 'any': op = 'check,set_master,restart_server,disk_timeout,net_timeout,switch_schema'
        op =  random.choice(op.split(','))
        print 'select op: %s'%(op)
        func = ob.get("random_%s"%(op))
        if not callable(func): raise Fail('no callable "random_%s" defined for test!'%(op))
        if ob.get('_dryrun_'): ts, (new_op, new_server, ret) = time.time(), (op, 'x', 'dryrun')
        else: ts, (new_op, new_server, ret) = time.time(), func(change_history=change_history, **ob)
        change_history.append((ts, new_op, new_server, ret))
        print '### %s %s(%s):%s'%(time.strftime('%Y-%m-%d %X', time.localtime(ts)), op, new_server, str(ret))

    def _check_sync(**ob):
        if not call_(ob, "check_basic", _quiet_=True):
            raise Fail('not all server are alive OR client check fail!')
        _, id_list1, id_list2 =  obi.get_sync_status(**ob)
        if all(i <= 0 for i in id_list1) or all(i <= 0 for i in id_list2):
            raise Fail('no ups are live %s->%s!'%(id_list1, id_list2))
        if len([id for id in id_list2 if id > max(id_list1)]) <= 1:
            return id_list1, id_list2
        return IterEnd()
    def _get_sync_ups(**ob):
        name_list, id_list1, id_list2 = obi.get_sync_status(**ob)
        return [name for name,id in zip(name_list, id_list2) if id > max(id_list1)]

    def random_check(**ob):
        return 'check', 'ob', 'OK'
    def random_set_master(change_history=None, wait_sync_timeout=60, **ob):
        if not change_history: raise Fail('need change_history')
        _, last_op, last_server, ret = change_history[-1]
        if not check_until_timeout(lambda :_check_sync(**ob), int(wait_sync_timeout), interval=1):
            raise Fail('not sync before timeout[%ds]!'%(int(wait_sync_timeout)))
        for i in range(100):
            sync_ups_list = [ups for ups in _get_sync_ups(**ob) if ups != last_server]
            if sync_ups_list: break
        else:
            raise Fail('no sync ups selectable!')
        return set_master(random.choice(sync_ups_list), **ob)
    def random_restart_server(sig='-SIGTERM', pat='updateserver', wait_sync_timeout=60, **ob):
        if pat == 'updateserver':
            if len(get_match_child(ob, pat).keys()) > 1:
                if not check_until_timeout(lambda :_check_sync(**ob), int(wait_sync_timeout), interval=1):
                    raise Fail('not sync before timeout[%ds]!'%(int(wait_sync_timeout)))
        servers = get_match_child(ob, pat).keys()
        if not servers: raise Fail('no server matched:', pat)
        return restart_server(random.choice(servers), sig, **ob)
    def random_disk_eio(pat='updateserver', **ob):
        servers = get_match_child(ob, pat).keys()
        return disk_eio(random.choice(servers), **ob)
    def random_disk_timeout(pat='updateserver', **ob):
        servers = get_match_child(ob, pat).keys()
        return disk_timeout(random.choice(servers), **ob)
    def random_net_timeout(pat='updateserver', **ob):
        servers = get_match_child(ob, pat).keys()
        return net_timeout(random.choice(servers), **ob)
    def random_major_freeze(session=None, min_major_freeze_interval=0.1, **ob):
        if not session: raise Fail('no session!')
        if session.get('_last_frozen_time', 0) + float(min_major_freeze_interval) > time.time():
            return 'major_freeze', 'ob', 'skip'
        ret = call_(ob, 'major_freeze')
        if ret == 0:
            session['_last_frozen_time'] = time.time()
        return 'major_freeze', 'ob', ret
    def random_restart_client(**ob):
        ret = call_(ob, 'ct.restart')
        return 'restart_client', 'ob', ret
    def random_switch_schema(session=None, **ob):
        if not session: raise Fail('no session!')
        next_table_id = session.get('_next_table_id', 1100)
        ret = call_(ob, 'rs0.add_table', new_table_id=next_table_id)
        session['_next_table_id'] = next_table_id + 1
        return 'switch_schema', 'ob', ret
    def set_master(ups='',**ob):
        if not ups: raise Exception('need to specify "ups"')
        cmd = '''$localdir/tools/rs_admin -r ${rs0.ip} -p ${rs0.port} change_ups_master -o ups_ip=${%(ups)s.ip},ups_port=${%(ups)s.port},force''' % dict(ups=ups)
        ret = sh(sub2(cmd, ob))
        return 'set_master', ups, ret
    def restart_server(server='', sig='-SIGTERM', wait_before_start='0.0', **ob):
        if not server: raise Exception('need to specify server')
        if not check_until_timeout(lambda : call_(ob, '%s.kill_by_name'%(server), sig) == 1 and IterEnd() or 'kill_and_wait', 60, interval=1):
            raise Fail('server not exit before timeout!', server)
        time.sleep(float(wait_before_start))
        if not check_until_timeout(lambda : call_(ob, '%s.start'%(server)) == 0 and IterEnd() or 'try start', 30, interval=1):
            raise Fail('server not start before timeout!', server)
        return 'restart', server, 'OK'
    def _iof_injection(op='', server='', **ob):
        if not op or not server: raise Exception('need to specify op and server')
        set_err = call_(ob, '%s.iof_%s'%(server, op))
        time.sleep(1)
        clear_err = call_(ob, '%s.iof_clear'%(server))
        return op, server, set_err
        
    def disk_eio(server='', **ob):
        return _iof_injection('disk_eio', server, **ob)

    def disk_timeout(server='', **ob):
        return _iof_injection('disk_timeout', server, **ob)
        
    def net_timeout(server='', **ob):
        return _iof_injection('net_timeout', server, **ob)

    def set_master_rs(rs='',**ob):
        if not rs: raise Exception('need to specify "rs"')

    def get_obi_role(**ob):
        m = re.search("obi_role=(\w+)", call_(ob, 'rs0.rs_admin_p', 'get_obi_role'))
        if not m: return None
        return m.group(1)

    def get_master_obi(**ob):
        inst1, inst2 = ob.get('inst1'), ob.get('inst2')
        if not inst1 or not inst2: raise Fail("get_master_obi: no inst1/inst2 specified!")
        obi_role_list = [(inst, call_(ob, "%s.get_obi_role"%(inst))) for inst in (inst1, inst2)]
        master_obi_list = [(inst, obi_role) for inst, obi_role in obi_role_list if obi_role == 'MASTER']
        if len(master_obi_list) != 1:
            raise Fail("no master obi or too many obi", master_obi_list)
        return master_obi_list[0][0]

    def set_master_obi(inst=None, **ob):
        master_obi = get_master_obi(**ob)
        if not inst: raise Fail("set_master_obi: no inst specified!")
        if master_obi == inst: return "%s is Master already!"%(inst)
        ret = call_(ob, "%s.rs0.set_obi_role"%(master_obi), obi_role='OBI_SLAVE')
        if ret != 0:
            raise Fail('set MASTER obi[%s] to SLAVE failed[%s]!'%(master_obi, ret))
        ret = call_(ob, "%s.rs0.set_obi_role"%(inst), obi_role='OBI_MASTER')
        if ret != 0:
            raise Fail('set SLAVE obi[%s] to MASTER failed[%s]!'%(inst, ret))
        return 'set_master_obi(inst=%s, old_master_obi=%s)'%(inst, master_obi)
    def change_master_obi(**ob):
        inst1, inst2 = ob.get('inst1'), ob.get('inst2')
        if not inst1 or not inst2: raise Fail("change_master_obi: no inst1/inst2 specified!")
        master_obi = get_master_obi(**ob)
        slave_obi_list = [inst for inst in (inst1, inst2) if inst != master_obi]
        if not slave_obi_list: raise Fail("change_master_obi: no slave_obi avaiable!")
        return set_master_obi(slave_obi_list[0], **ob)
    
    def check_obi_sync(**ob):
        id_list1, id_list2 = call_(ob, 'get_obi_log_id_list'), call_(ob, 'get_obi_log_id_list')
        if all(i <= 0 for i in id_list1) or all(i <= 0 for i in id_list2):
            raise Fail('check_obi_sync: no active obi %s->%s!'%(id_list1, id_list2))
        if len([id for id in id_list2 if id > max(id_list1)]) <= 1:
            return id_list1, id_list2
        return 'OK'
        
    def switch_obi(loop=10, **ob):
        inst1, inst2 = ob.get('inst1'), ob.get('inst2')
        wait_time = 60
        if not inst1 or not inst2: raise Fail("change_master_obi: no inst1/inst2 specified!")
        def _check_obi_sync(**ob):
            print [('%s.ups0.start'%(obi), call_(ob, '%s.ups0.start'%(obi))) for obi in (inst1, inst2)]
            ret = check_obi_sync(**ob)
            if ret == 'OK': return IterEnd()
            return ret
        for i in range(int(loop)):
            print 'require all ups being alive'
            print 'wait %fs for slave start and sync'%(wait_time)
            for obi in (inst1, inst2):
                if not check_until_timeout(lambda :_check_obi_sync(**ob), wait_time, interval=1):
                    raise Fail('obi not sync before timeout!')
            print 'change master obi: iter=%d'%(i)
            #print [('%s.ups0.minor_freeze'%(obi), call_(ob, '%s.ups0.minor_freeze'%(obi))) for obi in (inst1, inst2)]
            #print 'wait %fs for minor freeze'%(wait_time)
            #time.sleep(wait_time)
            print change_master_obi(**ob)
        return 'Done'
    return locals()

def fault_test_install():
    ObCfg.iof_net_fault_prob = 0.1
    ObCfg.iof_disk_fault_prob = 1.0
    ObCfg.iof_net_timeout_us = 10000000
    ObCfg.iof_disk_timeout_us = 10000000
    ObCfg.server_ld_preload += ' $dir/tools/iof'
    ObCfg.server_start_environ += ' iof_ctrl=log/iof.$ip:$port'
    ObCfg.local_tools += ',iof'
    role_vars.update(dict_filter_out_special_attrs(fault_test_role_attrs(role_vars)))
    obi_vars.update(dict_filter_out_special_attrs(fault_test_obi_attrs(Env(obi_vars))))
fault_test_install()
