#!/usr/bin/env python2.6
'''
Usages:
  ./deploy.py path-to-config-file:path-to-method [extra-args] [options]
  # You can omit `path-to-config-file', in which case the last `config[0-9]*.py' (in lexicographic order) will be used
Common options:
  ./deploy.py ... _dryrun_=True
  ./deploy.py ... _verbose_=True
  ./deploy.py ... _quiet_=True
  ./deploy.py ... _loop_=10
Examples:
  ./deploy.py check_ssh # check ObCfg.all_hosts
  ./deploy.py tpl.gencfg config.py # generate a config template file, You can run all server on localhost without 'config.py'
  ## edit config.py.
  ./deploy.py ob1.update_local_bin
  ./deploy.py ob1[,.ct].reboot  # deploy and reboot obi and client, Will Cleanup Data
  ./deploy.py ob1[,.ct].restart # restart obi and client, Will Not Cleanup Data
  ./deploy.py ob1.random_test check 10 wait_time=1 # 'check' may be replaced by 'set_master/restart_server/disk_timeout/net_timeout/major_freeze/switch_schema/restart_client' or their combinations
  ./deploy.py ob1.random_test disk_timeout,set_master,major_freeze 10 wait_time=1 min_major_freeze_interval=10 # interval in seconds
  ./deploy.py ob1.switch_obi loop=10
  ./deploy.py ob1[,.ct].check_alive # check whether server and client are alive
  ./deploy.py ob1.ct.check_new_error
  ./deploy.py ob1[.ct,][stop,cleanup] # stop and cleanup obi and client
  ./deploy.py ob1.sysbench [prepare|run|cleanup] ['threads=1'] ['seconds=60'] ['requests=10000'] ['rows=10000'] # sysbench test
  ./deploy.py ob1.mysqltest [_quiet_=True] ['testset=test1[,test2[,...]]'] ['testpat=rowkey_*'] [record|test]  # start mysqltest, more info plz dig into mysql_test dir
  ./deploy.py ob1.obmysql [extra='extra-arguments-for-mysql-client'] [cmd]  # connect to oceabase by mysql client
  ./deploy.py tr1.run case_pat=update_local_bin # special test case to update local bin
  ./deploy.py tr1.run fast_mode=True keep_going=True # run test case
More Examples:
  ./deploy.py ob1.[id,pid,version]  # view ip, port, ver, data_dir, pid
  ./deploy.py ob1.print_schema
  ./deploy.py ob1.rs0.[ip,port]
  ./deploy.py ob1.ups0.vi # view server config file
  ./deploy.py ob1.ups0.less # view log
  ./deploy.py ob1.ups0.grep
  ./deploy.py ob1.cs0.kill -41 # 41: debugon, 42: debugoff, 43: traceon, 44: traceoff
  ./deploy.py ob1.cs0.gdb
  ./deploy.py ob1.rs0.rs_admin stat -o ups
  ./deploy.py ob1.ups0.ups_admin get_clog_cursor
  ./deploy.py ob1.ms[0,1,2].restart
  ./deploy.py ob1.ms0.ssh
  ./deploy.py ob1.ms0.ssh ps -uxf
  ./deploy.py ob[1,2,3,4].all_server_do kill_by_port
  ./deploy.py ob1.set_obi_role obi_role=OBI_SLAVE
  ./deploy.py ob1.change_master_obi
  ./deploy.py ob1.[get_master_ups,sleep] _loop_=100 sleep_time=1
  ...
More Configuration:
  ./deploy.py tpl.gencfg detail=True # dump detail configurable item
  ./deploy.py tpl.gensvrcfg # generate schema and server config template file in dir 'tpl'
'''
import sys, os
_base_dir_ = os.path.dirname(os.path.realpath(__file__))
sys.path.extend([os.path.join(_base_dir_, path) for path in '.'.split(':')])
import glob
from common import *
import random
import inspect
import signal
import mysql_test
import sysbench_test
import tablet_join_test
from pprint import pprint, pformat

def usages():
    sys.stderr.write(__doc__)
    sys.exit(1)

def genconf(**attrs):
    role = attrs.get('role', 'unknown-role')
    tpl = find_attr(attrs, 'tpl.%s_template'%role)
    schema = find_attr(attrs, 'tpl.schema_template')
    if not tpl: raise Exception("Failed to Get '%s_template'"%(role))
    path, content = sub2('etc/$role.conf.$ip:$port', attrs), sub2(tpl, attrs)
    mkdir('etc')
    if find_attr(attrs, 'new_style_config') == 'False':
        write(path, content)
    write(sub2('$schema', attrs), sub2(schema, attrs))
    return path

def get_match_child(d, pat):
    return dict_filter(lambda (k, v): type(v) == dict and re.match(pat, v.get('role', '')), d)

def update_bin(src, dest, workdir, nccjob=10):
    if not src: return 'no need to update_bin'
    server_list = '{rootserver,updateserver,chunkserver,mergeserver}'
    server_list_in_src = '{rootserver/rootserver,updateserver/updateserver,chunkserver/chunkserver,mergeserver/mergeserver}'
    if re.match('^http:.*rpm$', src):
	cmd = '''mkdir -p $workdir $dest && ( [ -e $rpm ] || wget $src -O $rpm)
 && (cd $workdir; [ -e extract ] || rpm2cpio $rpm | cpio -idmv --no-absolute-filenames && touch extract)
 && rsync -a $workdir/home/admin/oceanbase/bin/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=workdir, server_list=server_list, rpm='%s/ob.rpm'%(workdir))
    elif re.match('^http:.*svn.*branches', src):
	cmd = '''mkdir -p $workdir $dest && svn co $src $workdir
 && (cd $workdir; ([ -e .configured ] || (./build.sh init && ./configure --with-release) && touch .configured) && make -j $nccjob -C src)
 && rsync -a $workdir/src/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=workdir, server_list=server_list_in_src, nccjob=nccjob)
    elif src:
	cmd = '''mkdir -p $workdir $dest
 && (cd $workdir; ([ -e .configured ] || (./build.sh init && ./configure --with-release) && touch .configured) && make -j $nccjob -C src)
 && rsync -a $workdir/src/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=src, server_list=server_list_in_src, nccjob=nccjob)
    if gcfg.get('_verbose_', False):
        print cmd
    if gcfg.get('_dryrun_', False):
        return cmd
    elif 0 != sh(cmd):
        raise Fail('update_local_bin fail', cmd)

def HostsPool(n_hosts=1, pat='.*', **attr):
    def select_hosts(timeout=1, **hs):
        host_list = find_attr(hs, 'all_hosts')
        if not host_list: raise Fail('all_hosts not defined')
        check_result = async_get(async_map(lambda ip: call_(hs, 'check_host', ip=ip), host_list), int(timeout))
        check_result = zip(check_result, host_list)
        fail = filter(lambda (result, ip): type(result) != float, check_result)
        info('Failed Hosts: %s'% fail)
        sorted_hosts = sorted([(result,ip) for result,ip in check_result if type(result) == float], key=lambda (result,ip): result)
        pprint(sorted_hosts)
        return [ip for result,ip in sorted_hosts]
    def reconfigure(n_hosts=1, pat='.*', **hs):
        hosts = call_(hs, 'select_hosts')
        hosts = filter(lambda h: re.match(pat, h), hosts)
        if len(hosts) < int(n_hosts):
            raise Fail('no enough hosts present[request=%s, avail=%s, pat=%s]'%(str(n_hosts), hosts, pat))
        path, key = sub2('$cfg_cache_path', hs), sub2('$dir', hs)
        if not path or not key: raise Fail('cfg_cache_path or dir is not set')
        return _store_hosts(path, key, hosts[:n_hosts])
    def get_hosts(**hs):
        path, key = sub2('$cfg_cache_path', hs), sub2('$dir', hs)
        if not path or not key: raise Fail('cfg_cache_path or dir is not set')
        return _get_hosts(path, key);
    def _get_hosts(path, key):
        try:
            return eval(read(path)).get(key, [])
        except Exception,e:
            print 'Fail to get hosts[path=%s, key=%s]'%(path, key)
            return []
    def _store_hosts(path, key, hosts):
        try:
            store = eval(safe_read(path) or '{}')
            store.update({key:hosts})
            write(path, repr(store))
            return hosts
        except Exception,e:
            raise Fail('Fail to store hosts[path=%s, key=%s, hosts=%s]'%(path, key, hosts))
    return dict_updated(dict_filter_out_special_attrs(locals()), n_hosts=n_hosts, pat=pat, **attr)

def make_role(role, **attr):
    return dict_updated(role_vars, idx=ObCfg.role_counter.next(), role=role, **attr)

def Role():
    id = '$role$ver@$ip:$port:$dir'
    pid = '''popen: ssh $usr@$ip "pgrep -u $usr -f '^$exe'" || echo PopenException: NoProcessFound '''
    version = '''popen: ssh $usr@$ip "$exe -V" '''
    set_obi_role = '''sup: $rs_admin_cmd -r $ip -p $port set_obi_role -o $obi_role'''
    boot_strap_eof = '''sup: $localdir/tools/rs_admin -t $boot_strap_timeout -r $ip -p $port boot_strap # ExceptionOnFail'''
    boot_strap = '''sup: $rs_admin_cmd -t $boot_strap_timeout -r $ip -p $port boot_strap'''
    print_schema = '''sh: $rs_admin_cmd -r $ip -p $port print_schema'''
    start_old_way = '''sh: ssh $usr@$ip '(cd $dir; ulimit -c unlimited; pgrep -u $usr -f "^$exe" || $server_start_environ $exe -f etc/$role.conf.$ip:$port $_rest_)' '''
    def start(**info):
        if find_attr(info, 'new_style_config') == 'True':
            return call(info, 'start_new_way')
        else:
            return call(info, 'start_old_way')
    def start_new_way(**info):
        cs_data_path = find_attr(info, "cs_data_path")
        cluster_id = find_attr(info, "cluster_id")
        outline = '''ssh $usr@$ip '(cd $dir; ulimit -c unlimited; pgrep -u $usr -f "^$exe" || $server_start_environ %s )' '''
        if find_attr(info, 'tidy'):
            cmd = sub2(outline % "$exe", info)
        else:
            cmd = sub2({
                    "rootserver" : outline % "$exe -r ${rs0.ip}:${rs0.port} -R ${mmrs} -i ${dev:-bond0} -C ${cluster_id:-1} > /dev/null 2>&1",
                    "updateserver" : outline % "$exe -r ${rs0.ip}:${rs0.port} -n ${app_name} -p $port -m $inner_port -i ${dev:-bond0} > /dev/null 2>&1",
                    "chunkserver" : outline % "$exe -r ${rs0.ip}:${rs0.port} -n ${app_name} -p $port -D ${cs_data_path:-$dir/data/cs} -i ${dev:-bond0} ${cs_extra_args} > /dev/null 2>&1",
                    "mergeserver" : outline % "$exe -r ${rs0.ip}:${rs0.port} -z $mysql_port -p $port -i ${dev:-bond0} ${ms_extra_args} > /dev/null 2>&1",
                    "monitorserver" : outline % "$exe -f etc/$role.conf.$ip:$port",
                    }[sub2("$role", info)], info)
        return sh(cmd)

    stop = 'call: kill_by_name -9'
    kill = '''sh: ssh $usr@$ip "pkill $_rest_ -u $usr -f '^$exe'"'''
    kill_by_name = '''sh: ssh $usr@$ip "pkill $_rest_ -u $usr -f '^$exe'"'''
    kill_by_port = '''sh: ssh $usr@$ip "/sbin/fuser -n tcp $port -k $_rest_"'''
    conf = genconf
    rsync = '''sh: rsync -aR bin$ver/$role $schema etc lib tools$tool_ver $usr@$ip:$dir'''
    rsync_cs_sstable = '''sh: [ -z '$local_sstable_dir' ] || ( rsync -a $local_sstable_dir/ $usr@$ip:${remote_sstable_cache_dir} && ssh $usr@$ip rsync -aK ${remote_sstable_cache_dir}/ ${cs_data_path} )'''
    clear_clog = '''sh: ssh $usr@$ip rm -rf $dir/data/ups_commitlog/*'''
    clear_sstable = '''sh: ssh $usr@$ip rm -f $dir/data/ups_commitlog/log_replay_point $dir/data/ups_data/raid*/store*/*'''
    rsync2 = '''sh: rsync -a $_rest_ $usr@$ip:$dir/'''
    rsync_clog = '''sh: rsync -av $clog_src $usr@$ip: && ssh $usr@$ip cp $clog_src/* $dir/data/ups_commitlog # ExceptionOnFail'''
    touch_and_link_log_file = '''sh: ssh $usr@$ip 'touch $dir/log/$role.log.$ip:$port && ln $dir/log/$role.log{.$ip:$port,}' '''
    mkdir = '''sh: ssh $usr@$ip 'mkdir -p $dir/data/ups_data/raid{0,1} $dir/data/cs $dir/data/rs $dir/log
mkdir -p $data_dir/$sub_dir_list/Recycle $data_dir/$sub_dir_list/$sub_data_dir/{sstable,ups_store,bypass} $clog_dir/$sub_data_dir/{rs,ups}_commitlog/pool
mkdir -p $sstable_dir_list/$sub_data_dir/sstable
chown $$USER $data_dir/$sub_dir_list/$sub_data_dir -R
ln -sf $clog_dir/$sub_data_dir/rs_commitlog $dir/data/
ln -sf $clog_dir/$sub_data_dir/ups_commitlog $dir/data/
ln -sf $sstable_dir_list $dir/data/cs/
for i in {0,1}; do for j in {0,1}; do [ -e $dir/data/ups_data/raid$$i/store$$j ] || ln -sf $data_dir/$$((1 + i * 2 + j))/$sub_data_dir/ups_store $dir/data/ups_data/raid$$i/store$$j;done; done' '''.replace('\n', '; ')
    rmdir = '''sh: ssh $usr@$ip "rm -rf $data_dir/$sub_dir_list/$sub_data_dir/* $dir/{etc,data,log} $clog_dir/$sub_data_dir/* ${cs_data_path}"'''
    debugon = '''sh: ssh $usr@$ip "pkill -41 -u $usr -f '^$exe'"'''
    debugoff = '''sh: ssh $usr@$ip "pkill -42 -u $usr -f '^$exe'"'''
    ssh = '''sh: ssh -t $ip $_rest_'''
    ssh_no_tty = '''sh: ssh $ip $_rest_'''
    gdb = '''sh: ssh -t $usr@$ip 'cd $dir; gdb bin$ver/$role --pid=`pgrep -f ^$exe`' '''
    bgdb = '''sh: ssh -t $usr@$ip "(cd $dir; gdb $exe --batch --eval-command=\\"$_rest_\\"  \`pgrep -u $usr -f '^$exe'\` | sed '/New Thread/d')"'''
    gdb_p = '''popen: ssh -t $usr@$ip "(cd $dir; gdb $exe --batch --eval-command=\\"$_rest_\\"  \`pgrep -u $usr -f '^$exe'\` | sed '/New Thread/d')"'''
    save_log = '''sh: scp $usr@$ip:$dir/log/$role.log.$ip\:$port $_rest_'''
    vi = '''sh: ssh -t $usr@$ip "vi $dir/etc/$role.conf.$ip\:$port"'''
    less = '''sh: ssh -t $usr@$ip "less $dir/log/$role.log.$ip\:$port"'''
    #less = '''sh: ssh -t $usr@$ip "less $dir/log/$role.log"'''
    log = '''$dir/log/$role.log.$ip\:$port'''
    tail = '''sh: ssh -t $usr@$ip "tail -f $dir/log/$role.log.$ip\:$port"'''
    #grep = '''sh: ssh $usr@$ip "grep '$_rest_' $dir/log/$role.log"'''
    grep = '''sh: ssh $usr@$ip "grep '$_rest_' $dir/log/$role.log.$ip:$port"'''
    grep_p = '''popen: ssh $usr@$ip "grep '$_rest_' $dir/log/$role.log.$ip:$port"'''
    restart = '''seq: stop rsync start'''
    ups_admin = '''sh: $ups_admin_cmd -a ${ip} -p ${port} -t $_rest_'''
    rs_admin = '''sh: $rs_admin_cmd -r ${ip} -p ${port} $_rest_'''
    cs_admin = '''sh: $localdir/tools/cs_admin -s ${ip} -p ${port} -i $_rest_'''
    watch = 'sh: $localdir/tools/cs_admin -s ${ip} -p ${port}  -i "dump_server_stats 1 0 20 0 1~3"'
    minor_freeze = '''sh: $ups_admin_cmd -a ${ip} -p ${port} -t minor_freeze'''
    major_freeze = '''sh: $ups_admin_cmd -a ${ip} -p ${port} -t major_freeze'''
    ups_admin_p = '''popen: $ups_admin_cmd -a ${ip} -p ${port} -t $_rest_'''
    rs_admin_p = '''popen: $rs_admin_cmd -r ${ip} -p ${port} $_rest_'''
    reload_conf = '''sh: $ups_admin_cmd -a ${ip} -p ${port} -t reload_conf -f $dir/etc/$role.conf.$ip:$port'''
    roottable = '''popen: $localdir/tools/rs_admin -r ${ip} -p ${port} print_root_table'''
    collect_log = '''sh: mkdir -p $collected_log_dir/$run_id && ssh $usr@$ip sync && scp $usr@$ip:$dir/log/$role.log $collected_log_dir/$run_id/$role.log.$ip:$port && scp $usr@$ip:$dir/log/$role.profile $collected_log_dir/$run_id/$role.profile.$ip:$port'''
    clean_log = '''sh: ssh $usr@$ip 'rm -f $dir/log/$role.log.$ip:$port' '''
    switch_schema = '''seq: rsync rs_admin[switch_schema]'''
    return locals()

def role_op(ob, role, op, *args, **kw_args):
    def mycall(ob, path, *args, **kw_args):
        ret = call(ob, path, *args, **kw_args)
        return ret
    return [mycall(ob, '%s.%s'%(k, op), *args, **kw_args) for k, v in sorted(ob.items(), key=lambda x: x[0]) if type(v) == dict and re.match(role, v.get('role', ''))]

def obi_op(obi, op, *args, **attrs):
    return list_merge(role_op(obi, role, op, *args, **attrs) for role in 'rootserver updateserver mergeserver chunkserver lsyncserver proxyserver'.split())

def check_ssh_connection(ip, timeout):
    return timed_popen('ssh -T %s -o ConnectTimeout=%d true'%(ip, timeout), timeout)

def get_load(ip):
    output = popen('ssh -T %s uptime'%(ip))
    match = re.search('load average: [0-9.]+, ([0-9.]+), [0-9.]+', output)
    if not match: return 'get_load(%s) fail'%(ip)
    return float(match.group(1))

def check_data_dir_permission(_dir, min_disk_space=10000000, timeout=1):
    return 0
    ip, dir = _dir.split(':', 2)
    cmd = '''ssh %(ip)s "mkdir -p %(dir)s && touch %(dir)s/.mark-by-deploy.$USER && test \`df -P /home|sed -n '/home/p' |awk '{print \$4}'\` -gt %(min_disk_space)d"'''
    cmd = cmd % dict(ip=ip, dir=dir, min_disk_space=min_disk_space / 1024)
    if gcfg.get('_verbose_', False):
        print cmd
    return timed_popen(cmd, timeout)

def is_server_alive(server, timeout=3):
    return sh(sub2('''ssh $usr@$ip -o ConnectTimeout=%d "pgrep -u $usr -f '^$exe'" >/dev/null'''%(timeout), server)) == 0

def ObInstance():
    is_start_service_now = '''sh: ssh ${rs0.usr}@${rs0.ip} "grep '\[NOTICE\] start service now' ${rs0.dir}/log/${rs0.role}.log.${rs0.ip}:${rs0.port}*"'''
    is_finish_reporting = '''sh: $rs_admin_cmd -r ${rs0.ip} -p ${rs0.port} stat -o rs_status|grep INITED'''
    is_bootstrap_ok = '''sh: ssh ${rs0.usr}@${rs0.ip} "grep 'set boot ok, all initial tables created' ${rs0.dir}/log/${rs0.role}.log"'''

    update_local_tools = '''sh: make tools$tool_ver/{iof,ups_admin,rs_admin} src_dir=$src'''

    def test_bootstrap(cnt=10, **ob):
        gcfg['_quiet_'] = True
        print "begin boot strap testing, tobal cnt: [%s]" % cnt
        for i in range(int(cnt)):
            sys.stdout.write("The [%d] test: " % i)
            result = call_(ob, "reboot")
            if 0 == result[-1][-1]:
                force_info("bootstrap OK!")
            else:
                force_info("bookstrap fail!")
                break

    def check_alive(**ob):
        return all(map(is_server_alive, get_match_child(ob, '.*server$').values()))

    def check_basic(cli='ct', **ob):
        return call_(ob, 'check_alive') and call_(ob, '%s.check_alive'%(cli)) and call_(ob, '%s.check_new_error'%(cli))

    def require_start(**ob):
        if not check_until_timeout(lambda : call_(ob, 'is_finish_reporting', _quiet_=True) == 0 and IterEnd() or 'wait ob start service', 180, 1):
            raise Fail('ob not start before timeout!')
        return 'OK'

    def require_boot_ok(**ob):
        if not check_until_timeout(lambda : call_(ob, 'is_bootstrap_ok', _quiet_=True) == 0 and IterEnd() or 'wait ob boot ok', 180, 1):
            raise Fail('ob not boot ok before timeout!')
        return 'OK'

    def check_local_bin(**ob):
        if 0 != sh(sub2('ls  bin$ver/{$local_servers} lib/libsnappy_1.0.so tools$tool_ver/{$local_tools} >/dev/null', ob)):
            raise Fail('local bin file check failed. make sure "bin" "lib" "tools" dir and files exists!')
        return 'OK'
    def prepare_cs_data(cs_data='~/data', **ob):
        cs0 = call_(ob, 'cs0.ip');
        outline = '''ssh $usr@%s '(cp -r %s/* %s; sync )' '''
        if 0 != sh(sub2(outline % (cs0, cs_data, "$cs_data_path"), ob)):
            raise Fail('Prepare cs data failed!')
        return 'OK'
    def check_ssh(timeout=1, **ob):
        ip_list = set(sub2('$usr@$ip', v) for v in get_match_child(ob, '.+server$').values())
        result = async_get(async_map(lambda ip: check_ssh_connection(ip, timeout), ip_list), timeout+1)
        if any(map(lambda x: x != 0, result)):
            raise Fail('check ssh fail, make sure you can ssh to these machine without passwd',
                    map(lambda ip, ret: '%s:%s'%(ip, ret == 0 and 'OK' or 'Fail'), ip_list, result))
        return 'OK'

    def check_dir(timeout=1, min_disk_space=1000, **ob):
        dir_list = set(sub2('$usr@$ip:$data_dir/$sub_dir_list', v) for v in get_match_child(ob, '.+server$').values())
        result = async_get(async_map(lambda dir: check_data_dir_permission(dir, min_disk_space, timeout), dir_list), timeout+1)
        if any(map(lambda x: x != 0, result)):
            raise Fail('check data dir fail, make sure you have write permission on configured data_dir and have enough space on home dir(>%dM):\n%s'%(min_disk_space,
                    pformat(map(lambda dir, ret: '%s:%s'%(dir, ret == 0 and 'OK' or 'Fail'), dir_list, result))))
        return 'OK'

    def check_host(timeout=1, min_disk_space=1000, **ob):
        ssh_spec, data_dir_spec = sub2('$usr@$ip', ob), sub2('$usr@$ip:$data_dir/$sub_dir_list', ob)
        if 0 != check_ssh_connection(ssh_spec, timeout): return 'check ssh[%s] fail!'%(ssh_spec)
        if 0 != check_data_dir_permission(data_dir_spec, min_disk_space, timeout): return 'check data_dir[%s] fail!'%(data_dir_spec)
        return get_load(ssh_spec)
    def set_obi_role_until_success(timeout=0.5, obi_role='OBI_MASTER', **ob):
        if not check_until_timeout(lambda : call_(ob, 'rs0.set_obi_role', obi_role=obi_role) == 0 and IterEnd() or 'try set_obi_role', 30, timeout):
            return 'Fail[%s]'%(obi_role)
        return 'OK[%s]'%(obi_role)

    def boot_strap_maybe(obi_role='OBI_MASTER', **ob):
        if obi_role == 'OBI_SLAVE': return 'OK[SLAVE No need to bootstrap]'
        ret = call_(ob, 'rs0.boot_strap')
        if find_attr(ob, 'exception_on_bootstrap_fail') and ret != 0:
            raise Fail('bootstrap fail!', ret)
        #add liuxiao
        str_port=str(ob['mysql_port']);
        print 'echo "create database test" | mysql -h '+ob['ms0']['ip']+' -P'+str_port+' -uadmin -padmin;sleep 5;'
        p=Popen('echo "create database test" | mysql -h '+ob['ms0']['ip']+' -P'+str_port+' -uadmin -padmin;sleep 5;',shell=True, stdout=PIPE, stderr=PIPE)
        p.wait()
        #add e
        return ret

    def switch_to_slave_obi_maybe(timeout=0.5, obi_role='OBI_MASTER', **ob):
        if obi_role == 'OBI_MASTER': return 'OK[this is OBI_MASTER]'
        return call_(ob, 'switch_to_slave_obi')

    def get_master_ups(**ob):
        m = re.search('[^0-9.]([0-9.]+):(\d+)\(\d+\s+master', call_(ob, 'rs0.rs_admin_p', 'stat -o ups'))
        if not m: return None
        return '%s:%s'% m.groups()

    def get_master_ups_name(**ob):
        ups = get_master_ups(**ob)
        master_ups = [k for k,v in get_match_child(ob, '^updateserver$').items() if type(v) == dict and sub2('$ip:$port', v) == ups]
        if not master_ups: return None
        else: return master_ups[0]

    def major_freeze(**ob):
        master_ups = get_master_ups_name(**ob)
        if not master_ups: raise Fail('no master ups present!')
        return call_(ob, '%s.major_freeze'%(master_ups))

    def all_server_do(*args, **ob):
        return par_do(ob, '.+server', *args)

    def status(full=False, **ob):
        call_(ob, 'rs0.rs_admin', 'get_obi_role')
        ups_list = get_match_child(ob, 'updateserver').keys()
        [call_(ob, '%s.ups_admin' %(ups), 'get_clog_status') for ups in ups_list]
        if full:
            call_(ob, 'rs0.rs_admin', 'stat -o ups')
            call_(ob, 'rs0.rs_admin', 'stat -o cs')
            call_(ob, 'rs0.rs_admin', 'stat -o ms')

    def update_local_server(src=None, dest='bin$ver', workdir='$workdir/ob$ver', nccjob=10, **ob):
        if src:
            return update_bin(sub2(src, ob), sub2(dest, ob), sub2(workdir, ob), nccjob)
        else:
            return 'no src defined.'

    def do_cpbin(src=None, dest='bin$ver', workdir='$workdir/ob$ver', **obm):
        server_list_in_src = '{rootserver/rootserver,updateserver/updateserver,chunkserver/chunkserver,mergeserver/mergeserver}'
        cmd = "rsync -a $workdir/src/$server_list $dest"
        if src:
            cmd = string.Template(cmd.replace('\n', '')).substitute(dest=dest, workdir=src, server_list=server_list_in_src)
            if gcfg.get('_verbose_', False):
                force_info(cmd)
            if gcfg.get('_dryrun_', False):
                return cmd
            elif 0 != sh(cmd):
                raise Fail('update_local_bin fail', cmd)
        else:
            return 'no src defined!'

    def update_faster(src=None, des='bin$ver', workdir='$workdri/ob$ver', nccjob=10, **ob):
        server_list = '{rootserver,updateserver,chunkserver,mergeserver}'
        server_list_in_src = '{rootserver/rootserver,updateserver/updateserver,chunkserver/chunkserver,mergeserver/mergeserver,lsync/lsyncserver}'
        if src:
            cmd = '''mkdir -p $workdir $des && make -j $nccjob -C $src/src $servers && rsync -a $workdir/src/$server_list $des'''
            cmd = string.Template(cmd).substitute(src=sub2(src, ob), des=sub2(des, ob),
                                                  workdir=sub2(src, ob), server_list=server_list_in_src,
                                                  servers=server_list, nccjob=nccjob)
        else:
            return 'no src defined.'
        if gcfg.get('_verbose_', False):
            print cmd
        if gcfg.get('_dryrun_', False):
            return cmd
        elif 0 != sh(cmd):
            raise Fail('update_faster fail', cmd)

    def dosql(*args, **ob):
        if args:
            cmd = 'echo \'%s\' | mysql -t -h ${ms0.ip} -P ${mysql_port} -uadmin -padmin' % args
            cmd = sub2(cmd, ob)
            info(cmd)
            sh(cmd)
        else:
            force_info("No sql specified!")

    update_local_bin = 'seq: update_local_server update_local_tools'
    cpbin = 'seq: do_cpbin'
    get_master_ups_using_client = 'popen: log_level=WARN $localdir/tools/client get_master_ups ${rs0.ip}:${rs0.port}'
    def get_master_ups_name2(**ob):
        ups = call_(ob, 'get_master_ups_using_client')
        master_ups = [k for k,v in get_match_child(ob, '^updateserver$').items() if type(v) == dict and sub2('$ip:$port', v) == ups]
        if not master_ups: return None
        else: return master_ups[0]

    def client(*req, **ob):
        cmd = 'log_level=INFO $client_cmd %(req)s rs=${rs0.ip}:${rs0.port} %(kw)s'%dict(req=' '.join(req), kw=kw_format(ob.get('__dynamic__', {})))
        return sh(sub2(cmd, ob))

    def obmysql(*args, **ob):
        ori_cmd = 'mysql -h ${ms0.ip} -P ${mysql_port} -uadmin -padmin'
        if 'extra' in ob:
            ori_cmd += ' %s' % ob['extra']
        cmd = sub2(ori_cmd, ob)
        if 'cmd' in args:
            return cmd;
        else:
            return sh(cmd);

    def sysbench(*args, **ob):
        def do_before_test(mgr):
            force_info("begin run sysbench ...")
        def do_after_test(mgr):
            force_info("finish run sysbench ...")
        opt = sysbench_test.opt
        opt["mysql-port"] = call_(ob, 'mysql_port')
        opt["mysql-host"] = call_(ob, 'ms0.ip')

        if "cleanup" in args:
            opt["command"] = "cleanup"
        elif "prepare" in args:
            opt["command"] = "prepare"
        elif "run" in args:
            opt["command"] = "run"
        if "threads" in ob:
            opt["num-threads"] = ob["threads"]
        if "rows" in ob:
            opt["oltp-table-size"] = ob["rows"]
        if "seconds" in ob:
            opt["max-time"] = ob["seconds"]
        if "requests" in ob:
            opt["max-requests"] = ob["requests"]

        mgr = sysbench_test.Manager(opt)
        mgr.before_one = do_before_test
        mgr.after_one = do_after_test
        mgr.start()

    def tablet_jointest(*args, **ob):

        def do_before_test(mgr):
            force_info("begin run tablet_join_test ...")
            #call_(ob, 'reboot')
            #time.sleep(30)

        def do_after_test(mgr):
            call_(ob, 'collect_log', run_id="tablet_join_test")

        opt={}

        opt["port"] = call_(ob, 'mysql_port')
        opt["host"] = call_(ob, 'ms0.ip')
        opt["binary"] = call_(ob, 'localdir') + "/tools/ob_tablet_join_test"

        if "N" in ob:
            opt["N"] =  ob["N"]

        mgr = tablet_join_test.Manager(opt)
        mgr.before_one = do_before_test
        mgr.after_one = do_after_test
        mgr.start()


#    def update_cluster(*args, **ob):
#        ob['extra'] = "-e\"replace into __all_cluster(cluster_vip, cluster_port,cluster_id,cluster_role,cluster_flow_percent) values('" + call_(ob, 'ms0.ip') + "', " + str(call_(ob, 'mysql_port'))+ " ,1,1,100);\""
#        return call_(ob, "obmysql")

    def quicktest(*args, **ob):
        gcfg['_quiet_'] = True
        mysql_test.pinfo('run libmysql quicktest ...')
        call_(ob, "mysqltest","quick","disable-reboot"); 
        mysql_test.pinfo('run libmysql+ps quicktest ...')
        call_(ob, "mysqltest","quick","disable-reboot","ps"); 
        mysql_test.pinfo('run java quicktest ...')
        call_(ob, "mysqltest","quick","disable-reboot","java"); 
        mysql_test.pinfo('run java+ps quicktest ...')
        call_(ob, "mysqltest","quick","disable-reboot","java","ps"); 
               

    def mysqltest(*args, **ob):
        def do_before_test(mgr):
            sh("sync")
            if need_reboot:
                reboot_again = True
                while reboot_again:
                    mysql_test.pinfo("rebooting...")
                    ret = call_(ob, 'reboot')
                    if  ret[-1][-1] != 0:
                        force_info("reboot failed, retry after 60 seconds")
                        time.sleep(60)
                    else:
                        call_(ob, "update_cluster")
                        reboot_again = False
            else:
                call_(ob, "update_cluster")
            os.putenv('OBMYSQL_PORT', str(mgr.opt["port"]))
            os.putenv('OBMYSQL_MS0', str(mgr.opt["host"]))
            os.putenv('RS0_IP', call_(ob, 'rs0.ip'))
            os.putenv('RS0_PORT', str(call_(ob, 'rs0.port')))

            master_ups = str(call_(ob, 'get_master_ups_name'))
            if master_ups == "ups0":
                os.putenv('NEW_MASTER_UPS_IP', str(call_(ob, 'ups1.ip')))
                os.putenv('NEW_MASTER_UPS_PORT', str(call_(ob, 'ups1.port')))
                os.putenv('MASTER_UPS_IP', str(call_(ob, master_ups + ".ip")))
                os.putenv('MASTER_UPS_PORT', str(call_(ob, master_ups + ".port")))
            else:
                os.putenv('NEW_MASTER_UPS_IP', str(call_(ob, 'ups0.ip')))
                os.putenv('NEW_MASTER_UPS_PORT', str(call_(ob, 'ups0.port')))
                os.putenv('MASTER_UPS_IP', str(call_(ob, master_ups + ".ip")))
                os.putenv('MASTER_UPS_PORT', str(call_(ob, master_ups + ".port")))

            os.putenv('UPS0_PORT', str(call_(ob, 'ups0.port')))
            os.putenv('UPS0_IP', str(call_(ob, 'ups0.ip')))
            os.putenv('LOCAL_DIR', str(call_(ob, 'localdir')))
        def do_after_test(mgr):
            if need_collect:
                call_(ob, 'collect_log', run_id=mgr.test)
            if gcfg['stop']:
                mgr.stop = True;

        need_reboot,need_collect = True,True
        with_cs_data = False
        opt = mysql_test.opt
        opt["port"] = call_(ob, 'mysql_port')


        opt["host"] = call_(ob, 'ms0.ip')

        if "quick" in args or "quicktest" in args:
            opt["test-set"] = ['hints','create','count_distinct','join_basic','group_by_1','sq_from','compare', 'ps_complex', 'update','func_in']
            need_reboot = False
            need_collect = False
        if "record" in args:
            opt["record"] = True
        if "java" in args:
            wget="wget http://10.232.4.35:8877/mytest.jar -o ./mytest.down -O ./mysql_test/java/mytest.jar"
            target='./mysql_test/java/mytest.jar'

            if os.path.exists(target) == False:
                force_info("execute cmd: " + wget)
                sh(wget)
            if  os.path.exists(target) == False:
                force_info("failed to execute cmd: " + wget)
                sys.exit(1)
            addr = os.getenv('PWD')+"/mysql_test/java:" + os.getenv('PATH')
            os.environ['PATH'] = addr
        else:
            wget="wget http://10.232.4.35:8877/mysqltest -o mysqltest.down -O ./mysqltest"
            target="./mysqltest"
            if os.path.exists(target) == False:
                force_info("execute cmd: " + wget)
                sh(wget)
                sh("chmod +x ./mysqltest")
            if  os.path.exists(target) == False:
                force_info("failed to execute cmd: " + wget)
                sys.exit(1)
            addr = os.getenv('PWD')+":" + os.getenv('PATH')
            os.environ['PATH'] = addr
                
        if "ps" in args:
            opt['ps_protocol'] = True
        elif "test" in args:
            opt["record"] = False
        if "with-cs-data" in args:
            with_cs_data = True
        if "disable-reboot" in args:
            need_reboot = False
        if "filter" in ob:
            opt["filter"] = ob["filter"]
        if "collect" in ob:
            if ob["collect"] == 'False':
                need_collect = False
        if "testset" in ob:
            opt["test-set"] = ob["testset"].split(',')
        if "testpat" in ob:
            # remove "test-set" that mysqltest'll ignore it
            if "test-set" in opt:
                del opt["test-set"]
            opt["test-pattern"] = ob["testpat"]

        mgr = mysql_test.Manager(opt)
        mgr.before_one = do_before_test
        mgr.after_one = do_after_test
        mgr.start()

    def snapshot(*args, **ob):
        dir = call_(ob, 'dir')
        ms_log = os.path.relpath(call_(ob, 'ms0.log'), dir)
        rs_log = os.path.relpath(call_(ob, 'rs0.log'), dir)
        ups_log = os.path.relpath(call_(ob, 'ups0.log'), dir)
        cs_log = os.path.relpath(call_(ob, 'cs0.log'), dir)
        bkdir = "snapshot"
        bkfile = "backup-%s.tar.gz" % time.strftime("%F_%k%M%S")

        filelist = os.listdir(dir)
        filelist = filter(lambda x: not os.path.isdir(dir + "/" + x), filelist)
        filelist = filter(lambda x: re.match('.*core.*', x), filelist)
        if len(filelist) != 0:
            core = max(filelist, key=lambda x: os.stat("%s/%s" % (dir,x)).st_mtime)
        else:
            core = ""

        cmd = "cd {0} && mkdir -p snapshot && tar -cvzf snapshot/{1} bin/chunkserver bin/rootserver bin/mergeserver bin/updateserver {2} {3} {4} {5} {6}".format(dir, bkfile, core, ms_log, cs_log, ups_log, rs_log)

        force_info("begin make snapshot of files, bin, log, core...")
        if not os.system(cmd):
            force_info("snapshot done!")
        else:
            force_info("snapshot error! see last error above!")

    def save_master_log(target='log', **ob):
        master = get_master_ups_name2(**ob)
        if not master: raise Fail('no master ups present!')
        return call_(ob, '%s.save_log'%(master), target)

    cleanup_for_test = 'seq: stop cleanup ct.stop ct.clear'
    start_for_test = 'seq: reboot sleep[1] start_servers'
    start_ct_for_test = 'seq: ct.reboot sleep[1] ct.start'
    end_for_test = 'seq: stop collect_log'

    id = 'all: .+server id'
    pid = 'par: .+server pid'
    version = 'all: .+server version'
    set_obi_role = 'call: rs0.set_obi_role'
    boot_strap = 'call: rs0.boot_strap'
    print_schema = 'call: rs0.print_schema'
    start_servers = 'par: .+server start'
    debugall = 'call: rs0.kill -41'
    start = 'seq: start_servers debugall'
    stop = 'par: .+server kill_by_name'
    force_stop = 'par: .+server kill_by_name -9'
    rsync = 'all: .+server rsync'
    rsync_cs_sstable = 'all: chunkserver rsync_cs_sstable'
    conf = 'all: .+server conf'
    mkdir = 'all: .+server mkdir'
    cleanup = 'all: .+server rmdir'
    collect_server_log = 'all: .+server collect_log'
    collect_log = 'seq: collect_server_log ct.collect_log'
    ssh = 'all: .+server ssh'
    ct_check_local_file = 'all: .*clientset check_local_file'
    ct_rsync = 'all: .*clientset rsync'
    ct_configure = 'all: .*clientset configure_obi'
    ct_prepare = 'all: .*clientset prepare'
    reload_conf = 'all: .+server reload_conf'
    reconf = 'seq: conf rsync reload_conf'
    restart = 'seq: force_stop conf rsync mkdir ct_rsync start'
    touch_and_link_log_file = 'all: .+server touch_and_link_log_file'
    exec_init_sql = '''sh: mysql -h ${ms0.ip} -P ${ms0.mysql_port} -uadmin -padmin -e "\. $init_sql_file" '''
    cs_data_prepare = 'seq: prepare_cs_data'
    switch_to_slave_obi = 'seq: set_obi_role_until_success[obi_role=OBI_SLAVE] all_do[updateserver,kill_by_name,-9] all_do[updateserver,clear_clog] all_do[updateserver,start]'
    reboot = 'seq: check_local_bin check_ssh check_dir ct_check_local_file force_stop cleanup conf rsync mkdir ct_rsync ct_prepare rsync_cs_sstable tc_prepare touch_and_link_log_file start set_obi_role_until_success boot_strap_maybe'
    reconfigure = 'seq: force_stop hosts.reconfigure'
    return locals()

def ClientAttr():
    id = '$client@$ip:$dir'
    role = 'client'
    ssh = '''sh: ssh -t $ip $_rest_'''
    rsync = '''sh: cp ${obi.rs0.schema} $client/${obi.app_name}.schema; rsync -az $client $usr@$ip:$dir'''
    start = '''sh: ssh $usr@$ip "${client_env_vars} client_idx=$idx $dir/$client/stress.sh start ${type} ${client_start_args}"'''
    clear = '''sh: ssh $usr@$ip "$dir/$client/stress.sh clear"'''
    cleanup = '''sh: ssh $usr@$ip "$dir/$client/stress.sh clear"'''
    tail = '''sh: ssh $usr@$ip "$dir/$client/stress.sh tail_"'''
    stop = 'sh: ssh $usr@$ip $dir/$client/stress.sh stop ${type}'
    check = 'sh: ssh $usr@$ip $dir/$client/stress.sh check ${type}'
    collect_log = '''sh: mkdir -p $collected_log_dir/$run_id && scp $usr@$ip:$dir/$client/*.log $collected_log_dir/$run_id/ '''
    def check_correct(session=None, **self):
        if type(session) != dict: raise Fail('invalid session')
        if not session.has_key('last_error'): session['last_error'] = ''
        error = popen(sub2('ssh $usr@$ip $dir/$client/stress.sh check_correct', self))
        is_ok = error == session['last_error']
        session['last_error'] = error
        if is_ok:
            return 0
        else:
            print error
            return 1
    reboot = 'seq: stop rsync clear start'
    return locals()

def ClientSetAttr():
    id = 'all: client id'
    role = 'clientset'
    custom_attrs = {}
    nthreads = 5
    def client_op(self, role, op, *args, **kw_args):
        def mycall(self, path, *args, **kw_args):
            ret = call(self, path, *args, **kw_args)
            return ret
        return [mycall(self, '%s.%s'%(k, op), *args, **kw_args) for k, v in sorted(self.items(), key=lambda x: x[0]) if type(v) == dict and re.match(role, v.get('role', ''))]
    def check_alive(type='all', **self):
        return all(map(lambda cli: call_(cli, 'check', _quiet_=True, type=type) == 0, get_match_child(self, 'client$').values()))
    def check_new_error(**self):
        return all(map(lambda cli: call_(cli, 'check_correct', _quiet_=True, type='all') == 0, get_match_child(self, 'client$').values()))
    def configure_obi(**self):
        pass
    check_local_file = 'all: client check_local_file'
    reconfigure = 'seq: stop hosts.reconfigure'
    conf = 'all: client conf'
    prepare = 'all: client prepare'
    rsync = 'all: client rsync'
    start = 'all: client start type=all'
    stop = 'all: client stop type=all'
    clear = 'all: client clear type=all'
    check = 'all: client check type=all _quiet_=True'
    check_correct = 'all: client check_correct type=all _quiet_=True'
    collect_log = 'all: client collect_log'
    def require_start(type='all', **self):
        return check_until_timeout(lambda : check_alive(type=type, **self) and IterEnd() or (call_(self, 'start', **self.get('__dynamic__', {})) and 'try start client'), 30, 1)
    reboot = 'seq: stop conf rsync clear obi.require_start require_start'
    restart = 'seq: stop conf rsync obi.require_start require_start'
    return locals()

role_vars = dict_filter_out_special_attrs(Role())
obi_vars = dict_filter_out_special_attrs(ObInstance())
client_vars = dict_filter_out_special_attrs(ClientAttr())
ct_vars = dict_filter_out_special_attrs(ClientSetAttr())

def make_role(role, **attr):
    return dict_updated(role_vars, idx=ObCfg.role_counter.next(), role=role, **attr)

def RootServer(ip='127.0.0.1', port='$rs_port', **attr):
    return make_role('rootserver', ip=ip, port=port, role_data_dir='data/rs_commitlog', **attr)

def UpdateServer(ip='127.0.0.1', port='$ups_port', inner_port='$ups_inner_port', lsync_ip='', lsync_port='$default_lsync_port', master_ip='', master_port='$ups_port', **attr):
    return make_role('updateserver', ip=ip, port=port, inner_port=inner_port,
                     lsync_ip=lsync_ip, lsync_port=lsync_port, master_ip=master_ip, master_port=master_port,
                     role_data_dir='data/ups_commitlog data/ups_data/raid*/store*', **attr)

def MonitorServer(ip='127.0.0.1', port='$monitor_port', **attr):
    return make_role('monitorserver', ip=ip, port=port, role_data_dir='data/mon', **attr)

def ChunkServer(ip='127.0.0.1', port='$cs_port', **attr):
    return make_role('chunkserver', ip=ip, port=port, role_data_dir='data/cs/*/$app_name/sstable', **attr)

def MergeServer(ip='127.0.0.1', port='$ms_port', **attr):
    return make_role('mergeserver', ip=ip, port=port, role_data_dir='', **attr)

def ObProxy(ip='127.0.0.1', port='$proxy_port', **attr):
    proxy_start = '''sh: ssh $usr@$ip '(sleep 1; cd $dir; ulimit -c unlimited; pgrep -u $usr -f "^$exe" || \
    iof_ctrl=log/iof.$ip:$port LD_LIBRARY_PATH=$dir/lib:/usr/local/lib:/usr/lib:/usr/lib64:$LD_LIBRARY_PATH \
    $exe -p $port --obhost=${rs0.ip} --obport=${rs0.port} --loglevel=INFO --logpath=log/$role.log.$ip:$port \
    --obloglevel=INFO --oblogpath=log/obapi.log $_rest_) >proxy.log 2>&1 &' '''
    return make_role('proxyserver', ip=ip, port=port, role_data_dir='', start=proxy_start, **attr)

def LsyncServer(ip='127.0.0.1', port='$default_lsync_port', start_file_id=1, retry_wait_time_us=100000, **attr):
    return make_role('lsyncserver', ip=ip, port=port, start_file_id=start_file_id, retry_wait_time_us=100000, **attr)

def OBI(obi_role='OBI_MASTER', port_suffix=None, masters=[], slaves=[], proxys=[], monitors=[], hosts=[], need_lsync=False, fake_ms=False, **attr):
    session = dict()
    obi_idx = ObCfg.obi_counter.next()
    if type(hosts) == dict: hosts.update(cfg_cache_path=ObCfg.cfg_cache_path, dir='ob%d'%(obi_idx))
    if not port_suffix: port_suffix = obi_idx
    if not hosts: selected_hosts = ObCfg.default_hosts
    elif type(hosts) == dict: selected_hosts = call_(hosts, 'get_hosts') or ['localhost']
    elif type(hosts) != list: raise Fail("hosts is not list and not dict")
    else: selected_hosts = hosts
    deried_vars = dict(
        role = 'obi',
        session = session,
        obi_idx=obi_idx,
        rs_port = ObCfg.rs_port + port_suffix,
        ups_port = ObCfg.ups_port + port_suffix,
        ups_inner_port = ObCfg.ups_inner_port + port_suffix,
        monitor_port = ObCfg.monitor_port + port_suffix,
        cs_port = ObCfg.cs_port + port_suffix,
        ms_port = ObCfg.ms_port + port_suffix,
        mysql_port = ObCfg.mysql_port + port_suffix,
        proxy_port = ObCfg.proxy_port + port_suffix)
    if not get_match_child(attr, '^rootserver$'):
        if not masters: masters = selected_hosts
        if not slaves: slaves = selected_hosts
        if not proxys: proxys = selected_hosts
        if not masters or not slaves:
            raise Fail('not enough master or slave support!')
        for idx, master in enumerate(masters):
            deried_vars['rs%d'%(idx)] = RootServer(master)
            deried_vars['ups%d'%(idx)] = UpdateServer(master)
            if fake_ms:
                deried_vars['fake_ms%d'%(idx)] = MergeServer(master, mysql_port=2828)
            if need_lsync:
                deried_vars['lsync%d'%(idx)] = LsyncServer(master)
        for idx, slave in enumerate(slaves):
            deried_vars['cs%d'%(idx)] = ChunkServer(slave)
            deried_vars['ms%d'%(idx)] = MergeServer(slave)
        for idx, mon in enumerate(monitors):
            deried_vars['mon%d'%(idx)] = MonitorServer(mon)
        if any((v.get('client') == 'bigquery' or v.get('client') == 'sqltest' ) for v in get_match_child(attr, 'client').values()):
            for idx, proxy in enumerate(proxys):
                deried_vars['proxyserver%d'%(idx)] = ObProxy(proxy)
    obi = dict_updated(obi_vars, obi_role=obi_role, hosts=hosts, **dict_merge(deried_vars, attr))
    if not get_match_child(obi, '^.*server$'):
        raise Fail('ob%d: not even one server defined: default_hosts=%s!'%(obi_idx, ObCfg.default_hosts))
    if (any((v.get('client') == 'bigquery' or v.get('client') == 'sqltest' ) for v in get_match_child(attr, 'client').values()) and not get_match_child(obi, '^proxyserver$')):
        raise Fail('ob%d: no proxy server defined, but you request use "bigquery" or "seqltest" as client'%(obi_idx))
    obi.update(local_servers = ObCfg.local_servers)
    if get_match_child(obi, '^lsyncserver$'):
        obi['local_servers'] += ',lsyncserver'
    if get_match_child(obi, '^proxyserver$'):
        obi['local_servers'] += ',proxyserver'
    if get_match_child(obi, '^monitorserver$'):
        obi['local_servers'] += ',monitorserver'
    for ct in get_match_child(obi, '^.*clientset$').values():
        ct.update(obi=obi)
    def AfterLoadHook(obcfg):
        saved_quiet_value = gcfg.get('_quiet_')
        gcfg.update(_quiet_=True)
        call(obi, "ct_configure")
        gcfg.update(_quiet_=saved_quiet_value)
    ObCfg.after_load_hook.append(AfterLoadHook)
    return obi

def Client(ip='127.0.0.1', cfg=None, **attr):
    if not cfg: raise Fail('client cfg not set')
    cfg_path = search_file(cfg, ObCfg.module_search_path)
    if not cfg_path: raise Fail('not found cfg', cfg)
    deried_vars = load_file_vars(cfg_path, globals())
    deried_vars.update(deried_vars.get('client_custom_attr', {}))
    session = dict()
    return dict_updated(client_vars, ip=ip, session=session, **dict_merge(deried_vars, attr))

def CT(client='mixed_test', hosts=[], **attr):
    if not os.path.exists(client): raise Fail('no dir exists', client)
    idx = ObCfg.ct_counter.next()
    if type(hosts) == dict: hosts.update(cfg_cache_path=ObCfg.cfg_cache_path, dir='ct%d'%(idx))
    if not hosts: selected_hosts = ObCfg.default_hosts
    elif type(hosts) == dict: selected_hosts = call_(hosts, 'get_hosts') or ['localhost']
    elif type(hosts) != list: raise Fail("hosts is not list and not dict")
    else: selected_hosts = hosts
    common_vars = dict_updated(dict(cfg='%s/%s.py'%(client, client)), **attr)
    deried_vars = {}
    if not get_match_child(attr, '^.*client$'):
        if not hosts: hosts = ObCfg.default_hosts[:3]
        for idx, ip in enumerate(selected_hosts):
            deried_vars['c%d'%(idx)] = Client(ip, idx=idx, **dict_updated(common_vars, **attr.get('c%d'%(idx), {})))
    ct = dict_updated(ct_vars, client=client, idx=idx, hosts=hosts, **dict_merge(deried_vars, attr))
    if not get_match_child(ct, '^.*client$'):
        raise Fail('not even one client defined: default_hosts=%s!'%(ObCfg.default_hosts))
    return ct

class ObCfg:
    def all_do(pat, method, *arg, **ob):
        return par_do(ob, pat, method, *arg)

    def sleep(*arg, **ob):
        sleep_time = float(ob.get('sleep_time', 1))
        time.sleep(sleep_time)
        return sleep_time
    def check_ssh(timeout=1, **attr):
        hosts = ['%s@%s'%(ObCfg.usr, h) for h in ObCfg.all_hosts]
        result = async_get(async_map(lambda ip: check_ssh_connection(ip, timeout), hosts), timeout+1)
        return map(lambda ip,ret: (ip, ret == 0 and 'OK' or 'Fail'), hosts, result)
    new_style_config = 'True'
    all_hosts = load_file_vars('gcfg.py').get('host_list', [])
    obi_counter = itertools.count(1)
    role_counter = itertools.count(1)
    ct_counter = itertools.count(1)
    case_counter = itertools.count(1)
    localdir = os.path.realpath('.')
    dev = popen(r'/sbin/ifconfig | sed -n "1s/^\(\S*\).*/\1/p"').strip()
    ip = popen(r'/sbin/ifconfig |sed -n "/inet addr/s/ *inet addr:\([.0-9]*\).*/\1/p" |head -1').strip()
    addr = '$ip:$port'
    local_ip = ip
    usr = os.getenv('USER')
    uid = int(popen('id -u'))
    tpl = load_file_vars('%s/tpl.py'%(_base_dir_), globals())
    app_name='${name}.$usr'
    home = os.path.expanduser('~')
    dir = '$home/${name}'
    ver=''
    mmrs = '${rs0.ip}:${rs0.port}'
    # schema = 'etc/${app_name}.schema'
    schema = 'etc/schema.ini'
    max_sstable_size=268435456
    merge_thread_per_disk = 2
    total_memory_limit = 11
    table_memory_limit = 6
    active_mem_limit_gb = 3
    minor_num_limit = 6
    blockcache_size_mb = 1024
    blockindex_cache_size_mb = 2048
    warm_up_time_s = 300
    workdir = '$home/ob.workdir'
    data_dir='/data'
    sub_dir_list = '{1..10}'
    sub_data_dir = '$app_name'
    clog_dir = '$data_dir/1'
    sstable_dir_list = '$data_dir/$sub_dir_list'
    exe = '$dir/bin$ver/$role'
    cfg = '$dir/etc/$role.conf.$ip:$port'
    collected_log_dir = '$localdir/collected_log'
    self_id = '$name'
    start_time = get_ts_str()
    run_id = '$self_id.$start_time'
    trace_log_level='debug'
    log_sync_type = 1
    log_size = 64 # MB
    log_level = 'debug'
    _port_base = 50 * (uid % 1000)
    rs_port = _port_base + 0
    ups_port = _port_base + 10
    ups_inner_port = _port_base + 20
    monitor_port = _port_base + 21
    cs_port = _port_base + 30
    ms_port = _port_base + 40
    proxy_port = _port_base + 45
    mysql_port = _port_base + 46
    default_lsync_port = _port_base + 50
    module_search_path = ['.', _base_dir_]
    default_hosts = [ip]
    after_load_hook = []
    server_ld_preload = ''
    environ_extras = ''
    server_start_environ = 'LD_LIBRARY_PATH=./lib:/usr/local/lib:/usr/lib:/usr/lib64:/usr/local/lib64:$LD_LIBRARY_PATH LD_PRELOAD="$server_ld_preload" $environ_extras'
    local_servers = 'rootserver,updateserver,mergeserver,chunkserver'
    local_tools = 'rs_admin,ups_admin'
    rs_admin_cmd = '$localdir/tools$ver/rs_admin'
    ups_admin_cmd = '$localdir/tools$ver/ups_admin'
    client_cmd = '$localdir/tools$ver/client'
    convert_switch_log = 0
    #replay_checksum_flag = 1
    replay_checksum_flag = 0
    lsync_retry_wait_time_us = 100000
    total_memory_limit = 9
    table_memory_limit = 4
    warm_up_time_s = 30
    blockcache_size_mb = 512
    blockindex_cache_size_mb = 1024
    active_mem_limit_gb = 2
    frozen_mem_limit_gb = 1 # not used anymore
    wait_slave_sync_time_us = 0
    boot_strap_timeout = 60000000
    cfg_cache_path = '.cfg_cache'
    init_sql_file = 'init.sql'
    local_sstable_dir = ''
    remote_sstable_cache_dir = 'sstable.cache'
    cs_data_path = '$dir/data/cs'
    @staticmethod
    def get_cfg(path):
        cfg = ObCfg.get_cfg_(path)
        [v.get('name') or v.update(name=k) for k,v in get_match_child(cfg, 'obi').items()]
        return cfg
    @staticmethod
    def get_cfg_(path):
        if not os.path.exists(path):
            return dict_updated(load_str_vars(ObCfg.tpl.get("simple_config_template"), globals()), __parent__=[get_class_vars(ObCfg)])
        else:
            return dict_updated(load_file_vars(path, globals()), __parent__=[get_class_vars(ObCfg)])


def load_file(*file_list):
    for file in file_list:
        execfile('%s/%s'%(_base_dir_, file), globals())

def stop(sig, frame):
    print "catch sig: %d"%(sig)
    if gcfg['stop']:
        sys.exit(1)
    gcfg['stop'] = True

if __name__ == '__main__':
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    list_args, kw_args = parse_cmd_args(sys.argv[1:])
    list_args or usages()
    gcfg['_dryrun_'] = kw_args.get('_dryrun_') == 'True'
    gcfg['_verbose_'] = kw_args.get('_verbose_') == 'True'
    gcfg['_quiet_'] = kw_args.get('_quiet_') == 'True'
    _loop_ = int(kw_args.get('_loop_', '1'))
    method, rest_args = list_args[0], list_args[1:]
    method = method.split(':', 1)
    if len(method) == 2:
        config, method = method
    else:
        config, method = get_last_matching_file('.', '^config[0-9]*.py$'), method[0]
    # info('Use "%s" as Config File'%(config))
    # junyue start #
    if method.find("mysqltest") != -1:
        gcfg['_quiet_'] = True
    # junyue end #
    if method.find("dosql") != -1:
        gcfg['_quiet_'] = True
    try:
        ob_cfg = ObCfg.get_cfg(config)
    except Fail as e:
        print e
        sys.exit(1)
    ob_cfg = add_parent_link(ob_cfg)
    ob_cfg.update(help=lambda *ls,**kw: usages())
    if method.find("list") != -1:
        gcfg['_quiet_'] = True
        pref = len(rest_args) and len(rest_args[0]) and rest_args[0] or ""
        pref = re.sub('.?[^.]*$', '', pref)
        obj = find_attr(ob_cfg, pref)
        if type(obj) == dict:
            margs = [pref and "%s.%s" % (pref,k) or k \
                         for k in obj.keys() if not re.match('^__', k)]
            print(" ".join(margs))
            # for k in obj.keys():
            #     if not re.match("^__", k):
            #         print pref and "%s.%s" % (pref,k) or k
    for hook in ObCfg.after_load_hook:
        hook(ob_cfg)
    for i in range(_loop_):
        if _loop_ > 1:
            print 'iter %d'%(i)
        try:
            for m in multiple_expand(method):
                #info(pformat(dict_strip(call(ob_cfg, m, *rest_args, **kw_args))))
                key, result = call(ob_cfg, m, *rest_args, **kw_args)
                result = dict_strip(result)
                info(pformat((key, result)))
                sys.stdout.flush()
        except Fail as e:
            print e
            sys.exit(1)
