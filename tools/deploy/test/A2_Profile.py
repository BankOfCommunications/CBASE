@Test(tc_prepare='all: updateserver rsync_clog', clog_src='~/ups_commitlog')
def profile_replay_log(obi1=None, loop=10, **attr):
    if type(obi1) != dict: raise Fail('ob1 is not a dict:', obi1)
    if len(get_match_child(obi1, 'updateserver').keys()) != 1: raise Fail('only support one ups!')
    def get_replay_time_log():
        return call_(obi1, 'ups0.grep_p', 'replay_local_log_profile:')
    history = [get_replay_time_log()]
    if not wait_change(get_replay_time_log, history, 6000):
        raise Fail('not found log indicate replay log finished!')
    return history[-1]

profile_case_attr = dict(test_start = 'seq: tc_update_bin tc_cleanup tc_start_obi', test_end = 'seq: tc_end')
@Test(target='profile_write.log', **profile_case_attr)
def profile_write(obi1=None, duration=60, **attr):
    if sub2('${ct.client}', obi1) != 'simple_client': raise Fail('must configure "simple_client" as CT')
    call(obi1, 'ct.reboot', duration=int(duration) * 1000000, **attr)
    def client_is_exit():
        if call_(obi1, 'ct.check_alive'):
            return 'still alive'
        return IterEnd()
    if not check_until_timeout(client_is_exit, int(duration) + 5, 1):
        raise Fail('client not exit before timeout', duration)
    save_cmd = 'scp $usr@${obi1.ct.c0.ip}:${obi1.dir}/simple_client/client.log $target # ExceptionOnFail'
    save_cmd = sub2(save_cmd, dict_updated(attr, obi1=obi1))
    if sh(save_cmd) != 0: raise Fail('save log fail!')
    return sub2('$target', attr)
