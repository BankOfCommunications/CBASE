@Test()
def one_cluster_check_alive(obi1=None, loop=10, **attr):
    return call_(obi1, 'random_fault_test', 'check', n=int(loop), **attr)

@Test()
def one_cluster_restart_ups(obi1=None, loop=10, **attr):
    return call_(obi1, 'random_fault_test', 'restart_server', n=int(loop), **attr)

@Test()
def one_cluster_set_master(obi1=None, loop=10, **attr):
    return call_(obi1, 'random_fault_test', 'set_master', n=int(loop), **attr)

@Test()
def one_cluster_disk_timeout(obi1=None, loop=10, **attr):
    return call_(obi1, 'random_fault_test', 'disk_timeout', n=int(loop), **attr)

@Test()
def one_cluster_net_timeout(obi1=None, loop=10, **attr):
    return call_(obi1, 'random_fault_test', 'net_timeout', n=int(loop), **attr)

@Test()
def two_cluster_change_obi(obi1=None, obi2=None, loop=10, **attr):
    if type(obi1) != dict or type(obi2) != dict: raise Fail('need at least 2 obi!')
    succ, result = call(obi1, 'change_master_obi_loop', loop=int(loop))
    if not succ: raise Fail('change obi role test fail!', result)
    return result
