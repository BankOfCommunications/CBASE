@Test(**null_case_attr)
def hello(self_id=None, start_time=None, **attr):
    return 'hello from %s at %s'%(self_id, start_time)

@Test(**null_case_attr)
def fail(**attr):
    raise Fail('Fail for test')

@Test(**null_case_attr)
def update_local_bin(obi1=None, obi2=None, **attr):
    return call_(obi1, 'update_local_bin'), obi2 and call_(obi2, 'update_local_bin')
    
@Test(**null_case_attr)
def pid(obi1=None, obi2=None, **attr):
    return call_(obi1, 'id'), call_(obi1, 'pid'), obi2 and call_(obi2, 'id'), obi2 and call_(obi2, 'pid')

