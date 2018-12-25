def expand_host(spec):
    return list_merge(map(multiple_expand, spec.split()))
host_list = expand_host('''10.232.36.[29,30,31,32,33,42]
10.232.36.[171,175,176,177,178,200]
10.232.36.[181,182,183,184,184,186,188]
10.235.162.[1,2,3,4,5,6,7,8,9]''')
[host_list.remove(h) for h in '10.232.36.32'.split() if h in host_list]
