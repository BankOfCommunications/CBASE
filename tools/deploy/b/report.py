#!/bin/env python2

'''
./report.py data_dir target.html profile_spec
'''
import sys
import os
import common
import re
import copy
from subprocess import Popen, PIPE, STDOUT
import itertools

class Fail(Exception):
    def __init__(self, msg, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return 'Fail:%s %s'%(self.msg, self.obj != None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)

class Env(dict):
    def __init__(self, d={}, **kw):
        dict.__init__(self)
        self.update(d, **kw)

    def __getattr__(self, name):
        return self.get(name)

def find_attr(env, key):
    return eval(key, env, globals())

def sub(_str, env, find_attr=find_attr):
    '''>>> sub('$ab $$cd ${ef} $gh ${ij} $gh.i ${ij.i} $kl.i ${kl.i}', dict(gh=1, ij=2, kl=dict(i=3)))
'$ab $cd ${ef} 1 2 1.i ${ij.i} $kl.i 3'
>>> sub('${{a=2}} a=$a', {})
' a=2'
'''
    new_env = copy.copy(env)
    def handle_repl(m):
        def remove_brace(x):
            return re.sub('^{(.*)}$', r'\1', x or '')
        if m.group(1):
            expr = m.group(1)
            try:
                exec expr in globals(), new_env
            except Exception, e:
                print e, traceback.format_exc()
            return ''
        else:
            origon_expr = m.group(2) or m.group(3)
            expr = remove_brace(origon_expr)
            try:
                value = find_attr(new_env, expr)
                if type(value) == int or type(value) == float:
                    return '%.5g'%(value)
                if value == None or type(value) != str and type(value) != int and type(value) != float:
                    return '$%s'%(origon_expr)
                return str(value)
            except Exception, e:
                print 'WARN: sub():', e, 'when sub:', origon_expr
                #return 'X'
                return '$%s'%(origon_expr)
                #raise Fail('no sub', origon_expr)
    return re.sub('(?s)(?<![$])\${{(.+?)}}|\$(\w+)|\$({.+?})', handle_repl, _str).replace('$$', '$')

def sub2(template, env):
    old, cur = "", template
    for i in range(10):
        if cur == old:break
        old, cur = cur, sub(cur, env)
    return cur

def read(path):
    f = open(path, 'r')
    content = f.read()
    f.close()
    return content

def wait_child(p, timeout=-1):
    if timeout < 0: return p.wait()
    start_time = time.time()
    while p.returncode == None and time.time() < start_time + timeout:
        p.poll()
        time.sleep(0.01)
    else:
        try:
            p.terminate()
        except OSError, e:
            pass
        return -1
    return p.returncode

def popen(cmd, cwd=None, exception_on_fail=False, timeout=-1):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, cwd=cwd)
    output = p.communicate()[0]
    err = wait_child(p, timeout)
    if err:
        if exception_on_fail:
            raise Fail('popen Fail', cmd)
        output = 'Popen Error: dir=%s %s'%(cwd, cmd) + output
    return output

def show_help():
    print __doc__

def query_single_data_dir(single_data_dir, sql):
    cmd = "b/tquery.py '%s/$name.tab' '%s'" %(single_data_dir, sql)
    return popen(cmd)

def parse(spec):
    pat = 'c(?P<client_num>\d+)x(?P<thread_num>\d+)_(?P<write_type>[a-z]+)(?P<trans_size>\d+)_(?P<table_name>.+)'
    m = re.match(pat, spec)
    if not m: raise Fail('not valid spec %s'%(spec))
    return dict_updated(m.groupdict(), self=spec, spec=spec)

def rt_tps(spec):
    return '${%(self)s.rt * %(trans_size)s}/${%(self)s.tps * %(client_num)s}'%(parse(spec))

def make_img_html(url):
    return '<div>%s</div><img src="%s" alt="%s"/>'%(url, url, "image not found!")
    #return '<div>%s</div><img src="%s" alt="%s" width="400" height="300"/>'%(url, url, "image not found!")

def load_single_data_dir(data_dir):
    data = object()
    spec = os.path.basename(data_dir)
    meta = parse(spec)
    def query(sql):
        return query_single_data_dir(data_dir, sql).strip().split('\n')[-1].split()
    try:
        tps, rt, sc = query('select avg(tps),avg(rt),count(tps) from t_client')
        tps, rt, sc = float(tps) * int(meta.get('client_num')), float(rt) * int(meta.get('trans_size')), float(sc)
    except Exception,e:
        print e
        tps, rt, sc = 0, 0, 0
    try:
        disk, net, batch = query('select avg(disk),avg(net),avg(end_id-start_id) from t_flush')
        disk, net, batch = float(disk), float(net), float(batch)
    except Exception, e:
        print e
        disk, net, batch = 0, 0, 0
    img_dict = dict(('%s_img'%(i), make_img_html('%s/%s.png'%(spec, i))) for i in 'rt tps batch disk net'.split())
    return dict(tps=tps, rt=rt, sc=sc, disk=disk, net=net, batch=batch,
                **dict_updated(meta, **img_dict))
    
def get_perf_data(data_dir):
    env = dict(log_dir=data_dir, plot_url='http://10.232.35.40:8001/home/yuanqi.xhf/ob.dev/tools/deploy/%s'%(data_dir))
    for d in os.listdir(data_dir):
        if os.path.exists('%s/%s/profile.done'%(data_dir, d)):
            env[d] = Env(load_single_data_dir('%s/%s'%(data_dir, d)))
    return env

def chext(path, ext):
    return re.sub(r'(\.[^/.]*)$', ext, path)
def write(path, content):
    with open(path, 'w') as f:
        f.write(content)
def org2html(src, target):
    cmd = """emacs --batch --execute '(add-to-list `load-path "/home/yuanqi.xhf/.emacs.d/local/org/lisp")' --execute '(require `org)' --visit=%s --execute '(progn (setq org-export-headline-levels 2)
(setq org-export-html-postamble "") (setq org-export-html-preamble "")
(setq org-export-html-style "<link rel=\\"stylesheet\\" type=\\"text/css\\" href=\\"$fsh.css\\">")
(setq org-export-htmlize-output-type `css) (org-export-as-html-batch))'""".replace('\n', ' ')
    output = popen(cmd.replace('\n', ' ') % src)
    return output

def write_html(target, content):
    write(chext(target, '.org'), content)
    org2html(chext(target, '.org'), target)
    return target

def dict_updated(d, **kw):
    new_dict = copy.copy(d)
    new_dict.update(**kw)
    return new_dict

def list_merge(ls):
    '''>>> list_merge([[1,2], [3,4]])
[1, 2, 3, 4]'''
    return reduce(lambda a,b: list(a)+list(b), ls, [])

def multiple_expand(str):
    '''>>> multiple_expand('ob[1,2].cs[1,2].start')
['ob1.cs1.start', 'ob1.cs2.start', 'ob2.cs1.start', 'ob2.cs2.start']
>>> multiple_expand('[abc]')
['abc']'''
    return [''.join(parts) for parts in itertools.product(*[re.split('[ ,]+', i) for i in re.split('\[(.*?)\]', str)])]

def expand(spec):
    return list_merge(map(multiple_expand, spec.split()))

def li(format='%s', sep=' '):
    return lambda seq: sep.join([format% i for i in seq])

def render_table(table):
    tpl = """<div><table border="1">
  ${li('<tr>%s</tr>')([li('<th>%s</th>')(row) for row in data[:1]])}
  ${li('<tr>%s</tr>')([li('<td>%s</td>')(row) for row in data[1:]])}
  </table></div>"""
    return sub(tpl, dict(data=table))

def plot_table(spec_list, column):
    def data_row(spec):
        return ['${%s.%s}'%(spec, i) for i in column.split()]
    return render_table([column.split()] + [data_row(spec) for spec in spec_list])

def plot_all(spec_list):
    avg_columns, img_columns = 'spec rt tps batch net disk:spec rt_img tps_img batch_img net_img disk_img'.split(':')
    avg_columns, img_columns = 'spec rt tps:spec rt_img tps_img'.split(':')
    return '<h3>AVG</h3>%s\n<h3>Plot</h3>%s\n'%(plot_table(spec_list, avg_columns), plot_table(spec_list, img_columns))

if __name__ == '__main__':
    len(sys.argv) == 4 or show_help() or sys.exit(1)
    perf_data = get_perf_data(sys.argv[1])
    perf_data.update(profile_spec=sys.argv[3])
    print write_html(sys.argv[2], sub2(read('b/report.tpl.org'), perf_data))
