#!/usr/bin/env python2
import sys,os
import traceback
import pprint
import re, string
import copy
import itertools
import time
import shlex
from subprocess import Popen, PIPE, STDOUT
from threading import Thread
import fcntl
import socket
import struct
def ip2str(ip):
    return socket.inet_ntoa(struct.pack('I',ip))

def str2ip(str):
    return struct.unpack('I',socket.inet_aton(str))[0]

def timeformat(ts):
    return time.strftime('%Y-%m-%d %X', time.localtime(ts))

def get_ts_str(ts=None):
    if ts == None: ts = time.time()
    return time.strftime('%m%d-%X', time.localtime(ts))

class Fail(Exception):
    def __init__(self, msg, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return 'Fail:%s %s'%(self.msg, self.obj != None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)

class IterEnd(object):
    pass

class Env(dict):
    def __init__(self, d={}, **kw):
        dict.__init__(self)
        self.update(d, **kw)

    def __getattr__(self, name):
        return self.get(name)

def short_repr(x):
    if len(x) < 80: return x
    else: return x[:80] + '...'

def traceit(func):
    def wrapper(*args, **kw):
        args_repr = [repr(arg) for arg in args]
        kw_repr = ['%s=%s'%(k, repr(v)) for k,v in list(kw.items())]
        full_repr = list(map(short_repr, args_repr + kw_repr))
        print('%s(%s)'%(func.__name__, ', '.join(full_repr)))
        result = func(*args, **kw)
        print('=> %s'%(repr(result)))
        return result
    return wrapper

def kw_format(d):
    return ' '.join('%s=%s'%(k,v) for k,v in d.items())

def dict_updated(d, **kw):
    new_dict = copy.copy(d)
    new_dict.update(**kw)
    return new_dict

def dict_filter(f, d):
    return dict(filter(f, d.items()))

def dict_filter_out_special_attrs(d):
    '''>>> dict_filter_out_special_attrs(dict(__special_attr=1, normal_attr=2))
{'normal_attr': 2}'''
    return dict_filter(lambda (k,v): not k.startswith('__'), d)

def to_simple_obj(v):
    if type(v) == int or type(v) == float or type(v) == str:
        return v
    else:
        return type(v)
def dict_strip(d):
    if (type(d) == dict) and ('__parent__' in d):
        return dict((k, to_simple_obj(v)) for k,v in d.items())
    else:
        return d

def dict_map(func, d):
    '''>>> dict_map(lambda x:x*x, dict(i1=1,i2=2))
{'i1': 1, 'i2': 4}'''
    return dict((k, func(v)) for (k,v) in list(d.items()))

def dict_merge(d1, d2):
    if type(d1) != dict: return d2
    if type(d2) != dict: return d1
    for k,v in d2.items():
        d1[k] = dict_merge(d1.get(k), v)
    return d1

def dict_add(d1, d2):
    if type(d1) != dict: return d2
    if type(d2) != dict: return d1
    for k,v in d2.items():
      if type(v) != int:
        raise Exception("Not Integer")
      elif d1.has_key(k):
        d1[k] += v
      else:
        d1[k] = v
    return d1

def list_merge(ls):
    '''>>> list_merge([[1,2], [3,4]])
[1, 2, 3, 4]'''
    return reduce(lambda a,b: list(a)+list(b), ls, [])

def first_non_nil(seq):
    '''>>> first_non_nil([None, 1, 2])
1'''
    for i in seq:
        if i != None: return i

def add_parent_link(d, parent={}, self='top', all_parents=[]):
    '''
>>> a, b, c = {}, {}, {}
>>> a.update(v1=a, v2=b, c=c)
>>> b.update(v1=a, v2=b, c=c)
>>> c = add_parent_link(dict(a=a, b=b))
'''
    if type(d) != dict: return d
    d['__self__'] = self
    if d in all_parents:
        return d
    all_parents.append(parent)
    if not d.has_key('__parent__'): d['__parent__'] = []
    d['__parent__'].append(parent)
    [add_parent_link(v, d, self=k, all_parents=copy.copy(all_parents)) for k,v in d.items() if k != '__parent__']
    return d

def _get_attr(cur_node, key, searched_list):
    if id(cur_node) in searched_list: return []
    searched_list.append(id(cur_node))
    if type(cur_node) != dict: return []
    self_value = cur_node.has_key(key) and [cur_node.get(key)] or []
    parent_values = list_merge(_get_attr(e, key, searched_list) for e in cur_node.get('__parent__', []))
    return self_value + parent_values

def _find_attr(nodes, path):
    if not path: return nodes
    return _find_attr(list_merge(_get_attr(node, path[0], []) for node in nodes), path[1:])

def find_attr(d, path):
    '''
>>> # nest lookup rule
>>> a = add_parent_link(dict(i0=1, c1=dict(i1=2, c2=dict(i2=3))))
>>> assert(find_attr(a, '') == a)
>>> assert(find_attr(a, 'i0') == 1)
>>> assert(find_attr(a, 'c1.c2.i0') == 1)
>>> assert(find_attr(a, 'c1.c2.i1') == 2)
>>> assert(find_attr(a, 'c1.c2.i2') == 3)
>>> assert(find_attr(a, 'c1.c2.c1.c2.i2') == 3)
>>> # circular loop reference
>>> a, b, c = dict(i=1), dict(i=2), dict(i=3)
>>> a.update(v1=a, v2=b, c=c)
>>> b.update(v1=a, v2=b, c=c)
>>> c = add_parent_link(dict(a=a, b=b))
>>> assert(find_attr(c, 'a.i') == 1)
>>> assert(find_attr(c, 'b.i') == 2)
>>> assert(find_attr(c, 'a.b.i') == 2)
>>> assert(find_attr(c, 'a.b.c.i') == 3)
>>> # backtrack when one path fail
>>> b = add_parent_link(dict(c1=dict(c2=2), a=dict(c1=1)))
>>> assert(find_attr(b, 'a.c1') == 1)
>>> assert(find_attr(b, 'a.c1.c2') == 2)
>>> # diamond lookup
>>> a1, a2, b, c, d = dict(name='a1'), dict(name='a2'), dict(name='b'), dict(name='c'), dict(name='d')
>>> a1.update(b=b)
>>> a2.update(c=c)
>>> b.update(d=d)
>>> c.update(d=d)
>>> g = add_parent_link(dict(a1=a1, a2=a2, b=b, c=c, d=d))
>>> assert(find_attr(g, 'a1.b.d.name') == 'd')
'''
    return first_non_nil(_find_attr([d], path and path.split('.') or []))

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
            if not re.match('([\w.])+(:-.+)?$', expr):
                raise Exception('not support expr: %s'%(m.group(0)))
            exprs = re.split(':-', expr, 1)
            parent_path = re.sub('[.]?[^.]+$', '', expr)
            parent, value = find_attr(new_env, parent_path), find_attr(new_env, exprs[0])
            if 2 == len(exprs) and value == None:
                return sub(exprs[1], env, find_attr)
            try:
                if value == None or type(value) != str and type(value) != int and type(value) != float:
                    return '$%s'%(origon_expr)
                # if type(value) == int or type(value) == float:
                #     return format('%.2f'% value)
                return sub(str(value), parent)
            except Exception, e:
                print e
                raise Fail('no sub', origon_expr)
    return re.sub('(?s)(?<![$])\${{(.+?)}}|\$(\w+)|\$({.+?})', handle_repl, _str).replace('$$', '$')

def sub2(template, env):
    old, cur = "", template
    for i in range(10):
        if cur == old:break
        old, cur = cur, sub(cur, env)
    return cur

def iter_until_null(func, initial=None, terminate=lambda x: not x):
    while not terminate(initial):
        yield initial
        initial = func(initial)

def check_until_timeout(call, timeout, interval=0.1):
    end_time = time.time() + timeout
    while not gcfg.get('stop', False) and time.time() < end_time:
        result = call()
        if type(result) == IterEnd: return True
        info('wait %fs: %s'%(time.time() + timeout - end_time, result))
        sys.stdout.flush()
        time.sleep(interval)

class Thread2(Thread):
    def __init__(self, func, *args, **kw):
        self.result = None
        def call_and_set(*args, **kw):
            self.result = func(*args, **kw)
        Thread.__init__(self, target=call_and_set, *args, **kw)
        self.setDaemon(True)

    def get(self, timeout=None):
        self.join(timeout)
        #return self.isAlive() and self.result
        return self.result

def make_async_func(f):
    def async_func(*args, **kw):
        t = Thread2(f, args=args, kwargs=kw)
        t.start()
        return t
    return async_func

def async_map(f, seq):
    return map(make_async_func(f), seq)

def async_get(seq, timeout):
    return map(lambda i: i.get(timeout), seq)

def parse_cmd_args(args):
    '''>>> assert(parse_cmd_args(['1', '2', 'a=3', 'b=4']) == (['1', '2'], dict(a='3', b='4')))'''
    return [i for i in args if not re.match('^\w+=', i)], dict(i.split('=', 1) for i in args if re.match('^\w+=', i))

def multiple_expand(str):
    '''>>> multiple_expand('ob[1,2].cs[1,2].start')
['ob1.cs1.start', 'ob1.cs2.start', 'ob2.cs1.start', 'ob2.cs2.start']
>>> multiple_expand('[abc]')
['abc']'''
    return [''.join(parts) for parts in itertools.product(*[re.split('[ ,]+', i) for i in re.split('\[(.*?)\]', str)])]

def expand(spec):
    return list_merge(map(multiple_expand, spec.split()))

def lp_exists(path, suffix=''):
    '''
>>> sh('mkdir -p test-dir/a/b/c; touch test-dir/makefile test-dir/a/makefile')
0
>>> lp_exists('test-dir/a/b/c', '/makefile')
'test-dir/a'
>>> sh('rm -rf test-dir')
0'''
    paths = filter(lambda x: os.path.exists(x + suffix), iter_until_null(lambda p: re.sub('/?[^/]*$', '', p), path))
    return paths and paths[0] or None

def get_class_vars(cls):
    return dict_filter_out_special_attrs(cls.__dict__)

def replace_file_name(path, file):
    return os.path.join(os.path.dirname(path), file)

def search_file(file, search_list):
    path_list = [os.path.join(path, file) for path in search_list]
    for path in path_list:
        if os.path.exists(path): return path

GLOBALS = {}
def load_file_vars(__path__, __globals__=globals()):
    try:
        execfile(__path__, __globals__, locals())
    except Fail, e:
        raise e
    except Exception, __e:
        print __e, traceback.format_exc()
        raise Fail('load file failed!', __path__)
    return dict_filter_out_special_attrs(locals())

def load_str_vars(__str__, __globals__=globals()):
    try:
        exec __str__ in  __globals__, locals()
    except Exception, __e:
        print __e, traceback.format_exc()
    return dict_filter_out_special_attrs(locals())

def load_multi_file_vars(file_list, __globals__=globals()):
    file_env = {}
    for file in file_list:
        try:
            env = load_file_vars(file, __globals__)
        except Fail as e:
            print e
        else:
            file_env.update(env)
    return file_env

gcfg = dict(_dryrun_=False, stop=False)
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
    if gcfg.get('_dryrun_', False): return cmd
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, cwd=cwd)
    output = p.communicate()[0]
    err = wait_child(p, timeout)
    if err:
        if exception_on_fail:
            raise Fail('popen Fail', cmd)
        output = 'Popen Error: dir=%s %s'%(cwd, cmd) + output
    return output

def sh(cmd, cwd=None, exception_on_fail=False, timeout=-1):
    if gcfg.get('_dryrun_', False): return cmd
    if gcfg.get('_verbose_', False): print cmd
    p = Popen(cmd, shell=True)
    err = wait_child(p, timeout)
    if err:
        if exception_on_fail:
            raise Fail('popen Fail', cmd)
        info('cmd fail:%s'% cmd)
    return err

def sh_using_pipe(cmd, cwd=None, exception_on_fail=False, timeout=-1):
    if gcfg.get('_dryrun_', False): return cmd
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    output, errput = p.communicate()
    info(output)
    retcode = p.returncode
    if retcode:
       if exception_on_fail:
           raise Fail('popen Fail', cmd)
       info('cmd fail: %s' % cmd)
       info('err msg: %s' % errput)
    return retcode

def timed_popen(cmd, timeout):
    p = Popen(cmd, shell=True, stdout=file('/dev/null'), stderr=STDOUT)
    start_time = time.time()
    ret = None
    while p.returncode == None and time.time() < start_time + timeout:
        ret = p.poll()
        time.sleep(0.01)
    else:
        try:
            p.terminate()
        except OSError, e:
            pass
    return ret

def make_non_block(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

def mypopen(cmd, timeout=3):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    make_non_block(p.stdout.fileno())
    start_time = time.time()
    output = []
    while p.returncode == None and time.time() < start_time + timeout:
        ret = p.poll()
        time.sleep(0.01)
        output.append(p.stdout.read())
    else:
        try:
            p.terminate()
        except OSError, e:
            pass
    return ''.join(output)

def ssh(host, cmd, cwd='.', usr='$USER'):
    return sh('ssh %s@%s "%s"'%(usr, host, cmd.replace('"', '\"')))

def mkdir(path):
    os.path.exists(path) or os.mkdir(path)

def ls_re(dir, pat):
    return filter(lambda path: re.match(pat, path), os.listdir(dir))

def get_last_matching_file(dir, pat):
    matched_file_list = sorted(ls_re(dir, pat))
    if not matched_file_list: raise Fail('can not found even one matching file: dir=%s, pat=%s'%(dir, pat))
    return matched_file_list[-1]

def existed(**file_list):
    return filter(lambda f: os.path.exists(f), file_list)

def get_one_existed(**file_list):
    existed_files = existed(**file_list)
    if not existed_files: raise Fail('can not found a existed file', file_list)
    return existed_files[0]

def read(path):
    f = open(path, 'r')
    content = f.read()
    f.close()
    return content

def safe_read(path):
    try:
        return read(path)
    except Exception,e:
        return ''

def write(path, content):
    f = open(path, 'w')
    f.write(content)
    f.close()

def append(path, content):
    f = open(path, 'a')
    f.write(content)
    f.close
    return True

def load_dir_vars(path):
    if os.path.exists(path):
        return dict((i.replace('.', '_'), safe_read('%s/%s'%(path, i))) for i in os.listdir(path) if os.path.isfile('%s/%s'%(path, i)))
    else:
        return {}

def simple_obj_repr(v):
    if type(v) == int or type(v) == float or type(v) == str:
        return str(v)
    else:
        return str(type(v))

def dict_repr(d):
    return '\n'.join('%s = %s'%(k, simple_obj_repr(v)) for k,v in sorted(d.items(), key=lambda (k,v): k))

def list_repr(ls):
    return '\n'.join(map(simple_obj_repr, ls))

def obj_repr(o):
    if type(o) == dict:
        return dict_repr(o)
    elif type(o) == list or type(o) == tuple:
        return list_repr(o)
    else:
        return simple_obj_repr(o)

def info(msg):
    if not gcfg.get('_quiet_'):
        force_info(msg)

def force_info(msg):
    print('%s INFO: %s' % (time.strftime('%Y-%m-%d %X', time.localtime(time.time())), msg))

def call(d, method, *args, **kw):
    if kw.has_key('__parent__'): del kw['__parent__']
    if kw.has_key('__dynamic__'): del kw['__dynamic__']
    if gcfg.get('stop', False): raise Fail("request stop")
    def debug(msg, log_level='INFO'):
        if kw.get('_verbose_', d.get('_verbose_', 'False')) == 'True':
            sys.stdout.write('%s DEBUG: %s'%(time.strftime('%Y-%m-%d %X', time.localtime(time.time())), msg))
    info('%s%s'%(method, args))
    if type(d) != dict or type(method) != str or not method:
        raise Exception('call(type(d)=%s, type(method)=%s): invalid argument'% (type(d), type(method)))
    base_path = re.sub('[.][^.]+$', '', ('.' in method) and method or '.' + method)
    try:
        #debug('method:%s base_path:%s\n'%(method, base_path))
        env, _method = find_attr(d, base_path), find_attr(d, method)
        #debug('method eval to:%s\n'%(pprint.pformat(_method)))
    except Exception,e:
        sys.stderr.write("method: `%s' not defined\n"% method)
        print e
        print traceback.format_exc()
    else:
        report = None
        sh_prefix, popen_prefix, seq_prefix, call_prefix, all_prefix, par_prefix, sup_prefix = 'sh:', 'popen:', 'seq:', 'call:', 'all:', 'par:', 'sup:'
        if callable(_method):
            debug('func: %s(%s)\n'%(_method.func_name, args))
            result = _method(*args, **dict_updated(env, __dynamic__=kw, **kw))
        elif type(_method) == str:
            start_time = time.time()
            new_env = dict(__parent__=[env], _rest_=' '.join(args), **kw)
            _method = sub2(_method, new_env)
            report_match = re.search('#report:(.*)$', _method)
            _report = report_match and report_match.group(1) or None
            debug('report: %s\n'% _report)
            if _method.startswith(sh_prefix):
                debug('%s\n'% _method)
                result = sh(_method[len(sh_prefix):], exception_on_fail=re.search('# ExceptionOnFail', _method))
            elif _method.startswith(popen_prefix):
                debug('%s\n'% _method)
                result = popen(_method[len(popen_prefix):], exception_on_fail=re.search('# ExceptionOnFail', _method)).strip()
            elif _method.startswith(seq_prefix):
                info('%s'%_method)
                base_prefix = base_path and base_path + '.' or ''
                cmd_seq = [(cmd, arg and parse_cmd_args(arg.split(',')) or ([],{})) for cmd, arg in re.findall('([0-9a-zA-Z._]+)(?:\[(.*?)\])?', _method[len(seq_prefix):])]
                result = [call(d, '%s%s'%(base_prefix, cmd), *(_args + list(args)), **dict_updated(_kw, **kw)) for cmd, (_args,_kw) in cmd_seq]
            elif _method.startswith(call_prefix):
                info(_method)
                base_prefix = base_path and base_path + '.' or ''
                _args, _kw = parse_cmd_args(shlex.split(_method[len(call_prefix):]))
                if _args:
                    result = call(d, '%s%s'%(base_prefix, _args[0]), *(_args[1:]+list(args)), **dict_updated(_kw,**kw))
                else:
                    result = None
            elif _method.startswith(all_prefix):
                info(_method)
                base_prefix = base_path and base_path + '.' or ''
                _args, _kw = parse_cmd_args(shlex.split(_method[len(all_prefix):]))
                if _args:
                    result = all_do(env, _args[0], *(_args[1:]+list(args)), **dict_updated(_kw,**kw))
                else:
                    result = None
            elif _method.startswith(par_prefix):
                info(_method)
                base_prefix = base_path and base_path + '.' or ''
                _args, _kw = parse_cmd_args(shlex.split(_method[len(par_prefix):]))
                if _args:
                    result = par_do(env, _args[0], *(_args[1:]+list(args)), **dict_updated(_kw,**kw))
                else:
                    result = None
            elif _method.startswith(sup_prefix):
                debug('%s\n'% _method)
                result = sh_using_pipe(_method[len(sup_prefix):], exception_on_fail=re.search('# ExceptionOnFail', _method))
            else:
                result = _method

            # do some report after run.
            end_time = time.time()
            run_time = end_time-start_time
            profile_env = dict(__parent__=[env], start_time=start_time, end_time=end_time, run_time=run_time)
            if _report:
                report = sub2(_report, profile_env).strip()
                debug('report eval to:%s'% report)
                try:
                    report = (report, eval(report))
                except Exception,e:
                    report = (report, traceback.format_exc())
        else:
            result = _method
        if report != None:
            return method, result, report
        else:
            return method, result

def call_(d, method, *args, **kw):
    return call(d, method, *args, **kw)[1]

def get_match_child(d, pat):
    return dict_filter(lambda (k, v): type(v) == dict and re.match(pat, v.get('role', '')), d)

def get_sorted_match_child(d, pat):
    return [k for k, v in sorted(get_match_child(d, pat).items(), key=lambda (k,v): v.get('idx', 0))]

def all_do(d, pat, method, *args, **kw):
    info('all_do %s %s' % (pat, method))
    return [('%s.%s'%(c, method), call_(d, '%s.%s'%(c, method), *args, **kw)) for c in get_sorted_match_child(d, pat)]

def par_do(d, pat, method, *args, **kw):
    matched_keys = get_sorted_match_child(d, pat)
    results = async_get(async_map(lambda x: call_(d, '%s.%s'%(x, method), *args, **kw), matched_keys), kw.get('timeout', 1))
    return zip(['%s.%s'%(k, method) for k in matched_keys], results)

def wait_change(src, history=[], timeout=30, interval=1):
    start_time = time.time()
    while time.time() < start_time + timeout:
        output = src()
        if history and history[0] == output:
            time.sleep(interval)
        else:
            history.append(output)
            break
    else:
        return False
    return True
if __name__ == "__main__":
    import doctest
    doctest.testmod()
