null_case_attr = dict(test_start='test_start', test_end='test_end')
def TestRunner():
    case_pat = '^.+$'
    tc_update_bin = 'seq: obi1.update_local_bin obi2.update_local_bin'
    tc_cleanup = 'seq: obi1.cleanup_for_test obi2.cleanup_for_test'
    tc_start_obi = 'seq: obi1.start_for_test obi2.start_for_test'
    tc_start_ct = 'seq: obi1.start_ct_for_test obi2.start_ct_for_test'
    tc_end = 'seq: obi1.end_for_test obi2.end_for_test'
    test_start = 'seq: tc_update_bin tc_cleanup tc_start_obi tc_start_ct'
    test_end='seq: tc_end'
    def load_case_dict(pat):
        return load_multi_file_vars(glob.glob(pat), __globals__=globals())
    def load_case_pairs(pat):
        return sorted([(name, func) for name, func in load_case_dict(pat).items() if getattr(func, 'TEST', None)], key=lambda (name,func): func.case_idx)
    def run_hook(case, inst1, inst2, hook, case_attr):
        return call(case.__dict__, hook, ob1=inst1, ob2=inst2, **case_attr)
    def _run(desc, case_pat, case_src_pat, obi=None, keep_going=True, no_hook=False, **attr):
        keep_going = keep_going != 'False'
        no_hook = no_hook == 'True'
        desc_kw = dict([i.split('=') for i in desc.split(':')[1:]])
        _obi = find_attr(attr, desc_kw.get('obi', obi))
        if not _obi: raise Fail('no obi specified!')
        inst1, inst2 = _obi.get('inst1', None), _obi.get('inst2', None)
        _inst1, _inst2 = inst1 and find_attr(attr, inst1) or _obi, inst2 and find_attr(attr, inst2)
        result = []
        print 'testrunner.run(file_pat="%s", case_pat="%s")'%(case_src_pat, case_pat)
        for name, case in load_case_pairs(case_src_pat):
            if not getattr(case, 'TEST', None) or not re.match(case_pat, name): continue
            ts = time.time()
            case_attr = dict(self_id=name, start_time=get_ts_str(ts), obi1=_inst1, obi2=_inst2, __parent__=[attr], **dict_updated(case.__dict__, **desc_kw))
            case_attr.update(**attr.get('__dynamic__', {}))
            print '%s runcase %s'%(timeformat(ts), name)
            succ = True
            if gcfg.get('_dryrun_', False):
                print 'runcase %s'%(name)
                ret = 'dryrun'
            else:
                try:
                    [inst.update(**case.__dict__) for inst in (_inst1, _inst2) if inst]
                    if not no_hook:
                        pprint(call(case_attr, 'test_start'))
                    ret = call(case_attr, 'self')
                    pprint(ret)
                except Exception as e:
                    succ = False
                    ret = e
                    print e, traceback.format_exc()
                finally:
                    if not no_hook:
                        pprint(call(case_attr, 'test_end'))
            result.append((succ, name, get_ts_str(ts), obi, ret))
            if not succ and not keep_going: break
        failed = filter(lambda x: not x[0], result)
        return result, failed
    def run(desc='desc', case_pat='^.+$', case_src_pat='test/Test*.py', obi=None, **attr):
        result, failed = _run(desc, case_pat, case_src_pat, obi, **attr)
        print '====================All===================='
        pprint(result)
        print '==================Failed==================='
        pprint(failed)
        print '==================Summary=================='
        print 'All Case: %d, Failed Case: %d: %s'%(len(result), len(failed), ','.join([c[1] for c in failed]))
        return len(failed)
    return locals()

def Attr(**attrs):
    def decorator(f):
        for k, v in attrs.items():
            setattr(f, k, v)
        f.self = f
        return f
    return decorator

def Test(TEST=True, **attr):
    return Attr(TEST=TEST, case_idx=ObCfg.case_counter.next(), **attr)
tr_vars = dict_filter_out_special_attrs(TestRunner())

def TR(obi=None, **attr):
    return dict_updated(tr_vars, obi=obi, **attr)
