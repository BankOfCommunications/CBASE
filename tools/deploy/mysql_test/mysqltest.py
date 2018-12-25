#!/usr/bin/env python2.6

# This file defines some structs and its operations about mysql test,
# and will be part of `deploy.py' script.
# Author: Fufeng
# Last change: 2013-04-26 14:17:05 #

import re
from subprocess import Popen, PIPE
import shlex
import glob
import errno
import pprint
from os import chdir, getcwd, makedirs
from os.path import basename, join, dirname, realpath
from time import strftime, time
import sys
from filter import *

def my_mkdir(path):
    try:
        makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def pheader(msg):
    print '\033[32m[==========]\033[0m %s' % msg
    sys.stdout.flush()

def pinfo(msg):
    print '\033[32m[----------]\033[0m %s' % msg
    sys.stdout.flush()

def prun(msg):
    print '\033[32m[ RUN      ]\033[0m %s' % msg
    sys.stdout.flush()
    sys.stdout.flush()

def pfail(msg):
    print '\033[31m[  FAILED  ]\033[0m %s' % msg
    sys.stdout.flush()

def psucc(msg):
    print '\033[32m[       OK ]\033[0m %s' % msg
    sys.stdout.flush()

def ppasslst(msg):
    print '\033[32m[ PASS LST ]\033[0m %s' % msg
    sys.stdout.flush()

def ppass(msg):
    print '\033[32m[  PASSED  ]\033[0m %s' % msg
    sys.stdout.flush()

def pfaillst(msg):
    print '\033[31m[ FAIL LST ]\033[0m %s' % msg
    sys.stdout.flush()

class Arguments:
    def add(self, k, v = None):
        self.args.update({k:v});

    def __str__(self):
        str = ""
        for k,v in self.args.items():
            if v != None:
                if re.match("^--\w", k):
                    str += " %s=%s" % (k, v)
                else:
                    str += " %s %s" % (k, v)
            else:
                str += " %s" % k
        return str

    def __init__(self, opt):
        self.args = dict()
        try:
            self.add("--host", opt["host"])
            self.add("--port", opt["port"])
            self.add("--tmpdir", opt["tmp-dir"])
            self.add("--logdir", "%s/log" % opt["var-dir"])
            my_mkdir(opt["tmp-dir"])
            my_mkdir("%s/log" % opt["var-dir"])
        except Exception as e:
            print "[Error] %s option not specify in configure file" % e
            raise

        self.add("--silent")
        # our mysqltest doesn't support this option
        # self.add("--skip-safemalloc")

        if "user" in opt:
            self.add("--user", opt["user"])
        if "password" in opt:
            self.add("--password", opt["password"])
        if "database" in opt:
            self.add("--database", opt["database"])
        if "charsetdsdir" in opt:
            self.add("--character-sets-dir", opt["charsetsdir"])
        if "basedir" in opt:
            self.add("--basedir", opt["basedir"])
        if "mark-progress" in opt:
            self.add("--mark-progress")
        if "ps_protocol" in opt:
            self.add("--ps-protocol")
        if "sp_protocol" in opt:
            self.add("--sp-protocol")
        if "view_protocol" in opt:
            self.add("--view-protocol")
        if "cursor_protocol" in opt:
            self.add("--cursor-protocol")
        # if "strace_client" in opt:
        #     exe = opt["strace_client"] || "strace";
        #     self.add("-o", "%s/log/mysqltest.strace %s_mysqltest" % (opt["vardir"], exe)

        self.add("--timer-file", "%s/log/timer" % opt["var-dir"]);

        if "compress" in opt:
            self.add("--compress")
        if "sleep" in opt:
            self.add("--sleep", "%d" % opt["sleep"])
        # # Turn on SSL for _all_ test cases if option --ssl was used
        # if "ssl" in opt:
        #     self.add("--ssl")
        if "max_connections" in opt:
            self.add("--max-connections", "%d" % opt["max_connections"])

        if "test-file" in opt:
            self.add("--test-file", opt["test-file"])

        # Number of lines of resut to include in failure report
        self.add("--tail-lines", ("tail-lines" in opt and opt["tail-lines"]) or 20);

        if "record" in opt and opt["record"] and "record-file" in opt:
            self.add("--record")
            self.add("--result-file", opt["record-file"])
        else:                                    # diff result & file
            self.add("--result-file", opt["result-file"])

class Tester:
    def __init__(self):
        pass

    def find_bin(self, opt):
        if 'mysqltest-bin' in opt:
            # check version
            #version = Popen([opt['mysqltest-bin'], "--version"], stdout=PIPE)
            #if version.communicate()[0].find("Ver 3.3 Distrib 5.5.27") != -1:
            return opt["mysqltest-bin"]
            #else:
            #    print("Please download Ver 3.3 Distrib 5.5.27 using cmd: wget 'http://10.232.23.18:8877/mysqltest'")
            #    return None
        else:
            try:
                p = Popen(["which", "mysqltest"], stdout=PIPE)
                path = p.communicate()[0]
                if p.returncode == 0:
                    # check version
                    #version = Popen([path.strip(), "--version"], stdout=PIPE)
                    #if version.communicate()[0].find("Ver 3.3 Distrib 5.5.27") != -1:
                    return path
                    #else:
                    #    print("Please download Ver 3.3 Distrib 5.5.27 using cmd: wget 'http://10.232.23.18:8877/mysqltest'")
                    #    return None
                else:
                    print("Please set `mysqltest-bin' option")
                    return None
            except Exception as e:
                print("%s, Please set `mysqltest-bin' option" % e)
                return None

    def run(self, test, opt):
        if None == self.find_bin(opt):
            exit(-1)

        opt["test-file"] = join(opt["test-dir"], test + ".test")
        opt["result-file"] = join(opt["result-dir"], test + ".result")
        opt["record-file"] = join(opt["record-dir"], test + ".record")

        retcode = 0
        output = ""
        errput = ""
        cmd = self.find_bin(opt) + str(Arguments(opt))
        #print cmd
        try:
            p = Popen(shlex.split(cmd), stdout = PIPE, stderr = PIPE)
            output, errput = p.communicate()
            retcode = p.returncode
        except Exception as e:
            errput = e;
            retcode = 255;
        return {"name" : test, "ret" : retcode, "output" : output,\
                "cmd" : cmd, "errput" : errput}

class Manager:
    test_set = None
    test = None
    opt = None
    before_one = None
    after_one = None
    log_fp = None
    case_index = None
    stop = False

    def __init__(self, opt):
        '''Check and autofill "opt" before run a "Tester".
        Check list contains "test-dir", "result-dir", "record-dir"
        '''
        cwd = getcwd()
        chdir(dirname(realpath(__file__)))
        if "test-dir" in opt:
            opt["test-dir"] = realpath(opt["test-dir"])
        else:
            opt["test-dir"] = realpath("t")
        if "result-dir" in opt:
            opt["result-dir"] = realpath(opt["result-dir"])
        else:
            opt["result-dir"] = realpath("r")
        if "record-dir" in opt:
            opt["record-dir"] = realpath(opt["record-dir"])
        else:
            opt["record-dir"] = realpath("r")
        if "log-dir" in opt:
            opt["log-dir"] = realpath(opt["log-dir"])
        else:
            opt["log-dir"] = realpath("log")
        if "tmp-dir" in opt:
            opt["tmp-dir"] = realpath(opt["tmp-dir"])
        else:
            opt["tmp-dir"] = realpath("tmp")
        if "var-dir" in opt:
            opt["var-dir"] = realpath(opt["var-dir"])
        else:
            opt["var-dir"] = realpath("var")
        chdir(cwd)

        my_mkdir(opt["record-dir"])
        my_mkdir(opt["log-dir"])

        self.opt = opt

    def check_tests(self):
        if "test-set" in self.opt:
            self.test_set = self.opt["test-set"]
        else:
            if not "test-pattern" in self.opt:
                self.opt["test-pattern"] = "*.test"
            pat = join(self.opt["test-dir"], self.opt["test-pattern"])
            self.test_set = [basename(test).rsplit('.', 1)[0] for test in glob.glob(pat)]

        # exclude somt tests.
        if "exclude" not in self.opt:
            self.opt["exclude"] = None
        self.test_set = filter(lambda k: k not in self.opt["exclude"], self.test_set)
        if "filter" in self.opt:
            if self.opt["filter"]=="c":
                exclude_list = c_list
            elif self.opt["filter"]=="cp":
                exclude_list = cp_list
            elif self.opt["filter"]=="j":
                exclude_list = j_list
            elif self.opt["filter"]=="jp":
                exclude_list = jp_list
            elif self.opt["filter"]=="o":
                exclude_list = o_list
            elif self.opt["filter"]=="op":
                exclude_list = op_list
            else:
                exclude_list = []
            self.test_set = filter(lambda k: k not in exclude_list, self.test_set)
            #print self.test_set
        self.test_set = sorted(self.test_set)
            

    def shrink_errmsg(self, errmsg):
        if type(errmsg) == str:
            return re.split("\nThe result from queries just before the failure was:", errmsg, 1)[0]
        elif isinstance(errmsg, Exception):
            return errmsg
        else:
            return "UNKNOWN ERR MSG"

    def result_stat(self):
        ret = ""
        total = len(self.result)
        succ = len(filter(lambda item: item["ret"] == 0, self.result))
        fail = total - succ
        ret += "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
        ret += "success %d out of %d\n" % (succ, total)
        ret += "fail tests list:\n"
        for line in [ "%-12s: %s\n" % (item["name"], item["errput"]) for item in self.result ]:
            ret += line
        ret += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n"
        return ret

    def run_one(self, test):
        self.log_fp.write('============================================================\n')
        self.log_fp.write('%s INFO: [ %s ] case start!\n' % (strftime("%Y-%m-%d %X"), test))
        self.test = test
        if self.before_one:
            self.before_one(self)

        prun("%s [ %d / %d ]" % (test, self.case_index, len(self.test_set)))
        self.case_index += 1
        start = time()
        #if test.find("ps_") == -1 :
        result = Tester().run(test, self.opt)
        #else:
        #    my_opt = self.opt.copy()
        #    my_opt['ps_protocol'] = True
        #    result = Tester().run(test, my_opt)
        during = time() - start;
        if result["ret"] == 0:
            self.log_fp.write('%s INFO: [ %s ] case success!\n' % (strftime("%Y-%m-%d %X"), result["name"]))
            psucc("%s ( %f s )" % (test, during))
        else:
            self.log_fp.write('%s INFO: [ %s ] case failed!\n' % (strftime("%Y-%m-%d %X"), result["name"]))
            self.log_fp.write("%s\n" % str(result["errput"]).strip())
            pfail("%s ( %f s )\n%s" % (test, during, self.shrink_errmsg(result["errput"])))

        if self.after_one:
            self.after_one(self)

        return result

    def start(self):
        def is_passed():
            return len(filter(lambda x: x["ret"] != 0, self.result)) == 0

        self.check_tests()
        log_file = join(self.opt["log-dir"], \
                        (self.opt["log-temp"] or "mysqltest-%s.log") %\
                        strftime("%Y-%m-%d_%X"))

        runmode = "record" in self.opt and self.opt["record"] and "Record" or "Test"
        pheader("Running %d cases ( %s Mode )" % (len(self.test_set), runmode))
        pinfo(strftime("%F %X"))
        try:
            self.log_fp = open(log_file, "w")
            self.case_index = 1
            self.result = [ self.stop or self.run_one(test) for test in self.test_set ]
            self.result = filter(lambda item: type(item) == dict, self.result);
            self.log_fp.write(self.result_stat())
        except Exception as e:
            raise
        finally:
            self.log_fp.close()

        passcnt = len(filter(lambda x: x["ret"] == 0, self.result))
        totalcnt = len(self.result)
        failcnt = totalcnt - passcnt
        pheader("%d tests run done!" % len(self.result))
        if is_passed():
            ppass("%d tests" % len(self.result))
        else:
            pfail("%d tests are failed out of %s total" % (failcnt, totalcnt))
            passlst,faillst='',''
            for i,t in enumerate(self.result):
                if t["ret"] == 0:
                    passlst=t["name"] if passlst == '' else (passlst+','+t["name"])
                else:
                    faillst=t["name"] if faillst == '' else (faillst+','+t["name"])
            ppasslst(passlst)
            pfaillst(faillst)
            exit(1)
        return self.result

if __name__ == '__main__':
    mgr = Manager(opt)
    mgr.set_tests(["bool", "default", "bigint", "compare"])
    mgr.start()

__all__ = ["Manager", "pinfo"]

# Local Variables:
# time-stamp-line-limit: 1000
# time-stamp-start: "Last change:[ \t]+"
# time-stamp-end: "[ \t]+#"
# time-stamp-format: "%04y-%02m-%02d %02H:%02M:%02S"
# End:
