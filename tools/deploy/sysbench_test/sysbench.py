#!/usr/bin/env python2.6

# This file defines some structs and its operations about sysbench test,
# and will be part of `deploy.py' script.
# Author: junyue
# Last change: 2012-10-23 10:47:32 #

import re
from subprocess import Popen, PIPE
import shlex
import glob
import errno
import pprint
from os import chdir, getcwd, makedirs
from os.path import basename, join, dirname, realpath
from time import strftime, time

def my_mkdir(path):
    try:
        makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def pfail(msg):
    print '\033[31m------- SYSBENCH ERR ---------  \033[0m \n%s' % msg
    print '\033[31m------- SYSBENCH ERR ---------  \033[0m'

def psucc(msg):
    print '\033[32m------- SYSBENCH LOG ---------  \033[0m \n%s' % msg
    print '\033[32m------- SYSBENCH LOG ---------  \033[0m'
class Arguments:
    args = dict()

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
        try:
            self.add("--mysql-host", opt["mysql-host"])
            self.add("--mysql-port", opt["mysql-port"])
            self.add("--mysql-user", opt["mysql-user"])
            self.add("--mysql-password", opt["mysql-password"])
            self.add("--test", "oltp")
            self.add("--ob", "on")
            self.add("--oltp-table-name", opt["oltp-table-name"])

            if opt["command"] == "prepare":
                if "oltp-table-size" in opt:
                    self.add("--oltp-table-size", opt["oltp-table-size"])
                if "oltp-auto-inc" in opt:
                    self.add("--oltp-auto-inc", opt["oltp-auto-inc"])

            elif opt["command"] == "run":    
                if "num-threads" in opt:
                    self.add("--num-threads", opt["num-threads"])
                if "max-requests" in opt:
                    self.add("--max-requests", opt["max-requests"])
                if "max-time" in opt:
                    self.add("--max-time", opt["max-time"])
                if "forced-shutdown" in opt:
                    self.add("--forced-shutdown", opt["forced-shutdown"])
                if "thread-stack-size" in opt:
                    self.add("--thread-stack-size", opt["thread-stack-size"])
                if "init-rng" in opt:
                    self.add("--init-rng", opt["init-rng"])
                if "debug" in opt:
                    self.add("--debug", opt["debug"])
                if "validate" in opt:
                    self.add("--validate", opt["validate"])
                if "oltp-test-mode" in opt:
                    self.add("--oltp-test-mode", opt["oltp-test-mode"])
                if "oltp-reconnect-mode" in opt:
                    self.add("--oltp-reconnect-mode", opt["oltp-reconnect-mode"])
                if "oltp-sp-name" in opt:
                    self.add("--oltp-sp-name", opt["oltp-sp-name"])
                if "oltp-read-only" in opt:
                    self.add("--oltp-read-only", opt["oltp-read-only"])
                if "oltp-skip-trx" in opt:
                    self.add("--oltp-skip-trx", opt["oltp-skip-trx"])
                if "oltp-range-size" in opt:
                    self.add("--oltp-range-size", opt["oltp-range-size"])
                if "oltp-point-selects" in opt:
                    self.add("--oltp-point-selects", opt["oltp-point-selects"])
                if "oltp-simple-ranges" in opt:
                    self.add("--oltp-simple-ranges", opt["oltp-simple-ranges"])
                if "oltp-sum-ranges" in opt:
                    self.add("--oltp-sum-ranges", opt["oltp-sum-ranges"])
                if "oltp-order-ranges" in opt:
                    self.add("--oltp-order-ranges", opt["oltp-order-ranges"])
                if "oltp-distinct-ranges" in opt:
                    self.add("--oltp-distinct-ranges", opt["oltp-distinct-ranges"])
                if "oltp-join-point-selects" in opt:
                    self.add("--oltp-join-point-selects", opt["oltp-join-point-selects"])
                if "oltp-join-simple-ranges" in opt:
                    self.add("--oltp-join-simple-ranges", opt["oltp-join-simple-ranges"])
                if "oltp-join-sum-ranges" in opt:
                    self.add("--oltp-join-sum-ranges", opt["oltp-join-sum-ranges"])
                if "oltp-join-order-ranges" in opt:
                    self.add("--oltp-join-order-ranges", opt["oltp-join-order-ranges"])
                if "oltp-join-distinct-ranges" in opt:
                    self.add("--oltp-join-distinct-ranges", opt["oltp-join-distinct-ranges"])
                if "oltp-nontrx-mode" in opt:
                    self.add("--oltp-nontrx-mode", opt["oltp-nontrx-mode"])
                if "oltp-auto-inc" in opt:
                    self.add("--oltp-auto-inc", opt["oltp-auto-inc"])
                if "oltp-connect-delay" in opt:
                    self.add("--oltp-connect-delay", opt["oltp-connect-delay"])
                if "oltp-user-delay-min" in opt:
                    self.add("--oltp-user-delay-min", opt["oltp-user-delay-min"])
                if "oltp-user-delay-max" in opt:
                    self.add("--oltp-user-delay-max", opt["oltp-user-delay-max"])
                if "oltp-table-size" in opt:
                    self.add("--oltp-table-size", opt["oltp-table-size"])
                if "oltp-dist-type" in opt:
                    self.add("--oltp-dist-type", opt["oltp-dist-type"])
                if "oltp-dist-iter" in opt:
                    self.add("--oltp-dist-iter", opt["oltp-dist-iter"])
                if "oltp-dist-pct" in opt:
                    self.add("--oltp-dist-pct", opt["oltp-dist-pct"])
                if "oltp-dist-res" in opt:
                    self.add("--oltp-dist-res", opt["oltp-dist-res"])
                if "db-ps-mode" in opt:
                    self.add("--db-ps-mode", opt["db-ps-mode"])

        except Exception as e:
            print "[Error] %s option not specify in configure file" % e
            raise

class Tester:
    def __init__(self):
        pass

    def find_bin(self, opt):
        if 'sysbench-bin' in opt:
            return opt["sysbench-bin"]
        else:
            try:
                p = Popen(["which", "sysbench"], stdout=PIPE)
                path = p.communicate()[0]
                if p.returncode == 0:
                    return path
                else:
                    print("Please set 'sysbench-bin' option")
                    return None
            except Exception as e:
                print("%s, Please set `sysbench-bin' option" % e)
                return None

    def run(self, opt):
        if None == self.find_bin(opt):
            exit(-1)

        retcode = 0
        output = ""
        errput = ""
        cmd =  self.find_bin(opt) + str(Arguments(opt)) + " " +  opt["command"]
          
        print(cmd)
        try:
            if "sysbench-lib" in opt:
                p = Popen(shlex.split(cmd), stdout = PIPE, stderr = PIPE, env=dict({"LD_LIBRARY_PATH":opt["sysbench-lib"]})) 
            else:
                p = Popen(shlex.split(cmd), stdout = PIPE, stderr = PIPE) 
                
            output, errput = p.communicate()
            retcode = p.returncode
        except Exception as e:
            errput = e;
            retcode = 255;
        return {"ret" : retcode, "output" : output,\
                "cmd" : cmd, "errput" : errput}

class Manager:
    opt = None
    before_one = None
    after_one = None
    log_fp = None

    def __init__(self, opt):
        self.opt = opt

    def run(self):
        if self.before_one:
            self.before_one(self)
        start = time()
        result = Tester().run(self.opt)
        during = time() - start;

        if self.after_one:
            self.after_one(self)

        return result

    def start(self):
        if "log_file" in self.opt:
            log_file = self.opt["log_file"]
        else:
            log_file = "sysbench_test/sysbench.log"
        self.log_fp = open(log_file, "w")
        try:
            self.result = self.run()
            if self.result["ret"] == 0:
                psucc(self.result["output"])
                self.log_fp.write(self.result["output"])
            else:
                pfail(self.result["output"])
                pfail(self.result["errput"])
                self.log_fp.write(str(self.result["errput"]) + "\n" + self.result["output"])
        except Exception as e:
            raise
        finally:
            self.log_fp.close()

__all__ = ["Manager"]

