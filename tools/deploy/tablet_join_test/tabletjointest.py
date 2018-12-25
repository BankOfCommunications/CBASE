#!/usr/bin/env python2.6

# Author: junyue
# Last change: 2012-11-05 10:47:32 #

import re
from subprocess import Popen, PIPE
import shlex
import glob
import errno
import pprint
from os import chdir, getcwd, makedirs
from os.path import basename, join, dirname, realpath
from time import strftime, time

def pfail(msg):
    print '\033[31m------- Tablet Join Test ERR ---------  \033[0m \n%s' % msg
    print '\033[31m------- Tablet Join Test ERR ---------  \033[0m'

def psucc(msg):
    print '\033[32m------- Tablet Join Test LOG ---------  \033[0m \n%s' % msg
    print '\033[32m------- Tablet Join Test LOG ---------  \033[0m'

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
            self.add("-H", opt["host"])
            self.add("-P", opt["port"])
            if "N" in opt:
                self.add("-N", opt["N"])

        except Exception as e:
            print "[Error] %s option not specify in configure file" % e
            raise

class Tester:
    def __init__(self):
        pass

    def run(self, opt):
        retcode = 0
        output = ""
        errput = ""
        cmd =  opt["binary"] + str(Arguments(opt))
          
        print(cmd)
        try:
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
                pfail(self.result["errput"])
                self.log_fp.write(str(self.result["errput"]) + "\n" + self.result["output"])
        except Exception as e:
            raise
        finally:
            self.log_fp.close()

__all__ = ["Manager"]

