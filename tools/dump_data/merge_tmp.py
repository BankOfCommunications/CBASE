#!/usr/bin/python

import sys
import os
import time

def gettime():
  return time.strftime("%Y-%m-%d %k:%M:%S", time.localtime())

def hadoop_ls(dir):
  cmd = "hadoop fs -ls %s" % dir
  file = os.popen(cmd)
  line = file.readline();
  reslist = []

  while True:
    line = file.readline()
    if len(line) == 0:
      break

    strs = line.split()
    reslist.append(strs[len(strs) - 1]);
  ret = file.close()

  if ret is not None:
    reslist = None

  return reslist

def execute_cmd(cmd):
  file = os.popen(cmd)
  ret = file.close()
  if ret == None:
    return 0
  else:
    return 1

def hadoop_mv(src, dst):
  cmd = "hadoop fs -mv %s %s" % (src, dst)
  return os.system(cmd)

def hadoop_isdir(dir):
  cmd = "hadoop fs -test -d %s" % dir
  return os.system(cmd)

def hadoop_rm(src):
  cmd = "hadoop fs -rm %s" % src
  return os.system(cmd)

if len(sys.argv) != 3:
  print "merge_tmp.py data_path obmerge.jar"
  sys.exit(0)

data_path = sys.argv[1]
jar_path = sys.argv[2]

data_tmp_path = "%s/_data_tmp" % data_path

print "[%s] INFO datapath = %s, jarfile = %s" % (gettime(), data_path, jar_path)
files = hadoop_ls(data_tmp_path)
if files == None:
  print "[%s] ERROR can't ls dir %s" % (gettime(), data_tmp_path)
  sys.exit(1)

ret = 0
for file in files:
  dirname = os.path.dirname(file)
  basename = os.path.basename(file)
  destdirname = data_path + basename
  merge_cmd = "hadoop jar %s com.ali.dba.ObDumpMerge -m 100 %s %s" % (jar_path, file, destdirname)
  print merge_cmd

  ret = execute_cmd(merge_cmd)
  if not ret == 0:
    print "[%s] ERROR can't execute cmd = %s" % (gettime(), merge_cmd)
    break

sys.exit(ret)
#os.system()
