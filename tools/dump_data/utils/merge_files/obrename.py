#!/usr/bin/python

import sys
import os

def hadoop_ls(dir):
  cmd = "hadoop fs -ls %s" % dir
  file = os.popen(cmd)
  line = file.readline();
  reslist = []

  while True:
    line = file.readline()
    if len(line) == 0:
      break;
    strs = line.split()
    reslist.append(strs[len(strs) - 1]);

  return reslist

def hadoop_mv(src, dst):
  cmd = "hadoop fs -mv %s %s" % (src, dst)
  print cmd
  return os.system(cmd)

def hadoop_mkdir(dir_name):
  cmd = "hadoop fs -mkdir %s" % dir_name
  print cmd
  return os.system(cmd)

if len(sys.argv) != 2:
  print "obrename.py [data dir]"
  os.exit(0)

data_dir = sys.argv[1]
name_prefix = data_dir + "part-"
file_part = 0
files = hadoop_ls(data_dir)

for file in files:
  if (file_part % 100) == 0:
  file_part = file_part + 1
  name = "%s%06d" % (name_prefix,file_part)
  #hadoop_mv(file, name)
