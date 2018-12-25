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
  return os.system(cmd)

def hadoop_isdir(dir):
  cmd = "hadoop fs -test -d %s" % dir
  return os.system(cmd)

def hadoop_rm(src):
  cmd = "hadoop fs -rm %s" % src
  return os.system(cmd)

if len(sys.argv) != 5:
  print "obcleanup.py [fs type:hadoop or posix] [result file] [data dir] [backup dir]"
  sys.exit(0)

file = file(sys.argv[2])
backup_dir=sys.argv[4]
data_dir = sys.argv[3]

result = {}

while True:
  line = file.readline()
  if len(line) == 0:
    break;

  if line[0] == '#':
    continue

  elems = line.split(":")
  if len(elems) == 2:
    if sys.argv[1] == "posix" :
      str = elems[1][elems[1].rfind('/') + 1:]
      str = str.rstrip('\n')
    else:
      str = elems[1].rstrip('\n')

    result[str] = 1

if sys.argv[1] == "posix":
  data_dir_path = sys.argv[3]
  dirs = os.listdir(data_dir_path)
  print dirs
  for dir in dirs:
    dir_path = data_dir_path + "/" + dir
    if os.path.isdir(dir_path):
      files = os.listdir(dir_path)
      for file in files:
        file_path = dir_path + "/" + file
        if os.path.isfile(file_path):
          if not result.has_key(file):
            backup_path=backup_dir + "/" + file
            os.rename(file_path, backup_path)
            print "mv file %s to backup pos" % file_path, backup_path

else:
  print len(result)
  dirs = hadoop_ls(data_dir)
  print dir
  for dir in dirs:
    files = hadoop_ls(dir)
    for file in files:
      if not result.has_key(file) and hadoop_isdir(file):
        file_name = file[file.rfind('/') + 1:]
        backup_path = backup_dir + "/" + file_name
        print "rm file %s " % file
        hadoop_rm(file)

