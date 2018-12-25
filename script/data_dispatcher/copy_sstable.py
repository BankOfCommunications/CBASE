#!/usr/bin/python2.6

import os
import sys
import Queue
import threading
from subprocess import Popen, PIPE, STDOUT
import copy
import time
import re
import logging

class ExecutionError(Exception): pass

class Shell:
  @classmethod
  def sh(cls, cmd, host=None, username=None):
    '''Execute a command locally or remotely
    >>> Shell.sh('ls > /dev/null')
    0
    >>> Shell.sh('ls > /dev/null', host='10.232.36.29')
    0
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh {host} '{cmd}'".format(**locals())
    ret = os.system(cmd)
    if ret != 0:
      err_msg = 'Shell.sh({0}, host={1})=>{2}\n'.format(
          cmd, host, ret);
      sys.stderr.write(err_msg)
      raise ExecutionError(err_msg)
    else:
      logging.debug('"{0}" Execute SUCCESS'.format(cmd))
    return ret 

  @classmethod
  def popen(cls, cmd, host=None, username=None):
    '''Execute a command locally, and return
    >>> Shell.popen('ls > /dev/null')
    ''
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh {host} '{cmd}'".format(**locals())
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    output = p.communicate()[0]
    err = p.wait()
    if err:
        output = 'Shell.popen({0})=>{1} Output=>"{2}"'.format(cmd, err, output)
        raise ExecutionError(output)
    return output

  @classmethod
  def scp(cls, src, host, dst, username=None):
    '''remote copy
    >>> Shell.scp('build1.py', '10.232.36.29', '')
    0
    '''
    if username is not None:
      cmd = 'scp {0} {1}@{2}:{3}'.format(src, username, host, dst)
    else:
      cmd = 'scp {0} {1}:{2}'.format(src, host, dst)
    return Shell.sh(cmd)

  @classmethod
  def mkdir(cls, path, host=None):
    '''make directory locally or remotely
    >>> Shell.mkdir('test', host='10.232.36.29')
    0
    >>> Shell.mkdir('test')
    0
    '''
    if host is None:
      os.path.exists(path) or os.mkdir(path)
      return 0
    else:
      return Shell.sh('mkdir -p {0}'.format(path), host)

class WorkerPool:
  class Worker(threading.Thread):
    def __init__(self, task_queue, status):
      threading.Thread.__init__(self)
      self.task_queue = task_queue
      self.status = status
      self.__stop = 0
      self.err = None

    def run(self):
      #cwd = 'thread' + str(self.ident)
      #Shell.mkdir(cwd)
      while not self.__stop:
        try:
          task = self.task_queue.get(timeout=1)
          task()
          self.task_queue.task_done()
        except Queue.Empty:
          pass
        except BaseException as e:
          self.task_queue.task_done()
          if self.err is None:
            self.err = e
          logging.error('thread' + str(self.ident) + ' ' + str(e))
      if self.err is not None:
        raise self.err

    def stop(self):
      self.__stop = True

  class Status:
    def __init__(self):
      self.status = 0
    def set_active(self):
      self.status = 1
    def set_idle(self):
      self.status = 2
    def is_idle(self):
      self.status == 2

  def __init__(self, num):
    self.task_queue = Queue.Queue()
    self.n = num
    self.status = [WorkerPool.Status()] * num
    self.workers = [None] * num
    for i in range(num):
      self.workers[i] = WorkerPool.Worker(self.task_queue, self.status[i]);
      self.workers[i].start()

  def add_task(self, task):
    self.task_queue.put(task)

  def all_idle(self):
    for i in range(num):
      if not self.status[i].is_idle():
        return False
    return True

  def wait(self):
    self.task_queue.join()
    for w in self.workers:
      w.stop()

def R(cmd, local_vars):
  G = copy.copy(globals())
  G.update(local_vars)
  return cmd.format(**G)

class GetFromHadoop:
  def __init__(self, task):
    '''task should be a dict with these fields:
         dir
         files
    '''
    self.task = task

  def __call__(self):
    dir = self.task['dir']
    files = self.task['files']
    for f in files:
      m = re.match(r'.*/([^/]+)', f)
      if m is not None:
        filename = m.group(1)
        dest_dir = dir
        tmp_dir = R('{dest_dir}/tmp', locals())
        mkdir_tmp_dir = R('mkdir -p {tmp_dir}', locals())
        hadoop_get_cmd = R('{hadoop_bin_dir}hadoop fs -get {f} {tmp_dir}', locals())
        commit_mv_cmd = R('mv {tmp_dir}/{filename} {dest_dir}', locals())
        logging.debug(mkdir_tmp_dir)
        logging.debug(hadoop_get_cmd)
        logging.debug(commit_mv_cmd)
        Shell.sh(mkdir_tmp_dir)
        Shell.sh(hadoop_get_cmd)
        Shell.sh(commit_mv_cmd)
        msg = R('Successfully get "{filename}" to "{dest_dir}"', locals())
        logging.info(msg)

def round_inc(n, ceiling):
  n += 1
  if n >= ceiling:
    n = 0
  return n

def DoWork():
  try:
    wp = WorkerPool(thread_num)
    dir_num = len(data_dir_list)
    tasks = []
    for i in range(dir_num):
      tasks.append(dict(disk_id=None, files=[]))
    file_list = []

    hadoop_ls_cmd = R("{hadoop_bin_dir}hadoop fs -ls {input_dir}", locals())
    logging.debug(hadoop_ls_cmd)

    ls_output = Shell.popen(hadoop_ls_cmd).split('\n')
    for l in ls_output:
      m = re.match(r'^[-d].* ([^ ]+)$', l)
      if m is not None:
        file_list.append(m.group(1))
    logging.debug(file_list)

    disk_index = 0
    for f in file_list:
      if f != '':
        tasks[disk_index]['files'].append(f)
        disk_index = round_inc(disk_index, dir_num)

    for i in range(dir_num):
      tasks[i]['dir'] = data_dir_list[i]
      logging.debug(str(tasks[i]))
      wp.add_task(GetFromHadoop(tasks[i]))
  finally:
    wp.wait()

def gen_list(pattern):
  match = re.search(r'\[(.+)\]', pattern)
  if match is None:
    return [pattern]
  specifier = match.group(1)
  match = re.match('([0-9]+)-([0-9]+)', specifier)
  if not match:
      raise Exception('illformaled range specifier: %s'%(specifier))
  _start, _end = match.groups()
  start,end = int(_start), int(_end)
  formatter = re.sub('\[.+\]', '%0'+str(len(_start))+'d', pattern)
  return [formatter%(x) for x in range(start, end+1)]

if __name__ == '__main__':
  from optparse import OptionParser
  parser = OptionParser()
  parser.add_option('-b', '--hadoop_bin_dir',
      help='Hadoop bin utils directory',
      dest='hadoop_bin_dir')
  parser.add_option('-i', '--input_dir',
      help='Input directory in HDFS',
      dest='input_dir')
  parser.add_option('-d', '--data_dir',
      help='bypass data directory, using [1-10] pattern representing 10 disks.'
           'for example: "-d /data/[1-4]/ups_data/bypass"',
      dest='data_dir')
  parser.add_option('-t', '--thread_num',
      help='thread number to get SSTable, default is 8',
      dest='thread_num')
  parser.add_option('-l', '--log_level',
      help='Log level: ERROR, WARN, INFO, DEBUG',
      dest='log_level')
  (options, args) = parser.parse_args()
  if (options.hadoop_bin_dir is None
      or options.input_dir is None
      or options.data_dir is None):
    print(parser.format_help())
    parser.exit(status=1)

  hadoop_bin_dir = options.hadoop_bin_dir
  input_dir = options.input_dir
  data_dir = options.data_dir
  data_dir_list = gen_list(data_dir)
  if options.thread_num is not None:
    thread_num = int(options.thread_num)
  else:
    thread_num = 8

  LEVELS = {'debug': logging.DEBUG,
      'info': logging.INFO,
      'warning': logging.WARNING,
      'error': logging.ERROR,
      'critical': logging.CRITICAL}
  if options.log_level is not None:
    level = LEVELS.get(options.log_level.lower(), logging.NOTSET)
  logging.basicConfig(level=level,
      format='[%(asctime)s] %(levelname)s %(message)s')

  DoWork()
