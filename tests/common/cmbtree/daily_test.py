#!/usr/local/bin/python2.6

# This script is made cmbtree test run automatically

import os
import logging
from subprocess import Popen, PIPE, STDOUT

# Get environments
Environ = os.environ
HOME_DIR                     = Environ['HOME']

# Const variables
OB_DIR                       = HOME_DIR + '/code/oceanbase/dev'
OB_SRC_DIR                   = OB_DIR + '/src'
CMBTREE_DIR                  = OB_DIR + '/src/common/cmbtree'
TEST_APPS                    = ('./test1', './test4',)
LOG_FILE                     = 'daily_test.log'

logging.basicConfig(filename = LOG_FILE, level = logging.DEBUG,
    format='[%(asctime)s] %(levelname)s  %(funcName)s (%(filename)s:%(lineno)d) [%(thread)d] %(message)s')

class ExecutionError(Exception): pass

class Mail(object):
  SEND_MAIL = "/usr/sbin/sendmail"
  def __init__(self):
    self.mail = os.popen("%s -t" % self.__class__.SEND_MAIL, "w")
  def setTo(self, t): 
    self.mail.write("To: " + t + os.linesep)
  def setCc(self, c): 
    self.mail.write("Cc: " + c + os.linesep)
  def setFrom(self, f): 
    self.mail.write("From: " + f + os.linesep)
  def setSubject(self, subject):
    self.mail.write("Subject: " + subject + os.linesep)
  def setContentType(self, contentType):
    self.mail.write("Content-Type: " + contentType + os.linesep)
  def setContent(self, content):
    self.mail.write(os.linesep)
    self.mail.write(content + os.linesep)
  def setContentTransferEncoding(self, encoding):
    self.mail.write("Content-Transfer-Encoding: " + encoding + os.linesep)
  def send(self):
    self.mail.close()

def SendMail(subject, body):
  m = Mail()
  m.setFrom('yanran.hfs@alipay.com')
  m.setTo('yanran.hfs@alipay.com')
  m.setSubject(subject)
  m.setContentType("text/html; charset=utf-8")
  m.setContentTransferEncoding("8bit")
  m.setContent(body)
  m.send()

def OSExecute(cmd, wd = None, logfn = None, ignore_error = False):
  '''Execute a command locally, and return
  >>> OSExecute('ls > /dev/null')
  ''
  '''
  p = Popen(cmd, shell = True, stdout = PIPE, stderr = STDOUT, cwd = wd)
  output = p.communicate()[0]
  err = p.wait()
  if not ignore_error and err:
      output = 'OSExecute({0})=>{1} Output=>"{2}"'.format(cmd, err, output)
      logging.exception(output)
      raise ExecutionError(output)
  if logfn is not None:
    logfn('OSExecute({0}) Output=>"{1}"'.format(cmd, output))
  return output

def UpdateSourceCode():
  OSExecute('svn update', OB_DIR, logging.info)

def CompileCMBtree():
  OSExecute('./build.sh clean; ./build.sh init; ./build.sh', OB_DIR, logging.info, ignore_error = True)
  OSExecute('make clean; make -j', OB_SRC_DIR, logging.info)
  OSExecute('scons', CMBTREE_DIR, logging.info)

def RunTest():
  try:
    for app in TEST_APPS:
      output = OSExecute(app, wd = CMBTREE_DIR)
      log_msg = 'Running test "{app}" OK, the output is: {output}'.format(**locals())
      logging.info(log_msg)
  except ExecutionError:
    err_msg = 'Running test "{app}" error'.format(**locals())
    logging.error(err_msg)
    SendMail(err_msg, '')

def main():
  try:
    UpdateSourceCode()
    CompileCMBtree()
    RunTest()
  except Exception as e:
    SendMail('Error unexpected happened', '')

if __name__ == '__main__':
  main()
