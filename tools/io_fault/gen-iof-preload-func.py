#!/usr/bin/env python2
import re
import string
func_list = '''
ssize_t read(int fd, void *buf, size_t count);
ssize_t write(int fd, const void *buf, size_t count);
ssize_t pread(int fd, void *buf, size_t count, off_t offset);
ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
ssize_t send(int fd, const void *buf, size_t len, int flags);
ssize_t recv(int fd, void *buf, size_t len, int flags);
'''
def make_func_def(ret, func, args):
    template = string.Template('''$ret $func($args)
{
  int err = 0;
  ssize_t (*real_$func)($args) = dlsym(RTLD_NEXT, "$func");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_$func($arg_names);
  }
  return err;
}
''')
    arg_names = ', '.join(re.findall('(\w+),', args + ','))
    return template.substitute(**locals())

headers = '''
#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
extern int iof_hook(int fd);
'''
print headers
for ret,func,args in re.findall("^(\w+)\s+(\w+)\((.*)\);", func_list, re.M):
    print make_func_def(ret, func, args)
