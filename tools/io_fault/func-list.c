
#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
extern int iof_hook(int fd);

ssize_t read(int fd, void *buf, size_t count)
{
  int err = 0;
  ssize_t (*real_read)(int fd, void *buf, size_t count) = dlsym(RTLD_NEXT, "read");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_read(fd, buf, count);
  }
  return err;
}

ssize_t write(int fd, const void *buf, size_t count)
{
  int err = 0;
  ssize_t (*real_write)(int fd, const void *buf, size_t count) = dlsym(RTLD_NEXT, "write");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_write(fd, buf, count);
  }
  return err;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
  int err = 0;
  ssize_t (*real_pread)(int fd, void *buf, size_t count, off_t offset) = dlsym(RTLD_NEXT, "pread");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_pread(fd, buf, count, offset);
  }
  return err;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
  int err = 0;
  ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = dlsym(RTLD_NEXT, "pwrite");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_pwrite(fd, buf, count, offset);
  }
  return err;
}

ssize_t send(int fd, const void *buf, size_t len, int flags)
{
  int err = 0;
  ssize_t (*real_send)(int fd, const void *buf, size_t len, int flags) = dlsym(RTLD_NEXT, "send");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_send(fd, buf, len, flags);
  }
  return err;
}

ssize_t recv(int fd, void *buf, size_t len, int flags)
{
  int err = 0;
  ssize_t (*real_recv)(int fd, void *buf, size_t len, int flags) = dlsym(RTLD_NEXT, "recv");
  if (0 == (err = iof_hook(fd)))
  {
    err = real_recv(fd, buf, len, flags);
  }
  return err;
}

