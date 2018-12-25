#ifndef __OB_LOG_TOOL_FILE_UTILS_H__
#define __OB_LOG_TOOL_FILE_UTILS_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int get_file_len(const char* path, int64_t& len)
{
  int err = OB_SUCCESS;
  struct stat _stat;
  if (NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 != stat(path, &_stat))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "fstat(): %s", strerror(errno));
  }
  else
  {
    len = _stat.st_size;
  }
  return err;
}

int file_map_read(const char* path, const int64_t len, const char*& buf)
{
  int err = OB_SUCCESS;
  int fd = 0;
  if (NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if ((fd =open(path, O_RDONLY)) < 0)
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "open(%s):%s", path, strerror(errno));
  }
  else if (NULL == (buf = (char*)mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0)) || MAP_FAILED == buf)
  {
    buf = NULL;
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "mmap[%s] failed:%s", path, strerror(errno));
  }
  if (fd > 0)
    close(fd);
  return err;
}

int file_map_write(const char* path, const int64_t len, char*& buf)
{
  int err = OB_SUCCESS;
  int fd = 0;
  if (NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if ((fd =open(path, O_RDWR|O_CREAT, S_IRWXU|S_IRGRP)) < 0)
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "open(%s):%s", path, strerror(errno));
  }
  else if (0 != ftruncate(fd, len))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "ftruncate(%s):%s", path, strerror(errno));
  }
  else if (NULL == (buf = (char*)mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) || MAP_FAILED == buf)
  {
    buf = NULL;
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "mmap[%s] failed:%s", path, strerror(errno));
  }
  if (fd > 0)
    close(fd);
  return err;
}

int read_file(const char* path, char* buf, const int64_t buf_size, int64_t& read_bytes, const int64_t offset)
{
  int err = 0;
  int fd = -1;
  if (NULL == path || NULL == buf || 0 >= buf_size)
  {
    err = EINVAL;
  }
  else if (0 > (fd = open(path, O_RDONLY)))
  {
    err = EIO;
  }
  else if (0 > (read_bytes = pread(fd, buf, buf_size-1, offset)))
  {
    err = EIO;
  }
  close(fd);
  return err;
}

#endif /* __OB_LOG_TOOL_FILE_UTILS_H__ */
