#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#define __USE_GNU
#include <dlfcn.h>

const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-x86-64.so.2";

#define INTRODUCTION \
    "This library is used to adapt the IO operation,\n" \
    "Simulate IO error, etc.. \n"


static ssize_t (*real_write)(int fd, const void *buf, size_t count) = NULL;
static ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = NULL;

void *config = NULL;

int test_fd = -1;

static void __mtrace_init(void)
{
  real_write = dlsym(RTLD_NEXT, "write");
  if (NULL == real_write)
  {
    fprintf(stderr, "Error in `dlsym`: %s\n", dlerror());
  }

  real_pwrite = dlsym(RTLD_NEXT, "pwrite");
  if (NULL == real_pwrite)
  {
    fprintf(stderr, "Error in `dlsym`: %s\n", dlerror());
  }

  int fd = open("suite_config", O_RDONLY);
  if (-1 == fd)
  {
    fprintf(stderr, "open config failed: %s\n", strerror(errno));
  }
  else
  {
    config = mmap(NULL, sysconf(_SC_PAGE_SIZE), PROT_READ, MAP_SHARED, fd, 0);
    if (config == MAP_FAILED)
    {
      fprintf(stderr, "mmap failed: %s\n", strerror(errno));
    }
  }

  //test_fd = open("suite.log", O_WRONLY | O_CREAT | O_APPEND, S_IRWXU);
  //if (-1 == test_fd)
  //{
  //  fprintf(stderr, "open config failed: %s\n", strerror(errno));
  //}
}

int64_t get_sleep_time()
{
  int64_t t = 0;
  if (config)
  {
    t = *(int64_t*)config;
  }
  return t;
}

ssize_t write(int fd, const void *buf, size_t count)
{
  if (real_write == NULL) __mtrace_init();
  //struct stat st;
  //if (fstat(fd, &st) == 0)
  //{
  //  char buf[BUFSIZ];
  //  int len = snprintf(buf, BUFSIZ, "write fd=%d st_mode=%X\n", fd, st.st_mode);
  //  int wl = real_write(test_fd, buf, len);
  //  if (wl != len)
  //  {
  //    fprintf(stderr, "log write error: %s\n", strerror(errno));
  //  }
  //  fsync(test_fd);
  //  if (S_ISREG(st.st_mode))
  //  {
  //    int64_t st = get_sleep_time();
  //    if (st > 0)
  //    {
  //      usleep(st);
  //    }
  //  }
  //}
  return real_write(fd, buf, count);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
  if (real_pwrite == NULL) __mtrace_init();
  struct stat st;
  if (fstat(fd, &st) == 0)
  {
    /*
    char buf[BUFSIZ];
    int len = snprintf(buf, BUFSIZ, "pwrite fd=%d st_mode=%X\n", fd, st.st_mode);
    int wl = real_write(test_fd, buf, len);
    if (wl != len)
    {
      fprintf(stderr, "log write error: %s\n", strerror(errno));
    }
    fsync(test_fd);
    */
    if (S_ISREG(st.st_mode))
    {
      int64_t st = get_sleep_time();
      if (st > 0)
      {
        usleep(st);
      }
    }
  }
  return real_pwrite(fd, buf, count, offset);
}

const char* svn_version();
const char* build_date();
const char* build_time();

int so_main()
{
  printf(INTRODUCTION);
  printf("SVN Revision: %s\n", svn_version());
  printf("SO Build Time: %s %s\n", build_date(), build_time());
  printf("GCC Version: %s\n", __VERSION__);
  printf("\n");
  exit(0);
}

