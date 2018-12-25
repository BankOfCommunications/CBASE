#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>

int main(int argc, char* argv[])
{
  int ret = 0;
  int fd = 0;
  char buf[BUFSIZ];
  int64_t t = 0;

  if (argc == 1)
  {
    t = 0;
  }
  else
  {
    t = atoll(argv[1]);
  }
  fd = open("suite_config", O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  if (-1 == fd)
  {
    fprintf(stderr, "open error: %s\n", strerror(errno));
    ret = 1;
  }
  else
  {
    memset(buf, 0x00, BUFSIZ);
    memcpy(buf, &t, sizeof(t));
    ssize_t s = write(fd, buf, BUFSIZ);
    if (s != BUFSIZ)
    {
      fprintf(stderr, "write error, s=%ld", s);
      ret = 1;
    }
    close(fd);
  }
  return ret;
}

