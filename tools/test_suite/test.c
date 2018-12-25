#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

int main()
{
  int fd = open("test.data", O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  char buf[BUFSIZ];
  memset(buf, 0x00, BUFSIZ);
  int64_t t = 4000000;
  memcpy(buf, &t, sizeof(t));
  ssize_t s = write(fd, buf, BUFSIZ);
  if (s != BUFSIZ)
  {
    fprintf(stderr, "write error, s=%ld", s);
  }
  close(fd);
  return 0;
}

