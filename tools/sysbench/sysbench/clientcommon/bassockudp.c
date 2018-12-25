/******************************************************************************

       File: bassockudp.c
    Package: atoms
Description: unreliable networking

Copyright (c) 1996-1998 Inktomi Corporation.  All Rights Reserved.

Confidential

$Id: bassockudp.c,v 1.1 2005/12/13 18:13:35 dbowen Exp $

******************************************************************************/

static const char rcsid[] = "$Id: bassockudp.c,v 1.1 2005/12/13 18:13:35 dbowen Exp $";


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include "basatomdefs.h"
#include "bassockudp.h"


int
BasSockUdpAlloc(void)
{
  int  fd;
  int  val;
  int  res;

  fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (fd < 0)
    return -1;

  val = 0xffff;
  res = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *) &val, sizeof(val));
  res = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *) &val, sizeof(val));

  return fd;
}


int
BasSockUdpNonBlock(int sock)
{
  int  res;

  res = fcntl(sock, F_SETFL, O_NDELAY);
  if (res)
    return -1;
  return 0;
}


int
BasSockUdpOpenPassive(int sock, const struct sockaddr_in *local)
{
  int  val;
  int  res;

  val = 1;
  res = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &val, sizeof(val));

  res = bind(sock, (struct sockaddr *) local, sizeof(*local));
  if (res)
    return -1;

  return 0;
}


void
BasSockUdpClose(int sock)
{
  /* no need to shutdown, not connected */
  close(sock);
}


int
BasSockUdpAllocPassive(int *sock, unsigned short port)
{
  int                 s;
  struct sockaddr_in  local;

  s = BasSockUdpAlloc();
  if (s < 0)
    return -1;

  local.sin_family = AF_INET;
  local.sin_port = htons(port);
  local.sin_addr.s_addr = 0;

  if (BasSockUdpOpenPassive(s, &local)) {
    close(s);
    return -1;
  }

  *sock = s;
  return 0;
}


int
BasSockUdpSend(int sock, const char *buf, int size, struct sockaddr_in *dest)
{
  int  res;
  int  code;

  do {
    res = sendto(sock, buf, size, 0, (struct sockaddr *) dest, sizeof(*dest));
    if (res < 0)
      code = errno;
    else
      code = 0;
  } while (code == EINTR);

  return res;
}


int
BasSockUdpReceive(int sock, char *buf, int size, struct sockaddr_in *from)
{
  int  fromlen;
  int  res;
  int  code;

  do {
    fromlen = sizeof(*from);
    res = recvfrom(sock, buf, size, 0, (struct sockaddr *) from, &fromlen);
    if (res < 0)
      code = errno;
    else
      code = 0;
  } while (code == EINTR);

  return res;
}
