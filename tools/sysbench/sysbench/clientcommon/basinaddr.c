/******************************************************************************

       File: basinaddr.c
    Package: atoms
Description: internet address utilites

Copyright (c) 1996-1998 Inktomi Corporation.  All Rights Reserved.

Confidential

******************************************************************************/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <netdb.h>
#include <resolv.h>
#include <string.h>
#include <stdio.h>
#include "basinaddr.h"


int
BasInetAddrToNumber(const struct sockaddr_in *addr, char *buf, size_t size)
{
  unsigned char  *bytes;
 
  if (size < 16)
    return -1;
 
  bytes = (unsigned char *) &addr->sin_addr.s_addr;
  sprintf(buf, "%d.%d.%d.%d",
          bytes[0], bytes[1], bytes[2], bytes[3]);
 
  return 0;
}


int
BasInetAddrToName(const struct sockaddr_in *addr, char *buf, size_t size)
{
  struct hostent   he;
  struct hostent  *hInfo;
  char             hbuf[1024];
  int              herr;
  char            *dot;
  int              len;
 
#if (defined(Linux))
  if (gethostbyaddr_r((char *) &addr->sin_addr, sizeof(struct in_addr),
                       AF_INET, &he, hbuf, sizeof(hbuf), &hInfo, &herr)) {
    BasInetAddrToNumber(addr, buf, size);
    return 1;                   /* warning */
  }
#elif SunOS
  if ( !(hInfo = gethostbyaddr_r((char *) &addr->sin_addr, sizeof(struct in_addr),
                       AF_INET, &he, hbuf, sizeof(hbuf), &herr)) ) {
    BasInetAddrToNumber(addr, buf, size);
    return 1;                   /* warning */
  }
#else
# error needs porting
#endif
 
  --size;
  strncpy(buf, hInfo->h_name, size);
  buf[size] = '\0';
 
  /* ensure domain name */
  dot = strchr(buf, '.');
  if (!dot && _res.defdname[0]) {
    ++dot;
    len = dot - buf;
    /* get default domain from resolver */
    strncpy(dot, _res.defdname, size - len);
    buf[size] = '\0';
  }
 
  return 0;
}


int
BasInetAddrSet(struct sockaddr_in  *outAddr,     /* can be null */
               char                *outName,     /* can be null */
               size_t               outNameSize,
               in_addr_t            ip,          /* either ip */
               const char          *name,        /* or name */
               unsigned short       port,
               char               **errMsg)
{
  struct hostent  he;
  struct hostent *hInfo;
  char            buf[1024];
  int herr;

  if (outAddr) {
    outAddr->sin_family = AF_INET;
    outAddr->sin_port = htons(port);
    outAddr->sin_addr.s_addr = 0;
  }
 
  if (name) {                   /* provided a hostname */
    ip = inet_addr(name);
    if (ip == INADDR_NONE) {
#if (defined(Linux))
      int rv;
      rv = gethostbyname_r(name, &he, buf, sizeof(buf), &hInfo, &herr);
      if (rv != 0 || hInfo == NULL) {
       *errMsg = (char *)hstrerror(herr);
        return -1;
      }
#elif SunOS
      if ( !(hInfo = gethostbyname_r(name, &he, buf, sizeof(buf), &herr)) )
        return -1;
#else
# error needs porting
#endif
      if (outAddr)
        memcpy(&outAddr->sin_addr, hInfo->h_addr, 4); /* takes care of alignment */
      if (outName) {
        --outNameSize;
        strncpy(outName, hInfo->h_name, outNameSize);
        outName[outNameSize] = '\0';
      }
      ip = 0;
    }
  }
  if (ip) {                     /* provided an ip address */
    if (outAddr)
      outAddr->sin_addr.s_addr = ip;
    if (outName)
      BasInetAddrToName(outAddr, outName, outNameSize);
  }
 
  return 0;
}
