/******************************************************************************

       File: bassockudp.h
    Package: atoms
Description: unreliable networking header

Copyright (c) 1996-1998 Inktomi Corporation.  All Rights Reserved.

Confidential

$Id: bassockudp.h,v 1.1 2005/12/13 18:13:35 dbowen Exp $

******************************************************************************/

#ifndef BASSOCKUDP_H
#define BASSOCKUDP_H


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>


#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */


int BasSockUdpAlloc(void);

int BasSockUdpNonBlock(int sock);

int BasSockUdpOpenPassive(int sock, const struct sockaddr_in *local);

void BasSockUdpClose(int sock);

int BasSockUdpAllocPassive(int *sock, unsigned short port);

int BasSockUdpSend(int                  sock,
                   const char          *buf,
                   int                  size,
                   struct sockaddr_in  *dest);

int BasSockUdpReceive(int sock, char *buf, int size, struct sockaddr_in *from);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* !BASSOCKUDP_H */

