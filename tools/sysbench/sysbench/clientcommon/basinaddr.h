/******************************************************************************

       File: basinaddr.h
    Package: atoms
Description: internet address utilities header

Copyright (c) 1996-1998 Inktomi Corporation.  All Rights Reserved.

Confidential

******************************************************************************/

#ifndef BASINADDR_H
#define BASINADDR_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>


int BasInetAddrToNumber(const struct sockaddr_in  *addr,
                        char                      *buf,
                        size_t                     size);
 
int BasInetAddrToName(const struct sockaddr_in *addr, char *buf, size_t size);
 
int BasInetAddrSet(struct sockaddr_in  *outAddr,     /* can be null */
                   char                *outName,     /* can be null */
                   size_t               outNameSize,
                   in_addr_t            ip,          /* either ip */
                   const char          *name,        /* or name */
                   unsigned short       port,
                   char               **herr);       /* for errno */


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* !BASINADDR_H */
