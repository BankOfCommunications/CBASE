/******************************************************************************

       File: basthr.c
    Package: atoms
Description: multiple platform threads wrapper

Copyright (c) 1996-2001 Inktomi Corporation.  All Rights Reserved.

Confidential

$Id: basthr.c,v 1.1 2005/12/13 18:13:35 dbowen Exp $

******************************************************************************/

#include <stdlib.h>
#include <string.h>
#include "basthr.h"

BasThrMutexT listMux = BASTHRMUTEXDEFAULT;
BasThrThreadIdListT *gThrList;  /* Global list of thread ids */

/******************************************************************************/

#if (defined(Linux))

# include <sched.h>
# include <errno.h>

int
BasThrThreadCreate(void           *(*func)(void *),
                   void             *arg,
                   BasThrThreadT    *id,
                   int               bound,
                   int               detached)
{
  int             res;
  pthread_attr_t  attr;
  pthread_t       tid;

  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, 1048576);

  if (bound)
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
  if (detached)
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

  res = pthread_create(&tid, &attr, func, arg);
  pthread_attr_destroy(&attr);
  if (id)
    *id = tid;
  return res;
}


int
BasThrThreadHighPri(int pri)
{
  int                 policy  = SCHED_RR;
  int                 pmax;
  int                 pmin;
  int                 res;
  pthread_t           tid;
  struct sched_param  params;

  pmax = sched_get_priority_max(policy);
  pmin = sched_get_priority_min(policy);

  if ((pmax < 0) || (pmin < 0))
    return -1;

  if (pri < pmin)
    pri = pmin;
  else if (pri > pmax)
    pri = pmax;

  tid = pthread_self();
  params.sched_priority = pri;

  res = pthread_setschedparam(tid, policy, &params);
  if (res)
    return -1;
  return 0;
}

int
BasThrCondTimedWait(BasThrCondT *cond,
                    BasThrMutexT *mutex,
                    const struct timespec *abstime) {
  int ret;
  ret = pthread_cond_timedwait(cond, mutex, abstime);
  if( ret == ETIMEDOUT ) {
    ret = ETIME;
  }
  return ret;
}

#elif SunOS

#include <sys/priocntl.h>
#include <sys/tspriocntl.h>

int
BasThrThreadHighPri(int pri)
{
  long        x;
  pcparms_t   pcparms;
  pcinfo_t    pcinfo;
  tsinfo_t   *tsi;
  tsparms_t  *tsp;

  strcpy(pcinfo.pc_clname, "TS"); /* timeshare class always installed */
  x = priocntl(0, 0, PC_GETCID, (char *) &pcinfo);
  if (x < 0)
    goto abort;

  tsi = (tsinfo_t *) pcinfo.pc_clinfo;
  if (pri < 0)
    pri = 0;
  else if (pri > tsi->ts_maxupri)
    pri = tsi->ts_maxupri;

  pcparms.pc_cid = pcinfo.pc_cid;
  tsp = (tsparms_t *) pcparms.pc_clparms;
  tsp->ts_uprilim = pri;
  tsp->ts_upri = pri;
  x = priocntl(P_LWPID, P_MYID, PC_SETPARMS, (char *) &pcparms);
  if (x < 0)
    goto abort;

  return 0;

abort:
  return -1;
}

#else
# error needs porting
#endif

int BasThrAddToThreadIdList(BasThrThreadIdListT *list, const BasThrThreadT *tid){
  BasThrThreadIdListT ptr;

  /* If no specific list specified, use the global list */
  if(!list)
    list = gThrList;

  ptr = (BasThrThreadIdListT)malloc(sizeof(BasThrThreadIdListNode));
  if(!ptr)
    return -1;
  memcpy(&(ptr->tid), tid, sizeof(BasThrThreadT));
  ptr->next = NULL;

  BasThrMutexLock(&listMux);
  if(*list){
    ptr->next = *list;
    *list = ptr;
  }
  else{
    *list = ptr;
  }
  BasThrMutexUnlock(&listMux);
  return 0;
}

int BasThrCreateAddToThreadList(BasThrThreadIdListT *thrlist, 
                                void *(*func)(void *), void *arg,
                                BasThrThreadT *id, int bound, int detached){
  BasThrThreadT tid;

  BasThrThreadCreate(func, arg, &tid, bound, detached);
  BasThrAddToThreadIdList(thrlist, &tid);

  if (id != NULL) 
    *id = tid;

  return 0;
}

int BasThrThreadListJoin(BasThrThreadIdListT list){
  /* BasThrMutexLock(&listMux); */
  while(list){
    BasThrThreadJoin(list->tid, NULL);
    list = list->next;
  }
  /* BasThrMutexUnlock(&listMux); */
  return 0;
}

int BasThrMutexInit(BasThrMutexT *mutex)
{
  pthread_mutexattr_t attr;

  if (pthread_mutexattr_init(&attr) < 0 ||
      pthread_mutexattr_settype(&attr, BASTHRMUTEXDEFAULTTYPE) < 0) {
      return (-1);
  }
  return (pthread_mutex_init(mutex, &attr));
}
