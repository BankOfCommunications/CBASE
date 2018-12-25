/******************************************************************************

       File: basthr.h
    Package: atoms
Description: multiple platform threads wrapper

Copyright (c) 1996-2001 Inktomi Corporation.  All Rights Reserved.

Confidential

$Id: basthr.h,v 1.1 2005/12/13 18:13:35 dbowen Exp $

******************************************************************************/

#ifndef BASTHR_H
#define BASTHR_H


#if defined(Linux)
# include <pthread.h>
# include <semaphore.h>
#elif defined(SunOS)
# include <thread.h>
#else
# error needs porting
#endif 


#ifdef __cplusplus
extern "C" {
#endif


#if defined(Linux)

typedef pthread_mutex_t BasThrMutexT;

#define BASTHRMUTEXDEFAULT	PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP
#define BASTHRMUTEXDEFAULTTYPE	PTHREAD_MUTEX_ADAPTIVE_NP

int	BasThrMutexInit(BasThrMutexT *m);
#define BasThrMutexLock(m) pthread_mutex_lock(m)
#define BasThrMutexTryLock(m) pthread_mutex_trylock(m)
#define BasThrMutexUnlock(m) pthread_mutex_unlock(m)
#define BasThrMutexDestroy(m) pthread_mutex_destroy(m)

typedef pthread_t BasThrThreadT;
#define BasThrThreadSelf pthread_self
int BasThrThreadCreate(void *(*func)(void *), void *arg,
                     BasThrThreadT *id, int bound, int detached);
#define BasThrThreadJoin(tid, status) pthread_join(tid, status)
#define BasThrThreadYield pthread_yield
#define BasThrThreadKillOtherThreads() pthread_kill_other_threads_np()
/* setconcurrency() has no effect with linux */
#define BasThrThreadSetConcurrency(c) pthread_setconcurrency(c)
#define BasThrThreadExit(c) pthread_exit(c)
/* as root, make this bound thread high priority */
int BasThrThreadHighPri(int pri);

typedef pthread_cond_t BasThrCondT;
#define BASTHRCONDDEFAULT PTHREAD_COND_INITIALIZER
#define BasThrCondInit(c) pthread_cond_init(c, NULL)
#define BasThrCondWait(c, m) pthread_cond_wait(c, m)
#define BasThrCondSignal(c) pthread_cond_signal(c)
#define BasThrCondBroadcast(c) pthread_cond_broadcast(c)
#define BasThrCondDestroy(c) pthread_cond_destroy(c)
int BasThrCondTimedWait(BasThrCondT *c, BasThrMutexT *m, const struct timespec *t);

typedef sem_t BasThrSemaT;
#define BasThrSemaInit(c)        sem_init(c, 0, 0) 
#define BasThrSemaInitCount(c,n) sem_init(c, 0, n) 
#define BasThrSemaPost(c)        sem_post(c) 
#define BasThrSemaWait(c)        sem_wait(c) 
#define BasThrSemaTryWait(c)     sem_trywait(c) 
#define BasThrSemaDestroy(c)     sem_destroy(c)  

typedef pthread_rwlock_t BasRwLockT;
#define BASRWLOCKDEFAULT PTHREAD_RWLOCK_INITIALIZER
#define BasRwLockInit(c) pthread_rwlock_init(c, NULL) 
#define BasRwLockDestroy(c) pthread_rwlock_destroy(c) 
#define BasRwLockRdLock(c) pthread_rwlock_rdlock(c) 
#define BasRwLockWrLock(c) pthread_rwlock_wrlock(c) 
#define BasRwLockTryRdLock(c) pthread_rwlock_tryrdlock(c) 
#define BasRwLockTryWrLock(c) pthread_rwlock_trywrlock(c) 
#define BasRwLockUnlock(c) pthread_rwlock_unlock(c) 

#elif defined(SunOS)

typedef mutex_t BasThrMutexT;
#define BASTHRMUTEXDEFAULT DEFAULTMUTEX
#define BasThrMutexInit(m) mutex_init(m, USYNC_THREAD, NULL)
#define BasThrMutexLock(m) mutex_lock(m)
#define BasThrMutexTryLock(m) mutex_trylock(m)
#define BasThrMutexUnlock(m) mutex_unlock(m)
#define BasThrMutexDestroy(m) mutex_destroy(m)

typedef thread_t BasThrThreadT;
#define BasThrThreadSelf thr_self
#define BasThrThreadCreate(fnc, arg, id, bnd, det) \
  thr_create(NULL, 0, fnc, arg, \
             ((bnd) ? THR_BOUND : 0) | ((det) ? THR_DETACHED : 0), id)
#define BasThrThreadJoin(tid, status) thr_join(tid, NULL, status)
#define BasThrThreadYield thr_yield
#define BasThrThreadKillOtherThreads() do{}while(0)
#define BasThrThreadSetConcurrency(c) thr_setconcurrency(c)
#define BasThrThreadExit(c) thr_exit(c)
/* as root, make this bound thread high priority */
int BasThrThreadHighPri(int pri);
#define BasThrThreadGetPrio(tid, prio) thr_getprio(tid, prio)
#define BasThrThreadSetPrio(tid, prio) thr_setprio(tid, prio)

typedef cond_t BasThrCondT;
#define BASTHRCONDDEFAULT DEFAULTCV
#define BasThrCondInit(c) cond_init(c, USYNC_THREAD, NULL)
#define BasThrCondWait(c, m) cond_wait(c, m)
#define BasThrCondTimedWait(c, m, t) cond_timedwait(c, m, t)
#define BasThrCondSignal(c) cond_signal(c)
#define BasThrCondBroadcast(c) cond_broadcast(c)
#define BasThrCondDestroy(c) cond_destroy(c)

typedef sema_t BasThrSemaT;
#define BasThrSemaInit(c)        sema_init(c, 0, USYNC_THREAD, NULL) 
#define BasThrSemaInitCount(c,n) sema_init(c, n, USYNC_THREAD, NULL) 
#define BasThrSemaPost(c)        sema_post(c) 
#define BasThrSemaWait(c)        sema_wait(c) 
#define BasThrSemaTryWait(c)     sema_trywait(c) 
#define BasThrSemaDestroy(c)     sema_destroy(c) 

typedef rwlock_t BasRwLockT;
#define BASRWLOCKDEFAULT DEFAULTRWLOCK
#define BasRwLockInit(c) rwlock_init(c, USYNC_THREAD, NULL) 
#define BasRwLockDestroy(c) rwlock_destroy(c) 
#define BasRwLockRdLock(c) rw_rdlock(c) 
#define BasRwLockWrLock(c) rw_wrlock(c) 
#define BasRwLockTryRdLock(c) rw_tryrdlock(c) 
#define BasRwLockTryWrLock(c) rw_trywrlock(c) 
#define BasRwLockUnlock(c) rw_unlock(c) 

#else
# error needs porting
#endif 


typedef struct ThreadIdList  BasThrThreadIdListNode;
typedef struct ThreadIdList *BasThrThreadIdListT;

struct ThreadIdList {
  BasThrThreadT       tid;
  BasThrThreadIdListT next;
}; 

int BasThrAddToThreadIdList(BasThrThreadIdListT *, const BasThrThreadT *);
int BasThrCreateAddToThreadList(BasThrThreadIdListT *thrlist, 
                                void *(*func)(void *), void *arg,
                                BasThrThreadT *id, int bound, int detached);
int BasThrThreadListJoin(BasThrThreadIdListT);


#ifdef __cplusplus
}
#endif


#endif /* !BASTHR_H */
