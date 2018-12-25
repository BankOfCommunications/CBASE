#ifndef __OB_COMMON_OB_PRELOAD_H__
#define __OB_COMMON_OB_PRELOAD_H__

#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdarg.h>
#include <execinfo.h>
#include "tbsys.h"

inline int64_t& bt(const char* msg)
{
  int i = 0;
  static int64_t enable_bt = 0;
  if (enable_bt > 0)
  {
    void *buffer[100];
    int size = backtrace(buffer, 100);
    char **strings = backtrace_symbols(buffer, size);
    TBSYS_LOG(INFO, "%s", msg);
    if (NULL != strings)
    {
      for (i = 0; i < size; i++)
      {
        TBSYS_LOG(INFO, "BT[%d] @[%s]", i, strings[i]);
      }
      free(strings);
    }
  }
  return enable_bt;
}

#ifdef __ENABLE_PRELOAD__
inline int pthread_key_create(pthread_key_t *key, void (*destructor)(void*))
{
  int (*real_func)(pthread_key_t *key, void (*destructor)(void*)) = (typeof(real_func))dlsym(RTLD_NEXT, "pthread_key_create");
  bt("pthread_key_create");
  return real_func(key, destructor);
}

inline int pthread_key_delete(pthread_key_t key)
{
  int (*real_func)(pthread_key_t key) = (typeof(real_func))dlsym(RTLD_NEXT, "pthread_key_delete");
  bt("pthread_key_delete");
  return real_func(key);
}
#endif

#endif /* __OB_COMMON_OB_PRELOAD_H__ */
