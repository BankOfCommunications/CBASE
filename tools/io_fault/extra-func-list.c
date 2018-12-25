#define _GNU_SOURCE 1
#include <stdint.h>
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdarg.h>
#include <execinfo.h>
#include <stdlib.h>

#define INFO(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
int64_t pthread_key_create_count_;
int64_t pthread_key_delete_count_;
void bt(const char* format, ...)
{
  int i = 0;
  void *buffer[100];
  int size = backtrace(buffer, 100);
  char **strings = backtrace_symbols(buffer, size);
  va_list ap;
  va_start(ap, format);
  vfprintf(stderr, format, ap);
  va_end(ap);
  if (NULL != strings)
  {
    INFO(" pthread_key_count: %ld-%ld=%ld", pthread_key_create_count_, pthread_key_delete_count_, pthread_key_create_count_ - pthread_key_delete_count_);
    for (i = 0; i < size; i++)
    {
      INFO("BT[%d] @[%s]", i, strings[i]);
    }
    free(strings);
  }
}

int pthread_key_create(pthread_key_t *key, void (*destructor)(void*))
{
  int (*real_func)(pthread_key_t *key, void (*destructor)(void*)) = (typeof(real_func))dlsym(RTLD_NEXT, "pthread_key_create");
  __sync_fetch_and_add(&pthread_key_create_count_, 1);
  bt("pthread_key_create");
  return real_func(key, destructor);
}

int pthread_key_delete(pthread_key_t key)
{
  int (*real_func)(pthread_key_t key) = (typeof(real_func))dlsym(RTLD_NEXT, "pthread_key_delete");
  __sync_fetch_and_add(&pthread_key_delete_count_, 1);
  bt("pthread_key_delete");
  return real_func(key);
}
