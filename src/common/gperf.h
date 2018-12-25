#ifndef __OB_COMMON_GPERF_H__
#define __OB_COMMON_GPERF_H__
#ifdef __NEED_PERF__
#include <gperftools/profiler.h>
struct PerfGuard
{
  PerfGuard(const char* key): file_(getenv(key)) { if (file_) ProfilerStart(file_); }
  ~PerfGuard(){ if (file_) ProfilerStop(); }
  void register_threads() { ProfilerRegisterThread(); }
  const char* file_;
};
#else
struct PerfGuard
{
  PerfGuard(const char* str){ UNUSED(str); }
  ~PerfGuard(){}
  void register_threads() {}
};
#endif
#endif /* __OB_COMMON_GPERF_H__ */
