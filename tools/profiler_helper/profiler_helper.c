/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - This is a shared library used to invoke google perf tools start and stop
 */

#include <google/profiler.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-x86-64.so.2";

#define SIG_PROFILER_START   60
#define SIG_PROFILER_STOP    61
#define SIG_PROFILER_FLUSH   62

void __attribute__ ((constructor)) initialize(void);
void __attribute__ ((destructor))  destroy   (void);

void register_handlers             ();
void sig_profiler_start_handler    (int sig);
void sig_profiler_stop_handler     (int sig);
void sig_profiler_flush_handler    (int sig);

// initialize the shared library
void initialize(void)
{
  register_handlers();
}

// destroy the shared library
void destroy(void)
{
}

void register_handlers()
{
  signal(SIG_PROFILER_START, sig_profiler_start_handler);
  signal(SIG_PROFILER_STOP,  sig_profiler_stop_handler);
  signal(SIG_PROFILER_FLUSH, sig_profiler_flush_handler);
}

void sig_profiler_start_handler(int sig)
{
  char * profile_output = getenv("PROFILEOUTPUT");
  if (NULL != profile_output)
  {
    ProfilerStart(profile_output);
    ProfilerRegisterThread();
  }
}

void sig_profiler_stop_handler(int sig)
{
  ProfilerStop();
}

void sig_profiler_flush_handler(int sig)
{
  ProfilerFlush();
}

void so_main()
{
  printf("This library is used to control google perf cpu profiler with signals:\n"
         "    %d     Profiler start\n"
         "    %d     Profiler stop\n"
         "    %d     Profiler flush\n"
         "Profile statistics will output to file with name of environment PROFILEOUTPUT indicating.\n"
         "\n"
         "This library is built with gcc %s at %s %s.\n\n",
         SIG_PROFILER_START, SIG_PROFILER_STOP, SIG_PROFILER_FLUSH,
         __VERSION__, __DATE__, __TIME__
         );
  exit(0);
}

#ifdef __cplusplus
}  // extern "C"
#endif

