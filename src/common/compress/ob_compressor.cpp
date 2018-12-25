/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_compressor.h is for what ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dlfcn.h>
#include "ob_compressor.h"

#define LIB_FNAME_FORMAT_LENGTH 6 // lib.so
#define MAX_LIB_FNAME_BUFFER_SIZE (MAX_LIB_NAME_LENGTH + LIB_FNAME_FORMAT_LENGTH + 1)

static void *get_lib_handle_(const char *compressor_lib_name)
{
  void *sohandle = NULL;
  static __thread char soname[MAX_LIB_FNAME_BUFFER_SIZE];
  int64_t inner_ret = 0;
  if (NULL != compressor_lib_name
      && -1 < (inner_ret = snprintf(soname, MAX_LIB_FNAME_BUFFER_SIZE, "lib%s.so", compressor_lib_name))
      && MAX_LIB_FNAME_BUFFER_SIZE > inner_ret)
  {
    sohandle = dlopen(soname, RTLD_LAZY);
  }
  return sohandle;
}

ObCompressor *create_compressor(const char *compressor_lib_name)
{
  ObCompressor *ret = NULL;
  void *sohandle = NULL;
  compressor_constructor_t compressor_constructor = NULL;
  if (NULL != (sohandle = get_lib_handle_(compressor_lib_name)))
  {
    if (NULL != (compressor_constructor = (compressor_constructor_t)dlsym(sohandle, "create")))
    {
      if (NULL != (ret = compressor_constructor()))
      {
        ret->set_sohandle(sohandle);
      }
    }
    else
    {
      dlclose(sohandle);
    }
  }
  else
  {
    fprintf(stderr, "%s\n", dlerror());
  }
  return ret;
}

void destroy_compressor(ObCompressor *compressor)
{
  void *sohandle = NULL;
  compressor_deconstructor_t compressor_deconstructor = NULL;
  if (NULL != compressor)
  {
    if (NULL != (sohandle = compressor->get_sohandle()))
    {
      if (NULL != (compressor_deconstructor = (compressor_deconstructor_t)dlsym(sohandle, "destroy")))
      {
        compressor_deconstructor(compressor);
      }
      dlclose(sohandle);
    }
  }
}

