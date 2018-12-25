/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * none_compressor.cpp for compressor without compress. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <new>
#include "none_compressor.h"

#define COMPRESSOR_NAME "none";

int NoneCompressor::compress(const char *src_buffer,
                             const int64_t src_data_size,
                             char *dst_buffer,
                             const int64_t dst_buffer_size,
                             int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;

  (void)src_buffer;
  (void)dst_buffer;
  (void)dst_buffer_size;
  dst_data_size = src_data_size;

  return ret;
}

int NoneCompressor::decompress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;

  (void)src_buffer;
  (void)dst_buffer;
  (void)dst_buffer_size;
  dst_data_size = src_data_size;

  return ret;
}

int64_t NoneCompressor::get_max_overflow_size(const int64_t src_data_size) const
{
  (void)src_data_size;
  return 0;
}

const char *NoneCompressor::get_compressor_name() const
{
  return COMPRESSOR_NAME;
}

ObCompressor *create()
{
  return (new(std::nothrow) NoneCompressor());
}

void destroy(ObCompressor *none)
{
  if (NULL != none)
  {
    delete none;
    none = NULL;
  }
}
