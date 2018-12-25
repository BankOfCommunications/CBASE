/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * snappy_compressor.cpp is for what ...
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *
 */

#include <new>
#include <string>
#include <snappy.h>
#include "snappy_compressor.h"

const char * SnappyCompressor::NAME = "snappy";

int SnappyCompressor::compress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;
  
  if (NULL == dst_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size)
  {
    ret = COM_E_INVALID_PARAM;
  }
  else if ((src_data_size + get_max_overflow_size(src_data_size)) > dst_buffer_size)
  {
    ret = COM_E_OVERFLOW;
  }
  else
  {
    snappy::RawCompress(src_buffer, static_cast<size_t>(src_data_size), dst_buffer,
                        reinterpret_cast<size_t*>(&dst_data_size));
  }
  return ret;
}

int SnappyCompressor::decompress(const char *src_buffer,
                                 const int64_t src_data_size,
                                 char *dst_buffer,
                                 const int64_t dst_buffer_size,
                                 int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;
  bool flag = true;
  size_t decom_size = 0;
  if (NULL == dst_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size)
  {
    ret = COM_E_INVALID_PARAM;
  }
  
  if (COM_E_NOERROR == ret)
  {
    flag = snappy::GetUncompressedLength(src_buffer,static_cast<size_t>(src_data_size), &decom_size);
    if (!flag)
    {
      ret = COM_E_INTERNALERROR;
    }
  }

  if (COM_E_NOERROR == ret)
  {
    if (decom_size > (size_t)dst_buffer_size)
    {
      ret = COM_E_OVERFLOW;
    }
  }

  if (COM_E_NOERROR == ret)
  {
    flag = snappy::RawUncompress(src_buffer, static_cast<size_t>(src_data_size), dst_buffer);
    if (!flag)
    {
      ret = COM_E_INTERNALERROR;
    }
    else
    {
      dst_data_size = decom_size;
    }
  }
  return ret;
}

const char * SnappyCompressor::get_compressor_name() const
{
  return NAME;
}

int64_t SnappyCompressor::get_max_overflow_size(const int64_t src_data_size) const
{
  int64_t size = 32 + src_data_size/6;
  return size;
}

ObCompressor *create()
{
  return (new(std::nothrow) SnappyCompressor());
}

void destroy(ObCompressor *snappy)
{
  if (NULL != snappy)
  {
    delete snappy;
    snappy = NULL;
  }
}
