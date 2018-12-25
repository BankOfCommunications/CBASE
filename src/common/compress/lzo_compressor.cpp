/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * lzo_compressor.cpp is for what ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <new>
#include <string.h>
#include <stdio.h>
#include "lzo_compressor.h"
#include "lzo/lzo1x.h"

#define COMPRESSOR_NAME "lzo_1.0";
#define COMPRESS_METHOD lzo1x_1_compress
#define DECOMPRESS_METHOD lzo1x_decompress

int LZOCompressor::trans_errno_(int lzo_errno)
{
  int ret = COM_E_NOERROR;
  switch (lzo_errno)
  {
    case LZO_E_OK:
      ret = COM_E_NOERROR;
      break;
    case LZO_E_EOF_NOT_FOUND:
    case LZO_E_INPUT_OVERRUN:
    case LZO_E_OUTPUT_OVERRUN:
    case LZO_E_INPUT_NOT_CONSUMED:
    case LZO_E_LOOKBEHIND_OVERRUN:
      ret = COM_E_DATAERROR;
      break;
    //  ret = COM_E_OVERFLOW;
    //  break;
    case LZO_E_ERROR:
    default:
      ret = COM_E_INTERNALERROR;
      break;
  }
  return ret;
}

int LZOCompressor::compress(const char *src_buffer,
                          const int64_t src_data_size,
                          char *dst_buffer,
                          const int64_t dst_buffer_size,
                          int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;
  static __thread char wrkmem[LZO1X_1_MEM_COMPRESS];
  memset(wrkmem, 0, LZO1X_1_MEM_COMPRESS);
  int lzo_errno = LZO_E_OK;
  int64_t compress_ret_size = dst_buffer_size;
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
  else if (LZO_E_OK != (lzo_errno = lzo_init()))
  {
    ret = trans_errno_(lzo_errno);
  }
  else if (LZO_E_OK != (lzo_errno = COMPRESS_METHOD((lzo_bytep)src_buffer, (lzo_uint)src_data_size, (lzo_bytep)dst_buffer,
                                                    (lzo_uint*)&compress_ret_size, wrkmem)))
  {
    ret = trans_errno_(lzo_errno);
  }
  else
  {
    dst_data_size = compress_ret_size;
  }
  return ret;
}

int LZOCompressor::decompress(const char *src_buffer,
                            const int64_t src_data_size,
                            char *dst_buffer,
                            const int64_t dst_buffer_size,
                            int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;
  int lzo_errno = LZO_E_OK;
  int64_t decompress_ret_size = dst_buffer_size;
  if (NULL == dst_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size)
  {
    ret = COM_E_INVALID_PARAM;
  }
  else if (LZO_E_OK != (lzo_errno = lzo_init()))
  {
    ret = trans_errno_(lzo_errno);
  }
  else if (LZO_E_OK != (lzo_errno = DECOMPRESS_METHOD((lzo_bytep)src_buffer, (lzo_uint)src_data_size, (lzo_bytep)dst_buffer,
                                                      (lzo_uint*)&decompress_ret_size, NULL)))
  {
    ret = trans_errno_(lzo_errno);
  }
  else
  {
    dst_data_size = decompress_ret_size;
  }
  return ret;
}

int64_t LZOCompressor::get_max_overflow_size(const int64_t src_data_size) const
{
  //When dealing with uncompressible data, LZO expands the input
  //block by a maximum of 16 bytes per 1024 bytes input.
  int64_t ret = ((src_data_size / 1024l + ((0 == src_data_size % 1024l) ? 0l : 1l)) * 16l);
  return ret;
}

const char *LZOCompressor::get_compressor_name() const
{
  return COMPRESSOR_NAME;
}

ObCompressor *create()
{
  return (new(std::nothrow) LZOCompressor());
}

void destroy(ObCompressor *lzo)
{
  if (NULL != lzo)
  {
    delete lzo;
    lzo = NULL;
  }
}

