/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * snappy_compressor.h is for what ...
 *
 * Authors:
 *   fangji.hcm <fangji.hcm@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_

#include "ob_compressor.h"

class SnappyCompressor : public ObCompressor
{
public:
  const static char * NAME;
public:
  int compress(const char *src_buffer,
               const int64_t src_data_size,
               char *dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size);
  int decompress(const char *src_buffer,
                 const int64_t src_data_size,
                 char *dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size);
  const char * get_compressor_name() const;
  int64_t get_max_overflow_size(const int64_t src_data_size) const;
};

extern "C" ObCompressor *create();
extern "C" void destroy(ObCompressor *snappy);
#endif //OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_ 
