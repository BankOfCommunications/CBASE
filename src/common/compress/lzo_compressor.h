/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * lzo_compressor.h is for what ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_COMPRESS_LZO_COMPRESSOR_H_
#define  OCEANBASE_COMMON_COMPRESS_LZO_COMPRESSOR_H_

#include "ob_compressor.h"

class LZOCompressor : public ObCompressor
{
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
  private:
    int trans_errno_(int lzo_errno);
};

extern "C" ObCompressor *create();
extern "C" void destroy(ObCompressor *lzo);

#endif // OCEANBASE_COMMON_COMPRESS_LZO_COMPRESSOR_H_
