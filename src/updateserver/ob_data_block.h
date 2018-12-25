/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_DATA_BLOCK_H__
#define __OB_UPDATESERVER_OB_DATA_BLOCK_H__
#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObDataBlock
    {
      public:
        // block末尾至少会保留的字节数，保证切换block的操作总是可以在读这个block时检测到
        static const int64_t BLOCK_RESERVED_BYTES = 16;
        ObDataBlock();
        ~ObDataBlock();
      public:
        int init(const int64_t block_size_shift);
        // 这个read()函数不负责判断读取数据的有效性，数据的有效性由调用者检查
        int read(const int64_t pos, char* buf, const int64_t len, int64_t& read_count) const;
        // append的数据长度超过了block_size, 返回OB_ERR_UNEXPECTED;
        // append一批数据之前如果剩余空间不够，返回OB_BUF_NOT_ENOUGH，
        int append(const char* buf, const int64_t len);
        // reset()设置end_pos_的值0
        int reset();
      public:
        int64_t get_remain_size() const;
        int64_t get_block_size() const;
        int64_t get_end_pos() const;
        int dump_for_debug() const;
      protected:
        bool is_inited() const;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObDataBlock);
        char* block_buf_;
        int64_t block_size_;
        volatile int64_t end_pos_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_DATA_BLOCK_H__ */
