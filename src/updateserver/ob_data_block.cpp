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
#include "tbsys.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "ob_data_block.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    int64_t clear_lower_bits(const int64_t x, int64_t n_bits)
    {
      return x & ~((1<<n_bits)-1);
    }

    int64_t get_lower_bits(const int64_t x, int64_t n_bits)
    {
      return x & ((1<<n_bits)-1);
    }

    ObDataBlock::ObDataBlock(): block_buf_(NULL), block_size_(0), end_pos_(0)
    {}

    ObDataBlock::~ObDataBlock()
    {
      if (NULL != block_buf_)
      {
        ob_free(block_buf_);
        block_buf_ = NULL;
      }
    }

    bool ObDataBlock::is_inited() const
    {
      return NULL != block_buf_ && 0 < block_size_;
    }

    int ObDataBlock::init(const int64_t block_size)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (0 >= block_size)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (block_buf_ = (char*)ob_malloc(block_size, ObModIds::OB_UPS_DATA_BLOCK)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        block_size_ = block_size;
      }
      return err;
    }

    int ObDataBlock::dump_for_debug() const
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "blocks[0--pos[%ld]--size[%ld]]}", end_pos_, block_size_);
      return err;
    }

    int64_t ObDataBlock::get_block_size() const
    {
      return block_size_;
    }

    int64_t ObDataBlock::get_end_pos() const
    {
      return end_pos_;
    }

    int64_t ObDataBlock::get_remain_size() const
    {
      return get_block_size() - get_end_pos() - BLOCK_RESERVED_BYTES;
    }

    int ObDataBlock::reset()
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {
        end_pos_ = 0;
      }
      return err;
    }

    int ObDataBlock::read(const int64_t pos, char* buf, const int64_t len, int64_t& read_count) const
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 >= len || 0 > pos)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "read(buf=%p[%ld], pos=%ld): invalid argument", buf, len, pos);
      }
      else if (0 > (read_count = min(len, end_pos_ - pos)))
      {
        err = OB_DATA_NOT_SERVE;
        TBSYS_LOG(DEBUG, "read(pos=%ld, len=%ld, end_pos=%ld, block_size=%ld): unexpected error.",
                  pos, len, end_pos_, get_block_size());
      }
      else
      {
        memcpy(buf, block_buf_ + pos, read_count);
      }
      return err;
    }

    int ObDataBlock::append(const char* buf, const int64_t len)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "append(buf=%p[%ld]): invalid argument", buf, len);
      }
      else if (get_end_pos() + len + BLOCK_RESERVED_BYTES > get_block_size())
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "append(end_pos=%ld, len=%ld, block_size=%ld): unexpected error.",
                  get_end_pos(), len, get_block_size());
      }
      else
      {
        memcpy(block_buf_ + get_end_pos(), buf, len);
        __sync_synchronize();
        end_pos_ += len;
      }
      return err;
    }

  }; // end namespace updateserver
}; // end namespace oceanbase
