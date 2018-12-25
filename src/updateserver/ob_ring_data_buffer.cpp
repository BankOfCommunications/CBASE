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
#include "common/utility.h"
#include "common/ob_malloc.h"
#include "ob_ring_data_buffer.h"
#include "ob_data_block.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    // x != 0
    bool is_power_of_2(int64_t x)
    {
      return 0 == (x & (x-1));
    }

    ObRingDataBuffer::ObRingDataBuffer(): block_size_shift_(0),
                                          n_blocks_(0), start_pos_(0), end_pos_(0)
    {}

    ObRingDataBuffer::~ObRingDataBuffer()
    {}

    bool ObRingDataBuffer::is_inited() const
    {
      return 0 < n_blocks_ && 0 < block_size_shift_;
    }

    int ObRingDataBuffer::dump_for_debug() const
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "pos=[%ld,%ld)", start_pos_, end_pos_);
      TBSYS_LOG(INFO, "block=%ld*(1<<%ld)", n_blocks_, block_size_shift_);
      if (NULL != blocks_)
      {
        for(int i = 0; i < n_blocks_; i++)
        {
          blocks_[i].dump_for_debug();
        }
      }
      return err;
    }

    int ObRingDataBuffer::check_state() const
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      return err;
    }

    int64_t ObRingDataBuffer::get_next_block_pos(const int64_t pos) const
    {
      return ((pos >> block_size_shift_) + 1) << block_size_shift_;
    }

    int ObRingDataBuffer::init(const int64_t n_blocks, const int64_t block_size_shift)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (n_blocks <= 0 || n_blocks > MAX_N_BLOCKS || 0 > block_size_shift)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for (int64_t i = 0; OB_SUCCESS == err && i < n_blocks; i++)
      {
        if (OB_SUCCESS != (err = blocks_[i].init(1<<block_size_shift)))
        {
          TBSYS_LOG(ERROR, "blocks[%ld].init()=>%d", i, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        block_size_shift_ = block_size_shift;
        n_blocks_ = n_blocks;
      }
      return err;
    }

    int64_t ObRingDataBuffer::get_start_pos() const
    {
      return start_pos_;
    }

    int64_t ObRingDataBuffer::get_end_pos() const
    {
      return end_pos_;
    }

    bool ObRingDataBuffer::is_pos_valid(const int64_t pos) const
    {
      return pos >= start_pos_ && pos < end_pos_;
    }

    int64_t ObRingDataBuffer::get_block_id(int64_t pos) const
    {
      return (pos>>block_size_shift_) % n_blocks_;
    }

    int64_t ObRingDataBuffer::get_block_offset(int64_t pos) const
    {
      return pos & ((1<<block_size_shift_)-1);
    }

    int64_t ObRingDataBuffer::get_block_size() const
    {
      return 1<<block_size_shift_;
    }

    int ObRingDataBuffer::get_read_pos(const int64_t pos, int64_t& real_pos) const
    {
      int err = OB_SUCCESS;
      // 如果读到了一个block末尾，需要跳到下一个block的开始
      // 1. 如果pos所在的block(cur_block)是当前正在写的block，进入这个函数之前保证cur_block->end_pos_ > get_block_offset(pos)
      //    所以不会跳过正在写的block
      // 2. 如果cur_block被覆盖，可能会跳过也可能不会跳过cur_block，并且read_count可能为负，不过返回读取的数据之前要再次检验
      //    读取位置的有效性，所以real_pos和read_count错误也没问题。
      real_pos = blocks_[get_block_id(pos)].get_end_pos() <= get_block_offset(pos)? get_next_block_pos(pos): pos;
      return err;
    }

    int ObRingDataBuffer::read(const int64_t pos, int64_t& real_pos, char* buf, const int64_t len, int64_t& read_count) const
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 >= len || 0 > pos)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "read(buf=%p[%ld], pos=%ld):invalid argument", buf, len, pos);
      }
      else if (pos == end_pos_) // 这种情况pos是有效的，所以返回OB_SUCCESS，避免重新定位
      {
        real_pos = pos;
        read_count = 0;
      }
      else if (!is_pos_valid(pos))
      {
        err = OB_DATA_NOT_SERVE;
      }
      else if (OB_SUCCESS != (err = get_read_pos(pos, real_pos))) // real_pos和read_count可能都是错误的
      {
        TBSYS_LOG(ERROR, "get_read_pos(pos=%ld)=>%d", pos, err);
      }
      else if (OB_SUCCESS != (err = blocks_[get_block_id(real_pos)].read(get_block_offset(real_pos), buf, len, read_count))
        && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "block[%ld].read(pos=%ld)=>%d", pos>>block_size_shift_, pos, err);
      }
      else if (OB_DATA_NOT_SERVE == err)
      {}
      else if (!is_pos_valid(pos))
      {
        err = OB_DATA_NOT_SERVE;
      }
      return err;
    }

    int ObRingDataBuffer::next_block_for_append()
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {
        end_pos_ = get_next_block_pos(end_pos_);
        start_pos_ = max(start_pos_, end_pos_ - (n_blocks_-1) * (1<<block_size_shift_));
      }
      if (OB_SUCCESS == err && OB_SUCCESS != (err = blocks_[get_block_id(end_pos_)].reset()))
      {
        TBSYS_LOG(ERROR, "block[%ld].reset(new_pos=%ld)=>%d", end_pos_>>block_size_shift_, end_pos_, err);
      }
      __sync_synchronize();
      return err;
    }

    int ObRingDataBuffer::append(const char* buf, const int64_t len)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 > len || len + ObDataBlock::BLOCK_RESERVED_BYTES > (1<<block_size_shift_))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "append(buf=%p[%ld]):invalid argument", buf, len);
      }
      else if (blocks_[get_block_id(end_pos_)].get_remain_size() < len
               && OB_SUCCESS != (err = next_block_for_append()))
      {
        TBSYS_LOG(ERROR, "next_block_for_append()=%d", err);
      }
      else if (OB_SUCCESS != (err = blocks_[get_block_id(end_pos_)].append(buf, len)))
      {
        TBSYS_LOG(ERROR, "blocks[%ld].append(pos=%ld)=>%d", end_pos_>>block_size_shift_, end_pos_, err);
      }
      else
      {
        __sync_synchronize();
        end_pos_ += len;
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase


