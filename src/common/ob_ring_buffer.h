/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ring_buffer.h,v 0.1 2011/04/26 10:40:16 ruohai Exp $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - modified by chuanhui <rizhao.ych@taobao.com>
 *
 */
#ifndef __OCEANBASE_COMMON_OB_RING_BUFFER_H__
#define __OCEANBASE_COMMON_OB_RING_BUFFER_H__

#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    struct RingBlock
    {
      uint32_t magic;
      int32_t  task_num;
      int64_t  block_size;
      int64_t  data_size;
      int64_t  version;
      int64_t  queue_idx;
      char     buf[0];
    };

    struct RingTask
    {
      uint32_t magic;
      int32_t  size;
      RingBlock* block;
      char     buf[0];
    };

    // ObRingBuffer is used to receive network data.
    // Note: Memory used by this class will be re-used but not freed to system.
    class ObRingBuffer
    {
      public:
        explicit ObRingBuffer();
        ~ObRingBuffer();

        int clear();
        int init();

      public:
        // Push the task to ring buffer, will copy the task content.
        // This method is not thread safe.
        int push_task(const void* ptr, const int64_t task_len, void*& ret_ptr);
        // Pop the specified task.
        // This method is not thread safe.
        int pop_task(const void* ptr);
        // Get the next pre-precess task, will copy the task content to the specified buf.
        int get_next_preprocess_task(char* buf, const int64_t buf_len, int64_t& task_len);

      public:
        // Get allocated memory.
        int64_t get_memory_allocated() const;

      private:
        void reset_();
        int add_a_block_();
        int alloc_a_block_(RingBlock*& block);
        int get_next_offset_(const int64_t offset, int64_t& next_offset);
        int translate_offset_(int64_t& offset, RingTask*& ret_next_task);
        int is_offset_valid_(const int64_t offset);
        int64_t get_offset_(const int64_t version, const int64_t queue_idx, const int64_t task_offset);
        void extra_offset_(const int64_t offset, int64_t& version, int64_t& queue_idx, int64_t& task_offset);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObRingBuffer);

      private:
        static const int64_t DEF_BLOCK_BITS = 22;
        static const int64_t DEF_BLOCK_SIZE = 1 << DEF_BLOCK_BITS; // 4M
        static const int64_t RING_QUEUE_BITS = 10;
        static const int64_t RING_QUEUE_SIZE = 1 << RING_QUEUE_BITS;  // 1k
        static const uint32_t BLOCK_MAGIC_NUM = 0xcececece;      // block magic num
        static const uint32_t TASK_MAGIC_NUM = 0xcdcdcdcd;       // task magic num

      private:
        struct RingQueue
        {
          int64_t ring_head_idx;
          int64_t ring_tail_idx;
          RingBlock* blocks[RING_QUEUE_SIZE]; // shared by push/pop and pre-process thread
          int64_t versions[RING_QUEUE_SIZE];
        };

      private:
        bool is_init_;
        RingQueue queue_;
        volatile int64_t pre_process_offset_;
        volatile int64_t last_write_offset_;
        volatile int64_t next_read_offset_;
    };
  }
}

#endif //__OB_RING_BUFFER_H__

