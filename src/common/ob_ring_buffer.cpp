/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ring_buffer.cpp,v 0.1 2011/04/26 10:40:58 ruohai Exp $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - modified by chuanhui <rizhao.ych@taobao.com>
 *
 */
#include "ob_ring_buffer.h"
#include "ob_atomic.h"

namespace oceanbase
{
  namespace common
  {
    ObRingBuffer::ObRingBuffer()
    {
      reset_();
    }

    ObRingBuffer::~ObRingBuffer()
    {
      clear();
    }

    int ObRingBuffer::clear()
    {
      RingBlock* block = NULL;

      if (is_init_)
      {
        int64_t idx = queue_.ring_head_idx;
        while (1)
        {
          block = queue_.blocks[idx];
          if (NULL == block)
          {
            TBSYS_LOG(ERROR, "the %ld-th block is NULL", idx);
          }
          else
          {
            ob_free(block, ObModIds::OB_COMMON_NETWORK);
          }
          if (idx == queue_.ring_tail_idx)
          {
            break;
          }
          else
          {
            idx = (idx + 1) % RING_QUEUE_SIZE;
          }
        }
      }
      reset_();

      return OB_SUCCESS;
    }

    void ObRingBuffer::reset_()
    {
      is_init_ = false;
      memset(&queue_, 0x00, sizeof(queue_));
      for (int64_t i = 0; i < RING_QUEUE_SIZE; ++i)
      {
        queue_.versions[i] = -1;
      }
      pre_process_offset_ = -1;
      last_write_offset_ = -1;
      next_read_offset_ = 0;
    }

    int ObRingBuffer::init()
    {
      int err = OB_SUCCESS;

      RingBlock* block = NULL;
      err = alloc_a_block_(block);
      if (OB_SUCCESS != err || NULL == block)
      {
        TBSYS_LOG(WARN, "failed to alloc block from system, err=%d, block=%p", err, block);
        err = OB_ERROR;
      }
      else
      {
        block->version = 0;
        block->queue_idx = 0;

        // the queue will always be non-empty
        queue_.ring_head_idx = 0;
        queue_.ring_tail_idx = 0;
        queue_.blocks[queue_.ring_tail_idx] = block;
        queue_.versions[queue_.ring_tail_idx] = 0;

        is_init_ = true;
      }

      return err;
    }

    int ObRingBuffer::push_task(const void* ptr, const int64_t task_len, void*& ret_ptr)
    {
      int err = OB_SUCCESS;
      RingBlock* block = NULL;
      ret_ptr = NULL;

      if (NULL == ptr || task_len <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ptr=%p, task_len=%ld", ptr, task_len);
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_init_)
      {
        TBSYS_LOG(WARN, "not init");
        err = OB_NOT_INIT;
      }
      else
      {
        int64_t size = sizeof(RingTask) + task_len;
        block = queue_.blocks[queue_.ring_tail_idx];
        if (NULL == block)
        {
          TBSYS_LOG(ERROR, "the %ld-th block is NULL", queue_.ring_tail_idx);
          err = OB_ERROR;
        }
        else if (block->data_size + size > block->block_size)
        {
          // current block has no enough space
          err = add_a_block_();
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to add block, err=%d", err);
          }
          else
          {
            block = queue_.blocks[queue_.ring_tail_idx];
          }
        }

        if (OB_SUCCESS == err)
        {
          if (block->data_size + size > block->block_size)
          {
            TBSYS_LOG(ERROR, "too large task, data_size=%ld, task_size=%ld, block_size=%ld",
                block->data_size, size, block->block_size);
            err = OB_ERROR;
          }
          else
          {
            RingTask task;
            task.magic = TASK_MAGIC_NUM;
            task.size = static_cast<int32_t>(task_len);
            task.block = block;
            ++(block->task_num);
            int64_t offset = get_offset_(queue_.versions[queue_.ring_tail_idx],
                queue_.ring_tail_idx, block->data_size);
            ret_ptr = block->buf + block->data_size + sizeof(RingTask);

            *((RingTask*)(block->buf + block->data_size)) = task;
            memcpy(block->buf + block->data_size + sizeof(RingTask), ptr, task_len);
            block->data_size += (sizeof(RingTask) + task_len);

            last_write_offset_ = offset;
          }
        }
      }

      return err;
    }

    int ObRingBuffer::pop_task(const void* ptr)
    {
      int err = OB_SUCCESS;
      if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid param, ptr=%p", ptr);
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_init_)
      {
        TBSYS_LOG(WARN, "not init");
        err = OB_NOT_INIT;
      }
      else
      {
        RingTask* task = (RingTask*) ((const char*) ptr - sizeof(RingTask));
        if (task->magic != TASK_MAGIC_NUM)
        {
          TBSYS_LOG(ERROR, "invalid task magic, expected=%u, real=%u",
              TASK_MAGIC_NUM, task->magic);
          err = OB_ERROR;
        }
        else
        {
          RingBlock* block = task->block;

          if (NULL == block)
          {
            TBSYS_LOG(ERROR, "block is NULL");
            err = OB_ERROR;
          }
          else if (block->magic != BLOCK_MAGIC_NUM)
          {
            TBSYS_LOG(ERROR, "invalid block magic, expected=%u, real=%u",
                BLOCK_MAGIC_NUM, block->magic);
            err = OB_ERROR;
          }
          else if (block->task_num <= 0)
          {
            TBSYS_LOG(ERROR, "the block has no task, task_num=%d", block->task_num);
            err = OB_ERROR;
          }
          else
          {
            int64_t version = block->version;
            int64_t queue_idx = block->queue_idx;
            int64_t task_offset = ((char*) task) - block->buf;
            int64_t next_task_offset = task_offset + task->size + sizeof(RingTask);

            if (next_task_offset > block->data_size)
            {
              TBSYS_LOG(WARN, "invalid status, next_task_offset=%ld, data_size=%ld",
                  next_task_offset, block->data_size);
              err = OB_ERROR;
            }
            else
            {
              int64_t pop_offset = get_offset_(version, queue_idx, task_offset);
              if (pop_offset < next_read_offset_)
              {
                TBSYS_LOG(WARN, "Pop an invalid task, pop_offset=%ld, next_read_offset_=%ld",
                    pop_offset, next_read_offset_);
                err = OB_ERROR;
              }
              else
              {
                next_read_offset_ = get_offset_(version, queue_idx, next_task_offset);
              }
            }
          }

          if (OB_SUCCESS == err)
          {
            --(block->task_num);
          }
        }
      }

      return err;
    }

    int64_t ObRingBuffer::get_memory_allocated() const
    {
      int64_t size = DEF_BLOCK_SIZE + sizeof(RingBlock) + sizeof(RingTask);
      int64_t queue_size = (queue_.ring_tail_idx - queue_.ring_head_idx + 1 + RING_QUEUE_SIZE) % RING_QUEUE_SIZE;
      if (0 == queue_size)
      {
        queue_size = RING_QUEUE_SIZE;
      }
      return queue_size * size;
    }

    int ObRingBuffer::add_a_block_()
    {
      int err = OB_SUCCESS;
      RingBlock* block = NULL;

      RingBlock* first_block = queue_.blocks[queue_.ring_head_idx];
      if (NULL != first_block && 0 == first_block->task_num)
      {
        // reuse head block of queue
        queue_.blocks[queue_.ring_head_idx] = NULL;
        queue_.ring_head_idx = (queue_.ring_head_idx + 1) % RING_QUEUE_SIZE;
        block = first_block;
        block->data_size = 0;
        block->version = -1;
        block->queue_idx = -1;
      }
      else if ((queue_.ring_tail_idx + 1) % RING_QUEUE_SIZE == queue_.ring_head_idx)
      {
        TBSYS_LOG(ERROR, "the ring buffer is full");
        err = OB_SIZE_OVERFLOW;
      }
      else
      {
        err = alloc_a_block_(block);
        if (OB_SUCCESS != err || NULL == block)
        {
          TBSYS_LOG(WARN, "failed to alloc block from system, err=%d", err);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        memset(block->buf, 0xfe, block->block_size); // for debug only
      }

      if (OB_SUCCESS == err)
      {
        // add block to ring queue
        queue_.ring_tail_idx = (queue_.ring_tail_idx + 1) % RING_QUEUE_SIZE;
        ++queue_.versions[queue_.ring_tail_idx];

        block->version = queue_.versions[queue_.ring_tail_idx];
        block->queue_idx = queue_.ring_tail_idx;

        queue_.blocks[queue_.ring_tail_idx] = block;
      }
      return err;
    }

    int ObRingBuffer::alloc_a_block_(RingBlock*& block)
    {
      int err = OB_SUCCESS;
      block = NULL;

      // allocate block with ob_malloc
      int64_t size = DEF_BLOCK_SIZE + sizeof(RingBlock);
      void* ptr = ob_malloc(size, ObModIds::OB_COMMON_NETWORK);
      if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "not enough memory");
        err = OB_MEM_OVERFLOW;
      }
      else
      {
        block = (RingBlock*) ptr;
        block->magic = BLOCK_MAGIC_NUM;
        block->task_num = 0;
        block->block_size = DEF_BLOCK_SIZE;
        block->data_size = 0;
        block->version = -1;
        block->queue_idx = -1;
      }

      return err;
    }

    int ObRingBuffer::get_next_preprocess_task(char* buf, const int64_t buf_len, int64_t& task_len)
    {
      int err = OB_SUCCESS;
      RingTask* next_task = NULL;

      int64_t ori_offset = pre_process_offset_;
      int64_t new_offset = 0;
      int64_t next_read_offset = next_read_offset_;
      int64_t last_write_offset = last_write_offset_;

      task_len = 0;

      if (NULL == buf || buf_len <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld", buf, buf_len);
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_init_)
      {
        TBSYS_LOG(WARN, "not init");
        err = OB_NOT_INIT;
      }
      else
      {
        if (ori_offset < next_read_offset)
        {
          new_offset = next_read_offset;
        }
        else
        {
          err = get_next_offset_(ori_offset, new_offset);
          if (OB_NEED_RETRY == err)
          {
            // does nothing
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get next offset, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          err = translate_offset_(new_offset, next_task);
          if (OB_NEED_RETRY == err)
          {
            // does nothing
          }
          else if (OB_SUCCESS != err || NULL == next_task)
          {
            TBSYS_LOG(WARN, "failed to get next offset, ori_offset=%ld, err=%d", ori_offset, err);
            err = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        if (new_offset > last_write_offset || last_write_offset < 0)
        {
          err = OB_NEED_RETRY;
        }
        else
        {
          int64_t tmp_val = atomic_compare_exchange((volatile uint64_t*) &pre_process_offset_, new_offset, ori_offset);
          if (tmp_val != ori_offset)
          {
            // other pre-process thread get the task, need retry
            err = OB_NEED_RETRY;
          }
          else
          {
            if (next_task->size > buf_len)
            {
              TBSYS_LOG(WARN, "buf is not enough, task_size=%d, buf_len=%ld", next_task->size, buf_len);
              err = OB_NEED_RETRY;
            }
            else
            {
              // copy to outer buffer
              memcpy(buf, next_task->buf, next_task->size);
              task_len = next_task->size;

              // double check new_offset
              err = is_offset_valid_(new_offset);
              if (OB_NEED_RETRY == err)
              {
                // does nothing
              }
              else if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "invalid offset or block reused, new_offset=%ld, err=%d", new_offset, err);
              }
            }
          }
        }
      }

      return err;
    }

    int ObRingBuffer::get_next_offset_(const int64_t offset, int64_t& next_offset)
    {
      int err = OB_SUCCESS;

      if (offset < 0)
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t version = 0;
        int64_t queue_idx = 0;
        int64_t task_offset = 0;
        extra_offset_(offset, version, queue_idx, task_offset);

        RingBlock* block = queue_.blocks[queue_idx];
        if (NULL == block || version != queue_.versions[queue_idx])
        {
          TBSYS_LOG(WARN, "maybe the block is re-used, block=%p, version=%ld, expected_version=%ld",
              block, version, queue_.versions[queue_idx]);
          err = OB_NEED_RETRY;
        }
        else
        {
          RingTask* cur_task = (RingTask*) (block->buf + task_offset);
          if (TASK_MAGIC_NUM != cur_task->magic)
          {
            TBSYS_LOG(WARN, "invalid magic, expected=%u, real=%u", TASK_MAGIC_NUM, cur_task->magic);
            err = OB_NEED_RETRY;
          }
          else
          {
            int64_t task_size = cur_task->size + sizeof(RingTask);
            if (task_size < 0 || task_offset + task_size > block->data_size)
            {
              TBSYS_LOG(WARN, "offset overflow, task_size=%ld, task_offset=%ld, block_data_size=%ld",
                  task_size, task_offset, block->data_size);
              err = OB_NEED_RETRY;
            }
            else
            {
              next_offset = offset + task_size;
            }
          }
        }
      }

      return err;
    }

    int ObRingBuffer::translate_offset_(int64_t& offset, RingTask*& ret_next_task)
    {
      int err = OB_SUCCESS;

      if (offset < 0)
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t last_write_offset = last_write_offset_;

        int64_t version = 0;
        int64_t queue_idx = 0;
        int64_t task_offset = 0;
        extra_offset_(offset, version, queue_idx, task_offset);

        RingBlock* block = queue_.blocks[queue_idx];
        RingTask* next_task = NULL;

        if (offset > last_write_offset)
        {
          TBSYS_LOG(WARN, "preprocess too fast, offset=%ld, last_write_offset=%ld",
              offset, last_write_offset);
          err = OB_NEED_RETRY;
        }
        else if (NULL == block || version != queue_.versions[queue_idx])
        {
          TBSYS_LOG(WARN, "maybe the block is re-used, block=%p, version=%ld, expected_version=%ld",
              block, queue_.versions[queue_idx], version);
          err = OB_NEED_RETRY;
        }
        else if (task_offset == block->data_size)
        {
          // maybe the last task of current block
          int64_t next_queue_idx = queue_idx + 1;
          int64_t next_version = version;
          if (next_queue_idx == RING_QUEUE_SIZE)
          {
            next_queue_idx = 0;
            ++next_version;
          }

          RingBlock* next_block = queue_.blocks[next_queue_idx];
          if (next_block == NULL)
          {
            err = OB_NEED_RETRY;
          }
          else
          {
            next_task = (RingTask*) (next_block->buf);
            // change offset
            offset = get_offset_(next_version, next_queue_idx, 0);
          }
        }
        else
        {
          next_task = (RingTask*) (block->buf + task_offset);
        }

        ret_next_task = next_task;
      }

      return err;
    }

    int ObRingBuffer::is_offset_valid_(const int64_t offset)
    {
      int err = OB_SUCCESS;

      if (offset < 0)
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t next_read_offset = next_read_offset_;
        int64_t last_write_offset = last_write_offset_;

        if (offset > last_write_offset || offset < next_read_offset)
        {
          TBSYS_LOG(WARN, "invalid offset, offset=%ld, next_read_offset=%ld, last_write_offset=%ld",
              offset, next_read_offset, last_write_offset);
          err = OB_NEED_RETRY;
        }
        else
        {
          int64_t version = 0;
          int64_t queue_idx = 0;
          int64_t task_offset = 0;
          extra_offset_(offset, version, queue_idx, task_offset);

          RingBlock* block = queue_.blocks[queue_idx];
          int64_t expected_version = queue_.versions[queue_idx];

          if (NULL == block || version != expected_version)
          {
            TBSYS_LOG(WARN, "invalid queue_idx or version, queue_idx=%ld, block=%p, "
                "version=%ld, expected_version=%ld", queue_idx, block, version, expected_version);
            err = OB_NEED_RETRY;
          }
          else if (task_offset > block->data_size)
          {
            TBSYS_LOG(WARN, "invalid offset, task_offset=%ld, data_size=%ld",
                task_offset, block->data_size);
            err = OB_NEED_RETRY;
          }
        }
      }

      return err;
    }

    int64_t ObRingBuffer::get_offset_(const int64_t version, const int64_t queue_idx, const int64_t task_offset)
    {
      int64_t offset = 0;
      offset = version << RING_QUEUE_BITS;                   // queue_size = 1 << 10
      offset = (offset | queue_idx) << DEF_BLOCK_BITS;       // block_size = 1 << 22
      offset |= task_offset;

      return offset;
    }
    
    void ObRingBuffer::extra_offset_(const int64_t offset, int64_t& version, int64_t& queue_idx, int64_t& task_offset)
    {
      version = offset / DEF_BLOCK_SIZE / RING_QUEUE_SIZE;
      queue_idx = offset / DEF_BLOCK_SIZE % RING_QUEUE_SIZE;
      task_offset = offset % DEF_BLOCK_SIZE;
    }
  }
}
