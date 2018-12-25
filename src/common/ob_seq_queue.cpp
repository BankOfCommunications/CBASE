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
#include "ob_seq_queue.h"

namespace oceanbase
{
  namespace common
  {
    ObSeqQueue::ObSeqQueue(): seq_(0), items_(NULL), limit_(0)
    {}

    ObSeqQueue::~ObSeqQueue()
    {}

    int ObSeqQueue::init(const int64_t limit, SeqItem* buf)
    {
      int err = OB_SUCCESS;
      if (0 >= limit)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "init(limit=%ld, buf=%p): INVALID_ARGUMENT", limit, buf);
      }
      else if (limit_ > 0 || NULL != items_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == (items_ = (buf?: (SeqItem*)buf_holder_.get(sizeof(SeqItem) * limit))))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "buf_holder.get(%ld)=>NULL", sizeof(SeqItem) * limit);
      }
      else
      {
        limit_ = limit;
        memset(items_, 0, sizeof(SeqItem) * limit);
        for(int64_t i = 0; i < limit; i++)
        {
          items_[i].seq_ = -1;
        }
      }
      return err;
    }

    bool ObSeqQueue::is_inited() const
    {
      return NULL != items_ && limit_ > 0;
    }

    int ObSeqQueue::start(const int64_t seq)
    {
      int err = OB_SUCCESS;
      if (0 >= seq)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "start(seq=%ld): INVALID_ARGUMENT", seq);
      }
      else if (NULL == items_)
      {
        err = OB_NOT_INIT;
      }
      else if (seq_ > 0)
      {
        err = OB_INIT_TWICE;
      }
      else
      {
        err = update(seq);
      }
      return err;
    }

    int64_t ObSeqQueue::get_seq()
    {
      return seq_;
    }

    tbsys::CThreadCond* ObSeqQueue::get_cond(const int64_t seq)
    {
      return cond_ + (seq % N_COND);
    }

    int ObSeqQueue::add(const int64_t seq, void* data)
    {
      int err = OB_SUCCESS;
      SeqItem* pitem = NULL;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (seq_ <= 0)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "seq_[%ld] <= 0", seq_);
      }
      else if (seq < seq_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "add(seq[%ld] < seq_[%ld]): INVALID_ARGUMEN", seq, seq_);
      }
      else if (seq_ + limit_ <= seq)
      {
        err = OB_EAGAIN;
      }
      else if (seq <= (pitem = items_ + seq % limit_)->seq_)
      {
        err = OB_ENTRY_EXIST;
        TBSYS_LOG(ERROR, "add(seq=%ld): ENTRY_EXIST", seq);
      }
      else if (!__sync_bool_compare_and_swap(&pitem->seq_, -1, -2))
      {
        err = OB_EAGAIN;
      }
      else
      {
        tbsys::CThreadCond* cond = get_cond(seq);
        cond->lock();
        pitem->data_ = data;
        __sync_synchronize();
        pitem->seq_ = seq;
        cond->signal();
        cond->unlock();
      }
      return err;
    }

    bool ObSeqQueue::next_is_ready() const
    {
      int64_t seq = seq_;
      return NULL != items_ && (items_ + seq % limit_)->seq_ == seq;
    }

    int ObSeqQueue::get(int64_t& seq, void*& data, const int64_t timeout_us)
    {
      int err = OB_EAGAIN;
      SeqItem* pitem = NULL;
      int64_t end_time_us = tbsys::CTimeUtil::getTime() + timeout_us;
      int64_t wait_time_us = timeout_us;
      int wait_time_ms = (int)(wait_time_us/1000LL);
      tbsys::CThreadCond* cond = NULL;
      seq = seq_;
      cond = get_cond(seq);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (seq_ < 0)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "seq_[%ld] < 0", seq_);
      }
      else
      {
        cond->lock();
        while(OB_EAGAIN == err)
        {
          if (seq != seq_)
          {
            break;
          }
          if ((pitem = items_ + seq % limit_)->seq_ != seq)
          {
            if ((wait_time_ms = (int)(wait_time_us/1000LL)) <= 0)
            {
              break;
            }
            else
            {
              cond->wait(wait_time_ms);
              wait_time_us = end_time_us - tbsys::CTimeUtil::getTime();
            }
          }
          else if (__sync_bool_compare_and_swap(&seq_, seq, seq + 1))
          {
            err = OB_SUCCESS;
          }
        }
        cond->unlock();
      }
      if (OB_SUCCESS == err)
      {
        pitem = items_ + seq % limit_;
        data = pitem->data_;
        __sync_synchronize();
        pitem->seq_ = -1;
      }
      return err;
    }

    int ObSeqQueue::update(const int64_t seq)
    {
      int err = OB_SUCCESS;
      if (seq < seq_)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "seq[%ld] < seq_[%ld]", seq, seq_);
      }
      else
      {
        tbsys::CThreadCond* cond = get_cond(seq_);
        cond->lock();
        seq_ = seq;
        cond->signal();
        cond->unlock();
      }
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
