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
#ifndef __OB_COMMON_OB_SEQ_QUEUE_H__
#define __OB_COMMON_OB_SEQ_QUEUE_H__
#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    struct BufHolder
    {
      BufHolder(): buf_(NULL) {}
      ~BufHolder() {
        if (NULL != buf_)
        {
          ob_free(buf_);
          buf_ = NULL;
        }
      }
      const void* get(const int64_t size)
      {
        void* buf = NULL;
        if (NULL != buf_)
        {
          TBSYS_LOG(ERROR, "buf_holder.get(size=%ld): not allowed to get second time", size);
        }
        else if (NULL == (buf = ob_malloc(size, ObModIds::OB_SEQ_QUEUE)))
        {
          TBSYS_LOG(ERROR, "ob_malloc(size=%ld)=>NULL", size);
        }
        else
        {
          buf_ = buf;
        }
        return buf;
      }
      void* buf_;
    };

    //假定有多个线程向队列中加元素，但只有一个线程会取元素，并且不会有两个线程同时向队列中加入编号相同的元素。
    class ObSeqQueue
    {
      static const int64_t N_COND = 256;
      struct SeqItem
      {
        volatile int64_t seq_;
        void* volatile data_;
      };
      public:
        ObSeqQueue();
        ~ObSeqQueue();
      public:
        int init(const int64_t limit, SeqItem* buf = NULL);
        int start(const int64_t seq);
        int add(const int64_t seq, void* data);
        int get(int64_t& seq, void*& data, const int64_t timeout_us);
        int update(const int64_t seq);
        bool next_is_ready() const;
        int64_t get_seq();
      protected:
        bool is_inited() const;
        tbsys::CThreadCond* get_cond(const int64_t seq);
      private:
        BufHolder buf_holder_;
        volatile int64_t seq_;
        SeqItem* items_ CACHE_ALIGNED;
        int64_t limit_;
        tbsys::CThreadCond cond_[N_COND] CACHE_ALIGNED;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_SEQ_QUEUE_H__ */
