////===================================================================
 //
 // ob_balance_filter.cpp common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2013-06-19 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "hash/ob_hashutils.h"
#include "ob_balance_filter.h"

namespace oceanbase
{
  namespace common
  {
    ObBalanceFilter::ObBalanceFilter() : inited_(false),
                                         bucket_node_num_(0),
                                         thread_node_num_(0),
                                         bucket_nodes_(NULL),
                                         thread_nodes_(NULL),
                                         bucket_round_robin_(0)
    {
    }

    ObBalanceFilter::~ObBalanceFilter()
    {
      destroy();
    }

    int ObBalanceFilter::init(const int64_t thread_num, const bool dynamic_rebalance)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= thread_num)
      {
        TBSYS_LOG(WARN, "invalid param, thread_num=%ld",  thread_num);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        bucket_node_num_ = hash::cal_next_prime(thread_num * AMPLIFICATION_FACTOR);
        thread_node_num_ = thread_num;
        if (NULL == (bucket_nodes_ = (BucketNode*)ob_malloc(sizeof(BucketNode) * bucket_node_num_, ObModIds::BALANCE_FILTER)))
        {
          TBSYS_LOG(WARN, "alloc bucket nodes fail");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (thread_nodes_ = (ThreadNode*)ob_malloc(sizeof(ThreadNode) * MAX_THREAD_NUM, ObModIds::BALANCE_FILTER)))
        {
          TBSYS_LOG(WARN, "alloc thread nodes fail");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int64_t i = 0; i < bucket_node_num_; i++)
          {
            bucket_nodes_[i].thread_pos = -1;
            bucket_nodes_[i].cnt = 0;
          }
          for (int64_t i = 0; i < thread_node_num_; i++)
          {
            thread_nodes_[i].cnt = 0;
          }

          if (dynamic_rebalance
              && 1 != start())
          {
            TBSYS_LOG(WARN, "start thread to rebalance fail");
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            bucket_round_robin_ = 0;
            inited_ = true;
            TBSYS_LOG(INFO, "init succ, bucket_node_num=%ld bucket_nodes=%p thread_node_num=%ld thread_nodes=%p dynamic_rebalance=%s",
                      bucket_node_num_, bucket_nodes_, thread_node_num_, thread_nodes_, STR_BOOL(dynamic_rebalance));
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObBalanceFilter::destroy()
    {
      stop();
      wait();
      inited_ = false;
      bucket_round_robin_ = 0;
      if (NULL != thread_nodes_)
      {
        ob_free(thread_nodes_);
        thread_nodes_ = NULL;
      }
      if (NULL != bucket_nodes_)
      {
        ob_free(bucket_nodes_);
        bucket_nodes_ = NULL;
      }
      thread_node_num_ = 0;
      bucket_node_num_ = 0;
    }

    void ObBalanceFilter::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      while (!_stop)
      {
        int64_t max_thread_pos = 0;
        int64_t min_thread_pos = 0;
        for (int64_t i = 0; i < thread_node_num_; i++)
        {
          if (thread_nodes_[max_thread_pos].cnt < thread_nodes_[i].cnt)
          {
            max_thread_pos = i;
          }
          if (thread_nodes_[min_thread_pos].cnt > thread_nodes_[i].cnt)
          {
            min_thread_pos = i;
          }
        }
        if (0 < thread_nodes_[max_thread_pos].cnt
            && thread_nodes_[max_thread_pos].cnt > (thread_nodes_[min_thread_pos].cnt * 2))
        {
          int64_t bucket_cnt = 0;
          int64_t min_bucket_pos = 0;
          for (int64_t i = 0; i < bucket_node_num_; i++)
          {
            if (max_thread_pos == bucket_nodes_[i].thread_pos
                && 0 < bucket_nodes_[i].cnt)
            {
              bucket_cnt += 1;
              if (0 == bucket_nodes_[min_bucket_pos].cnt)
              {
                min_bucket_pos = i;
              }
              if (bucket_nodes_[min_bucket_pos].cnt > bucket_nodes_[i].cnt)
              {
                min_bucket_pos = i;
              }
            }
          }
          if (1 < bucket_cnt
              && 0 <= bucket_nodes_[min_bucket_pos].thread_pos
              && 0 < bucket_nodes_[min_bucket_pos].cnt)
          {
            migrate(min_bucket_pos, min_thread_pos);
          }
          else
          {
            TBSYS_LOG(INFO, "thread=%ld cnt=%ld has only one bucket=%ld cnt=%ld, need not rebalance",
                      max_thread_pos, thread_nodes_[max_thread_pos].cnt, min_bucket_pos, bucket_nodes_[min_bucket_pos].cnt);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "max thread=%ld cnt=%ld, min thread=%ld cnt=%ld, need not rebalance",
                    max_thread_pos, thread_nodes_[max_thread_pos].cnt, min_thread_pos, thread_nodes_[min_thread_pos].cnt);
        }
        for (int64_t i = 0; i < thread_node_num_; i++)
        {
          thread_nodes_[i].cnt = 0;
        }
        for (int64_t i = 0; i < bucket_node_num_; i++)
        {
          bucket_nodes_[i].cnt = 0;
        }
        usleep(REBALANCE_INTERVAL);
      }
    }

    uint64_t ObBalanceFilter::filt(const uint64_t input)
    {
      uint64_t ret = 0;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else
      {
        uint64_t bucket_pos = input % bucket_node_num_;
        BucketNode &bucket_node = bucket_nodes_[bucket_pos];
        while (true)
        {
          if (0 <= bucket_node.thread_pos)
          {
            break;
          }
          if (-1 == ATOMIC_CAS(&(bucket_node.thread_pos), -1, -2))
          {
            int64_t thread_pos = (ATOMIC_ADD(&bucket_round_robin_, 1) - 1) % thread_node_num_;
            bucket_node.thread_pos = thread_pos;
            break;
          }
        }
        ret = bucket_node.thread_pos;
        ATOMIC_ADD(&bucket_node.cnt, 1);
        ATOMIC_ADD(&thread_nodes_[ret].cnt, 1);
      }
      return ret;
    }

    void ObBalanceFilter::migrate(const int64_t bucket_pos, const int64_t thread_pos)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else if (0 > bucket_pos
              || bucket_node_num_ <= bucket_pos
              || 0 > thread_pos
              || thread_node_num_ <= thread_pos)
      {
        TBSYS_LOG(WARN, "invalid param, bucket_pos=%ld thread_pos=%ld", bucket_pos, thread_pos);
      }
      else
      {
        TBSYS_LOG(INFO, "migrate bucket_pos=%ld bucket_cnt=%ld thread_pos from %ld:%ld to %ld:%ld",
                  bucket_pos, bucket_nodes_[bucket_pos].cnt,
                  bucket_nodes_[bucket_pos].thread_pos, thread_nodes_[bucket_nodes_[bucket_pos].thread_pos].cnt,
                  thread_pos, thread_nodes_[thread_pos].cnt);
        bucket_nodes_[bucket_pos].thread_pos = thread_pos;
      }
    }

    void ObBalanceFilter::log_info()
    {
      TBSYS_LOG(INFO, "==========balance filter info start==========");
      for (int64_t i = 0; i < bucket_node_num_; i++)
      {
        TBSYS_LOG(INFO, "[balance_filter][bucket] bucket_pos=%ld thread_pos=%ld cnt=%ld",
                  i, bucket_nodes_[i].thread_pos, bucket_nodes_[i].cnt);
      }
      for (int64_t i = 0; i < thread_node_num_; i++)
      {
        TBSYS_LOG(INFO, "[balance_filter][thread] thread_pos=%ld cnt=%ld",
                  i, thread_nodes_[i].cnt);
      }
      TBSYS_LOG(INFO, "==========balance filter info end==========");
    }

    void ObBalanceFilter::add_thread(const int64_t add_num)
    {
      for (int64_t n = 0; n < add_num; n++)
      {
        thread_nodes_[thread_node_num_ + n].cnt = 0;
      }
      ATOMIC_ADD(&thread_node_num_, add_num);
    }

    void ObBalanceFilter::sub_thread(const int64_t sub_num)
    {
      for (int64_t n = 0; n < sub_num; n++)
      {
        const int64_t last_pos = ATOMIC_ADD(&thread_node_num_, -1);
        for (int64_t i = 0; i < bucket_node_num_; i++)
        {
          if (last_pos == bucket_nodes_[i].thread_pos)
          {
            bucket_nodes_[i].thread_pos = -1;
          }
        }
      }
    }
  }
}
