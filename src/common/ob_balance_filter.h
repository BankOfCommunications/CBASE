////===================================================================
 //
 // ob_balance_filter.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#ifndef  OCEANBASE_COMMON_BALANCE_FILTER_H_
#define  OCEANBASE_COMMON_BALANCE_FILTER_H_

#include "tbsys.h"
#include "ob_define.h"
#include "hash/ob_hashutils.h"

#define CPU_CACHE_LINE 64

namespace oceanbase
{
  namespace common 
  {
    class ObBalanceFilter : public tbsys::CDefaultRunnable
    {
      struct BucketNode
      {
        volatile int64_t thread_pos;
        volatile int64_t cnt;
      };
      struct ThreadNode
      {
        volatile int64_t cnt;
      };
      static const int64_t AMPLIFICATION_FACTOR = 100;
      static const int64_t MAX_THREAD_NUM = 4096;
      static const int64_t REBALANCE_INTERVAL = 3L * 1000000L;
      public:
        ObBalanceFilter();
        ~ObBalanceFilter();
      public:
        int init(const int64_t thread_num, const bool dynamic_rebalance);
        void destroy();
        virtual void run(tbsys::CThread* thread, void* arg);
      public:
        uint64_t filt(const uint64_t input);
        void migrate(const int64_t bucket_pos, const int64_t thread_pos);
        void log_info();
        void add_thread(const int64_t add_num);
        void sub_thread(const int64_t sub_num);
      private:
        bool inited_;
        int64_t bucket_node_num_;
        volatile int64_t thread_node_num_;
        BucketNode *bucket_nodes_;
        ThreadNode *thread_nodes_;
        uint64_t bucket_round_robin_;
    };
  }
}

#endif //OCEANBASE_COMMON_BALANCE_FILTER_H_

