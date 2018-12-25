#ifndef OB_BLOOM_FILTER_TASK_QUEUE_THREAD_H_
#define OB_BLOOM_FILTER_TASK_QUEUE_THREAD_H_

#include "tbsys.h"
#include "common/ob_range2.h"
#include "common/ob_lighty_queue.h"
#include "ob_ms_rpc_proxy.h"
#include "common/ob_adapter_allocator.h"
#include "common/ob_kv_storecache.h"
#include "ob_insert_cache.h"
namespace oceanbase
{
  namespace mergeserver
  {
    struct ObBloomFilterTask
    {
      uint64_t table_id;
      ObRowkey rowkey;
    };
    class ObBloomFilterTaskQueueThread : public tbsys::CDefaultRunnable
    {
      public:
        ObBloomFilterTaskQueueThread();
        virtual ~ObBloomFilterTaskQueueThread();
        void init(ObMergerRpcProxy *proxy, int queue_capacity = BF_QUEUE_CAPACITY);
        void set_task_limit(const int task_limit);
        int push(ObBloomFilterTask *task);
        void run(tbsys::CThread* thread, void* arg);
        void stop();
      private:
        int handleTask(ObBloomFilterTask *task, void* arg);
      private:
        common::LightyQueue queue_;
        bool stop_;
        mergeserver::ObMergerRpcProxy *proxy_;
        timespec timeout_;
        common::ObStringBuf tablet_location_list_buf_;
        // 128 * 1024
        static const int BF_QUEUE_CAPACITY = 1 << 17;
        static const int TASK_LIMIT_DEFAULT = 5000;
        int task_limit_;
        int wait_time_;
        static const int WAIT_TIME_DEFAULT = 1 * 1000;//1ms
        static const int MAX_WAIT_TIME = 5 * 1000 * 1000;//5 seconds
    };
  }
}
#endif
