#include "common/ob_malloc.h"
#include "ob_bloom_filter_task_queue_thread.h"
#include "ob_merge_server_main.h"
#include "common/bloom_filter.h"
#include "common/ob_adapter_allocator.h"

using namespace oceanbase;
using namespace oceanbase::mergeserver;

ObBloomFilterTaskQueueThread::ObBloomFilterTaskQueueThread()
  :stop_(false), proxy_(NULL), task_limit_(TASK_LIMIT_DEFAULT), wait_time_(WAIT_TIME_DEFAULT)
{
}

void ObBloomFilterTaskQueueThread::init(ObMergerRpcProxy *proxy, int queue_capacity)
{
  timeout_.tv_sec = 2;
  timeout_.tv_nsec = 0;
  proxy_ = proxy;
  queue_.init(queue_capacity, NULL);
  setThreadCount(1);
}
ObBloomFilterTaskQueueThread::~ObBloomFilterTaskQueueThread()
{
  queue_.destroy();
}
void ObBloomFilterTaskQueueThread::set_task_limit(const int task_limit)
{
  task_limit_ = task_limit;
}
int ObBloomFilterTaskQueueThread::handleTask(ObBloomFilterTask *task, void* arg)
{
  UNUSED(arg);
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  ObString bf_buffer;
  ObTabletLocationList loc_list;
  tablet_location_list_buf_.reuse();
  loc_list.set_buffer(tablet_location_list_buf_);
  ObMergeServer & merge_server = ObMergeServerMain::get_instance()->get_merge_server();
  ObInsertCache & insert_cache = merge_server.get_insert_cache();
  const ObMergeServerService & merge_server_service = merge_server.get_service();
  ObTabletLocationCacheProxy * location_cache = merge_server.get_cache_proxy();
  if (NULL == task || NULL == location_cache)
  {
    TBSYS_LOG(ERROR, "task is NULL or ObMergeServer get_cache_proxy return NULL, fatal error."
        " task: %p  location_cache: %p", task, arg);
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (err = location_cache->get_tablet_location(
            task->table_id, task->rowkey, loc_list)))
  {
    TBSYS_LOG(ERROR, "get tablet location failed, err: %d  table_id: %lu  rowkey: %s",
        err, task->table_id, to_cstring(task->rowkey));
  }
  else if (loc_list.size() <= 0)
  {
    TBSYS_LOG(WARN, "tablet location size is %ld, no chunkserver could be selected, "
        "table_id: %lu  rowkey: %s", loc_list.size(), task->table_id, to_cstring(task->rowkey));
  }
  else
  {
    ObNewRange range = loc_list.get_tablet_range();
    ObServer target_cs = loc_list[rand() % loc_list.size()].server_.chunkserver_;
    int tablet_version = merge_server_service.get_frozen_version().major_;
    int bloom_filter_version = 1;
    InsertCacheKey insert_cache_key;
    InsertCacheWrapValue insert_cache_value;
    insert_cache_key.range_ = range;
    insert_cache_key.tablet_version_ = tablet_version;

    if (OB_SUCCESS == (ret = insert_cache.get(insert_cache_key, insert_cache_value)))
    {
      TBSYS_LOG(DEBUG, "bloomfilter already exists");
      int err = OB_SUCCESS;
      if (OB_SUCCESS == (err = insert_cache.revert(insert_cache_value)))
      {
        insert_cache_value.reset();
      }
      else
      {
        TBSYS_LOG(WARN, "revert bloom filter failed, err=%d", err);
      }
    }
    else
    {
      ret = proxy_->get_bloom_filter(target_cs, range, tablet_version, bloom_filter_version, bf_buffer);
      if (OB_SUCCESS != ret && OB_CS_EMPTY_TABLET != ret)
      {
        TBSYS_LOG(WARN, "get bloom filter failed, target_cs=%s, range=%s, "
            "tablet_version=%d, bloom_filter_version=%d, ret=%d",
            to_cstring(target_cs), to_cstring(range),
            tablet_version, bloom_filter_version, ret);
      }
      else if (OB_CS_EMPTY_TABLET == ret)
      {
        // create a fake bloomfilter and put
        BloomFilter *bf = create_bloom_filter<AdapterAllocator>(bloom_filter_version);
        if (NULL != bf)
        {
          ObBloomFilterAdapterV1 *fake_bf = dynamic_cast<ObBloomFilterAdapterV1*>(bf);
          char buf[1];
          fake_bf->get_allocator()->init(buf);
          fake_bf->init(1L, 1L);
          if (OB_SUCCESS != (ret = insert_cache.put(insert_cache_key, *fake_bf)))
          {
            TBSYS_LOG(ERROR, "put empty bloom filter failed, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "put empty bloom filter success, tablet: %s tablet_version: %d",
                to_cstring(range), tablet_version);
          }
          int destroy_ret = OB_SUCCESS;
          if (OB_SUCCESS != (destroy_ret = destroy_bloom_filter(bf)))
          {
            TBSYS_LOG(WARN, "destroy bloom filter failed, ret=%d", destroy_ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "destroy bloom filter success");
          }
        }
      }
      else
      {
        char *buf = reinterpret_cast<char*>(ob_tc_malloc(bf_buffer.length(), ObModIds::OB_BLOOM_FILTER));
        if (buf == NULL)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "allocate bloom filter failed,ret=%d", ret);
        }
        else
        {
          BloomFilter *bf = create_bloom_filter<AdapterAllocator>(bloom_filter_version);
          if (NULL != bf)
          {
            ObBloomFilterAdapterV1 *bf_v2 = dynamic_cast<ObBloomFilterAdapterV1*>(bf);
            int64_t pos = 0;
            bf_v2->get_allocator()->init(buf);
            if (OB_SUCCESS != (ret = bf_v2->deserialize(bf_buffer.ptr(), bf_buffer.length(), pos)))
            {
              TBSYS_LOG(WARN, "deserialize bloom filter failed ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = insert_cache.put(insert_cache_key, *bf_v2)))
            {
              TBSYS_LOG(WARN, "put to insert cache failed, ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "update insert cache success, tablet: %s tablet_version: %d",
                  to_cstring(range), tablet_version);
            }
            int destroy_ret = OB_SUCCESS;
            if (OB_SUCCESS != (destroy_ret = destroy_bloom_filter(bf)))
            {
              TBSYS_LOG(WARN, "destroy bloom filter failed, ret=%d", destroy_ret);
            }
            else
            {
              TBSYS_LOG(DEBUG, "destroy bloom filter success");
            }
          }
          ob_tc_free(buf);
        }
        ob_tc_free(bf_buffer.ptr(), ObModIds::OB_BLOOM_FILTER);
      }
    }
  }
  task->~ObBloomFilterTask();
  ob_tc_free(reinterpret_cast<void*>(task), ObModIds::OB_MS_UPDATE_BLOOM_FILTER);
  return ret;
}
int ObBloomFilterTaskQueueThread::push(ObBloomFilterTask *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "task is null");
  }
  else
  {
    if (queue_.size() < task_limit_)
    {
      queue_.push(task, NULL, false);
    }
    else
    {
      ret = OB_TOO_MANY_BLOOM_FILTER_TASK;
      TBSYS_LOG(DEBUG, "too many task, task_limit_=%d, ret=%d", task_limit_, ret);
    }
  }
  return ret;
}
void ObBloomFilterTaskQueueThread::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  void *task = NULL;
  ObBloomFilterTask *bf_task = NULL;
  int ret = OB_SUCCESS;
  int sleep_s = 0;
  int sleep_us = 0;
  while(!stop_)
  {
    sleep_s = wait_time_ / (1000 * 1000);
    sleep_us = wait_time_ % (1000 * 1000);
    if (sleep_s != 0)
    {
      sleep(sleep_s);
    }
    if (sleep_us != 0)
    {
      usleep(static_cast<int>(sleep_us));
    }
    if (OB_SUCCESS != queue_.pop(task, &timeout_))
    {
      continue;
    }
    bf_task = reinterpret_cast<ObBloomFilterTask*>(task);
    ret = handleTask(bf_task, NULL);
    if (OB_SUCCESS != ret && OB_CS_EMPTY_TABLET != ret)
    {
      wait_time_ = wait_time_ * 2;
      if (wait_time_ > MAX_WAIT_TIME)
      {
        wait_time_ = MAX_WAIT_TIME;
      }
    }
    else
    {
      wait_time_ = WAIT_TIME_DEFAULT;
    }
  }
}
void ObBloomFilterTaskQueueThread::stop()
{
  stop_ = true;
}
