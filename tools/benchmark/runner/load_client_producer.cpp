#include "load_manager.h"
#include "load_util.h"
#include "load_filter.h"
#include "load_client_producer.h"
#include "load_manager.h"
#include "load_rpc.h"
#include "common/ob_get_param.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadClientProducer::LoadClientProducer()
{
  last_time_ = 0;
  rpc_ = NULL;
  std_time_ = 10000;
}

LoadClientProducer::~LoadClientProducer()
{
}

void LoadClientProducer::init(const SourceParam & param, LoadRpc * rpc, LoadFilter * filter, LoadManager * manager)
{
  LoadProducer::init(param, filter, manager);
  rpc_ = rpc;
  std_time_ = 1000 * 1000L / param_.query_per_second_;
}

void LoadClientProducer::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;
  if (manager_ != NULL)
  {
    int64_t count = 1;
    while (!_stop)
    {
      ret = get_request(param_.stream_server_, count);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "load input load file failed:ret[%d]", ret);
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "load one file request succ:count[%ld]", count);
        if ((param_.max_query_count_> 0) && (param_.max_query_count_ == count))
        {
          break;
        }
      }
    }
    manager_->set_finish();
  }
  else
  {
    TBSYS_LOG(WARN, "check load manager failed:load[%p]", manager_);
  }
}

int LoadClientProducer::get_request(const ObServer & server, const int64_t count)
{
  int ret = OB_SUCCESS;
  QueryInfo query;
  UNUSED(count);
  ret = rpc_->fetch_query(server, TIMEOUT, query);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check get request failed:ret[%d]", ret);
  }
  else if (filter_ != NULL)
  {
    FIFOPacket * header = query.packet_;
    if (false == filter_->check(header->type, header->buf, header->buf_size))
    {
      manager_->filter();
    }
    else
    {
      ret = push_request(query);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "push request failed:ret[%d]", ret);
      }
      last_time_ = tbsys::CTimeUtil::getTime();
    }
  }
  // control get too quick
  while (!_stop && manager_->size() >= param_.max_sleep_count_)
  {
    sleep(1);
  }
  return ret;
}

int LoadClientProducer::push_request(const QueryInfo & query)
{
  int ret = OB_SUCCESS;
  // push to the task queue
  do
  {
    ret = manager_->push(query);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "push new task failed:ret[%d]", ret);
    }
    else
    {
      // load control for produce
      int64_t cur_time = tbsys::CTimeUtil::getTime();
      while (cur_time - last_time_ < std_time_)
      {
        //sleep_ms_time = (std_time - (cur_time - last_time)) / 1000;
        //TBSYS_LOG(ERROR, "std_ms:%ld sleep ms:%ld", std_time, sleep_ms_time);
        //usleep(sleep_ms_time/10);
        cur_time = tbsys::CTimeUtil::getTime();
      }
    }
  } while (ret != OB_SUCCESS && !_stop);
  return ret;
}

