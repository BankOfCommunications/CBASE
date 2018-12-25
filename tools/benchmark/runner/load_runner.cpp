#include "load_runner.h"
#include "load_param.h"
#include "load_producer.h"
#include "load_consumer.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadRunner::LoadRunner()
{
  producer_count_ = 0;
  consumer_count_ = 0;
}

LoadRunner::~LoadRunner()
{
  stop();
}

void LoadRunner::stop()
{
  producer_.stop();
  producer_count_ = 0;
  consumer_.stop();
  consumer_count_ = 0;
  //watcher_.stop();
}

void LoadRunner::wait()
{
  consumer_.wait();
  producer_.wait();
  consumer_.end();
  //watcher_.wait();
}

int LoadRunner::start(const MeterParam & param, const bool read_only)
{
  int ret = OB_SUCCESS;
  if (param.ctrl_info_.thread_count_ <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check thread count failed:count[%d]", param.ctrl_info_.thread_count_);
  }
  else
  {
    // stop();
    //filter_.allow(OB_GET_REQUEST);
    //filter_.allow(OB_SCAN_REQUEST);
    // enable read master or not
    filter_.read_master(param.ctrl_info_.enable_read_master_);
    //ret = client_.init();
  }
  //
  if (OB_SUCCESS == ret)
  {
    producer_.init(param.load_source_, &filter_, & manager_, read_only);
    producer_count_ = 1;
    producer_.setThreadCount(producer_count_);
    producer_.start();
    consumer_count_ = param.ctrl_info_.thread_count_;
    manager_.init(500, consumer_count_);

    consumer_.init(param.ctrl_info_,&manager_);
    consumer_.setThreadCount(consumer_count_);
    //sleep(3);
    consumer_.start();

    sleep(1);
    watcher_.init(param.ctrl_info_.compare_result_, param.load_source_.max_query_count_,
        param.ctrl_info_.status_interval_, this, &manager_);
  }
  return ret;
}

