#include "load_watcher.h"
#include "load_runner.h"
#include "load_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadWatcher::LoadWatcher()
{
  compare_ = false;
  max_count_ = -1;
  interval_ = DEFAULT_INTERVAL;
  manager_ = NULL;
  runner_ = NULL;
}

LoadWatcher::~LoadWatcher()
{
}

void LoadWatcher::init(const bool compare, const int64_t max_count,
    const int64_t interval, LoadRunner * runner, const LoadManager * manager)
{
  compare_ = compare;
  max_count_ = max_count;
  interval_ = interval;
  runner_ = runner;
  manager_ = manager;
}

void LoadWatcher::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  if (manager_ != NULL)
  {
    Statics new_data;
    Statics old_data;
    while (!_stop)
    {
      manager_->status(new_data);
      new_data.print(compare_, "TOTAL", stdout);
      (new_data - old_data).print(compare_, "CURR", stdout);
      fprintf(stdout, "\n");
      // stop exit
      if (((max_count_ != -1) && (old_data.new_server_.consume_count_ == max_count_))
          && (true == manager_->get_finish())
          && (new_data.new_server_ == old_data.new_server_))
      {
        TBSYS_LOG(INFO, "consume all");
        if (NULL != runner_)
        {
          usleep(int32_t(interval_));
          runner_->stop();
        }
      }
      old_data = new_data;
      usleep(int32_t(interval_));
    }
  }
  else
  {
    TBSYS_LOG(WARN, "check load failed:load[%p]", manager_);
  }
}

