#include "tbsys.h"
#include "ob_define.h"
#include "ob_access_counter.h"

using namespace oceanbase::common;

ObAccessCounter::ObAccessCounter(const int64_t timeout)
{
  memset(this, 0, sizeof(ObAccessCounter));
  timeout_ = timeout;
}

ObAccessCounter::~ObAccessCounter()
{
}

int64_t ObAccessCounter::find(const int64_t server_id, int64_t & old_server)
{
  int64_t ret = -1;
  int64_t index = 0;
  int64_t least_timestamp = INT64_MAX;
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    if (data_[i].server_id_ == server_id)
    {
      ret = i;
      break;
    }
    else if ((cur_count_ >= MAX_COUNT) && (data_[i].timestamp_ < least_timestamp))
    {
      least_timestamp = data_[i].timestamp_;
      index = i;
    }
  }
  // not find same server or empty pos so using lru washout
  if (ret == -1)
  {
    if (cur_count_ < MAX_COUNT)
    {
      // insert a new item 
      ret = cur_count_++;
    }
    else
    {
      // washout a old item 
      ret = index;
      old_server = data_[ret].server_id_;
    }
    data_[ret].counter_ = 0;
    data_[ret].server_id_ = server_id;
  }
  return ret;
}

int ObAccessCounter::inc(const int64_t server_id)
{
  int ret = OB_SUCCESS;
  int64_t old_server = -1;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  clear(timestamp);
  int64_t index = find(server_id, old_server);
  if (index < 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "find peer id failed:server[%ld], index[%ld]", server_id, index);
  }
  else
  {
    ++data_[index].counter_;
    data_[index].timestamp_ = timestamp;
  }
  return ret;
}

void ObAccessCounter::clear(const int64_t timestamp)
{
  if ((timeout_ > 0) && (clear_timestamp_ + timeout_ < timestamp))
  {
    cur_count_ = 0;
    clear_timestamp_ = timestamp;
    memset(data_, 0, MAX_COUNT * sizeof(ObAccessInfo));
  }
}

void ObAccessCounter::print(void)
{
  clear(tbsys::CTimeUtil::getTime());
  TBSYS_LOG(DEBUG, "print all counter:count[%ld]", cur_count_);
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    TBSYS_LOG(INFO, "access counter info:server[%ld], counter[%ld]",
        data_[i].server_id_, data_[i].counter_);
  }
}


