#include "ob_ms_server_counter.h"
#include "common/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::mergeserver;


ObMergerServerCounter::ObMergerServerCounter()
{
  int err = init(INIT_SERVER_COUNT, DEFAULT_INTERVAL);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "init bucket count failed:ret[%d]", err);
  }
  last_reset_timestamp_ = 0;
}

ObMergerServerCounter::~ObMergerServerCounter()
{
}

int ObMergerServerCounter::init(const int64_t count, const int64_t interval)
{
  int ret = OB_SUCCESS;
  if (count <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check input param failed:count[%ld]", count);
  }
  else
  {
    ret = counter_map_.create(count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "hashmap create failed:ret[%d]", ret);
    }
    else
    {
      reset_interval_ = interval;
    }
  }
  return ret;
}

int ObMergerServerCounter::reset(void)
{
  return counter_map_.clear();
}

void ObMergerServerCounter::dump(void) const
{
  int64_t count = 0;
  int32_t ip = 0;
  int32_t port = 2600;
  ObServer server;
  ObHashMap<int32_t, int64_t>::const_iterator iter = counter_map_.begin();
  for (;iter != counter_map_.end(); ++iter)
  {
    ip = iter->first;
    count = iter->second;
    server.set_ipv4_addr(ip, port);
    TBSYS_LOG(TRACE, "dump server query counter:server[%s], count[%ld]", server.to_cstring(), count);
  }
}

int ObMergerServerCounter::inc(const ObServer & server, const int64_t count)
{
  // first reset the old values
  check_reset();
  int64_t server_count = 0;
  int ret = counter_map_.get(server.get_ipv4(), server_count);
  if (HASH_EXIST == ret)
  {
    server_count += count;
  }
  else if (HASH_NOT_EXIST == ret)
  {
    server_count = 0;
  }
  else
  {
    TBSYS_LOG(WARN, "get server count failed:server[%s], ret[%d]", server.to_cstring(), ret);
  }
  // no matter succ or fail
  ret = counter_map_.set(server.get_ipv4(), server_count, 1);
  if ((ret != HASH_OVERWRITE_SUCC) && (ret != HASH_INSERT_SUCC))
  {
    TBSYS_LOG(WARN, "overwrite or insert failed:server[%s], ret[%d]", server.to_cstring(), ret);
  }
  else
  {
    ret = OB_SUCCESS;
    TBSYS_LOG(DEBUG, "update server count succ:server[%s], count[%ld]",
        server.to_cstring(), server_count);
  }
  return ret;
}
ObHashMap<int32_t, int64_t> & ObMergerServerCounter::get_counter_map()
{
    return counter_map_;
}
int64_t ObMergerServerCounter::get(const common::ObServer & server) const
{
  int64_t count = 0;
  int ret = counter_map_.get(server.get_ipv4(), count);
  if ((HASH_EXIST != ret) && (HASH_NOT_EXIST != ret))
  {
    TBSYS_LOG(WARN, "get server count failed:server[%s], ret[%d]", server.to_cstring(), ret);
  }
  else
  {
    ret = OB_SUCCESS;
    TBSYS_LOG(DEBUG, "get server count succ:server[%s], count[%ld]", server.to_cstring(), count);
  }
  return count;
}

void ObMergerServerCounter::check_reset(void)
{
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  if ((reset_interval_ > 0) && (timestamp - last_reset_timestamp_> reset_interval_))
  {
    dump();
    int ret = reset();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "reset counter failed:last[%ld], interval[%ld], current[%ld], ret[%d]",
          last_reset_timestamp_, reset_interval_, timestamp, ret);
    }
    last_reset_timestamp_ = timestamp;
  }
}

