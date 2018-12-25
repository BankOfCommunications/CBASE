#include "common/ob_scan_param.h"
#include "common/ob_string.h"
#include "ob_general_rpc_proxy.h"
#include "ob_tablet_location_cache.h"
#include "ob_tablet_location_cache_proxy.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::common;
using namespace oceanbase::common;

ObTabletLocationCacheProxy::ObTabletLocationCacheProxy()
{
  root_rpc_ = NULL;
  tablet_cache_ = NULL;
}

ObTabletLocationCacheProxy::ObTabletLocationCacheProxy(const ObServer & server,
  ObGeneralRootRpcProxy * rpc, ObTabletLocationCache * cache)
{
  // set max rowkey as 0xFF
  server_ = server;
  root_rpc_ = rpc;
  tablet_cache_ = cache;
}

ObTabletLocationCacheProxy::~ObTabletLocationCacheProxy()
{
}

int ObTabletLocationCacheProxy::init(const uint64_t id_lock_count, const uint64_t seq_lock_count)
{
  int ret = inner_cache_lock_.init(id_lock_count);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "init array lock failed:count[%lu]", id_lock_count);
  }
  else
  {
    ret = user_cache_lock_.init(seq_lock_count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "init sequence lock failed:count[%lu]", seq_lock_count);
    }
  }
  return ret;
}

tbsys::CThreadMutex * ObTabletLocationCacheProxy::acquire_lock(const uint64_t table_id)
{
  tbsys::CThreadMutex * ret = NULL;
  if (table_id < inner_cache_lock_.size())
  {
    ret = inner_cache_lock_.acquire_lock(table_id);
  }
  else
  {
    ret = user_cache_lock_.acquire_lock();
  }
  return ret;
}

int ObTabletLocationCacheProxy::del_cache_item(const uint64_t table_id, const ObRowkey & search_key)
{
  int ret = OB_SUCCESS;
  if ((NULL == search_key.ptr()) || (0 == search_key.length()))
  {
    TBSYS_LOG(WARN, "check search key failed:table[%lu], ptr[%p]", table_id, search_key.ptr());
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = tablet_cache_->del(table_id, search_key);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(DEBUG, "del cache item failed:table_id[%lu], ret[%d], search key[%s]",
          table_id, ret, to_cstring(search_key));
    }
    else
    {
      TBSYS_LOG(DEBUG, "del cache item succ:table_id[%lu]", table_id);
    }
  }
  return ret;
}



int ObTabletLocationCacheProxy::update_cache_item(const uint64_t table_id, const ObRowkey & rowkey,
  const ObTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  if ((NULL == rowkey.ptr()) || (0 == rowkey.length()))
  {
    TBSYS_LOG(WARN, "check rowkey failed:table_id[%lu], rowkey[%p]", table_id, rowkey.ptr());
  }
  else
  {
    ret = tablet_cache_->update(table_id, rowkey, list);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", table_id, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "update cache item succ:table_id[%lu]", table_id);
    }
  }
  return ret;
}



int ObTabletLocationCacheProxy::server_fail(const uint64_t table_id, const common::ObRowkey & search_key,
  ObTabletLocationList & list, const ObServer & server)
{
  int ret = get_tablet_location(table_id, search_key, list);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get tablet location cache:ret[%d]", ret);
  }
  else
  {
    for (int64_t i = 0; i < list.size(); i ++)
    {
      if (list[i].server_.chunkserver_ == server)
      {
        list[i].err_times_ ++;
        if (OB_SUCCESS != (ret = update_cache_item(table_id, search_key, list)))
        {
          TBSYS_LOG(WARN, "fail to update cache item:ret[%d]", ret);
        }
        break;
      }
    }
  }
  return ret;
}

//
int ObTabletLocationCacheProxy::set_item_invalid(const uint64_t table_id, const ObRowkey & rowkey,
  const ObTabletLocationItem & addr, ObTabletLocationList & list)
{
  assert(list.get_buffer() != NULL);
  int ret = tablet_cache_->get(table_id, rowkey, list);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "get cache item failed:table_id[%lu], ret[%d]",
      table_id, ret);
  }
  else
  {
    ret = list.set_item_invalid(addr);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "set item invalid failed:table_id[%lu], ret[%d]",
        table_id, ret);
    }
    else
    {
      ret = tablet_cache_->update(table_id, rowkey, list);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "update cache item failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "set item invalid succ");
      }
    }
  }
  return ret;
}

// set item server invalid

int ObTabletLocationCacheProxy::get_search_key(const common::ScanFlag::Direction scan_direction,
    const common::ObNewRange * range, ObRowKeyContainer & objs, common::ObRowkey& search_key)
{
  int ret = OB_SUCCESS;
  if (NULL == range)
  {
    TBSYS_LOG(WARN, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    if (ScanFlag::FORWARD == scan_direction)
    {
      int64_t length = range->start_key_.length();
      if (range->start_key_.is_min_row())
      {
        // range start key mean nothing when is_min_value
        length = 0;
      }
      for (int64_t i = 0; i < length; ++i)
      {
        objs.obj_array_[i] = range->start_key_.ptr()[i];
      }

      // not include the start row, convert the row_key
      if (!range->border_flag_.inclusive_start() || range->start_key_.is_min_row())
      {
        // add a new min_value obj to the rowkey
        // add the min obj
        objs.obj_array_[length] = ObRowkey::MIN_OBJECT;
        length += 1;
      }
      search_key.assign(objs.obj_array_, length);
    }
    else
    {
      if (range->end_key_.is_max_row())
      {
        // set the max_value obj to the rowkey
        search_key.assign(&ObRowkey::MAX_OBJECT, 1);
      }
      else
      {
        search_key = range->end_key_;
      }
    }
  }
  return ret;
}

// can not use search key after reset buffer
void ObTabletLocationCacheProxy::reset_search_key(ObRowkey & search_key)
{
  UNUSED(search_key);
}

int ObTabletLocationCacheProxy::get_tablet_location(const ScanFlag::Direction scan_direction,
  const common::ObNewRange * range, ObTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  ObRowkey search_key;
  ObRowKeyContainer temp_objs;
  if (NULL == range)
  {
    TBSYS_LOG(WARN, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = get_search_key(scan_direction, range, temp_objs, search_key);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get search key failed:range[%s], ret[%d]", to_cstring(*range), ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "get_search_key sd:%d, range:%s, sk:%s,ek:%s, search_key[%s]",
          scan_direction, to_cstring(*range), to_cstring(range->start_key_),
          to_cstring(range->end_key_), to_cstring(search_key));
      ret = get_tablet_location(range->table_id_, search_key, list);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get tablet location failed:range[%s],  search_key[%s], ret[%d]",
          to_cstring(*range), to_cstring(search_key), ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "get tablet location list succ:table_id[%lu]", range->table_id_);
      }
    }
    reset_search_key(search_key);
  }
  return ret;

}


int ObTabletLocationCacheProxy::renew_location_item(const uint64_t table_id,
  const ObRowkey & row_key, ObTabletLocationList & location, bool force_renew)
{
  // Warning: other get new tablet location thread will locked by one thread
  TBSYS_LOG(DEBUG, "local tablet location cache not exist:table_id[%lu]", table_id);
  // lock and check again
  tbsys::CThreadGuard lock(acquire_lock(table_id));
  TBSYS_LOG(DEBUG, "cache not exist check local cache with lock:table_id[%lu]", table_id);
  int ret = tablet_cache_->get(table_id, row_key, location);
  if (ret != OB_SUCCESS || true == force_renew)
  {
    TBSYS_LOG(DEBUG, "cache not hit, begin scan root table with table_id[%ld],rowkey[%s]",
        table_id, to_cstring(row_key));
    // step 2. scan root server and renew the cache
    int64_t start_time = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, SCAN_ROOT_TABLE_START_TIME, start_time);
    ret = root_rpc_->scan_root_table(tablet_cache_, table_id, row_key, server_, location);
    int64_t end_time = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, SCAN_ROOT_TABLE_END_TIME, end_time);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get tablet location failed:table_id[%lu], ret[%d], rowkey[%s]",
          table_id, ret, to_cstring(row_key));
    }
    else if (0 == location.size())
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check scan root table failed:table_id[%lu], count[%ld]",
        table_id, location.size());
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "cache item is fetched by other thread:table_id[%lu]", table_id);
  }
  return ret;
}

int ObTabletLocationCacheProxy::update_timeout_item(const uint64_t table_id,
  const ObRowkey & row_key, ObTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  // check location cache item timeout
  if (timestamp - location.get_timestamp() > tablet_cache_->get_timeout(table_id))
  {
    // lock and check again
    TBSYS_LOG(DEBUG, "cache timeout get local cache with lock:table_id[%lu]", table_id);
    tbsys::CThreadGuard lock(acquire_lock(table_id));
    ret = tablet_cache_->get(table_id, row_key, location);
    if ((ret != OB_SUCCESS) || (timestamp - location.get_timestamp()
      > tablet_cache_->get_timeout(table_id)))
    {
      int64_t start_time = tbsys::CTimeUtil::getTime();
      PROFILE_LOG(DEBUG, SCAN_ROOT_TABLE_START_TIME, start_time);
      ret = root_rpc_->scan_root_table(tablet_cache_, table_id, row_key, server_, location);
      int64_t end_time = tbsys::CTimeUtil::getTime();
      PROFILE_LOG(DEBUG, SCAN_ROOT_TABLE_END_TIME, end_time);
      // if root server die renew the item cache timestamp for longer alive time
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get tablet location from root failed:table_id[%lu], rowkey[%s], ret[%d]",
          table_id, to_cstring(row_key), ret);
        // update location list timestamp for root server failed
        location.set_timestamp(timestamp);
        ret = tablet_cache_->update(table_id, row_key, location);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "update cache item failed:table_id[%lu], rowkey[%s], ret[%d]",
              table_id, to_cstring(row_key), ret);
        }
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "already update the cache item by other thread:table_id[%lu]", table_id);
    }
  }
  return ret;
}

// get tablet location through rowkey, return the cs includeing this row_key data
int ObTabletLocationCacheProxy::get_tablet_location(const uint64_t table_id,
  const ObRowkey & row_key, ObTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(WARN, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    bool hit_cache = false;
    assert(location.get_buffer() != NULL);
    ret = tablet_cache_->get(table_id, row_key, location);
    if (ret != OB_SUCCESS)
    {
      // scan root table insert new location item to cache
      ret = renew_location_item(table_id, row_key, location);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch tablet location through rpc call failed:table_id[%lu], ret[%d]",
            table_id, ret);
      }
    }
    else
    {
      // check timeout item for lazy washout
      int err = update_timeout_item(table_id, row_key, location);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "update timeout item failed:table_id[%lu], ret[%d]", table_id, err);
      }
      // check invalid location
      if (0 == location.get_valid_count() || 0 == location.size())
      {
        TBSYS_LOG(WARN, "check all the chunk server invalid:size[%ld]", location.size());
        err = del_cache_item(table_id, row_key);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "del invalid cache item failed:table_id[%lu], ret[%d]", table_id, err);
        }
        // scan root table insert new location item to cache
        ret = renew_location_item(table_id, row_key, location);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fetch tablet location through rpc call failed:table_id[%lu], ret[%d]",
              table_id, ret);
        }
      }
      else
      {
        hit_cache = true;
      }
    }
    // inc hit cache monitor
    inc_cache_monitor(table_id, hit_cache);
  }
  return ret;
}

void ObTabletLocationCacheProxy::inc_cache_monitor(const uint64_t table_id, const bool hit_cache)
{
  if (false == hit_cache)
  {
    OB_STAT_TABLE_INC(COMMON, table_id, MISS_CS_CACHE_COUNT);
  }
  else
  {
    OB_STAT_TABLE_INC(COMMON, table_id, HIT_CS_CACHE_COUNT);
  }
}
