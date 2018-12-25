#ifndef OB_COMMON_TABLET_LOCATION_PROXY_H_
#define OB_COMMON_TABLET_LOCATION_PROXY_H_

#include "tbsys.h"
#include "common/ob_chunk_server_item.h"
#include "common/ob_scan_param.h"
#include "sql/ob_sql_scan_param.h"
#include "common/ob_array_lock.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_general_rpc_proxy.h"
#include "ob_tablet_location_cache.h"
#include "ob_tablet_location_cache_proxy.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    class ObSqlScanParam;
  }
  namespace common
  {
    class ObString;
    class ScanFlag;
  }

  namespace common
  {
    class ObGeneralRootRpcProxy;
    class ObTabletLocationItem;
    class ObTabletLocationList;
    class ObTabletLocationCache;
    class ObTabletLocationCacheProxy
    {
    public:
      ObTabletLocationCacheProxy();

      ObTabletLocationCacheProxy(const common::ObServer & server,
        ObGeneralRootRpcProxy * rpc, ObTabletLocationCache * cache);
      virtual ~ObTabletLocationCacheProxy();

    public:
      // init lock counter
      int init(const uint64_t id_lock_count, const uint64_t seq_lock_count);

      // get server for localhost query statistic
      const common::ObServer & get_server(void) const
      {
        return server_;
      }

      // get tablet location according the table_id and row_key [include mode]
      // at first search the cache, then ask the root server, at last update the cache
      // param  @table_id table id of tablet
      //        @row_key row key included in tablet range
      //        @location tablet location
      int get_tablet_location(const uint64_t table_id, const common::ObRowkey& row_key,
        ObTabletLocationList & location);

      // get the first tablet location according range's first table_id.row_key [range.mode]
      // param  @range scan range
      //        @location the first range tablet location
      template <typename T>
      int get_tablet_location(const T & scan_param,
          ObTabletLocationList & list)
      {
        int ret = OB_SUCCESS;
        if (NULL == scan_param.get_range())
        {
          TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", scan_param.get_range());
          ret = OB_INPUT_PARAM_ERROR;
        }

        if (OB_SUCCESS == ret &&
            OB_SUCCESS != ( ret = get_tablet_location(scan_param.get_scan_direction(), scan_param.get_range(), list)))
        {
          TBSYS_LOG(WARN, "get tablet location fail");
        }

        return ret;
      }

      int get_tablet_location(const common::ScanFlag::Direction scan_direction,
        const common::ObNewRange * range, ObTabletLocationList & location);

    public:
      // del cache item according to tablet range
      template <typename T>
      int del_cache_item(const T & scan_param)
      {
        int ret = OB_SUCCESS;
        const ObNewRange * range = scan_param.get_range();
        if (NULL == range)
        {
          TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
          ret = OB_INPUT_PARAM_ERROR;
        }
        else
        {
          ObRowkey search_key;
          ObRowKeyContainer objs;
          ret = get_search_key(scan_param, objs, search_key);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get search key failed:ret[%d], range[%s]", ret, to_cstring(*range));
          }
          else
          {
            ret = del_cache_item(range->table_id_, search_key);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(DEBUG, "del cache item failed:table_id[%lu], ret[%d], search key[%s]",
                  range->table_id_, ret, to_cstring(search_key));
            }
          }
          reset_search_key(search_key);
        }
        return ret;
      }

      // del cache item according to table id + search rowkey
      int del_cache_item(const uint64_t table_id, const common::ObRowkey& search_key);

      // update cache item according to tablet range
      template <typename T>
      int update_cache_item(const T & scan_param,
        const ObTabletLocationList & list)
      {
        int ret = OB_SUCCESS;
        const ObNewRange * range = scan_param.get_range();
        if (NULL == range)
        {
          TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
          ret = OB_INPUT_PARAM_ERROR;
        }
        else
        {
          ObRowkey search_key;
          ObRowKeyContainer objs;
          ret = get_search_key(scan_param, objs, search_key);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get search key failed:ret[%d], range[%s]", ret, to_cstring(*range));
          }
          else
          {
            ret = update_cache_item(range->table_id_, search_key, list);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", range->table_id_, ret);
            }
          }
          reset_search_key(search_key);
        }
        return ret;
      }

      // update cache item according to table id + search rowkey
      int update_cache_item(const uint64_t table_id, const common::ObRowkey& search_key,
        const ObTabletLocationList & list);

      // set item addr invalid according to tablet range
      template <typename T>
      int set_item_invalid(const T & scan_param,
        const ObTabletLocationItem & addr, ObTabletLocationList & list)
      {
        int ret = OB_SUCCESS;
        const ObNewRange * range = scan_param.get_range();
        if (NULL == range)
        {
          TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
          ret = OB_INPUT_PARAM_ERROR;
        }
        else
        {
          ObRowkey search_key;
          ObRowKeyContainer objs;
          int ret = get_search_key(scan_param, objs, search_key);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get search key failed:ret[%d]", ret);
            range->hex_dump();
          }
          else
          {
            ret = set_item_invalid(range->table_id_, search_key, addr, list);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(DEBUG, "set item invalid failed:table_id[%lu], ret[%d]", range->table_id_, ret);
            }
          }
          // reset search key buffer
          reset_search_key(search_key);
        }
        return ret;
      }

      // set item addr invalid according to table id + search rowkey
      int set_item_invalid(const uint64_t table_id, const common::ObRowkey& search_key,
        const ObTabletLocationItem & addr, ObTabletLocationList & list);

      template <typename T>
      int server_fail(const T & scan_param,
        ObTabletLocationList & list, const oceanbase::common::ObServer & server)
      {
        int ret = get_tablet_location(scan_param, list);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,"fail to get tablet location cache [ret:%d]", ret);
        }
        else
        {
          for (int64_t i = 0; i < list.size(); i ++)
          {
            if (list[i].server_.chunkserver_ == server)
            {
              list[i].err_times_ ++;
              if (OB_SUCCESS != (ret = update_cache_item(scan_param,list)))
              {
                TBSYS_LOG(WARN,"fail to update cache item [ret:%d]", ret);
              }
              break;
            }
          }
        }
        return ret;
      }

      int server_fail(const uint64_t table_id, const common::ObRowkey & search_key,
        ObTabletLocationList & list, const oceanbase::common::ObServer & server);

      template <typename T>
      int renew_location_item(const T & scan_param, ObTabletLocationList & list, bool force_renew = false)
      {
        int ret = OB_SUCCESS;
        const ObNewRange * range = scan_param.get_range();
        if (NULL == range)
        {
          TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
          ret = OB_INPUT_PARAM_ERROR;
        }
        else
        {
          ObRowkey search_key;
          ObRowKeyContainer objs;
          int ret = get_search_key(scan_param, objs, search_key);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get search key failed:ret[%d]", ret);
            range->hex_dump();
          }
          else
          {
            ret = renew_location_item(range->table_id_, search_key, list, force_renew);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(DEBUG, "set item invalid failed:table_id[%lu], ret[%d]", range->table_id_, ret);
            }
          }
          // reset search key buffer
          reset_search_key(search_key);
        }
        return ret;
      }
      // get and fetch new item if root server is ok and insert the items
      int renew_location_item(const uint64_t table_id, const common::ObRowkey & row_key,
        ObTabletLocationList & location, bool force_renew=false);

    private:
      // search temp key obj array
      typedef struct ObObjArray
      {
        common::ObObj obj_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER + 1];
      } ObRowKeyContainer;

      // check inner stat
      bool check_inner_stat(void) const;

      // collect hit cache ratio monitor statistic data
      void inc_cache_monitor(const uint64_t table_id, const bool hit_cache);

      // get search cache key
      // warning: the seach_key buffer is allocated if the search key != range.start_key_,
      // after use must dealloc search_key.ptr() in user space if temp is not null
      template <typename T>
      int get_search_key(const T & scan_param, ObRowKeyContainer& objs, common::ObRowkey& search_key)
      {
        return get_search_key(scan_param.get_scan_direction(), scan_param.get_range(), objs, search_key);
      }

      int get_search_key(const common::ScanFlag::Direction scan_direction,
          const common::ObNewRange * range, ObRowKeyContainer& temp_objs, common::ObRowkey& search_key);

      // resest the search key's buffer to null after using it
      void reset_search_key(common::ObRowkey& search_key);

      // update timetout cache item if root server is ok and can refresh the items
      int update_timeout_item(const uint64_t table_id, const common::ObRowkey & row_key,
        ObTabletLocationList & location);

      /// acquire lock for access root table from root server
      tbsys::CThreadMutex * acquire_lock(const uint64_t table_id);
      /// max len
      static const int64_t MAX_ROWKEY_LEN = 8;

    private:
      common::ObServer server_;               // merge server addr for sort
      ObGeneralRootRpcProxy * root_rpc_;             // root server rpc proxy
      ObTabletLocationCache * tablet_cache_;  // merge server tablet location cache
      // the lock for root server access
      ObArrayLock inner_cache_lock_;                // inner id cache lock for access inner table
      ObSequenceLock user_cache_lock_;                 // sequence lock for access root server
    };

    inline bool ObTabletLocationCacheProxy::check_inner_stat(void) const
    {
      return((tablet_cache_ != NULL) && (root_rpc_ != NULL));
    }
  }
}



#endif // OB_COMMON_TABLET_LOCATION_PROXY_H_
