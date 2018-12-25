#include <algorithm>
#include "ob_server_stats.h"

using namespace oceanbase::common;

namespace oceanbase 
{ 
  namespace obsql 
  {

    //---------------------------------------------------------------
    // class Present 
    //---------------------------------------------------------------
    Present::Present()
    {
      init();
    }

    const Present::ServerInfo & Present::get_server_info(const int32_t server_type) const
    {
      assert(server_type < SERVER_COUNT+1);
      return server_info_[server_type-1];
    }

    void Present::init()
    {
      // rootserver
      ServerInfo & root_server_info = server_info_[ObStatManager::SERVER_TYPE_ROOT-1];
      root_server_info.push_back(Item("succ_get_count", 4, PerSecond)); 
      root_server_info.push_back(Item("succ_scan_count", 4, PerSecond));
      root_server_info.push_back(Item("fail_get_count", 4, PerSecond));
      root_server_info.push_back(Item("fail_scan_count", 4, PerSecond));

      // chunkserver
      ServerInfo & chunk_server_info = server_info_[ObStatManager::SERVER_TYPE_CHUNK-1];
      chunk_server_info.push_back(Item("get_count", 4, PerSecond));
      chunk_server_info.push_back(Item( "scan_count", 4, PerSecond));
      chunk_server_info.push_back(Item( "get_time", 8, PerSecond));
      chunk_server_info.push_back(Item( "scan_time", 8, PerSecond));
      chunk_server_info.push_back(Item("block_index_cache_hit", 4, PerSecond));
      chunk_server_info.push_back(Item( "block_index_cache_miss", 4, PerSecond));
      chunk_server_info.push_back(Item( "block_cache_hit", 4, PerSecond));
      chunk_server_info.push_back(Item( "block_cache_miss", 4, PerSecond));
      chunk_server_info.push_back(Item( "get_bytes", 8, PerSecond));
      chunk_server_info.push_back(Item( "scan_bytes", 8, PerSecond));
      chunk_server_info.push_back(Item( "disk_io_num", 4, PerSecond));
      chunk_server_info.push_back(Item( "disk_io_bytes", 8, PerSecond));
      chunk_server_info.push_back(Item( "block_index_cache_hit_ratio", 3, Ratio));
      chunk_server_info.push_back(Item( "block_cache_hit_ratio", 3, Ratio));
      chunk_server_info.push_back(Item( "average_get_time", 8, Ratio));
      chunk_server_info.push_back(Item( "average_scan_time", 8, Ratio));

      // mergeserver
      ServerInfo & merge_server_info = server_info_[ObStatManager::SERVER_TYPE_MERGE - 1];
      merge_server_info.push_back(Item( "succ_get_count", 4, PerSecond));
      merge_server_info.push_back(Item( "succ_get_time", 8, PerSecond));
      merge_server_info.push_back(Item("fail_get_count", 4, PerSecond)); 
      merge_server_info.push_back(Item( "fail_get_time", 8, PerSecond));
      // scan
      merge_server_info.push_back(Item( "succ_scan_count", 4, PerSecond));
      merge_server_info.push_back(Item( "succ_scan_time", 8, PerSecond));
      merge_server_info.push_back(Item( "fail_scan_count", 4, PerSecond));
      merge_server_info.push_back(Item( "fail_scan_time", 8, PerSecond));
      // cache hit
      merge_server_info.push_back(Item( "cs_cache_hit", 4, PerSecond));
      merge_server_info.push_back(Item( "cs_cache_miss", 4, PerSecond));
      // cs version error
      merge_server_info.push_back(Item( "fail_cs_version", 4, PerSecond));
      // local query
      merge_server_info.push_back(Item( "local_cs_query", 4, PerSecond));
      merge_server_info.push_back(Item( "remote_cs_query", 4, PerSecond));
      merge_server_info.push_back(Item( "cs_cache_hit_ratio", 3, Ratio));
      merge_server_info.push_back(Item( "average_succ_get_time", 8, Ratio));
      merge_server_info.push_back(Item( "average_succ_scan_time", 8, Ratio));


      // updateserver
      ServerInfo &update_server_info = server_info_[ObStatManager::SERVER_TYPE_UPDATE-1];
      update_server_info.push_back(Item( "min", 0, PerSecond));
      update_server_info.push_back(Item( "get_count", 4, PerSecond));
      update_server_info.push_back(Item( "scan_count", 4, PerSecond));
      update_server_info.push_back(Item( "apply_count",  4, PerSecond));
      update_server_info.push_back(Item( "batch_count", 4, PerSecond));
      update_server_info.push_back(Item( "merge_count", 4, PerSecond));
      update_server_info.push_back(Item( "get_timeu", 8, PerSecond));
      update_server_info.push_back(Item( "scan_timeu", 8, PerSecond));
      update_server_info.push_back(Item( "apply_timeu",  8, PerSecond));
      update_server_info.push_back(Item( "batch_timeu", 8, PerSecond));
      update_server_info.push_back(Item( "merge_timeu", 8, PerSecond));
      update_server_info.push_back(Item( "memory_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "memory_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "active_memtable_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "active_memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "active_memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "active_total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "frozen_memtable_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "frozen_memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "frozen_memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "frozen_total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "apply_fail_count", 4, PerSecond));
      update_server_info.push_back(Item( "tbsys_drop_count", 4, PerSecond));
      update_server_info.push_back(Item( "max", 0, 1));

      // daily merge
      ServerInfo & daily_merge_info = server_info_[Present::SERVER_COUNT-1];
      daily_merge_info.push_back(Item( "total_tablet_num", 4, ShowCurrent));
      daily_merge_info.push_back(Item( "need_merge_num", 4, ShowCurrent));
      daily_merge_info.push_back(Item( "merged_num", 4, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_default", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_network", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_threadbuffer", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_tablet", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_bi_cache", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_block_cache", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_bi_cache_unserving", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_block_cache_unserving", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_join_cache", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_merge_buffer", 8, ShowCurrent));
      daily_merge_info.push_back(Item( "mu_merge_split_buffer", 8, ShowCurrent));
    }

    //---------------------------------------------------------------
    // class ObServerStats
    //---------------------------------------------------------------

    int32_t ObServerStats::init()
    {
      return 0;
    }

    // retrive data from dataserver.
    int32_t ObServerStats::refresh(const oceanbase::common::ObServer *remote_server) 
    {
      int32_t ret = OB_SUCCESS;
      int32_t server_type = store_.diff.get_server_type();
      
      if (remote_server) 
        ret = rpc_stub_.fetch_stats(*remote_server, store_.current);
      else if (server_type == ObStatManager::SERVER_TYPE_ROOT)
        ret = rpc_stub_.fetch_stats(rpc_stub_.get_root_server(), store_.current);
      else if (server_type == ObStatManager::SERVER_TYPE_UPDATE)
        ret = rpc_stub_.fetch_stats(rpc_stub_.get_update_server(), store_.current);
      else if (server_type == ObStatManager::SERVER_TYPE_CHUNK ||
               server_type == ObStatManager::SERVER_TYPE_MERGE)
      {
        std::vector<ObServer> *server_list = NULL;
        std::vector<ObServer>::iterator it;
        if (server_type == ObStatManager::SERVER_TYPE_CHUNK)
          server_list = &(rpc_stub_.get_chunk_server_list());
        else
          server_list = &(rpc_stub_.get_merge_server_list());
        for (it = server_list->begin(); it != server_list->end(); it++)
        {
          ObStatManager node_stats;
          node_stats.set_server_type(server_type);
          rpc_stub_.fetch_stats(*it, node_stats);
          if (it == server_list->begin())
            store_.current = node_stats;
          else
            store_.current.add(node_stats);
        }
      }

      return ret;
    }

    int ObServerStats::calc_hit_ratio(oceanbase::common::ObStat &stat_item, 
        const int ratio, const int hit, const int miss)
    {
      int64_t hit_num = stat_item.get_value(hit);
      int64_t miss_num = stat_item.get_value(miss);
      if (hit_num + miss_num == 0) return 0;
      int64_t ratio_num  = hit_num * 100 / (hit_num + miss_num);
      return stat_item.set_value(ratio, ratio_num);
    }

    int ObServerStats::calc_div_value(oceanbase::common::ObStat &stat_item, 
        const int div, const int count, const int time)
    {
      int64_t count_value = stat_item.get_value(count);
      int64_t time_value = stat_item.get_value(time);
      int64_t div_value = 0;
      if (count_value != 0) div_value = time_value / count_value;
      return stat_item.set_value(div, div_value);
    }

    int32_t ObServerStats::calc() 
    {
      //int ret = store_.current.subtract(store_.prev, store_.diff);
      int64_t stats_item_size = store_.current.end() - store_.current.begin();
      if (0 == stats_item_size )
      {
        fprintf(stderr, "server has not got any requests, no stats information.\n");
      }
      store_.diff = store_.current;
      store_.diff.subtract(store_.prev);
      if (store_.diff.get_server_type() == 2) // chunk server 
      {
        ObStatManager::const_iterator it = store_.diff.begin();
        while (it != store_.diff.end())
        {
          calc_hit_ratio(*const_cast<ObStat*>(it), 12, 4, 5);
          calc_hit_ratio(*const_cast<ObStat*>(it), 13, 6, 7);
          calc_div_value(*const_cast<ObStat*>(it), 14, 0, 2);
          calc_div_value(*const_cast<ObStat*>(it), 15, 1, 3);
          ++it;
        }
      }
      else if (store_.diff.get_server_type() == 3) //mergeserver
      {
        ObStatManager::const_iterator it = store_.diff.begin();
        while (it != store_.diff.end())
        {
          calc_hit_ratio(*const_cast<ObStat*>(it), 13, 8, 9);
          calc_div_value(*const_cast<ObStat*>(it), 14, 0, 1);
          calc_div_value(*const_cast<ObStat*>(it), 15, 4, 5);
          ++it;
        }
      }

      // special case for daily merge
      if (store_.diff.get_server_type() == ObStatManager::SERVER_TYPE_CHUNK 
          && table_filter_.size() == 1 && *table_filter_.begin() == 0)
      {
        store_.diff.set_server_type(Present::SERVER_COUNT);
        store_.prev.set_server_type(Present::SERVER_COUNT);
        store_.current.set_server_type(Present::SERVER_COUNT);
      }
      return 0;
    }

    int32_t ObServerStats::save() 
    {
      store_.prev = store_.current;
      store_.diff = store_.current;
      return 0;
    }

    int32_t ObServerStats::print_item(const Present::ServerInfo & server_info, 
                                       ObStatManager::const_iterator it, 
                                       const uint32_t index, 
                                       const int32_t interval) const
    {
      int32_t ret = OB_SUCCESS;
      const int32_t FMT_SIZE = 24;
      char fmt[FMT_SIZE];
      memset(fmt, 0, FMT_SIZE);
      memcpy(fmt, "%s = %", 6);
      snprintf(fmt + 6, FMT_SIZE - 6, "%dld\n", server_info[index].width);
      int64_t value = it->get_value(index);

      if (interval > 0 && server_info[index].calc_type == Present::PerSecond)
      {
        value /= interval;
      }
      else if (server_info[index].calc_type == Present::ShowCurrent)
      {
        // get value from current;
        ObStat *cur_stat = NULL;
        int ret = store_.current.get_stat(it->get_table_id(), cur_stat);
        if (OB_SUCCESS == ret && NULL != cur_stat)
        {
          value = cur_stat->get_value(index);
        }
      }
      else if (server_info[index].calc_type == Present::Ratio)
      {
        // do nothing
      }
      fprintf(stderr, fmt, server_info[index].name.c_str(), value) ;
      return ret;
    }

    int32_t ObServerStats::output(const int32_t interval)
    {
      const Present::ServerInfo & server_info = present_.get_server_info(store_.diff.get_server_type());
      int32_t show_size = server_info.size();

      time_t t;
      time(&t);
      struct tm tm;
      ::localtime_r((const time_t*)&t, &tm);
      char date_buffer[OB_MAX_FILE_NAME_LENGTH];
      snprintf(date_buffer,OB_MAX_FILE_NAME_LENGTH,"%04d-%02d-%02d %02d:%02d:%02d",
          tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

      ObStatManager::const_iterator it = store_.diff.begin();
      while (it != store_.diff.end())
      {
        fprintf(stderr, "------------------Table Separation----------------\n");
        if (table_filter_.size() > 0 
            && table_filter_.find(it->get_table_id()) == table_filter_.end())
        {
          ++it; 
          continue;
        }

        if (show_date_) fprintf(stderr, "%s \n", date_buffer);
        fprintf(stderr, "table_id = %8ld \n", it->get_table_id());

        if (index_filter_.size() > 0 )
        {
          for (uint32_t i = 0 ; i < index_filter_.size(); ++i)
          {
            uint32_t index = index_filter_[i];
            if (index >= server_info.size())
            {
              fprintf(stderr, "%4ld ", 0L) ;
            }
            else
            {
              print_item(server_info, it, index, interval);
            }
          }
        }
        else
        {
          for (int i = 0 ; i < show_size  ; ++i)
          {
            print_item(server_info, it, i, interval);
          }
        }
        ++it;
      }
      return 0;
    }

  }
}
