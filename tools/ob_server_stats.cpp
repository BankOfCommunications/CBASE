#include <algorithm>
#include "ob_server_stats.h"

using namespace oceanbase::common;

/*
static char* root_server_stats_str[] =
{
  "succ_get_count",
  "succ_scan_count",
  "fail_get_count",
  "fail_scan_count",
};

static char* chunk_server_stats_str[] =
{
  "get_count",
  "scan_count",

  "get_time",
  "scan_time",

  "block_index_cache_hit",
  "block_index_cache_miss",

  "block_cache_hit",
  "block_cache_miss",

  "get_bytes",
  "scan_bytes",

  "disk_io_num",
  "disk_io_bytes",

  "block_index_cache_hit_ratio",
  "block_cache_hit_ratio",
};

static char* merge_server_stats_str[] =
{
  "succ_get_count",
  "succ_get_time",
  "fail_get_count",
  "fail_get_time",

  // scan
  "succ_scan_count",
  "succ_scan_time",
  "fail_scan_count",
  "fail_scan_time",

  // cache hit
  "cs_cache_hit",
  "cs_cache_miss",

  // cs version error
  "fail_cs_version",

  // local query
  "local_cs_query",
  "remote_cs_query",

  "cs_cache_hit_ratio"
};

static char* update_server_stats_str[] =
{
  "min",

  "get_count",
  "scan_count",
  "apply_count",
  "batch_count",
  "merge_count",

  "get_timeu",
  "scan_timeu",
  "apply_timeu",
  "batch_timeu",
  "merge_timeu",

  "memory_total",
  "memory_limit",
  "memtable_total",
  "memtable_used",
  "total_line",

  "active_memtable_limit",
  "active_memtable_total",
  "active_memtable_used",
  "active_total_line",

  "frozen_memtable_limit",
  "frozen_memtable_total",
  "frozen_memtable_used",
  "frozen_total_line",

  "apply_fail_count",
  "tbsys_drop_count",

  "max",
};


static char** stats_info_str[] =
{
  NULL,
  root_server_stats_str,
  chunk_server_stats_str,
  merge_server_stats_str,
  update_server_stats_str
};
*/

//static int stats_size[5] = {0, 4, 14, 14, 26};
static const char* server_name[] = {"unknown", "rs:table", "cs:table", "ms:table", "ups:table", "dailymerge"};

namespace oceanbase
{
  namespace tools
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
      ServerInfo & root_server_info = server_info_[OB_ROOTSERVER - 1];
      root_server_info.push_back(Item("rs_succ_get_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_succ_scan_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_fail_get_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_fail_scan_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_get_obi_role_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_migrate_count", 4, ShowCurrent));
      root_server_info.push_back(Item("rs_copy_count", 4, ShowCurrent));

      // chunkserver
      ServerInfo & chunk_server_info = server_info_[OB_CHUNKSERVER - 1];
      chunk_server_info.push_back(Item( "cs_get_count", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_scan_count", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_get_time", 8, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_scan_time", 8, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_block_index_cache_hit", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_block_index_cache_miss", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_block_cache_hit", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_block_cache_miss", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_get_bytes", 8, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_scan_bytes", 8, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_disk_io_num", 4, ShowCurrent));
      chunk_server_info.push_back(Item( "cs_disk_io_bytes", 8, ShowCurrent));
      //chunk_server_info.push_back(Item( "cs_block_index_cache_hit_ratio", 3, Ratio));
      //chunk_server_info.push_back(Item( "cs_block_cache_hit_ratio", 3, Ratio));
      //chunk_server_info.push_back(Item( "cs_average_get_time", 8, Ratio));
      //chunk_server_info.push_back(Item( "cs_average_scan_time", 8, Ratio));

      // mergeserver
      ServerInfo & merge_server_info = server_info_[OB_MERGESERVER - 1];
      merge_server_info.push_back(Item( "ms_succ_get_count", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_succ_get_time", 8, ShowCurrent));
      merge_server_info.push_back(Item( "ms_fail_get_count", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_fail_get_time", 8, ShowCurrent));
      // scan
      merge_server_info.push_back(Item( "ms_succ_scan_count", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_succ_scan_time", 8, ShowCurrent));
      merge_server_info.push_back(Item( "ms_fail_scan_count", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_fail_scan_time", 8, ShowCurrent));
      // cache hit
      merge_server_info.push_back(Item( "ms_cs_cache_hit", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_cs_cache_miss", 4, ShowCurrent));
      // cs version error
      merge_server_info.push_back(Item( "ms_fail_cs_version", 4, ShowCurrent));
      // local query
      merge_server_info.push_back(Item( "ms_local_cs_query", 4, ShowCurrent));
      merge_server_info.push_back(Item( "ms_remote_cs_query", 4, ShowCurrent));
      //merge_server_info.push_back(Item( "ms_cs_cache_hit_ratio", 3, Ratio));
      //merge_server_info.push_back(Item( "ms_average_succ_get_time", 8, Ratio));
      //merge_server_info.push_back(Item( "ms_average_succ_scan_time", 8, Ratio));


      // updateserver
      ServerInfo &update_server_info = server_info_[OB_UPDATESERVER - 1];
      update_server_info.push_back(Item( "min", 0, ShowCurrent));
      update_server_info.push_back(Item( "ups_get_count", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_scan_count", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_apply_count",  4, ShowCurrent));
      update_server_info.push_back(Item( "ups_batch_count", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_merge_count", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_get_timeu", 8, ShowCurrent));
      update_server_info.push_back(Item( "ups_scan_timeu", 8, ShowCurrent));
      update_server_info.push_back(Item( "ups_apply_timeu",  8, ShowCurrent));
      update_server_info.push_back(Item( "ups_batch_timeu", 8, ShowCurrent));
      update_server_info.push_back(Item( "ups_merge_timeu", 8, ShowCurrent));
      //update_server_info.push_back(Item( "ups_average_get_timeu", 8, Ratio));
      //update_server_info.push_back(Item( "ups_average_scan_timeu", 8, Ratio));
      //update_server_info.push_back(Item( "ups_average_apply_timeu",  8, Ratio));
      //update_server_info.push_back(Item( "ups_average_batch_timeu", 8, Ratio));
      //update_server_info.push_back(Item( "ups_average_merge_timeu", 8, Ratio));

      update_server_info.push_back(Item( "ups_memory_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_memory_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_active_memtable_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_active_memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_active_memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_active_total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_frozen_memtable_limit", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_frozen_memtable_total", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_frozen_memtable_used", 12, ShowCurrent));
      update_server_info.push_back(Item( "ups_frozen_total_line", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_apply_fail_count", 4, ShowCurrent));
      update_server_info.push_back(Item( "ups_tbsys_drop_count", 4, ShowCurrent));
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
      daily_merge_info.push_back(Item( "mu_total", 8, ShowCurrent));
    }

    //---------------------------------------------------------------
    // class ObServerStats
    //---------------------------------------------------------------

    int32_t ObServerStats::init()
    {
      return 0;
    }

    void ObServerStats::initialize_empty_value()
    {
      const int64_t server_type = store_.current.get_server_type();
      const Present::ServerInfo & server_info =
        present_.get_server_info(static_cast<int32_t>(server_type));
      std::set<uint64_t>::const_iterator it = table_filter_.begin();
      while (it != table_filter_.end())
      {
        uint64_t table_id = *it;
        for (int i = 0; i < (int)server_info.size(); ++i)
        {

          store_.current.set_value(mod_, table_id, i, 0);
          store_.prev.set_value(mod_, table_id, i, 0);
          store_.diff.set_value(mod_, table_id, i, 0);
        }
        ++it;
      }
    }

    // retrive data from dataserver.
    int32_t ObServerStats::refresh()
    {
      int ret = rpc_stub_.fetch_stats(store_.current);
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
      int64_t stats_item_size = store_.current.end(mod_) - store_.current.begin(mod_);
      if (0 == stats_item_size )
      {
        //fprintf(stderr, "server has not got any requests, no stats information.\n");
        initialize_empty_value();
      }
      store_.diff = store_.current;
      store_.diff.subtract(store_.prev);
      if (store_.diff.get_server_type() == OB_CHUNKSERVER) // chunk server
      {
        ObStatManager::const_iterator it = store_.diff.begin(mod_);
        while (it != store_.diff.end(mod_))
        {
          calc_hit_ratio(*const_cast<ObStat*>(it), 12, 4, 5);
          calc_hit_ratio(*const_cast<ObStat*>(it), 13, 6, 7);
          calc_div_value(*const_cast<ObStat*>(it), 14, 0, 2);
          calc_div_value(*const_cast<ObStat*>(it), 15, 1, 3);
          ++it;
        }
      }
      else if (store_.diff.get_server_type() == OB_MERGESERVER) //mergeserver
      {
        ObStatManager::const_iterator it = store_.diff.begin(mod_);
        while (it != store_.diff.end(mod_))
        {
          calc_hit_ratio(*const_cast<ObStat*>(it), 13, 8, 9);
          calc_div_value(*const_cast<ObStat*>(it), 14, 0, 1);
          calc_div_value(*const_cast<ObStat*>(it), 15, 4, 5);
          ++it;
        }
      }
      else if (store_.diff.get_server_type() == OB_UPDATESERVER) //updateserver
      {
        ObStatManager::const_iterator it = store_.diff.begin(mod_);
        while (it != store_.diff.end(mod_))
        {
          calc_div_value(*const_cast<ObStat*>(it), 6, 1, 6 );
          calc_div_value(*const_cast<ObStat*>(it), 7, 2, 7);
          calc_div_value(*const_cast<ObStat*>(it), 8, 3, 8);
          calc_div_value(*const_cast<ObStat*>(it), 9, 4, 9);
          calc_div_value(*const_cast<ObStat*>(it), 10, 5, 10);
          ++it;
        }
      }

      // special case for daily merge
      if (store_.diff.get_server_type() == OB_CHUNKSERVER
          && table_filter_.size() == 1 && *table_filter_.begin() == 0)
      {
        store_.diff.set_server_type(static_cast<ObRole>(Present::SERVER_COUNT));
        store_.prev.set_server_type(static_cast<ObRole>(Present::SERVER_COUNT));
        store_.current.set_server_type(static_cast<ObRole>(Present::SERVER_COUNT));

        int64_t sum_value = 0;
        ObStatManager::const_iterator it = store_.current.begin(mod_);
        while (it != store_.current.end(mod_))
        {
          if (it->get_table_id() == 0)
          {
            for (int index = 3; index <= 13; ++index)
            {
              sum_value += const_cast<ObStat*>(it)->get_value(index);
            }
            const_cast<ObStat*>(it)->set_value(14, sum_value);
            break;
          }
          ++it;
        }
      }
      return 0;
    }

    int32_t ObServerStats::save()
    {
      store_.prev = store_.current;
      return 0;
    }


    void ObServerStats::output_header()
    {
      //char** info_str = stats_info_str[store_.diff.get_server_type()];
      //int32_t show_size = stats_size[store_.diff.get_server_type()];
      const Present::ServerInfo & server_info =
        present_.get_server_info(static_cast<int32_t>(store_.diff.get_server_type()));
      if (show_date_) fprintf(stderr, "%18s|", "date      time");
      fprintf(stderr, "%s|", server_name[store_.diff.get_server_type()]);
      if (index_filter_.size() > 0 )
      {
        for (uint32_t i = 0 ; i < index_filter_.size(); ++i)
        {
          uint32_t index = index_filter_[i];
          if (index >= server_info.size())
          {
            fprintf(stderr, "%8s ", "unknown") ;
          }
          else
          {
            fprintf(stderr, "%8s ", server_info[index].name.c_str()) ;
          }
        }
      }
      else
      {
        for (uint32_t i = 0 ; i < server_info.size(); ++i)
        {
          fprintf(stderr, "%8s ", server_info[i].name.c_str()) ;
        }
      }
      fprintf(stderr, "\n");
    }

    int64_t ObServerStats::print_value(const Present::ServerInfo & server_info,
        ObStatManager::const_iterator it, const uint32_t index, const int32_t interval) const
    {
      const int32_t FMT_SIZE = 24;
      char fmt[FMT_SIZE];
      fmt[0] = '%';
      snprintf(fmt+1, FMT_SIZE-1, "%dld ", server_info[index].width);
      int64_t value = it->get_value(index);
      //fprintf(stderr, "index=%d,interval=%d,value=%ld\n", index,interval, value);
      if (server_info[index].calc_type == Present::PerSecond)
      {
        value /= interval;
      }
      else if (server_info[index].calc_type == Present::ShowCurrent)
      {
        // get value from current;
        ObStat *cur_stat = NULL;
        int ret = store_.current.get_stat(it->get_mod_id(), it->get_table_id(), cur_stat);
        if (OB_SUCCESS == ret && NULL != cur_stat)
        {
          value = cur_stat->get_value(index);
        }
      }
      else if (server_info[index].calc_type == Present::Ratio)
      {
        // do nothing
      }
      fprintf(stderr, fmt, value ) ;
      return 0;
    }

    int32_t ObServerStats::output(const int32_t count, const int32_t interval)
    {
      if (show_header_ > 0 && count % show_header_ == 0) output_header();
      //if (count == 0) return 0;

      const Present::ServerInfo & server_info =
        present_.get_server_info(static_cast<int32_t>(store_.diff.get_server_type()));
      int32_t show_size = static_cast<int32_t>(server_info.size());

      time_t t;
      time(&t);
      struct tm tm;
      ::localtime_r((const time_t*)&t, &tm);
      char date_buffer[OB_MAX_FILE_NAME_LENGTH];
      snprintf(date_buffer,OB_MAX_FILE_NAME_LENGTH,"%04d-%02d-%02d %02d:%02d:%02d",
          tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
          tm.tm_hour, tm.tm_min, tm.tm_sec);

      ObStatManager::const_iterator it = store_.diff.begin(mod_);
      while (it != store_.diff.end(mod_))
      {
        if (table_filter_.size() > 0
            && table_filter_.find(it->get_table_id()) == table_filter_.end())
        {
          ++it;
          continue;
        }
        if ((int64_t)store_.diff.get_server_type() != Present::SERVER_TYPE_DAILY_MERGE
            && it->get_table_id() == 0)
        {
          ++it;
          continue;
        }

        if (show_date_) fprintf(stderr, "%s ", date_buffer);
        fprintf(stderr, "%8ld ", it->get_table_id());

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
              print_value(server_info, it, index, interval);
            }
          }
        }
        else
        {
          for (int i = 0 ; i < show_size  ; ++i)
          {
            print_value(server_info, it, i, interval);
          }
        }
        fprintf(stderr, "\n");
        ++it;
      }
      return 0;
    }

  }
}
