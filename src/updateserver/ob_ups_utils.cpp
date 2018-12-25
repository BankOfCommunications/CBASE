////===================================================================
 //
 // ob_ups_utils.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2010-10-13 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_malloc.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_action_flag.h"
#include "common/utility.h"
#include "ob_table_engine.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"
#include "ob_ups_stat.h"
#include "ob_sstable_mgr.h"
#include "ob_ups_table_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;
    Dummy __dummy__;
    GConf g_conf = {true, 0, true};

    template <>
    int ups_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
    };

    template <>
    int ups_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi64(buf, data_len, pos, data);
    };

    template <>
    int ups_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
    };

    template <>
    int ups_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi64(buf, data_len, pos, &data);
    };

    template <>
    int ups_serialize<uint32_t>(const uint32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi32(buf, data_len, pos, (int32_t)data);
    };

    template <>
    int ups_serialize<int32_t>(const int32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi32(buf, data_len, pos, data);
    };

    template <>
    int ups_deserialize<uint32_t>(uint32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi32(buf, data_len, pos, (int32_t*)&data);
    };

    template <>
    int ups_deserialize<int32_t>(int32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi32(buf, data_len, pos, &data);
    };

    template <>
    int ups_serialize<ObDataBuffer>(const ObDataBuffer &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf
          || 0 >= (data_len - pos))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (data.get_position() > (data_len - pos))
      {
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(buf + pos, data.get_data(), data.get_position());
        pos += data.get_position();
      }
      return ret;
    }

    bool is_range_valid(const ObVersionRange &version_range)
    {
      bool bret = false;
      if (version_range.border_flag_.is_min_value()
          || version_range.border_flag_.is_max_value())
      {
        bret = true;
      }
      else if (version_range.start_version_ < version_range.end_version_)
      {
        bret = true;
      }
      else if (version_range.start_version_ == version_range.end_version_)
      {
        if (version_range.border_flag_.inclusive_start()
            && version_range.border_flag_.inclusive_end())
        {
          bret = true;
        }
      }
      return bret;
    }

    bool is_in_range(const int64_t key, const ObVersionRange &version_range)
    {
      bool bret = false;
      if (version_range.is_whole_range())
      {
        bret = true;
      }
      else if (key > version_range.start_version_
              && version_range.border_flag_.is_max_value())
      {
        bret = true;
      }
      else if (key < version_range.end_version_
              && version_range.border_flag_.is_min_value())
      {
        bret = true;
      }
      else if (key > version_range.start_version_
              && key < version_range.end_version_)
      {
        bret = true;
      }
      else if (key == version_range.start_version_
              && version_range.border_flag_.inclusive_start())
      {
        bret = true;
      }
      else if (key == version_range.end_version_
              && version_range.border_flag_.inclusive_end())
      {
        bret = true;
      }
      else
      {
        // do nothing
      }
      return bret;
    }


    int precise_sleep(const int64_t microsecond)
    {
      int ret = OB_SUCCESS;
      if (0 < microsecond)
      {
        int64_t end_time = tbsys::CTimeUtil::getMonotonicTime() + microsecond;
        int64_t time2sleep = microsecond;
        struct timeval tv;
        do
        {
          tv.tv_sec = time2sleep / 1000000;
          tv.tv_usec = time2sleep % 1000000;
          int select_ret = select(0, NULL, NULL, NULL, &tv);
          if (0 == select_ret)
          {
            break;
          }
          else if (EINTR == errno)
          {
            int64_t cur_time = tbsys::CTimeUtil::getMonotonicTime();
            if (0 < (time2sleep = (end_time - cur_time)))
            {
              continue;
            }
            else
            {
              break;
            }
          }
          else
          {
            ret = OB_ERROR;
          }
        }
        while (end_time > tbsys::CTimeUtil::getMonotonicTime());
      }
      return ret;
    }

    const char *inet_ntoa_r(easy_addr_t addr)
    {
      static const int64_t BUFFER_SIZE = 64;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread uint64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      return easy_inet_addr_to_str(&addr, buffer, BUFFER_SIZE);
    }

    void SwitchSKeyDuty::runTimerTask()
    {
      return;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get ups_main fail");
      }
      else
      {
        ObUpdateServer &ups = ups_main->get_update_server();
        ups.submit_switch_skey();
      }
    }

    int64_t get_max_row_cell_num()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ret = ups_main->get_update_server().get_param().max_row_cell_num;
      }
      return ret;
    }

    int64_t get_table_available_warn_size()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ret = ups_main->get_update_server().get_param().table_available_warn_size;
      }
      return ret;
    }

    int64_t get_table_available_error_size()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ret = ups_main->get_update_server().get_param().table_available_error_size;
      }
      return ret;
    }

    int64_t get_table_memory_limit()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ret = ups_main->get_update_server().get_param().table_memory_limit;
      }
      return ret;
    }

    bool ups_available_memory_warn_callback(const int64_t mem_size_available)
    {
      bool bret = true;
      static volatile uint64_t lock = 0;
      OnceGuard lock_guard(lock);
      static const int64_t MIN_PROC_INTERVAL = 10L * 1000L * 1000L;
      int64_t table_available_error_size = 0;
      
      if (!lock_guard.try_lock())
      {}
      else
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get updateserver main fail");
        }
        else
        {
          table_available_error_size = ups_main->get_update_server().get_param().table_available_error_size;
          if (table_available_error_size >= mem_size_available)
          {
            ups_main->get_update_server().submit_immediately_drop();
            if (REACH_TIME_INTERVAL(MIN_PROC_INTERVAL))
            {
              TBSYS_LOG(ERROR, "available memory reach the error-size=%ld, will try to free a memtable",
                        mem_size_available);
            }
          }
          else
          {
            ups_main->get_update_server().submit_delay_drop();
          }
        }

        if (REACH_TIME_INTERVAL(MIN_PROC_INTERVAL))
        {
          TBSYS_LOG(INFO, "available memory reach the warn-size=%ld, will try to free a memtable",
                    mem_size_available);
        }
      }
      return bret;
    }

    int64_t get_conf_warm_up_time()
    {
      int64_t warm_up_time = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        warm_up_time = ups_main->get_update_server().get_param().warm_up_time;
      }
      return warm_up_time;
    }

    void set_warm_up_percent(const int64_t warm_up_percent)
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().get_table_mgr().set_warm_up_percent(warm_up_percent);
      }
    }

    void submit_force_drop()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().submit_force_drop();
      }
    }

    void schedule_warm_up_duty()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().schedule_warm_up_duty();
      }
    }

    bool using_memtable_bloomfilter()
    {
      bool bret = false;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        if (ups_main->get_update_server().get_param().using_memtable_bloomfilter)
        {
          bret = true;
        }
      }
      return bret;
    }

    bool sstable_dio_writing()
    {
      bool bret = true;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        if (!ups_main->get_update_server().get_param().write_sstable_use_dio)
        {
          bret = false;
        }
      }
      return bret;
    }

    void log_scanner(common::ObScanner *scanner)
    {
      if (TBSYS_LOG_LEVEL_DEBUG == TBSYS_LOGGER._level
          && NULL != scanner)
      {
        int64_t i = 0;
        ObCellInfo *ci = NULL;
        while (OB_SUCCESS == scanner->next_cell())
        {
          if (OB_SUCCESS == scanner->get_cell(&ci)
              && NULL != ci)
          {
            TBSYS_LOG(DEBUG, "[LOG_SCANNER][%ld] %s", i++, print_cellinfo(ci));
          }
          else
          {
            break;
          }
        }
        scanner->reset_iter();
      }
    }

    const char *print_scanner_info(common::ObScanner *scanner)
    {
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread uint64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      if (NULL != scanner)
      {
        ObRowkey last_row_key;
        scanner->get_last_row_key(last_row_key);

        bool is_full_filled = true;
        int64_t fullfilled_item_num = 0;
        scanner->get_is_req_fullfilled(is_full_filled, fullfilled_item_num);

        snprintf(buffer, BUFFER_SIZE, "is_empty=%s last_row_key=%s is_full_filled=%s fullfilled_item_num=%ld "
                "data_version=%ld size=%ld id_name_type=%d",
                STR_BOOL(scanner->is_empty()), to_cstring(last_row_key), STR_BOOL(is_full_filled), fullfilled_item_num,
                scanner->get_data_version(), scanner->get_size(), scanner->get_id_name_type());
      }
      return buffer;
    }

    int64_t get_active_mem_limit()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ret = ups_main->get_update_server().get_param().active_mem_limit;
      }
      return ret;
    }

    int64_t get_oldest_memtable_size()
    {
      int64_t ret = 0;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        int64_t oldest_memtable_size = 0;
        uint64_t major_version = SSTableID::MAX_MAJOR_VERSION;
        if (OB_SUCCESS == ups_main->get_update_server().get_table_mgr().get_oldest_memtable_size(oldest_memtable_size, major_version))
        {
          ret = oldest_memtable_size;
        }
      }
      return ret;
    }

    void submit_load_bypass(const common::ObPacket *packet)
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().submit_load_bypass(packet);
      }
    }

    void submit_immediately_drop()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().submit_immediately_drop();
      }
    }

    void submit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum)
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().submit_check_sstable_checksum(sstable_id, checksum);
      }
    }

    uint64_t get_create_time_column_id(const uint64_t table_id)
    {
      static const int64_t CACHE_SIZE = 32;
      static __thread bool inited = false;
      static __thread int64_t CACHED_SCHEMA_VERSION = 0;
      static __thread uint64_t COLUMN_ID_CACHE[CACHE_SIZE];
      if (!inited
          || CACHED_SCHEMA_VERSION != g_conf.global_schema_version)
      {
        for (int64_t i = 0; i < CACHE_SIZE; i++)
        {
          COLUMN_ID_CACHE[i] = OB_INVALID_ID;
        }
        inited = true;
        CACHED_SCHEMA_VERSION = g_conf.global_schema_version;
        TBSYS_LOG(INFO, "create time column id cache is cleared");
      }

      uint64_t ret = OB_CREATE_TIME_COLUMN_ID;

      bool small_table_id = false;
      if (table_id >= OB_APP_MIN_TABLE_ID
          && table_id < (OB_APP_MIN_TABLE_ID + 32))
      {
        small_table_id = true;
        ret = COLUMN_ID_CACHE[table_id - OB_APP_MIN_TABLE_ID];
      }

      if (!small_table_id
          || OB_INVALID_ID == ret)
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get updateserver main fail");
        }
        else
        {
          ret = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_create_time_column_id(table_id);
          ret = (OB_INVALID_ID == ret) ? OB_CREATE_TIME_COLUMN_ID : ret;
          if (small_table_id)
          {
            COLUMN_ID_CACHE[table_id - OB_APP_MIN_TABLE_ID] = ret;
            TBSYS_LOG(INFO, "table_id=%lu create time column_id=%lu add to cache", table_id, ret);
          }
        }
      }
      return ret;
    }

    uint64_t get_modify_time_column_id(const uint64_t table_id)
    {
      static const int64_t CACHE_SIZE = 32;
      static __thread bool inited = false;
      static __thread int64_t CACHED_SCHEMA_VERSION = 0;
      static __thread uint64_t COLUMN_ID_CACHE[CACHE_SIZE];
      if (!inited
          || CACHED_SCHEMA_VERSION != g_conf.global_schema_version)
      {
        for (int64_t i = 0; i < CACHE_SIZE; i++)
        {
          COLUMN_ID_CACHE[i] = OB_INVALID_ID;
        }
        inited = true;
        CACHED_SCHEMA_VERSION = g_conf.global_schema_version;
        TBSYS_LOG(INFO, "modify time column id cache is cleared");
      }

      uint64_t ret = OB_MODIFY_TIME_COLUMN_ID;

      bool small_table_id = false;
      if (table_id >= OB_APP_MIN_TABLE_ID
          && table_id < (OB_APP_MIN_TABLE_ID + 32))
      {
        small_table_id = true;
        ret = COLUMN_ID_CACHE[table_id - OB_APP_MIN_TABLE_ID];
      }

      if (!small_table_id
          || OB_INVALID_ID == ret)
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get updateserver main fail");
        }
        else
        {
          ret = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_modify_time_column_id(table_id);
          ret = (OB_INVALID_ID == ret) ? OB_MODIFY_TIME_COLUMN_ID : ret;
          if (small_table_id)
          {
            COLUMN_ID_CACHE[table_id - OB_APP_MIN_TABLE_ID] = ret;
            TBSYS_LOG(INFO, "table_id=%lu modify time column_id=%lu add to cache", table_id, ret);
          }
        }
      }
      return ret;
    }

    int get_ups_schema_mgr(CommonSchemaManagerWrapper& schema_mgr)
    {
      int err = OB_SUCCESS;
      ObUpdateServerMain *ups_main = NULL;
      if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else if (OB_SUCCESS != (err = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(schema_mgr)))
      {
        TBSYS_LOG(ERROR, "get_schema_mgr()=>%d", err);
      }
      return err;
    }

    void set_client_mgr_err(const int err)
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        (const_cast<common::ObClientManager&>(ups_main->get_update_server().get_client_manager())).set_error(err);
      }
    }

    const ObRowkeyInfo *RowkeyInfoCache::get_rowkey_info(const uint64_t table_id) const
    {
      const ObRowkeyInfo *rki = NULL;
      ObUpdateServerMain *ups_main = NULL;
      if (rkinfo_table_id_ == table_id)
      {
        rki = &rkinfo_;
      }
      else if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        UpsSchemaMgrGuard sm_guard;
        const CommonSchemaManager *sm = NULL;
        const CommonTableSchema *tm = NULL;
        if (NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard))
            && NULL != (tm = sm->get_table_schema(table_id)))
        {
          rkinfo_table_id_ = table_id;
          rkinfo_ = tm->get_rowkey_info();
          rki = &rkinfo_;
        }
      }
      TBSYS_LOG(DEBUG, "cached_table_id=%lu table_id=%lu cached_rowkey_info=[%s] rowkey_info=%p",
                rkinfo_table_id_, table_id, to_cstring(rkinfo_), rki);
      return rki;
    }

    const ObRowkeyInfo *RowkeyInfoCache::get_rowkey_info(ObIUpsTableMgr &table_mgr, const uint64_t table_id) const
    {
      const ObRowkeyInfo *rki = NULL;
      if (rkinfo_table_id_ == table_id)
      {
        rki = &rkinfo_;
      }
      else
      {
        UpsSchemaMgrGuard sm_guard;
        const CommonSchemaManager *sm = NULL;
        const CommonTableSchema *tm = NULL;
        if (NULL != (sm = table_mgr.get_schema_mgr().get_schema_mgr(sm_guard))
            && NULL != (tm = sm->get_table_schema(table_id)))
        {
          rkinfo_table_id_ = table_id;
          rkinfo_ = tm->get_rowkey_info();
          rki = &rkinfo_;
        }
      }
      return rki;
    }

    int64_t get_memtable_hash_buckets_size()
    {
      return ObUpdateServerMain::get_instance()->get_update_server().get_param().memtable_hash_buckets_size;
    }

    bool is_inmemtable(uint64_t table_id)
    {
      return OB_INVALID_ID != table_id && table_id < OB_APP_MIN_TABLE_ID;
    }

    uint64_t get_table_id(const ObGetParam& get_param)
    {
      uint64_t ret = OB_INVALID_ID;
      ObCellInfo* cell = NULL;

      if (get_param.get_cell_size() <= 0)
      {}
      else if (NULL == (cell = get_param[0]))
      {
        TBSYS_LOG(ERROR, "get_param[0] == NULL");
      }
      else
      {
        ret = cell->table_id_;
      }
      return ret;
    }

    uint64_t get_table_id(const ObScanParam& scan_param)
    {
      return scan_param.get_table_id();
    }

    bool &tc_is_replaying_log()
    {
      static __thread bool is_replaying_log = false;
      return is_replaying_log;
    }

    int64_t get_time(const char *str)
    {
      const static int64_t TIME_MICROSECOND = 1UL;
      const static int64_t TIME_MILLISECOND = 1000UL;
      const static int64_t TIME_SECOND = 1000 * 1000UL;
      const static int64_t TIME_MINUTE = 60 * 1000 * 1000UL;
      const static int64_t TIME_HOUR = 60 * 60 * 1000 * 1000UL;
      const static int64_t TIME_DAY = 24 * 60 * 60 * 1000 * 1000UL;

      int64_t value = -1;
      if (NULL == str || '\0' == str[0])
      {
        TBSYS_LOG(ERROR, "invalid param, str=%p", str);
      }
      else
      {
        char* p_unit = NULL;
        value = strtol(str, &p_unit, 0);
        if (0 > value)
        {
          TBSYS_LOG(ERROR, "invalid time, str=[%s]", str);
          value = -1;
        }
        else if ('\0' == *p_unit || 0 == strcasecmp("us", p_unit))
        {
          /* default is usec */
          value *= TIME_MICROSECOND;
        }
        else if (0 == strcasecmp("ms", p_unit))
        {
          value *= TIME_MILLISECOND;
        }
        else if (0 == strcasecmp("s", p_unit))
        {
          value *= TIME_SECOND;
        }
        else if (0 == strcasecmp("m", p_unit))
        {
          value *= TIME_MINUTE;
        }
        else if (0 == strcasecmp("h", p_unit))
        {
          value *= TIME_HOUR;
        }
        else if (0 == strcasecmp("d", p_unit))
        {
          value *= TIME_DAY;
        }
        else
        {
          TBSYS_LOG(ERROR, "invalid unit, str=[%s]", str);
          value = -1;
        }
      }
      return value;
    }

    int64_t ob_atoll(const char *str)
    {
      return ::atoll(str);
    }

    int64_t split_string(const char *str, int64_t *values, str_to_int_pt *callbacks, const int64_t size, const char d)
    {
      int64_t idx = 0;
      if (NULL != str)
      {
        static const int64_t BUFFER_SIZE = 1024;
        char buffer[BUFFER_SIZE];
        snprintf(buffer, BUFFER_SIZE, "%s", str);
        char *iter = buffer;
        char *deli = NULL;
        char *end = iter + strlen(iter);

        while (iter < end
              && idx < size)
        {
          deli = strchr(iter, d);
          TBSYS_LOG(DEBUG, "split deli=[%s] iter=[%s]", deli, iter);
          if (NULL != deli)
          {
            *deli = '\0';
          }

          values[idx] = callbacks[idx](iter);
          if (0 > values[idx])
          {
            break;
          }

          idx += 1;
          if (NULL == deli)
          {
            break;
          }
          iter = deli + 1;
        }
      }
      TBSYS_LOG(INFO, "str=[%s] num=%ld values=[%s]", str, idx, print_array(values, idx));
      return idx;
    }
  }
}

void memory_limit_callback()
{
  static __thread bool callback_running = false; // avoid recursion
  if (!callback_running)
  {
    callback_running = true;
    TBSYS_LOG(INFO, "ups memory_limit_callback");
    if (REACH_TIME_INTERVAL(1L * 1000000L))
    {
      updateserver::ObUpdateServerMain *ups_main = updateserver::ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().submit_immediately_drop();
      }
    }
    callback_running = false;
  }
}

