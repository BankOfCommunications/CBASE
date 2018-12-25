////===================================================================
 //
 // ob_ups_utils.h / hash / common / Oceanbase
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

#ifndef  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#define  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_file.h"
#include "common/ob_timer.h"
#include "common/ob_scanner.h"
#include "common/ob_packet.h"
#include "common/utility.h"
#include "common/ob_tablet_info.h"
#include "common/data_buffer.h"
#include "common/ob_schema.h"
#include "common/priority_packet_queue_thread.h"

#define APPLY
#define SCAN
#define GET
#define COUNT
#define TIMEU

#define get_stat_num(level, req_type, stat_type) \
  (common::PriorityPacketQueueThread::HIGH_PRIV == level) \
    ? UPS_STAT_HL_##req_type##_##stat_type \
    : (common::PriorityPacketQueueThread::LOW_PRIV == level) \
      ? UPS_STAT_LL_##req_type##_##stat_type \
      : UPS_STAT_NL_##req_type##_##stat_type

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanParam;
  };
  namespace updateserver
  {
    class CommonSchemaManagerWrapper;
    typedef common::ObSchemaManagerV2 CommonSchemaManager;
    typedef common::ObColumnSchemaV2 CommonColumnSchema;
    typedef common::ObTableSchema CommonTableSchema;

    class TEKey;
    class TEValue;
    struct CacheWarmUpConf;
    struct SSTableID;
    extern bool is_in_range(const int64_t key, const common::ObVersionRange &version_range);
    extern bool is_range_valid(const common::ObVersionRange &version_range);
    extern int precise_sleep(const int64_t microsecond);
    extern const char *inet_ntoa_r(easy_addr_t addr);
    extern int64_t get_max_row_cell_num();
    extern int64_t get_table_available_warn_size();
    extern int64_t get_table_available_error_size();
    extern int64_t get_table_memory_limit();
    extern bool ups_available_memory_warn_callback(const int64_t mem_size_available);
    extern int64_t get_conf_warm_up_time();
    extern void set_warm_up_percent(const int64_t warm_up_percent);
    extern void submit_force_drop();
    extern void schedule_warm_up_duty();
    extern bool using_memtable_bloomfilter();
    extern bool sstable_dio_writing();
    extern void log_scanner(common::ObScanner *scanner);
    extern const char *print_scanner_info(common::ObScanner *scanner);
    extern int64_t get_active_mem_limit();
    extern int64_t get_oldest_memtable_size();
    extern void submit_load_bypass(const common::ObPacket *packet = NULL);
    extern void submit_immediately_drop();
    extern void submit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum);
    extern uint64_t get_create_time_column_id(const uint64_t table_id);
    extern uint64_t get_modify_time_column_id(const uint64_t table_id);
    extern int get_ups_schema_mgr(CommonSchemaManagerWrapper& schema_mgr);
    extern void set_client_mgr_err(const int err);
    extern int64_t get_memtable_hash_buckets_size();
    extern bool &tc_is_replaying_log();
    extern int64_t get_time(const char *str);
    extern int64_t ob_atoll(const char *str);
    typedef int64_t (*str_to_int_pt) (const char *str);
    extern int64_t split_string(const char *str, int64_t *values, str_to_int_pt *callbacks, const int64_t size, const char d = ';');
    bool is_inmemtable(uint64_t table_id);
    uint64_t get_table_id(const common::ObGetParam& get_param);
    uint64_t get_table_id(const common::ObScanParam& scan_param);
    inline void rebind_cpu(const int64_t cpu, const char *file, const int line)
    {
      static __thread int64_t bind_cpu = 0;
      static __thread int64_t have_bind = false;
      static const int64_t core_num = sysconf(_SC_NPROCESSORS_ONLN);
      if (!have_bind
          || bind_cpu != cpu)
      {
        if (0 <= cpu
            && core_num > cpu)
        {
          int64_t cpu2bind = cpu % core_num;
          cpu_set_t cpuset;
          CPU_ZERO(&cpuset);
          CPU_SET(cpu2bind, &cpuset);
          int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
          TBSYS_LOG(INFO, "[%s][%d] rebind setaffinity tid=%ld ret=%d cpu=%ld",
                    file, line, GETTID(), ret, cpu2bind);
          bind_cpu = cpu2bind;
          have_bind = true;
        }
      }
    }
    inline void rebind_cpu(const int64_t cpu_start, const int64_t cpu_end, volatile uint64_t &cpu, const char *file, const int line)
    {
      static __thread int64_t bind_cpu_start = 0;
      static __thread int64_t bind_cpu_end = 0;
      static __thread int64_t have_bind = false;
      static const int64_t core_num = sysconf(_SC_NPROCESSORS_ONLN);
      if (!have_bind
          || bind_cpu_start != cpu_start
          || bind_cpu_end != cpu_end)
      {
        if (0 <= cpu_start
            && cpu_start <= cpu_end
            && core_num > cpu_end)
        {
          //static volatile uint64_t cpu = 0;
          uint64_t cpu2bind = (__sync_fetch_and_add(&cpu, 1) % (cpu_end - cpu_start + 1)) + cpu_start;
          cpu_set_t cpuset;
          CPU_ZERO(&cpuset);
          CPU_SET(cpu2bind, &cpuset);
          int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
          TBSYS_LOG(INFO, "[%s][%d] rebind setaffinity tid=%ld ret=%d cpu=%ld start=%ld end=%ld",
                    file, line, GETTID(), ret, cpu2bind, cpu_start, cpu_end);
          bind_cpu_start = cpu_start;
          bind_cpu_end = cpu_end;
          have_bind = true;
        }
      }
    }

    struct GConf
    {
      bool using_static_cm_column_id;
      volatile int64_t global_schema_version;
      bool using_hash_index;
    };
    extern GConf g_conf;

#define OB_UPS_CREATE_TIME_COLUMN_ID(table_id) \
    ({ \
      uint64_t ret = OB_CREATE_TIME_COLUMN_ID; \
      if (!g_conf.using_static_cm_column_id) \
      { \
        ret = get_create_time_column_id(table_id); \
      } \
      ret; \
    })
#define OB_UPS_MODIFY_TIME_COLUMN_ID(table_id) \
    ({ \
      uint64_t ret = OB_MODIFY_TIME_COLUMN_ID; \
      if (!g_conf.using_static_cm_column_id) \
      { \
        ret = get_modify_time_column_id(table_id); \
      } \
      ret; \
    })

    class IObjIterator
    {
      public:
        virtual ~IObjIterator() {};
      public:
        virtual int next_obj() = 0;
        virtual int get_obj(common::ObObj **obj) = 0;
    };

    template <class T>
    const char *print_array(const T &array, const int64_t length, const char *deli = ",")
    {
      static const int64_t BUFFER_NUM = 5;
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffers[BUFFER_NUM][BUFFER_SIZE];
      static __thread uint64_t i = 0;
      char *buffer = buffers[i++ % BUFFER_NUM];
      int64_t pos = 0;
      for (int64_t i = 0; i < length; i++)
      {
        if ((length - 1) > i)
        {
          databuff_printf(buffer, BUFFER_SIZE, pos, "%s%s", to_cstring(array[i]), deli);
        }
        else
        {
          databuff_printf(buffer, BUFFER_SIZE, pos, "%s", to_cstring(array[i]));
        }
      }
      return buffer;
    };

    template <class T>
    int ups_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.serialize(buf, data_len, pos);
    };

    template <class T>
    int ups_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.deserialize(buf, data_len, pos);
    };

    template <>
    int ups_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_serialize<uint32_t>(const uint32_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_serialize<int32_t>(const int32_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_deserialize<uint32_t>(uint32_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_deserialize<int32_t>(int32_t &data, char *buf, const int64_t data_len, int64_t& pos);    

    template <>
    int ups_serialize<common::ObDataBuffer>(const common::ObDataBuffer &data, char *buf, const int64_t data_len, int64_t& pos);

    struct Dummy
    {
      int serialize(char* buf, int64_t len, int64_t& pos) const
      {
        UNUSED(buf); UNUSED(len); UNUSED(pos);
        return common::OB_SUCCESS;
      }
      int deserialize(char* buf, int64_t len, int64_t& pos)
      {
        UNUSED(buf); UNUSED(len); UNUSED(pos);
        return common::OB_SUCCESS;
      }
    };
    extern Dummy __dummy__;

    class SwitchSKeyDuty : public common::ObTimerTask
    {
      public:
        SwitchSKeyDuty() {};
        virtual ~SwitchSKeyDuty() {};
        virtual void runTimerTask();
    };

    struct TableMemInfo
    {
      int64_t memtable_used;
      int64_t memtable_total;
      int64_t memtable_limit;
      TableMemInfo()
      {
        memtable_used = 0;
        memtable_total = 0;
        memtable_limit = INT64_MAX;
      };
    };

    struct UpsPrivQueueConf
    {
      int64_t low_priv_network_lower_limit;
      int64_t low_priv_network_upper_limit;
      int64_t low_priv_adjust_flag;
      int64_t low_priv_cur_percent;
      int64_t low_priv_max_percent;

      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(buf + pos, this, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      }

      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(this, buf + pos, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };

      int64_t get_serialize_size(void) const
      {
        return sizeof(*this);
      };
    };

    struct UpsMemoryInfo
    {
      const int64_t version;
      int64_t total_size;
      int64_t cur_limit_size;
      TableMemInfo table_mem_info;
      UpsMemoryInfo() : version(1), table_mem_info()
      {
        total_size = 0;
        cur_limit_size = INT64_MAX;
      };
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(buf + pos, this, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };
      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(this, buf + pos, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };
      int64_t get_serialize_size(void) const
      {
        return sizeof(*this);
      };
    };

    struct CacheWarmUpConf
    {
      static const int64_t STOP_PERCENT = 100; // 100%
      static const int64_t STEP_PERCENT = 1; // 1%
    };

    struct TabletInfoList
    {
      common::ObStringBuf allocator;
      common::ObTabletInfoList inst;
    };

    class ObIUpsTableMgr;
    class RowkeyInfoCache
    {
      public:
        RowkeyInfoCache() : rkinfo_table_id_(common::OB_INVALID_ID),
                            rkinfo_()
        {
        };
        virtual ~RowkeyInfoCache()
        {
        };
      public:
        virtual const common::ObRowkeyInfo *get_rowkey_info(const uint64_t table_id) const;
        virtual const common::ObRowkeyInfo *get_rowkey_info(ObIUpsTableMgr &table_mgr, const uint64_t table_id) const;
      protected:
        mutable uint64_t rkinfo_table_id_;
        mutable common::ObRowkeyInfo rkinfo_;
    };
  }

  namespace common
  {
    template <>
    struct ob_vector_traits<ObTabletInfo>
    {
    public:
      typedef ObTabletInfo& pointee_type;
      typedef ObTabletInfo value_type;
      typedef const ObTabletInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };

    struct ObTableInfoEndkeyComp
    {
      bool operator() (const ObTabletInfo &a, const ObTabletInfo &b) const
      {
        return (a.range_.end_key_ < b.range_.end_key_);
      };
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_UPS_UTILS_H_

