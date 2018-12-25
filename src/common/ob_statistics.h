/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-12-06
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_COMMON_OB_STATISTICS_H_
#define OCEANBASE_COMMON_OB_STATISTICS_H_
#include "ob_define.h"
#include "ob_array_helper.h"
#include "ob_server.h"

namespace oceanbase
{
  namespace common
  {
    enum ObStatMod
    {
      OB_STAT_ROOTSERVER = 0,  // rs
      OB_STAT_CHUNKSERVER = 1, // cs
      OB_STAT_MERGESERVER = 2, // ms
      OB_STAT_UPDATESERVER = 3,// ups
      OB_STAT_SQL = 4, // sql
      OB_STAT_OBMYSQL = 5, // obmysql
      OB_STAT_COMMON = 6, // common
      OB_STAT_SSTABLE = 7, // sstable
      OB_MAX_MOD_NUMBER, // max
    };

    class ObNewScanner;
    class ObStatManager;
    class ObStat
    {
      public:
        enum
        {
          MAX_STATICS_PER_TABLE = 100,
        };
        ObStat();
        uint64_t get_mod_id() const;
        uint64_t get_table_id() const;
        int64_t get_value(const int32_t index) const;

        int set_value(const int32_t index, int64_t value);
        int inc(const int32_t index, const int64_t inc_value = 1);

        int add2scanner(ObNewScanner &scanner) const;

        void set_table_id(const uint64_t table_id);
        void set_mod_id(const uint64_t mod_id);
        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        uint64_t mod_id_;
        uint64_t table_id_;
        volatile int64_t values_[MAX_STATICS_PER_TABLE];
    };
    class ObStatManager
    {
      public:
        typedef int64_t (*OpFunc)(const int64_t lv, const int64_t rv);
        static int64_t addop(const int64_t lv, const int64_t rv);
        static int64_t subop(const int64_t lv, const int64_t rv);
      public:
        typedef const ObStat* const_iterator;

        ObStatManager(ObRole server_type = OB_INVALID);
        virtual ~ObStatManager();
        ObStatManager & operator=(const ObStatManager &rhs);

        void init(const ObServer &server);
        ObRole get_server_type() const;
        void set_server_type(const ObRole server_type);

        int add_new_stat(const ObStat& stat);
        int set_value(const uint64_t mod, const uint64_t table_id, const int32_t index, const int64_t value);
        int inc(const uint64_t mod, const uint64_t table_id, const int32_t index, const int64_t inc_value = 1);
        int reset();
        int get_stat(const uint64_t mod, const uint64_t table_id, ObStat* &stat) const;
        int64_t get_value(const uint64_t mod, const uint64_t table_id, const int32_t index) const;
        const char* get_name(const uint64_t mod, int32_t index) const;

        int get_scanner(ObNewScanner &scanner) const;
        void set_id2name(const uint64_t mod, const char *map[], int32_t len);

        void print_info() const;

        ObStatManager & subtract(const ObStatManager &minuend);
        ObStatManager & add(const ObStatManager &augend);
        ObStatManager & operate(const ObStatManager &operand, OpFunc op);
        const_iterator begin(uint64_t mod) const;
        const_iterator end(uint64_t mod) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        tbsys::CThreadMutex lock_;
        ObRole server_type_;
        ObServer server_;
        int32_t stat_cnt_[OB_MAX_MOD_NUMBER];
        const char **map_[OB_MAX_MOD_NUMBER];
        ObStat data_holder_[OB_MAX_MOD_NUMBER][OB_MAX_TABLE_NUMBER];
        ObArrayHelper<ObStat> table_stats_[OB_MAX_MOD_NUMBER];

    };
  }
}
#endif
