/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-10-20
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_TABLET_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_TABLET_INFO_MANAGER_H_
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_array.h"
#include "common/ob_tablet_info.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObTabletInfoManager;
    class ObTabletCrcHistoryHelper
    {
      public:
        enum
        {
          MAX_KEEP_HIS_COUNT = 3,
        };
        ObTabletCrcHistoryHelper();
        void reset();
        void rest_all_crc_sum();
        int check_and_update(const int64_t version, const uint64_t crc_sum, const uint64_t row_checksum);
        int get_row_checksum(const int64_t version, uint64_t &row_checksum) const;
        void reset_crc_sum(const int64_t version);
        void get_min_max_version(int64_t &min_version, int64_t &max_version) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
        friend class ObTabletInfoManager;
      private:
        void update_crc_sum(const int64_t version, const int64_t new_version, const uint64_t crc_sum, const uint64_t row_checksum);
      private:
        int64_t version_[MAX_KEEP_HIS_COUNT];
        uint64_t crc_sum_[MAX_KEEP_HIS_COUNT];
        uint64_t row_checksum_[MAX_KEEP_HIS_COUNT];
    };
    class ObTabletInfoManager
    {
      public:
        typedef common::ObTabletInfo* iterator;
        typedef const common::ObTabletInfo* const_iterator;

        ObTabletInfoManager();
        void set_allocator(common::CharArena *allocator);

        void clear();
        int add_tablet_info(const common::ObTabletInfo& tablet_info, int32_t& out_index);

        const_iterator begin() const;
        const_iterator end() const;

        iterator begin();
        iterator end();

        int32_t get_array_size() const;

        int32_t get_index(const_iterator it) const;
        //add liumz, [secondary index static_index_build] 20150319:b
        int32_t get_index(const common::ObTabletInfo &tablet_info);
        //add:e
        const_iterator get_tablet_info(const int32_t index) const;
        iterator get_tablet_info(const int32_t index);

        ObTabletCrcHistoryHelper* get_crc_helper(const int32_t index);
        const ObTabletCrcHistoryHelper* get_crc_helper(const int32_t index) const;

        void hex_dump(const int32_t index, const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
        void dump_as_hex(FILE* stream) const;
        void read_from_hex(FILE* stream);

        NEED_SERIALIZE_AND_DESERIALIZE;
      public:
        static const int64_t MAX_TABLET_COUNT = 10 * 1024 * 1024;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletInfoManager);
      private:
        common::ObTabletInfo data_holder_[MAX_TABLET_COUNT];
        ObTabletCrcHistoryHelper crc_helper_[MAX_TABLET_COUNT];
        common::ObArrayHelper<common::ObTabletInfo> tablet_infos_;
        common::CharArena allocator_;
    };

  }
}

#endif
