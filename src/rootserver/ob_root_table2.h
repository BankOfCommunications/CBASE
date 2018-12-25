/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-10-21
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOTTABLE2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOTTABLE2_H_
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "common/ob_rowkey.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
//add wenghaixing [secondary index col checksum] 20141204
#include "common/hash/ob_hashutils.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_define.h"
//add e
//add liumz, [secondary index static_index_build] 20150319:b
#include "common/ob_tablet_info.h"
//add:e

#ifdef GTEST
#define private public
#define protected public
#endif

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootServer2;
    class ObRootBalancer;

    class ObRootTable2
    {
      public:
        static const int16_t ROOT_TABLE_MAGIC = static_cast<int16_t>(0xABCD);
        typedef ObRootMeta2* iterator;
        typedef const ObRootMeta2*  const_iterator;
        enum
        {
          POS_TYPE_ADD_RANGE = 0,
          POS_TYPE_SAME_RANGE = 1,
          POS_TYPE_SPLIT_RANGE = 2,
          POS_TYPE_MERGE_RANGE = 3,
          POS_TYPE_ERROR = 5,
          POS_TYPE_UNINIT = 6,
        };
        enum
        {
          FIRST_TABLET_VERSION = 1,
        };
      public:
        friend class ObRootServer2;
        explicit ObRootTable2(ObTabletInfoManager* tim);
        virtual ~ObRootTable2();

        inline iterator begin() { return &(data_holder_[0]); }
        inline iterator end()  { return begin() + meta_table_.get_array_index(); }
        inline iterator sorted_end()  { return begin() + sorted_count_; }

        inline const_iterator begin() const { return &(data_holder_[0]); }
        inline const_iterator end()  const { return begin() + meta_table_.get_array_index(); }
        inline const_iterator sorted_end() const { return begin() + sorted_count_; }
        inline bool is_empty() const { return begin() == end(); }

        void get_cs_version(const int64_t index, int64_t &version);
        //
        void get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & data_size) const;
        // find a proper position for insert operation
        // return SUCCESS when same range found or the proper new pos found for insert
        // return OB_FIND_OUT_OF_RANGE when the proper new pos found for insert is end()
        // return OB_ERROR_OUT_OF_RANGE when no proper new pos found or internal error happen.
        int find_range(const common::ObNewRange& range,
            const_iterator& first,
            const_iterator& last) const;

        int find_key(const uint64_t table_id,
            const common::ObRowkey& key,
            int32_t adjacent_offset,
            const_iterator& first,
            const_iterator& last,
            const_iterator& ptr) const;

        bool table_is_exist(const uint64_t table_id) const;
        int get_deleted_table(const common::ObSchemaManagerV2 & schema, const ObRootBalancer& balancer, common::ObArray<uint64_t>& deleted_tables) const;
        void server_off_line(const int32_t server_index, const int64_t time_stamp);

        void dump() const;
        //add liumz, [secondary index static_index_build] 20150601:b
        void dump(const int32_t index) const;
        //add:e
        void dump_cs_tablet_info(const int server_index, int64_t &tablet_num)const;
        void dump_unusual_tablets(int64_t current_version, int32_t replicas_num, int32_t &num) const;

        int check_tablet_version_merged(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const;
        //add liumz, [secondary index static_index_build] 20150422:b
        int check_tablet_version_merged_v2(const int64_t tablet_version, const int64_t safe_count, bool &is_merged, const common::ObSchemaManagerV2 &schema_mgr) const;
        //add:e
        //add liumz, [secondary index static_index_build] 20150529:b
        int check_tablet_version_merged_v3(const uint64_t table_id, const int64_t tablet_version, const int64_t safe_count, bool &is_merged);
        //add:e
        //add liumz, [secondary index static_index_build] 20150601:b
        const common::ObTabletInfo* get_tablet_info(const int32_t meta_index) const;
        common::ObTabletInfo* get_tablet_info(const int32_t meta_index);
        //add:e
        const common::ObTabletInfo* get_tablet_info(const const_iterator& it) const;
        common::ObTabletInfo* get_tablet_info(const const_iterator& it);
        int get_table_row_checksum(const int64_t tablet_version, const uint64_t table_id, uint64_t &row_checksum);

        static int64_t get_max_tablet_version(const const_iterator& it);
        int64_t get_max_tablet_version();
        //add liumz, [secondary index static_index_build] 20150319:b
        int get_root_meta(const common::ObTabletInfo &tablet, const_iterator &root_meta);
        int get_root_meta(const int32_t meta_index, const_iterator &root_meta);
        //add:e
        //add liumz, [secondary index static_index_build] 20150601:b
        int get_root_meta_index(const common::ObTabletInfo &tablet, int32_t &meta_index);
        //add:e
        //add liumz, [secondary index static_index_build] 20150324:b
        const_iterator find_pos_by_range(const common::ObNewRange &range);
        //add:e
        // remove one replica
        int remove(const const_iterator& it, const int32_t safe_count, const int32_t server_index);
        int modify(const const_iterator& it, const int32_t dest_server_index, const int64_t tablet_version);
        int replace(const const_iterator& it, const int32_t src_server_index, const int32_t dest_server_index, const int64_t tablet_version);
        /*
         * 得到range会对root table产生的影响
         * 1个range可能导致root table分裂 合并 无影响等
         */
        int get_range_pos_type(const common::ObNewRange& range, const const_iterator& first, const const_iterator& last) const;
        int split_range(const common::ObTabletInfo& tablet_info, const const_iterator& pos, const int64_t tablet_version, const int32_t server_index);
        int add_range(const common::ObTabletInfo& tablet_info, const const_iterator& pos, const int64_t tablet_version, const int32_t server_index);

        int add(const common::ObTabletInfo& tablet, const int32_t server_index,
            const int64_t tablet_version, const int64_t tablet_seq = 0);
        int create_table(const common::ObTabletInfo& tablet, const common::ObArray<int32_t> &chunkservers, const int64_t tablet_version);
        bool add_lost_range();
        bool check_lost_data(const int64_t server_index)const;
        //bool check_lost_range();
        bool check_lost_range(const common::ObSchemaManagerV2 *schema_mgr = NULL);
        bool check_tablet_copy_count(const int32_t copy_count) const;
        void sort();

        /*
         * root table第一次构造的时候使用
         * 整理合并相同的tablet, 生成1份新的root table
         */
        int shrink_to(ObRootTable2* shrunk_table, common::ObTabletReportInfoList &delete_list);
        static int32_t find_suitable_pos(const const_iterator& it, const int32_t server_index,
            const int64_t tablet_version, common::ObTabletReportInfo *to_delete = NULL);
        int check_tablet_version(const int64_t tablet_version, int safe_copy_count) const;
        //void remove_old_tablet();
        ObTabletCrcHistoryHelper* get_crc_helper(const const_iterator& it);
        int delete_tables(const common::ObArray<uint64_t> &deleted_tables);
      void clear();
      public:
        int write_to_file(const char* filename);
        int read_from_file(const char* filename);
        void dump_as_hex(FILE* stream) const;
        void read_from_hex(FILE* stream);

      private:
      	const_iterator lower_bound(const common::ObNewRange& range) const;
      	iterator lower_bound(const common::ObNewRange& range);
        void merge_one_tablet(ObRootTable2* shrunk_table, const int32_t last_tablet_index,
            const_iterator it, common::ObTabletReportInfo &to_delete);
        bool move_back(const int32_t from_index_inclusive, const int32_t move_step);
        // @pre is sorted
        int shrink();
        bool has_been_sorted() const;
        bool internal_check() const;
      private:
        ObRootMeta2 data_holder_[ObTabletInfoManager::MAX_TABLET_COUNT];
        common::ObArrayHelper<ObRootMeta2> meta_table_;
        ObTabletInfoManager* tablet_info_manager_;
        int32_t sorted_count_;
    };
  }
}
#endif
