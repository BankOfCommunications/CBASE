#ifndef OCEANBASE_CHUNKSERVER_GEN_SSTABLE_H_
#define OCEANBASE_CHUNKSERVER_GEN_SSTABLE_H_
#include <errno.h>
#include <string.h>
#include <map>

#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_range2.h"
#include "chunkserver/ob_tablet.h"
#include "chunkserver/ob_tablet_image.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_block_builder.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"

namespace oceanbase
{
  namespace chunkserver
  {
    //class sstable::ObSSTableSchema;
    class ObGenSSTable
    {
      public:
        static const int MAX_COLUMS = 50;
        static const int MAX_ROW_KEY_LEN = 1024;
        static const uint64_t SSTABLE_ID_BASE = 1000;
        static const int32_t MAX_PATH = 256;
        static const int64_t MAX_SSTABLE_SIZE = 256 * 1024 * 1024; //256M
        static const int MAX_TABLE_NUM = 100;
        static const int64_t DISK_NUM = 10;
        static const int64_t ITEMS_PER_USER = 200;

        struct ObGenSSTableArgs
        {
          ObGenSSTableArgs() : file_no_(1),
                               reserve_ids_(30),
                               disk_no_(1),
                               max_rows_(1),
                               max_sstable_num_(1),
                               seed_(0),
                               suffix_(0),
                               step_length_(1),
                               block_size_(sstable::ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE),
                               set_min_(false),
                               set_max_(false),
                               gen_join_table_(false),
                               binary_rowkey_format_(false),
                               dest_dir_(NULL),
                               schema_mgr_(NULL),
                               config_file_(NULL),
                               comp_name_(NULL),
                               sstable_version_(0)
          {
            memset(&table_id_list_,0,sizeof(table_id_list_));
          }
          int32_t file_no_;
          int32_t reserve_ids_;
          int32_t disk_no_;
          int32_t max_rows_;
          int64_t max_sstable_num_;
          int64_t seed_;
          int64_t suffix_;
          int32_t step_length_;
          int64_t block_size_;
          bool    set_min_;
          bool    set_max_;
          bool    gen_join_table_;
          bool    binary_rowkey_format_;
          const char *dest_dir_;
          int64_t c_uid_;
          const common::ObSchemaManagerV2 *schema_mgr_;
          int32_t table_id_list_[MAX_TABLE_NUM];
          const char* config_file_;
          const char* comp_name_;
          int32_t sstable_version_;
        };

        ObGenSSTable();
        ~ObGenSSTable();
        void init(ObGenSSTableArgs& args);
        int start_builder();
        int start_builder2();

      private:
        class TableGen
        {
          public:
            typedef int (ObGenSSTable::TableGen::*cmd_call)(uint64_t group_id);

          public:
            TableGen(ObGenSSTable& gen_sstable);
            int init(uint64_t table_id, const bool is_join_table = false);
            int generate();
            int generate2();
          private:

            int generate_range_by_prefix(common::ObNewRange& range, int64_t prefix, int64_t suffix);
            int create_new_sstable(int64_t table_id);
            int create_new_sstable2(int64_t table_id);
            int finish_sstable(bool set_max = false);

            int fill_row_common(uint64_t group_id);
            int fill_row_chunk(uint64_t group_id);
            int fill_row_with_aux_column(uint64_t group_id);
            int fill_row_with_aux_column2(uint64_t group_id);
            int fill_row_null();
            int fill_row_item();
            int fill_row_common_item(uint64_t group_id);
            int fill_row_consistency_test();
            int fill_row_consistency_test_item();

            int create_rowkey_common();
            int create_rowkey_chunk();
            int create_rowkey_aux();
            int create_rowkey_aux2();
            int create_rowkey_null();
            int create_rowkey_item();
            int create_rowkey_common_item();
            int create_rowkey_consistency_test();
            int create_rowkey_consistency_test_item();
          private:
            int64_t row_key_prefix_;
            int64_t row_key_suffix_;
            int64_t step_length_;
            int64_t max_rows_;
            int64_t max_sstable_num_;
            bool    null_sstable_;
            bool    sstable_with_aux_column_;
            bool    set_min_;
            bool    set_max_;
            bool    is_join_table_;

            ObGenSSTable&   gen_sstable_;
            sstable::ObSSTableSchema schema_;
            compactsstablev2::ObSSTableSchema schema2_;
            const           common::ObTableSchema *table_schema_;
            const           common::ObSchemaManagerV2 *schema_mgr_;

            bool first_key_;
            bool inited_;

            const char* dest_dir_;
            char dest_file_[MAX_PATH];

            common::ObString dest_file_string_;

            uint64_t table_id_;
            int64_t  total_rows_;

            int32_t disk_no_;
            int64_t  current_sstable_size_;

            sstable::ObSSTableId sstable_id_;
            common::ObNewRange range_;

            char row_key_buf_[MAX_ROW_KEY_LEN];
            char start_key_[MAX_ROW_KEY_LEN];
            char varchar_buf_[MAX_ROW_KEY_LEN];

            sstable::ObSSTableRow sstable_row_;
            common::ObRow row_;
            common::ObRowDesc row_desc_;

            common::ObRowkey row_key_;
            common::ObRowkey last_end_key_;
            common::ObObj rowkey_object_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
            common::CharArena allocator_;

            sstable::ObSSTableWriter writer_;
            compactsstablev2::ObCompactSSTableWriter writer2_;
            ObTabletImage& tablet_image_;
            int64_t curr_uid_;
            int64_t curr_tid_;
            int32_t curr_itype_;
            const char* config_file_;
            const char* comp_name_;
            int32_t sstable_version_;
        };

        friend class TableGen;

        int64_t get_sstable_id();
        const common::ObString& get_compressor() const;

      private:

        int32_t file_no_;
        int32_t reserve_ids_;
        int32_t disk_no_;

        const char* dest_dir_;
        char dest_file_[MAX_PATH];
        char dest_path_[MAX_PATH];

        int32_t table_id_list_[MAX_TABLE_NUM];
        bool    gen_join_table_;
        int64_t block_size_;

        int64_t row_key_prefix_;
        int64_t row_key_suffix_;
        int64_t step_length_;
        int64_t max_rows_;
        int64_t max_sstable_num_;
        bool    set_min_;
        bool    set_max_;
        bool    binary_rowkey_format_;

        const common::ObSchemaManagerV2 *schema_mgr_;
        sstable::ObSSTableSchema schema_;
        const common::ObTableSchema *table_schema_;

        char compressor_name_[MAX_PATH];
        common::ObString compressor_string_;

        uint64_t current_sstable_id_;
        ObTabletImage tablet_image_;
        int64_t curr_uid_;
        int64_t curr_tid_;
        int32_t curr_itype_;
        const char* config_file_;
        const char* comp_name_;
        int32_t sstable_version_;
    };
  } /* chunkserver */
} /* oceanbase */
#endif /*OCEANBASE_CHUNKSERVER_DATABUILDER_H_ */
