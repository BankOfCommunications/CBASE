#ifndef OCEANBASE_CHUNKSERVER_DATABUILDER_H_
#define OCEANBASE_CHUNKSERVER_DATABUILDER_H_
#include <errno.h>
#include <string.h>
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "compactsstablev2/ob_sstable_schema.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "chunkserver/ob_tablet.h"
#include "chunkserver/ob_tablet_image.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/ob_row_desc.h"
#include "common/ob_row.h"

namespace oceanbase 
{

  namespace chunkserver 
  {
    class ObDataBuilder
    {
      public:
        static const int MAX_COLUMS = 50;
        static const int MAX_LINE_LEN = 4096;
        static const int ROW_KEY_BUF_LEN = 1024; //TODO
        static const uint64_t SSTABLE_ID_BASE = 1000;
        static const int32_t MAX_PATH = 256;
        static const int64_t DEFAULT_MAX_SSTABLE_SIZE = 256 * 1024 * 1024; //256M
        static const int64_t MAX_LINE = 1000;
        static const int64_t DEFAULT_MAX_SSTABLE_NUMBER = 30;

        struct ObDataBuilderArgs 
        {
          ObDataBuilderArgs() : table_id_(0),
                                max_sstable_size_(DEFAULT_MAX_SSTABLE_SIZE),
                                file_no_(-1),
                                reserve_ids_(DEFAULT_MAX_SSTABLE_NUMBER),
                                disk_no_(-1),
                                version_(1),
                                filename_(NULL),
                                syntax_file_(NULL),
                                dest_dir_(NULL),
                                compressor_string_(NULL),
                                schema_(NULL)
          {}
          int64_t table_id_;
          int64_t max_sstable_size_;

          int32_t file_no_;
          int32_t reserve_ids_;
          int32_t disk_no_;
          int32_t version_;

          const char *filename_;
          const char *syntax_file_;
          const char *dest_dir_;
          const char *compressor_string_;

          const common::ObSchemaManagerV2 *schema_;
        };

        
        struct ObDataBuilderArgs args;
        struct ObColumn
        {
          char *column_;
          int32_t len_;
        };
        
        ObDataBuilder();
        ~ObDataBuilder();
        int init(ObDataBuilderArgs& args);
        int start_builder();
        int build_with_no_cache();
        int build_with_cache();

      private:
        
        int prepare_new_sstable_name();
        int record_sstable_range();
        int write_idx_info();

        char *read_line(int &fields);
        int process_line(int fields);
        int process_line_with_cache(int fields,int64_t& serialize_size_);
        
        common::ObDateTime transform_date_to_time(const char *str);
        int create_rowkey(char *buf,int64_t buf_len,int64_t& pos);
        int create_and_append_rowkey(const char *fields,int index,char *buf,int64_t buf_len,int64_t& pos);

      private:
        
        char filename_[MAX_PATH];
        char next_filename_[MAX_PATH];
        char dest_dir_[MAX_PATH];
        char dest_file_[MAX_PATH];
        char compressor_name_[MAX_PATH];

        FILE *fp_;
        int32_t file_no_;
        bool first_key_;
        int32_t reserve_ids_;
        int32_t disk_no_;
        int64_t split_pos_;
        char index_file_path_[MAX_PATH];

        ObTabletImage image_;
        sstable::ObSSTableId id_;

        int64_t max_sstable_size_;
        uint64_t current_sstable_id_;

        int64_t current_line_;
        int64_t serialize_size_;
        int64_t row_key_buf_index_;
        int64_t wrote_keys_;
        int64_t read_lines_;
        int64_t table_id_;
        int64_t total_rows_;

        ObColumn colums_[MAX_COLUMS];
        char buf_[2][MAX_LINE_LEN];

        char row_key_buf_[2][ROW_KEY_BUF_LEN];
        char last_end_key_buf_[ROW_KEY_BUF_LEN];

        
        common::ObString row_key_;
        common::ObString last_key_;
        
      common::ObRow sstable_row_;
      common::ObRowDesc* row_desc_;
        compactsstablev2::ObCompactSSTableWriter writer_;

        const common::ObTableSchema *table_schema_;
        const common::ObSchemaManagerV2 *schema_;
        compactsstablev2::ObSSTableSchema *sstable_schema_;
      ObRowDesc* desc_;

        common::CharArena arena_;

        common::ObObj values_[MAX_LINE][MAX_COLUMS];
        common::ObObj row_value_[MAX_COLUMS];

        common::ObString last_end_key_;
        common::ObString dest_file_string_;
        common::ObString compressor_string_;
        common::ObString key_array_[MAX_LINE];
    };

  } /* chunkserver */
} /* oceanbase */
#endif /*OCEANBASE_CHUNKSERVER_DATABUILDER_H_ */
