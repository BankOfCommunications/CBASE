#ifndef OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "sstable/ob_sstable_row.h"
#include "common/ob_schema.h"
#include "common/ob_rowkey.h"
#include "common/ob_row_desc.h"
#include "sstable_writer.h"
#include "com_taobao_mrsstable_SSTableBuilder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    struct data_format
    {
      common::ObObjType type;
      int32_t len;
      uint32_t column_id;
      int32_t index; //index in raw data, -1 means the item is new ,no data in raw data
    };

    class SSTableBuilder
    {
      public:
        struct ObColumn
        {
          char *column_;
          int32_t len_;
        };

        SSTableBuilder();
        ~SSTableBuilder();

        int init(const uint64_t param_table_id, const uint64_t schema_table_id,
            const common::ObSchemaManagerV2* schema,
            const bool is_skip_invalid_row);
        int start_builder();
        int append(const char* input, const int64_t input_size,
          bool is_first, bool is_last, bool is_include_min, bool is_include_max,
          const char** output, int64_t* output_size);

      private:
        int close_sstable();
        const char *read_line(const char* input, const int64_t input_size,
          int64_t& pos, int &fields, bool is_rowkey, bool& is_invalid_row);
        int process_line(int fields, bool is_start_key, bool is_end_key);

        int transform_date_to_time(const char *str, common::ObDateTime& val);
        int build_row_desc();
        int process_rowkey(int fields);

      private:
        int64_t current_sstable_size_;
        int64_t serialize_size_;
        int64_t param_table_id_;// the table id passed from jni, which is specified by cmd or configuration file of mrsstable.jar
        int64_t schema_table_id_; // the table id specified in data_syntax.ini and schema.ini
        int64_t total_rows_;
        bool is_skip_invalid_row_;

        ObColumn colums_[common::OB_MAX_COLUMN_NUMBER];
        common::ObMemBuf row_key_buf_;
        const common::ObRowkey* row_key_;
        common::ObMemBuf start_rowkey_buf_;
        common::ObMemBuf end_rowkey_buf_;
        common::ObNewRange range_;
        common::ObObj row_value_[common::OB_MAX_COLUMN_NUMBER];
        uint64_t column_id_[common::OB_MAX_COLUMN_NUMBER];

        common::ObString compressor_string_;
        common::ObRow row_;

        const common::ObTableSchema *table_schema_;
        const common::ObSchemaManagerV2 *schema_;
        sstable::ObSSTableSchema *sstable_schema_;
        common::ObRowDesc row_desc_;
        SSTableWriter writer_;
    };
  } /* chunkserver */
} /* oceanbase */

#ifdef __cplusplus
extern "C" {
#endif

int init(const char* schema_file, const char* syntax_file, const uint64_t table_id_in,
    bool is_skip_invalid_row);
int append(const char* input, const int64_t input_size,
  bool is_first, bool is_last, bool is_include_min, bool is_include_max,
  const char** output, int64_t* output_size);
void do_close();

#ifdef __cplusplus
}
#endif

#endif /*OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_ */
