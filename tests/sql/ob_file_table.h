
#ifndef OCEANBASE_FILE_TABLE_H_
#define OCEANBASE_FILE_TABLE_H_

#include "sql/ob_rowkey_phy_operator.h"
#include "common/ob_row.h"
#include "test_utility.h"
#include "common/ob_schema.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    namespace test
    {
      class ObFileTable : public ObRowkeyPhyOperator
      {
        public:
          ObFileTable(const char *file_name);
          virtual ~ObFileTable();

          virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
          virtual int open();
          virtual int close();
          virtual ObPhyOperatorType get_type() const { return PHY_INVALID; }
          virtual int get_next_row(const common::ObRow *&row);
          virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
          virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
          virtual int64_t to_string(char* buf, const int64_t buf_len) const;
          void reset() {}
          void reuse() {}

        public:
          uint64_t table_id_;
          int32_t row_count_;
          int32_t column_count_;
          uint64_t column_ids_[100];
          enum ObObjType column_type_[100];

          inline enum ObObjType get_column_type(uint64_t column_id)
          {
            for(int32_t i=0;i<column_count_;i++)
            {
              if(column_id == column_ids_[i])
              {
                return column_type_[i];
              }
            }
            return ObMinType;
          }

        protected:
          virtual int parse_line(const ObRow *&row);
          virtual ObRow* get_curr_row();

        protected:
          const char *file_name_;
          FILE *fp_;
          int32_t curr_count_;
          char line_[1024];
          char line_copy_[1024];
          char *tokens_[100];
          int32_t count_;
          ObRowkey cur_rowkey_;
          ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];
          ObRow curr_row_;
          ObRowDesc row_desc_;
          ObRowkeyInfo rowkey_info_;
      };
    }
  }
}

#endif /* OCEANBASE_FILE_TABLE_H_ */
