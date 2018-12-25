#ifndef _TASK_TABLE_CONF_H__
#define  _TASK_TABLE_CONF_H__

#include <vector>
#include <string>
#include "common/ob_common_param.h"
#include "common/data_buffer.h"
#include "common/ob_string.h"

#define kNullColumn  "__null_column__"

namespace oceanbase {
  namespace tools {
    using namespace common;

    class RecordDelima {
      public:
        enum RecordDelimaType {
          CHAR_DELIMA,
          SHORT_DELIMA
        };
      public:
        RecordDelima() {
          type_ = CHAR_DELIMA;
          part1_ = 1;
        }

        RecordDelima(char delima) {
          part1_ = delima;
          type_ = CHAR_DELIMA;
        }

        RecordDelima(char part1, char part2) {
          part1_ = part1;
          part2_ = part2;
          type_ = SHORT_DELIMA;
        }

        void set_short_delima(char part1, char part2) {
          part1_ = part1;
          part2_ = part2;
          type_ = SHORT_DELIMA;
        }

        void set_char_delima(char delima) {
          part1_ = delima;
          type_ = CHAR_DELIMA;
        }

        bool is_delima(const char *buff, int64_t pos, int64_t buff_size) const;

        RecordDelimaType delima_type() const { return type_; }

        void skip_delima(int64_t &pos) const { pos += type_ == CHAR_DELIMA ? 1 : 2; }

        void append_delima(char *buff, int64_t pos, int64_t buff_size) const {
          if (type_ == CHAR_DELIMA && pos < buff_size) {
            buff[pos] = part1_;
          } else if (type_ == SHORT_DELIMA && (pos + 1) < buff_size) {
            buff[pos] = part1_;
            buff[pos + 1] = part2_;
          }
        }

        int length() const { return type_ == CHAR_DELIMA ? 1 : 2; }

      public:
        unsigned char part1_;
        unsigned char part2_;
        RecordDelimaType type_;
    };

    class TableConf;
    class RowkeyWrapper {
      public:
        struct RowkeyItem;
        friend class TableConf;

        typedef std::vector<RowkeyItem>::const_iterator Iterator;
      public:
        enum RowkeyItemType {
          NONE = -1,
          INT8,
          INT16,
          INT32,
          INT64,
          STR,
          VSTR
        };
      public:
        //struct descripe a element of rowkey
        struct RowkeyItem {
          RowkeyItem() : start_pos(0), item_len(0) { }

          //parse RowkeyItem from string
          int parse_from_string(const char *input_str);

          //convert type string to RowkeyItemType
          RowkeyItemType str_to_type(const char *str);

          int start_pos;
          int item_len;
          std::string name;
          RowkeyItemType type;
        };

        //fill items for conf
        int load(const std::vector<const char*> &conf);

        //encode rowkey to ObDataBuffer
        int encode(const ObString &rowkey, ObDataBuffer &buff, const RecordDelima &delima) const;

        Iterator begin() const { return items_.begin(); }
        Iterator end() const { return items_.end(); }

      private:
        int append_rowkey_item(const ObString &key, const RowkeyItem &item, 
                               ObDataBuffer &buffer) const;

        //all items that makeup rowkey
        std::vector<RowkeyItem> items_;
    };

    class TableConf {
      public:
        typedef std::vector<const char *>::const_iterator ColumnIterator;
      public:
        TableConf() : valid_(false), delima_(1), rec_delima_(10) { }

        TableConf(const std::vector<const char *> &revise_columns, 
                  const std::vector<const char *> &rowkey_items, 
                  const std::vector<const char *> &columns,
                  const char *table_name,
                  int64_t table_id);

        //is revise column int --> price
        bool is_revise_column(const ObString &col) const;

        //revise column int --> price
        int revise_column(const ObCellInfo &cell, ObDataBuffer &data) const;

        bool is_rowkey(const char *column) const;

        //encode rowkey to ObDataBuffer
        int encode_rowkey(const ObString &rowkey, ObDataBuffer &data) const;

        //whether the config is valid
        bool valid() const { return valid_; }

        int64_t table_id() const { return table_id_; }

        const std::string &table_name() const { return table_name_; }

        std::string DebugString() const;

        void set_output_path(const char *path) { if (path) output_path_ = path; }

        const std::string &output_path() const { return output_path_; }

        bool is_null_column(const char *column) const { return strcmp(kNullColumn, column) == 0; }

        ColumnIterator column_begin() const { return columns_.begin(); }

        ColumnIterator column_end() const { return columns_.end(); }

        int append_rowkey_item(const ObString &key, const char *rk_item_name, 
                               ObDataBuffer & buffer) const;

        static int loadConf(const char *table_name, TableConf &conf);

        void set_delima(const RecordDelima &delima) { delima_ = delima; }

        void set_rec_delima(const RecordDelima &rec_delima) { rec_delima_ = rec_delima; }

        const RecordDelima& delima() const { return delima_; }

        const RecordDelima& rec_delima() const { return rec_delima_; }
      private:
        std::vector<const char *> revise_cols_;
        std::vector<const char *> columns_;

        RowkeyWrapper rowkey_wrappper_;
        int64_t table_id_;
        bool valid_;
        std::string output_path_;
        std::string table_name_;

        RecordDelima delima_;
        RecordDelima rec_delima_;
    };
  }
}

#endif
