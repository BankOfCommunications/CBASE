/*
 * =====================================================================================
 *
 *       Filename:  DbRecord.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 08:14:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OP_API_DBRECORD_H
#define  OP_API_DBRECORD_H

#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include <string>
#include <map>

namespace oceanbase {
    namespace api {
        class DbRecord;

        class DbRecord {
          public:
              typedef std::map< std::string, common::ObCellInfo> RowData;
              typedef RowData::iterator Iterator;
              DbRecord() {
                rowkey_objs_nr_ = 0;
                memset(rowkey_objs_, 0, common::OB_MAX_ROWKEY_COLUMN_NUMBER * sizeof(common::ObObj));
              }
              ~DbRecord();

              int get(const std::string &column, common::ObCellInfo **cell);
              int get(const char *column_str, common::ObCellInfo **cell);

              int get_table_id(int64_t &table_id);
              int get_rowkey(common::ObRowkey &rowkey);

              void append_column(common::ObCellInfo &cell);

              void reset();

              Iterator begin() { return row_.begin(); }
              Iterator end() { return row_.end(); }

              int serialize(common::ObDataBuffer &data_buff);

              bool empty() { return row_.empty(); }

              void dump();

          private:
              common::ObObj rowkey_objs_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
              RowData row_;
              int64_t rowkey_objs_nr_ ;
        };
    }
}

#endif
