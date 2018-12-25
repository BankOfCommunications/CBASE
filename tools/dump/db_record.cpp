/*
 * =====================================================================================
 *
 *       Filename:  DbRecord.cpp
 *
 *        Version:  1.0
 *        Created:  04/12/2011 11:45:59 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record.h"
#include "db_utils.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_define.h"

#include <sstream>
#include <iostream>
#include <string>

namespace oceanbase {
  namespace api {

    using namespace oceanbase::common;

    DbRecord::~DbRecord()
    {
      reset();
    }

    void DbRecord::reset()
    {
      row_.clear();
    }

    int DbRecord::get(const char *column_str, common::ObCellInfo **cell)
    {
      std::string column = column_str;
      return get(column, cell);
    }

    int DbRecord::get(const std::string &column, common::ObCellInfo **cell)
    {
      int ret = OB_SUCCESS;

      RowData::iterator itr = row_.find(column);
      if (itr == row_.end())
        ret = OB_ENTRY_NOT_EXIST;
      else
        *cell = &itr->second;

      return ret;
    }

    void DbRecord::append_column(ObCellInfo &cell)
    {
      ObRowkey &rowkey = cell.row_key_;
      const ObObj *ptr = rowkey.ptr();
      rowkey_objs_nr_ = rowkey.length();
      for (int64_t i = 0;i < rowkey.length();i++) {
        rowkey_objs_[i] = ptr[i];
      }

      std::string column(cell.column_name_.ptr(), cell.column_name_.length());
      row_.insert(std::make_pair(column, cell));

#if 0
      {
        char buf[256];
        int64_t len = cell.value_.to_string(buf, 256);
        buf[len] = 0;

        char key_buf[256];
        len = cell.row_key_.to_string(key_buf, 256);
        key_buf[len] = 0;
        TBSYS_LOG(INFO, "append_column: [%s]:[%s], rowkey=[%s]",column.c_str(), buf, key_buf);
      }
#endif
    }

    int DbRecord::get_table_id(int64_t &table_id)
    {
      int ret = OB_SUCCESS;

      if (row_.empty()) {
        ret = OB_ERROR;
      } else {
        table_id = row_.begin()->second.table_id_;
      }

      return ret;
    }

    int DbRecord::get_rowkey(common::ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      if (row_.empty()) {
        ret = OB_ERROR;
      } else {
        rowkey.assign(rowkey_objs_, rowkey_objs_nr_);
      }

      return ret;
    }


    void DbRecord::dump()
    {
      RowData::iterator itr = row_.begin();
      while (itr != row_.end()) {
        TBSYS_LOG(INFO, "%s", print_cellinfo(&itr->second, "DUMP-REC"));
        itr++;
      }
    }
  }
}
