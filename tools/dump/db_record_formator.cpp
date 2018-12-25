/*
 * =====================================================================================
 *
 *       Filename:  db_record_formator.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/17/2011 01:07:03 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record_formator.h"
#include "db_dumper_config.h"
#include "db_table_info.h"
#include "db_utils.h"
#include <algorithm>

using namespace std;
using namespace oceanbase::common;

enum RowkeyItemType {
  ITEM_INT8 = 1,
  ITEM_INT64
};

const char *kNullFiller = "__output_null";

//header: timestamp:seqapp_nametable_nameoprowkey
int UniqFormatorHeader::append_header(const ObRowkey &rowkey, int op, uint64_t timestamp,
                                      int64_t seq, const string &app_name,
                                      const string &table_name, ObDataBuffer &buffer)
{
  static __thread char bin_rowkey_buffer[k2M];
  int len = 0;
  int ret = OB_SUCCESS;

  int cap = static_cast<int32_t>(buffer.get_remain());
  char *data = buffer.get_data() + buffer.get_position();

  char delima = DUMP_CONFIG->get_header_delima();
  const char *op_str = get_op_string(op);

  //format : timestamp:seqapp_nametable_nameop
  len = snprintf(data, cap, "%ld:%ld%c%s:%s%c%s%c0:", 
                 timestamp, seq, delima, app_name.c_str(), 
                 table_name.c_str(), delima, op_str, delima);
  if (len <= 0) {
    TBSYS_LOG(ERROR, "buffer size not enough, buff_size=%d", cap);
    ret = OB_ERROR;
  } else {
    buffer.get_position() += len;
  }
  
  //rowkey
  int64_t bin_rowkey_len = 0;
  if (ret == OB_SUCCESS) {
    if ((ret = rowkey.serialize(bin_rowkey_buffer, k2M, bin_rowkey_len)) != OB_SUCCESS) {
      TBSYS_LOG(WARN, "large rowkey exceed header format buffer[%d]", k2M);
    }
  }

  if (ret == OB_SUCCESS) {
    data = buffer.get_data() + buffer.get_position();
    cap = static_cast<int32_t>(buffer.get_remain());

    len = hex_to_str(bin_rowkey_buffer, bin_rowkey_len, data, cap);
    if (len == 0) {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "no enough memory to append rowkey");
    } else {
      buffer.get_position() += 2 * len;
    }
  }

  return ret;
}


int append_int64(int64_t value, ObDataBuffer &buffer)
{
  int ret = OB_SUCCESS;

  char *data = buffer.get_data() + buffer.get_position();
  int cap = static_cast<int32_t>(buffer.get_remain());

  int len = snprintf(data, cap, "%ld", value);
  if (len > 0) {
    buffer.get_position() += len;
  } else {
    TBSYS_LOG(ERROR, "append_int64 error");
    ret = OB_ERROR;
  }

  return ret;
}

int append_double(double value, ObDataBuffer &buffer)
{
  int ret = OB_SUCCESS;

  char *data = buffer.get_data() + buffer.get_position();
  int cap = static_cast<int32_t>(buffer.get_remain());

  int len = snprintf(data, cap, "%f", value);
  if (len > 0) {
    buffer.get_position() += len;
  } else {
    TBSYS_LOG(ERROR, "append_double error");
    ret = OB_ERROR;
  }

  return ret;
}

int append_int8(int8_t value, ObDataBuffer &buffer)
{
  int ret = OB_SUCCESS;
  char *data = buffer.get_data() + buffer.get_position();
  int cap = static_cast<int32_t>(buffer.get_remain());

  int len = snprintf(data, cap, "%d", value);
  if (len > 0) {
    buffer.get_position() += len;
  } else {
    ret = OB_ERROR;
  }

  return ret;
}

int append_rowkey_item(const ObRowkey &key, const DbTableConfig::RowkeyItem &item, 
                       ObDataBuffer &buffer)
{
  int ret = OB_SUCCESS;
  int len = append_obj(key.ptr()[item.info_idx], buffer);
  if (len < 0) {
    ret = OB_ERROR;
  }

  return ret;
}

//when appending a deleted column, returns OB_ENTRY_NOT_EXIST
//when appending successfully, returns OB_SUCCESS,
//else return OB_ERROR
int append_item(const DbTableConfig *cfg, const string &column, DbRecord *rec, ObDataBuffer &buffer)
{
  int ret;
  ObCellInfo *cell;

  ret = rec->get(column, &cell);
  if (ret == OB_SUCCESS) {
    //only support int --> double price
    if (cfg->is_revise_column(column) && cell->value_.get_type() == ObIntType) {
      int64_t tmp;

      //convert int64 to price ,price = int64 / 1000
      ret = cell->value_.get_int(tmp);
      if (ret == OB_SUCCESS) {
        double value = (double)tmp / 100;

        ret = append_double(value, buffer);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "append_double error, column:%s", column.c_str());
        }
      } else {
        TBSYS_LOG(ERROR, "obcellinfo get value failed");
      }
    } else {
      int len = serialize_cell(cell, buffer);
      if (len >= 0)
        ret = OB_SUCCESS;
      else {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "append item error,column=%s", column.c_str());
      }
    }
  }

  return ret;
}

int append(const DbTableConfig *cfg, const string &column, DbRecord *rec,
                             const ObRowkey &rowkey, ObDataBuffer &buffer)
{
  bool is_rowkey = false;
  DbTableConfig::RowkeyItem item(column);
  int ret = OB_SUCCESS;
  is_rowkey = cfg->get_is_rowkey_column(item);

  if (ret == OB_SUCCESS && is_rowkey) {
    ret = append_rowkey_item(rowkey, item, buffer);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "append_rowkey_item error");
    }
  } else if (ret == OB_SUCCESS) {
    ret = append_item(cfg, column, rec, buffer);
    if (ret != OB_SUCCESS && ret != OB_ENTRY_NOT_EXIST)
      TBSYS_LOG(ERROR, "append_item error");
    else {
#ifdef DUMP_DEBUG
      if (ret == OB_ENTRY_NOT_EXIST) {
        char buf[256];
        int len = hex_to_str(rowkey.ptr(), rowkey.length(), buf, 256);
        buf[2 * len] = 0;
        TBSYS_LOG(ERROR, "can't get column:%s, table:%s, rowkey:%s", column.c_str(), cfg->table().c_str(), buf);
//        rec->dump();
      }
#endif
//      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int DbRecordFormator::format_record(int64_t table_id, DbRecord *record, 
                                    const ObRowkey &rowkey, ObDataBuffer &buffer)
{
  const DbTableConfig *cfg;

  int ret = DUMP_CONFIG->get_table_config(table_id, cfg);
  if (ret == OB_SUCCESS) {
    const vector<string> &output_columns = cfg->output_columns();
    for(size_t i = 0;i < output_columns.size(); i++) {
      if (output_columns[i].compare(kNullFiller) == 0) {
        //output null
      } else {
        ret = append(cfg, output_columns[i], record, rowkey, buffer);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "append column:%s failed", output_columns[i].c_str());
          break;
        }
      }

      ret = append_delima(buffer);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "append deliam failed");
        break;
      }
    }

    if (ret == 0) {
      ret = append_end_rec(buffer);
      if (ret > 0)                              /* wrap return value */
        ret = 0;
    }

  } else {
    TBSYS_LOG(ERROR, "DbRecordFormator can't find table id [%lu]", table_id);
  }

  return ret;
}
