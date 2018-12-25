/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_scan_param.cpp is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */


#include "common/utility.h"
#include "ob_sstable_scan_param.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace sstable
  {
    ObSSTableScanParam::ObSSTableScanParam()
    {
      reset();
    }
    ObSSTableScanParam::~ObSSTableScanParam()
    {
    }

    void ObSSTableScanParam::reset()
    {
      scan_flag_.flag_ = 0;
      memset(column_ids_, 0, sizeof(column_ids_));
      column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_);
    }

    bool ObSSTableScanParam::is_valid() const
    {
      int ret = true;
      if (range_.empty())
      {
        TBSYS_LOG(ERROR, "range=%s is empty, cannot find any key.", to_cstring(range_));
        ret = false;
      }
      else if (column_id_list_.get_array_index() <= 0)
      {
        TBSYS_LOG(ERROR, "query not request any columns=%ld", 
            column_id_list_.get_array_index());
        ret = false;
      }
      return ret;
    }

    int64_t ObSSTableScanParam::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      pos += snprintf(buf, buf_len, "%s", "scan range:");
      if (pos < buf_len) 
      {
        pos += range_.to_string(buf + pos, buf_len - pos);
      }

      if (pos < buf_len)
      {
        for (int64_t i = 0; i < column_id_list_.get_array_index() && pos < buf_len;  ++i)
        {
          databuff_printf(buf, buf_len, pos, "<id=%lu>,", column_ids_[i]);
        }
      }
      return pos;
    }
  }
}

