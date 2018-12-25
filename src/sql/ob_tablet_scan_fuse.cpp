/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_scan_fuse.h"
#include "common/ob_row_fuse.h"
#include "common/utility.h"
using namespace oceanbase::sql;

ObTabletScanFuse::ObTabletScanFuse()
  :sstable_scan_(NULL),
  incremental_scan_(NULL),
  last_sstable_row_(NULL),
  last_incr_row_(NULL),
  sstable_rowkey_(NULL),
  incremental_rowkey_(NULL),
  last_rowkey_(NULL)
{
}

ObTabletScanFuse::~ObTabletScanFuse()
{
}

void ObTabletScanFuse::reset()
{
  sstable_scan_ = NULL;
  incremental_scan_ = NULL;
  last_sstable_row_ = NULL;
  last_incr_row_ = NULL;
  sstable_rowkey_ = NULL;
  incremental_rowkey_ = NULL;
  last_rowkey_ = NULL;
}

void ObTabletScanFuse::reuse()
{
  sstable_scan_ = NULL;
  incremental_scan_ = NULL;
  last_sstable_row_ = NULL;
  last_incr_row_ = NULL;
  sstable_rowkey_ = NULL;
  incremental_rowkey_ = NULL;
  last_rowkey_ = NULL;
}

bool ObTabletScanFuse::check_inner_stat()
{
  bool ret = true;
  //mod zhaoqiong [Truncate Table]:20160318:b
  //if(NULL == sstable_scan_ || NULL == incremental_scan_)
  //mod:e
  if(NULL == incremental_scan_)
  {
    ret = false;
    TBSYS_LOG(WARN, "check inner stat fail:sstable_scan_[%p], incremental_scan_[%p]", sstable_scan_, incremental_scan_);
  }
  //add zhaoqiong [Truncate Table]:20160318:b
  if(NULL == sstable_scan_)
  {
    TBSYS_LOG(DEBUG, "sstable_scan_ is null");
  }
  //add:e
  return ret;
}

int ObTabletScanFuse::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(ERROR, "not implement");
  return ret;
}

int ObTabletScanFuse::set_sstable_scan(ObSSTableScan *sstable_scan)
{
  int ret = OB_SUCCESS;
  if(NULL == sstable_scan)
  {
    TBSYS_LOG(WARN, "argument sstable_scan is null");
    sstable_scan_ = NULL; //add zhaoqiong [Truncate Table]:20160318
  }
  else
  {
    sstable_scan_ = sstable_scan;
  }
  return ret;
}

int ObTabletScanFuse::set_incremental_scan(ObUpsScan *incremental_scan)
{
  int ret = OB_SUCCESS;
  if(NULL == incremental_scan)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "argument incremental_scan is null");
  }
  else
  {
    incremental_scan_ = incremental_scan;
  }
  return ret;
}

int ObTabletScanFuse::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;

  last_sstable_row_ = NULL;
  last_incr_row_ = NULL;
  sstable_rowkey_ = NULL;
  incremental_rowkey_ = NULL;
  last_rowkey_ = NULL;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  //mod zhaoqiong [Truncate Table]:20160318:b
  //else if(OB_SUCCESS != (ret = sstable_scan_->open()))
  else if((sstable_scan_ != NULL) && (OB_SUCCESS != (ret = sstable_scan_->open())))
  //mod:e
  {
    TBSYS_LOG(WARN, "open sstable scan fail:ret[%d]", ret);
  }
  //mod zhaoqiong [Truncate Table]:20160318:b
  //else if (OB_SUCCESS != (ret = sstable_scan_->get_row_desc(row_desc) ))
  else if ((sstable_scan_ != NULL) && (OB_SUCCESS != (ret = sstable_scan_->get_row_desc(row_desc))))
  //mod:e
  {
    TBSYS_LOG(WARN, "fail to get sstable scan row desc:ret[%d]", ret);
  }
  //add zhaoqiong [Truncate Table]:20160318:b
  else if (OB_SUCCESS != (ret = incremental_scan_->get_row_desc(row_desc)))
  {
    TBSYS_LOG(WARN, "fail to get incremental scan row desc:ret[%d]", ret);
  }
  //add:e
  else
  {
    curr_row_.set_row_desc(*row_desc);
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = incremental_scan_->open()))
    {
      TBSYS_LOG(WARN, "open incremental scan fail:ret[%d]", ret);
    }
    else
    {
      last_incr_row_ = NULL;
      last_sstable_row_ = NULL;
    }
  }
  FILL_TRACE_LOG("open scan fuse op done ret =%d", ret);

  return ret;
}

int ObTabletScanFuse::close()
{
  int ret = OB_SUCCESS;

  if(NULL != sstable_scan_)
  {
    if(OB_SUCCESS != (ret = sstable_scan_->close()))
    {
      TBSYS_LOG(WARN, "close sstable scan fail:ret[%d]", ret);
    }
  }

  if(NULL != incremental_scan_)
  {
    if(OB_SUCCESS != (ret = incremental_scan_->close()))
    {
      TBSYS_LOG(WARN, "close incremental scan fail:ret[%d]", ret);
    }
  }

  return ret;
}

// 分别从sstable_scan_和incremental_scan_读取数据，按照rowkey归并排序输出
int ObTabletScanFuse::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_row_exist = true;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  do
  {
    is_row_exist = true;
    if(OB_SUCCESS == ret)
    {
      if (NULL == last_incr_row_)
      {
        const ObRow *tmp_last_incr_row = NULL;
        ret = incremental_scan_->get_next_row(incremental_rowkey_, tmp_last_incr_row);
        if(OB_SUCCESS == ret)
        {
          last_incr_row_ = dynamic_cast<const ObUpsRow *>(tmp_last_incr_row);
          if(NULL == last_incr_row_)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "incremntal scan should return ObUpsRow");
          }
        }
        else if(OB_ITER_END == ret)
        {
          last_incr_row_ = NULL;
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "incremental scan get next row fail:ret[%d]", ret);
        }
      }

      //mod zhaoqiong [Truncate Table]:20160318:b
      //if(OB_SUCCESS == ret && NULL == last_sstable_row_)
      if(OB_SUCCESS == ret && NULL == last_sstable_row_ && sstable_scan_ != NULL)
        //mod:e
      {
        ret = sstable_scan_->get_next_row(sstable_rowkey_, last_sstable_row_);
        if(OB_ITER_END == ret)
        {
          last_sstable_row_ = NULL;
          ret = OB_SUCCESS;
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "sstable scan get next row fail:ret[%d]", ret);
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(NULL != last_sstable_row_)
        {
          if(NULL != last_incr_row_)
          {
            int cmp_result = compare_rowkey(*incremental_rowkey_, *sstable_rowkey_);
            if (cmp_result < 0)
            {
              if (last_incr_row_->get_is_all_nop())
              {
                last_incr_row_ = NULL;
                is_row_exist = false;
              }
              else if(OB_SUCCESS != (ret = ObRowFuse::assign(*last_incr_row_, curr_row_)))
              {
                TBSYS_LOG(WARN, "assign last incr row to curr_row fail:ret[%d]", ret);
              }
              else
              {
                row = &curr_row_;
                last_rowkey_ = incremental_rowkey_;
                last_incr_row_ = NULL;
              }
            }
            else if (cmp_result == 0)
            {
              if(last_incr_row_->get_is_delete_row() && last_incr_row_->get_is_all_nop())
              {
                last_incr_row_ = NULL;
                last_sstable_row_ = NULL;
                is_row_exist = false;
              }
              else if(OB_SUCCESS != (ret = ObRowFuse::fuse_row(last_incr_row_, last_sstable_row_, &curr_row_)))
              {
                TBSYS_LOG(WARN, "fuse row fail:ret[%d]", ret);
              }
              else
              {
                row = &curr_row_;
                last_rowkey_ = incremental_rowkey_;
                last_incr_row_ = NULL;
                last_sstable_row_ = NULL;
              }
            }
            else
            {
              row = last_sstable_row_;
              last_rowkey_ = sstable_rowkey_;
              last_sstable_row_ = NULL;
            }
          }
          else
          {
            row = last_sstable_row_;
            last_rowkey_ = sstable_rowkey_;
            last_sstable_row_ = NULL;
          }
       }
       else
       {
         if(NULL != last_incr_row_)
         {
            if(last_incr_row_->get_is_delete_row() && last_incr_row_->get_is_all_nop())
            {
              last_incr_row_ = NULL;
              is_row_exist = false;
            }
            else if(OB_SUCCESS != (ret = ObRowFuse::assign(*last_incr_row_, curr_row_)))
            {
              TBSYS_LOG(WARN, "assign last incr row to curr_row fail:ret[%d]", ret);
            }
            else
            {
              row = &curr_row_;
              last_rowkey_ = incremental_rowkey_;
              last_incr_row_ = NULL;
            }
         }
         else
         {
           ret = OB_ITER_END;
         }
       }
     }
    }
  } while(OB_SUCCESS == ret && !is_row_exist);
  return ret;
}

int ObTabletScanFuse::compare_rowkey(const ObRowkey &rowkey1, const ObRowkey &rowkey2)
{
  return rowkey1.compare(rowkey2);
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTabletScanFuse, PHY_TABLET_SCAN_FUSE);
  }
}

int64_t ObTabletScanFuse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "TabletFuse()\n");
  if (NULL != sstable_scan_)
  {
    pos += sstable_scan_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != incremental_scan_)
  {
    pos += incremental_scan_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

int ObTabletScanFuse::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if(NULL == sstable_scan_)
  {
    //mod zhaoqiong [Truncate Table]:20160318:b
    //ret = OB_ERROR;
    //TBSYS_LOG(WARN, "sstable scan is null");
    if (OB_SUCCESS != (ret = incremental_scan_->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    //mod:e
  }
  else
  {
    if(OB_SUCCESS != (ret = sstable_scan_->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
  }
  return ret;
}


int ObTabletScanFuse::get_last_rowkey(const ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = last_rowkey_;
  return ret;
}
