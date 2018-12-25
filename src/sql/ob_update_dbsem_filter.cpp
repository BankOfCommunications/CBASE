/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert_dbsem_filter.cpp
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#include "ob_update_dbsem_filter.h"
#include "ob_multiple_get_merge.h"
#include "common/utility.h"
#include "sql/ob_physical_plan.h"
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObUpdateDBSemFilter, PHY_UPDATE_DB_SEM_FILTER);
  }
}
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObUpdateDBSemFilter::ObUpdateDBSemFilter():could_update_(false)
    {
    }

    ObUpdateDBSemFilter::~ObUpdateDBSemFilter()
    {
    }

    void ObUpdateDBSemFilter::reset()
    {
      ObSingleChildPhyOperator::reset();
      could_update_ = false;
    }

    void ObUpdateDBSemFilter::reuse()
    {
      ObSingleChildPhyOperator::reuse();
      could_update_ = false;
    }

    int ObUpdateDBSemFilter::open()
    {
      int ret = OB_SUCCESS;
      //add lbzhong [Update rowkey] 20161223:b
      could_update_ = true;
      //add:e
      if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
      {
        if (!IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
        }
      }
      else
      {
        const ObRow *row = NULL;
        //ObRow duplicated_row;
        //ObRowDesc row_desc;
        bool is_duplicated = false;
        while(OB_SUCCESS == ret)
        {
          ret = child_op_->get_next_row(row);
          if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
          /*
          else if(OB_SUCCESS != (ret = get_duplicated_row(row, duplicated_row, row_desc)))
          {
            TBSYS_LOG(WARN, "fail to get duplicated row:ret[%d]", ret);
          }
          */
          //mod lbzhong [Update rowkey] 20161223:b
          else if (is_duplicated)
          {
            //if(NULL != row && row->is_new_row())
            //{
              //is_duplicated = true;
              char rowkey_buf[512];
              memset(rowkey_buf, 0, sizeof(rowkey_buf));
              row->rowkey_to_string(rowkey_buf, sizeof(rowkey_buf));
              TBSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", rowkey_buf);
              ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
              break;
            //}
          }
          else
          {
            is_duplicated = true;
          }
         //mod:e
        }
        if(PHY_MULTIPLE_GET_MERGE == child_op_->get_type())
        {
          ObMultipleGetMerge *fuse_op = static_cast<ObMultipleGetMerge*>(child_op_);
          fuse_op->reset_iterator();
        }
      }
      return ret;
    }

    int ObUpdateDBSemFilter::get_duplicated_row(const ObRow *&row, ObRow &duplicated_row, ObRowDesc& row_desc) const
    {
      int ret = OB_SUCCESS;
      //remove action flag column
      int64_t count = 0;
      const ObObj *cell = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;

      if(row_desc.get_column_num() == 0) // first row
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < row->get_column_num(); i ++)
        {
          if (OB_SUCCESS != (ret = row->get_row_desc()->get_tid_cid(i, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
          }
          else if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
          {
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, column_id)))
            {
              TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
          }
        }
        duplicated_row.set_row_desc(row_desc);
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < row->get_column_num(); i ++)
      {
        if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
        }
        else if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
          if (OB_SUCCESS != (ret = duplicated_row.raw_set_cell(count ++, *cell)))
          {
            TBSYS_LOG(WARN, "fail to set cell:ret[%d]", ret);
          }
        }
      }
      return ret;
    }

    int ObUpdateDBSemFilter::close()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::close()))
      {
        if (!IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "failed to close child_op, err=%d", ret);
        }
      }
      else
      {
        could_update_ = false;
      }
      return ret;
    }

    int ObUpdateDBSemFilter::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (!could_update_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op_ is NULL");
      }
      else if (OB_SUCCESS != (ret = child_op_->get_next_row(row)))
      {
        if (OB_ITER_END != ret
            && !IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
        }
      }
      return ret;
    }

    int ObUpdateDBSemFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (could_update_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op_ is NULL");
      }
      else if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get row desc, err=%d", ret);
      }
      return ret;
    }

    int64_t ObUpdateDBSemFilter::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObUpdateDBSemFilter(is_duplicated=%d)\n", could_update_);
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    PHY_OPERATOR_ASSIGN(ObUpdateDBSemFilter)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObUpdateDBSemFilter);
      reset();
      could_update_ = o_ptr->could_update_;
      return ret;
    }

    DEFINE_SERIALIZE(ObUpdateDBSemFilter)
    {
      int ret = OB_SUCCESS;
      UNUSED(buf);
      UNUSED(buf_len);
      UNUSED(pos);
      return ret;
    }

    DEFINE_DESERIALIZE(ObUpdateDBSemFilter)
    {
      int ret = OB_SUCCESS;
      UNUSED(buf);
      UNUSED(data_len);
      UNUSED(pos);
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObUpdateDBSemFilter)
    {
      return 0;
    }
  } // end namespace sql
} // end namespace oceanbase
