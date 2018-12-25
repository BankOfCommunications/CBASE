/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "common/ob_define.h"
#include "ob_inc_scan.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"
#include "common/ob_packet.h"


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObIncScan, PHY_INC_SCAN);
  }
}
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObGetParamPool& get_get_param_pool()
    {
      static ObGetParamPool get_param_pool;
      return get_param_pool;
    }

    ObScanParamPool& get_scan_param_pool()
    {
      static ObScanParamPool scan_param_pool;
      return scan_param_pool;
    }

    ObIncScan::ObIncScan()
      : lock_flag_(LF_NONE), scan_type_(ST_NONE),
        get_param_guard_(get_get_param_pool()),
        scan_param_guard_(get_scan_param_pool()),
        get_param_(NULL),
        scan_param_(NULL),
        values_subquery_id_(common::OB_INVALID_ID),
        cons_get_param_with_rowkey_(false),
        hotspot_(false)
       //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        ,data_mark_param_()
       //add duyr 20160531:e
    {
    }

    ObIncScan::~ObIncScan()
    {
    }

    void ObIncScan::reset()
    {
      lock_flag_ = LF_NONE;
      scan_type_ = ST_NONE;
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = common::OB_INVALID_ID;
      cons_get_param_with_rowkey_ = false;
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      //add duyr 20160531:e
    }

    void ObIncScan::reuse()
    {
      lock_flag_ = LF_NONE;
      scan_type_ = ST_NONE;
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = common::OB_INVALID_ID;
      cons_get_param_with_rowkey_ = false;
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      //add duyr 20160531:e
    }

    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    int ObIncScan::set_data_mark_param(const ObDataMarkParam &param)
    {
        int ret = OB_SUCCESS;
        if (!param.is_valid())
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(ERROR,"invalid data mark param[%s]!ret=%d",to_cstring(param),ret);
        }
        else
        {
          data_mark_param_ = param;
        }
        return ret;
    }

    const ObDataMarkParam& ObIncScan::get_data_mark_param()const
    {
        return data_mark_param_;
    }

    int ObIncScan::fill_scan_param_with_data_mark_param()const
    {
        int ret = OB_SUCCESS;
        if (NULL == scan_param_)
        {
            ret = OB_NOT_INIT;
            TBSYS_LOG(ERROR,"scan param can't be NULL!ret=%d",ret);
        }
        else
        {
            scan_param_->set_data_mark_param(data_mark_param_);
            if (OB_SUCCESS == ret && data_mark_param_.need_modify_time_
                && OB_SUCCESS != (ret = scan_param_->add_column(data_mark_param_.modify_time_cid_)))
            {
                TBSYS_LOG(WARN,"fail to add modify time cid!ret=%d",ret);
            }
            else if (data_mark_param_.need_major_version_
                     && OB_SUCCESS != (ret = scan_param_->add_column(data_mark_param_.major_version_cid_)))
            {
                TBSYS_LOG(WARN,"fail to add major ver cid!ret=%d",ret);
            }
            else if (data_mark_param_.need_minor_version_
                     && OB_SUCCESS != (ret = scan_param_->add_column(data_mark_param_.minor_ver_start_cid_)))
            {
                TBSYS_LOG(WARN,"fail to add minor_ver_start_cid_!ret=%d",ret);
            }
            else if (data_mark_param_.need_minor_version_
                     && OB_SUCCESS != (ret = scan_param_->add_column(data_mark_param_.minor_ver_end_cid_)))
            {
                TBSYS_LOG(WARN,"fail to add minor_ver_end_cid_!ret=%d",ret);
            }
            else if (data_mark_param_.need_data_store_type_
                     && OB_SUCCESS != (ret = scan_param_->add_column(data_mark_param_.data_store_type_cid_)))
            {
                TBSYS_LOG(WARN,"fail to add data store type cid!ret=%d",ret);
            }
        }

        return ret;
    }
    //add duyr 20160531:e

    ObGetParam* ObIncScan::get_get_param()
    {
      ObGetParam* get_param = (get_param_?: get_param_ = get_get_param_pool().get(get_param_guard_));
      if (NULL != get_param)
      {
        get_param->set_deep_copy_args(true);
      }
      return get_param;
    }

    ObScanParam* ObIncScan::get_scan_param()
    {
      return scan_param_?: scan_param_ = get_scan_param_pool().get(scan_param_guard_);
    }

    int ObIncScan::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, lock_flag_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_bool(buf, buf_len, new_pos, hotspot_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, scan_type_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (ST_MGET == scan_type_)
      {
        if (OB_UNLIKELY(common::OB_INVALID_ID == values_subquery_id_))
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "values is invalid");
        }
        else if (NULL == get_param_)
        {
          if (NULL == const_cast<ObIncScan*>(this)->get_get_param())
          {
            TBSYS_LOG(WARN, "failed to get get_param");
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
        if (OB_LIKELY(OB_SUCCESS == err))
        {
          get_param_->reset(true);
          if (OB_SUCCESS != (err = const_cast<ObIncScan*>(this)->create_get_param_from_values(get_param_)))
          {
            TBSYS_LOG(WARN, "failed to create get param, err=%d", err);
          }
          else
          {
            ObVersionRange version_range;
            version_range.border_flag_.set_inclusive_start();
            version_range.border_flag_.set_max_value();
            version_range.start_version_ = my_phy_plan_->get_curr_frozen_version() + 1;
            //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
            if (data_mark_param_.is_valid() && my_phy_plan_->get_curr_frozen_version()>=2)
            {
                // add by maosy  [Delete_Update_Function_isolation] for fix not master 20161010:b
                // version_range.start_version_ = my_phy_plan_->get_curr_frozen_version();
                version_range.start_version_ = my_phy_plan_->get_curr_frozen_version()-1;
                version_range.border_flag_.unset_inclusive_start();
                // add maosy 20161010:e
            }
            //add duyr 20160531:e
            get_param_->set_version_range(version_range);
            //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
            get_param_->set_data_mark_param(data_mark_param_);
            //add duyr 20160531:e
            TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(*get_param_));
            FILL_TRACE_LOG("inc_version=%s", to_cstring(version_range));
            if (OB_SUCCESS != (err = get_param_->serialize(buf, buf_len, new_pos)))
            {
              TBSYS_LOG(ERROR, "get_param.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
            }
            else
            {
              get_param_->reset(true);
            }
          }
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL == scan_param_)
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "scan_param == NULL");
        }
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        else if (OB_SUCCESS != (err = fill_scan_param_with_data_mark_param()))
        {
          TBSYS_LOG(WARN,"fail to fill scan param with data mark param!ret=%d",err);
        }
        //add duyr 20160531:e
        else if (OB_SUCCESS != (err = scan_param_->serialize(buf, buf_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "scan_param.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        pos = new_pos;
      }
      return err;
    }

    int ObIncScan::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&lock_flag_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_bool(buf, data_len, new_pos, &hotspot_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&scan_type_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (ST_MGET == scan_type_)
      {
        if (NULL == get_get_param())
        {
          err = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "get_param == NULL");
        }
        else if (OB_SUCCESS != (err = get_param_->deserialize(buf, data_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "get_param.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL == get_scan_param())
        {
          err = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "get_param == NULL");
        }
        else if (OB_SUCCESS != (err = scan_param_->deserialize(buf, data_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "scan_param.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObIncScan::get_serialize_size() const
    {
      int64_t size = serialization::encoded_length_i32(lock_flag_) + serialization::encoded_length_i32(scan_type_);
      if (ST_MGET == scan_type_)
      {
        if (NULL != get_param_)
        {
          size += get_param_->get_serialize_size();
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL != scan_param_)
        {
          size += scan_param_->get_serialize_size();
        }
      }
      return size;
    }

    int ObIncScan::create_get_param_from_values(common::ObGetParam* get_param)
    {
      int ret = OB_SUCCESS;
      ObExprValues *values = NULL;
      if (common::OB_INVALID_ID == values_subquery_id_)
      {
        TBSYS_LOG(WARN, "values is invalid");
        ret = OB_NOT_INIT;
      }
      else if (NULL == (values = dynamic_cast<ObExprValues*>(my_phy_plan_->get_phy_query_by_id(values_subquery_id_))))
      {
        TBSYS_LOG(ERROR, "subquery not exist");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = values->open()))
      {
        TBSYS_LOG(WARN, "failed to open values, err=%d", ret);
      }
      else
      {
        const ObRow *row = NULL;
        const ObRowkey *rowkey = NULL;
        ObCellInfo cell_info;
        const common::ObObj *cell = NULL;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        uint64_t request_sign = 0;
        while (OB_SUCCESS == ret)
        {
          ret = values->get_next_row(row);
          if (OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = row->get_rowkey(rowkey)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey, err=%d", ret);
            break;
          }
          else
          {
            int64_t cell_num = cons_get_param_with_rowkey_ ? rowkey->length() : row->get_column_num();
            for (int64_t i = 0; i < cell_num; ++i)
            {
              if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, tid, cid)))
              {
                TBSYS_LOG(WARN, "failed to get cell, err=%d i=%ld", ret, i);
                break;
              }
              else
              {
                cell_info.row_key_ = *rowkey;
                cell_info.table_id_ = tid;
                cell_info.column_id_ = cid;
                //add lbzhong [Update rowkey] 20160421:b
                if(i >= rowkey->length() && ObInt32Type == cell->get_type())
                {
                  int32_t int_val = 0;
                  cell->get_int32(int_val);
                  if(ObActionFlag::OP_NEW_ROW == int_val)
                  {
                    cell_info.is_new_row_ = true;
                  }
                  else
                  {
                    cell_info.is_new_row_ = false;
                  }
                }
                else
                {
                  cell_info.is_new_row_ = false;
                }
                //add:e
                if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                {
                  TBSYS_LOG(WARN, "failed to add cell into get param, err=%d", ret);
                  break;
                }
                //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
                if (OB_SUCCESS == ret && cell_num - 1 == i && data_mark_param_.is_valid())
                {//finish add real cid,begin and data mark cids!
                    //first cid
                    if (OB_SUCCESS == ret && data_mark_param_.need_modify_time_)
                    {
                        //cell_info.row_key_ = *rowkey;
                        cell_info.table_id_ = data_mark_param_.table_id_;
                        cell_info.column_id_ = data_mark_param_.modify_time_cid_;
                        if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                        {
                          TBSYS_LOG(WARN, "failed to add modify_time_cid into get param, err=%d", ret);
                        }
                    }

                    //second cid
                    if (OB_SUCCESS == ret && data_mark_param_.need_major_version_)
                    {
                        //cell_info.row_key_ = *rowkey;
                        cell_info.table_id_ = data_mark_param_.table_id_;
                        cell_info.column_id_ = data_mark_param_.major_version_cid_;
                        if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                        {
                          TBSYS_LOG(WARN, "failed to add major_version_cid_ into get param, err=%d", ret);
                        }
                    }

                    //third cid
                    if (OB_SUCCESS == ret && data_mark_param_.need_minor_version_)
                    {
                        //cell_info.row_key_ = *rowkey;
                        cell_info.table_id_ = data_mark_param_.table_id_;
                        cell_info.column_id_ = data_mark_param_.minor_ver_start_cid_;
                        if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                        {
                          TBSYS_LOG(WARN, "failed to add minor_ver_start_cid_ into get param, err=%d", ret);
                        }
                    }

                    //fourth cid
                    if (OB_SUCCESS == ret && data_mark_param_.need_minor_version_)
                    {
                        //cell_info.row_key_ = *rowkey;
                        cell_info.table_id_ = data_mark_param_.table_id_;
                        cell_info.column_id_ = data_mark_param_.minor_ver_end_cid_;
                        if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                        {
                          TBSYS_LOG(WARN, "failed to add minor_ver_end_cid_ into get param, err=%d", ret);
                        }
                    }

                    //fifth cid
                    if (OB_SUCCESS == ret && data_mark_param_.need_data_store_type_)
                    {
                        //cell_info.row_key_ = *rowkey;
                        cell_info.table_id_ = data_mark_param_.table_id_;
                        cell_info.column_id_ = data_mark_param_.data_store_type_cid_;
                        if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                        {
                          TBSYS_LOG(WARN, "failed to add data_store_type_cid_ into get param, err=%d", ret);
                        }
                    }
                }
                //add duyr 20160531:e
              }
            } // end for
            if (0 == request_sign
                && hotspot_)
            {
              request_sign = rowkey->murmurhash2((uint32_t)tid);
            }
          }
        } // end while
        if (hotspot_)
        {
          ObPacket::tsi_req_sign() = request_sign;
        }
      }
      return ret;
    }

    PHY_OPERATOR_ASSIGN(ObIncScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObIncScan);
      lock_flag_ = o_ptr->lock_flag_;
      scan_type_ = o_ptr->scan_type_;
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_ = o_ptr->data_mark_param_;
      //add duyr 20160531:e
      scan_param_guard_.reset();
      get_param_guard_.reset();
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = o_ptr->values_subquery_id_;
      cons_get_param_with_rowkey_ = o_ptr->cons_get_param_with_rowkey_;
      hotspot_ = o_ptr->hotspot_;
      return ret;
    }
  }; // end namespace sql
}; // end namespace oceanbase
