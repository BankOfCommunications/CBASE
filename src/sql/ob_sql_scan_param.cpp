#include "common/ob_action_flag.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "ob_sql_scan_param.h"

namespace oceanbase
{
  namespace sql
  {
    const char * SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX = "__WHERE_";

    void ObSqlScanParam::reset(void)
    {
      ObSqlReadParam::reset();
      reset_local();
    }

    void ObSqlScanParam::reset_local(void)
    {
      range_.table_id_ = OB_INVALID_ID;
      range_.start_key_.assign(NULL, 0);
      range_.end_key_.assign(NULL, 0);
      buffer_pool_.reset();
      scan_flag_.flag_ = 0;
      version_ = OB_INVALID_VERSION; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170320
    }

    // ObSqlScanParam
    ObSqlScanParam::ObSqlScanParam() :
      buffer_pool_(ObModIds::OB_SQL_SCAN_PARAM),
      range_(), scan_flag_()
    {
        version_ = OB_INVALID_VERSION; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170320
    }

    ObSqlScanParam::~ObSqlScanParam()
    {
    }

    int ObSqlScanParam::set_range(const ObNewRange& range, bool deep_copy_args)
    {
      int err = OB_SUCCESS;
      range_ = range;
      if (deep_copy_args)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.start_key_,&(range_.start_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.start_key_ to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.end_key_,&(range_.end_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.end_key_ to local buffer [err:%d]", err);
        }
      }
      return err;
    }

    // BASIC_PARAM_FIELD
    int ObSqlScanParam::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = set_ext_obj_value(buf, buf_len, pos, ObActionFlag::BASIC_PARAM_FIELD)))
      {
        TBSYS_LOG(WARN, "fail to serialize BASIC_PARAM_FIELD. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, range_.table_id_)))
      {
        TBSYS_LOG(WARN, "fail to serialize table_id_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, range_.border_flag_.get_data())))
      {
        TBSYS_LOG(WARN, "fail to serialize border_flag_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_rowkey_obj_array(buf, buf_len, pos,
              range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt())))
      {
        TBSYS_LOG(WARN, "fail to serialize start key. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_rowkey_obj_array(buf, buf_len, pos,
              range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt())))
      {
        TBSYS_LOG(WARN, "fail to serialize end key. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, scan_flag_.flag_)))
      {
        TBSYS_LOG(WARN, "fail to serialize read_mode_ obj. ret=%d", ret);
      }
      //add zhujun [transaction read uncommit]2016/3/24
      else if (OB_SUCCESS != (ret = trans_id_.serialize(buf,buf_len,pos)))
      {
        TBSYS_LOG(WARN, "fail to serialize trans_id. ret=%d", ret);
      }
      //add:e
      //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, version_)))
      {
        TBSYS_LOG(WARN, "fail to serialize tablet_version. ret=%d", ret);
      }
      //add:e
      return ret;
    }

    int ObSqlScanParam::deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t table_id = 0;
      int64_t border_flag = 0;
      ObObj start_rowkey_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj end_rowkey_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t start_key_len = OB_MAX_ROWKEY_COLUMN_NUMBER;
      int64_t end_key_len = OB_MAX_ROWKEY_COLUMN_NUMBER;
      int64_t scan_flag = 0;
      ObNewRange tmp_range;
      common::ObTransID trans_id;//add by zhujun
      int64_t version; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
      // deserialize scan range
      // tid
      if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, table_id)))
      {
        TBSYS_LOG(WARN, "fail to deserialize table_id_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, border_flag)))
      {
        TBSYS_LOG(WARN, "fail to deserialize border_flag_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos,
              start_rowkey_obj_array, start_key_len)))
      {
        TBSYS_LOG(WARN, "fail to get start rowkey.ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos,
              end_rowkey_obj_array, end_key_len)))
      {
        TBSYS_LOG(WARN, "fail to get end rowkey. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, scan_flag)))
      {
        TBSYS_LOG(WARN, "fail to deserialize border_flag_ obj. ret=%d", ret);
      }
      //add zhujun [transaction read uncommit]2016/3/24
      else if (OB_SUCCESS != (ret = trans_id.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize trans_id. ret=%d", ret);
      }
      //add:e
      //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
      else if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, version)))
      {
        TBSYS_LOG(WARN, "fail to serialize tablet_version. ret=%d", ret);
      }
      //add:e
      else
      {
        tmp_range.table_id_ = table_id;
        tmp_range.border_flag_.set_data(static_cast<int8_t>(border_flag));
        tmp_range.start_key_.assign(start_rowkey_obj_array, start_key_len);
        tmp_range.end_key_.assign(end_rowkey_obj_array, end_key_len);
        scan_flag_.flag_ = scan_flag;
        trans_id_ = trans_id;
        version_ = version; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
        ret = set_range(tmp_range, true);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deep_copy range:%s, ret:%d",
              to_cstring(tmp_range), ret);
        }
        //add huangcc [fix transaction read uncommit bug]2016/11/21
        if (trans_id_.is_valid())
        {
           is_read_master_ = true;
        }
        //add end
      }

      return ret;
    }
    ObTransID ObSqlScanParam::get_trans_id() const
    {
        return trans_id_;
    }

    int64_t ObSqlScanParam::get_basic_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // BASIC_PARAM_FIELD
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      // scan range
      obj.set_int(range_.table_id_);
      total_size += obj.get_serialize_size();
      obj.set_int(range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      // start_key_
      total_size += get_rowkey_obj_array_size(
          range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt());
      // end_key_
      total_size += get_rowkey_obj_array_size(
          range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt());
      obj.set_int(scan_flag_.flag_);
      total_size += obj.get_serialize_size();

      //add zhujun [transaction read uncommit]2016/3/24
      total_size += trans_id_.get_serialize_size();
      //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
      obj.set_int(version_);
      total_size += obj.get_serialize_size();
      //add:e
      return total_size;
    }

    int64_t ObSqlScanParam::get_end_param_serialize_size(void) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.get_serialize_size();
    }

    DEFINE_SERIALIZE(ObSqlScanParam)
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      // read param info
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObSqlReadParam::serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize ObSqlReadParam. ret=%d", ret);
        }
      }
      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      // END_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialize_end_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize end param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlScanParam)
    {
      // reset contents
      reset();
      ObObj obj;
      int ret = OB_SUCCESS;

      ret = ObSqlReadParam::deserialize(buf, data_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to deserialize ObSqlReadParam. ret=%d", ret);
      }
      while (OB_SUCCESS == ret)
      {
        do
        {
          if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to deserialize meta type. ret=%d. data_len=%ld,pos=%ld", ret, data_len, pos);
          }
        } while (OB_SUCCESS == ret && ObExtendType != obj.get_type());

        if (OB_SUCCESS == ret && ObActionFlag::END_PARAM_FIELD != obj.get_ext())
        {
          switch (obj.get_ext())
          {
            case ObActionFlag::BASIC_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = deserialize_basic_param(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize basic param. ret=%d", ret);
                }
                break;
              }
            default:
              {
                // deserialize next cell
                // ret = obj.deserialize(buf, data_len, pos);
                break;
              }
          }
        }
        else
        {
          break;
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlScanParam)
    {
      int64_t total_size = ObSqlReadParam::get_serialize_size();
      total_size += get_basic_param_serialize_size();
      total_size += get_end_param_serialize_size();
      return total_size;
    }

    void ObSqlScanParam::dump(void) const
    {
    }

    int64_t ObSqlScanParam::to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      pos += ObSqlReadParam::to_string(buf+pos, buf_len-pos);
      //pos += range_.to_string(buf+pos, buf_len-pos);
      return pos;
    }

    int ObSqlScanParam::assign(const ObSqlReadParam* other)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObSqlScanParam);
      reset();
      if ((ret = ObSqlReadParam::assign(other)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign ObSqlReadParam failed, ret=%d", ret);
      }
      else
      {
        deep_copy_args_ = o_ptr->deep_copy_args_;
        scan_flag_ = o_ptr->scan_flag_;
        version_ = o_ptr->version_; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
        if ((ret = set_range(o_ptr->range_, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add ObRowkey failed, ret=%d", ret);
        }
      }
      return ret;
    }
  } /* sql */
} /* oceanbase */
