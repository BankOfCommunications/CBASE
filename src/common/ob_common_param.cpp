#include "ob_object.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"
#include "ob_schema.h"
#include "ob_rowkey_helper.h"
//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
#include "ob_ups_row.h"
#include "ob_row_fuse.h"
//add duyr 20160531:e

namespace oceanbase
{
  namespace common
  {
    bool ObCellInfo::operator == (const ObCellInfo & other) const
    {
      return ((table_name_ == other.table_name_) && (table_id_ == other.table_id_)
          && (row_key_ == other.row_key_) && (column_name_ == other.column_name_)
          && (column_id_ == other.column_id_) && (value_ == other.value_));
    }

    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    ObDataMarkParam::ObDataMarkParam(const ObDataMarkParam &other)
    {
        reset();
        need_modify_time_   = other.need_modify_time_;
        need_major_version_ = other.need_major_version_;
        need_minor_version_ = other.need_minor_version_;
        need_data_store_type_ = other.need_data_store_type_;
        modify_time_cid_    = other.modify_time_cid_;
        major_version_cid_  = other.major_version_cid_;
        minor_ver_start_cid_= other.minor_ver_start_cid_;
        minor_ver_end_cid_  = other.minor_ver_end_cid_;
        data_store_type_cid_= other.data_store_type_cid_;
        table_id_           = other.table_id_;
    }

    ObDataMarkParam& ObDataMarkParam::operator =(const ObDataMarkParam &other)
    {
        if (this != &other)
        {
            reset();
            need_modify_time_   = other.need_modify_time_;
            need_major_version_ = other.need_major_version_;
            need_minor_version_ = other.need_minor_version_;
            need_data_store_type_ = other.need_data_store_type_;
            modify_time_cid_    = other.modify_time_cid_;
            major_version_cid_  = other.major_version_cid_;
            minor_ver_start_cid_= other.minor_ver_start_cid_;
            minor_ver_end_cid_  = other.minor_ver_end_cid_;
            data_store_type_cid_= other.data_store_type_cid_;
            table_id_           = other.table_id_;
        }
        return *this;
    }

    void ObDataMarkParam::reset()
    {
       need_modify_time_   = false;
       need_major_version_ = false;
       need_minor_version_ = false;
       need_data_store_type_ = false;
       modify_time_cid_    = 0;
       major_version_cid_  = 0;
       minor_ver_start_cid_= 0;
       minor_ver_end_cid_  = 0;
       data_store_type_cid_= 0;
       table_id_           = OB_INVALID_ID;
    }

    bool ObDataMarkParam::is_inited()const
    {
        bool bret = false;
        if (need_modify_time_
            || need_major_version_
            || need_minor_version_
            || need_data_store_type_)
        {
            bret = true;
        }
        return bret;
    }

    bool ObDataMarkParam::is_valid()const
    {
      bool bret = false;
      if (!is_inited())
      {
      }
      else if (OB_INVALID_ID == table_id_
               || table_id_ <= 0)
      {
          TBSYS_LOG(WARN,"invalid table id=%ld",table_id_);
      }
      else if (need_modify_time_
               && (modify_time_cid_ <= 0
                   || OB_INVALID_ID == modify_time_cid_))
      {
          TBSYS_LOG(WARN,"invalid modify time cid[%ld]!",modify_time_cid_);
      }
      else if (need_major_version_
               && (major_version_cid_ <= 0
                   || OB_INVALID_ID == major_version_cid_))
      {
          TBSYS_LOG(WARN,"invalid major version cid[%ld]!",major_version_cid_);
      }
      else if (need_minor_version_
               && (minor_ver_start_cid_ <= 0
                   || minor_ver_end_cid_ <= 0
                   || OB_INVALID_ID == minor_ver_start_cid_
                   || OB_INVALID_ID == minor_ver_end_cid_))
      {
          TBSYS_LOG(WARN,"invalid minor_ver start cid[%ld] or end cid[%ld]!",
                    minor_ver_start_cid_,minor_ver_end_cid_);
      }
      else if (need_data_store_type_
               && (data_store_type_cid_ <= 0
                   || OB_INVALID_ID == data_store_type_cid_))
      {
          TBSYS_LOG(WARN,"invalid data store type cid[%ld]!",data_store_type_cid_);
      }
      else
      {
          bret = true;
      }
      return bret;
    }

    bool ObDataMarkParam::is_data_mark_cid(const uint64_t cid) const
    {
        bool bret = false;
        if ((need_modify_time_ && cid == modify_time_cid_)
            || (need_major_version_ && cid == major_version_cid_)
            || (need_minor_version_ && cid == minor_ver_start_cid_)
            || (need_minor_version_ && cid == minor_ver_end_cid_)
            || (need_data_store_type_ && cid == data_store_type_cid_))
        {
            bret = true;
        }
        return bret;
    }

    int64_t ObDataMarkParam::to_string(char *buf, int64_t buf_len) const
    {
        int64_t pos = 0;
        databuff_printf(buf, buf_len, pos, "ObDataMarkParam(");
        databuff_printf(buf, buf_len, pos, "table_id=%ld,",
                        table_id_);
        databuff_printf(buf, buf_len, pos, "need_modify_time_=%d,",
                        need_modify_time_);
        databuff_printf(buf, buf_len, pos, "need_major_version_=%d,",
                        need_major_version_);
        databuff_printf(buf, buf_len, pos, "need_prevcommit_trans_verset_=%d,",
                        need_minor_version_);
        databuff_printf(buf, buf_len, pos, "need_data_store_type_=%d,",
                        need_data_store_type_);
        databuff_printf(buf, buf_len, pos, "modify_time_cid_=%ld,",
                        modify_time_cid_);
        databuff_printf(buf, buf_len, pos, "major_version_cid_=%ld,",
                        major_version_cid_);
        databuff_printf(buf, buf_len, pos, "minor_ver_start_cid_=%ld,",
                        minor_ver_start_cid_);
        databuff_printf(buf, buf_len, pos, "minor_ver_end_cid_=%ld,",
                        minor_ver_end_cid_);
        databuff_printf(buf, buf_len, pos, "data_store_type_cid_=%ld,",
                        data_store_type_cid_);
        databuff_printf(buf, buf_len, pos, ")");
        return pos;
    }

    DEFINE_SERIALIZE(ObDataMarkParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,table_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_modify_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_minor_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_data_store_type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,modify_time_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,major_version_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_start_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_end_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,data_store_type_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObDataMarkParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
          buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_modify_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_minor_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_data_store_type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&modify_time_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&major_version_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_start_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_end_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&data_store_type_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObDataMarkParam)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_i64(table_id_);
      total_size += serialization::encoded_length_bool(need_modify_time_);
      total_size += serialization::encoded_length_bool(need_major_version_);
      total_size += serialization::encoded_length_bool(need_minor_version_);
      total_size += serialization::encoded_length_bool(need_data_store_type_);
      total_size += serialization::encoded_length_i64(modify_time_cid_);
      total_size += serialization::encoded_length_i64(major_version_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_start_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_end_cid_);
      total_size += serialization::encoded_length_i64(data_store_type_cid_);
      return total_size;
    }

    int ObDataMarkHelper::get_data_mark(const ObRow *row, const ObDataMarkParam &param, ObDataMark &out_data_mark)
    {
        int ret = OB_SUCCESS;
        uint64_t table_id  = param.table_id_;
        const ObObj *cell  = NULL;
        int64_t  cell_idx  = OB_INVALID_INDEX;
        uint64_t tid       = OB_INVALID_ID;
        uint64_t cid       = OB_INVALID_ID;
        const ObRowDesc* row_desc = NULL;
        out_data_mark.reset();
        if (NULL == row
            || !param.is_valid()
            || NULL == (row_desc = row->get_row_desc()))
        {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR,"invalid argument!row=%p,row_desc=%p,param=[%s],ret=%d",
                      row,row_desc,to_cstring(param),ret);
        }
        else
        {
            if (OB_SUCCESS == ret
                && param.need_modify_time_)
            {
                cell     = NULL;
                cell_idx = OB_INVALID_INDEX;
                tid      = OB_INVALID_ID;
                cid      = OB_INVALID_ID;
                if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                      param.modify_time_cid_)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"row didn't cont mtime cid!row=[%s],ret=%d",
                              to_cstring(*row),ret);
                }
                else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
                {
                  TBSYS_LOG(WARN,"fail to get mtime obj!table_id=%ld,ret=%d",
                            table_id,ret);
                }
                else if (ObNullType == cell->get_type()
                         || (ObExtendType == cell->get_type()
                             && ObActionFlag::OP_NOP == cell->get_ext()))
                {
                    TBSYS_LOG(DEBUG,"mul_del::debug,cur row has no mtime!cell=[%s],row=[%s]",
                              to_cstring(*cell),to_cstring(*row));
                }
                else if (ObIntType != cell->get_type())
                {
                  ret =OB_ERR_UNEXPECTED;
                  TBSYS_LOG(ERROR,"invalid mtime cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.modify_time_)))
                {
                  TBSYS_LOG(WARN,"fail to get mtime from cell[%s],ret=%d",to_cstring(*cell),ret);
                }
            }

            if (OB_SUCCESS == ret
                && param.need_major_version_)
            {
                cell     = NULL;
                cell_idx = OB_INVALID_INDEX;
                tid      = OB_INVALID_ID;
                cid      = OB_INVALID_ID;
                if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                      param.major_version_cid_)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"row didn't cont major ver cid!row=[%s],ret=%d",
                              to_cstring(*row),ret);
                }
                else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
                {
                  TBSYS_LOG(WARN,"fail to get major ver obj!table_id=%ld,ret=%d",
                            table_id,ret);
                }
                else if (ObNullType == cell->get_type()
                         || (ObExtendType == cell->get_type()
                             && ObActionFlag::OP_NOP == cell->get_ext()))
                {
                    TBSYS_LOG(INFO,"mul_del::debug,cur row has no major ver!cell=[%s],row=[%s]",
                              to_cstring(*cell),to_cstring(*row));
                }
                else if (ObIntType != cell->get_type())
                {
                  ret =OB_ERR_UNEXPECTED;
                  TBSYS_LOG(ERROR,"invalid major ver cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.major_version_)))
                {
                  TBSYS_LOG(WARN,"fail to get major ver from cell[%s],ret=%d",to_cstring(*cell),ret);
                }
            }

            if (OB_SUCCESS == ret
                && param.need_minor_version_)
            {
                cell     = NULL;
                cell_idx = OB_INVALID_INDEX;
                tid      = OB_INVALID_ID;
                cid      = OB_INVALID_ID;
                if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                      param.minor_ver_start_cid_)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"row didn't cont minor ver start cid!row=[%s],ret=%d",
                              to_cstring(*row),ret);
                }
                else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
                {
                  TBSYS_LOG(WARN,"fail to get minor ver start obj!table_id=%ld,ret=%d",
                            table_id,ret);
                }
                else if (ObNullType == cell->get_type()
                         || (ObExtendType == cell->get_type()
                             && ObActionFlag::OP_NOP == cell->get_ext()))
                {
                    TBSYS_LOG(INFO,"mul_del::debug,cur row has no minor ver start!cell=[%s],row=[%s]",
                              to_cstring(*cell),to_cstring(*row));
                }
                else if (ObIntType != cell->get_type())
                {
                  ret =OB_ERR_UNEXPECTED;
                  TBSYS_LOG(ERROR,"invalid minor ver start cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.minor_ver_start_)))
                {
                  TBSYS_LOG(WARN,"fail to get minor ver start from cell[%s],ret=%d",to_cstring(*cell),ret);
                }
            }

            if (OB_SUCCESS == ret
                && param.need_minor_version_)
            {
                cell     = NULL;
                cell_idx = OB_INVALID_INDEX;
                tid      = OB_INVALID_ID;
                cid      = OB_INVALID_ID;
                if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                      param.minor_ver_end_cid_)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"row didn't cont minor ver end cid!row=[%s],ret=%d",
                              to_cstring(*row),ret);
                }
                else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
                {
                  TBSYS_LOG(WARN,"fail to get minor ver end obj!table_id=%ld,ret=%d",
                            table_id,ret);
                }
                else if (ObNullType == cell->get_type()
                         || (ObExtendType == cell->get_type()
                             && ObActionFlag::OP_NOP == cell->get_ext()))
                {
                    TBSYS_LOG(INFO,"mul_del::debug,cur row has no minor ver end!cell=[%s],row=[%s]",
                              to_cstring(*cell),to_cstring(*row));
                }
                else if (ObIntType != cell->get_type())
                {
                  ret =OB_ERR_UNEXPECTED;
                  TBSYS_LOG(ERROR,"invalid minor ver end cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.minor_ver_end_)))
                {
                  TBSYS_LOG(WARN,"fail to get minor ver end  from cell[%s],ret=%d",to_cstring(*cell),ret);
                }
            }

            if (OB_SUCCESS == ret
                && param.need_data_store_type_)
            {
                int64_t tmp_type = INVALID_STORE_TYPE;
                cell     = NULL;
                cell_idx = OB_INVALID_INDEX;
                tid      = OB_INVALID_ID;
                cid      = OB_INVALID_ID;
                if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                      param.data_store_type_cid_)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"row didn't cont data store type cid!row=[%s],ret=%d",
                              to_cstring(*row),ret);
                }
                else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
                {
                  TBSYS_LOG(WARN,"fail to get data store type obj!table_id=%ld,ret=%d",
                            table_id,ret);
                }
                else if (ObNullType == cell->get_type()
                         || (ObExtendType == cell->get_type()
                             && ObActionFlag::OP_NOP == cell->get_ext()))
                {
                    TBSYS_LOG(INFO,"mul_del::debug,cur row has no data store type!cell=[%s],row=[%s]",
                              to_cstring(*cell),to_cstring(*row));
                }
                else if (ObIntType != cell->get_type())
                {
                  ret =OB_ERR_UNEXPECTED;
                  TBSYS_LOG(ERROR,"invalid data store type cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else if (OB_SUCCESS != (ret = cell->get_int(tmp_type)))
                {
                  TBSYS_LOG(WARN,"fail to get data store type from cell[%s],ret=%d",to_cstring(*cell),ret);
                }
                else
                {
                    out_data_mark.data_store_type_ = static_cast<ObDataMarkDataStoreType>(tmp_type);
                }
            }
        }
		
		if (NULL != row)
		{
		   TBSYS_LOG(DEBUG,"mul_del::debug,finish get data mark!orig_row=[%s],out_data_mark=[%s],orig_param=[%s],ret=%d",
		             to_cstring(*row),to_cstring(out_data_mark),to_cstring(param),ret);
		}

        return ret;
    }

    int ObDataMarkHelper::convert_data_mark_row_to_normal_row(const ObDataMarkParam &param,
                                                              const ObRow *src_row,
                                                              ObRow *des_row)
    {
        int ret = OB_SUCCESS;
        const ObRowDesc *src_row_desc = NULL;
        ObRowDesc *des_row_desc = NULL;
        int64_t src_row_col_num = 0;
        const ObObj *cell   = NULL;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        if (NULL == src_row
           || NULL == (src_row_desc = src_row->get_row_desc())
           || NULL == des_row
           || NULL == (des_row_desc = const_cast<ObRowDesc *>(des_row->get_row_desc()))
           || !param.is_valid())
        {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR,"invalid argum!src_row=%p,src_row_desc=%p,des_row=%p,des_row_desc=%p,"
                      "param_is_valid=%d,param=[%s],ret=%d",src_row,src_row_desc,
                      des_row,des_row_desc,param.is_valid(),to_cstring(param),ret);
        }
        else
        {
            des_row->clear();
            des_row_desc->reset();
            src_row_col_num = src_row_desc->get_column_num();
            TBSYS_LOG(DEBUG,"mul_del::debug,begin convert!orig_row=[%s]",to_cstring(*src_row));
        }

        //1:cons row desc
        if (OB_SUCCESS == ret)
        {
            for (int64_t idx=0;OB_SUCCESS == ret && idx<src_row_col_num;idx++)
            {
                tid = OB_INVALID_ID;
                cid = OB_INVALID_ID;
                if (OB_SUCCESS != (ret = src_row_desc->get_tid_cid(idx,tid,cid)))
                {
                    TBSYS_LOG(WARN,"fail to get %ldth tid[%ld] cid[%ld]!ret=%d",
                              idx,tid,cid,ret);
                }
                else if (!param.is_data_mark_cid(cid)
                         && OB_SUCCESS != (ret = des_row_desc->add_column_desc(tid,cid)))
                {
                    TBSYS_LOG(WARN,"fail to add %ldth tid[%ld] cid[%ld]!ret=%d",
                              idx,tid,cid,ret);
                }
            }

            if (OB_SUCCESS == ret)
            {
                des_row->set_row_desc(*des_row_desc);
            }
        }

        //2:set row cell
        if (OB_SUCCESS == ret)
        {
            for (int64_t col_idx=0;OB_SUCCESS == ret && col_idx < src_row_col_num;col_idx++)
            {
                cell = NULL;
                tid  = OB_INVALID_ID;
                cid  = OB_INVALID_ID;
                if (OB_SUCCESS != (ret = src_row->raw_get_cell(col_idx,cell,tid,cid)))
                {
                    TBSYS_LOG(WARN,"fail to get %ldth cell[%p] tid[%ld] cid[%ld],ret=%d",
                              col_idx,cell,tid,cid,ret);
                }
                else if (!param.is_data_mark_cid(cid)
                         && OB_SUCCESS != (ret = des_row->set_cell(tid,cid,*cell)))
                {
                    TBSYS_LOG(WARN,"fail to set %ldth cell[%s] tid[%ld] cid[%ld],ret=%d",
                              col_idx,to_cstring(*cell),tid,cid,ret);
                }
            }
        }

        TBSYS_LOG(DEBUG,"mul_del::debug,finish convert!final_row=%p,ret=%d",des_row,ret);
        if (NULL != des_row)
            TBSYS_LOG(DEBUG,"mul_del::debug,final convert row=[%s]",to_cstring(*des_row));
        return ret;
    }

    //add duyr 20160531:e

    ObReadParam::ObReadParam()
    {
      reset();
    }

    void ObReadParam::reset()
    {
      is_read_master_ = 1;
      is_result_cached_ = 0;
      version_range_.start_version_ = 0;
      version_range_.end_version_ = 0;
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      //add duyr 20160531:e

    }

    ObReadParam::~ObReadParam()
    {
    }

    void ObReadParam::set_is_read_consistency(const bool consistency)
    {
      is_read_master_ = consistency;
    }

    bool ObReadParam::get_is_read_consistency()const
    {
      return (is_read_master_ > 0);
    }

    void ObReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    bool ObReadParam::get_is_result_cached()const
    {
      return (is_result_cached_ > 0);
    }

    void ObReadParam::set_version_range(const ObVersionRange & range)
    {
      version_range_ = range;
    }

    ObVersionRange ObReadParam::get_version_range(void) const
    {
      return version_range_;
    }

    int ObReadParam::serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      // serialize RESERVER PARAM FIELD
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_read_consistency());
        ret = obj.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObReadParam::deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int64_t int_value = 0;
      int ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is read master
          set_is_read_consistency(int_value);
        }
      }
      return ret;
    }

    int64_t ObReadParam::get_reserve_param_serialize_size(void) const
    {
      ObObj obj;
      // reserve for read master
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int64_t total_size = obj.get_serialize_size();
      obj.set_int(get_is_read_consistency());
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(ObReadParam::get_is_result_cached());
      int ret = obj.serialize(buf, buf_len, pos);
      // scan version range
      if (ret == OB_SUCCESS)
      {
        ObVersionRange version_range = ObReadParam::get_version_range();;
        obj.set_int(version_range.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.start_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }

        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.end_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      if (OB_SUCCESS == ret)
      {
          data_mark_param_.serialize(buf,buf_len,pos);
      }
      //add duyr  20160531:e

      return ret;
    }

    DEFINE_DESERIALIZE(ObReadParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is cached
          set_is_result_cached(int_value);
        }
      }

      // version range
      if (OB_SUCCESS == ret)
      {
        // border flag
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.border_flag_.set_data(static_cast<int8_t>(int_value));
          }
        }
      }

      // start version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.start_version_ = int_value;
          }
        }
      }

      // end version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.end_version_ = int_value;
          }
        }
      }

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      if (OB_SUCCESS == ret)
      {
          ret = data_mark_param_.deserialize(buf,data_len,pos);
      }
      //add duyr 20160531:e

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(get_is_result_cached());
      int64_t total_size = obj.get_serialize_size();

      // scan version range
      obj.set_int(version_range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.start_version_);
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.end_version_);
      total_size += obj.get_serialize_size();

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      total_size += data_mark_param_.get_serialize_size();
      //add duyr 20160531:e
      return total_size;
    }


    //-------------------------------------------------------------------------------
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, int64_t& value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObExtendType == obj.get_type())
      {
        ret = obj.get_ext(value);
      }
      return ret;
    }

    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_int_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, int64_t & int_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObIntType == obj.get_type())
      {
        ret = obj.get_int(int_value);
      }
      return ret;
    }

    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_varchar(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObVarcharType == obj.get_type())
      {
        ret = obj.get_varchar(str_value);
      }
      return ret;
    }


    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = set_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_SUCCESS == (ret = get_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].deserialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size)
    {
      int64_t total_size = 0;
      ObObj obj;
      obj.set_int(size);
      total_size += obj.get_serialize_size();

      for (int64_t i = 0; i < size; ++i)
      {
        total_size += array[i].get_serialize_size();
      }
      return total_size;
    }

    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
        const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool& is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      int64_t obj_count = 0;
      ObString str_value;

      is_binary_rowkey = false;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos)) )
      {
        if (ObIntType == obj.get_type() && (OB_SUCCESS == (ret = obj.get_int(obj_count))))
        {
          // new rowkey format.
          for (int64_t i = 0; i < obj_count && OB_SUCCESS == ret; ++i)
          {
            if (i >= size)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              ret = array[i].deserialize(buf, buf_len, pos);
            }
          }

          if (OB_SUCCESS == ret) size = obj_count;
        }
        else if (ObVarcharType == obj.get_type() && OB_SUCCESS == (ret = obj.get_varchar(str_value)))
        {
          is_binary_rowkey = true;
          // old fashion , binary rowkey stream
          if (size < info.get_size())
          {
            TBSYS_LOG(WARN, "input size=%ld not enough, need rowkey obj size=%ld", size, info.get_size());
            ret = OB_SIZE_OVERFLOW;
          }
          else if (str_value.length() == 0)
          {
            // allow empty binary rowkey , incase min, max range.
            size = 0;
          }
          else if (str_value.length() < info.get_binary_rowkey_length())
          {
            TBSYS_LOG(WARN, "binary rowkey length=%d < need rowkey length=%ld",
                str_value.length(), info.get_binary_rowkey_length());
            ret = OB_SIZE_OVERFLOW;
          }
          else
          {
            size = info.get_size();
            ret = ObRowkeyHelper::binary_rowkey_to_obj_array(info, str_value, array, size);
          }
        }
      }

      return ret;
    }

    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
        const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* tbl = NULL;
      if (NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (table_id > 0 && table_id != OB_INVALID_ID)
      {
        tbl = schema_mgr->get_table_schema(table_id);
      }
      else if (NULL != table_name.ptr())
      {
        tbl = schema_mgr->get_table_schema(table_name);
      }

      if (NULL == tbl)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rowkey_info = tbl->get_rowkey_info();
      }
      return ret;
    }

  } /* common */
} /* oceanbase */
