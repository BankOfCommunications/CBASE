//
// Created by shili on 2016/9/28.
//

#include "ob_big_log_writer.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    /*
     * @berif   将数据分多次同步并写入到日志中
     * @param   tid[in]  本事务是的事务id
     * @parma   session_ctx[in out] big log 当前的session
     */
    int ObBigLogWriter::write_big_log(const ObTransID &tid, RWSessionCtx &session_ctx)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = 0;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      ObUpsLogMgr &log_mgr = main->get_update_server().get_log_mgr();
      if(!is_inited())
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "not init ,ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = unpackage_big_log(tid)))
      {
        TBSYS_LOG(WARN, "unpackage big log fail,ret=%d", ret);
      }
      else
      {
        //将拆分的小的包，分别刷盘
        for(int64_t i = 0; OB_SUCCESS == ret && i < package_num_; i++)
        {
          serialize_size = 0;
          memset(log_buffer_, LOG_BUFFER_SIZE, 0);
          ret = package_store_.at(i).serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld "
              "serialize_size=%ld ret=%d",log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
          }
          else if(log_mgr.is_buffer_clear())
          {
            // do nothing
          }
          else
          {
            int64_t end_log_id = 0;
            CLEAR_TRACE_BUF(TraceLog::get_logbuffer());
            ret = log_mgr.async_flush_log(end_log_id, TraceLog::get_logbuffer());
            if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "log_mgr  flush log twice fail,ret=%d", ret);
            }
          }

          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = log_mgr.write_and_flush_log(OB_UPS_BIG_LOG_DATA, log_buffer_, serialize_size)))
            {
              TBSYS_LOG(WARN, "write_and_flush_log fail, ret=%d",ret);
            }
            else
            {
              session_ctx.set_frozen();
            }
          }
        }//end for
      }
      return ret;
    }

    /*@berif 所有的日志数据已经完整，反序列化成完整数据，并执行相应的应用操作*/
    int ObBigLogWriter::apply(ObLogTask &task)
    {
      int err = OB_SUCCESS;
      switch(log_cmd_)
      {
        case OB_LOG_UPS_MUTATOR:
          if(OB_SUCCESS != (err = log_applier_->handle_normal_mutator(ptr_, data_length_, task)))
          {
            TBSYS_LOG(WARN, "fail to handle normal mutator. err=%d", err);
          }
          break;
        default:
          err = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "no supported,ret=%d",err);
      }
      return err;
    }


    bool ObBigLogWriter::is_first_package(const ObNormalLogData &log_data)
    {
      return log_data.seq_ == 0;
    }

    bool ObBigLogWriter::is_all_package_completed(const ObNormalLogData &log_data)
    {
      return package_num_ - 1 == log_data.seq_;
    }

    /* @berif  对log_data 进行错误检查*/
    int ObBigLogWriter::check_data_log(const ObNormalLogData &log_data)
    {
      int ret = OB_SUCCESS;
      if(!log_data.trans_id_.is_valid())
      {
        ret = OB_INVALID_SESSION_ID;
        TBSYS_LOG(WARN, " log data trans id is invalid,ret=%d",ret);
      }
      else if(is_first_package(log_data))
      {
        if(last_trans_id_.equal(log_data.trans_id_))
        {
          ret = OB_INVALID_SESSION_ID;
          TBSYS_LOG(WARN, "last trans id  equal to this trans id,ret=%d",ret);
        }
        else if(last_seq_ + 1 != package_num_)/*上一个big_log 最后一个或者最后几个块 丢失，不需要处理,只报警报*/
        {
          TBSYS_LOG(WARN, "last big log seq  don't equal to package num");
        }
      }
      else
      {
        if(!last_trans_id_.equal(log_data.trans_id_))/*最开头的一个或者几个块 丢失*/
        {
          ret = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "big log is not continuous,ret=%d", ret);
        }
        else if(last_seq_ + 1 != log_data.seq_) /*中间某些块丢失*/
        {
          ret = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "big log is not continuous,ret=%d", ret);
        }
      }
      return ret;
    }


    //@berif 将多个包组成一个包,如果是big log中最后一个包，进行反序列化
    int ObBigLogWriter::package_big_log(const char *log_data, const int64_t data_len, bool &is_completed)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t write_size=0;
      ObNormalLogData tmp_log_data;
      is_completed = false;
      if(!is_inited())
      {
        ret = OB_NOT_INIT;
      }
      else if(OB_SUCCESS != (ret = tmp_log_data.deserialize(log_data, data_len, pos)))
      {
        TBSYS_LOG(WARN, "log data deserialize fail,ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = check_data_log(tmp_log_data)))
      {
        TBSYS_LOG(WARN, "log data is invalid,ret=%d", ret);
      }
      else if(is_first_package(tmp_log_data))
      {
        resue();
        buffer_size_ = tmp_log_data.total_size_;
        log_cmd_ = tmp_log_data.log_cmd_;
        last_trans_id_ = tmp_log_data.trans_id_;
        ptr_ = (char *)ob_malloc(buffer_size_, ObModIds::OB_UPS_BIG_LOG_DATA);
        if(ptr_ == NULL)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "alloc  log data memery fail,ret=%d", ret);
        }
        else
        {
          last_seq_ = 0;
          package_num_ = tmp_log_data.total_package_num_;
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(!is_first_package(tmp_log_data))
        {
          last_seq_++;
        }

        if(0==(write_size = write(tmp_log_data.data_buf_, tmp_log_data.buf_size_)))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "write log data to log_writer fail,ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = package_store_.push_back(tmp_log_data)))
        {
          TBSYS_LOG(WARN, "push back fail,ret=%d", ret);
        }
        else if(is_all_package_completed(tmp_log_data))
        {
          is_completed= true;
        }
      }

      if(OB_SUCCESS != ret)
      {
        resue();
      }
      return ret;
    }


    /*@berif  处理big_log task*/
    int ObBigLogWriter::handle_big_log(ObLogTask &task)
    {
      int ret = OB_SUCCESS;
      const char *log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      task.is_big_log_completed_ = false;
      if(OB_SUCCESS!=( ret= package_big_log(log_data,data_len,task.is_big_log_completed_)))
      {
        TBSYS_LOG(WARN, "package_big_log fail,ret=%d",ret);
      }
      else if(!task.is_big_log_completed_)
      {
      }
      else if(OB_SUCCESS != (ret = apply(task)))//all  package are reviced
      {
        TBSYS_LOG(WARN, "apply  fail,ret=%d", ret);
      }
      return ret;
    }

    /*
     * @berif   将大的数据  拆分成多个 小数据
     * @param tid [in]  本次事务的事务id
     */
    int ObBigLogWriter::unpackage_big_log(const ObTransID &tid)
    {
      int ret = OB_SUCCESS;
      ObNormalLogData  normal_log_data;
      int32_t max_serialize_size=(int32_t)(MAX_SERIALIZE_SIZE - normal_log_data.get_header_serialize_size());
      data_length_ = 0;
      package_num_ = 0;
      last_seq_ = 0;
      if(ptr_ == NULL)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "ptr_ is null,ret=%d", ret);
      }
      while(OB_SUCCESS == ret && data_length_ < buffer_size_)
      {
        ObNormalLogData tmp_log_data;
        if(data_length_ + (int32_t)max_serialize_size <= buffer_size_)
        {
          tmp_log_data.assign(ptr_ + data_length_, max_serialize_size, last_seq_, buffer_size_);
          data_length_ += max_serialize_size;
        }
        else
        {
          tmp_log_data.assign(ptr_ + data_length_, buffer_size_ - data_length_, last_seq_, buffer_size_);
          data_length_ = buffer_size_;
        }
        if(OB_SUCCESS != (ret = package_store_.push_back(tmp_log_data)))
        {
          TBSYS_LOG(WARN, "push back fail,ret=%d", ret);
        }
        else
        {
          last_seq_++;
          package_num_++;
        }
      }

      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0; i < package_store_.count(); i++)
        {
          package_store_.at(i).set_total_package_num(package_num_);
          package_store_.at(i).set_log_cmd(log_cmd_);
          package_store_.at(i).set_trans_id(tid);
        }
      }
      return ret;
    }

    /*@berif 初始化 log buffer*/
    int ObBigLogWriter::init()
    {
      int err = OB_SUCCESS;
      if(is_inited_)
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(WARN, "big log writer had inited, err=%d", err);
      }
        //note:此处内存分配 需要 手动ob_free
      else if(NULL == (log_buffer_ = (char *) ob_malloc(LOG_BUFFER_SIZE, ObModIds::OB_UPS_BIG_LOG_DATA)))
      {
        TBSYS_LOG(WARN, "malloc log_buffer fail size=%ld", LOG_BUFFER_SIZE);
        err = OB_ERROR;
      }
      else
      {
        is_inited_ = true;
      }
      return err;
    }


    int ObNormalLogData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int res = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();
      if(buf == NULL || serialize_size > buf_len - pos)
      {
        res = OB_ERROR;
      }

      if(res == OB_SUCCESS)
      {
        *(reinterpret_cast<int32_t*> (buf + pos)) = seq_;
        pos += sizeof(int32_t);
        *(reinterpret_cast<int32_t*> (buf + pos)) = total_size_;
        pos += sizeof(int32_t);
        *(reinterpret_cast<int32_t*> (buf + pos)) = total_package_num_;
        pos += sizeof(int32_t);
        *(reinterpret_cast<int32_t*> (buf + pos)) = (int32_t) log_cmd_;
        pos += sizeof(int32_t);

        if(OB_SUCCESS != (res = trans_id_.serialize_4_biglog(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN,"serialize trans id fail,ret=%d",res);
        }
        else if(OB_SUCCESS != (res = serialization::encode_vstr(buf, buf_len, pos, data_buf_, buf_size_)))
        {
          TBSYS_LOG(WARN,"serialize vstr fail,ret=%d",res);
        }
      }
      return res;
    }

    int ObNormalLogData::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
    {
      int res = OB_SUCCESS;
      int64_t len = 0;
      if(NULL == buf || (data_len - pos) < 2)  //at least need two bytes
      {
        res = OB_ERROR;
      }
      else
      {
        reset();
      }

      if(OB_SUCCESS == res)
      {
        seq_ = *(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        total_size_ = *(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        total_package_num_ = *(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        log_cmd_ = (LogCommand)*(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        if(OB_SUCCESS != (res = trans_id_.deserialize_4_biglog(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN,"deserialize trans id fail,ret=%d",res);
        }
      }

      if(OB_SUCCESS == res)
      {
        data_buf_ = const_cast<char *>(serialization::decode_vstr(buf, data_len, pos, &len));
        if(NULL == data_buf_)
        {
          res = OB_ERROR;
          TBSYS_LOG(WARN,"deserialize string fail,ret=%d",res);
        }
        buf_size_ = static_cast<int32_t>(len);
      }
      return res;
    }

    int64_t ObNormalLogData::get_serialize_size(void) const
    {
      int64_t serialize_size = serialization::encoded_length_vstr(buf_size_);
      serialize_size += serialization::encoded_length_i32(seq_);
      serialize_size += serialization::encoded_length_i32(total_size_);
      serialize_size += serialization::encoded_length_i32(total_package_num_);
      serialize_size += serialization::encoded_length_i32((int32_t) log_cmd_);
      serialize_size += trans_id_.get_serialize_size_4_biglog();
      return serialize_size;
    }

    /*@berif 获的ObNormalLogData 的头的 序列化大小*/
    int64_t ObNormalLogData::get_header_serialize_size(void) const
    {
      int64_t serialize_size = serialization::get_str_length_except_data(buf_size_);
      serialize_size += serialization::encoded_length_i32(seq_);
      serialize_size += serialization::encoded_length_i32(total_size_);
      serialize_size += serialization::encoded_length_i32(total_package_num_);
      serialize_size += serialization::encoded_length_i32((int32_t) log_cmd_);
      serialize_size += trans_id_.get_serialize_size_4_biglog();
      return serialize_size;
    }
  }
}