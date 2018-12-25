//
// Created by shili on 2016/9/28.
//

#ifndef __OB_UPDATESERVER_OB_BIG_LOG_WRITER_H__
#define __OB_UPDATESERVER_OB_BIG_LOG_WRITER_H__
#include "common/serialization.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/murmur_hash.h"
#include "common/ob_mutator.h"
#include "ob_ups_log_utils.h"
#include "ob_ups_utils.h"
#include "ob_sessionctx_factory.h"
#include "ob_async_log_applier.h"
#include <algorithm>
#include <iostream>

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;


    struct ObNormalLogData
    {
      NEED_SERIALIZE_AND_DESERIALIZE;
      int64_t get_header_serialize_size(void) const;

      void assign(char * buf,int32_t size,int32_t seq,int32_t total_size)
      {
        data_buf_ = buf;
        buf_size_ = size;
        if(data_buf_==NULL)
        {
          buf_size_=0;
        }
        seq_=seq;
        total_size_=total_size;
      }

      void set_total_package_num(int32_t num)
      {
        total_package_num_ = num;
      }

      void set_log_cmd(LogCommand  log_cmd)
      {
        log_cmd_ = log_cmd;
      }

      void set_trans_id(const ObTransID &trans_id)
      {
        trans_id_ = trans_id;
      }

      void reset()
      {
        data_buf_=NULL;
        buf_size_=0;
        seq_=0;
        total_size_=0;
        total_package_num_=0;
        log_cmd_ =OB_LOG_UNKNOWN;
        trans_id_.reset();
      }

      char * data_buf_;
      int32_t  buf_size_;
      int32_t seq_;
      int32_t total_size_;
      int32_t total_package_num_;
      LogCommand  log_cmd_;
      ObTransID  trans_id_;
    };


    class ObBigLogWriter
    {
    public:
      const static int64_t LOG_BUFFER_SIZE = 1024 * 1024 * 2;
      static const int64_t LOG_FILE_ALIGN_SIZE = 1<<OB_DIRECT_IO_ALIGN_BITS;
      static const int64_t LOG_BUF_RESERVED_SIZE = 3 * LOG_FILE_ALIGN_SIZE; // nop + switch_log + eof
      const static int64_t MAX_SERIALIZE_SIZE = OB_MAX_LOG_BUFFER_SIZE - LOG_BUF_RESERVED_SIZE;

      ObBigLogWriter() : buffer_size_(0), data_length_(0), ptr_(NULL), last_seq_(0), log_buffer_(NULL),
                         package_num_(0), is_inited_(false), is_completed_(false)
      {
        last_trans_id_.reset();
      }

      ObBigLogWriter(const int32_t size, const int32_t data_length, char *ptr) : buffer_size_(size),
                                            data_length_(data_length), ptr_(ptr)
      {
        if(ptr_ == NULL)
        {
          buffer_size_ = 0;
          data_length_ = 0;
        }
        ptr_ = NULL;
        last_seq_ =0;
        log_buffer_=NULL;
        package_num_=0;
        is_inited_ = false;
        is_completed_ = false;
        last_trans_id_.reset();
      }

      ~ObBigLogWriter()
      {
        resue();
        if(log_buffer_!=NULL) //log_buffer_在重置是不需要free
        {
          ob_free(log_buffer_);
        }
      }

      void resue()
      {
        buffer_size_ = 0;
        data_length_ = 0;
        last_seq_ =0;
        package_num_=0;
        is_completed_ =false;
        package_store_.clear();
        if(ptr_ !=NULL )
        {
          ob_free(ptr_);
          ptr_ = NULL;//保证 在ptr_.free 的时候 ptr_为null
        }
      }

      void reset()
      {
        buffer_size_ = 0;
        data_length_ = 0;
        last_seq_ =0;
        package_num_=0;
        is_completed_ =false;
        package_store_.clear();
        ptr_ = NULL;//保证 在ptr_.free 的时候 ptr_为null
      }

      int32_t write(const char *bytes, const int32_t length)
      {
        int32_t writed = 0;
        if(!bytes || length <= 0)
        {
        }
        else
        {
          if(data_length_ + length <= buffer_size_)
          {
            memcpy(ptr_ + data_length_, bytes, length);
            data_length_ += length;
            writed = length;
          }
        }
        return writed;
      }

      void assign(char *bytes, const int32_t length,LogCommand  log_cmd=OB_LOG_UNKNOWN)
      {
        ptr_ = bytes;
        buffer_size_ = length;
        data_length_ = length;
        log_cmd_ = log_cmd;
        if(ptr_ == NULL)
        {
          buffer_size_ = 0;
          data_length_ = 0;
        }
      }

      void set_log_cmd(LogCommand  log_cmd)
      {
        log_cmd_ = log_cmd;
      }

      bool is_inited()
      {
        return is_inited_;
      }

      void set_log_applier(ObAsyncLogApplier * log_applier)
      {
        log_applier_ = log_applier;
      }

      void get_buffer(char * &ptr,int32_t & size)
      {
        ptr = ptr_;
        size = data_length_;
      }

      int32_t get_package_num()
      {
        return package_num_;
      }

      ObArray<ObNormalLogData>& get_package_store()
      {
        return package_store_;
      }

      //for cdc, init last_trans_id_
      void reset_Last_tranID()
      {
          last_trans_id_.reset();
      }

      int init();
      int write_big_log(const ObTransID &trans_id, RWSessionCtx &session_ctx);
      int unpackage_big_log(const ObTransID &tid);
      int package_big_log(const char *log_data, const int64_t size, bool &is_completed);
      int handle_big_log(ObLogTask &task);
      bool is_first_package(const ObNormalLogData & log_data);
      int check_data_log(const ObNormalLogData & log_data);
      bool is_all_package_completed(const ObNormalLogData &log_data);
      int apply(ObLogTask& task);



    private:
      int32_t buffer_size_;
      int32_t data_length_;
      char *ptr_;              //  用于存储  需要写到日志文件中的 所有内容
      int32_t last_seq_;
      char * log_buffer_;     //写日志时 用于存储  单条日志
      int32_t  package_num_;
      ObArray<ObNormalLogData> package_store_;
      bool is_inited_;
      bool is_completed_;
      LogCommand  log_cmd_;
      ObAsyncLogApplier * log_applier_;
      ObTransID  last_trans_id_;
    };

  }
}
#endif //__OB_UPDATESERVER_OB_BIG_LOG_WRITER_H__
