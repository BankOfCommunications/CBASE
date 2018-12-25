#ifndef TASK_INFO_H_
#define TASK_INFO_H_

#include "common/utility.h"
#include "common/ob_string_buf.h"
#include "common/ob_common_param.h"
#include "common/ob_scan_param.h"
#include "common/ob_malloc.h"
#include "tablet_location.h"
#include "task_packet.h"
#include <string>

namespace oceanbase
{
  namespace tools
  {
    struct TaskCounter
    {
    public:
      TaskCounter()
      {
        total_count_ = 0;
        wait_count_ = 0;
        doing_count_ = 0;
        finish_count_ = 0;
      }

      int64_t total_count_;
      int64_t wait_count_;
      int64_t doing_count_;
      int64_t finish_count_;

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class TaskInfo
    {
      struct ParamBuffer
      {
        mutable tbsys::CThreadMutex lock_;
        volatile int64_t ref_count_;
        char buffer_[0];
        ParamBuffer()
        {
          ref_count_ = 1;
        }
      };

    public:
      TaskInfo()
      {
        timestamp_ = -1;
        task_token_ = -1;
        task_id_ = INVALID_ID;
        task_limit_ = 0;
        first_index_ = 0;
        param_buffer_ = NULL;
        table_id_ = INVALID_ID;
      }

      TaskInfo(const TaskInfo & other)
      {
        if (other.param_buffer_)
        {
          tbsys::CThreadGuard lock(&other.param_buffer_->lock_);
          ++other.param_buffer_->ref_count_;
        }
        memcpy(this, &other, sizeof(TaskInfo));
        // deep copy the scam_param_ start_key objs
        int64_t length = scan_param_.get_range()->start_key_.length();
        for (int64_t i = 0; i < length; ++i)
        {
          start_key_ptr_[i] = scan_param_.get_range()->start_key_.ptr()[i];
        }
        const_cast<common::ObNewRange *>(scan_param_.get_range())->start_key_.assign(start_key_ptr_, length);
        // deep copy the scam_param_ end_key objs
        length = scan_param_.get_range()->end_key_.length();
        for (int64_t i = 0; i < length; ++i)
        {
          end_key_ptr_[i] = scan_param_.get_range()->end_key_.ptr()[i];
        }
        const_cast<common::ObNewRange*>(scan_param_.get_range())->end_key_.assign(end_key_ptr_, length);
        /*
        TBSYS_LOG(TRACE, "construct the task info:[%p->%p], src_range[%p:%s], new_range[%p:%s]",
            &other, this, other.scan_param_.get_range(), to_cstring(*other.scan_param_.get_range()),
            scan_param_.get_range(), to_cstring(*scan_param_.get_range()));
        */
      }

      virtual ~TaskInfo()
      {
        dec_ref();
      }

      void dec_ref()
      {
        if (param_buffer_)
        {
          tbsys::CThreadGuard lock(&param_buffer_->lock_);
          if (--param_buffer_->ref_count_ == 0)
          {
            param_buffer_->~ParamBuffer();
            common::ob_free(param_buffer_);
            param_buffer_ = NULL;
          }
          else if (param_buffer_->ref_count_ < 0)
          {
            TBSYS_LOG(ERROR, "not release buffer[%p], ref_count[%ld]",
                param_buffer_->buffer_, param_buffer_->ref_count_);
          }
        }
      }

    public:
      int64_t get_token(void) const
      {
        return task_token_;
      }

      void set_token(const int64_t token)
      {
        task_token_ = token;
      }

      int64_t get_timestamp(void) const
      {
        return timestamp_;
      }

      void set_timestamp(const int64_t timestamp)
      {
        timestamp_ = timestamp;
      }

      uint64_t get_id(void) const
      {
        return task_id_;
      }

      void set_id(const uint64_t id)
      {
        task_id_ = id;
      }

      uint64_t get_limit(void) const
      {
        return task_limit_;
      }

      void set_limit(const uint64_t limit)
      {
        task_limit_ = limit;
      }

      int64_t get_index(void) const
      {
        return first_index_;
      }

      void set_index(const int64_t index)
      {
        first_index_ = index;
      }

      const TabletLocation & get_location(void) const
      {
        return servers_;
      }

      void set_location(const TabletLocation & location)
      {
        servers_ = location;
      }

      const common::ObScanParam & get_param(void) const
      {
        return scan_param_;
      }

      int set_param(const common::ObScanParam & param);

      void set_table_id(int64_t table_id)
      {
        table_id_ = table_id;
      }

      int64_t get_table_id() const
      {
        return table_id_;
      }

      void set_table_name(const char *table_name)
      {
        table_name_.assign_ptr(const_cast<char *>(table_name), int32_t(strlen(table_name)));
      }

      const common::ObString &get_table_name() const
      {
        return table_name_;
      }

      bool operator == (const TaskInfo & other)
      {
        return ((timestamp_ == other.timestamp_)
          && (task_token_ == other.task_token_)
          && (task_id_ == other.task_id_)
          && (task_limit_ == other.task_limit_)
          && (first_index_ == other.first_index_)
          && (servers_ == other.servers_));
          //&& (scan_param_ == other.scan_param_)
      }

      void operator = (const TaskInfo & other)
      {
        dec_ref();
        if (other.param_buffer_)
        {
          tbsys::CThreadGuard lock(&other.param_buffer_->lock_);
          ++other.param_buffer_->ref_count_;
        }
        memcpy(this, &other, sizeof(TaskInfo));
        // deep copy the scam_param_ start_key objs
        int64_t length = scan_param_.get_range()->start_key_.length();
        for (int64_t i = 0; i < length; ++i)
        {
          start_key_ptr_[i] = scan_param_.get_range()->start_key_.ptr()[i];
        }
        const_cast<common::ObNewRange*>(scan_param_.get_range())->start_key_.assign(start_key_ptr_, length);
        // deep copy the scam_param_ end_key objs
        length = scan_param_.get_range()->end_key_.length();
        for (int64_t i = 0; i < length; ++i)
        {
          end_key_ptr_[i] = scan_param_.get_range()->end_key_.ptr()[i];
        }
        const_cast<common::ObNewRange*>(scan_param_.get_range())->end_key_.assign(end_key_ptr_, length);
      }

    private:
      int64_t timestamp_;
      int64_t task_token_;
      uint64_t task_id_;
      uint64_t task_limit_;
      int64_t first_index_;
      int64_t table_id_;
      TabletLocation servers_;
      common::ObScanParam scan_param_;
      common::ObObj start_key_ptr_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      common::ObObj end_key_ptr_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      ParamBuffer * param_buffer_;
      common::ObString table_name_;
    public:
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
  }
}


#endif // TASK_INFO_H_


