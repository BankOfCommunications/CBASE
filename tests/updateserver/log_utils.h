#ifndef __OB_UPDATESERVER_LOG_UTILS_H__
#define __OB_UPDATESERVER_LOG_UTILS_H__

#include "common/ob_log_entry.h"
#include "common/ob_log_cursor.h"
#include "updateserver/ob_ups_log_utils.h"
#include "common/ob_log_generator.h"
#include "common/ob_log_writer2.h"
#define LOG_ALIGN OB_DIRECT_IO_ALIGN

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
namespace oceanbase
{
  namespace test
  {
    template<typename T>
    int64_t get_log_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t log_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = static_cast<int32_t>(t.get_cursor(log_cursor))))
      {}
      else
      {
        log_id = log_cursor.log_id_;
      }
      return log_id;
    }

    template<typename T>
    int64_t get_file_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t file_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = t.get_cursor(log_cursor)))
      {}
      else
      {
        file_id = log_cursor.file_id_;
      }
      return file_id;
    }

    template<typename T>
    int64_t get_start_log_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t log_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = t.get_start_cursor(log_cursor)))
      {}
      else
      {
        log_id = log_cursor.log_id_;
      }
      return log_id;
    }

    template<typename T>
    int64_t get_start_file_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t file_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = t.get_start_cursor(log_cursor)))
      {}
      else
      {
        file_id = log_cursor.file_id_;
      }
      return file_id;
    }

    template<typename T>
    int64_t get_end_log_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t log_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = t.get_end_cursor(log_cursor)))
      {}
      else
      {
        log_id = log_cursor.log_id_;
      }
      return log_id;
    }

    template<typename T>
    int64_t get_end_file_id(const T& t)
    {
      int err = OB_SUCCESS;
      int64_t file_id = 0;
      ObLogCursor log_cursor;
      if (OB_SUCCESS != (err = t.get_end_cursor(log_cursor)))
      {}
      else
      {
        file_id = log_cursor.file_id_;
      }
      return file_id;
    }

    int get_cbuf(ThreadSpecificBuffer& thread_buffer, char*& cbuf, int64_t& len)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        err = OB_ERROR;
      }
      else
      {
        cbuf = my_buffer->current();
        len = my_buffer->remain();
      }
      return err;
    }

    int gen_next_log(const int64_t log_id, char* buf, const int64_t len, int64_t& real_len)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        real_len = max(random() % len, 4);
        memset(buf, static_cast<int32_t>(log_id), len);
      }
      return err;
    }
        
    class IObBatchLogHandler
    {
      public:
        IObBatchLogHandler() {}
        virtual ~IObBatchLogHandler() {}
        virtual int handle_log(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                           const char* log_data, const int64_t data_len) = 0;
    };

    int get_log_and_handle(IObBatchLogHandler* log_handler, ObLogGenerator& log_generator) {
      int err = OB_SUCCESS;
      ObLogCursor start_cursor;
      ObLogCursor end_cursor;
      char* buf = NULL;
      int64_t len = 0;
      if (NULL == log_handler)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = log_generator.get_log(start_cursor, end_cursor, buf, len)))
      {
        TBSYS_LOG(ERROR, "get_log()=>%d", err);
      }
      else if (OB_SUCCESS != (err = log_handler->handle_log(start_cursor, end_cursor, buf, len))
               && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "handle_log(log_range=[%ld,%ld], len=%ld)=>%d",
                  start_cursor.log_id_, end_cursor.log_id_, len, err);
      }
      else if (OB_NEED_RETRY == err)
      {}
      else if (OB_SUCCESS != (err = log_generator.commit(end_cursor)))
      {
        TBSYS_LOG(ERROR, "log_generator.commit_log(end_cursor=%s)=>%d", end_cursor.to_str(), err);
      }
      return err;
    }
    
    int gen_log_and_handle(IObBatchLogHandler* log_handler, ObLogGenerator& log_generator, const int64_t end_id) {
      int err = OB_SUCCESS;
      ObLogCursor start_cursor;
      ObLogCursor end_cursor;
      char single_log_buf[1<<10];
      int64_t single_log_buf_len = 0;
      if (NULL == log_handler || 0 > end_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      while(OB_SUCCESS == err && get_end_log_id(log_generator) < end_id) {
        if (OB_SUCCESS != (err = gen_next_log(get_end_log_id(log_generator), single_log_buf, sizeof(single_log_buf), single_log_buf_len)))
        {
          TBSYS_LOG(ERROR, "gen_next_log()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_generator.write_log(OB_LOG_UPS_MUTATOR, single_log_buf, single_log_buf_len))
                 && OB_BUF_NOT_ENOUGH != err)
        {
          TBSYS_LOG(ERROR, "log_mgr.write_log(MUTATOR, len=%ld)=>%d", single_log_buf_len, err);
        }
        else if (OB_BUF_NOT_ENOUGH != err)
        {}
        else if (OB_SUCCESS != (err = get_log_and_handle(log_handler, log_generator)))
        {
          TBSYS_LOG(ERROR, "get_log_and_handle(log_handler=%p, log_generator=%p)=>%d", &log_handler, &log_generator, err);
        }
      }
      if (OB_SUCCESS == err
          && OB_SUCCESS != (err = get_log_and_handle(log_handler, log_generator)))
      {
        TBSYS_LOG(ERROR, "get_log_and_handle(log_handler=%p, log_generator=%p)=>%d", &log_handler, &log_generator, err);
      }
      return err;
    }

    class ObUpsLogMgr2
    {
      private:
        const char* log_dir_;        
        ObLogCursor end_cursor_;
        ObLogGenerator log_generator_;
        ObLogWriterV2 log_writer_;
      public:
        ObLogGenerator& get_log_generator() {
          return log_generator_;
        }
        ObLogWriterV2& get_log_writer() {
          return log_writer_;
        }
      public:
        ObUpsLogMgr2(): log_dir_(NULL), end_cursor_() {}
        ~ObUpsLogMgr2() {}
        bool is_inited() const {
          return NULL != log_dir_;
        }
        int init(const char* log_dir, const int64_t log_file_max_size, int64_t log_buf_size) {
          int err = OB_SUCCESS;
          int64_t log_sync_type = OB_LOG_SYNC;
          if (is_inited())
          {
            err = OB_INIT_TWICE;
          }
          else if (NULL == log_dir || 0 > log_file_max_size || 0 > log_buf_size)
          {
            err = OB_INVALID_ARGUMENT;
          }
          else if (OB_SUCCESS != (err = log_generator_.init(log_buf_size, log_file_max_size)))
          {
            TBSYS_LOG(ERROR, "log_generator.init(log_dir=%s)=>%d", log_dir, err);
          }
          else if (OB_SUCCESS != (err = log_writer_.init(log_dir, LOG_ALIGN-1, log_sync_type)))
          {
            TBSYS_LOG(ERROR, "log_writer.init(log_dir=%s)=>%d", log_dir, err);
          }
          else
          {
            log_dir_ = log_dir;
          }
          return err;
        }

        int start_log(const ObLogCursor& start_cursor) {
          int err = OB_SUCCESS;
          if (!is_inited())
          {
            err = OB_NOT_INIT;
          }
          else if (end_cursor_.is_valid())
          {
            err = OB_INIT_TWICE;
          }
          else if (OB_SUCCESS != (err = log_generator_.start_log(start_cursor)))
          {
            TBSYS_LOG(ERROR, "log_generator.start_log(start_cursor)=>%d", err);
          }
          else if (OB_SUCCESS != (err = log_writer_.start_log(start_cursor)))
          {
            TBSYS_LOG(ERROR, "log_writer.start_log(start_cursor)=>%d", err);
          }
          else
          {
            end_cursor_ = start_cursor;
          }
          return err;
        }

        int64_t get_cursor(ObLogCursor& cursor) const {
          int err = OB_SUCCESS;
          cursor = end_cursor_;
          return err;
        }

        int write_log(LogCommand cmd, char* buf, int64_t len) {
          int err = OB_SUCCESS;
          UNUSED(cmd);
          if (!is_inited() || !end_cursor_.is_valid())
          {
            err = OB_NOT_INIT;
          }
          else if (NULL == buf || 0 > len)
          {
            err = OB_INVALID_ARGUMENT;
          }
          else if (OB_SUCCESS != (err = log_generator_.write_log(OB_LOG_NOP, buf, len))
                   && OB_BUF_NOT_ENOUGH != err)
          {
            TBSYS_LOG(ERROR, "log_generator.write_log()=>%d", err);
          }
          else if (OB_BUF_NOT_ENOUGH != err)
          {}
          else if (OB_SUCCESS != (err = flush_log()))
          {
            TBSYS_LOG(ERROR, "flush_log()=>%d", err);
          }
          return err;
        }

        int switch_log(int64_t& log_file_id) {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = log_generator_.switch_log(log_file_id)))
          {
            TBSYS_LOG(ERROR, "switch_log()=>%d", err);
          }
          else if (OB_SUCCESS != (err = flush_log()))
          {
            TBSYS_LOG(ERROR, "flush_log()=>%d", err);
          }
          return err;
        }

        int flush_log() {
          int err = OB_SUCCESS;
          char* log_buf = NULL;
          int64_t log_buf_len = 0;
          ObLogCursor start_cursor;
          ObLogCursor end_cursor;
          if (!is_inited() || !end_cursor_.is_valid())
          {
            err = OB_NOT_INIT;
          }
          else if (OB_SUCCESS != (err = log_generator_.get_log(start_cursor, end_cursor, log_buf, log_buf_len)))
          {
            TBSYS_LOG(ERROR, "log_generator.get_log()=>%d", err);
          }
          else if (OB_SUCCESS != (err = log_writer_.write_log(log_buf, log_buf_len)))
          {
            TBSYS_LOG(ERROR, "log_writer.write_log(buf=%p[%ld])=>%d", log_buf, log_buf_len, err);
          }
          else if (OB_SUCCESS != (err = log_generator_.commit(end_cursor)))
          {
            TBSYS_LOG(ERROR, "log_generator.commit(end_cursor=%s)=>%d", end_cursor.to_str(), err);
          }
          else if (OB_SUCCESS != (err = log_writer_.get_cursor(end_cursor_)))
          {
            TBSYS_LOG(ERROR, "log_writer.get_log()=>%d", err);
          }
          else
          {
            TBSYS_LOG(INFO, "write log: %s", end_cursor_.to_str());
          }
          return err;
        }
        int write_log(LogCommand cmd) {
          int err = OB_SUCCESS;
          char single_log_buf[1<<10];
          int64_t log_id = get_log_id(*this);
          int64_t log_len = 0;
          UNUSED(cmd);
          if (OB_SUCCESS != (err = gen_next_log(log_id, single_log_buf, sizeof(single_log_buf), log_len)))
          {}
          else if (OB_SUCCESS != (err = write_log(OB_LOG_UPS_MUTATOR, single_log_buf, log_len)))
          {}
          return err;
        }
    };

    int write_log_to(ObUpsLogMgr2& log_mgr, int64_t end_id)
    {
      int err = OB_SUCCESS;
      while(OB_SUCCESS == err && get_log_id(log_mgr) < end_id)
      {
        err = log_mgr.write_log(OB_LOG_UPS_MUTATOR);
      }
      return err;
    }

    class WriteLogHandler : public IObBatchLogHandler
    {
      private:
        ObLogWriterV2& log_writer_;
      public:
        WriteLogHandler(ObLogWriterV2& log_writer): log_writer_(log_writer) {}
        virtual ~WriteLogHandler() {}
        virtual int handle_log(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                           const char* log_data, const int64_t data_len) {
          UNUSED(start_cursor);
          UNUSED(end_cursor);
          //TBSYS_LOG(INFO, "write_log([%ld,%ld])=>%d", start_cursor.log_id_, end_cursor.log_id_);
          return log_writer_.write_log(log_data,  data_len);
        }
    };

    int write_log_to_end(ObLogWriterV2& log_writer, ObLogGenerator& log_generator, const int64_t end_id)
    {
      WriteLogHandler write_log_handler(log_writer);
      return gen_log_and_handle(&write_log_handler, log_generator, end_id);
    }
  }; // end namespace test
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_LOG_UTILS_H__ */
