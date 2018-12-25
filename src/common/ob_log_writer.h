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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_LOG_WRITER_H_
#define OCEANBASE_COMMON_OB_LOG_WRITER_H_

#include "tblog.h"

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_slave_mgr.h"
#include "ob_log_entry.h"
#include "ob_file.h"
#include "ob_log_cursor.h"
#include "ob_log_generator.h"
#include "ob_log_data_writer.h"

namespace oceanbase
{
  namespace common
  {
    class ObILogWriter
    {
      public:
        virtual ~ObILogWriter() {};
      public:
        virtual int switch_log_file(uint64_t &new_log_file_id) = 0;
        virtual int write_replay_point(uint64_t replay_point) = 0;
    };

    class ObLogWriter : public ObILogWriter
    {
    public:
      static const int64_t LOG_BUFFER_SIZE = OB_MAX_LOG_BUFFER_SIZE;
      static const int64_t LOG_FILE_ALIGN_BIT = 9;
      static const int64_t LOG_FILE_ALIGN_SIZE = 1 << LOG_FILE_ALIGN_BIT;
      static const int64_t LOG_FILE_ALIGN_MASK = LOG_FILE_ALIGN_SIZE - 1;
      static const int64_t DEFAULT_DU_PERCENT = 60;

    public:
      ObLogWriter();
      virtual ~ObLogWriter();

      /// 初始化
      /// @param [in] log_dir 日志数据目录
      /// @param [in] log_file_max_size 日志文件最大长度限制
      /// @param [in] slave_mgr ObSlaveMgr用于同步日志
      int init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr *slave_mgr,
               int64_t log_sync_type, MinAvailFileIdGetter* min_avail_fid_getter = NULL, const ObServer* server=NULL);

      int reset_log();
      //add wangdonghui [ups_replication] 20170220 :b
      int clear();
      //add :e
      virtual int write_log_hook(const bool is_master,
                                 const ObLogCursor start_cursor, const ObLogCursor end_cursor,
                                 const char* log_data, const int64_t data_len)
      {
          UNUSED(is_master);
          UNUSED(start_cursor);
          UNUSED(end_cursor);
          UNUSED(log_data);
          UNUSED(data_len);
          return OB_SUCCESS;
      }
      bool check_log_size(const int64_t size) const { return log_generator_.check_log_size(size); }
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      bool check_mutator_size(const int64_t size, const int64_t max_mutator_size) const
      {
        return log_generator_.check_log_size(size, max_mutator_size);
      }
      bool is_normal_mutator_size(const int64_t size) const { return log_generator_.is_normal_mutator_size(size); }
      //add e
      int start_log(const ObLogCursor& start_cursor);
      int start_log_maybe(const ObLogCursor& start_cursor);
      int get_flushed_cursor(ObLogCursor& log_cursor) const;
      int get_filled_cursor(ObLogCursor& log_cursor) const;
      bool is_all_flushed() const;
      int64_t get_file_size() const {return log_writer_.get_file_size();};
      int64_t to_string(char* buf, const int64_t len) const;

      /// @brief 写日志
      /// write_log函数将日志存入自己的缓冲区, 缓冲区大小LOG_BUFFER_SIZE常量
      /// 首先检查日志大小是否还允许存入一整个缓冲区, 若不够则切日志
      /// 然后将日志内容写入缓冲区
      /// @param [in] log_data 日志内容
      /// @param [in] data_len 长度
      /// @retval OB_SUCCESS 成功
      /// @retval OB_BUF_NOT_ENOUGH 内部缓冲区已满
      /// @retval otherwise 失败
      int write_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

      template<typename T>
      int write_log(const LogCommand cmd, const T& data);

      int write_keep_alive_log();

        int async_flush_log(int64_t& end_log_id, TraceLog::LogBuffer &tlog_buffer = oceanbase::common::TraceLog::get_logbuffer());
        int64_t get_flushed_clog_id();
      /// @brief 将缓冲区中的日志写入磁盘
      /// flush_log首相将缓冲区中的内容同步到Slave机器
      /// 然后写入磁盘
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int flush_log(TraceLog::LogBuffer &tlog_buffer = oceanbase::common::TraceLog::get_logbuffer(),
                    const bool sync_to_slave = true, const bool is_master = true);

      /// @brief 写日志并且写盘
      /// 序列化日志并且写盘
      /// 内部缓冲区原先如有数据, 会被清空
      int write_and_flush_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      /// @brief  log buffer 是否为空
      bool is_buffer_clear()
      {
        return log_generator_.is_clear();
      }
      //add e

      /// @brief 将缓冲区内容写入日志文件
      /// 然后将缓冲区内容刷入磁盘
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int store_log(const char* buf, const int64_t buf_len, const bool sync_to_slave=false);

        void set_disk_warn_threshold_us(const int64_t warn_us);
        void set_net_warn_threshold_us(const int64_t warn_us);
      /// @brief Master切换日志文件
      /// 产生一条切换日志文件的commit log
      /// 同步到Slave机器并等待返回
      /// 关闭当前正在操作的日志文件, 日志序号+1, 打开新的日志文件
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      virtual int switch_log_file(uint64_t &new_log_file_id);
      virtual int write_replay_point(uint64_t replay_point)
      {
        UNUSED(replay_point);
        TBSYS_LOG(WARN, "not implement");
        return common::OB_NOT_IMPLEMENT;
      };

      /// @brief 写一条checkpoint日志并返回当前日志号
      /// 写完checkpoint日志后切日志
      int write_checkpoint_log(uint64_t &log_file_id);

      inline ObSlaveMgr* get_slave_mgr() {return slave_mgr_;}

      inline int64_t get_last_net_elapse() {return last_net_elapse_;}
      inline int64_t get_last_disk_elapse() {return last_disk_elapse_;}
      inline int64_t get_last_flush_log_time() {return last_flush_log_time_;}
      //add wangjiahao [Paxos ups_replication_tmplog] 20150724 :b
      inline int64_t* get_log_term_ptr()
      {
          return log_generator_.get_log_term_ptr();
      }
      //add :e
      //add wangdonghui [ups_replication] 20170420 :b
      inline int64_t log_size()
      {
          return log_generator_.get_log_size();
      }

      //add :e

    protected:
      inline int check_inner_stat() const
      {
        int ret = OB_SUCCESS;
        if (!is_initialized_)
        {
          TBSYS_LOG(ERROR, "ObLogWriter has not been initialized");
          ret = OB_NOT_INIT;
        }
        return ret;
      }

      protected:
        bool is_initialized_;
        ObSlaveMgr *slave_mgr_;
        ObLogGenerator log_generator_;
        ObLogDataWriter log_writer_;
        int64_t net_warn_threshold_us_;
        int64_t disk_warn_threshold_us_;
        int64_t last_net_elapse_;  //上一次写日志网络同步耗时
        int64_t last_disk_elapse_;  //上一次写日志磁盘耗时
        int64_t last_flush_log_time_; // 上次刷磁盘的时间
    };
    template<typename T>
    int ObLogWriter::write_log(const LogCommand cmd, const T& data)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = check_inner_stat()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
      }
      else if (OB_SUCCESS != (ret = log_generator_.write_log(cmd, data))
               && OB_BUF_NOT_ENOUGH != ret)
      {
        TBSYS_LOG(WARN, "log_generator.write_log(cmd=%d, data=%p)=>%d", cmd, &data, ret);
      }
      return ret;
    }

  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_WRITER_H_
