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

#ifndef OCEANBASE_COMMON_OB_LOG_READER_H_
#define OCEANBASE_COMMON_OB_LOG_READER_H_

#include "ob_log_entry.h"
#include "ob_single_log_reader.h"
#include "ob_atomic.h"
#include "ob_log_cursor.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 日志读取类, 从一个指定的日志id开始, 遇到日志文件结束, 则打开下一个日志文件
     * 主要有两种使用方法:
     *   1. Master启动时回放日志, 读到所有日志结束则回放完成
     *   2. Slave的日志回放线程使用: Slave采用一个线程同步日志, 另一个线程回放日志时,
     *      回放线程不停追赶日志文件, 当读到日志结束时, 应该等待小段时间后, 再次读日志
     */
    class ObLogReader
    {
    public:
      static const int64_t WAIT_TIME = 1000000; //us
      static const int FAIL_TIMES = 60;
    public:
      ObLogReader();
      virtual ~ObLogReader();

      /**
       * 初始化
       * @param [in] reader 读单个文件的单元
       * @param [in] log_dir 日志目录
       * @param [in] log_file_id_start 日志读取起始文件id
       * @param [in] log_seq 日志读取上一条日志的seq
       * @param [in] is_wait 在打开文件和读取数据出错时，是否重试
       */
      int init(ObSingleLogReader *reader, const char* log_dir,
          const uint64_t log_file_id_start, const uint64_t log_seq,
          bool is_wait);

      /**
       * @brief 读日志
       * 遇到SWITCH_LOG日志时直接打开下一个日志文件
       * 在打开下一个日志文件时, 如果遇到文件不存在, 可能是产生日志的地方正在切文件
       * 等待1ms后重试, 最多重试10次, 如果还不存在则报错
       * @return OB_SUCCESS 成功
       * @return OB_READ_NOTHING 没有读取到内容, 可能是日志结束了,
       *                         也可能是日志正在产生, 读到了中间状态数据
       *                         由调用者根据自己逻辑做不同处理
       * @return otherwise 失败
       */
      int read_log(LogCommand &cmd, uint64_t &seq, char* &log_data, int64_t &data_len);
      //重新打开一个新的文件，并定位到当前日志的下一位
      int reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start);

      inline void set_max_log_file_id(uint64_t max_log_file_id)
      {
        atomic_exchange(&max_log_file_id_, max_log_file_id);
        has_max_ = true;
      }

      inline uint64_t get_max_log_file_id() const
      {
        return max_log_file_id_;
      }

      inline bool get_has_max() const
      {
        return has_max_;
      }

      inline void set_has_no_max()
      {
        has_max_ = false;
      }
      inline uint64_t get_cur_log_file_id() const
      {
        return cur_log_file_id_;
      }

      inline uint64_t get_cur_log_file_id()
      {
        return cur_log_file_id_;
      }
      inline uint64_t get_last_log_seq_id()
      {
        return cur_log_seq_id_;
      }
      inline uint64_t get_last_log_offset()
      {
        return NULL != log_file_reader_? log_file_reader_->get_last_log_offset(): 0;
      }
        inline int get_cursor(common::ObLogCursor& cursor)
      {
        int err = OB_SUCCESS;
        if (NULL == log_file_reader_)
        {
          err = OB_READ_NOTHING;
        }
        else
        {
          cursor.file_id_ = cur_log_file_id_;
          cursor.log_id_ = cur_log_seq_id_;
          cursor.offset_ = log_file_reader_->get_last_log_offset();
        }
        return err;
      }
        inline int get_next_cursor(common::ObLogCursor& cursor)
      {
        int err = OB_SUCCESS;
        if (NULL == log_file_reader_)
        {
          err = OB_READ_NOTHING;
        }
        else
        {
          cursor.file_id_ = cur_log_file_id_;
          cursor.log_id_ = cur_log_seq_id_  + 1;
          cursor.offset_ = log_file_reader_->get_last_log_offset();
        }
        return err;
      }
    private:
      int seek(uint64_t log_seq);

      int open_log_(const uint64_t log_file_id, const uint64_t last_log_seq = 0);

      int read_log_(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len);

    private:
      uint64_t cur_log_file_id_;
      uint64_t cur_log_seq_id_;
      uint64_t max_log_file_id_;
      ObSingleLogReader* log_file_reader_;
      bool is_initialized_;
      bool is_wait_;
      bool has_max_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_READER_H_
