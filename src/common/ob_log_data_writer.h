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
#ifndef __OB_COMMON_OB_LOG_DATA_WRITER_H__
#define __OB_COMMON_OB_LOG_DATA_WRITER_H__
#include "ob_define.h"
#include "ob_log_cursor.h"

namespace oceanbase
{
  namespace common
  {
    class MinAvailFileIdGetter
    {
      public:
        MinAvailFileIdGetter(){}
        virtual ~MinAvailFileIdGetter(){}
        virtual int64_t get() = 0;
    };
    class ObLogDataWriter
    {
      class AppendBuffer
      {
        public:
          static const int64_t DEFAULT_BUF_SIZE = 1<<22;
          AppendBuffer();
          ~AppendBuffer();
          int write(const char* buf, int64_t len, int64_t pos);
          int flush(int fd);
        private:
          int64_t file_pos_;
          char* buf_;
          int64_t buf_end_;
          int64_t buf_limit_;
      };
      public:
        static const int OPEN_FLAG = O_WRONLY | O_DIRECT;
        static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        static const int CREATE_FLAG = OPEN_FLAG | O_CREAT;
        ObLogDataWriter();
        ~ObLogDataWriter();
        int init(const char* log_dir, const int64_t file_size, const int64_t du_percent,
                 const int64_t log_sync_type,
                 MinAvailFileIdGetter* first_useful_file_id_getter);
        int write(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                  const char* data, const int64_t data_len);
        int start_log(const ObLogCursor& cursor);
        int reset();
        int get_cursor(ObLogCursor& cursor) const;
        inline int64_t get_file_size() const {return file_size_;};
        int64_t to_string(char* buf, const int64_t len) const;
      protected:
        int check_eof_after_log_cursor(const ObLogCursor& cursor);
        int prepare_fd(const int64_t file_id);
        int reuse(const char* pool_file, const char* fname);
        const char* select_pool_file(char* fname, const int64_t limit);
      private:
        AppendBuffer write_buffer_;
        const char* log_dir_;
        int64_t file_size_;
        ObLogCursor end_cursor_;
        int64_t log_sync_type_;
        int fd_;
        int64_t cur_file_id_;
        int64_t num_file_to_add_;
        int64_t min_file_id_;
        int64_t min_avail_file_id_;
        MinAvailFileIdGetter* min_avail_file_id_getter_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_LOG_DATA_WRITER_H__ */
