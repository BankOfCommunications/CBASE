/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_run_file.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RUN_FILE_H
#define _OB_RUN_FILE_H 1
#include "common/ob_file.h"
#include "common/ob_array.h"
#include "common/ob_row.h"
namespace oceanbase
{
  namespace sql
  {
    // run file for merge sort
    // support multi-bucket, multi-run in one physical file
    // @note not thread-safe
    class ObRunFile
    {
      public:
        ObRunFile();
        virtual ~ObRunFile();

        int open(const common::ObString &filename);
        int close();
        bool is_opened() const;

        int begin_append_run(const int64_t bucket_idx);
        int append_row(const common::ObString &compact_row);
        int end_append_run();

        int begin_read_bucket(const int64_t bucket_idx, int64_t &run_count);
        /// @return OB_ITER_END when reaching the end of this run
        int get_next_row(const int64_t run_idx, common::ObRow &row);
        int end_read_bucket();
      private:
        // types and constants
        static const int64_t MAGIC_NUMBER = 0x656c69666e7572; // "runfile"
        struct RunTrailer
        {
          int64_t magic_number_;
          int64_t bucket_idx_;
          int64_t prev_run_trailer_pos_;
          int64_t curr_run_size_;
          RunTrailer()
            :magic_number_(MAGIC_NUMBER), bucket_idx_(-1),
             prev_run_trailer_pos_(-1), curr_run_size_(0)
          {
          }
        };
        struct RunBlock: public common::ObFileBuffer
        {
          static const int64_t BLOCK_SIZE = 2*1024*1024LL;
          int64_t run_end_offset_;
          int64_t block_offset_;
          int64_t block_data_size_;
          int64_t next_row_pos_;
          RunBlock();
          ~RunBlock();
          void reset();
          void free_buffer();
          bool need_read_next_block() const;
          bool is_end_of_run() const;
        };
      private:
        // disallow copy
        ObRunFile(const ObRunFile &other);
        ObRunFile& operator=(const ObRunFile &other);
        // function members
        int find_last_run_trailer(const int64_t bucket_idx, RunTrailer *&bucket_info);
        int read_next_run_block(RunBlock &run_block);
        int block_get_next_row(RunBlock &run_block, common::ObRow &row);
        // @return OB_BUF_NOT_ENOUGH or OB_SUCCESS or other errors
        int parse_row(const char* buf, const int64_t buf_len, common::ObString &compact_row, common::ObRow &row);
      private:
        // data members
        common::ObFileAppender file_appender_;
        common::ObFileReader file_reader_;
        common::ObArray<RunTrailer> buckets_last_run_trailer_;
        RunTrailer *curr_run_trailer_;
        common::ObArray<RunBlock> run_blocks_;
        int64_t curr_run_row_count_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RUN_FILE_H */

