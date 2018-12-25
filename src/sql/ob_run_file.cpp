/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_run_file.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_run_file.h"
#include "common/ob_compact_cell_iterator.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObRunFile::ObRunFile()
  :curr_run_trailer_(NULL), curr_run_row_count_(0)
{
}

ObRunFile::~ObRunFile()
{
  for (int32_t i = 0; i < run_blocks_.count(); ++i)
  {
    run_blocks_.at(i).free_buffer();
  } // end for
  run_blocks_.clear();
}

int ObRunFile::open(const ObString &filename)
{
  int ret = OB_SUCCESS;
  if (file_appender_.is_opened()
      || file_reader_.is_opened())
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(ERROR, "file is opened");
  }
  else if (OB_SUCCESS != (ret = file_appender_.open(filename, true,
                                                    true, true)))
  {
    TBSYS_LOG(WARN, "failed to open file, err=%d name=%.*s",
              ret, filename.length(), filename.ptr());
  }
  else if (OB_SUCCESS != (ret = file_reader_.open(filename, true)))
  {
    file_appender_.close();
    TBSYS_LOG(WARN, "reader failed to open file, err=%d name=%.*s",
              ret, filename.length(), filename.ptr());
  }
  else
  {
    TBSYS_LOG(INFO, "open run file, name=%.*s", filename.length(), filename.ptr());
  }
  return ret;
}

int ObRunFile::close()
{
  int ret = OB_SUCCESS;
  file_appender_.close();
  file_reader_.close();
  for (int32_t i = 0; i < run_blocks_.count(); ++i)
  {
    run_blocks_.at(i).free_buffer();
  } // end for
  run_blocks_.clear();
  curr_run_trailer_ = NULL;
  buckets_last_run_trailer_.clear();
  return ret;
}

bool ObRunFile::is_opened() const
{
  return file_appender_.is_opened() && file_reader_.is_opened();
}

int ObRunFile::find_last_run_trailer(const int64_t bucket_idx, RunTrailer *&bucket_info)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int32_t i = 0; i < buckets_last_run_trailer_.count(); ++i)
  {
    if (bucket_idx == buckets_last_run_trailer_.at(i).bucket_idx_)
    {
      bucket_info = &buckets_last_run_trailer_.at(i);
      ret = OB_SUCCESS;
      break;
    }
  } // end for
  return ret;
}

int ObRunFile::begin_append_run(const int64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (!is_opened())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "run file not opened");
  }
  else if (OB_SUCCESS != (ret = find_last_run_trailer(bucket_idx, curr_run_trailer_)))
  {
    RunTrailer run_trailer;
    run_trailer.bucket_idx_ = bucket_idx;
    run_trailer.prev_run_trailer_pos_ = -1; // we are the first run of this bucket
    run_trailer.curr_run_size_ = 0;
    if (OB_SUCCESS != (ret = buckets_last_run_trailer_.push_back(run_trailer)))
    {
      TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
    }
    else
    {
      curr_run_trailer_ = &buckets_last_run_trailer_.at(static_cast<int32_t>(buckets_last_run_trailer_.count()-1));
      TBSYS_LOG(INFO, "begin append run for bucket=%ld", bucket_idx);
    }
  }
  else
  {
    curr_run_trailer_->curr_run_size_ = 0;
    curr_run_row_count_ = 0;
    TBSYS_LOG(INFO, "begin append run for bucket=%ld", bucket_idx);
  }
  return ret;
}

int ObRunFile::append_row(const common::ObString &compact_row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = file_appender_.append(compact_row.ptr(), compact_row.length(), false)))
  {
    TBSYS_LOG(WARN, "failed to append file, err=%d", ret);
  }
  else
  {
    ++curr_run_row_count_;
    curr_run_trailer_->curr_run_size_ += compact_row.length();
  }
  return ret;
}

int ObRunFile::end_append_run()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = file_appender_.fsync()))
  {
    TBSYS_LOG(WARN, "failed to fsync run file, err=%d", ret);
  }
  else
  {
    int64_t trailer_pos = file_appender_.get_file_pos();
    if (OB_SUCCESS != (ret = file_appender_.append(curr_run_trailer_, sizeof(*curr_run_trailer_), false)))
    {
      TBSYS_LOG(WARN, "failed to write run trailer, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = file_appender_.fsync()))
    {
      TBSYS_LOG(WARN, "failed to fsync run file, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "end append run, bucket=%ld row_count=%ld, run_trailer_pos=%ld run_size=%ld",
                curr_run_trailer_->bucket_idx_, curr_run_row_count_, trailer_pos, curr_run_trailer_->curr_run_size_);
      curr_run_trailer_->prev_run_trailer_pos_ = trailer_pos;
      curr_run_trailer_->curr_run_size_ = 0;
      curr_run_trailer_ = NULL;
      curr_run_row_count_ = 0;
    }
  }
  return ret;
}

ObRunFile::RunBlock::RunBlock()
  :run_end_offset_(0), block_offset_(0), block_data_size_(0), next_row_pos_(0)
{
}

ObRunFile::RunBlock::~RunBlock()
{
  reset();
}

void ObRunFile::RunBlock::reset()
{
  buffer_ = NULL;     // don't free the buffer
  buffer_size_ = 0;
  base_pos_ = 0;
  run_end_offset_ = 0;
  block_offset_ = 0;
  block_data_size_ = 0;
  next_row_pos_ = 0;
}

void ObRunFile::RunBlock::free_buffer()
{
  if (NULL != buffer_)
  {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

bool ObRunFile::RunBlock::is_end_of_run() const
{
  return (next_row_pos_ >= block_data_size_) && (block_offset_ + block_data_size_ >= run_end_offset_);
}

int ObRunFile::begin_read_bucket(const int64_t bucket_idx, int64_t &run_count)
{
  int ret = OB_SUCCESS;
  run_count = 0;
  RunTrailer *run_trailer = NULL;
  if (0 != run_blocks_.count())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "run blocks is not empty");
  }
  else if (OB_SUCCESS != (ret = find_last_run_trailer(bucket_idx, run_trailer)))
  {
    TBSYS_LOG(ERROR, "no run for this bucket=%ld", bucket_idx);
  }
  else
  {
    // read the first block of every run
    int64_t run_trailer_pos = run_trailer->prev_run_trailer_pos_;
    TBSYS_LOG(INFO, "last run trailer, bucket_idx=%ld pos=%ld", bucket_idx, run_trailer_pos);
    RunBlock run_block;
    int64_t read_size = 0;
    while (OB_SUCCESS == ret
           && 0 < run_trailer_pos)
    {
      if (OB_SUCCESS != (ret = run_block.assign(run_block.BLOCK_SIZE, FileComponent::DirectFileReader::DEFAULT_ALIGN_SIZE)))
      {
        TBSYS_LOG(ERROR, "failed to alloc block, err=%d", ret);
      }
      // read the run trailer
      else if (OB_SUCCESS != (ret = file_reader_.pread(sizeof(RunTrailer), run_trailer_pos, run_block, read_size))
               || sizeof(RunTrailer) != read_size)
      {
        TBSYS_LOG(WARN, "failed to read run file, err=%d offset=%ld read_size=%ld",
                  ret, run_trailer_pos, read_size);
        ret = OB_IO_ERROR;
      }
      else
      {
        run_trailer = reinterpret_cast<RunTrailer*>(run_block.get_buffer()+run_block.get_base_pos());
        if (MAGIC_NUMBER != run_trailer->magic_number_)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "invalid run trailer, data corrupted, magic=%lX", run_trailer->magic_number_);
        }
        else if (bucket_idx != run_trailer->bucket_idx_)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "invalid bucket idx=%ld expected=%ld", run_trailer->bucket_idx_, bucket_idx);
        }
        else if (0 > run_trailer_pos - run_trailer->curr_run_size_)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "invalid run size, trailer_pos=%ld run_size=%ld", run_trailer_pos,
                    run_trailer->curr_run_size_);
        }
        else
        {
          run_block.run_end_offset_ = run_trailer_pos;
          run_block.block_offset_ = run_trailer_pos - run_trailer->curr_run_size_;
          run_block.block_data_size_ = run_trailer->curr_run_size_ < run_block.BLOCK_SIZE ?
            run_trailer->curr_run_size_ : run_block.BLOCK_SIZE;
          run_block.next_row_pos_ = 0;

          run_trailer_pos = run_trailer->prev_run_trailer_pos_; // update the previous run trailer
          TBSYS_LOG(INFO, "run trailer, bucket_idx=%ld prev_trailer_pos=%ld curr_run_size=%ld",
                    bucket_idx, run_trailer_pos, run_trailer->curr_run_size_);
          // read the first data block
          if (OB_SUCCESS != (ret = file_reader_.pread(run_block.block_data_size_, run_block.block_offset_, run_block, read_size))
              || read_size != run_block.block_data_size_)
          {
            TBSYS_LOG(WARN, "failed to read run file, err=%d offset=%ld read_size=%ld data_size=%ld",
                      ret, run_block.block_offset_, read_size, run_block.block_data_size_);
            ret = OB_IO_ERROR;
          }
          else if (OB_SUCCESS != (ret = run_blocks_.push_back(run_block)))
          {
            TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "read run first block, offset=%ld size=%ld", run_block.block_offset_, read_size);
            ++run_count;
            run_block.reset();
          }
        }
      }
    } // end while
  }
  return ret;
}

/// @return OB_ITER_END when reaching the end of this run
int ObRunFile::get_next_row(const int64_t run_idx, common::ObRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t run_count = run_blocks_.count();
  if (run_idx >= run_count || 0 >= run_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid run idx=%ld count=%ld", run_idx, run_count);
  }
  else
  {
    RunBlock &run_block = run_blocks_.at(static_cast<int32_t>(run_count - run_idx - 1));
    if (run_block.is_end_of_run())
    {
      TBSYS_LOG(INFO, "reach end of run, run_idx=%ld", run_idx);
      ret = OB_ITER_END;
    }
    else if (OB_SUCCESS != (ret = block_get_next_row(run_block, row)))
    {
      TBSYS_LOG(WARN, "failed to get the next row from the block, err=%d run_idx=%ld", ret, run_idx);
    }
  }
  return ret;
}

int ObRunFile::block_get_next_row(RunBlock &run_block, common::ObRow &row)
{
  int ret = OB_SUCCESS;
  if (run_block.next_row_pos_ >= run_block.block_data_size_)
  {
    if (OB_SUCCESS != (ret = read_next_run_block(run_block)))
    {
      TBSYS_LOG(WARN, "failed to read next block, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObString compact_row;
    if (OB_SUCCESS != (ret = parse_row(run_block.get_buffer()+run_block.get_base_pos()+run_block.next_row_pos_,
                                       run_block.block_data_size_ - run_block.next_row_pos_, compact_row, row)))
    {
      if (OB_BUF_NOT_ENOUGH == ret
          || OB_ITER_END == ret)
      {
        if (OB_SUCCESS != (ret = read_next_run_block(run_block)))
        {
          TBSYS_LOG(WARN, "failed to read next block, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = parse_row(run_block.get_buffer()+run_block.get_base_pos()+run_block.next_row_pos_,
                                                run_block.block_data_size_ - run_block.next_row_pos_,
                                                compact_row, row)))
        {
          TBSYS_LOG(WARN, "failed to get next row after read new block, err=%d", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      run_block.next_row_pos_ += compact_row.length();
    }
  }
  return ret;
}

int ObRunFile::parse_row(const char* buf, const int64_t buf_len, ObString &compact_row, common::ObRow &row)
{
  int ret = OB_SUCCESS;
  ObCompactCellIterator cell_reader;
  ObString input_buffer;
  input_buffer.assign_ptr(const_cast<char*>(buf), static_cast<int32_t>(buf_len));
  cell_reader.init(input_buffer);
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell;
  bool is_row_finished = false;
  int64_t cell_idx = 0;
  while(OB_SUCCESS == (ret = cell_reader.next_cell()))
  {
    if (OB_SUCCESS != (ret = cell_reader.get_cell(column_id, cell, &is_row_finished, &compact_row)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (is_row_finished)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != (ret = row.raw_set_cell(cell_idx++, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
      break;
    }
  }
  if (OB_SUCCESS == ret && is_row_finished)
  {
    if (cell_idx != row.get_column_num())
    {
      TBSYS_LOG(ERROR, "corrupted row data, col_num=%ld cell_num=%ld", row.get_column_num(), cell_idx);
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObRunFile::read_next_run_block(RunBlock &run_block)
{
  int ret = OB_SUCCESS;
  const int64_t read_offset = run_block.block_offset_ + run_block.next_row_pos_;
  const int64_t count = (run_block.run_end_offset_ - read_offset < run_block.BLOCK_SIZE) ?
    (run_block.run_end_offset_ - read_offset) : run_block.BLOCK_SIZE;
  run_block.block_data_size_ = count;
  run_block.block_offset_ = read_offset;
  run_block.next_row_pos_ = 0;
  // read the next data block
  int64_t read_size = 0;
  if (OB_SUCCESS != (ret = file_reader_.pread(run_block.block_data_size_, run_block.block_offset_, run_block, read_size))
      || read_size != run_block.block_data_size_)
  {
    TBSYS_LOG(WARN, "failed to read run file, err=%d offset=%ld read_size=%ld data_size=%ld",
              ret, run_block.block_offset_, read_size, run_block.block_data_size_);
    ret = OB_IO_ERROR;
  }
  else
  {
    TBSYS_LOG(DEBUG, "read block, size=%ld offset=%ld", read_size, run_block.block_offset_);
  }
  return ret;
}

int ObRunFile::end_read_bucket()
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; i < run_blocks_.count(); ++i)
  {
    run_blocks_.at(i).free_buffer();
  } // end for
  run_blocks_.clear();
  return ret;
}
