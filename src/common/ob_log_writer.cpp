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

#include "ob_log_writer.h"

#include "ob_trace_log.h"
#include "ob_malloc.h"

using namespace oceanbase::common;

ObLogWriter::ObLogWriter(): is_initialized_(false),
                            net_warn_threshold_us_(5000),
                            disk_warn_threshold_us_(5000),
                            last_net_elapse_(0), last_disk_elapse_(0), last_flush_log_time_(0)
{
}

ObLogWriter::~ObLogWriter()
{
}

int ObLogWriter::init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr *slave_mgr,
                      int64_t log_sync_type, MinAvailFileIdGetter* fufid_getter, const ObServer* server)
{
  int ret = OB_SUCCESS;
  int64_t du_percent = DEFAULT_DU_PERCENT;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogWriter has been initialized");
    ret = OB_INIT_TWICE;
  }
  else if (NULL == log_dir || NULL == slave_mgr)
  {
    TBSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p slave_mgr=%p]", log_dir, slave_mgr);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = log_generator_.init(LOG_BUFFER_SIZE, log_file_max_size, server)))
  {
    TBSYS_LOG(ERROR, "log_generator.init(buf_size=%ld, file_limit=%ld)=>%d", LOG_BUFFER_SIZE, log_file_max_size, ret);
  }
  else if (OB_SUCCESS != (ret = log_writer_.init(log_dir, log_file_max_size, du_percent, log_sync_type, fufid_getter)))
  {
    TBSYS_LOG(ERROR, "log_writer.init(log_dir=%s, file_limit=%ld, du_percent=%ld, sync_type=%ld, fufid_getter=%p)=>%d",
              log_dir, log_file_max_size, du_percent, log_sync_type, fufid_getter, ret);
  }

  if (OB_SUCCESS == ret)
  {
    slave_mgr_ = slave_mgr;
    is_initialized_ = true;
    TBSYS_LOG(INFO, "ObLogWriter initialize successfully[log_dir_=%s log_file_max_size_=%ld"
              " slave_mgr_=%p log_sync_type_=%ld]",
        log_dir, log_file_max_size, slave_mgr_, log_sync_type);
  }

  return ret;
}

int ObLogWriter::reset_log()
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = log_writer_.reset()))
  {
    TBSYS_LOG(ERROR, "log_writer.reset()=>%d", err);
  }
  else if (OB_SUCCESS != (err = log_generator_.reset()))
  {
    TBSYS_LOG(ERROR, "log_generator.reset()=>%d", err);
  }
  return err;
}

//add wangdonghui [ups_replication] 20170220 :b
int ObLogWriter::clear()
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = log_writer_.reset()))
  {
    TBSYS_LOG(ERROR, "log_writer.reset()=>%d", err);
  }
  else if (OB_SUCCESS != (err = log_generator_.clear()))
  {
    TBSYS_LOG(ERROR, "log_generator.reset()=>%d", err);
  }
  return err;
}
//add :e

int ObLogWriter::start_log(const ObLogCursor& log_cursor)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = log_writer_.start_log(log_cursor)))
  {
    TBSYS_LOG(ERROR, "log_writer.start_log(%s)=>%d", to_cstring(log_cursor), err);
  }
  else if (OB_SUCCESS != (err = log_generator_.start_log(log_cursor)))
  {
    TBSYS_LOG(ERROR, "log_generator.start_log(%s)=>%d", to_cstring(log_cursor), err);
  }
  return err;
}

int ObLogWriter::start_log_maybe(const ObLogCursor& start_cursor)
{
  int err = OB_SUCCESS;
  ObLogCursor log_cursor;
  if (log_generator_.is_log_start())
  {
    if (OB_SUCCESS != (err = log_generator_.get_end_cursor(log_cursor)))
    {
      TBSYS_LOG(ERROR, "log_generator.get_end_cursor()=>%d", err);
    }
    else if (!log_cursor.equal(start_cursor))
    {
      err = OB_DISCONTINUOUS_LOG;
      TBSYS_LOG(ERROR, "log_cursor.equal(start_cursor=%s)=>%d", to_cstring(start_cursor), err);
    }
  }
  else if (OB_SUCCESS != (err = start_log(start_cursor)))
  {
    TBSYS_LOG(ERROR, "start_log(%s)=>%d", to_cstring(start_cursor), err);
  }
  return err;
}

int ObLogWriter::get_flushed_cursor(ObLogCursor& log_cursor) const
{
  return log_writer_.get_cursor(log_cursor);
}

int ObLogWriter::write_log(const LogCommand cmd, const char* log_data, const int64_t data_len)
{
  int ret = check_inner_stat();

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.write_log(cmd, log_data, data_len))
           && OB_BUF_NOT_ENOUGH != ret)
  {
    TBSYS_LOG(WARN, "log_generator.write_log(cmd=%d, buf=%p[%ld])=>%d", cmd, log_data, data_len, ret);
  }
  return ret;
}

inline int64_t get_align_padding_size(const int64_t x, const int64_t mask)
{
  return -x & mask;
}

int64_t ObLogWriter::to_string(char* buf, const int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "LogWriter(generator=%s, data_writer=%s)",
                  to_cstring(log_generator_), to_cstring(log_writer_));
  return pos;
}

int ObLogWriter::write_keep_alive_log()
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = log_generator_.gen_keep_alive()))
  {
    TBSYS_LOG(ERROR, "write_keep_alive_log()=>%d", err);
  }
  return err;
}

int64_t ObLogWriter::get_flushed_clog_id()
{
  return slave_mgr_->get_acked_clog_id();
}

int ObLogWriter::get_filled_cursor(ObLogCursor& log_cursor) const
{
  return log_generator_.get_end_cursor(log_cursor);
}

bool ObLogWriter::is_all_flushed() const
{
  ObLogCursor cursor;
  log_generator_.get_start_cursor(cursor);
  return slave_mgr_->get_acked_clog_id() >= cursor.log_id_;
}

int ObLogWriter::async_flush_log(int64_t& end_log_id, TraceLog::LogBuffer &tlog_buffer)
{
  int ret = check_inner_stat();
  int send_err = OB_SUCCESS;
  char* buf = NULL;
  int64_t len = 0;
  ObLogCursor start_cursor;
  ObLogCursor end_cursor;
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.get_log(start_cursor, end_cursor, buf, len)))
  {
    TBSYS_LOG(ERROR, "log_generator.get_log()=>%d", ret);
  }
  else if (len <= 0)
  {}
  else if (0 != (len & ObLogGenerator::LOG_FILE_ALIGN_MASK))
  {
    ret = OB_LOG_NOT_ALIGN;
    TBSYS_LOG(ERROR, "len=%ld cursor=[%s,%s], not align", len, to_cstring(start_cursor), to_cstring(end_cursor));
    while(1);
  }
  else
  {
    int64_t store_start_time_us = tbsys::CTimeUtil::getTime();
    if (OB_SUCCESS != (send_err = slave_mgr_->post_log_to_slave(start_cursor, end_cursor, buf, len)))
    {
      TBSYS_LOG(WARN, "slave_mgr.send_data(buf=%p[%ld], %s)=>%d", buf, len, to_cstring(*this), send_err);
    }
    if (OB_SUCCESS != (ret = log_writer_.write(start_cursor, end_cursor,
                                               buf, len + ObLogGenerator::LOG_FILE_ALIGN_SIZE)))
    {
      TBSYS_LOG(ERROR, "log_writer.write_log(buf=%p[%ld], cursor=[%s,%s])=>%d, maybe disk FULL or Broken",
                buf, len, to_cstring(start_cursor), to_cstring(end_cursor), ret);
    }
    else
    {
      last_disk_elapse_ = tbsys::CTimeUtil::getTime() - store_start_time_us;
      if (last_disk_elapse_ > disk_warn_threshold_us_)
      {
        TBSYS_LOG(WARN, "last_disk_elapse_[%ld] > disk_warn_threshold_us[%ld], cursor=[%s,%s], len=%ld",
                  last_disk_elapse_, disk_warn_threshold_us_, to_cstring(start_cursor), to_cstring(end_cursor), len);
      }
    }
  }
  FILL_TRACE_BUF(tlog_buffer, "write_log disk=%ld net=%ld len=%ld log=%ld:%ld",
                 last_disk_elapse_, last_net_elapse_, len,
                 start_cursor.log_id_, end_cursor.log_id_);
  if (OB_SUCCESS != ret)
  {}
  else if (OB_SUCCESS != (ret = log_generator_.commit(end_cursor)))
  {
    TBSYS_LOG(ERROR, "log_generator.commit(end_cursor=%s)", to_cstring(end_cursor));
  }
  else if (OB_SUCCESS != (ret = write_log_hook(true, start_cursor, end_cursor, buf, len)))
  {
    TBSYS_LOG(ERROR, "write_log_hook(log_id=[%ld,%ld))=>%d", start_cursor.log_id_, end_cursor.log_id_, ret);
  }
  else if (len > 0)
  {
    last_flush_log_time_ = tbsys::CTimeUtil::getTime();
  }
  end_log_id = end_cursor.log_id_;
  return ret;
}

int ObLogWriter::flush_log(TraceLog::LogBuffer &tlog_buffer, const bool sync_to_slave, const bool is_master)
{
  int ret = check_inner_stat();
  int send_err = OB_SUCCESS;
  char* buf = NULL;
  int64_t len = 0;
  ObLogCursor start_cursor;
  ObLogCursor end_cursor;
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.get_log(start_cursor, end_cursor, buf, len)))
  {
    TBSYS_LOG(ERROR, "log_generator.get_log()=>%d", ret);
  }
  else if (len <= 0)
  {}
  else
  {
    int64_t store_start_time_us = tbsys::CTimeUtil::getTime();
    if (sync_to_slave)
    {
      if (OB_SUCCESS != (send_err = slave_mgr_->post_log_to_slave(start_cursor, end_cursor, buf, len)))
      {
        TBSYS_LOG(WARN, "slave_mgr.send_data(buf=%p[%ld], %s)=>%d", buf, len, to_cstring(*this), send_err);
      }
    }
    if (OB_SUCCESS != (ret = log_writer_.write(start_cursor, end_cursor,
                                               buf, len + ObLogGenerator::LOG_FILE_ALIGN_SIZE)))
    {
      TBSYS_LOG(ERROR, "log_writer.write_log(buf=%p[%ld], cursor=[%s,%s])=>%d, maybe disk FULL or Broken",
                buf, len, to_cstring(start_cursor), to_cstring(end_cursor), ret);
    }
    else
    {
      last_disk_elapse_ = tbsys::CTimeUtil::getTime() - store_start_time_us;
      if (last_disk_elapse_ > disk_warn_threshold_us_)
      {
        TBSYS_LOG(TRACE, "last_disk_elapse_[%ld] > disk_warn_threshold_us[%ld], cursor=[%s,%s], len=%ld",
                  last_disk_elapse_, disk_warn_threshold_us_, to_cstring(start_cursor), to_cstring(end_cursor), len);
      }
    }
    if (sync_to_slave)
    {
      int64_t delay = -1;
      if (OB_SUCCESS != (send_err = slave_mgr_->wait_post_log_to_slave(buf, len, delay)))
      {
        TBSYS_LOG(ERROR, "slave_mgr.send_data(buf=%p[%ld], cur_write=[%s,%s], %s)=>%d", buf, len, to_cstring(start_cursor), to_cstring(end_cursor), to_cstring(*this), send_err);
      }
      else if (delay >= 0)
      {
        last_net_elapse_ = delay;
        if (last_net_elapse_ > net_warn_threshold_us_)
        {
          TBSYS_LOG(TRACE, "last_net_elapse_[%ld] > net_warn_threshold_us[%ld]", last_net_elapse_, net_warn_threshold_us_);
        }
      }
    }
  }
  FILL_TRACE_BUF(tlog_buffer, "write_log disk=%ld net=%ld len=%ld log=%ld:%ld",
                 last_disk_elapse_, last_net_elapse_, len,
                 start_cursor.log_id_, end_cursor.log_id_);
  if (OB_SUCCESS != ret)
  {}
  else if (OB_SUCCESS != (ret = log_generator_.commit(end_cursor)))
  {
    TBSYS_LOG(ERROR, "log_generator.commit(end_cursor=%s)", to_cstring(end_cursor));
  }
  else if (OB_SUCCESS != (ret = write_log_hook(is_master, start_cursor, end_cursor, buf, len)))
  {
    TBSYS_LOG(ERROR, "write_log_hook(log_id=[%ld,%ld))=>%d", start_cursor.log_id_, end_cursor.log_id_, ret);
  }
  else if (len > 0)
  {
    last_flush_log_time_ = tbsys::CTimeUtil::getTime();
  }
  return ret;
}

int ObLogWriter::write_and_flush_log(const LogCommand cmd, const char* log_data, const int64_t data_len)
{
  int ret = check_inner_stat();

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (!log_generator_.is_clear())
  {
    ret = OB_LOG_NOT_CLEAR;
    TBSYS_LOG(ERROR, "log_buffer not empty.");
  }
  else if (OB_SUCCESS != (ret = write_log(cmd, log_data, data_len)))
  {
    TBSYS_LOG(ERROR, "write_log(cmd=%d, log_data=%p[%ld])", cmd, log_data, data_len);
  }
  else if (OB_SUCCESS != (ret = flush_log(TraceLog::get_logbuffer())))
  {
    TBSYS_LOG(INFO, "log write write and flush log, log_cmd=%d", cmd);
  }
  return ret;
}

int ObLogWriter::store_log(const char* buf, const int64_t buf_len, const bool sync_to_slave)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_inner_stat()))
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.fill_batch(buf, buf_len)))
  {
    TBSYS_LOG(ERROR, "log_generator.fill_batch(%p[%ld])=>%d", buf, buf_len, ret);
  }
  else if (OB_SUCCESS != (ret = flush_log(TraceLog::get_logbuffer(), sync_to_slave, false)))
  {
    TBSYS_LOG(ERROR, "flush_log(buf=%p[%ld],sync_slave=%s)=>%d", buf, buf_len, STR_BOOL(sync_to_slave), ret);
  }
  return ret;
}

int ObLogWriter::switch_log_file(uint64_t &new_log_file_id)
{
  int ret = check_inner_stat();
  new_log_file_id = 0;
  TBSYS_LOG(INFO, "log write switch log file");
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = flush_log(TraceLog::get_logbuffer())))
  {
    TBSYS_LOG(ERROR, "flush_log()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.switch_log((int64_t&)new_log_file_id)))
  {
    TBSYS_LOG(ERROR, "log_generator.switch_log()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = flush_log(TraceLog::get_logbuffer())))
  {
    TBSYS_LOG(ERROR, "flush_log()=>%d", ret);
  }
  return ret;
}

int ObLogWriter::write_checkpoint_log(uint64_t &cur_log_file_id)
{
  int ret = check_inner_stat();
  uint64_t new_log_file_id = 0;
  cur_log_file_id = 0;

  TBSYS_LOG(INFO, "wirte_checkpoint_log");
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = flush_log(TraceLog::get_logbuffer())))
  {
    TBSYS_LOG(ERROR, "flush_log()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = log_generator_.check_point((int64_t&)cur_log_file_id)))
  {
    TBSYS_LOG(ERROR, "log_generator.check_point()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = switch_log_file(new_log_file_id)))
  {
    TBSYS_LOG(ERROR, "switch_log_file()=>%d", ret);
  }
  return ret;
}

void ObLogWriter::set_disk_warn_threshold_us(const int64_t warn_us)
{
  disk_warn_threshold_us_ = warn_us;
}

void ObLogWriter::set_net_warn_threshold_us(const int64_t warn_us)
{
  net_warn_threshold_us_ = warn_us;
}
