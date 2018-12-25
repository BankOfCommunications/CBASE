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

#include "ob_commit_log_receiver.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ObCommitLogReceiver::ObCommitLogReceiver()
{
  log_writer_ = NULL;
  base_server_ = NULL;
  time_warn_us_ = 0;
  is_initialized_ = false;
}

int ObCommitLogReceiver::init(common::ObLogWriter *log_writer, common::ObBaseServer *base_server, ObUpsSlaveMgr *slave_mgr, const int64_t time_warn_us)
{
  int ret = OB_SUCCESS;

  if (NULL == log_writer || NULL == base_server || NULL == slave_mgr)
  {
    TBSYS_LOG(WARN, "Parameters are invalid, log_writer=%p base_server=%p", log_writer, base_server);
  }
  else
  {
    log_writer_ = log_writer;
    slave_mgr_ = slave_mgr;
    base_server_ = base_server;
    time_warn_us_ = time_warn_us;
    is_initialized_ = true;
  }

  return ret;
}

int ObCommitLogReceiver::receive_log(const int32_t version, common::ObDataBuffer& in_buff,
    easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int ret = OB_SUCCESS;

  if (!log_writer_->get_is_log_start())
  {
    ret = start_log(in_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "start log_writer error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    uint64_t log_id;
    uint64_t log_seq;

    ret = write_data(in_buff.get_data() + in_buff.get_position(), in_buff.get_limit() - in_buff.get_position(), log_id, log_seq);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "decomposite log entry error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = end_receiving_log(version, req, channel_id, out_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "flush log error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ret=%d", ret);
  }

  return ret;
}

int ObCommitLogReceiver::start_log(ObDataBuffer& buff)
{
  int ret = OB_SUCCESS;

  ObLogEntry entry;
  ret = entry.deserialize(buff.get_data(), buff.get_limit(), buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObLogEntry deserialize error, ret=%d", ret);
  }
  else
  {
    if (OB_LOG_SWITCH_LOG != entry.cmd_)
    {
      TBSYS_LOG(ERROR, "when starting log, the log entry must be SWITCH_LOG");
    }
    else
    {
      uint64_t log_id = 0;
      ret = serialization::decode_i64(buff.get_data(), buff.get_limit(),
                buff.get_position(), (int64_t*)&log_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "decode_i64 log_id error, ret=%d", ret);
      }
      else
      {
        ret = log_writer_->start_log(log_id, entry.seq_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "start_log error, log_id=%lu ret=%d", log_id, ret);
        }
      }
    }
  }

  return ret;
}

int ObCommitLogReceiver::write_data(const char* log_data, const int64_t data_len, uint64_t &log_id, uint64_t &log_seq)
{
  int ret = OB_SUCCESS;
  int64_t switch_begin = 0;

  ret = check_inner_stat();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObCommitLogReceiver has not initialized");
  }

  if (NULL == log_data || data_len <= 0)
  {
    TBSYS_LOG(WARN, "Parameter is invalid, log_data=%p data_len=%ld", log_data, data_len);
    ret = OB_INVALID_ARGUMENT;
  }

  bool switch_log_flag = false;
  int64_t pos = 0;
  int64_t switch_begin_tmp = 0;
  while (OB_SUCCESS == ret && pos < data_len)
  {
    ObLogEntry log_ent;
    switch_begin_tmp = pos;
    ret = log_ent.deserialize(log_data, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObLogEntry deserialize error, ret=%d", ret);
    }
    else
    {
      log_seq = log_ent.seq_;

      // check switch_log
      if (OB_LOG_SWITCH_LOG == log_ent.cmd_)
      {
        switch_begin = switch_begin_tmp;
        if ((pos + log_ent.get_log_data_len()) != data_len)
        {
          TBSYS_LOG(ERROR, "swith_log is not at the end, this should not happen, "
              "pos=%ld log_data_len=%d data_len=%ld", pos, log_ent.get_log_data_len(), data_len);
          hex_dump(log_data, static_cast<int32_t>(data_len), TBSYS_LOG_LEVEL_WARN);
          ret = OB_ERROR;
        }
        else
        {
          ret = serialization::decode_i64(log_data, data_len, pos, (int64_t*)&log_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "decode_i64 log_id error, ret=%d", ret);
          }
          else
          {
            switch_log_flag = true;
          }
        }
      }
      pos += log_ent.get_log_data_len();
    }
  }

  if (OB_SUCCESS == ret && data_len > 0)
  {
    int64_t store_start_time = tbsys::CTimeUtil::getTime();
    ret = log_writer_->store_log(log_data, data_len);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObLogWriter store_log error, ret=%d", ret);
    }
    else
    {
      log_writer_->set_cur_log_seq(log_seq);
    }

    int64_t store_elapse = tbsys::CTimeUtil::getTime() - store_start_time;
    if (store_elapse > time_warn_us_)
    {
      TBSYS_LOG(WARN, "store log time is too long, store_time=%ld cur_time=%ld log_size=%ld",
                store_elapse, tbsys::CTimeUtil::getTime(), data_len);
    }
  }
  //send log to slave_slave
  // if (OB_SUCCESS == ret)
  // {
  //   ret = slave_mgr_->send_data(log_data, data_len, log_id, switch_log_flag, data_len - switch_begin);
  //   if (OB_SUCCESS != ret)
  //   {
  //     TBSYS_LOG(WARN, "fail to send data to slave");
  //   }
  // }

  if (OB_SUCCESS == ret && switch_log_flag)
  {
    ret = log_writer_->switch_to_log_file(log_id + 1);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "switch_to_log_file error, log_id=%lu ret=%d", log_id, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "switch_to_log_file log_id=%lu log_seq=%lu", log_id, log_seq);
      ++log_id;
    }
  }

  return ret;
}

int ObCommitLogReceiver::start_receiving_log()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObCommitLogReceiver::end_receiving_log(const int32_t version, easy_request_t* req, const int32_t channel_id, common::ObDataBuffer& out_buff)
{
  int ret = OB_SUCCESS;

  ret = log_writer_->flush_log();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObLogWriter flush_log error, ret=%d", ret);
  }
  else
  {
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObResultCode serialize error, ret=%d", ret);
    }
    else
    {
      ret = base_server_->send_response(OB_SEND_LOG_RES, version, out_buff, req, channel_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "send_response error, ret=%d", ret);
      }
    }

  }

  return ret;
}

