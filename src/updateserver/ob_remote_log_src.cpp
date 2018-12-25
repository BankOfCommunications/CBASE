/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 *
 */

#include "common/ob_server_getter.h"
#include "ob_remote_log_src.h"
#include "ob_fetched_log.h"
#include "ob_ups_rpc_stub.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    int get_log_from_fetch_server(ObUpsRpcStub* rpc_stub, const ObServer& server, int64_t fetch_timeout, ObFetchLogReq& req,
                                  const int64_t start_id, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      const ObServer null_server;
      ObFetchedLog fetched_log;
      end_id = start_id;
      read_count = 0;
      req.start_id_ = start_id;
      req.max_data_len_ = len;
      fetched_log.set_buf(buf, len);
      if (NULL == rpc_stub || NULL == buf || 0 >= len || 0 > start_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = rpc_stub->fetch_log(server, req, fetched_log, fetch_timeout))
               && OB_RESPONSE_TIME_OUT != err && OB_PACKET_NOT_SENT != err && OB_NEED_RETRY != err)

      {
        TBSYS_LOG(WARN, "rpc_stub->fetch_log(master=%s, start_id=%ld, timeout=%ld)=>%d",
                  server.to_cstring(), start_id, fetch_timeout, err);
      }
      else if (OB_SUCCESS == err)
      {
        req = fetched_log.next_req_;
        end_id = fetched_log.end_id_;
        read_count = fetched_log.data_len_;
        TBSYS_LOG(DEBUG, "rpc_stub->fetch_log(end_id=%ld, read_count=%ld)", end_id, read_count);
      }
      else if (OB_NEED_RETRY != err)
      {
        TBSYS_LOG(WARN, "rpc_stub->fetch_log(end_id=%ld)=>%d", end_id, err);
      }
      else
      {
        TBSYS_LOG(TRACE, "rpc_stub->fetch_log(): need retry");
      }
      return err;
    }

    int make_log_id_uniq(int64_t file_id, char* buf, int64_t len, int64_t& start_id, int64_t& end_id, bool& is_file_end)
    {
      int err = OB_SUCCESS;
      int64_t old_pos = 0;
      int64_t pos = 0;
      ObLogEntry log_entry;
      start_id = 0;
      if (0 >= file_id || NULL == buf || 0 >= len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      while(OB_SUCCESS == err && pos < len && !is_file_end)
      {
        old_pos = pos;
        if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_entry.check_header_integrity()))
        {
          TBSYS_LOG(ERROR, "check_header_integrity()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_entry.check_data_integrity(buf + pos)))
        {
          TBSYS_LOG(ERROR, "check_data_integrity()=>%d", err);
        }
        else
        {
          if (OB_LOG_SWITCH_LOG == log_entry.cmd_)
          {
            is_file_end = true;
          }
          log_entry.set_log_seq(log_entry.seq_ + ((OB_LOG_SWITCH_LOG == log_entry.cmd_) ?file_id : file_id - 1));
          end_id = log_entry.seq_ + 1;
          if (start_id == 0)
          {
            start_id = log_entry.seq_;
          }
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
        {
          TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
        }
        else if (old_pos != pos)
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "old_pos[%ld] != pos[%ld]", old_pos, pos);
        }
        if (OB_SUCCESS == err)
        {
          pos += log_entry.get_log_data_len();
        }
      }
      if (OB_SUCCESS == err && pos != len)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "is_file_end=%s, pos[%ld] != len[%ld]", is_file_end? "true": "false" , pos, len);
      }
      return err;
    }

    int get_log_from_lsync_server_func(bool& registered, ObUpsRpcStub* rpc_stub, const ObServer& server, int64_t timeout,
                                  ObLogCursor& start_cursor, char*& buf, int64_t& len)
    {
      int err = OB_SUCCESS;
      uint64_t file_id = 0;
      uint64_t log_id = 0;
      if (NULL == rpc_stub)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (!registered
               && OB_SUCCESS != (err = rpc_stub->slave_register_standalone(server, (uint64_t)start_cursor.file_id_,
                                                                           (uint64_t)start_cursor.log_id_, file_id, log_id, timeout))
               && OB_RESPONSE_TIME_OUT != err && OB_PACKET_NOT_SENT != err && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "slave_register()=>%d", err);
      }
      else if (OB_SUCCESS != err)
      {
        err = OB_NEED_RETRY;
      }
      else if (OB_SUCCESS == err)
      {
        if (0 == start_cursor.file_id_ && 0 != file_id)
        {
          start_cursor.file_id_ = file_id;
          start_cursor.log_id_ = log_id;
        }
        registered = true;
      }

      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = rpc_stub->fetch_lsync(server, start_cursor.file_id_,
                                                          max(0, start_cursor.log_id_ - start_cursor.file_id_),
                                                          buf, len, timeout))
               && OB_RESPONSE_TIME_OUT != err && OB_PACKET_NOT_SENT != err && OB_NEED_RETRY != err && OB_NOT_REGISTERED != err)
      {
        TBSYS_LOG(WARN, "rpc_stub->fetch_lsync(master=%s, start_cursor=%s, timeout=%ld)=>%d",
                  server.to_cstring(), start_cursor.to_str(), timeout, err);
      }
      else if (OB_NOT_REGISTERED == err)
      {
        registered = false;
        err = OB_NEED_RETRY;
      }
      else if (OB_SUCCESS != err)
      {
        err = OB_NEED_RETRY;
      }
      return err;
    }

    int64_t ObRemoteLogSrc::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      ObServer server;
      if (NULL != server_getter_)
      {
        server_getter_->get_server(server);
      }
      databuff_printf(buf, len, pos, "RemoteLogSrc(master=%s, req=%s[%ld], timeout=%ld)", to_cstring(server), to_cstring(req_), req_seq_, fetch_timeout_);
      return pos;
    }

    // start_cursor一定是有效的
    int ObRemoteLogSrc::get_log_from_lsync_server(const ObServer& server, const ObLogCursor& start_cursor,
                                                  ObLogCursor& end_cursor, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      int64_t start_id = 0;
      bool is_file_end = false;
      char* log_data = NULL;
      ObLogCursor log_cursor = start_cursor;
      end_cursor = start_cursor;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (0 >= start_cursor.log_id_)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = get_log_from_lsync_server_func(registered_to_lsync_server_, rpc_stub_, server,
                                                              fetch_timeout_, log_cursor, log_data, read_count))
               && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "get_from_lsync()=>%d", err);
      }
      else if (OB_NEED_RETRY == err)
      {}
      else if (NULL == log_data || log_cursor.file_id_ != start_cursor.file_id_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get_log_from_lsync(log_data=%p, log_cursor.file_id=%ld, start_cursor.file_id=%ld): UNEXPECTED ERROR", log_data, log_cursor.file_id_, start_cursor.file_id_);
      }
      else if (len < read_count)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "len[%ld] < read_count[%ld]", len, read_count);
      }
      else if (NULL == memcpy(buf, log_data, read_count))
      {
        err = OB_ERR_UNEXPECTED;
      }
      else if (0 >= read_count)
      {
        TBSYS_LOG(INFO, "receive no data from lsync[file_id=%ld, log_id=%ld].", start_cursor.file_id_, start_cursor.log_id_);
      }
      else if (OB_SUCCESS != (err = make_log_id_uniq(start_cursor.file_id_, buf, read_count, start_id, end_cursor.log_id_, is_file_end)))
      {
        TBSYS_LOG(ERROR, "make_log_id_uniq()=>%d", err);
      }
      else if (start_id != start_cursor.log_id_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "fetched_start_id[%ld] != request_log_id[%ld]", start_id, start_cursor.log_id_);
      }
      else if (is_file_end)
      {
        end_cursor.file_id_ = start_cursor.file_id_ + 1;
        end_cursor.offset_ = 0;
      }
      else
      {
        end_cursor.file_id_ = start_cursor.file_id_;
        end_cursor.offset_ = start_cursor.offset_ + read_count;
      }
      return err;
    }

    ObRemoteLogSrc::ObRemoteLogSrc(): registered_to_lsync_server_(false), req_seq_(0), server_getter_(NULL), rpc_stub_(NULL), fetch_timeout_(0)
    {}

    ObRemoteLogSrc::~ObRemoteLogSrc()
    {}

    bool ObRemoteLogSrc::is_inited() const
    {
      return NULL != server_getter_ && NULL != rpc_stub_ && 0 < fetch_timeout_;
    }

    int ObRemoteLogSrc::init(IObServerGetter* server_getter, ObUpsRpcStub* rpc_stub, const int64_t fetch_timeout)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == server_getter || NULL == rpc_stub || 0 >= fetch_timeout)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        server_getter_ = server_getter;
        rpc_stub_ = rpc_stub;
        fetch_timeout_ = fetch_timeout;
      }
      return err;
    }

    bool ObRemoteLogSrc::is_using_lsync() const
    {
      return server_getter_ && common::LSYNC_SERVER == server_getter_->get_type();
    }

    int ObRemoteLogSrc::fill_start_cursor(ObLogCursor& start_cursor)
    {
      int err = OB_SUCCESS;
      const ObServer null_server;
      ObServer server;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (start_cursor.log_id_ > 0)
      {}
      else if (OB_SUCCESS != (err = server_getter_->get_server(server)))
      {
        TBSYS_LOG(ERROR, "get_server()=>%d", err);
      }
      else if (null_server == server)
      {
        err = OB_NEED_RETRY;
      }
      else if (FETCH_SERVER == server_getter_->get_type())
      {
        if (OB_SUCCESS != (err = fill_start_cursor_with_fetch_server(server, start_cursor))
            && OB_NEED_RETRY != err)
        {
          TBSYS_LOG(ERROR, "get_log_from_fetch_server(start_cursor=%s)=>%d", start_cursor.to_str(), err);
        }
      }
      else
      {
        if (OB_SUCCESS != (err = fill_start_cursor_with_lsync_server(server, start_cursor))
            && OB_NEED_RETRY != err)
        {
          TBSYS_LOG(ERROR, "get_log_from_lsync_server(start_cursor=%s)=>%d", start_cursor.to_str(), err);
        }
      }
      return err;
    }

    int ObRemoteLogSrc::fill_start_cursor_with_fetch_server(const ObServer& server, ObLogCursor& start_cursor)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = rpc_stub_->fill_log_cursor(server, start_cursor, fetch_timeout_))
               && OB_RESPONSE_TIME_OUT != err && OB_PACKET_NOT_SENT != err)
      {
        TBSYS_LOG(ERROR, "rpc->fill_log_cursor()=>%d", err);
      }
      else if (OB_RESPONSE_TIME_OUT == err || OB_PACKET_NOT_SENT == err)
      {
        err = OB_NEED_RETRY;
      }
      return err;
    }

    int ObRemoteLogSrc::fill_start_cursor_with_lsync_server(const ObServer& server, ObLogCursor& start_cursor)
    {
      int err = OB_SUCCESS;
      ObLogEntry log_entry;
      int64_t pos = 0;
      char* log_data = NULL;
      int64_t read_count = 0;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = get_log_from_lsync_server_func(registered_to_lsync_server_, rpc_stub_, server,
                                                              fetch_timeout_, start_cursor, log_data, read_count))
               && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "fill_log_cursor_with_lsync()=>%d", err);
      }
      else if (OB_NEED_RETRY == err)
      {}
      else if (0 >= read_count)
      {
        err = OB_NEED_RETRY;
      }
      else if (OB_SUCCESS != (err = log_entry.deserialize(log_data, read_count, pos)))
      {
        TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
      }
      else
      {
        TBSYS_LOG(INFO, "fill_start_cursor(file_id=%ld, log_id=%ld)", start_cursor.file_id_, log_entry.seq_);
        start_cursor.log_id_ = log_entry.seq_ +
          ((OB_LOG_SWITCH_LOG == log_entry.cmd_) ?start_cursor.file_id_ : start_cursor.file_id_ - 1);
      }
      TBSYS_LOG(DEBUG, "fill_start_cursor(start_cursor=%s)=>%d", start_cursor.to_str(), err);
      return err;
    }

    ObFetchLogReq& ObRemoteLogSrc::copy_req(ObFetchLogReq& req)
    {
      SeqLockGuard guard(req_seq_);
      req = req_;
      return req;
    }

    ObFetchLogReq& ObRemoteLogSrc::update_req(ObFetchLogReq& req)
    {
      SeqLockGuard guard(req_seq_);
      req_ = req;
      return req;
    }

// len应该比2M小一些，否则取回的日志加上header可能大于2M。
    int ObRemoteLogSrc::get_log(const ObLogCursor& start_cursor, ObLogCursor& end_cursor, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      const ObServer null_server;
      ObServer server;
      ObFetchLogReq req;
      end_cursor = start_cursor;
      read_count = 0;

      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 >= len || 0 > start_cursor.log_id_)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = server_getter_->get_server(server)))
      {
        TBSYS_LOG(ERROR, "get_server()=>%d", err);
      }
      else if (null_server == server)
      {
        err = OB_NEED_RETRY;
      }
      else if (FETCH_SERVER == server_getter_->get_type())
      {
        end_cursor.file_id_ = 0;
        end_cursor.offset_ = 0;
        if (OB_SUCCESS != (err = get_log_from_fetch_server(rpc_stub_, server, fetch_timeout_, copy_req(req),
                                                           start_cursor.log_id_, end_cursor.log_id_, buf, len, read_count))
            && OB_NEED_RETRY != err)
        {
          TBSYS_LOG(WARN, "get_log_from_fetch_server(start_cursor=%s)=>%d", start_cursor.to_str(), err);
          req.session_.reset();
          update_req(req);
          err = OB_NEED_RETRY;
        }
        else if (OB_SUCCESS == err)
        {
          update_req(req);
          end_cursor.offset_ += read_count;
        }
      }
      else
      {
        if (0 == start_cursor.file_id_)
        {
          err = OB_LOG_SRC_CHANGED;
          TBSYS_LOG(INFO, "fetch_log_with_lsync(start_cursor.file_id == 0): Maybe log src reconfigured.");
        }
        else if (OB_SUCCESS != (err = get_log_from_lsync_server(server, start_cursor, end_cursor, buf, len, read_count))
            && OB_NEED_RETRY != err)
        {
          TBSYS_LOG(ERROR, "get_log_from_lsync_server(start_cursor=%s)=>%d", start_cursor.to_str(), err);
        }
      }
      TBSYS_LOG(DEBUG, "get_log(start_cursor=%s, end_id=%ld, read_count=%ld)=>%d", start_cursor.to_str(), end_cursor.log_id_, read_count, err);
      return err;
    }
  } // end namespace updateserver
} // end namespace oceanbase
