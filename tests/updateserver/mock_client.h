/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_client.h,v 0.1 2010/10/08 16:09:06 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_MOCK_CLIENT_H__
#define __OCEANBASE_CHUNKSERVER_MOCK_CLIENT_H__

#include "easy_io.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/thread_buffer.h"
#include "common/ob_bloomfilter.h"
#include "common/bloom_filter.h"
#include "common/ob_login_mgr.h"
#include "common/ob_token.h"
#include "common/ob_log_cursor.h"
#include "common/ob_tablet_info.h"
#include "common/ob_ups_info.h"
#include "common/ob_new_scanner.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_ups_stat.h"
#include "updateserver/ob_store_mgr.h"
#include "updateserver/ob_log_sync_delay_stat.h"
#include "updateserver/ob_ups_clog_status.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
extern const char *print_obj(const ObObj &obj);

template <int64_t SIZE>
struct IntArray
{
  int64_t v[SIZE];
  IntArray()
  {
    memset(v, 0, sizeof(v));
  };
  ~IntArray()
  {
  };
  int serialize(char *buffer, const int64_t buf_size, int64_t &buf_pos) const
  {
    int ret = OB_SUCCESS;
    int64_t pos = buf_pos;
    for (int64_t i = 0; i < SIZE; i++)
    {
      ret = serialization::encode_vi64(buffer, buf_size, pos, v[i]);
      if (OB_SUCCESS != ret)
      {
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      buf_pos = pos;
    }
    return ret;
  };
};

class BaseClient
{
  public:
    BaseClient(): eio_(NULL)
    {
    }
    virtual ~BaseClient()
    {
    }
  public:
    virtual int initialize();
    virtual int destroy();
    virtual int wait();

    ObClientManager * get_rpc()
    {
      return &client_;
    }

  public:
    ObClientManager client_;
    easy_io_t *eio_;
    easy_io_handler_pt client_handler_;
};

inline int BaseClient::initialize()
{
  ob_init_memory_pool();

  int ret = OB_ERROR;
  int rc = EASY_OK;
  eio_ = easy_eio_create(eio_, 1);
  if (NULL == eio_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "easy_io_create error");
  }
  else
  {
    memset(&client_handler_, 0, sizeof(easy_io_handler_pt));
    client_handler_.encode = ObTbnetCallback::encode;
    client_handler_.decode = ObTbnetCallback::decode;
    client_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
    if (OB_SUCCESS != (ret = client_.initialize(eio_, &client_handler_)))
    {
      TBSYS_LOG(ERROR, "failed to init client_mgr, err=%d", ret);
    }
    else
    {
      //start io thread
      if (ret == OB_SUCCESS)
      {
        rc = easy_eio_start(eio_);
        if (EASY_OK == rc)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "start io thread");
        }
        else
        {
          TBSYS_LOG(ERROR, "easy_eio_start failed");
          ret = OB_ERROR;
        }
      }
    }
  }
  return ret;
}

inline int BaseClient::destroy()
{
  easy_eio_stop(eio_);
  easy_eio_wait(eio_);
  easy_eio_destroy(eio_);
  return OB_SUCCESS;
}

inline int BaseClient::wait()
{
  return easy_eio_wait(eio_);
}

class MockClient : public BaseClient
{
  private:
    static const int64_t BUF_SIZE = 2 * 1024 * 1024;

  public:
    MockClient()
    {
    }

    ~MockClient()
    {
    }

    int init(ObServer& server)
    {
      int err = OB_SUCCESS;

      initialize();
      server_ = server;

      return err;
    }

    void set_server(const ObServer &server)
    {
      server_ = server;
    }

  public:
    int send_command(const int pcode, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
      else
      {
        // deserialize the response code
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
          }
          else
          {
            ret = result_code.result_code_;
          }
        }
      }
      return ret;
    }

    template <class Input>
      int send_command(const int pcode, const Input &param, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        static const int32_t MY_VERSION = 1;
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);

        ObClientManager* client_mgr = get_rpc();
        ret = ups_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else
            {
              ret = result_code.result_code_;
            }
          }
        }
        return ret;
      }

    template <class Output>
      int send_request(const int pcode, Output &result, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        static const int32_t MY_VERSION = 1;
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);

        ObClientManager* client_mgr = get_rpc();
        ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        }
        else
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else
            {
              ret = result_code.result_code_;
              if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = ups_deserialize(result, data_buff.get_data(), data_buff.get_position(), pos)))
              {
                TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
              }
            }
          }
        }
        return ret;
      }

    template <class Input, class Output>
      int send_request(const int pcode, const Input &param, Output &result, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        static const int32_t MY_VERSION = 1;
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);

        ObClientManager* client_mgr = get_rpc();
        ret = ups_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else
            {
              ret = result_code.result_code_;
              if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = ups_deserialize(result, data_buff.get_data(), data_buff.get_position(), pos)))
              {
                TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
              }
            }
          }
        }
        return ret;
      }

  public:
    int get_ups_list(ObUpsList &ups_list, const int64_t timeout)
    {
      return send_request(OB_GET_UPS, ups_list, timeout);
    }
    int ob_login(const ObLoginInfo &login_info, ObToken &token, const int64_t timeout)
    {
      return send_request(OB_LOGIN, login_info, token, timeout);
    }
    int minor_load_bypass(int64_t &loaded_num, const int64_t timeout)
    {
      return send_request(OB_UPS_MINOR_LOAD_BYPASS, loaded_num, timeout);
    }
    int major_load_bypass(int64_t &loaded_num, const int64_t timeout)
    {
      return send_request(OB_UPS_MAJOR_LOAD_BYPASS, loaded_num, timeout);
    }
    int delay_drop_memtable(const int64_t timeout)
    {
      return send_command(OB_UPS_DELAY_DROP_MEMTABLE, timeout);
    }
    int immediately_drop_memtable(const int64_t timeout)
    {
      return send_command(OB_UPS_IMMEDIATELY_DROP_MEMTABLE, timeout);
    }
    int enable_memtable_checksum(const int64_t timeout)
    {
      return send_command(OB_UPS_ENABLE_MEMTABLE_CHECKSUM, timeout);
    }
    int disable_memtable_checksum(const int64_t timeout)
    {
      return send_command(OB_UPS_DISABLE_MEMTABLE_CHECKSUM, timeout);
    }
    int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp, const int64_t timeout)
    {
      return send_request(OB_UPS_GET_TABLE_TIME_STAMP, major_version, time_stamp, timeout);
    }

    int switch_commit_log(uint64_t &new_log_file_id, const int64_t timeout)
    {
      return send_request(OB_UPS_SWITCH_COMMIT_LOG, new_log_file_id, timeout);
    }
    int reload_conf(const char* fname, const int64_t timeout)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(fname), static_cast<int32_t>(strlen(fname)));
      return send_command(OB_UPS_RELOAD_CONF, str, timeout);
    }
    int get_clog_status(ObUpsCLogStatus& stat, const int64_t timeout)
    {
      return send_request(OB_GET_CLOG_STATUS, stat, timeout);
    }
    int get_max_log_seq(int64_t& max_log_id, const int64_t timeout)
    {
      return send_request(OB_RS_GET_MAX_LOG_SEQ, max_log_id, timeout);
    }
    int get_max_clog_id(ObLogCursor& log_cursor, const int64_t timeout)
    {
      return send_request(OB_GET_CLOG_CURSOR, log_cursor, timeout);
    }
    int get_clog_master(ObServer& server, const int64_t timeout)
    {
      return send_request(OB_GET_CLOG_MASTER, server, timeout);
    }
    int get_log_sync_delay_stat(ObLogSyncDelayStat& delay_stat, const int64_t timeout)
    {
      return send_request(OB_GET_LOG_SYNC_DELAY_STAT, delay_stat, timeout);
    }
    int get_bloomfilter(const int64_t version, TableBloomFilter &table_bf, const int64_t timeout)
    {
      return send_request(OB_UPS_GET_BLOOM_FILTER, (const uint64_t)version, table_bf, timeout);
    }
    int store_memtable(const int64_t store_all, const int64_t timeout)
    {
      return send_command(OB_UPS_STORE_MEM_TABLE, store_all, timeout);
    }
    int reload_store(const oceanbase::updateserver::StoreMgr::Handle store_handle, const int64_t timeout)
    {
      return send_command(OB_UPS_RELOAD_STORE, store_handle, timeout);
    }

    inline int get_slave_ups_info(char* &buff, const int64_t buff_len, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff(buff, buff_len);
      ObClientManager* client_mgr = get_rpc();
      ret = client_mgr->send_request(server_, OB_UPS_GET_SLAVE_INFO, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
      else
      {
        // deserialize the response code
        data_buff.get_position() = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", data_buff.get_position(), ret);
          }
          else
          {
            ret = result_code.result_code_;
            if (OB_SUCCESS == ret)
            {
              printf("%s\n", data_buff.get_data() + data_buff.get_position());
            }
          }
        }
      }
      return ret;
    }
    int drop(const int64_t timeout)
    {
      return send_command(OB_UPS_DROP_MEM_TABLE, timeout);
    }
    int fetch_schema(const int64_t version, ObSchemaManagerV2 &schema_mgr, const int64_t timeout)
    {
      return send_request(OB_FETCH_SCHEMA, version, schema_mgr, timeout);
    }
    int get_sstable_range_list(IntArray<2> &vt, ObTabletInfoList &ti_list, const int64_t timeout)
    {
      return send_request(OB_RS_FETCH_SPLIT_RANGE, vt, ti_list, timeout);
    }
    int force_fetch_schema(const int64_t timeout)
    {
      return send_command(OB_UPS_FORCE_FETCH_SCHEMA, timeout);
    }
    int get_last_frozen_version(int64_t &version, const int64_t timeout)
    {
      return send_request(OB_UPS_GET_LAST_FROZEN_VERSION, version, timeout);
    }
    int fetch_ups_stat_info(oceanbase::updateserver::UpsStatMgr &stat_mgr, const int64_t timeout)
    {
      return send_request(OB_FETCH_STATS, stat_mgr, timeout);
    }
    int memory_watch(oceanbase::updateserver::UpsMemoryInfo &memory_info, const int64_t timeout)
    {
      return send_request(OB_UPS_MEMORY_WATCH, memory_info, timeout);
    }
    int memory_limit(const oceanbase::updateserver::UpsMemoryInfo &input,
        oceanbase::updateserver::UpsMemoryInfo &output, const int64_t timeout)
    {
      return send_request(OB_UPS_MEMORY_LIMIT_SET, input, output, timeout);
    }
    int priv_queue_conf(const oceanbase::updateserver::UpsPrivQueueConf &input,
        oceanbase::updateserver::UpsPrivQueueConf &output, const int64_t timeout)
    {
      return send_request(OB_UPS_PRIV_QUEUE_CONF_SET, input, output, timeout);
    }
    int reset_vip(const char* vip, const int64_t timeout)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(vip), static_cast<int32_t>(strlen(vip)));
      return send_command(OB_UPS_CHANGE_VIP_REQUEST, str, timeout);
    }
    int dump_memtable(const char *dump_dir, const int64_t timeout)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(dump_dir), static_cast<int32_t>(strlen(dump_dir)));
      return send_command(OB_UPS_DUMP_TEXT_MEMTABLE, str, timeout);
    }
    int umount_store(const char *store_dir, const int64_t timeout)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(store_dir), static_cast<int32_t>(strlen(store_dir)));
      return send_command(OB_UPS_UMOUNT_STORE, str, timeout);
    }
    int dump_schemas(const int64_t timeout)
    {
      return send_command(OB_UPS_DUMP_TEXT_SCHEMAS, timeout);
    }
    int clear_active_memtable(const int64_t timeout)
    {
      return send_command(OB_UPS_CLEAR_ACTIVE_MEMTABLE, timeout);
    }
    int load_new_store(const int64_t timeout)
    {
      return send_command(OB_UPS_LOAD_NEW_STORE, timeout);
    }
    int reload_all_store(const int64_t timeout)
    {
      return send_command(OB_UPS_RELOAD_ALL_STORE, timeout);
    }
    int erase_sstable(const int64_t timeout)
    {
      return send_command(OB_UPS_ERASE_SSTABLE, timeout);
    }
    int drop_memtable(const int64_t timeout)
    {
      return send_command(OB_UPS_DROP_MEM_TABLE, timeout);
    }
    int force_report_frozen_version(const int64_t timeout)
    {
      return send_command(OB_UPS_FORCE_REPORT_FROZEN_VERSION, timeout);
    }
    int freeze(const int64_t timeout, const bool major_freeze)
    {
      PacketCode pcode = OB_UPS_MINOR_FREEZE_MEMTABLE;
      if (major_freeze)
      {
        pcode = OB_FREEZE_MEM_TABLE;
      }
      return send_command(pcode, timeout);
    }
    int ups_apply(const ObMutator &mutator, const int64_t timeout)
    {
      return send_command(OB_WRITE, mutator, timeout);
    }
    int ups_get(const ObGetParam& get_param, ObScanner& scanner, const int64_t timeout)
    {
      return send_request(OB_GET_REQUEST, get_param, scanner, timeout);
    }
    int ups_scan(ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout)
    {
      return send_request(OB_SCAN_REQUEST, scan_param, scanner, timeout);
    }
    int ups_show_sessions(ObNewScanner &scanner, const int64_t timeout)
    {
      return send_request(OB_UPS_SHOW_SESSIONS, scanner, timeout);
    }
    int ups_kill_session(const uint32_t session_descriptor, const int64_t timeout)
    {
      return send_command(OB_UPS_KILL_SESSION, session_descriptor, timeout);
    }

    int get_thread_buffer_(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* buffer = rpc_buffer_.get_buffer();

      buffer->reset();
      data_buff.set_data(buffer->current(), buffer->remain());

      return err;
    }

    int kill_session(const int64_t timeout, const int64_t session_id)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      int pcode = OB_KILL_SESSION_REQUEST;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);
      ObObj obj;
      obj.set_int(session_id);
      if(OB_SUCCESS != (ret = obj.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
      {
        TBSYS_LOG(WARN,"fail to serialize sessionid [ret:%d]", ret);
      }

      ObClientManager* client_mgr = get_rpc();
      if((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff))))
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
      if(OB_SUCCESS == ret)
      {
        // deserialize the response code
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
          }
          else
          {
            TBSYS_LOG(WARN,"kill session:%d", result_code.result_code_);
          }
        }
      }
      return ret;
    }

    int list_sessions(const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      int pcode = OB_LIST_SESSIONS_REQUEST;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
      else
      {
        // deserialize the response code
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
          }
          else
          {
            ret = result_code.result_code_;
            ObObj result;
            ObString msg;
            if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = result.deserialize(data_buff.get_data(), data_buff.get_position(), pos))))
            {
              TBSYS_LOG(WARN,"fail to deserialize result [ret:%d]", ret);
            }
            if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = result.get_varchar(msg))))
            {
              TBSYS_LOG(WARN,"fail to get result msg [ret:%d]", ret);
            }
            if (OB_SUCCESS == ret)
            {
              fprintf(stderr, "%.*s\n", msg.length(), msg.ptr());
            }
          }
        }
      }
      return ret;
    }

    int change_log_level(int32_t log_level, const int64_t timeout)
    {
      return send_command(OB_CHANGE_LOG_LEVEL, log_level, timeout);
    }

    int execute_sql(const ObString &query, const int64_t timeout)
    {
      return send_command(OB_SQL_EXECUTE, query, timeout);
    }
  private:
    ObServer server_;
    ThreadSpecificBuffer rpc_buffer_;
};

#endif //__MOCK_CLIENT_H__
