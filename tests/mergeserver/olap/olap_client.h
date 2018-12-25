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

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_packet_factory.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/thread_buffer.h"
#include "common/ob_bloomfilter.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_ups_stat.h"
#include "updateserver/ob_store_mgr.h"

using namespace oceanbase::common;
extern const char *print_obj(const ObObj &obj);

class BaseClient
{
  public:
    BaseClient()
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
    tbnet::DefaultPacketStreamer streamer_;
    tbnet::Transport transport_;
    ObPacketFactory factory_;
    ObClientManager client_;
};

inline int BaseClient::initialize()
{
  ob_init_memory_pool();
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  return transport_.start();
}

inline int BaseClient::destroy()
{
  transport_.stop();
  return transport_.wait();
}

inline int BaseClient::wait()
{
  return transport_.wait();
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

    int init()
    {
      int err = OB_SUCCESS; 
      initialize(); 
      return err;
    }

  public:
    int send_command(const int pcode, const int64_t timeout, ObServer & server)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = client_mgr->send_request(server, pcode, MY_VERSION, timeout, data_buff);
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
    int send_command(const int pcode, const Input &param, const int64_t timeout, ObServer & server)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = ups_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = client_mgr->send_request(server, pcode, MY_VERSION, timeout, data_buff);
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
    int send_request(const int pcode, Output &result, const int64_t timeout, ObServer &server)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = client_mgr->send_request(server, pcode, MY_VERSION, timeout, data_buff);
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
    int send_request(const int pcode, const Input &param, Output &result, const int64_t timeout, ObServer & server)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      get_thread_buffer_(data_buff);

      ObClientManager* client_mgr = get_rpc();
      ret = ups_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = client_mgr->send_request(server, pcode, MY_VERSION, timeout, data_buff);
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
    int enable_memtable_checksum(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_ENABLE_MEMTABLE_CHECKSUM, timeout, server);
    }
    int disable_memtable_checksum(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_DISABLE_MEMTABLE_CHECKSUM, timeout, server);
    }
    int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_GET_TABLE_TIME_STAMP, major_version, time_stamp, timeout, server);
    }
    int switch_commit_log(uint64_t &new_log_file_id, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_SWITCH_COMMIT_LOG, new_log_file_id, timeout, server);
    }
    int reload_conf(const char* fname, const int64_t timeout, ObServer & server)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(fname), static_cast<int32_t>(strlen(fname)));
      return send_command(OB_UPS_RELOAD_CONF, str, timeout, server);
    }
    int get_bloomfilter(const int64_t version, TableBloomFilter &table_bf, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_GET_BLOOM_FILTER, (const uint64_t)version, table_bf, timeout, server);
    }
    int store_memtable(const int64_t store_all, const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_STORE_MEM_TABLE, store_all, timeout, server);
    }
    int reload_store(const oceanbase::updateserver::StoreMgr::Handle store_handle, const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_RELOAD_STORE, store_handle, timeout, server);
    }
    int drop(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_DROP_MEM_TABLE, timeout, server);
    }
    int fetch_schema(const int64_t version, ObSchemaManagerV2 &schema_mgr, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_FETCH_SCHEMA, version, schema_mgr, timeout, server);
    }
    int force_fetch_schema(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_FORCE_FETCH_SCHEMA, timeout, server);
    }
    int get_last_frozen_version(int64_t &version, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_GET_LAST_FROZEN_VERSION, version, timeout, server);
    }
    int fetch_ups_stat_info(oceanbase::updateserver::UpsStatMgr &stat_mgr, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_FETCH_STATS, stat_mgr, timeout, server);
    }
    int memory_watch(oceanbase::updateserver::UpsMemoryInfo &memory_info, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_MEMORY_WATCH, memory_info, timeout, server);
    }
    int memory_limit(const oceanbase::updateserver::UpsMemoryInfo &input,
                    oceanbase::updateserver::UpsMemoryInfo &output, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_MEMORY_LIMIT_SET, input, output, timeout, server);
    }
    int priv_queue_conf(const oceanbase::updateserver::UpsPrivQueueConf &input,
                        oceanbase::updateserver::UpsPrivQueueConf &output, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_UPS_PRIV_QUEUE_CONF_SET, input, output, timeout, server);
    }
    int reset_vip(const char* vip, const int64_t timeout, ObServer & server)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(vip), static_cast<int32_t>(strlen(vip)));
      return send_command(OB_UPS_CHANGE_VIP_REQUEST, str, timeout, server);
    }
    int dump_memtable(const char *dump_dir, const int64_t timeout, ObServer & server)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(dump_dir), static_cast<int32_t>(strlen(dump_dir)));
      return send_command(OB_UPS_DUMP_TEXT_MEMTABLE, str, timeout, server);
    }
    int umount_store(const char *store_dir, const int64_t timeout, ObServer & server)
    {
      ObString str;
      str.assign_ptr(const_cast<char*>(store_dir), static_cast<int32_t>(strlen(store_dir)));
      return send_command(OB_UPS_UMOUNT_STORE, str, timeout, server);
    }
    int dump_schemas(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_DUMP_TEXT_SCHEMAS, timeout, server);
    }
    int clear_active_memtable(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_CLEAR_ACTIVE_MEMTABLE, timeout, server);
    }
    int load_new_store(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_LOAD_NEW_STORE, timeout, server);
    }
    int reload_all_store(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_RELOAD_ALL_STORE, timeout, server);
    }
    int erase_sstable(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_ERASE_SSTABLE, timeout, server);
    }
    int drop_memtable(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_DROP_MEM_TABLE, timeout, server);
    }
    int force_report_frozen_version(const int64_t timeout, ObServer & server)
    {
      return send_command(OB_UPS_FORCE_REPORT_FROZEN_VERSION, timeout, server);
    }
    int freeze(const int64_t timeout, const bool major_freeze, ObServer & server)
    {
      PacketCode pcode = OB_UPS_MINOR_FREEZE_MEMTABLE;
      if (major_freeze)
      {
        pcode = OB_FREEZE_MEM_TABLE;
      }
      return send_command(pcode, timeout, server);
    }
    int ups_apply(const ObMutator &mutator, const int64_t timeout, ObServer & server)
    {
      return send_command(OB_WRITE, mutator, timeout, server);
    }
    int ups_get(const ObGetParam& get_param, ObScanner& scanner, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_GET_REQUEST, get_param, scanner, timeout, server);
    }
    int ups_scan(ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout, ObServer & server)
    {
      return send_request(OB_SCAN_REQUEST, scan_param, scanner, timeout, server);
    }

    int get_thread_buffer_(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* buffer = rpc_buffer_.get_buffer();

      buffer->reset();
      data_buff.set_data(buffer->current(), buffer->remain());

      return err;
    }

  private:
    ThreadSpecificBuffer rpc_buffer_;
};


#endif //__MOCK_CLIENT_H__

