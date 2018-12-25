/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_proxy_service.h"
#include "ob_proxy_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "common/ob_version.h"
#include "common/ob_profile_log.h"
#include "ob_proxy_server_main.h"
#include "common/ob_common_stat.h"
#include "ob_proxy_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace proxyserver
  {
    ObProxyService::ObProxyService()
    : proxy_server_(NULL), inited_(false)
    {
    }

    ObProxyService::~ObProxyService()
    {
      destroy();
    }

    /**
     * use ObChunkService after initialized.
     */
    int ObProxyService::initialize(ObProxyServer* proxy_server)
    {
      int ret = OB_SUCCESS;
      std::vector<std::string> proxy_names;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == proxy_server)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (ret = yunti_proxy_.initialize()))
      {
        TBSYS_LOG(ERROR, "yunti has some problem, check it, ret:%d", ret);
        ret = OB_ERROR;
      }
      else if(0 >= proxy_server->get_config().get_all_proxy_name(proxy_names))
      {
        TBSYS_LOG(ERROR, "config has some problem, check it, ret:%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        proxy_map_.create(DEFAULT_PROXY_NUM);
        for(uint32_t i = 0; i < proxy_names.size(); i++) 
        {
          if(0 == strcmp(ObProxyServerConfig::YUNTI_PROXY, proxy_names[i].c_str()))
          {
            proxy_map_.set(proxy_names[i].c_str(), &yunti_proxy_);
          }
        }
        proxy_server_ = proxy_server;
      }

      inited_ = true;
      return ret;
    }

    int ObProxyService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        proxy_server_ = NULL;
      }
      else
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }

    int ObProxyService::start()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }


    /*
     * after initialize() & start(), then can handle the request.
     */
    int ObProxyService::do_request(
        const int64_t receive_time,
        const int32_t packet_code,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      UNUSED(receive_time);
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        rc = OB_NOT_INIT;
      }

      if (OB_SUCCESS != rc)
      {
        common::ObResultCode result;
        result.result_code_ = rc;
        // send response.
        int serialize_ret = result.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
        }
        else
        {
          proxy_server_->send_response(
              packet_code + 1, version,
              out_buffer, req, channel_id);
        }
      }
      else
      {
        switch(packet_code)
        {
          case OB_CS_FETCH_DATA:
            rc = proxy_fetch_data(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_FETCH_RANGE_TABLE :
            rc = proxy_fetch_range(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          default:
            rc = OB_ERROR;
            TBSYS_LOG(WARN, "not support packet code[%d]", packet_code);
            break;
        }
      }
      return rc;
    }

    int ObProxyService::get_yunti_reader(
        YuntiProxy* proxy,
        ObDataSourceDesc &desc,
        const char* sstable_dir,
        int64_t &local_tablet_version,
        int16_t &local_sstable_version,
        int64_t sstable_type,
        ProxyReader* &proxy_reader)
    {
      int ret = OB_SUCCESS;
      YuntiFileReader* yunti_reader = GET_TSI_MULT(YuntiFileReader, TSI_YUNTI_PROXY_READER_1);
      if (NULL == yunti_reader)
      {
        TBSYS_LOG(ERROR, "failed to get thread local migrate_reader, "
            "migrate_reader=%p", proxy_reader);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        yunti_reader->reset();
        //open sstable scanner
        if(OB_SUCCESS != (ret = yunti_reader->prepare(proxy, desc.range_, desc.tablet_version_, static_cast<int16_t>(desc.sstable_version_), sstable_dir)))
        {
          TBSYS_LOG(WARN, "prepare yunti reader failed, ret:%d", ret);
        }
        else if(OB_SUCCESS != (ret = yunti_reader->get_sstable_version(sstable_dir, local_sstable_version)))
        {
          TBSYS_LOG(WARN, "get sstable version, ret:%d", ret);
        }
        else if(OB_SUCCESS != (ret = yunti_reader->get_tablet_version(local_tablet_version)))
        {
          TBSYS_LOG(WARN, "get tablet version, ret:%d", ret);
        }
        else if(OB_SUCCESS != (ret = yunti_reader->get_sstable_type(sstable_type)))
        {
          TBSYS_LOG(WARN, "get sstable type, ret:%d", ret);
        }
        else
        {
          proxy_reader = yunti_reader;
        }
      }
      return ret;
    }


    int ObProxyService::proxy_fetch_data(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t PROXY_FETCH_DATA_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      int serialize_ret = OB_SUCCESS;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        proxy_server_->get_default_task_queue_thread();
      UNUSED(timeout_time);

      if (version != PROXY_FETCH_DATA_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObDataSourceDesc desc;
      ProxyReader* proxy_reader = NULL;
      int16_t local_sstable_version = 0;
      int64_t local_tablet_version = 0;
      int64_t local_sstable_type = 0;


      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = desc.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse data source param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin fetch_data, %s", to_cstring(desc));

        //get proxy
        char* split_pos = NULL;
        char* data_source_name = strtok_r(desc.uri_.ptr(), ":", &split_pos);
        Proxy* proxy = NULL;
        int hash_ret = OB_SUCCESS;
        if(data_source_name == NULL)
        {
          TBSYS_LOG(ERROR, "data source name miss");
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else if(hash::HASH_EXIST != (hash_ret = proxy_map_.get(data_source_name, proxy)) || NULL == proxy)
        {
          TBSYS_LOG(ERROR, "data source:%s proxy not exist", data_source_name);
          rc.result_code_ = OB_DATA_SOURCE_NOT_EXIST;
        }
        else if(strcmp(data_source_name, ObProxyServerConfig::YUNTI_PROXY) == 0)
        {
          char* sstable_dir = strtok_r(NULL, ":", &split_pos);
          if(NULL == sstable_dir)
          {
            TBSYS_LOG(ERROR, "sstable dir miss");
            rc.result_code_ = OB_INVALID_ARGUMENT;
          }
          else
          {
            rc.result_code_ = get_yunti_reader(dynamic_cast<YuntiProxy*>(proxy), desc, sstable_dir + 1, local_tablet_version, local_sstable_version, local_sstable_type, proxy_reader);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "not support this data source:%s", data_source_name);
          rc.result_code_ = OB_DATA_SOURCE_NOT_EXIST;
        }
      }

      out_buffer.get_position() = rc.get_serialize_size();
      int64_t sequence_num = 0;
      int64_t row_checksum = 0;

      if(OB_SUCCESS != (serialize_ret = serialization::encode_i16(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), local_sstable_version)))
      {
        TBSYS_LOG(ERROR, "serialize sstable version object failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), sequence_num)))
      {
        TBSYS_LOG(ERROR, "serialize sequence object failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), row_checksum)))
      {
        TBSYS_LOG(ERROR, "serialize row_checksum object failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), local_sstable_type)))
      {
        TBSYS_LOG(ERROR, "serialize sstable type failed, ret:%d", serialize_ret);
      }

      if(OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        rc.result_code_ = proxy_reader->read_next(out_buffer);
        if (proxy_reader->has_new_data())
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      int64_t tmp_pos = 0;
      if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), tmp_pos)))
      {
        TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ &&
            proxy_reader->has_new_data() &&
            !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to prepare_for_next_request:err[%d]", rc.result_code_);
          }
        }

        // send response.
        if(OB_SUCCESS == serialize_ret)
        {
          proxy_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_CS_FETCH_DATA_RESPONSE, PROXY_FETCH_DATA_VERSION,
              out_buffer, req, response_cid, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && proxy_reader->has_new_data())
        {
          rc.result_code_ = queue_thread.wait_for_next_request(
              session_id, next_request, THE_PROXY_SERVER.get_config().get_network_timeout());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //end this session
            rc.result_code_ = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
                rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            out_buffer.get_position() = rc.get_serialize_size();
            rc.result_code_ = proxy_reader->read_next(out_buffer);
            if(!proxy_reader->has_new_data())
            {
              is_last_packet = true;
            }

            int64_t tmp_pos = 0;
            if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), tmp_pos)))
            {
              TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
              break;
            }
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      int ret = 0;
      if(NULL != proxy_reader && OB_SUCCESS != (ret = proxy_reader->close()))
      {
        TBSYS_LOG(WARN, "proxy reader close fail, ret:%d", ret);
      }


      TBSYS_LOG(INFO, "fetch data finish, rc:%d", rc.result_code_);

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      return rc.result_code_;
    }


    int ObProxyService::get_yunti_range_reader(YuntiProxy* proxy, const char* table_name, const char* sstable_dir, ProxyReader* &proxy_reader)
    {
      int ret = OB_SUCCESS;
      YuntiRangeReader* range_reader = GET_TSI_MULT(YuntiRangeReader, TSI_YUNTI_PROXY_READER_2);
      if (NULL == range_reader)
      {
        TBSYS_LOG(ERROR, "failed to get thread local migrate_reader, "
            "migrate_reader=%p", proxy_reader);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        range_reader->reset();
        if(OB_SUCCESS != (ret = range_reader->prepare(proxy,  table_name, sstable_dir)))
        {
          TBSYS_LOG(WARN, "prepare yunti range reader failed, ret:%d", ret);
        }
        else
        {
          proxy_reader = range_reader;
        }
      }

      return ret;
    }

    int ObProxyService::proxy_fetch_range(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t PROXY_FETCH_RANGE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      int serialize_ret = OB_SUCCESS;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        proxy_server_->get_default_task_queue_thread();
      UNUSED(timeout_time);

      if (version != PROXY_FETCH_RANGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ProxyReader* proxy_reader = NULL;
      char table_name[OB_MAX_TABLE_NAME_LENGTH];
      memset(table_name, 0, sizeof(table_name));
      char uri[OB_MAX_FILE_NAME_LENGTH];
      memset(uri, 0, sizeof(uri));
      int64_t uri_len = 0;
      int64_t table_name_len = 0;

      if(NULL == serialization::decode_vstr(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position(), table_name, OB_MAX_TABLE_NAME_LENGTH - 1, &table_name_len))
      {
        TBSYS_LOG(ERROR, "deserialize the table_name error, ret:%d", rc.result_code_);
        rc.result_code_ = OB_DESERIALIZE_ERROR;
      }
      else if(NULL == serialization::decode_vstr(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position(), uri, OB_MAX_FILE_NAME_LENGTH - 1, &uri_len))
      {
        TBSYS_LOG(ERROR, "deserialize the uri error, ret:%d", rc.result_code_);
        rc.result_code_ = OB_DESERIALIZE_ERROR;
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin fetch_range, uri:%s table_name:%s", uri, table_name);

        //get proxy
        char* split_pos = NULL;
        char* data_source_name = strtok_r(uri, ":", &split_pos); 
        Proxy* proxy = NULL;
        int hash_ret = OB_SUCCESS;
        if(data_source_name == NULL)
        {
          TBSYS_LOG(ERROR, "data source name miss");
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else if(hash::HASH_EXIST != (hash_ret = proxy_map_.get(data_source_name, proxy)) || NULL == proxy)
        {
          TBSYS_LOG(ERROR, "data source:%s proxy not exist", data_source_name);
          rc.result_code_ = OB_DATA_SOURCE_NOT_EXIST;
        }
        else if(strcmp(data_source_name, ObProxyServerConfig::YUNTI_PROXY) == 0)
        {
          char* sstable_dir = strtok_r(NULL, ":", &split_pos);
          if(NULL == sstable_dir)
          {
            TBSYS_LOG(ERROR, "sstable dir miss");
            rc.result_code_ = OB_INVALID_ARGUMENT;
          }
          else
          {
            TBSYS_LOG(INFO, "sstable dir:%s", sstable_dir);
            rc.result_code_ = get_yunti_range_reader(dynamic_cast<YuntiProxy*>(proxy), table_name, sstable_dir + 1, proxy_reader);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "not support this data source:%s", data_source_name);
          rc.result_code_ = OB_DATA_SOURCE_NOT_EXIST;
        }
      }

      out_buffer.get_position() = rc.get_serialize_size();

      if(OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = proxy_reader->read_next(out_buffer);
      }

      int64_t tmp_pos = 0;
      if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), tmp_pos)))
      {
        TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
      }
      if(OB_SUCCESS != rc.result_code_)
      {
        out_buffer.get_position() = tmp_pos;
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        if (proxy_reader->has_new_data())
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ &&
            proxy_reader->has_new_data() &&
            !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to prepare_for_next_request:err[%d]", rc.result_code_);
          }
        }


        // send response.
        if(OB_SUCCESS == serialize_ret)
        {
          proxy_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_FETCH_RANGE_TABLE_RESPONSE, PROXY_FETCH_RANGE_VERSION,
              out_buffer, req, response_cid, session_id);
          packet_cnt++;
        }


        if (OB_SUCCESS == rc.result_code_ && proxy_reader->has_new_data())
        {
          rc.result_code_ = queue_thread.wait_for_next_request(
              session_id, next_request, THE_PROXY_SERVER.get_config().get_network_timeout());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //end this session
            rc.result_code_ = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
                rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            out_buffer.get_position() = rc.get_serialize_size();
            rc.result_code_ = proxy_reader->read_next(out_buffer);

            int64_t tmp_pos = 0;
            if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), tmp_pos)))
            {
              TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
              break;
            }
            if(OB_SUCCESS != rc.result_code_)
            {
              out_buffer.get_position() = tmp_pos;
            }

            if(!proxy_reader->has_new_data())
            {
              is_last_packet = true;
            }
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      int ret = 0;
      if(NULL != proxy_reader && OB_SUCCESS != (ret = proxy_reader->close()))
      {
        TBSYS_LOG(WARN, "proxy reader close fail, ret:%d", ret);
      }


      TBSYS_LOG(INFO, "fetch data finish, rc:%d", rc.result_code_);

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      return rc.result_code_;
    }

  } // end namespace chunkserver
} // end namespace oceanbase
