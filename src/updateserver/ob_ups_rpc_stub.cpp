/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_stub.cpp,v 0.1 2010/09/27 19:50:56 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ups_rpc_stub.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"
#include "ob_fetched_log.h"
#include "common/ob_trigger_msg.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;

    ObUpsRpcStub :: ObUpsRpcStub()
    {
      client_mgr_ = NULL;
    }

    ObUpsRpcStub :: ~ObUpsRpcStub()
    {
    }


    //add zhaoqiong [fixed for Backup]:20150811:b
    int ObUpsRpcStub::init(const ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer)
    {
      OB_ASSERT(NULL != client_mgr);
      OB_ASSERT(NULL != tsbuffer);
      ObCommonRpcStub::init(client_mgr);
      thread_buffer_ = tsbuffer;
      return OB_SUCCESS;
    }
    //add:e

    int ObUpsRpcStub :: fetch_schema(const common::ObServer& root_server, const int64_t timestamp,
        CommonSchemaManagerWrapper& schema_mgr, const bool only_core_tables, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      int32_t version = 2;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      // step 1. serialize timestamp to data_buff
      if (OB_SUCCESS == err)
      {
        err = common::serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), timestamp);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], err[%d].",
              timestamp, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = serialization::encode_bool(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), only_core_tables);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "serialize fail. err=%d", err);
        }
      }

      // step 2. send request to fetch new schema
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_FETCH_SCHEMA, version, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to root server for fetch schema failed"
              ":timestamp[%ld], err[%d].", timestamp, err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      // step 4. deserialize the table schema
      if (OB_SUCCESS == err)
      {
        err = schema_mgr.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize schema from buff failed"
              "timestamp[%ld], pos[%ld], err[%d].", timestamp, pos, err);
        }
      }

      return err;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    int ObUpsRpcStub :: fetch_schema_next(const int64_t timeout_us, const common::ObServer& root_server, const int64_t timestamp,
        const int64_t table_start_pos,const int64_t column_start_pos, CommonSchemaManager& schema_mgr)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      int32_t version = 2;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      // step 1. serialize timestamp to data_buff
      if (OB_SUCCESS == err)
      {
        err = common::serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), timestamp);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], err[%d].",timestamp, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = serialization::encode_vi64(data_buff.get_data(),
                          data_buff.get_capacity(), data_buff.get_position(), table_start_pos)))
        {
           TBSYS_LOG(WARN, "serialize fail. err=%d", err);
        }
        else if (OB_SUCCESS != (err = serialization::encode_vi64(data_buff.get_data(),
                                data_buff.get_capacity(), data_buff.get_position(), column_start_pos)))
        {
           TBSYS_LOG(WARN, "serialize fail. err=%d", err);
        }
      }

      // step 2. send request to fetch new schema
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_FETCH_SCHEMA_NEXT, version, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to root server for fetch schema failed"
              ":timestamp[%ld], err[%d].", timestamp, err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      // step 4. deserialize the table schema
      if (OB_SUCCESS == err)
      {
        err = schema_mgr.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize schema from buff failed"
              "timestamp[%ld], pos[%ld], err[%d].", timestamp, pos, err);
        }
      }
      return err;
    }
    //add:e

    int ObUpsRpcStub :: slave_register_followed(const ObServer& master, const ObSlaveInfo &slave_info,
        ObSlaveSyncType slave_type, ObUpsFetchParam& fetch_param, uint64_t &max_log_seq, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      if (OB_SUCCESS == err)
      {
        err = slave_info.serialize(data_buff.get_data(), data_buff.get_capacity(),data_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObSlaveInfo serialize error, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = slave_type.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "slave_type serialize error, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master, OB_SLAVE_REG, DEFAULT_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send slave register to master. err=%d", err);
        }
      }
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      if (OB_SUCCESS == err)
      {
        err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to deserialize fetch_param. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, reinterpret_cast<int64_t*>(&max_log_seq));
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to decode max_log_seq, err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "slave_register_followed get master max log_id = %ld, master_ups=%s", max_log_seq, master.to_cstring());
        }
      }
      return err;
    }

    int ObUpsRpcStub :: slave_register_followed(const ObServer& master, const ObSlaveInfo &slave_info,
        ObUpsFetchParam& fetch_param, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize slave addr
      if (OB_SUCCESS == err)
      {
        err = slave_info.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObSlaveInfo serialize error, err=%d", err);
        }
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master,
            OB_SLAVE_REG, DEFAULT_VERSION, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send request to register failed err[%d].", err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      // step 3. deserialize fetch param
      if (OB_SUCCESS == err)
      {
        err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
        }
      }

      return err;
    }

    //add:e zhaoqiong [fixed for Backup]:20150811:b
    int ObUpsRpcStub :: backup_register(const ObServer& master, const ObSlaveInfo &slave_info,
        ObUpsFetchParam& fetch_param, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize slave addr
      if (OB_SUCCESS == err)
      {
        err = slave_info.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObSlaveInfo serialize error, err=%d", err);
        }
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master,
            OB_BACKUP_REG, DEFAULT_VERSION, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send request to register failed err[%d].", err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      // step 4. deserialize fetch param
      if (OB_SUCCESS == err)
      {
        err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
        }
      }

      return err;
    }
    //add:e

    int ObUpsRpcStub :: slave_register_standalone(const ObServer& master,
        const uint64_t log_id, const uint64_t log_seq,
        uint64_t &log_id_res, uint64_t &log_seq_res, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize slave addr
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position(), (int64_t)log_id)))
        {
          TBSYS_LOG(WARN, "log_id serialize error, err=%d", err);
        }
        else if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position(), (int64_t)log_seq)))
        {
          TBSYS_LOG(WARN, "log_seq serialize error, err=%d", err);
        }
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master,
            OB_SLAVE_REG, DEFAULT_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "send request to register failed err[%d]. lsync_addr=%s", err, master.to_cstring());
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
          if (OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_id_res)))
            {
              TBSYS_LOG(ERROR, "deserialize log_id_res error, err=%d", err);
            }
            else if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_seq_res)))
            {
              TBSYS_LOG(ERROR, "deserialize log_seq_res error, err=%d", err);
            }
          }
        }
      }

      return err;
    }

    int ObUpsRpcStub :: send_freeze_memtable_resp(const ObServer& root_server,
        const ObServer& ups_master, const int64_t schema_timestamp, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      ObServer update_server;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // serialize ups_master
      if (OB_SUCCESS == err)
      {
        err = ups_master.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
      }

      // serialize timestamp
      if (OB_SUCCESS == err)
      {
        err = common::serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), schema_timestamp);
      }

      // step 1. send freeze memtable resp
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_WAITING_JOB_DONE, DEFAULT_VERSION, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send freeze memtable failed, err[%d].", err);
        }
      }

      // step 2. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      return err;
    }

    int ObUpsRpcStub :: report_freeze(const common::ObServer &root_server,
        const common::ObServer &ups_master, const int64_t frozen_version, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      ObServer update_server;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // serialize ups_master
      if (OB_SUCCESS == err)
      {
        err = ups_master.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
      }

      // serialize timestamp
      if (OB_SUCCESS == err)
      {
        err = common::serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), frozen_version);
      }

      // step 1. send freeze memtable resp
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_UPDATE_SERVER_REPORT_FREEZE, DEFAULT_VERSION, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send freeze memtable failed, err[%d].", err);
        }
      }

      // step 2. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      return err;
    }

    template <typename Input, typename Output>
    int send_request(const ObClientManager* client_mgr, const ObServer& server,
                     const int32_t version, const int pcode, const Input &param, Output &result,
                     ObDataBuffer& buf, const int64_t timeout)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;

      if (NULL == client_mgr)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = ups_serialize(param, buf.get_data(), buf.get_capacity(), buf.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize()=>%d", err);
      }
      else if (OB_SUCCESS != (err = client_mgr->send_request(server, pcode, version, timeout, buf)))
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", err);
      }
      else if (OB_SUCCESS != (err = result_code.deserialize(buf.get_data(), buf.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, err);
      }
      else if (OB_SUCCESS != (err = result_code.result_code_))
      {
        TBSYS_LOG(ERROR, "result_code.result_code = %d", err);
      }
      else if (OB_SUCCESS != (err = ups_deserialize(result, buf.get_data(), buf.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, err);
      }
      return err;
    }

    template<typename Input, typename Output>
    int ObUpsRpcStub::send_request(const common::ObServer& server, const int pcode,
                     const Input& input, Output& output, const int64_t timeout)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (OB_SUCCESS != (err = get_thread_buffer_(data_buff)))
      {
        TBSYS_LOG(ERROR, "get_thread_buffer_()=>%d", err);
      }
      else if (OB_SUCCESS != (err = updateserver::send_request(client_mgr_, server, DEFAULT_VERSION, pcode,
                                                 input, output, data_buff, timeout)))
      {
        TBSYS_LOG(ERROR, "send_request(server=%s, pcode=%d)=>%d", to_cstring(server), pcode, err);
      }
      return err;
    }

    template <class Input>
      int ObUpsRpcStub :: send_command(const common::ObServer &server, const int pcode, const Input &param, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        static const int32_t MY_VERSION = 1;
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);

        if (NULL == client_mgr_)
        {
          TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          ret = ups_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        }

        if (OB_SUCCESS == ret)
        {
          ret = client_mgr_->send_request(server, pcode, MY_VERSION, timeout, data_buff);
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

    int ObUpsRpcStub :: send_mutator_apply(const common::ObServer& ups_master,
        const ObMutator &mutator, const int64_t timeout_us)
    {
      return send_command(ups_master, OB_INTERNAL_WRITE, mutator, timeout_us);
    }

    int ObUpsRpcStub :: fetch_lsync(const common::ObServer &lsync, const uint64_t log_id, const uint64_t log_seq,
        char* &log_data, int64_t &log_len, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;

      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize info
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position(), (int64_t)log_id)))
        {
          TBSYS_LOG(WARN, "Serialize log_id error, err=%d", err);
        }
        else if (OB_SUCCESS != (err = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position(), (int64_t)log_seq)))
        {
          TBSYS_LOG(WARN, "Serialize log_seq error, err=%d", err);
        }
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        int64_t send_bgn_time = tbsys::CTimeUtil::getMonotonicTime();

        err = client_mgr_->send_request(lsync,
            OB_LSYNC_FETCH_LOG, DEFAULT_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err && OB_RESPONSE_TIME_OUT != err)
        {
          TBSYS_LOG(ERROR, "send request to lsync failed err[%d].", err);
        }
        else if (OB_RESPONSE_TIME_OUT == err)
        {
          int64_t send_end_time = tbsys::CTimeUtil::getMonotonicTime();
          int64_t left_time = timeout_us - (send_end_time - send_bgn_time);
          if (left_time > 0)
          {
            //usleep(static_cast<useconds_t>(left_time));
          }
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_) && OB_NEED_RETRY != err && OB_NOT_REGISTERED != err)
        {
          TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
        }
        else if (OB_SUCCESS != err)
        {
          TBSYS_LOG(DEBUG, "fetch_lsync()=>%d", err);
        }
        else
        {
          if (OB_SUCCESS != (err = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, (int64_t*)&log_len)))
          {
            TBSYS_LOG(WARN, "Deserialize log_len error, err=%d", err);
          }
          else
          {
            log_data = data_buff.get_data() + pos;
            TBSYS_LOG(DEBUG, "log_data=%p log_len=%ld", log_data, log_len);
          }
        }
      }

      return err;
    }

    int ObUpsRpcStub:: fetch_log(const ObServer &master, const ObFetchLogReq& req, ObFetchedLog& fetched_log, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != (err = get_thread_buffer_(data_buff)))
      {
        TBSYS_LOG(ERROR, "get_thread_buffer_()=>%d", err);
      }
      else if (OB_SUCCESS !=- (err = req.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "req.serialize()=>%d", err);
      }

      if (OB_SUCCESS == err)
      {
        int64_t send_bgn_time = tbsys::CTimeUtil::getMonotonicTime();

        err = client_mgr_->send_request(master,
            OB_FETCH_LOG, DEFAULT_VERSION, timeout_us, data_buff);
        //TBSYS_LOG(DEBUG, "send_request(client_mgr->send_request()=>%d", err);
        if (OB_SUCCESS != err && OB_RESPONSE_TIME_OUT != err)
        {
          TBSYS_LOG(ERROR, "send request to master failed err[%d].", err);
        }
        else if (OB_RESPONSE_TIME_OUT == err)
        {
          int64_t send_end_time = tbsys::CTimeUtil::getMonotonicTime();
          int64_t left_time = timeout_us - (send_end_time - send_bgn_time);
          if (left_time > 0)
          {
            //usleep(static_cast<useconds_t>(left_time));
          }
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
      }
      else if (OB_SUCCESS != (err = result_code.result_code_) && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
      }
      else if (OB_NEED_RETRY == err)
      {
        TBSYS_LOG(DEBUG, "no data, retry");
      }
      else if (OB_SUCCESS != (err = fetched_log.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "fetched_log.deserialize()=>%d", err);
      }
      return err;
    }

    int ObUpsRpcStub ::fill_log_cursor(const ObServer &master, ObLogCursor& log_cursor, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      int64_t pos = 0;
      ObResultCode result_code;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != (err = get_thread_buffer_(data_buff)))
      {
        TBSYS_LOG(ERROR, "get_thread_buffer_()=>%d", err);
      }
      else if (OB_SUCCESS !=- (err = log_cursor.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "log_cursor.serialize()=>%d", err);
      }
      else if (OB_SUCCESS != (err = client_mgr_->send_request(master, OB_FILL_LOG_CURSOR, DEFAULT_VERSION,
                                                              timeout_us, data_buff)))
      {
        if (OB_RESPONSE_TIME_OUT != err)
        {
          TBSYS_LOG(WARN, "send request to master failed err[%d].", err);
        }
      }
      else if (OB_SUCCESS != (err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
      }
      else if (OB_SUCCESS != (err = result_code.result_code_) && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
      }
      else if (OB_NEED_RETRY == err)
      {
        TBSYS_LOG(DEBUG, "no data, retry");
      }
      else if (OB_SUCCESS != (err = log_cursor.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "fetched_log.deserialize()=>%d", err);
      }
      return err;
    }

    int ObUpsRpcStub::get_rs_last_frozen_version(const ObServer &root_server,
                                                 int64_t& frozen_version, const int64_t timeout_us)
    {
      return send_request(root_server, OB_RS_GET_LAST_FROZEN_VERSION, __dummy__, frozen_version, timeout_us);
    }

    int ObUpsRpcStub::get_thread_buffer_(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      //mod zhaoqiong [fixed for Backup]:20150811:b
//      ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
//      //get buffer for rpc send and receive
//      ObUpdateServerMain* obj = ObUpdateServerMain::get_instance();
//      if (NULL == obj)
//      {
//        TBSYS_LOG(ERROR, "get ObUpdateServerMain instance failed.");
//      }
//      else
//      {
//        const ObUpdateServer& server = obj->get_update_server();
//        rpc_buffer = server.get_rpc_buffer();
      if (NULL == thread_buffer_)
      {
        TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
        err = OB_ERROR;
      }
      else
      {
        common::ThreadSpecificBuffer::Buffer* rpc_buffer = thread_buffer_->get_buffer();
        //mod:e
        if (NULL == rpc_buffer)
        {
          TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
          err = OB_ERROR;
        }
        else
        {
          rpc_buffer->reset();
          data_buff.set_data(rpc_buffer->current(), rpc_buffer->remain());
        }
      }

      return err;
    }
    int ObUpsRpcStub :: ups_report_slave_failure(const common::ObServer &slave_server, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      common::ObMsgUpsSlaveFailure failure_info;
      failure_info.slave_addr_.set_ipv4_addr(slave_server.get_ipv4(), slave_server.get_port());
      common::ObServer self;
      common::ObServer rootserver;
      if (OB_SUCCESS == err)
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL != ups_main)
        {
          self = ups_main->get_update_server().get_self();
          rootserver = ups_main->get_update_server().get_root_server();
          failure_info.my_addr_.set_ipv4_addr(self.get_ipv4(), self.get_port());
        }
        else
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "fail to get ups_main");
        }
      }

      //step 1: serialize failure info
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = failure_info.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
        {
          TBSYS_LOG(WARN, "fail to serialize failure_info, err = %d", err);
        }
      }
      //step 2: send request to root_server
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = client_mgr_->send_request(rootserver, OB_RS_UPS_SLAVE_FAILURE, failure_info.MY_VERSION, timeout_us, data_buff)))
        {
          TBSYS_LOG(WARN, "fail to send request. err = %d", err);
        }
      }
      //step3: deserialize the response
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      return err;
    }

    int ObUpsRpcStub :: get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "INVALID argument. client_mgr_=%p", client_mgr_);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      common::ObServer master_inst_rs;
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = get_master_obi_rs(root_server, master_inst_rs, timeout_us)))
        {
          TBSYS_LOG(WARN, "fail to get master obi rootserver addr. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = get_master_ups_info(master_inst_rs, ups_master, timeout_us)))
        {
          TBSYS_LOG(WARN, "fail to get master obi ups addr. master_inst_rs=%s, err=%d", master_inst_rs.to_cstring(), err);
        }
      }

      return err;
    }

    int ObUpsRpcStub::get_master_obi_rs(const ObServer &rootserver,
                                        ObServer &master_obi_rs,
                                        const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        ret = OB_ERROR;
      }
      else
      {
        ret = get_thread_buffer_(data_buff);
      }


      if (OB_SUCCESS == ret)
      {
        ret = client_mgr_->send_request(rootserver, OB_GET_MASTER_OBI_RS,
                                        DEFAULT_VERSION, timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to send request. ret: [%d]", ret);
        }
      }
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed,"
                    " pos: [%ld], ret: [%d].", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = master_obi_rs.deserialize(data_buff.get_data(),
                                        data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize the response. ret: %d", ret);
        }
      }

      return ret;
    }

    /* //add
       int ObUpsRpcStub :: ups_report_slave_failure(const common::ObServer &slave_add, const int64_t timeout_us)
       {
       int err = OB_SUCCESS;
       ObDataBuffer data_buff;
       if (NULL == client_mgr_)
       {
       TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
       err = OB_ERROR;
       }
       else
       {
       err = get_thread_buffer_(data_buff);
       }
    //step1: serialize slave_failure_info
    ObUpdateServerMain* ups = ObUpdateServerMain::get_instance();
    ObServer self_addr = ups->get_update_server();
    ObServer root_server = ups->get_update_server().get_root_server();
    ObMsgUpsSlaveFailure slave_failure_info;
    if (OB_SUCCESS == err)
    {
    slave_failure_info.my_addr_.set_ipv4_addr(self_addr.get_ipv4(), self_addr.get_port());
    slave_failure_info.slave_addr_.set_ipv4_addr(slave_add.get_ipv4(), slave_add.get_port());
    if (OB_SUCCESS != (err = slave_failure_info.serialize(data_buff);))
    {
    TBSYS_LOG(WARN, "serialize slave_failure_info err. err= %d", err);
    }
    }
    //step 2 send report to rootserver
    if (OB_SUCCESS == err)
    {
    err = client_manager_.send_request(root_server, OB_RS_UPS_SLAVE_FAILURE, slave_failure_info.MY_VERSION, timeout_us, data_buff);
    if (OB_SUCCESS != err)
    {
    TBSYS_LOG(WARN, "fail to send request to rootserver. err = %d", err);
    }
    }

    //setp3, deserialize response
    int64_t pos = 0;
    if (OB_SUCCESS == err)
    {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
    TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
    err = result_code.result_code_;
    }
    }
    return err;
    }*/

    //add
    int ObUpsRpcStub :: ups_register(const common::ObServer &root_server,
                                     const ObMsgUpsRegister &msg_register,
                                     int64_t &lease_renew_time,
                                     int64_t &cluster_id, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      //setp 1. serialize the register info
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = msg_register.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
        {
          TBSYS_LOG(WARN, "Serialize msg_register error, err=%d", err);
        }
      }

      // step 2. send request to root_server
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_RS_UPS_REGISTER, msg_register.MY_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "send request  failed, err[%d], rootserver addr = %s", err, root_server.to_cstring());
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      if (OB_SUCCESS == err)
      {
        err = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &lease_renew_time);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to decode lease_renew_time. err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "get lease_renew_time = %ld", lease_renew_time);
        }
      }
      if (OB_SUCCESS == err && pos < data_buff.get_position())
      {
        err = serialization::decode_vi64(data_buff.get_data(),
                                         data_buff.get_position(),
                                         pos, &cluster_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to decode cluster id. err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "get cluster id = %ld", cluster_id);
        }
      }
      if (OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
      {
        TBSYS_LOG(WARN, "fail to register to rootserver. err=%d", err);
      }
      else
      {
        //del pangtianze [Paxos rs_election] 20170329:
        //err = OB_SUCCESS;
        //del:e
      }
      return err;
    }

    int ObUpsRpcStub::send_keep_alive(const common::ObServer &slave_node)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->post_request(slave_node, OB_UPS_KEEP_ALIVE, DEFAULT_VERSION, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to post request to slave [%s], err = %d", slave_node.to_cstring(), err);
        }
      }
      return err;
    }

    int ObUpsRpcStub::renew_lease(const common::ObServer &rootserver, const common::ObMsgUpsHeartbeatResp& msg)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      if (OB_SUCCESS == err)
      {
        err = msg.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      }
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->post_request(rootserver, OB_RS_UPS_HEARTBEAT_RESPONSE, msg.MY_VERSION, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to post request to rootserver [%s], err = %d", rootserver.to_cstring(), err);
        }
      }
      return err;
    }

    int ObUpsRpcStub :: check_table_merged(const common::ObServer& root_server, const uint64_t version,
        bool &merged, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }
      // step 1.
      if (OB_SUCCESS == err)
      {
        err = common::serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), version);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "serialize version failed:version[%lu], err[%d].",
              version, err);
        }
      }

      // step 2.
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(root_server,
            OB_RS_CHECK_TABLET_MERGED, DEFAULT_VERSION, timeout_us, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to root server for check table merged failed"
              ":version[%lu], err[%d].", version, err);
        }
      }

      // step 3.
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }
      // step 4.
      if (OB_SUCCESS == err)
      {
        int64_t result_code = 0;
        err = common::serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &result_code);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result code from buff failed"
              "version[%lu], pos[%ld], err[%d].", version, pos, err);
        }
        else
        {
          merged = (0 != result_code);
        }
      }

      return err;
    }
    int ObUpsRpcStub::notify_rs_trigger_event(const ObServer &rootserver, const ObTriggerMsg &msg, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      if (OB_SUCCESS == err)
      {
        err = msg.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      }
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(rootserver, OB_HANDLE_TRIGGER_EVENT, DEFAULT_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "fail to send request to rootserver [%s], err = %d", rootserver.to_cstring(), err);
        }
      }
      return err;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    int ObUpsRpcStub::notify_rs_ddl_trigger_event(const ObServer &rootserver, const ObDdlTriggerMsg &msg, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      if (OB_SUCCESS == err)
      {
        err = msg.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      }
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(rootserver, OB_HANDLE_DDL_TRIGGER_EVENT, DEFAULT_VERSION, timeout_us, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "fail to send request to rootserver [%s], err = %d", rootserver.to_cstring(), err);
        }
      }
      return err;
    }
    //add:e
  } // end namespace updateserver
}
