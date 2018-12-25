/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_stub.h,v 0.1 2010/09/27 16:59:49 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_UPDATESERVER_OB_UPS_RPC_STUB_H__
#define __OCEANBASE_UPDATESERVER_OB_UPS_RPC_STUB_H__

#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_common_rpc_stub.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_mutator.h"
#include "ob_ups_utils.h"
#include "ob_schema_mgrv2.h"
#include "ob_ups_fetch_runnable.h"
#include "ob_slave_sync_type.h"
#include "common/ob_log_cursor.h"
#include "ob_remote_log_src.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace common
  {
    class ObTriggerMsg;
    class ObDdlTriggerMsg; //add zhaoqiong [Schema Manager] 20150327
  };
  namespace updateserver
  {
    class ObFetchLogReq;
    class ObFetchedLog;

    class ObUpsRpcStub : public common::ObCommonRpcStub
    {
      public:
        ObUpsRpcStub();
        virtual ~ObUpsRpcStub();

      public:
        //add zhaoqiong [fixed for Backup]:20150811:b
        int init(const common::ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer);
        //add:e
        virtual int ups_register(const common::ObServer &root_addr,
                                 const ObMsgUpsRegister &msg_register,
                                 int64_t &lease_renew_time, int64_t &cluster_id,
                                 const int64_t timeout_us);
        int get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master, const int64_t timeout_us);
        virtual int ups_report_slave_failure(const common::ObServer &slave_add, const int64_t timeout_us);

        //send keep_alive to slave ups. used by UPS Master
        int send_keep_alive(const common::ObServer &slave_node);
        // fetch schema from Root Server, used by UPS Master.
        virtual int fetch_schema(const common::ObServer& root_addr, const int64_t timestamp,
            CommonSchemaManagerWrapper& schema, const bool only_core_tables, const int64_t timeout_us);

        //send response to rootserver, used by UPS
        int renew_lease(const common::ObServer & rootserver, const common::ObMsgUpsHeartbeatResp& msg);
        int ups_rs_keep_alive(const common::ObServer &root_server);
        virtual int slave_register_followed(const common::ObServer& master, const ObSlaveInfo &slave_info,
            ObUpsFetchParam& fetch_param, const int64_t timeout_us);

        //add zhaoqiong [fixed for Backup]:20150811:b
        virtual int backup_register(const common::ObServer& master, const ObSlaveInfo &slave_info,
            ObUpsFetchParam& fetch_param, const int64_t timeout_us);
        //add:e
        int slave_register_followed(const ObServer& master, const ObSlaveInfo &slave_info,
                    ObSlaveSyncType slave_type, ObUpsFetchParam& fetch_param,
                    uint64_t &max_log_seq, const int64_t timeout_us);
        virtual int slave_register_standalone(const common::ObServer& master,
            const uint64_t log_id, const uint64_t log_seq,
            uint64_t &log_id_res, uint64_t &log_seq_res, const int64_t timeout_us);
        // send freeze memtable resp.
        virtual int send_freeze_memtable_resp(const common::ObServer& root_server,
            const common::ObServer& ups_master, const int64_t schema_timestamp, const int64_t timeout_us);
        virtual int report_freeze(const common::ObServer &root_server,
            const common::ObServer &ups_master, const int64_t frozen_version, const int64_t timeout_us);
        virtual int send_mutator_apply(const common::ObServer& ups_master,
            const common::ObMutator &mutator, const int64_t timeout_us);

        virtual int fetch_lsync(const common::ObServer &lsync, const uint64_t log_id, const uint64_t log_seq,
            char* &log_data, int64_t &log_len, const int64_t timeout_us);
        virtual int fetch_log(const ObServer &master, const ObFetchLogReq& req, ObFetchedLog& fetched_log, const int64_t timeout_us);

        virtual int fill_log_cursor(const ObServer &master, common::ObLogCursor& cursor, const int64_t timeout_us);
        virtual int get_rs_last_frozen_version(const ObServer &root_server,
                                               int64_t& frozen_version, const int64_t timeout_us);

        virtual int check_table_merged(const ObServer &root_server, const uint64_t version, bool &merged, const int64_t timeout_us);
        virtual int notify_rs_trigger_event(const ObServer &rootserver, const ObTriggerMsg &msg, const int64_t timeout_us);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief if fetch_schema can not fetch a completion schema
         *        use this func fetch follow schema part
         */
        virtual int fetch_schema_next(const int64_t timeout_us, const common::ObServer& root_server, const int64_t timestamp,
            const int64_t table_start_pos,const int64_t column_start_pos, CommonSchemaManager& schema_mgr);
        /**
         * @brief notify rs trigger a ddl event
         */
        virtual int notify_rs_ddl_trigger_event(const ObServer &rootserver, const ObDdlTriggerMsg &msg, const int64_t timeout_us);
        //add:e
        int get_master_obi_rs(const ObServer &rootserver,
                              ObServer &master_obi_rs,
                              const int64_t timeout);

      private:
        template<typename Input, typename Output>
        int send_request(const common::ObServer& server, const int pcode,
                         const Input& input, Output& output, const int64_t timeout);
        template <class Input>
        int send_command(const common::ObServer &server, const int pcode, const Input &param, const int64_t timeout);

        int get_thread_buffer_(common::ObDataBuffer& data_buff);

      private:
        static const int32_t DEFAULT_VERSION = 1;
        common::ThreadSpecificBuffer *thread_buffer_; //add zhaoqiong [fixed for Backup]:20150811
    };
  }
}

#endif //__OB_UPS_RPC_STUB_H__
