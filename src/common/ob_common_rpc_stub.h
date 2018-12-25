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
#ifndef __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__
#define __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_server.h"
#include "ob_client_manager.h"
#include "ob_result.h"
#include "thread_buffer.h"
#include "ob_packet.h"
#include "ob_lease_common.h"
#include "ob_obi_role.h"
#include "ob_scan_param.h"
#include "ob_scanner.h"
#include "ob_mutator.h"
namespace oceanbase
{
  namespace common
  {
    class ObCommonRpcStub
    {
      public:
        ObCommonRpcStub();
        virtual ~ObCommonRpcStub();

        int init(const ObClientManager *client_mgr);
        const ObClientManager* get_client_mgr() const;
        //add: master ups report slave failure to rootserver
        virtual int ups_report_slave_failure(const common::ObServer &slave_add, const int64_t timeout_us);

        // send commit log to Slave
        virtual int send_log(const ObServer& ups_slave, ObDataBuffer& log_data,
            const int64_t timeout_us);

        // send renew lease request to Master, used by Slave
        virtual int renew_lease(const common::ObServer& ups_master,
            const common::ObServer& slave_addr, const int64_t timeout_us);

        // send grant lease to Slave, used by Master
        virtual int grant_lease(const common::ObServer& slave, const ObLease& lease,
            const int64_t timeout_us);
        //
        // send quit info to Master, called when Slave quit
        virtual int slave_quit(const common::ObServer& master, const common::ObServer& slave_addr,
            const int64_t timeout_us);
        
        //send keep_alive msg to slave. used by Master
        virtual int send_keep_alive(const common::ObServer &slave);

        //send grand_lease_response msg to rootserver, used by UPS
        virtual int renew_lease(const common::ObServer &rootserver);

        virtual int send_obi_role(const common::ObServer& slave, const common::ObiRole obi_role);
        //send request to rs to get master_ups_addr
        virtual int get_master_ups_info(const ObServer& rs, ObServer &master_ups, const int64_t timeout_us);
        virtual int get_obi_role(const common::ObServer& rs, common::ObiRole &obi_role, const int64_t timeout_us);

        //mod hongchen [UNLIMIT_TABLE] 20161031:b
        //virtual int scan(const ObServer& ms, const common::ObScanParam& scan_param, common::ObScanner& scanner, const int64_t timeout);
        virtual int scan(const ObServer& ms, const common::ObScanParam& scan_param, common::ObScanner& scanner, const int64_t timeout, int64_t* session_id = NULL);
        virtual int scan_for_next_packet(const common::ServerSession& ssession ,const int64_t timeout, ObScanner& scanner);
        //mod hongchen [UNLIMIT_TABLE] 20161031:e
        virtual int mutate(const ObServer& update_server, const common::ObMutator& mutator, const int64_t timeout);
      private:
        int get_thread_buffer_(ObDataBuffer& data_buff);

      private:
        static const int32_t DEFAULT_VERSION;
        static const int64_t DEFAULT_RPC_TIMEOUT_US;

      private:
        ThreadSpecificBuffer thread_buffer_;
      protected:
        const ObClientManager* client_mgr_;
    };
  }
}

#endif // __OCEANBASE_COMMON_OB_COMMON_RPC_STUB_H__
