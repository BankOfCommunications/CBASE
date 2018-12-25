/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_data_source_mgr.h
 *
 * Authors:
 *  yongle.xh@alipay.com
 *
 */
#ifndef _OB_ROOT_DATA_SOURCE_MGR_
#define _OB_ROOT_DATA_SOURCE_MGR__

#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_data_source_desc.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_new_scanner.h"
#include "common/ob_list.h"
#include "common/ob_vector.h"
#include "ob_chunk_server_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    using namespace common;
    class ObRootRpcStub;

    struct ObDataSourceProxy
    { // all memory is maintained in ObDataSourceProxyList
      bool is_support(const ObString& data_source_name);
      int64_t to_string(char* buffer, const int64_t length) const;
      ObServer server_;
      ObList<ObString> support_types_;
      int64_t failed_count_;
      int64_t total_count_;
    };

    struct ObDataSourceProxyList
    {
      static const int64_t MAX_DATA_SOURCE_SUPPORT_NAME_STRING_LENGTH = 512;
      ObDataSourceProxyList();
      ~ObDataSourceProxyList();
      int add_proxy(ObServer& server, ObString& support_types);

      ObVector<ObDataSourceProxy*> proxys_;
      common::ModulePageAllocator mod_;
      common::ModuleArena allocator_;
    };

    class ObDataSourceMgr
    { //TODO: only mgr the proxy of this cluster
      static const int64_t MAX_DATA_SOURCE_PROXY_NUM = 1024;
      static const int64_t UPDATE_DATA_SOURCE_PERIOD = 60*1000*1000L; //60s
      static const int64_t MAX_FAILED_COUNT = 3;

      public:
        ObDataSourceMgr();
        virtual ~ObDataSourceMgr();

        void reset();
        void print_data_source_info() const;

        //query table data_source_proxy from ob
        int update_data_source_proxy();
        // if there is a data source on the dest_server, use it first; if not ,choose randomly
        int select_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
            ObServer& dest_server, ObServer& data_source_server);
        // choose randomly
        int select_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
            ObServer& data_source_server);
        // get next data source not same as data_source_server, used in fetch range list
        int get_next_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
            ObServer& data_source_server);
        int get_next_proxy(ObString& uri, ObServer& data_source_server);
        int get_proxy_it(const ObServer& server, ObVector<ObDataSourceProxy*>::const_iterator& it);
        inline void set_server_manager(ObChunkServerManager *server_manager) {server_manager_ = server_manager;}
        inline void set_rpc_stub(ObRootRpcStub *rpc_stub) { rpc_stub_ = rpc_stub; }
        inline void set_config(ObRootServerConfig *config) {config_ = config;}

        void inc_failed_count(const ObServer& data_source_server);
        void dec_failed_count(const ObServer& data_source_server);
      private:
        int64_t last_update_time_;
        ObDataSourceProxyList* proxy_list_;
        ObChunkServerManager *server_manager_;
        ObRootRpcStub *rpc_stub_;
        ObRootServerConfig *config_;

        mutable tbsys::CRWLock data_source_lock_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
    };
  }
}

#endif // _OB_ROOT_DATA_SOURCE_MGR_
