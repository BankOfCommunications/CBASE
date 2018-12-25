/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *
 *  Version: 1.0 : 2010/08/22
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 */
#ifndef OCEANBASE_PROXYSERVER_PROXYSERVICE_H_
#define OCEANBASE_PROXYSERVER_PROXYSERVICE_H_

#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "easy_io.h"
#include "ob_yunti_proxy.h"
#include "ob_proxy_reader.h"
#include "common/ob_data_source_desc.h"

namespace oceanbase
{
  namespace proxyserver
  {
    class ObProxyServer;
    class ObProxyService
    {
      public:
        ObProxyService();
        ~ObProxyService();
      public:
        const static int64_t DEFAULT_PROXY_NUM = 10;
        typedef hash::ObHashMap<const char*, Proxy*> hashmap;
        int initialize(ObProxyServer* proxy_server);
        int start();
        int destroy();

      public:
        int do_request(
            const int64_t receive_time,
            const int32_t packet_code,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time = 0);

        int proxy_fetch_data(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int proxy_fetch_range(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

      private:
        int get_yunti_reader(
            YuntiProxy* proxy,
            ObDataSourceDesc &desc,
            const char* sstable_dir,
            int64_t &local_tablet_version,
            int16_t &local_sstable_version,
            int64_t sstable_type,
            ProxyReader* &proxy_reader);
        int get_yunti_range_reader(
            YuntiProxy* proxy,
            const char* sstable_dir,
            const char* table_name,
            ProxyReader* &proxy_reader);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObProxyService);
        ObProxyServer* proxy_server_;
        bool inited_;
        YuntiProxy yunti_proxy_;
        hashmap proxy_map_;

        common::ThreadSpecificBuffer query_service_buffer_;
    };


  } // end namespace proxyserver
} // end namespace oceanbase


#endif //OCEANBASE_PROXYSERVER_PROXYSERVICE_H_

