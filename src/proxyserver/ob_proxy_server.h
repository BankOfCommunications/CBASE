/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_PROXYSERVER_PROXYSERVER_H_
#define OCEANBASE_PROXYSERVER_PROXYSERVER_H_

#include <pthread.h>
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "ob_proxy_server_config.h"
#include "ob_proxy_service.h"

namespace oceanbase
{
  namespace proxyserver
  {
    class ObProxyServer : public common::ObSingleServer
    {
      public:
        static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int32_t RPC_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
      public:
        ObProxyServer(ObProxyServerConfig &config);
        ~ObProxyServer();
      public:
        /** called before start server */
        virtual int initialize();
        /** called after start transport and listen port*/
        virtual int start_service();
        virtual void wait_for_queue();
        virtual void destroy();

        virtual int do_request(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        const common::ThreadSpecificBuffer* get_thread_specific_rpc_buffer() const;

        const common::ObServer& get_self() const;

        const ObProxyServerConfig & get_config() const;
        ObProxyServerConfig & get_config();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObProxyServer);
        int set_self(const char* dev_name, const int32_t port);
        int64_t get_process_timeout_time(const int64_t receive_time,
                                         const int64_t network_timeout);
      private:
        ObProxyServerConfig &config_;
        ObProxyService service_;

        // thread specific buffers
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;

        // server information
        common::ObServer self_;
    };

  } // end namespace proxyserver
} // end namespace oceanbase


#endif //OCEANBASE_PROXYSERVER_PROXYSERVER_H_

