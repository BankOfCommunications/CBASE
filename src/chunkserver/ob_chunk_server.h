/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

#include <pthread.h>
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_file_service.h"
#include "common/ob_file_client.h"
#include "common/ob_general_rpc_stub.h"
#include "common/ob_statistics.h"
#include "ob_chunk_service.h"
#include "ob_tablet_manager.h"
#include "ob_chunk_server_stat.h"
#include "ob_rpc_proxy.h"
#include "ob_chunk_server_config.h"
#include "common/ob_common_stat.h"


namespace oceanbase
{
  namespace common
  {
    class ObConfigManager;
  }
  using oceanbase::common::ObConfigManager;
  namespace chunkserver
  {
    class ObChunkServer : public common::ObSingleServer
    {
      public:
        static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int32_t RPC_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
      public:
        ObChunkServer(ObChunkServerConfig &config, ObConfigManager &config_mgr);
        ~ObChunkServer();
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

        const common::ObClientManager& get_client_manager() const;
        const common::ObServer& get_self() const;
        const common::ObServer get_root_server() const;
        //add pangtianze [Paxos rs_election] 20150708:b
        /**
         * @brief set all related rs member variable
         * @param server
         */
        void set_all_root_server(const common::ObServer &server);
        //add:e
        //add huangjianwei [Paxos rs_election] 20160517:b
        void set_root_server(const common::ObServer &server);
        //add:e
        const ObChunkServerConfig & get_config() const;
        ObConfigManager & get_config_mgr();
        ObChunkServerConfig & get_config() ;

        int reload_config();

        ObChunkServerStatManager & get_stat_manager();
        const ObTabletManager & get_tablet_manager() const ;
        ObTabletManager & get_tablet_manager() ;
        //ObRootServerRpcStub & get_rs_rpc_stub();
        common::ObGeneralRpcStub & get_rpc_stub();
        ObMergerRpcProxy* get_rpc_proxy();
        common::ObFileClient& get_file_client();
        common::ObFileService& get_file_service();
        common::ObMergerSchemaManager* get_schema_manager();
        int init_merge_join_rpc();
        //add huangjianwei [Paxos rs_election] 20160517:b
        int init_rpc();
        //add:e

        int schedule_report_tablet() { return service_.schedule_report_tablet(); }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObChunkServer);
        //add pangtianze [Paxos rs_election] 20150710:b
        int init_root_server();
        //add:e
        int set_self(const char* dev_name, const int32_t port);
        int64_t get_process_timeout_time(const int64_t receive_time,
                                         const int64_t network_timeout);
        int init_file_service(const int32_t queue_size,
            const int32_t thread_cout, const int32_t band_limit);
      private:
        // request service handler
        ObChunkService service_;
        ObTabletManager tablet_manager_;
        ObChunkServerConfig &config_;
        ObConfigManager &config_mgr_;
        ObChunkServerStatManager stat_;

        // ob file service
        common::ObFileService file_service_;
        // ob file client
        common::ObFileClient file_client_;
        common::ThreadSpecificBuffer file_client_rpc_buffer_;

        // network objects.
        common::ObClientManager  client_manager_;
        //ObRootServerRpcStub rs_rpc_stub_;

        common::ObGeneralRpcStub rpc_stub_;
        ObSqlRpcStub sql_rpc_stub_;
        ObMergerRpcProxy* rpc_proxy_;
        common::ObMergerSchemaManager *schema_mgr_;

        // thread specific buffers
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;

        // server information
        common::ObServer self_;
        //add pangtianze [Paxos rs_election] 20150708:b
        common::ObServer root_server_;
        //add:e
    };

#ifndef CS_RPC_CALL
#define CS_RPC_CALL(func, server, args...)                              \
    THE_CHUNK_SERVER.get_rpc_stub().func(                               \
      THE_CHUNK_SERVER.get_config().network_timeout, server, args)
#endif

#ifndef CS_RPC_CALL_RS
#define CS_RPC_CALL_RS(func, args...)                          \
    THE_CHUNK_SERVER.get_rpc_stub().func(                      \
      THE_CHUNK_SERVER.get_config().network_timeout,           \
      THE_CHUNK_SERVER.get_root_server() , args)
#endif

#define RPC_BUSY_WAIT(running, ret, rpc)                                \
    while (running)                                                     \
    {                                                                   \
      ret = (rpc);                                                      \
      if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) break;      \
      usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_config().network_timeout)); \
    }

#define RPC_RETRY_WAIT(running, retry_times, ret, rpc)                  \
    while (running && retry_times-- > 0)                                \
    {                                                                   \
      ret = (rpc);                                                      \
      if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) break;      \
      usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_config().network_timeout)); \
    }


  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

