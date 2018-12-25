#include "mock_server.h"
#include "mms/ob_monitor.h"
#include "mms/ob_node.h"


using namespace oceanbase::mms;
namespace oceanbase
{
  namespace mms
  {
    namespace tests
    {
      class Handler;
      class MockNodeServer : public MockServer
      {
        public:
          MockNodeServer(int32_t port, int64_t retry_time, 
                         int64_t timeout, ObServer monitor, 
                         char* app_name, char* instance_name, 
                         char* hostname);
          ~MockNodeServer();
          int initialize();
          int start_service();
          int start_();
          void stop();
          ObNode* get_node()
          {
            return &node_;
          }
          int do_request(ObPacket* base_packet);
          tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection, tbnet::Packet *packet);
          bool handlePacketQueue(tbnet::Packet *packet, void *args);
          MMS_Status& get_status()
          {
            return status_;
          }
        private:
          int32_t port_;
          int64_t retry_times_;
          int64_t timeout_;
          ObServer monitor_;
          ObServer self_;
          char * hostname_;
          ObLease lease_;
          char* instance_name_;
          char* app_name_;
          MMS_Status status_;
          Handler *handler_;
          ObNode node_;
      };

      class Handler 
        : public IMMSStopSlave, 
        public ISlaveDown,
        public ITransfer2Master,
        public IMasterChanged,
        public ILeaseTimeout
      {
        public:
          Handler(MockNodeServer *mock_node)
          {
            if(mock_node != NULL)
            {
              mock_node_ = mock_node;
            }
          }
          ~Handler()
          {
            mock_node_ = NULL;
          }

          int handleMMSStopSlave()
          {
            TBSYS_LOG(INFO, "stop slave handler");
            return OB_SUCCESS;
          }

          int handleSlaveDown(const common::ObServer &server)
          {
            TBSYS_LOG(INFO, "slave down. need not to write log to %d", server.get_ipv4());
            return OB_SUCCESS;
          }

          int handleTransfer2Master()
          {
            TBSYS_LOG(INFO, "tranfer my role to master.");   
            MMS_Status &status = mock_node_->get_status();
            status = MASTER_ACTIVE;
            TBSYS_LOG(INFO, "I am master now");
            return OB_SUCCESS;
          }

          int handleMasterChanged()
          {
            TBSYS_LOG(INFO, "master have changed.");
            TBSYS_LOG(INFO, "I need to ask register to the new master");
            return OB_SUCCESS;
          }

          int handleLeaseTimeout()
          {
            TBSYS_LOG(INFO, "oh, lease timeout!!");
            TBSYS_LOG(INFO, "i am died !");
            mock_node_->stop();
            return OB_SUCCESS;
          }

        private:
          MockNodeServer *mock_node_;
      }; 
    }
  }
}

