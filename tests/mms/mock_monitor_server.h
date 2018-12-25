#ifndef MOCK_MONITOR_SERVER_H_ 
#define MOCK_MONITOR_SERVER_H_

#include "mock_server.h"
#include "mms/ob_monitor.h"

using namespace oceanbase::mms;
namespace oceanbase
{
  namespace mms
  {
    namespace tests
    {
      class MonitorHandler;
      class MockMonitorServer : public MockServer
      {
        public:
          MockMonitorServer(int32_t port, int64_t retry_time, int64_t timeout, int64_t lease_interval, int64_t renew_interval);
          ~MockMonitorServer()
          {
          }
          int initialize();
          void stop();
          int start_service();
          int do_request(ObPacket* base_packet);
          tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection, tbnet::Packet *packet);
          bool handlePacketQueue(tbnet::Packet *packet, void *args);
        private:
          MonitorHandler * handler_;
          int32_t port_;
          int64_t retry_times_;
          int64_t timeout_;
          ObMonitor monitor_;
          ObServer self_;
          int64_t lease_interval_;
          int64_t renew_interval_;
      };

      class MonitorHandler 
        : public INodeRegister, 
        public ISlaveFailure,
        public IChangeMaster
      {
        public:
          MonitorHandler(MockMonitorServer *mock_node)
          {
            mock_node_ = mock_node;
          }
          ~MonitorHandler()
          {
            mock_node_ = NULL;
          }
          int handleNodeRegister(const ObServerExt &node, const char* app_name, const char *inst_name, bool is_master)
          {
            UNUSED(node);
            UNUSED(app_name);
            UNUSED(inst_name);
            UNUSED(is_master);
            int err = OB_SUCCESS;
            TBSYS_LOG(INFO, "a new node register. should add to list");
            return err;
          }

          int handleSlaveFailure(const common::ObServer &slave)
          {
            int err = OB_SUCCESS;
            TBSYS_LOG(INFO, "a slave node failure. ip=%d", slave.get_ipv4());
            return err;
          }

          int handleChangeMaster(const common::ObServer &new_master)
          {
            int err = OB_SUCCESS;
            TBSYS_LOG(INFO, "change master. new master ip=%d", new_master.get_ipv4());
            return err;
          }

        private:
          MockMonitorServer *mock_node_;
      }; 
    }
  }
}

#endif //MOCK_UPDATE_SERVER_H_
