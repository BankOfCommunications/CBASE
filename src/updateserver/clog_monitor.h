/**
 * Authors: lbzhong
 */

#ifndef OCEANBASE_UPDATESERVER_CLOG_MONITOR_H_
#define OCEANBASE_UPDATESERVER_CLOG_MONITOR_H_
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_base_client.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_result.h"
#include "clog_status.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    class ClogMonitor
    {
    public:
      static const int32_t MONITOR_MAX_UPS_NUM = 14;
      static const int32_t MONITOR_MAX_IP_LEN = 32;
      static ClogMonitor* get_instance();
      virtual ~ClogMonitor();
      virtual int start(const int argc, char *argv[]);
    protected:
      ClogMonitor();
      static ClogMonitor* instance_;
      int initialize(const ObServer& server);
      void print_usage(const char *prog_name);
      int parse_cmd_line(const int argc, char *const argv[]);
      int do_work();
      int get_ups_list(const ObServer &server);
      int get_ups_clog_status(const ObServer &server, ClogStatus &status);
      int refresh_moitor();
      int parse_ups_list(const char* str, bool &is_master, char* master_ip, int32_t &master_port);
      int remove_server(const ObServer &server);

    private:
      ObServer master_ups_;
      ObBaseClient base_client_; //UNUSED
      ObClientManager client_;
      int64_t timeout_;
      ObServer ups_[MONITOR_MAX_UPS_NUM];
      ClogStatus ups_clog_status_[MONITOR_MAX_UPS_NUM];
      int32_t ups_num_;
    }; // end class ClogMonitor

  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_CLOG_MONITOR_H_
