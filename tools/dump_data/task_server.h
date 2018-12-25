#ifndef TASK_SERVER_H_
#define TASK_SERVER_H_

#include "rpc_stub.h"
#include "task_info.h"
#include "task_manager.h"
#include "task_factory.h"
#include "task_output.h"
#include "base_server.h"

namespace oceanbase
{
  namespace tools
  {
    class TaskServer: public BaseServer
    {
    public:
      // all tables in root server
      TaskServer(const char * file_name, const int64_t timeout_times, const int64_t max_count,
          const int64_t timeout, const common::ObServer & root_server);

      virtual ~TaskServer();

    public:
      // init
      int init(const int64_t thread, const int64_t queue, RpcStub * rpc,
          const char * dev, const int32_t port);

      // init task server
      virtual int initialize(void);
      // init service
      virtual int start_service(void);

      // dispatcher process
      int do_request(common::ObPacket * base_packet);

      // get task manager and factory
      TaskManager & get_task_manager(void);

      TaskFactory & get_task_factory(void);

      //dump_tablets
      void dump_task_info(const char *file);
    private:
      //signal handler
      void signal_handler(int sig);

      // check inner stat
      bool check_inner_stat(void) const;

      // fetch task
      int handle_fetch_task(common::ObPacket * packet);

      // finish task
      int handle_finish_task(common::ObPacket * packet);
    private:
      enum STAT
      {
        INVALID_STAT = 0,   // not init
        PREPARE_STAT = 100, // prepare service
        READY_STAT = 101,   // ready for service
        FINISH_STAT = 102,  // finish exit
      };
      STAT status_;
      RpcStub * rpc_;
      int64_t timeout_;
      int64_t memtable_version_;
      int64_t modify_timestamp_;
      const char * result_file_;
      TaskFactory task_factory_;
      TaskManager task_manager_;
      TaskOutput task_output_;
      common::ObServer root_server_;
      common::ObServer update_server_;
      common::ObSchemaManagerV2 schema_;
    };

    inline bool TaskServer::check_inner_stat(void) const
    {
      return ((rpc_ != NULL) && (result_file_ != NULL));
    }

    inline TaskManager & TaskServer::get_task_manager(void)
    {
      return task_manager_;
    }

    inline TaskFactory & TaskServer::get_task_factory(void)
    {
      return task_factory_;
    }
  }
}


#endif //TASK_SERVER_H_


