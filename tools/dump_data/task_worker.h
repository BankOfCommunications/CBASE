#ifndef _TASK_WORKER_H_
#define _TASK_WORKER_H_

#include "common/ob_server.h"
#include "common/ob_scan_param.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "common/page_arena.h"

#include "base_client.h"
#include "rpc_stub.h"
#include "task_info.h"
#include "task_env.h"
#include "task_stats.h"
#include "task_output_file.h"
#include "task_worker_param.h"

namespace oceanbase
{
  namespace tools
  {
    class TaskWorker: public BaseClient
    {
    public:
      TaskWorker(Env *env, const common::ObServer & server, const int64_t timeout,
                 const uint64_t retry_times, const TaskWorkerParam *param);
      virtual ~TaskWorker();

    public:
      /// man file path len
      static const int64_t MAX_PATH_LEN = 1024;
      /// retry times
      static const uint64_t MAX_RETRY_TIMES = 3;
      /// retry interval time
      static const int64_t RETRY_INTERVAL = 2000 * 1000; //usleep ms

      /// init client
      virtual int init(RpcStub * rpc, const char * data_path);

      /// fetch a new task
      virtual int fetch_task(TaskCounter & count, TaskInfo & task);

      /// report task complete succ or failed
      virtual int report_task(const int64_t result_code, const char * file_name, const TaskInfo & task);

      /// do task
      int do_task(const TaskInfo & task, char file_name[], const int64_t len);

    private:
      /// check inner stat
      bool check_inner_stat(void) const;

      /// do scan task
      int scan(const int64_t index, const TabletLocation & list, const common::ObScanParam & param,
          common::ObScanner & result);

      /// check finish
      int check_finish(const common::ObScanParam & param, const common::ObScanner & result, bool & finish);

      /// check result
      int check_result(const common::ObScanParam & param, const common::ObScanner & result);

      /// modify param
      int modify_param(const common::ObScanner & result, common::ModuleArena & allocator,
          common::ObNewRange & new_range, common::ObScanParam & param);

    protected:
      /// output scan result
      virtual int output(TaskOutputFile *file, common::ObScanner & result) const;

      /// do the task output file or stdout
      virtual int do_task(TaskOutputFile * file, const TaskInfo & task);

      /// do task output file
      virtual int do_task(const char * file, const TaskInfo & task);

      virtual int make_file_name(const TaskInfo &task, char file_name[], const int64_t len);

    private:
      RpcStub * rpc_;
      char * temp_result_;
      const char * output_path_;
      int64_t retry_times_;
      int64_t timeout_;
      common::ObServer server_;
      Env *env_;
      const TaskWorkerParam *param_;
      TaskStats stat_;
    };

    inline bool TaskWorker::check_inner_stat(void) const
    {
      return ((NULL != rpc_) && (NULL != temp_result_));
    }
  }
}

#endif // _TASK_Worker_H_

