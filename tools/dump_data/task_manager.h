#ifndef TASK_MANAGER_H_
#define TASK_MANAGER_H_

#include <map>
#include "rpc_stub.h"
#include "task_info.h"

namespace oceanbase
{
  namespace tools
  {
    class TaskManager
    {
    public:
      static const int64_t DEFAULT_COUNT = 10;  // 10 tasks simuntaneously
      static const int64_t DEFAULT_TIMES = 2;   // 2 times for timeout judgement
      static const int64_t MAX_WAIT_TIME = 1000 * 1000 * 30;//1000 * 1000 * 60 * 5; // 5 mins
      // timeout = avg * avg_times 
      TaskManager(const int64_t avg_times = DEFAULT_TIMES, const int64_t max_count = DEFAULT_COUNT);
      virtual ~TaskManager();
    
    public:
      // get task token
      int64_t get_token(void) const;

      // get task count
      uint64_t get_count(void) const;

      // insert task
      int insert_task(const TabletLocation & location, TaskInfo & task);

      // finish task
      int finish_task(const bool result, const TaskInfo & task);

      // fetch task
      int fetch_task(TaskCounter & counter, TaskInfo & task);

      // search for a available tablet version
      bool get_tablet_version(int64_t memtable_version, int64_t &version);

      // setup all tasks version
      void setup_all_tasks_vesion(int64_t version);
      
      // check all task finished
      bool check_finish(void) const;
      
      // check and repair all task namespace and merge_server addr
      bool check_tasks(const uint64_t tablet_count);
      
      // print info
      int print_info(void) const;

      void dump_tablets(const char *file);
    private:
      // get server task count
      int64_t get_server_task_count(const common::ObServer & server) const;
      
      // repair invalid task
      int repair_all_tasks(int64_t & count);
      
      //
      void print_access_server();
    private:
      // serve max count for simultaneously visit every server
      int64_t max_count_;
      // task token
      int64_t task_token_;
      tbsys::CThreadMutex lock_;
      // alloc task id
      uint64_t task_id_alloc_;
      uint64_t total_task_count_;
      
      // compute task timeout
      int64_t avg_times_;
      int64_t total_finish_time_;
      int64_t total_finish_count_;

      int64_t tablet_version_;

      // task queues
      std::map<uint64_t, TaskInfo> wait_queue_;
      std::map<uint64_t, TaskInfo> doing_queue_;
      std::map<uint64_t, TaskInfo> complete_queue_;
      
      // access server
      std::map<common::ObServer, int64_t> working_queue_;
      // merge server account
      std::map<common::ObServer, int64_t> server_manager_;
    };

    inline int64_t TaskManager::get_token(void) const
    {
      return task_token_;
    }
    
    inline uint64_t TaskManager::get_count(void) const
    {
      return total_task_count_;
    }
  }
}



#endif //TASK_MANAGER_H_

