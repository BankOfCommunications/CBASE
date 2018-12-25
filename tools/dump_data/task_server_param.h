#ifndef TASK_SERVER_PARAM_H_
#define TASK_SERVER_PARAM_H_

#include "common/ob_define.h"
#include "task_table_conf.h"
#include <vector>

namespace oceanbase
{
  namespace tools
  {
    class TaskServerParam
    {
    public:
      TaskServerParam();
      virtual ~TaskServerParam();

    public:
      static const int32_t OB_MAX_FILE_NAME = 1024;
      static const int32_t OB_MAX_IP_SIZE = 64;
      static const int32_t OB_MAX_LOG_LEVEL = 32;

      /// load string
      static int load_string(char * dest, const int32_t size, const char * section,
          const char * name, bool require = true);

      /// load from configure file
      int load_from_config(const char * file);

      const char * get_result_file(void) const
      {
        return result_file_;
      }

      const char * get_tablet_list_file(void) const
      {
        return tablet_list_file_;
      }

      const char * get_dev_name(void) const
      {
        return dev_name_;
      }

      int32_t get_task_server_port(void) const
      {
        return server_listen_port_;
      }

      const char * get_root_server_ip(void) const
      {
        return root_server_ip_;
      }

      int32_t get_root_server_port(void) const
      {
        return root_server_port_;
      }

      const char * get_log_name(void) const
      {
        return log_name_;
      }

      const char * get_log_level(void) const
      {
        return log_level_;
      }

      int32_t get_max_log_size(void) const
      {
        return max_log_size_;
      }

      int32_t get_max_visit_count(void) const
      {
        return max_visit_count_;
      }

      int32_t get_timeout_times(void) const
      {
        return max_timeout_times_;
      }

      int32_t get_thread_count(void) const
      {
        return task_thread_count_;
      }

      int32_t get_queue_size(void) const
      {
        return task_queue_size_;
      }

      int64_t get_network_timeout(void) const
      {
        return network_timeout_;
      }

      const std::vector<TableConf> &get_all_conf() const
      {
        return confs_;
      }

    private:
      char result_file_[OB_MAX_FILE_NAME];
      char dev_name_[OB_MAX_IP_SIZE];
      char root_server_ip_[OB_MAX_IP_SIZE];
      const char *log_name_;
      char tablet_list_file_[OB_MAX_FILE_NAME];
      char log_level_[OB_MAX_LOG_LEVEL];
      int32_t max_visit_count_;
      int32_t max_timeout_times_;
      int32_t max_log_size_;
      int32_t root_server_port_;
      int32_t server_listen_port_;
      int32_t task_thread_count_;
      int32_t task_queue_size_;
      int64_t network_timeout_;

      std::vector<TableConf> confs_;
    };
  }
}


#endif // TASK_SERVER_PARAM_H_
