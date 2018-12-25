#ifndef __TASK_WORKER_PARAM_H__
#define  __TASK_WORKER_PARAM_H__

#include <vector>
#include "hdfs_env.h"
#include "task_table_conf.h"

namespace oceanbase {
  namespace tools {

    class TaskWorkerParam {
      public:
        enum TaskFileType {
          POSIX_FILE,
          HDFS_FILE
        };

      public:
        static const int kHdfsGroupBuffSize = 1024;
        static const int kHdfsMaxGroupNr = 15;

        TaskWorkerParam();

        virtual ~TaskWorkerParam();

        int load(const char *file);

        const HdfsParam &hdfs_param() const { return hdfs_param_; }

        int task_file_type() const { return task_file_type_; }

        void DebugString();

        int get_table_conf(int64_t table_id, const TableConf *&conf) const;

        int get_table_conf(const common::ObString &table_name, const TableConf *&conf) const;
      public:
        int load_hdfs_config();
        int load_hdfs_config_env();

        HdfsParam hdfs_param_;
        int task_file_type_;
        char hdfs_group_buffer_[kHdfsGroupBuffSize];
        char *hdfs_group_ptr_[kHdfsMaxGroupNr];
        std::vector<TableConf> table_confs_;
    };
  }
}

#endif
