#ifndef __TASK_OUTPUT_FILE_H__
#define  __TASK_OUTPUT_FILE_H__

#include "task_env.h"
#include "common/ob_new_scanner.h"
#include "common/data_buffer.h"
#include "common/ob_malloc.h"
#include "task_worker_param.h"
#include "task_table_conf.h"

#include <vector>

namespace oceanbase
{
  namespace tools
  {
    class TaskOutputFile
    {
      public:
        TaskOutputFile();

        TaskOutputFile(const char *file_name, Env *env);

        ~TaskOutputFile();

        //open file_name_
        int open();

        //append ObScanner to output file
        int append(common::ObScanner &scanner);

        //flush data
        int flush();

        //close file_name_
        int close();

        //set table related info
        int set_table_conf(const TableConf *conf);
      private:
        int convert_result(const common::ObScanner &result, common::ObDataBuffer &buffer) const;

        int handle_rk_null(common::ObDataBuffer &buffer, common::ObCellInfo *cell, TableConf::ColumnIterator &itr, bool add_delima) const;

        //get temp buffer hold converted result
        int get_buffer(common::ObDataBuffer &buffer);

        common::ObMemBuf buffer_;
        const char *file_name_;
        Env *env_;
        WritableFile *file_;
        //when true, append rowkey info to each line
        bool split_rowkey_;
        const TableConf *conf_;
        RecordDelima delima_;
        RecordDelima rec_delima_;
    };
  }
}

#endif
