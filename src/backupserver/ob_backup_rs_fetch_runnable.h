#ifndef OCEANBASE_BACKUPSERVER_RS_FETCH_RUNNABLE_H_
#define OCEANBASE_BACKUPSERVER_RS_FETCH_RUNNABLE_H_

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_vector.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace backupserver
  {
    using namespace oceanbase::common;
    class ObBackupRootFetchRunnable : public tbsys::CDefaultRunnable
    {
      public:
        static const char* FIRST_META;
        static const int64_t WAIT_INTERVAL = 100 * 1000; //100ms
        static const int64_t DEFAULT_LIMIT_RATE = 1L << 14; // 16 * 1024 KBps
        static const int DEFAULT_FETCH_RETRY_TIMES = 3;
        static const char* DEFAULT_FETCH_OPTION;
      public:
        ObBackupRootFetchRunnable();

        int init(const ObServer &master, const char* log_dir);


        int wait_fetch_done();

        void run(tbsys::CThread* thread, void* arg);

      private:
        int set_usr_opt(const char* opt);
        int get_first_meta();
        bool exists_(const char * file_name, const char* fn_ext) const;

        bool remove_(const char * file_name, const char* fn_ext) const;

        int gen_full_name_(const char * file_name, const char* fn_ext, char *buf, const int buf_len) const;

        int gen_fetch_cmd_(const char * file_name, const char* fn_ext, char* cmd, const int64_t size);

        bool is_backup_done_;
        int64_t limit_rate_;
        ObServer master_;
        bool is_initialized_;
        char cwd_[OB_MAX_FILE_NAME_LENGTH];
        char log_dir_[OB_MAX_FILE_NAME_LENGTH];
        char *usr_opt_;
    };
  } /* backupserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOTSERVER_FETCH_THREAD_H_ */
#include "common/ob_fetch_runnable.h"

