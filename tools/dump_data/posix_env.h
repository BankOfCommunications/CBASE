#ifndef __POSIX_ENV_H__
#define  __POSIX_ENV_H__

#include "task_env.h"
#include "file_utils.h"
#include "common/ob_define.h"


namespace oceanbase {
  namespace tools {
    class PosixWritableFile : public WritableFile {
      public:
        PosixWritableFile(int fd) : fd_(fd), file_(fd_) { }

        ~PosixWritableFile();

        virtual int append(const common::ObDataBuffer & data);

        virtual int close();

        virtual int flush();

        virtual int sync();
      private:
        int fd_;
        common::FileUtils file_;
    };

    class PosixEnv : public Env {
      public:
        PosixEnv() { }

        ~PosixEnv() { }

        int rmfile(const char *path);

        int rename(const char *src, const char *target);

        virtual WritableFile *console();

        //create a new writable file
        virtual int new_writable_file(const std::string &file_name, WritableFile *&file);

        //setup env
        virtual int setup(const Env::Param *param) { UNUSED(param); valid_ = true;  return 0; }
    };
  }
}

#endif
