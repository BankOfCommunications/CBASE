#ifndef __TASK_ENV_H__
#define  __TASK_ENV_H__

#include <string>
#include "common/data_buffer.h"

namespace oceanbase {
  namespace tools {
    class WritableFile {
      public:
        WritableFile() { }
        virtual ~WritableFile() { }

        virtual int append(const common::ObDataBuffer& data) = 0;
        virtual int close() = 0;
        virtual int flush() = 0;
        virtual int sync() = 0;

      private:
        // No copying allowed
        WritableFile(const WritableFile&);
        void operator=(const WritableFile&);
    };

    class Env {
      public:
        Env() :valid_(false) { }

        virtual ~Env() { }

        struct Param {};

        static Env *Posix();

        static Env *Hadoop();

        //rm file
        virtual int rmfile(const char *path) = 0;

        //rename file src to target
        virtual int rename(const char *src, const char *target) = 0;

        //create a new writable file
        virtual int new_writable_file(const std::string &file_name, WritableFile *&file) = 0;

        //create a console output
        virtual WritableFile *console() { return NULL; }

        //setup env
        virtual int setup(const Env::Param *param) = 0;

        bool valid() const { return valid_; }
      protected:
        bool valid_;
    };
  }
}

#endif
