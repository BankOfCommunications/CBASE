#include "posix_env.h"
#include "common/utility.h"

namespace oceanbase {
  namespace tools {
    using namespace common;

    PosixWritableFile::~PosixWritableFile()
    {
      close();
    }

    int PosixWritableFile::append(const common::ObDataBuffer& data)
    {
      return file_.write(data.get_data(), data.get_position());
    }

    int PosixWritableFile::close()
    {
      file_.close();
      return 0;
    }

    int PosixWritableFile::flush()
    {
      return 0;
    }

    int PosixWritableFile::sync()
    {
      return 0;
    }


    WritableFile *PosixEnv::console()
    {
      WritableFile *file = NULL;
      int new_stdout = dup(STDOUT_FILENO);

      if (new_stdout != -1) 
      {
        file = new(std::nothrow) PosixWritableFile(new_stdout);
        if (file == NULL) 
        {
          TBSYS_LOG(ERROR, "can't allocate posix writable file");
        }
      }

      return file;
    }

    int PosixEnv::rmfile(const char *path)
    {
      return ::remove(path);
    }

    int PosixEnv::rename(const char *src, const char *target)
    {
      return ::rename(src, target);
    }

    int PosixEnv::new_writable_file(const std::string &file_name, WritableFile *&file)
    {
      int ret = 0;
      int fd = ::open(file_name.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_APPEND, 0644);

      if (fd < 0) 
      {
        TBSYS_LOG(ERROR, "can't crate output file %s", file_name.c_str());
        ret = -1;
      } 
      else 
      {
        file = new(std::nothrow) PosixWritableFile(fd);
        if (file == NULL) {
          TBSYS_LOG(ERROR, "can't allocate PosixWritableFile");
          ret = -1;
        }
      }

      return ret;
    }

    static PosixEnv *__posix_env = NULL;

    Env *Env::Posix() 
    {
      if (__posix_env == NULL) 
      {
        __posix_env = new PosixEnv;
      }

      return __posix_env;
    }
  }
}
