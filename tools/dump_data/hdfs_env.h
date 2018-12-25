#ifndef __HDFS_PORT_H__
#define  __HDFS_PORT_H__

#include "task_env.h"
#include "hdfs.h"

namespace oceanbase {
  namespace tools {
    struct HdfsParam {
      HdfsParam() 
        : host(NULL), port(0), user(NULL), 
        group_size(0), io_buffer_size(0),
        group(NULL),blocksize(0), replication(0) { }

      const char *host;
      unsigned short port;
      const char *user;
      int group_size;
      int64_t io_buffer_size;
      const char **group;
      int64_t blocksize;
      int64_t replication;
    };

    class HdfsEnv : public Env {
      public:
        HdfsEnv() { fs_ = NULL; fs_counter_ = 0; }
        ~HdfsEnv();

        //rename src to target
        int rename(const char *src, const char *target);

        int rmfile(const char *path);

        //creat new WritableFile
        virtual int new_writable_file(const std::string &file_name, WritableFile *&file);

        //setup env
        virtual int setup(const Env::Param *param);

        void unref() { __sync_fetch_and_sub(&fs_counter_, 1); }

        void ref() { __sync_fetch_and_add (&fs_counter_, 1); }

        hdfsFS fs() const { return fs_; }

      private:
        hdfsFS fs_;

        int fs_counter_;

        int64_t blocksize_;
        int64_t buffersize_;
        int64_t replica_;
    };

    class HdfsWritableFile : public WritableFile {
      public:
        HdfsWritableFile(HdfsEnv *env, hdfsFile fd) : env_(env), fd_(fd) { env_->ref(); }
        ~HdfsWritableFile();

        virtual int append(const common::ObDataBuffer &data);
        virtual int close();
        virtual int flush();
        virtual int sync(); 

      private:
        HdfsEnv *env_;
        hdfsFile fd_;
    };
  }
}

#endif
