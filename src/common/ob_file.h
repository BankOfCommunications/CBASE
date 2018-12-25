////===================================================================
 //
 // ob_file_utils.h / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-05-25 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_FILE_H_
#define  OCEANBASE_COMMON_FILE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <malloc.h>
#include <libaio.h>
#include "ob_define.h"
#include "ob_malloc.h"
#include "ob_atomic.h"
#include "ob_thread_objpool.h"
#include "ob_trace_log.h"
#include "ob_fileinfo_manager.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    class IFileBuffer
    {
      public:
        IFileBuffer() {};
        virtual ~IFileBuffer() {};
      public:
        virtual char *get_buffer() = 0;
        virtual int64_t get_base_pos() = 0;
        virtual void set_base_pos(const int64_t pos) = 0;
        virtual int assign(const int64_t size, const int64_t align) = 0;
        virtual int assign(const int64_t size) = 0;
    };

    class IFileAsyncCallback
    {
      public:
        IFileAsyncCallback() {};
        virtual ~IFileAsyncCallback() {};
      public:
        virtual void callback(const int io_ret, const int io_errno) = 0;
    };

    namespace FileComponent
    {
      class IFileReader
      {
        public:
          IFileReader() : fd_(-1) {};
          virtual ~IFileReader() {};
        public:
          virtual int open(const ObString &fname);
          virtual void close();
          virtual bool is_opened() const;
        public:
          virtual int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size) = 0;
          virtual int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size) = 0;
        public:
          virtual int get_open_flags() const = 0;
          virtual int get_open_mode() const = 0;
        public:
          virtual int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size) = 0;
          virtual int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size) = 0;
        protected:
          int fd_;
      };

      class IFileAppender
      {
        public:
          IFileAppender() : fd_(-1) {};
          virtual ~IFileAppender() {};
        public:
          virtual int open(const ObString &fname, const bool is_create, const bool is_trunc);
          virtual int create(const ObString &fname);
          virtual void close();
          virtual bool is_opened() const;
          virtual int get_fd() const;
        public:
          virtual int append(const void *buf, const int64_t count, const bool is_fsync) = 0;
          virtual int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback) = 0;
          virtual int fsync() = 0;
        public:
          virtual int64_t get_file_pos() const = 0;
          virtual int get_open_flags() const = 0;
          virtual int get_open_mode() const = 0;
        private:
          virtual int prepare_buffer_() = 0;
          virtual void set_normal_flags_() = 0;
          virtual void add_truncate_flags_() = 0;
          virtual void add_create_flags_() = 0;
          virtual void add_excl_flags_() = 0;
          virtual void set_file_pos_(const int64_t file_pos) = 0;
        protected:
          int fd_;
      };

      class BufferFileReader : public IFileReader
      {
        static const int OPEN_FLAGS = O_RDONLY;
        static const int OPEN_MODE = S_IRWXU;
        public:
          BufferFileReader();
          ~BufferFileReader();
        public:
          int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
          int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
          int get_open_flags() const;
          int get_open_mode() const;
        public:
          int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
          int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
      };

      class DirectFileReader : public IFileReader
      {
        static const int OPEN_FLAGS = O_RDONLY | O_DIRECT;
        static const int OPEN_MODE = S_IRWXU;
        public:
          static const int64_t DEFAULT_ALIGN_SIZE = 4L * 1024L;
          static const int64_t DEFAULT_BUFFER_SIZE = 1L * 1024L * 1024L;
        public:
          DirectFileReader(const int64_t buffer_size = DEFAULT_BUFFER_SIZE, const int64_t align_size = DEFAULT_ALIGN_SIZE);
          ~DirectFileReader();
        public:
          int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
          int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
          int get_open_flags() const;
          int get_open_mode() const;
        public:
          int pread_by_fd(const int fd, void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
          int pread_by_fd(const int fd, const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
        private:
          const int64_t align_size_;
          const int64_t buffer_size_;
          char *buffer_;
      };

      class BufferFileAppender : public IFileAppender
      {
        static const int NORMAL_FLAGS = O_WRONLY;
        static const int CREAT_FLAGS = O_CREAT;
        static const int TRUNC_FLAGS = O_TRUNC;
        static const int EXCL_FLAGS = O_EXCL;
        static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        public:
          static const int64_t DEFAULT_BUFFER_SIZE = 2L * 1024L * 1024L;
        public:
          BufferFileAppender(const int64_t buffer_size = DEFAULT_BUFFER_SIZE);
          ~BufferFileAppender();
        public:
          void close();
          int append(const void *buf, const int64_t count, const bool is_fsync);
          int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);
          int fsync();
          int get_open_flags() const;
          int get_open_mode() const;
          int64_t get_file_pos() const {return file_pos_;}
        private:
          int buffer_sync_();
        private:
          int prepare_buffer_();
          void set_normal_flags_();
          void add_truncate_flags_();
          void add_create_flags_();
          void add_excl_flags_();
          void set_file_pos_(const int64_t file_pos);
        private:
          int open_flags_;
          const int64_t buffer_size_;
          int64_t buffer_pos_;
          int64_t file_pos_;
          char *buffer_;
      };

      class DirectFileAppender : public IFileAppender
      {
        static const int64_t DEBUG_MAGIC = 0x1a2b3c4d;
        static const int NORMAL_FLAGS = O_RDWR | O_DIRECT;
        static const int CREAT_FLAGS = O_CREAT;
        static const int TRUNC_FLAGS = O_TRUNC;
        static const int EXCL_FLAGS = O_EXCL;
        static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        public:
          static const int64_t DEFAULT_ALIGN_SIZE = 4L * 1024L;
          static const int64_t DEFAULT_BUFFER_SIZE = 2L * 1024L * 1024L;
        public:
          DirectFileAppender(const int64_t buffer_size = DEFAULT_BUFFER_SIZE, const int64_t align_size = DEFAULT_ALIGN_SIZE);
          ~DirectFileAppender();
        public:
          void close();
          int append(const void *buf, const int64_t count, const bool is_fsync);
          int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);
          int fsync();
          int get_open_flags() const;
          int get_open_mode() const;
          int64_t get_file_pos() const {return file_pos_;}
          int set_align_size(const int64_t align_size);
        private:
          int buffer_sync_(bool *need_truncate = NULL);
        private:
          int prepare_buffer_();
          void set_normal_flags_();
          void add_truncate_flags_();
          void add_create_flags_();
          void add_excl_flags_();
          void set_file_pos_(const int64_t file_pos);
        private:
          int open_flags_;
          int64_t align_size_;
          const int64_t buffer_size_;
          int64_t buffer_pos_;
          int64_t file_pos_;
          char *buffer_;
          int64_t buffer_length_;
          int64_t align_warn_;
      };
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    // 如果目标不存在则改名 否在返回失败
    extern int atomic_rename(const char *oldpath, const char *newpath);

    // 不被信号打断的pwrite
    extern int64_t unintr_pwrite(const int fd, const void *buf, const int64_t count, const int64_t offset);

    // 不被信号打断的pread
    extern int64_t unintr_pread(const int fd, void *buf, const int64_t count, const int64_t offset);

    // 判断buf count offest是否对齐
    extern bool is_align(const void *buf, const int64_t count, const int64_t offset, const int64_t align_size);

    // 使用一个对齐的辅助buffer 进行dio读取
    extern int64_t pread_align(const int fd, void *buf, const int64_t count, const int64_t offset,
                              void *align_buffer, const int64_t buffer_size, const int64_t align_size);

    // 使用一个对齐的辅助buffer 进行dio写入 buffer可能存在为了对齐而保留的数据 因此需要传入和传出buffer_pos
    extern int64_t pwrite_align(const int fd, const void *buf, const int64_t count, const int64_t offset,
                              void *align_buffer, const int64_t buffer_size, const int64_t align_size, int64_t &buffer_pos);

    // 获取一个文件句柄指向文件的size
    extern int64_t get_file_size(const int fd);
    extern int64_t get_file_size(const char *fname);

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    class ObFileBuffer : public IFileBuffer
    {
      static const int64_t MIN_BUFFER_SIZE = 1L * 1024L * 1024L;
      public:
        ObFileBuffer();
        ~ObFileBuffer();
      public:
        char *get_buffer();
        int64_t get_base_pos();
        void set_base_pos(const int64_t pos);
        int assign(const int64_t size, const int64_t align);
        int assign(const int64_t size);
      public:
        void release();
      protected:
        char *buffer_;
        int64_t base_pos_;
        int64_t buffer_size_;
    };

    // file reader 线程不安全 只允许同时一个线程调用
    class ObFileReader
    {
      public:
        ObFileReader();
        ~ObFileReader();
      public:
        int open(const ObString &fname, const bool dio,
                const int64_t align_size = FileComponent::DirectFileReader::DEFAULT_ALIGN_SIZE);
        void close();
        bool is_opened() const;
      public:
        int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
        int pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf, int64_t &read_size);
      public:
        static int read_record(common::IFileInfoMgr& fileinfo_mgr,
                              const uint64_t file_id,
                              const int64_t offset,
                              const int64_t size,
                              IFileBuffer &file_buf);
        static int read_record(const common::IFileInfo& file_info,
                              const int64_t offset,
                              const int64_t size,
                              IFileBuffer &file_buf);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObFileReader);
      private:
        FileComponent::IFileReader *file_;
    };

    class ObIFileAppender
    {
      public:
        virtual ~ObIFileAppender() {};
      public:
        virtual int open(const ObString &fname, const bool dio, const bool is_create, const bool is_trunc = false,
                        const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE) = 0;
        virtual int create(const ObString &fname, const bool dio,
                          const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE) = 0;
        virtual void close() = 0;
        virtual int64_t get_file_pos() const = 0;
        virtual int append(const void *buf, const int64_t count, const bool is_fsync) = 0;
        virtual int fsync() = 0;
    };

    // file appender 线程不安全 只允许同时一个线程调用
    // 关闭普通io的实现 暂时只支持dio
    class ObFileAppender : public ObIFileAppender
    {
      public:
        ObFileAppender();
        ~ObFileAppender();
      public:
        // 子类可以重载open close get_file_pos
        int open(const ObString &fname, const bool dio, const bool is_create, const bool is_trunc = false,
                const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE);
        void close();
        int64_t get_file_pos() const {return NULL == file_ ? -1 : file_->get_file_pos();}
        virtual int get_fd() const { return file_->get_fd();}
      public:
        int create(const ObString &fname, const bool dio,
                  const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE);
        bool is_opened() const;
      public:
        // 子类可以重载append fsync
        // 如果没有设置is_fsync 那么默认使用内部buffer缓存数据 直到buffer满后刷磁盘
        int append(const void *buf, const int64_t count, const bool is_fsync);
        int fsync();
      public:
        // 暂时不实现
        int async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObFileAppender);
      private:
        FileComponent::IFileAppender *file_;
        FileComponent::DirectFileAppender direct_file_;
        FileComponent::BufferFileAppender buffer_file_;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

#define __USE_AIO_FILE
#ifdef __USE_AIO_FILE
    class ObIWaiter
    {
      public:
        virtual ~ObIWaiter() {};
        virtual void wait() = 0;
    };

    template <class T,
              int64_t SIZE>
    class ObWaitablePool
    {
      struct Node
      {
        T data;
        Node *next;
      };
      public:
        ObWaitablePool();
        ~ObWaitablePool();
      public:
        int alloc_obj(T *&obj, ObIWaiter &waiter);
        void free_obj(T *obj);
        int64_t used() const {return used_;};
      private:
        Node *objs_;
        Node *list_;
        int64_t used_;
    };

    template <class T, int64_t SIZE>
    ObWaitablePool<T, SIZE>::ObWaitablePool() : list_(NULL),
                                                used_(0)
    {
      if (NULL == (objs_ = (Node*)ob_malloc(sizeof(Node) * SIZE, ObModIds::OB_WAITABLE_POOL)))
      {
        TBSYS_LOG(ERROR, "alloc obj array fail");
      }
      else
      {
        for (int64_t i = 0; i < SIZE; i++)
        {
          new(&objs_[i].data) T();
          objs_[i].next = list_;
          list_ = &objs_[i];
        }
      }
    }

    template <class T, int64_t SIZE>
    ObWaitablePool<T, SIZE>::~ObWaitablePool()
    {
      Node *iter = list_;
      int64_t counter = 0;
      while (NULL != iter)
      {
        Node *next = iter->next;
        iter->data.~T();
        iter = next;
        counter++;
      }
      if (NULL != objs_)
      {
        if (SIZE != counter)
        {
          TBSYS_LOG(ERROR, "still have %ld node not been free, memory=%p will leek", SIZE - counter, objs_);
        }
        else
        {
          ob_free(objs_);
        }
      }
    }

    template <class T, int64_t SIZE>
    int ObWaitablePool<T, SIZE>::alloc_obj(T *&obj, ObIWaiter &waiter)
    {
      int ret = OB_SUCCESS;
      if (NULL == objs_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        if (NULL == list_)
        {
          waiter.wait();
        }
        if (NULL == list_)
        {
          ret = OB_PROCESS_TIMEOUT;
        }
        else
        {
          obj = &list_->data;
          list_ = list_->next;
          used_ += 1;
        }
      }
      return ret;
    }

    template <class T, int64_t SIZE>
    void ObWaitablePool<T, SIZE>::free_obj(T *obj)
    {
      if (NULL != objs_
          && NULL != obj)
      {
        Node *node = (Node*)(obj);
        node->next = list_;
        list_ = node;
        used_ -= 1;
      }
    }

    class ObFileAsyncAppender : public ObIFileAppender, ObIWaiter
    {
      struct AIOCB
      {
        AIOCB() : buffer_pos(0),
                  buffer(NULL)
        {
          memset(&cb, 0, sizeof(cb));
        };
        ~AIOCB()
        {
          if (NULL != buffer)
          {
            free(buffer);
          }
        };
        struct iocb cb;
        int64_t buffer_pos;
        char *buffer;
      };
      struct OpenParam
      {
        OpenParam(const bool dio,
            const bool is_create,
            const bool is_trunc,
            const bool is_excl) : flag(O_RDWR),
                                  mode(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
        {
          flag |= dio       ? O_DIRECT  : 0;
          flag |= is_create ? O_CREAT   : 0;
          flag |= is_trunc  ? O_TRUNC   : 0;
          flag |= is_excl   ? O_EXCL    : 0;
        };
        int get_open_flags() const
        {
          return flag;
        };
        int get_open_mode() const
        {
          return mode;
        };
        int flag;
        int mode;
      };
      static const int64_t AIO_BUFFER_SIZE = 16L<<20;
      static const int AIO_MAXEVENTS = 8;
      static const int64_t AIO_WAIT_TIME_US = 1000000;
      typedef ObWaitablePool<AIOCB, AIO_MAXEVENTS> Pool;
      public:
        ObFileAsyncAppender();
        ~ObFileAsyncAppender();
      public:
        int open(const ObString &fname,
                 const bool dio,
                 const bool is_create,
                 const bool is_trunc = false,
                 const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE);
        int create(const ObString &fname,
                 const bool dio,
                 const int64_t align_size = FileComponent::DirectFileAppender::DEFAULT_ALIGN_SIZE);
        void close();
        int64_t get_file_pos() const;
        int append(const void *buf,
                 const int64_t count,
                 const bool is_fsync);
        int fsync();
      public:
        void wait();
      private:
        int submit_iocb_(AIOCB *iocb);
        AIOCB *get_iocb_();
      private:
        Pool pool_;
        int fd_;
        int64_t file_pos_;
        int64_t align_size_;
        AIOCB *cur_iocb_;
        io_context_t ctx_;
    };
#endif // __USE_AIO_FILE
  }
}

#endif //OCEANBASE_COMMON_FILE_H_

