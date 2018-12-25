////===================================================================
 //
 // ob_multi_wakeup.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-03-04 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 多对一的唤醒器
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_MULTI_WAKEUP_H_
#define  OCEANBASE_COMMON_MULTI_WAKEUP_H_
#include <sys/epoll.h>
#include "common/hash/ob_hashmap.h"
#include "common/page_arena.h"

namespace oceanbase
{
  namespace common
  {
    namespace MultiWakeupComponent
    {
      static const int64_t PIPE_BUF_WRITE_SIZE = 1;
      static const int64_t PIPE_BUF_READ_SIZE = 1;

      bool fd_equal(const int a, const int b);

      struct FdPair
      {
        static const int64_t READ_FD_POS = 0;
        static const int64_t WRITE_FD_POS = 1;

        int fd_[2];
        int64_t timestamp_;
        int epoll_fd_;
        struct epoll_event read_event_;

        int get_write_fd();
        int init(const int epoll_fd, const int64_t timestamp);
        void destroy();
      };

      class ThreadLocalInfo
      {
        static const int64_t FD_HASH_SIZE = 1024;
        public:
          ThreadLocalInfo();
          ~ThreadLocalInfo();
        public:
          int get_write_fd(const int epoll_fd, const int64_t timestamp);
        private:
          hash::ObHashMap<int, FdPair*> fd_set_;
          PageArena<FdPair> allocator_;
      };
    }

    class ObMultiWakeup
    {
      static const int EPOLL_SIZE = 1024;
      public:
        ObMultiWakeup();
        ~ObMultiWakeup();
      public:
        int init();
        void destroy();
      public:
        int signal();
        int timedwait(const int64_t timeout_us);
      private:
        volatile bool cond_;
        int epoll_fd_;
        int64_t timestamp_;
    };
  }
}

#endif //OCEANBASE_COMMON_MULTI_WAKEUP_H_

