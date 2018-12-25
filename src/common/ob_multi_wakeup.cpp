////===================================================================
 //
 // ob_multi_wakeup.cpp common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-03-04 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_tsi_factory.h"
#include "ob_multi_wakeup.h"

namespace oceanbase
{
  namespace common
  {
#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
    namespace MultiWakeupComponent
    {
      bool fd_equal(const int a, const int b)
      {
        bool bret = false;
        struct stat st_a;
        struct stat st_b;
        if (0 == fstat(a, &st_a)
            && 0 == fstat(b, &st_b))
        {
          bret = (st_a.st_ino == st_b.st_ino);
          if (!bret)
          {
            TBSYS_LOG(INFO, "fd not equal a=%d b=%d a.ino=%ld b.ino=%ld", a, b, st_a.st_ino, st_b.st_ino);
          }
        }
        return bret;
      }

      int FdPair::init(const int epoll_fd, const int64_t timestamp)
      {
        int ret = OB_SUCCESS;
        memset(&read_event_, -1, sizeof(read_event_));
        if (-1 == epoll_fd)
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else if (0 != socketpair(AF_UNIX, SOCK_STREAM, 0, fd_))
        //else if (0 != pipe(fd_))
        {
          TBSYS_LOG(WARN, "create pipe pair fail errno=%u", errno);
          ret = OB_ERROR;
        }
        else if (-1 == (epoll_fd_ = dup(epoll_fd)))
        {
          TBSYS_LOG(WARN, "dup epoll_fd fail errno=%u", errno);
          ret = OB_ERROR;
        }
        else
        {
          timestamp_ = timestamp;

          int flags = fcntl(fd_[READ_FD_POS], F_GETFL);
          flags |= O_NONBLOCK;
          fcntl(fd_[READ_FD_POS], F_SETFL, flags);
          flags = fcntl(fd_[WRITE_FD_POS], F_GETFL);
          flags |= O_NONBLOCK;
          fcntl(fd_[WRITE_FD_POS], F_SETFL, flags);

          read_event_.events = EPOLLIN;
          read_event_.data.fd = fd_[READ_FD_POS];
          if (0 != epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd_[READ_FD_POS], &read_event_))
          {
            TBSYS_LOG(WARN, "epoll_ctl add fail epoll_fd=%d read_fd=%d read_event=%p",
                      epoll_fd, fd_[READ_FD_POS], &read_event_);
            ret = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "fd_pair init succ epoll_fd=%d dup=%d read_fd=%d write_fd=%d",
                      epoll_fd, epoll_fd_, fd_[READ_FD_POS], fd_[WRITE_FD_POS]);
          }
        }
        if (OB_SUCCESS != ret)
        {
          destroy();
        }
        return ret;
      }

      void FdPair::destroy()
      {
        if (-1 != fd_[READ_FD_POS])
        {
          if (-1 != epoll_fd_)
          {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd_[READ_FD_POS], &read_event_);
          }
          close(fd_[READ_FD_POS]);
          fd_[READ_FD_POS] = -1;
        }
        if (-1 != fd_[WRITE_FD_POS])
        {
          close(fd_[WRITE_FD_POS]);
          fd_[WRITE_FD_POS] = -1;
        }
        if (-1 != epoll_fd_)
        {
          close(epoll_fd_);
          epoll_fd_ = -1;
        }
      }

      int FdPair::get_write_fd()
      {
        return fd_[WRITE_FD_POS];
      };

      ////////////////////////////////////////////////////////////////////////////////////////////////////

      ThreadLocalInfo::ThreadLocalInfo()
      {
        if (0 != fd_set_.create(FD_HASH_SIZE))
        {
          TBSYS_LOG(ERROR, "fd_set create fail");
        }
      }

      ThreadLocalInfo::~ThreadLocalInfo()
      {
        hash::ObHashMap<int, FdPair*>::iterator iter;
        for (iter = fd_set_.begin(); iter != fd_set_.end(); iter++)
        {
          iter->second->destroy();
        }
      }

      int ThreadLocalInfo::get_write_fd(const int epoll_fd, const int64_t timestamp)
      {
        int write_fd = -1;
        FdPair *fd_pair = NULL;

        int tmp_ret = fd_set_.get(epoll_fd, fd_pair);
        if (hash::HASH_EXIST == tmp_ret
            && NULL != fd_pair)
        {
          if (timestamp == fd_pair->timestamp_)
          {
            write_fd = fd_pair->get_write_fd();
          }
          else
          {
            TBSYS_LOG(INFO, "epoll_fd=%d reused, pair_timestamp=%ld arg_timestamp=%ld, will rebuild fd_pair",
                      epoll_fd, fd_pair->timestamp_, timestamp);
            fd_set_.erase(epoll_fd);
            fd_pair->destroy();
          }
        }

        if (-1 == write_fd)
        {
          fd_pair = allocator_.alloc(sizeof(FdPair));
          if (NULL == fd_pair)
          {
            TBSYS_LOG(ERROR, "alloc fd_pair fail epoll_fd=%d", epoll_fd);
          }
          else if (OB_SUCCESS != fd_pair->init(epoll_fd, timestamp))
          {
            TBSYS_LOG(ERROR, "fd_pair init fail epoll_fd=%d", epoll_fd);
          }
          else
          {
            tmp_ret = fd_set_.set(epoll_fd, fd_pair, 1);
            if (hash::HASH_INSERT_SUCC == tmp_ret
                || hash::HASH_OVERWRITE_SUCC == tmp_ret)
            {
              if (hash::HASH_OVERWRITE_SUCC == tmp_ret)
              {
                TBSYS_LOG(WARN, "overwrite fd_pair succ epoll_fd=%d", epoll_fd);
              }
              write_fd = fd_pair->get_write_fd();
            }
            else
            {
              fd_pair->destroy();
            }
          }
        }

        return write_fd;
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    using namespace MultiWakeupComponent;

    ObMultiWakeup::ObMultiWakeup() : cond_(false), epoll_fd_(-1)
    {
      timestamp_ = tbsys::CTimeUtil::getMonotonicTime();
    }

    ObMultiWakeup::~ObMultiWakeup()
    {
      destroy();
    }

    int ObMultiWakeup::init()
    {
      int ret = OB_SUCCESS;
      if (-1 != epoll_fd_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (-1 == (epoll_fd_ = epoll_create(EPOLL_SIZE)))
      {
        TBSYS_LOG(WARN, "epoll_create errno=%u", errno);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "epoll_create fd=%d", epoll_fd_);
      }
      return ret;
    }

    void ObMultiWakeup::destroy()
    {
      if (-1 != epoll_fd_)
      {
        close(epoll_fd_);
        epoll_fd_ = -1;
      }
    }

    int ObMultiWakeup::signal()
    {
      int ret = OB_SUCCESS;
      MultiWakeupComponent::ThreadLocalInfo *tli = NULL;
      int write_fd = -1;
      if (-1 == epoll_fd_)
      {
        ret = OB_NOT_INIT;
      }
      else if (ATOMIC_CAS(&cond_, false, true))
      {
        // cond is true, return directly
      }
      else if (NULL == (tli = GET_TSI_MULT(MultiWakeupComponent::ThreadLocalInfo, 1)))
      {
        ret = OB_ERROR;
      }
      else if (-1 == (write_fd = tli->get_write_fd(epoll_fd_, timestamp_)))
      {
        ret = OB_ERROR;
      }
      else
      {
        char write_buffer[PIPE_BUF_WRITE_SIZE];
        write(write_fd, write_buffer, PIPE_BUF_WRITE_SIZE);
        //TBSYS_LOG(DEBUG, "write ret=%d errno=%u", write_ret, errno);
      }
      return ret;
    }

    int ObMultiWakeup::timedwait(const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      if (-1 == epoll_fd_)
      {
        ret = OB_NOT_INIT;
      }
      else if (ATOMIC_CAS(&cond_, true, false))
      {
        // cond is true, return directly
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        struct epoll_event events[EPOLL_SIZE];
        int event_num = 0;
        int64_t epoll_timeout = timeout_us;
        char read_buffer[PIPE_BUF_READ_SIZE];
        while (true)
        {
          epoll_timeout = timeout_us - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 > epoll_timeout)
          {
            ret = OB_RESPONSE_TIME_OUT;
            break;
          }
          event_num = epoll_wait(epoll_fd_, events, EPOLL_SIZE, (int32_t)epoll_timeout / 1000);
          if (0 < event_num)
          {
            for (int i = 0; i < event_num; i++)
            {
              read(events[i].data.fd, read_buffer, PIPE_BUF_READ_SIZE);
              //TBSYS_LOG(DEBUG, "read ret=%d errno=%u", read_ret, errno);
            }
            break;
          }
        }
      }
      return ret;
    }
  }
}

