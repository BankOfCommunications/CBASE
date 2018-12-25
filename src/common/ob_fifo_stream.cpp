////===================================================================
 //
 // ob_fifo_stream.cpp common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include "ob_fifo_stream.h"

namespace oceanbase
{
  namespace common
  {
    ObFIFOStream::ObFIFOStream() : inited_(false),
                                   fifo_fd_(-1),
                                   write_buffer_(NULL),
                                   expired_time_(INT64_MAX),
                                   alloc_counter_(0),
                                   free_counter_(0)
    {
    }

    ObFIFOStream::~ObFIFOStream()
    {
      destroy();
    }

    int ObFIFOStream::init(const char *fifo_path, const int64_t max_buffer_size, const int64_t expired_time)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == fifo_path
              || 0 >= max_buffer_size
              || 0 >= expired_time)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (-1 == (fifo_fd_ = mkfifo_(fifo_path)))
      {
        TBSYS_LOG(WARN, "open fifo file fail fifo_path=%s errno=%u", fifo_path, errno);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = allocator_.init(max_buffer_size, max_buffer_size, MEMPOOL_PAGE_SIZE)))
      {
        TBSYS_LOG(WARN, "mempool create fail");
      }
      else if (NULL == (write_buffer_ = (char*)alloc_(WRITE_BUFFER_SIZE)))
      {
        TBSYS_LOG(WARN, "alloc write_buffer fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = wakeup_.init()))
      {
        TBSYS_LOG(WARN, "wakeup init fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = queue_.init(max_buffer_size / AVG_PACKET_SIZE + 1)))
      {
        TBSYS_LOG(WARN, "queue init fail ret=%d max_num=%ld", ret, max_buffer_size / AVG_PACKET_SIZE + 1);
      }
      else
      {
        //int flags = fcntl(fifo_fd_, F_GETFL);
        //flags &= ~O_NONBLOCK;
        //fcntl(fifo_fd_, F_SETFL, flags);
        expired_time_ = expired_time;
        start();
        inited_ = true;
        TBSYS_LOG(INFO, "init succ fifo_path=%s max_buffer_size=%ld fifo_fd=%d", fifo_path, max_buffer_size, fifo_fd_);
      }
      if (OB_SUCCESS != ret)
      {
        queue_.destroy();
        wakeup_.destroy();
        if (NULL != write_buffer_)
        {
          free_(write_buffer_);
          write_buffer_ = NULL;
        }
        allocator_.destroy();
        if (-1 != fifo_fd_)
        {
          close(fifo_fd_);
          fifo_fd_ = -1;
        }
      }
      return ret;
    }

    void ObFIFOStream::destroy()
    {
      if (inited_)
      {
        wakeup_.signal();
        stop();
        wait();
        TBSYS_LOG(INFO, "alloc_counter_=%ld free_counter_=%ld queue_total=%ld queue_free=%ld mempool_total=%ld",
                  alloc_counter_, free_counter_, queue_.get_total(), queue_.get_free(), allocator_.allocated());

        queue_.destroy();
        wakeup_.destroy();
        if (NULL != write_buffer_)
        {
          free_(write_buffer_);
          write_buffer_ = NULL;
        }
        allocator_.destroy();
        if (-1 != fifo_fd_)
        {
          close(fifo_fd_);
          fifo_fd_ = -1;
        }

        inited_ = false;
      }
    }

    int ObFIFOStream::push(const ObPacket *packet)
    {
      int ret = OB_SUCCESS;
      FIFOPacket *pkt = NULL;
      Request *req = NULL;
      const ObDataBuffer *data_buffer = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == packet)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (data_buffer = packet->get_packet_buffer())
              || NULL == data_buffer->get_data()
              || data_buffer->get_capacity() < (packet->get_data_length() + packet->get_header_size()))
      {
        TBSYS_LOG(WARN, "invalid packet buffer capacity=%ld data_length=%d header_size=%ld",
                  data_buffer ? data_buffer->get_capacity() : -1,
                  packet->get_data_length(), packet->get_header_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t buffer_size = packet->get_data_length();
        int64_t request_size = sizeof(Request) + buffer_size;
        int64_t total_size = sizeof(FIFOPacket) + request_size;
        while (true)
        {
          if (NULL == (pkt = (FIFOPacket*)alloc_(total_size)))
          {
            release_old_();
            continue;
          }
          else
          {
            pkt->magic = FIFO_PACKET_MAGIC;
            pkt->version = CUR_FIFO_PACKET_VERSION;
            pkt->timestamp = tbsys::CTimeUtil::getTime();
            pkt->type = FIFO_PKT_REQUEST;
            pkt->buf_size = (int32_t)request_size;
            req = (Request*)(pkt->buf);
            req->magic = REQUEST_MAGIC;
            req->version = CUR_REQUEST_VERSION;
            req->pcode = packet->get_packet_code();
            //req->peer_id = (int32_t)(packet->get_connection() ? packet->get_connection()->getPeerId() : 0);
            req->api_version = packet->get_api_version();
            req->source_id = 0;//packet->get_source_id();
            req->target_id = packet->get_target_id();
            req->timeout = (int32_t)packet->get_source_timeout();
            req->receive_ts = packet->get_receive_ts();
            req->priority = packet->get_packet_priority();
            req->buf_size = (int32_t)buffer_size;
            if (0 < buffer_size)
            {
              memcpy(req->buf, data_buffer->get_data() + packet->get_header_size(), buffer_size);
              TBSYS_LOG(DEBUG, "packet buffer capacity=%ld data_length=%d header_size=%ld",
                        data_buffer ? data_buffer->get_capacity() : -1,
                        packet->get_data_length(), packet->get_header_size());
            }
            while (true)
            {
              ret = queue_.push(pkt);
              if (OB_SUCCESS == ret)
              {
                wakeup_.signal();
                break;
              }
              else if (OB_SIZE_OVERFLOW == ret)
              {
                release_old_();
                continue;
              }
              else
              {
                break;
              }
            }
            break;
          }
        }
        if (OB_SUCCESS != ret
            && NULL != pkt)
        {
          free_(pkt);
          pkt = NULL;
        }
      }
      return ret;
    }

    int ObFIFOStream::push(const char *buffer, const int32_t size)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == buffer
              || 0 >= size)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        FIFOPacket *pkt = NULL;
        while (true)
        {
          if (NULL == (pkt = (FIFOPacket*)alloc_(size + sizeof(FIFOPacket))))
          {
            release_old_();
            continue;
          }
          else
          {
            pkt->magic = FIFO_PACKET_MAGIC;
            pkt->version = CUR_FIFO_PACKET_VERSION;
            pkt->timestamp = tbsys::CTimeUtil::getTime();
            pkt->type = FIFO_PKT_BINARY;
            pkt->buf_size = size;
            memcpy(pkt->buf, buffer, size);
            while (true)
            {
              ret = queue_.push(pkt);
              if (OB_SUCCESS == ret)
              {
                wakeup_.signal();
                break;
              }
              else if (OB_SIZE_OVERFLOW == ret)
              {
                release_old_();
                continue;
              }
              else
              {
                break;
              }
            }
            break;
          }
        }
        if (OB_SUCCESS != ret
            && NULL != pkt)
        {
          free_(pkt);
          pkt = NULL;
        }
      }
      return ret;
    }

    int ObFIFOStream::push(const struct iovec *iov, int iovcnt)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == iov
              || 0 >= iovcnt)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        FIFOPacket *pkt = NULL;
        int32_t total_size = 0;
        for (int i = 0; i < iovcnt; ++i)
        {
          total_size += static_cast<int32_t>(iov[i].iov_len);
        }
        while (true)
        {
          if (NULL == (pkt = (FIFOPacket*)alloc_(total_size + sizeof(FIFOPacket))))
          {
            release_old_();
            continue;
          }
          else
          {
            pkt->magic = FIFO_PACKET_MAGIC;
            pkt->version = CUR_FIFO_PACKET_VERSION;
            pkt->timestamp = tbsys::CTimeUtil::getTime();
            pkt->type = FIFO_PKT_BINARY;
            pkt->buf_size = total_size;
            int64_t curr_buf_pos = 0;
            for (int i = 0; i < iovcnt; ++i)
            {
              memcpy(pkt->buf+curr_buf_pos, iov[i].iov_base, iov[i].iov_len);
              curr_buf_pos += iov[i].iov_len;
            }
            while (true)
            {
              ret = queue_.push(pkt);
              if (OB_SUCCESS == ret)
              {
                wakeup_.signal();
                break;
              }
              else if (OB_SIZE_OVERFLOW == ret)
              {
                release_old_();
                continue;
              }
              else
              {
                break;
              }
            }
            break;
          }
        }
        if (OB_SUCCESS != ret
            && NULL != pkt)
        {
          free_(pkt);
          pkt = NULL;
        }
      }
      return ret;
    }

    void ObFIFOStream::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      FIFOPacket *pkt = NULL;
      int64_t buffer_pos = 0;
      while (!_stop)
      {
        if (OB_SUCCESS != queue_.pop(pkt)
            || NULL == pkt)
        {
          pkt = NULL;
          if (0 == buffer_pos)
          {
            wakeup_.timedwait(SLEEP_TIME_US);
          }
          else
          {
            write_(fifo_fd_, write_buffer_, buffer_pos, SLEEP_TIME_US, _stop);
            buffer_pos = 0;
          }
        }
        else if ((tbsys::CTimeUtil::getTime() - pkt->timestamp) > expired_time_)
        {
          // skip
        }
        else if (WRITE_BUFFER_SIZE >= (get_packet_size_(pkt) + buffer_pos))
        {
          memcpy(write_buffer_ + buffer_pos, pkt, get_packet_size_(pkt));
          buffer_pos += get_packet_size_(pkt);
        }
        else
        {
          write_(fifo_fd_, write_buffer_, buffer_pos, SLEEP_TIME_US, _stop);
          buffer_pos = 0;
          if (WRITE_BUFFER_SIZE < get_packet_size_(pkt))
          {
            write_(fifo_fd_, (char*)pkt, get_packet_size_(pkt), SLEEP_TIME_US, _stop);
          }
          else
          {
            memcpy(write_buffer_ + buffer_pos, pkt, get_packet_size_(pkt));
            buffer_pos += get_packet_size_(pkt);
          }
        }
        if (NULL != pkt)
        {
          free_(pkt);
          pkt = NULL;
        }
      }
      while (OB_SUCCESS == queue_.pop(pkt))
      {
        if (NULL != pkt)
        {
          free_(pkt);
          pkt = NULL;
        }
      }
    }

    int64_t ObFIFOStream::get_queue_size() const
    {
      return queue_.get_total();
    }

    void *ObFIFOStream::alloc_(const int64_t size)
    {
      void *ret = allocator_.alloc(size);
      if (NULL != ret)
      {
        ATOMIC_INC(&alloc_counter_);
      }
      return ret;
    }

    void ObFIFOStream::free_(void *ptr)
    {
      ATOMIC_INC(&free_counter_);
      allocator_.free(ptr);
    }

    void ObFIFOStream::release_old_()
    {
      int64_t release_size = 0;
      FIFOPacket *pkt = NULL;
      while (release_size < MEMPOOL_PAGE_SIZE)
      {
        if (OB_SUCCESS == queue_.pop(pkt))
        {
          if (NULL != pkt)
          {
            release_size += get_packet_size_(pkt);
            free_(pkt);
            pkt = NULL;
          }
        }
        else
        {
          break;
        }
      }
    }

    int64_t ObFIFOStream::get_packet_size_(const FIFOPacket *pkt)
    {
      return pkt->buf_size + sizeof(FIFOPacket);
    }

    int64_t ObFIFOStream::write_(const int fd, const char *buffer, const int64_t buffer_size,
                                int64_t useconds, const bool &stop)
    {
      int64_t write_ret = 0;
      int64_t wrote_size = 0;
      while (!stop
          && wrote_size < buffer_size)
      {
        if (0 == swriteable_us_(fd, useconds))
        {
          continue;
        }
        write_ret = write(fd, buffer + wrote_size, buffer_size - wrote_size);
        if (0 > write_ret)
        {
          if (EINTR == errno)
          {
            continue;
          }
          else
          {
            break;
          }
        }
        else
        {
          wrote_size += write_ret;
        }
      }
      return wrote_size;
    }

    int ObFIFOStream::swriteable_us_(const int fd, const int64_t useconds)
    {
      fd_set fs;
      struct timeval tv;
      tv.tv_sec = useconds / 1000000;
      tv.tv_usec = (useconds % 1000000) * 1000;
      FD_ZERO(&fs);
      FD_SET(fd, &fs);
      return (select(fd + 1, NULL, &fs, NULL, &tv));
    }

    int ObFIFOStream::mkfifo_(const char *fifo_path)
    {
      int fifo_fd = -1;
      unlink(fifo_path);
      if (0 != mkfifo(fifo_path, S_IRWXU | S_IRGRP | S_IROTH))
      {
        TBSYS_LOG(WARN, "mkfifo fail fifo_path=%s errno=%u", fifo_path, errno);
      }
      else if (-1 == (fifo_fd = open(fifo_path, O_RDWR | O_NONBLOCK)))
      {
        unlink(fifo_path);
        TBSYS_LOG(WARN, "open fifo file fail fifo_path=%s errno=%u", fifo_path, errno);
      }
      else
      {
        chmod(fifo_path, S_IRUSR | S_IRGRP | S_IROTH);
        TBSYS_LOG(INFO, "mkfifo succ fifo_fd=%d", fifo_fd);
      }
      return fifo_fd;
    }
  }
}
