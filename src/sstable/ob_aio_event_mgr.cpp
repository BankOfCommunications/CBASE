/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_aio_event_mgr.cpp for manage aio event. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <tbsys.h>
#include "common/ob_define.h"
#include "ob_aio_event_mgr.h"
#include "ob_aio_buffer_mgr.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;

    ObAIOEventMgr::ObAIOEventMgr() : inited_(false), ctx_(NULL)
    {
      memset(&iocb_, 0, sizeof(struct iocb));
    }

    ObAIOEventMgr::~ObAIOEventMgr()
    {

    }

    int ObAIOEventMgr::init(io_context_t ctx)
    {
      int ret = OB_SUCCESS;

      if (NULL == ctx)
      {
        TBSYS_LOG(WARN, "invalid parameter, ctx=%p", ctx);
        ret = OB_ERROR; 
      }
      else
      {
        ctx_ = ctx;
        inited_ = true;
      }

      return ret;
    }

    int ObAIOEventMgr::aio_submit(const int fd, const int64_t offset, 
                                  const int64_t size, ObAIOBufferInterface& aio_buf)
    {
      int ret               = OB_SUCCESS;
      struct iocb* iocb_tmp = NULL;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio event manager doesn't init");
        ret = OB_ERROR;
      }
      else if (fd < 0 || offset < 0 || size <= 0)
      {
        TBSYS_LOG(WARN, "Invalid parameter, fd=%d, offset=%ld, size=%ld",
                  fd, offset, size);
        ret = OB_ERROR;
      }
      else
      {
        io_prep_pread(&iocb_, fd, aio_buf.get_buffer(), size, offset);
        iocb_.data = &aio_buf;
        iocb_tmp = &iocb_;
        ret = io_submit(ctx_, 1, &iocb_tmp);
        if (1 != ret)
        {
          TBSYS_LOG(WARN, "io_submit failed, ret=%d, error: %s", 
                    ret, strerror(-ret));
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    int ObAIOEventMgr::aio_wait(int64_t& timeout_us)
    {
      int ret               = OB_SUCCESS;
      int64_t event_nr      = 0;
      int inner_ret         = OB_CS_EAGAIN;
      int64_t start_time    = tbsys::CTimeUtil::getTime();
      int64_t cur_timeout_us  = timeout_us;
      int64_t max_events_nr = ObAIOBufferMgr::AIO_BUFFER_COUNT 
                              * OB_MAX_COLUMN_GROUP_NUMBER;
      struct io_event events[max_events_nr];
      struct timespec timeout;

      /**
       * NOTE: there are 2 aio buffer for each thread, maybe both of 
       * the aio buffer are reading data by aio, so aio_wait will 
       * detect both of the aio buffer, and check whether some aio 
       * buffer are ready. 
       */
      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio event manager doesn't init");
        ret = OB_ERROR;
      }
      else
      {
        while (OB_CS_EAGAIN == inner_ret)
        {
          timeout.tv_sec = cur_timeout_us / 1000000;
          timeout.tv_nsec = cur_timeout_us % 1000000 * 1000;
          event_nr = io_getevents(ctx_, 1, max_events_nr, events, &timeout);
          if (0 == event_nr)
          {
            TBSYS_LOG(WARN, "AIO read timeout, event_nr=%ld, timeout_us=%ld", 
                      event_nr, timeout_us);
            timeout_us = 0;
            ret = OB_AIO_TIMEOUT;
            break;
          }
          else
          {
            for (int64_t i = 0; i < event_nr; ++i)
            {
              if (static_cast<int64_t>(events[i].res) < 0 || 0 != events[i].res2)
              {
                TBSYS_LOG(WARN, "aio return invalid result, ret_code=%ld, error=%s", 
                    events[i].res2, strerror(static_cast<int32_t>(-events[i].res)));
                ret = OB_IO_ERROR;
              }
              static_cast<ObAIOBufferInterface*>(events[i].data)->aio_finished(
                  events[i].res, static_cast<int>(events[i].res2));
              if (OB_SUCCESS == ret && events[i].data == iocb_.data)
              {
                /**
                 * this event is what we wait, it means that the waiting aio 
                 * buffer finished aio read, so return OB_SUCCESS. 
                 */
                ret = OB_SUCCESS;
              }
            }

            if (OB_SUCCESS == ret && 0 == cur_timeout_us)
            {
              //just check aio status
              break;
            }
            else
            {
              cur_timeout_us = start_time + timeout_us - tbsys::CTimeUtil::getTime();
              if (OB_SUCCESS == ret)
              {
                timeout_us = cur_timeout_us;
                break;
              }

              if (cur_timeout_us <= 0)
              {
                timeout_us = 0;
                ret = OB_AIO_TIMEOUT;
                break;
              }
              else
              {
                inner_ret = OB_CS_EAGAIN;
              }
            }
          }
        } //while
      }

      return ret;
    }
  } //end namespace sstable
} //end namespace oceanbase
