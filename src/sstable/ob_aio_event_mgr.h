/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_aio_event_mgr.h for manage aio event. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_AIO_EVENT_MGR_H_
#define OCEANBASE_SSTABLE_OB_AIO_EVENT_MGR_H_

#include <libaio.h>

namespace oceanbase 
{
  namespace sstable 
  {
    class ObAIOBufferInterface;
    class ObAIOEventMgr
    {
    public:
      ObAIOEventMgr();
      ~ObAIOEventMgr();

      /**
       * initialize aio event manager with io_context_t
       * 
       * @param ctx the io_context_t to assign to the aio event 
       *            manager
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR
       */
      int init(io_context_t ctx);

      /**
       * submit an aio read request, it just encapsulate io_submit 
       * simply 
       * 
       * @param fd file to read
       * @param offset offset in file 
       * @param size size to read
       * @param aio_buf the destination buffer which store the read
       *               data
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR
       */
      int aio_submit(const int fd, const int64_t offset, 
                     const int64_t size, ObAIOBufferInterface& aio_buf);

      /**
       * wait aio read to complete, it just  encapsulate io_wait 
       * simply 
       * 
       * @param timeout_us [in/out] timeout in us
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR or OB_AIO_TIMEOUT
       */
      int aio_wait(int64_t& timeout_us);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObAIOEventMgr);
      bool inited_;
      io_context_t ctx_;  //io_context, thread local instance 
      struct iocb iocb_;  //io callback instance
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif //OCEANBASE_SSTABLE_OB_AIO_EVENT_MGR_H_
