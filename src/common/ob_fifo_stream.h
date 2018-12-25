////===================================================================
 //
 // ob_fifo_stream.h common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2012-03-04 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 流式导出工具 通过linux fifo文件传递出来
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_FIFO_STREAM_H_
#define  OCEANBASE_COMMON_FIFO_STREAM_H_
#include "tbsys.h"
#include "ob_multi_wakeup.h"
#include "ob_fixed_queue.h"
#include "ob_fifo_allocator.h"
#include "ob_packet.h"
#include <sys/uio.h>
namespace oceanbase
{
  namespace common
  {
#define FIFO_PACKET_MAGIC 0x544b505f4f464946 // "FIFO_PKT"
#define CUR_FIFO_PACKET_VERSION 1

#define REQUEST_MAGIC 0x5453455551455200 // "REQUEST"
#define CUR_REQUEST_VERSION 1

//#define ATOMIC_INC(val) (void)__sync_add_and_fetch((val), 1)
//#define ATOMIC_DEC(val) (void)__sync_sub_and_fetch((val), 1)

    enum
    {
      FIFO_PKT_BINARY = 1,
      FIFO_PKT_REQUEST = 2,
    };

    struct FIFOPacket
    {
      int64_t magic;
      int64_t version;
      int64_t timestamp;
      int32_t type;
      int32_t buf_size;
      char buf[];
    };

    struct Request
    {
      int64_t magic;
      int32_t version;

      int32_t pcode;
      uint64_t peer_id;

      int32_t api_version;
      int32_t source_id;
      int32_t target_id;
      int32_t timeout;

      int64_t receive_ts;
      int32_t priority;
      int32_t buf_size;

      char buf[];
    };

    class ObFIFOStream : public tbsys::CDefaultRunnable
    {
      static const int64_t MEMPOOL_PAGE_SIZE = 1L * 1024L * 1024L;
      static const int64_t AVG_PACKET_SIZE = 512;
      static const int64_t SLEEP_TIME_US = 1L * 1000L * 1000L;
      static const int64_t WRITE_BUFFER_SIZE = 512L * 1024L;
      public:
        ObFIFOStream();
        ~ObFIFOStream();
      public:
        int init(const char *fifo_path, const int64_t max_buffer_size, const int64_t expired_time = INT64_MAX);
        void destroy();
      public:
        int push(const char *buffer, const int32_t size);
        int push(const ObPacket *packet);
        int push(const struct iovec *iov, int iovcnt);
        int64_t get_queue_size() const;
      private:
        void run(tbsys::CThread *thread, void *arg);
        void *alloc_(const int64_t size);
        void free_(void *ptr);
        void release_old_();
        void setThreadCount(int threadCount);
      private:
        static int64_t get_packet_size_(const FIFOPacket *pkt);
        static int64_t write_(const int fd, const char *buffer, const int64_t buffer_size,
                              int64_t useconds, const bool &stop);
        static int swriteable_us_(const int fd, const int64_t useconds);
        static int mkfifo_(const char *fifo_path);
      private:
        bool inited_;
        int fifo_fd_;
        FIFOAllocator allocator_;
        char *write_buffer_;
        ObMultiWakeup wakeup_;
        ObFixedQueue<FIFOPacket> queue_;
        int64_t expired_time_;
        int64_t alloc_counter_;
        int64_t free_counter_;
    };
  }
}

#endif //OCEANBASE_COMMON_FIFO_STREAM_H_
