#ifndef _PACKET_QUEUE_H__
#define _PACKET_QUEUE_H__
#include "common/ob_define.h"
#include "load_query.h"
#include "common/ob_list.h"
namespace oceanbase
{
  namespace tools
  {
    struct HeadArray
    {
      int64_t count_;
      int64_t total_count_;
      tbsys::CThreadCond lock_;
      common::ObList<QueryInfo> queue_;
      HeadArray()
      {
        count_ = 0;
        total_count_ =0;
      }
    };
    class PacketQueue
    {
      public:
        PacketQueue();
        ~PacketQueue();
        static int get_iovect(const QueryInfo* query_info, struct iovec *buffer, int &count);
        static void print_iovect(struct iovec *buffer, int count);
        int filter_packet(struct iovec *buffer, const int count);
        int64_t size(void) const;
        int init(int64_t thread_count);
        int select_queue(int &fd, struct iovec *buffer, int count);
        int push(const QueryInfo &query_info, const bool read_only = false);
      public:
        const static int64_t MAX_COUNT = 30000;
        int64_t thread_count_;
        int64_t discart_count_;
        HeadArray *array_;
    };
  }
}
#endif

