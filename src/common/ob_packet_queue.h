#ifndef OCEANBASE_COMMON_PACKET_QUEUE_
#define OCEANBASE_COMMON_PACKET_QUEUE_


#include "ob_packet.h"
#include "ob_ring_buffer.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacketQueue
    {
      friend class ObPacketQueueThread;
      public:
      static const int64_t THREAD_BUFFER_SIZE = sizeof(ObPacket) + OB_MAX_PACKET_LENGTH;
      public:
      ObPacketQueue();
      ~ObPacketQueue();

      int init();

      ObPacket* pop();
      int pop_packets(ObPacket** packet_arr, const int64_t ary_size, int64_t& ret_size);
      void push(ObPacket* packet);
      void clear();
      void free(ObPacket* packet);

      int size() const;
      bool empty() const;
      void move_to(ObPacketQueue* destQueue);

      ObPacket* get_timeout_list(const int64_t now);
      ObPacket* get_packet_list();

      ObPacket* head()
      {
        return head_;
      }

      ObPacket* tail()
      {
        return tail_;
      }

      private:
      ObPacket* head_;
      ObPacket* tail_;
      int size_;
      ObRingBuffer ring_buffer_;
      ThreadSpecificBuffer* thread_buffer_;
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_PACKET_QUEUE_ */
