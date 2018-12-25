#ifndef OCEANBASE_COMMON_PACKET_FACTORY_H_
#define OCEANBASE_COMMON_PACKET_FACTORY_H_

#include "ob_define.h"
#include "ob_packet.h"
#include "thread_buffer.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacketFactory
    {
      public:
        ObPacketFactory()
        {
          packet_buffer_ = new ThreadSpecificBuffer(THREAD_BUFFER_SIZE);
        }

        ~ObPacketFactory()
        {
          if (packet_buffer_ != NULL)
          {
            delete packet_buffer_;
            packet_buffer_ = NULL;
          }
        }

        //这里的pcode没有用， 有这个参数是为了兼容
        ObPacket* createPacket(int pcode = 0)
        {
          UNUSED(pcode);
          ObPacket* packet = NULL;
          ThreadSpecificBuffer::Buffer* tb = packet_buffer_->get_buffer();
          if (tb == NULL)
          {
            TBSYS_LOG(ERROR, "get packet thread buffer failed, return NULL");
          }
          else
          {
            char* buf = tb->current();
            packet = new(buf) ObPacket();
            buf += sizeof(ObPacket);
            packet->set_packet_buffer(buf, OB_MAX_PACKET_LENGTH);
            packet->set_no_free();
          }
          return packet;
        }
        
        void destroyPacket(ObPacket* packet)
        {
          UNUSED(packet);
          //do nothing
        }

      private:
        static const int32_t THREAD_BUFFER_SIZE = sizeof(ObPacket) + OB_MAX_PACKET_LENGTH;

      private:
        ThreadSpecificBuffer *packet_buffer_;
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_PACKET_FACTORY_H_ */
