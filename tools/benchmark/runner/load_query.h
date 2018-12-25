#ifndef LOAD_QUERY_INFO_H_
#define LOAD_QUERY_INFO_H_

#include "common/ob_fifo_stream.h"

namespace oceanbase
{
  namespace tools
  {
    class QueryInfo
    {
    public:
      QueryInfo();
      virtual ~QueryInfo();
    public:
      // only set the packet to null
      void reset(void);
    public:
      int64_t id_;
      common::FIFOPacket * packet_;
    public:
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
  }
}

#endif //LOAD_QUERY_INFO_H_
