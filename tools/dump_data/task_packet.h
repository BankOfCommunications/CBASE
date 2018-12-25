
#ifndef _TASK_PACKET_H_
#define _TASK_PACKET_H_

namespace oceanbase
{
  namespace tools
  {
    static const uint64_t INVALID_ID = 0;
    static const int64_t DEFAULT_VERSION = 1;
    enum PacketCode
    {
      FETCH_TASK_REQUEST = 9000,
      FETCH_TASK_RESPONSE = 9001,
      
      REPORT_TASK_REQUEST = 9002,
      REPORT_TASK_RESPONSE = 9003
    };
  }
}



#endif // _TASK_PACKET_H_

