#ifndef TASK_SERVER_CALLBACK_H_
#define TASK_SERVER_CALLBACK_H_

#include "easy_io_struct.h"
namespace oceanbase
{
  namespace tools
  {
    class TaskServerCallback
    {
    public:
      static int process(easy_request_t* req);
    };
  }
}
#endif
