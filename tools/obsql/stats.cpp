#include <stdio.h>
#include "stats.h"
#include "common/utility.h"

namespace oceanbase
{ 
  using namespace oceanbase::common;
  namespace obsql
  {
    int32_t Stats::summary(ObServer *remote_server, const int32_t interval) 
    {
        int32_t ret = 0;
        ret = refresh(remote_server);
        if (ret) { fprintf(stderr, "refresh error, ret=%d\n", ret); return ret; }
        ret = save();
        if (ret) { fprintf(stderr, "save error, ret=%d\n", ret); return ret; }
        if (interval > 0)
        {
          sleep(interval);
          ret = refresh(remote_server);
          if (ret) { fprintf(stderr, "refresh error, ret=%d\n", ret); return ret; }
          ret = save();
          if (ret) { fprintf(stderr, "save error, ret=%d\n", ret); return ret; }
          ret = calc();
          if (ret) { fprintf(stderr, "calc error, ret=%d\n", ret); return ret; }
        }
        ret = output(interval);
        if (ret) { fprintf(stderr, "output error, ret=%d\n", ret); return ret; }
        
        return 0;
    }

  }
}



