#ifndef OCEANBASE_OBSQL_STATS_H
#define OCEANBASE_OBSQL_STATS_H

#include <stdint.h>
#include "common/ob_server.h"

namespace oceanbase
{
  using namespace oceanbase::common;
  namespace obsql
  {
    class Stats
    {
        public:
            virtual ~Stats() {}
            virtual int32_t summary(ObServer *remote_server, const int32_t interval) ;
            virtual int32_t init() = 0 ;
            virtual int32_t output(const int32_t interval) = 0;

        protected:
            virtual int32_t calc() = 0;
            virtual int32_t save() = 0;
            virtual int32_t refresh(const ObServer *remote_server) = 0;
    };

  }
}



#endif //OCEANBASE_OBSQL_STATS_H

