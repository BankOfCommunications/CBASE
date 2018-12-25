#ifndef OCEANBASE_TOOLS_STATS_H
#define OCEANBASE_TOOLS_STATS_H

#include <stdint.h>

namespace oceanbase { namespace tools {


    class Stats
    {
        public:
            virtual ~Stats() {}
            virtual int32_t summary(const int32_t count, const int32_t interval) ;
            virtual int32_t init() = 0 ;
            virtual int32_t output(const int32_t count, const int32_t interval) = 0;

        protected:
            virtual int32_t calc() = 0;
            virtual int32_t save() = 0;
            virtual int32_t refresh() = 0;
    };



}}



#endif //OCEANBASE_TOOLS_STATS_H

