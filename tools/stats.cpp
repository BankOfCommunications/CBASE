#include <stdio.h>
#include "stats.h"

namespace oceanbase { namespace tools {


    int32_t Stats::summary(const int32_t count, const int32_t interval) 
    {
        int32_t ret = 0;
        ret = refresh();
        if (ret) { fprintf(stderr, "refresh error, ret=%d\n", ret); return ret; }
        ret = calc();
        if (ret) { fprintf(stderr, "calc error, ret=%d\n", ret); return ret; }
        ret = output(count, interval);
        if (ret) {  fprintf(stderr, "output error, ret=%d\n", ret); return ret; }
        ret = save();
        if (ret) {  fprintf(stderr, "save error, ret=%d\n", ret); return ret; }
        return 0;
    }

}}



