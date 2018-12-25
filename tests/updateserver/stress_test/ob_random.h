/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_TEST_OB_RANDOM_H_
#define OCEANBASE_TEST_OB_RANDOM_H_

#include <math.h>
#include <stdlib.h>

namespace oceanbase
{
  namespace test
  {
    class ObRandom
    {
      public:
        static int64_t rand64(int64_t min, int64_t max)
        {
          if (min == max)
          {
            return min;
          }
          else
          {
            double r = drand48();
            int64_t range = max - min + 1;
            return lrint(r * range) + max;
          }
        }

        static int rand(int int_max)
        {
          return ::rand() % int_max + 1;
        }

        static int rand(int int_min, int int_max)
        {
          return ::rand() % (int_max - int_min + 1) + int_min;
        }

        void initt()
        {
          int64_t time = tbsys::CTimeUtil::getTime();
          xsubi[0] = time & 0xFFFF;
          xsubi[1] = (time >> 2) & 0xFFFF;
          xsubi[2] = (time >> 4) & 0xFFFF;
          //TBSYS_LOG(DEBUG, "========%x %d %d %d======", time, xsubi[0], xsubi[1], xsubi[2]);
        }

        int64_t rand64t(int64_t max)
        {
          return rand64t(1, max);
        }

        int64_t rand64t(int64_t min, int64_t max)
        {
          if (min == max)
          {
            return min;
          }
          else
          {
            double r = erand48(xsubi);
            int64_t range = max - min + 1;
            double v = trunc(r * range);
            int64_t ret = lrint(v) + min;
            //TBSYS_LOG(DEBUG, "========%f %ld %f %ld======", r, range, v, ret);
            return ret;
          }
        }

        int randt(int int_max)
        {
          return randt(1, int_max);
        }

        int randt(int int_min, int int_max)
        {
          int ret = static_cast<int>(rand64t(int_min, int_max));
          //TBSYS_LOG(DEBUG, "========%d %d %d======", int_min, int_max, ret);
          return ret;
        }
      private:
        unsigned short xsubi[3];
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_RANDOM_H_
