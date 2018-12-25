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

#ifndef OCEANBASE_TEST_OB_ROW_DIS_H_
#define OCEANBASE_TEST_OB_ROW_DIS_H_

#include <stdint.h>

namespace oceanbase
{
  namespace test
  {
    class ObRowDis
    {
      public:
        static const int64_t ROW_P1_NUM_DEFAULT = 100;
        static const int64_t ROW_P2_NUM_DEFAULT = 100;

        static ObRowDis& get_instance()
        {
          static ObRowDis instance;
          return instance;
        }

        int64_t get_row_p1_num()
        {
          return row_p1_num_;
        }

        int64_t get_row_p2_num()
        {
          return row_p2_num_;
        }

        int64_t get_row_p1(int64_t index)
        {
          int64_t ret = 0;
          if (index < 0 || index > row_p1_num_)
          {
          }
          else
          {
            ret = index * interval_p1_;
          }
          return ret;
        }

        int64_t get_row_p2(int64_t index)
        {
          int64_t ret = 0;
          if (index < 0 || index > row_p2_num_)
          {
          }
          else
          {
            ret = index * interval_p2_;
          }
          return ret;
        }

      private:
        ObRowDis(const int64_t row_p1_num = ROW_P1_NUM_DEFAULT,
                 const int64_t row_p2_num = ROW_P2_NUM_DEFAULT)
        {
          row_p1_num_ = row_p1_num;
          row_p2_num_ = row_p2_num;

          interval_p1_ = INT64_MAX / row_p1_num_;
          interval_p2_ = BOMB_INT_MAX / row_p2_num_;
        }

      private:
        int64_t row_p1_num_;
        int64_t row_p2_num_;
        int64_t interval_p1_;
        int64_t interval_p2_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_ROW_DIS_H_
