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

#ifndef OCEANBASE_TEST_OB_TEST_BOMB_H_
#define OCEANBASE_TEST_OB_TEST_BOMB_H_

#include <stdint.h>

#include <common/ob_mutator.h>
#include <common/ob_get_param.h>
#include <common/ob_scan_param.h>

namespace oceanbase
{
  namespace test
  {
    const int64_t BOMB_INT_RESERVED_BITS = 32;
    const int64_t BOMB_INT_MAX = INT64_MAX >> BOMB_INT_RESERVED_BITS;
    const int64_t BOMB_INT_MIN = INT_LEAST64_MIN >> BOMB_INT_RESERVED_BITS;

    const int UPDATE_BOMB = 101;
    const int GET_BOMB = 102;
    const int SCAN_BOMB = 103;

    class ObTestBomb
    {
      public:
        int get_type() const {return type_;}
        void set_type(const int type) {type_ = type;}

        void set_mutator(common::ObMutator* mutator)
        {
          mutator_ = mutator;
          set_type(UPDATE_BOMB);
        }

        common::ObMutator* get_mutator() const
        {
          return mutator_;
        }

        void set_get_param(common::ObGetParam* get_param)
        {
          get_param_ = get_param;
          set_type(GET_BOMB);
        }

        common::ObGetParam* get_get_param() const
        {
          return get_param_;
        }

        void set_scan_param(common::ObScanParam* scan_param)
        {
          scan_param_ = scan_param;
          set_type(SCAN_BOMB);
        }

        common::ObScanParam* get_scan_param() const
        {
          return scan_param_;
        }

      private:
        int type_;
        common::ObMutator* mutator_;
        common::ObGetParam* get_param_;
        common::ObScanParam* scan_param_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_TEST_BOMB_H_
