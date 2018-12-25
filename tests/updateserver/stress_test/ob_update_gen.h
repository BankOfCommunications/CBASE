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

#ifndef OCEANBASE_TEST_OB_UPDATE_GEN_H_
#define OCEANBASE_TEST_OB_UPDATE_GEN_H_

#include "common/ob_mutator.h"
#include "ob_test_bomb.h"
#include "ob_generator.h"
#include "ob_random.h"

namespace oceanbase
{
  namespace test
  {
    class ObUpdateGen : public ObGenerator
    {
      public:
        ObUpdateGen(int64_t max_cell_no);
        virtual ~ObUpdateGen();
        virtual int gen(ObTestBomb &bomb);
        static int gen_line(int n, common::ObString& row_key, int64_t sum, ObRandom& ran, common::ObMutator& mut);
      private:
        int64_t max_cell_no_;
        common::ObMutator mut_;
        ObRandom ran_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_GENERATOR_H_
