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

#ifndef OCEANBASE_TEST_OB_ADD_RUNNABLE_H_
#define OCEANBASE_TEST_OB_ADD_RUNNABLE_H_

#include "tbsys.h"

#include "common/ob_define.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace test
  {
    class ObAddRunnable : public tbsys::CDefaultRunnable
    {
    public:
      ObAddRunnable();
      virtual ~ObAddRunnable();

      virtual void run(tbsys::CThread* thread, void* arg);

      /// @brief 初始化
      /// @param [in] ms Mergerserver地址
      /// @param [in] ups Updateserver地址
      int init(const common::ObServer &ms, const common::ObServer &ups, int write_weight, int read_weight, int64_t update_max_cell_no, int64_t scan_max_line_no);
    protected:
      int64_t update_max_cell_no_;
      int64_t scan_max_line_no_;
      int write_weight_;
      int read_weight_;
      common::ObServer ms_;
      common::ObServer ups_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_ADD_RUNNABLE_H_
