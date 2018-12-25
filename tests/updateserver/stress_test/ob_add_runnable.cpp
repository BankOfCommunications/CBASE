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

#include "ob_add_runnable.h"

#include "ob_test_bomb.h"
#include "ob_executor.h"
#include "ob_update_gen.h"
#include "ob_scan_gen.h"
#include "ob_random.h"
#include "ob_row_dis.h"
#include "ob_schema_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::test;

ObAddRunnable::ObAddRunnable()
{
}

ObAddRunnable::~ObAddRunnable()
{
}

void ObAddRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);

  int ret = OB_SUCCESS;

  ObTestBomb tb;
  ObUpdateGen ug(update_max_cell_no_);
  ObScanGen sg(scan_max_line_no_);
  ObExecutor exer;
  ObGenerator *gen = NULL;
  ObRandom rand;

  rand.initt();

  ret = exer.init(ms_, ups_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObExecutor init error, ret=%d", ret);
  }

  while (OB_SUCCESS == ret && !_stop)
  {
    if (rand.randt(1, write_weight_ + read_weight_) <= write_weight_)
    {
      gen = &ug;
    }
    else
    {
      gen = &sg;
    }
    if (OB_SUCCESS == ret)
    {
      ret = gen->gen(tb);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObUpdateGen gen error, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = exer.exec(tb);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObExecutor exec error, ret=%d", ret);
      }
    }
  }

  TBSYS_LOG(INFO, "TestAddRunnable finish, ret=%d _stop=%d", ret, _stop);
  abort();
}

int ObAddRunnable::init(const ObServer &ms, const ObServer &ups, int write_weight, int read_weight, int64_t update_max_cell_no, int64_t scan_max_line_no)
{
  ms_ = ms;
  ups_ = ups;
  write_weight_ = write_weight;
  read_weight_ = read_weight;
  update_max_cell_no_ = update_max_cell_no;
  scan_max_line_no_ = scan_max_line_no;
  return 0;
}
