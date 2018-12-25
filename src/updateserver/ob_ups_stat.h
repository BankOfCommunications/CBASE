////===================================================================
 //
 // ob_ups_stat.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-12-07 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_UPS_STAT_H_
#define  OCEANBASE_UPDATESERVER_UPS_STAT_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"

namespace oceanbase
{
  namespace updateserver
  {
    class UpsStatMgr : public common::ObStatManager
    {
      public:
        UpsStatMgr() : ObStatManager(common::OB_UPDATESERVER)
      {
        set_id2name(common::OB_STAT_UPDATESERVER, common::ObStatSingleton::ups_map, common::UPDATESERVER_STAT_MAX);
        set_id2name(common::OB_STAT_COMMON, common::ObStatSingleton::common_map, common::COMMON_STAT_MAX);
        set_id2name(common::OB_STAT_SQL, common::ObStatSingleton::sql_map, common::SQL_STAT_MAX);
        set_id2name(common::OB_STAT_SSTABLE, common::ObStatSingleton::sstable_map, common::SSTABLE_STAT_MAX);
      }
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_UPS_STAT_H_
