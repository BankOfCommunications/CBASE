/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-12-06
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_

#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include "ob_root_stat_key.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootStatManager :public common::ObStatManager
    {
    public:
      ObRootStatManager():ObStatManager(common::OB_ROOTSERVER)
      {
        set_id2name(common::OB_STAT_COMMON, common::ObStatSingleton::common_map, common::COMMON_STAT_MAX);
        set_id2name(common::OB_STAT_ROOTSERVER, common::ObStatSingleton::rs_map, common::ROOTSERVER_STAT_MAX);
      }
    };
  }
}

#endif // OCEANBASE_ROOTSERVER_OB_ROOT_STAT_H_
