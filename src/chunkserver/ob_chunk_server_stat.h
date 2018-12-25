/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_server_stat.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_STAT_H_
#define OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_STAT_H_

#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkServerStatManager : public common::ObStatManager
    {
      public:
        ObChunkServerStatManager()
          : ObStatManager(common::OB_CHUNKSERVER)
        {
          set_id2name(common::OB_STAT_CHUNKSERVER, common::ObStatSingleton::cs_map, common::CHUNKSERVER_STAT_MAX);
          set_id2name(common::OB_STAT_COMMON, common::ObStatSingleton::common_map, common::COMMON_STAT_MAX);
          set_id2name(common::OB_STAT_SSTABLE, common::ObStatSingleton::sstable_map, common::SSTABLE_STAT_MAX);
        }
    };
  } /* chunkserver */
} /* oceanbase */
#endif
