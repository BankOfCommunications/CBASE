/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_cluster_stats.h is for what ...
 *
 *  Version: $Id: ob_cluster_stats.h 2010年12月22日 15时10分06秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef OCEANBASE_TOOLS_CLUSTER_STATS_H
#define OCEANBASE_TOOLS_CLUSTER_STATS_H

#include "ob_server_stats.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace tools
  {

    class ObClusterStats : public ObServerStats
    {
      public:
        ObClusterStats(ObClientRpcStub &stub, const common::ObRole server_type)
          : ObServerStats(stub, server_type) { }
        virtual ~ObClusterStats() {}
      protected:
        virtual int32_t refresh() ;
    };

    class ObAppStats : public ObServerStats
    {
      public:
        ObAppStats(ObClientRpcStub &stub, const common::ObRole server_type, const char* config_file) ;
        virtual ~ObAppStats() {}
      protected:
        virtual int32_t refresh() ;
      private:
        int sum_cluster_stats(const common::ObRole server_type, const common::ObArrayHelper<common::ObServer>& server_array);
        int sum_cs_cluster_stats(const common::ObServer& rs, const common::ObRole server_type);
        int sum_ups_cluster_stats(const common::ObServer& rs, const common::ObRole server_type);
      private:
        static const int64_t MAX_SERVER_NUM = 1024;
        common::ObServer root_servers_[MAX_SERVER_NUM];
        common::ObArrayHelper<common::ObServer> root_server_array_;
        int64_t master_index_;
    };

  }
}


#endif //OCEANBASE_TOOLS_CLUSTER_STATS_H
