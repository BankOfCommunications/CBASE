/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_inner_table_operator.h,  03/01/2013 17:27:08 PM zhidong Exp $
 *
 * Author:
 *   xielun.szd <xielun.szd@alipay.com>
 * Description:
 *   Util for inner Table
 *
 */

#ifndef __OB_COMMON_INNER_TABLE_OPERATOR_H__
#define __OB_COMMON_INNER_TABLE_OPERATOR_H__

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObiRole;
    class ObServer;
    class ObInnerTableOperator
    {
    public:
      // update __all_cluster <key:cluster_id>
      //mod bingo [Paxos Cluster.Balance] 20161019:b
      /*static int update_all_cluster(ObString & sql, const int64_t cluster_id, const ObServer & server,
          const ObiRole role, const int64_t flow_percent);*/
      static int update_all_cluster(ObString & sql, const int64_t cluster_id, const ObServer & server,
          const int32_t master_cluster_id , const int64_t flow_percent);
      //mod:e
      // update __all_trigger_event <key:timestamp>
      static int update_all_trigger_event(ObString & sql, const int64_t timestmap, const ObServer & server,
          const int64_t type, const int64_t param);
      // update __all_server <key:cluster_id+server_type+server_ip+sever_port>
      static int update_all_server(ObString & sql, const int64_t cluster_id, const char * server_type,
          const ObServer & server, const uint32_t inner_port, const char * server_version);
      // remove __all_server
      static int delete_all_server(ObString & sql, const int64_t cluster_id, const char * server_type,
          const ObServer & server);
    };
  }
}

#endif //__OB_COMMON_INNER_TABLE_OPERATOR_H__
