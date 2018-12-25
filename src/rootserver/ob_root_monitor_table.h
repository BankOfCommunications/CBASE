/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_ROOT_MONITOR_TABLE_H_
#define _OB_ROOT_MONITOR_TABLE_H_

#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_server.h"
#include "common/ob_vector.h"
#include "common/ob_cluster_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObObj;
    class ObRowkey;
    class ObScanner;
    class ObCellInfo;
  }

  namespace rootserver
  {
    class ObUpsManager;
    class ObChunkServerManager;
    // not thread safe virtual memory table
    class ObRootMonitorTable
    {
    public:
      ObRootMonitorTable();
      ObRootMonitorTable(const common::ObServer & root_server,
          const ObChunkServerManager & cs_manager, const ObUpsManager & ups_manager);
      virtual ~ObRootMonitorTable();
    public:
      typedef common::ObVector<common::ObClusterServer> ServerVector;
      void init(const common::ObServer & root_server, const ObChunkServerManager & cs_manager,
          const ObUpsManager & ups_manager);
      // get request
      int get(const common::ObRowkey & rowkey, common::ObScanner & scanner);
      int get_ms_only(const common::ObRowkey & rowkey, common::ObScanner & scanner);
      // print all the server info
      void print_info(const bool sort = false) const;
    protected:
      // convert server to inner table rowkey
      int convert_server_2_rowkey(const common::ObClusterServer & server, common::ObRowkey & rowkey);
      int convert_rowkey_2_server(const common::ObRowkey & rowkey, common::ObClusterServer & server) const;
    private:
      // fill all the virtual tablet to the scanner
      int fill_result(const int pos, const ServerVector & servers, int64_t & row_count, common::ObScanner & result);
      // add one server as one row
      int add_first_row(common::ObScanner & result);
      int add_normal_row(const int64_t index, const ServerVector & servers, common::ObScanner & result);
      int add_last_row(const ServerVector & servers, common::ObScanner & result);
      int add_server_columns(const common::ObServer & server, common::ObCellInfo & cell, common::ObScanner & result);
      // convert id role to string
      void convert_role(const common::ObRole & server_role, common::ObString & role);
      // convert string role obj to id
      int convert_role(const common::ObObj & obj, common::ObRole & role) const;
      // convert string ip to int
      int convert_addr(const common::ObObj & ip, const common::ObObj & port, common::ObServer & addr) const;
      // convert int to string ip
      int convert_addr(const common::ObServer & server, common::ObString & addr);
    protected:
      struct Compare
      {
        bool operator () (const common::ObClusterServer & s1, const common::ObClusterServer & s2)
        {
          return s1 < s2;
        }
      };
      // find first tablet
      int find_first_tablet(const common::ObRowkey & rowkey, const ServerVector & servers) const;
      // init all rs\cs\ms\ups
      void init_all_servers(ServerVector & servers, int64_t & count) const;
      // init all ms
      void init_all_ms(ServerVector & servers, int64_t &count) const;
      // sort the table rows
      void sort_all_servers(ServerVector & servers) const;
    private:
      int insert_all_meta_servers(ServerVector & servers) const;
      int insert_all_query_servers(ServerVector & servers) const;
      int insert_all_merge_servers(ServerVector & servers) const;
      int insert_all_update_servers(ServerVector & servers) const;
    private:
      static const int64_t OB_MAX_ADDR_LEN = 64;
      static const int64_t MAX_FILL_COUNT = 8;
      common::ObServer rootserver_vip_;
      common::ObObj rowkey_[4];
      char addr_key_[OB_MAX_ADDR_LEN];
      const ObChunkServerManager * cs_manager_;
      const ObUpsManager * ups_manager_;
    };
  }
}

#endif // _OB_ROOT_VIRTUAL_TABLE_H_
