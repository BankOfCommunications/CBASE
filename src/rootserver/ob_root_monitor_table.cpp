/*
 * Copyright (C) 2007-2013 Taobao Inc.
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

#include "ob_root_monitor_table.h"
#include "ob_chunk_server_manager.h"
#include "ob_ups_manager.h"
#include "common/ob_rowkey.h"
#include "common/ob_cluster_server.h"
#include "common/ob_extra_tables_schema.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

// only one server for this tablet
static const char * size = "occupy_size";
static const char * host = "1_ipv4";
static const char * port = "1_port";
static const ObString SIZE(static_cast<int32_t>(strlen(size)), static_cast<int32_t>(strlen(size)), size);
static const ObString IP(static_cast<int32_t>(strlen(host)), static_cast<int32_t>(strlen(host)), host);
static const ObString PORT(static_cast<int32_t>(strlen(port)), static_cast<int32_t>(strlen(port)), port);
static const ObString TNAME(static_cast<int32_t>(strlen(OB_ALL_SERVER_STAT_TABLE_NAME)),
                            static_cast<int32_t>(strlen(OB_ALL_SERVER_STAT_TABLE_NAME)),
                            OB_ALL_SERVER_STAT_TABLE_NAME);
//add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170316:b
static const char * tablet_version = "1_tablet_version";
static const ObString TABLET_VERSION(static_cast<int32_t>(strlen(tablet_version)), static_cast<int32_t>(strlen(tablet_version)), tablet_version);
//add:e

ObRootMonitorTable::ObRootMonitorTable()
{
  cs_manager_ = NULL;
  ups_manager_ = NULL;
}

ObRootMonitorTable::ObRootMonitorTable(const ObServer & root_server,
  const ObChunkServerManager & cs_manager, const ObUpsManager & ups_manager):
  rootserver_vip_(root_server), cs_manager_(&cs_manager),
  ups_manager_(&ups_manager)
{
}

ObRootMonitorTable::~ObRootMonitorTable()
{
}

void ObRootMonitorTable::init(const ObServer & root_server,
    const ObChunkServerManager & cs_manager, const ObUpsManager & ups_manager)
{
  rootserver_vip_ = root_server;
  cs_manager_ = &cs_manager;
  ups_manager_ = &ups_manager;
}

int ObRootMonitorTable::get(const ObRowkey & rowkey, ObScanner & scanner)
{
  int pos = -1;
  int ret = OB_SUCCESS;
  // add all type of server for in memory table
  int64_t count = 0;
  ServerVector servers;
  init_all_servers(servers, count);
  if (count <= 0)
  {
    TBSYS_LOG(WARN, "after init all servers:count[%ld]", count);
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    sort_all_servers(servers);
  }
  // find the frist tablet and fill rows to the scanner
  if (OB_SUCCESS == ret)
  {
    pos = find_first_tablet(rowkey, servers);
    scanner.reset();
    int64_t row_count = 0;
    ret = fill_result(pos, servers, row_count, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fill result failed:rowkey[%s], ret[%d]", to_cstring(rowkey), ret);
    }
    else if ((scanner.get_row_num() != row_count) || row_count <= 1)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "check fill row count error:result[%ld], fill[%ld]",
          scanner.get_row_num(), row_count);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fill result succ:rowkey[%s], count[%ld]",
          to_cstring(rowkey), row_count);
    }
  }
  return ret;
}

int ObRootMonitorTable::get_ms_only(const ObRowkey & rowkey, ObScanner & scanner)
{
  int pos = -1;
  int ret = OB_SUCCESS;
  // add all type of server for in memory table
  int64_t count = 0;
  ServerVector servers;
  init_all_ms(servers, count);
  if (count <= 0)
  {
    TBSYS_LOG(WARN, "after init all servers:count[%ld]", count);
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    sort_all_servers(servers);
  }
  // find the frist tablet and fill rows to the scanner
  if (OB_SUCCESS == ret)
  {
    pos = find_first_tablet(rowkey, servers);
    scanner.reset();
    int64_t row_count = 0;
    ret = fill_result(pos, servers, row_count, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fill result failed:rowkey[%s], ret[%d]", to_cstring(rowkey), ret);
    }
    else if ((scanner.get_row_num() != row_count) || row_count <= 1)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "check fill row count error:result[%ld], fill[%ld]",
          scanner.get_row_num(), row_count);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fill result succ:rowkey[%s], count[%ld]",
          to_cstring(rowkey), row_count);
    }
  }
  return ret;
}

int ObRootMonitorTable::insert_all_meta_servers(ServerVector & servers) const
{
  ObClusterServer server;
  server.role = OB_ROOTSERVER;
  server.addr = rootserver_vip_;
  int ret = servers.push_back(server);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "add root server failed:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "add root server succ:vip[%s]", server.addr.to_cstring());
  }
  return ret;
}

int ObRootMonitorTable::insert_all_query_servers(ServerVector & servers) const
{
  int ret = OB_SUCCESS;
  int64_t cs_count = 0;
  int64_t ms_count = 0;
  ObClusterServer server;
  const ObServerStatus* it = NULL;
  if (cs_manager_ != NULL)
  {
    for (it = cs_manager_->begin(); it != cs_manager_->end(); ++it)
    {
      if (ObServerStatus::STATUS_DEAD != it->status_)
      {
        server.role = OB_CHUNKSERVER;
        server.addr = it->server_;
        server.addr.set_port(it->port_cs_);
        ret = servers.push_back(server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "insert chunk server failed:ret[%d]", ret);
          break;
        }
        ++cs_count;
      }
      if (ObServerStatus::STATUS_DEAD != it->ms_status_)
      {
        server.role = OB_MERGESERVER;
        server.addr = it->server_;
        server.addr.set_port(it->port_ms_);
        ret = servers.push_back(server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "insert merge server failed:ret[%d]", ret);
          break;
        }
        ++ms_count;
      }
    }
  }
  TBSYS_LOG(DEBUG, "insert all chunk & merge server:ret[%d], cs[%ld], ms[%ld]",
      ret, cs_count, ms_count);
  return ret;
}

int ObRootMonitorTable::insert_all_merge_servers(ServerVector & servers) const
{
  int ret = OB_SUCCESS;
  int64_t ms_count = 0;
  ObClusterServer server;
  const ObServerStatus* it = NULL;
  if (cs_manager_ != NULL)
  {
    for (it = cs_manager_->begin(); it != cs_manager_->end(); ++it)
    {
      if (ObServerStatus::STATUS_DEAD != it->ms_status_)
      {
        server.role = OB_MERGESERVER;
        server.addr = it->server_;
        server.addr.set_port(it->port_ms_);
        ret = servers.push_back(server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "insert merge server failed:ret[%d]", ret);
          break;
        }
        ++ms_count;
      }
    }
  }
  TBSYS_LOG(DEBUG, "insert all merge server:ret[%d], ms[%ld]",
      ret, ms_count);
  return ret;
}

int ObRootMonitorTable::insert_all_update_servers(ServerVector & servers) const
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (ups_manager_ != NULL)
  {
    ObUpsList list;
    ObClusterServer server;
    ret = ups_manager_->get_ups_list(list);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get ups list failed:ret[%d]", ret);
    }
    else
    {
      server.role = OB_UPDATESERVER;
      for (int32_t i = 0; i < list.ups_count_; ++i)
      {
        server.addr = list.ups_array_[i].addr_;
        ret = servers.push_back(server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "insert merge server failed:ret[%d]", ret);
          break;
        }
        else
        {
          ++count;
        }
      }
    }
  }
  TBSYS_LOG(DEBUG, "insert all update server:ret[%d], count[%ld]", ret, count);
  return ret;
}

void ObRootMonitorTable::init_all_servers(ServerVector & servers, int64_t & count) const
{
  servers.clear();
  int ret = insert_all_meta_servers(servers);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "insert all root server failed:ret[%d]", ret);
  }
  ret = insert_all_query_servers(servers);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "insert all chunk server failed:ret[%d]", ret);
  }
  ret = insert_all_update_servers(servers);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "insert all update server failed:ret[%d]", ret);
  }
  count = servers.size();
}

void ObRootMonitorTable::init_all_ms(ServerVector & servers, int64_t & count) const
{
  servers.clear();
  int ret = insert_all_merge_servers(servers);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "insert all chunk server failed:ret[%d]", ret);
  }
  count = servers.size();
}

void ObRootMonitorTable::sort_all_servers(ServerVector & servers) const
{
  std::sort(servers.begin(), servers.end(), Compare());
}

int ObRootMonitorTable::find_first_tablet(const ObRowkey & rowkey, const ServerVector & servers) const
{
  int ret = -1;
  if (rowkey.is_min_row())
  {
    ret = 0;
  }
  else
  {
    ObClusterServer server;
    int err = convert_rowkey_2_server(rowkey, server);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "convert rowkey to role server failed:ret[%d]", err);
    }
    else
    {
      Compare comp;
      ObVector<ObClusterServer>::iterator iter;
      iter = std::lower_bound(servers.begin(), servers.end(), server, comp);
      ret = static_cast<int>(iter - servers.begin());
    }
  }
  return ret;
}

int ObRootMonitorTable::fill_result(const int pos, const ServerVector & servers,
    int64_t & row_count, ObScanner & result)
{
  int ret = OB_SUCCESS;
  int index = pos;
  if ((pos < 0) || (pos > servers.size()))
  {
    ret = OB_INNER_STAT_ERROR;
    TBSYS_LOG(WARN, "check find pos failed:pos[%d], size[%d]", pos, servers.size());
  }
  // fake first min obj row for the first tablet
  else if ((0 == pos) || (servers.size() == 1))
  {
    ret = add_first_row(result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add first row failed:ret[%d]", ret);
    }
    else
    {
      ++row_count;
      TBSYS_LOG(TRACE, "add the first row succ:pos[%d], server[%s], count[%ld]",
          0, servers.at(0).addr.to_cstring(), row_count);
    }
    // from index - 1
    index = pos + 1;
  }
  else if (pos >= servers.size())
  {
    // from index - 1
    index = servers.size() - 1;
  }
  // fill the server exclude the last server
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = index - 1; (i >= 0) && (i < servers.size() - 1) && (row_count < MAX_FILL_COUNT); ++i)
    {
      ret = add_normal_row(i, servers, result);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add row failed:ret[%d], pos[%ld], count[%ld]", ret, i, row_count);
        break;
      }
      else
      {
        ++row_count;
        TBSYS_LOG(TRACE, "add normal row succ:pos[%ld], server[%s], count[%ld]",
            i, servers.at(static_cast<int32_t>(i)).addr.to_cstring(), row_count);
      }
    }
  }
  // add the last server row
  if ((OB_SUCCESS == ret) && (row_count < MAX_FILL_COUNT))
  {
    ret = add_last_row(servers, result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add last row failed:ret[%d]", ret);
    }
    else
    {
      ++row_count;
      TBSYS_LOG(TRACE, "add the last row succ:pos[%d], server[%s], count[%ld]",
          servers.size() - 1, servers.at(servers.size() - 1).addr.to_cstring(), row_count);
    }
  }
  return ret;
}

int ObRootMonitorTable::add_normal_row(const int64_t index, const ServerVector & servers, ObScanner & result)
{
  int ret = OB_SUCCESS;
  if ((index < 0) || (index >= servers.size() - 1))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check normal server index failed:index[%ld], count[%d]",
        index, servers.size());
  }
  else
  {
    ObCellInfo cell;
    cell.table_name_ = TNAME;
    // the last role server
    ObClusterServer server = servers.at(static_cast<int32_t>(index));
    ObClusterServer next = servers.at(static_cast<int32_t>(index + 1));
    if (next.role != server.role)
    {
      ObString role;
      convert_role(server.role, role);
      rowkey_[0].set_varchar(role);
      rowkey_[1].set_max_value();
      rowkey_[2].set_max_value();
      rowkey_[3].set_max_value();
      cell.row_key_.assign(rowkey_, 4);
    }
    else
    {
      ret = convert_server_2_rowkey(server, cell.row_key_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "convert server to cell rowkey failed:ret[%d]", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = add_server_columns(server.addr, cell, result);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add server columns for normal tablet failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}

// ugly fake first row
int ObRootMonitorTable::add_first_row(ObScanner & result)
{
  ObCellInfo cell;
  cell.table_name_ = TNAME;
  rowkey_[0].set_min_value();
  cell.row_key_.assign(rowkey_, 1);
  cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  // no need column value
  cell.column_name_ = PORT;
  int ret = result.add_cell(cell);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "add fake row cell failed:ret[%d]", ret);
  }
  return ret;
}

int ObRootMonitorTable::add_server_columns(const ObServer & server, ObCellInfo & cell, ObScanner & result)
{
  // must be fake + port + ip + fake
  cell.column_name_ = SIZE;
  cell.value_.set_int(0);
  int ret = result.add_cell(cell);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "add fake port cell failed:ret[%d]", ret);
  }
  else
  {
    // add 1_port column
    cell.column_name_ = PORT;
    cell.value_.set_int(server.get_port());
    ret = result.add_cell(cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add port cell failed:ret[%d]", ret);
    }
    else
    {
      // add 1_ipv2 column
      cell.column_name_ = IP;
      cell.value_.set_int(server.get_ipv4());
      ret = result.add_cell(cell);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add ip cell failed:ret[%d]", ret);
      }
      //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170316:b
      else
      {
          cell.column_name_ = TABLET_VERSION;
          cell.value_.set_int(0);
          ret = result.add_cell(cell);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "add fake version cell failed:ret[%d]", ret);
          }
      }
      //add:e
    }

  }
  return ret ;
}

// ugly add the last row
int ObRootMonitorTable::add_last_row(const ServerVector & servers, ObScanner & result)
{
  int ret = OB_SUCCESS;
  int64_t index = servers.size() - 1;
  if (index < 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check last server index failed:index[%ld]", index);
  }
  else
  {
    ObCellInfo cell;
    // rowkey to max
    cell.table_name_ = TNAME;
    rowkey_[0].set_max_value();
    cell.row_key_.assign(rowkey_, 1);
    ObClusterServer server = servers.at(static_cast<int32_t>(index));
    ret = add_server_columns(server.addr, cell, result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add server columns for last tablet failed:ret[%d]", ret);
    }
  }
  return ret;
}


int ObRootMonitorTable::convert_server_2_rowkey(const ObClusterServer & server, ObRowkey & rowkey)
{
  int ret = OB_SUCCESS;
  // for every row the key is [role , ip , port , max_obj]
  ObString role;
  convert_role(server.role, role);
  ObString host;
  ret = convert_addr(server.addr, host);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "convert int ip to string host failed:server[%s], ret[%d]",
        server.addr.to_cstring(), ret);
  }
  else
  {
    rowkey_[0].set_varchar(role);
    rowkey_[1].set_varchar(host);
    rowkey_[2].set_int(server.addr.get_port());
    rowkey_[3].set_max_value();
    // set all the rowkey
    rowkey.assign(rowkey_, 4);
  }
  return ret;
}

int ObRootMonitorTable::convert_rowkey_2_server(const ObRowkey & rowkey, ObClusterServer & server) const
{
  int ret = OB_SUCCESS;
  const ObObj * keys = rowkey.get_obj_ptr();
  if ((NULL == keys) || (rowkey.length() > ObExtraTablesSchema::TEMP_ROWKEY_LENGTH))
  {
    TBSYS_LOG(WARN, "check rowkey failed:length[%ld], ptr[%p]", rowkey.length(), keys);
  }
  else
  {
    ret = convert_role(keys[0], server.role);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "convert role failed:ret[%d]", ret);
    }
    else
    {
      ret = convert_addr(keys[1], keys[2], server.addr);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "convert addr failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}

// convert id to string role
void ObRootMonitorTable::convert_role(const ObRole & server_role, ObString & role)
{
  role = ObString::make_string(print_role(server_role));
}

// convert string to id role
int ObRootMonitorTable::convert_role(const ObObj & obj, ObRole & role) const
{
  int ret = OB_SUCCESS;
  if (obj.is_min_value())
  {
    role = OB_INVALID;
  }
  else
  {
    ObString str_role;
    ret = obj.get_varchar(str_role);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get varchar type failed:ret[%d]", ret);
    }
    else
    {
      role = OB_INVALID;
      bool find = false;
      for (int i = static_cast<int>(OB_INVALID + 1); i <= static_cast<int>(OB_UPDATESERVER); ++i)
      {
        if (str_role.compare(print_role(static_cast<ObRole>(i))) == 0)
        {
          role = static_cast<ObRole>(i);
          find = true;
          break;
        }
      }
      if (!find)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check role error:role[%.*s]", str_role.length(), str_role.ptr());
      }
    }
  }
  return ret;
}

// convert int ip to string addr not include port
int ObRootMonitorTable::convert_addr(const ObServer & server, ObString & addr)
{
  int ret = OB_SUCCESS;
  // convert int ip to string
  if (false == server.ip_to_string(addr_key_, sizeof(addr_key_)))
  {
    ret = OB_INNER_STAT_ERROR;
    TBSYS_LOG(WARN, "ip to string failed:server[%s]", server.to_cstring());
  }
  else
  {
    addr.assign(addr_key_, static_cast<int32_t>(strlen(addr_key_)));
  }
  return ret;
}

// convert string to int server addr
int ObRootMonitorTable::convert_addr(const ObObj & ip, const ObObj & port, ObServer & server) const
{
  int ret = OB_SUCCESS;
  // set to max value for compare
  if (ip.is_max_value())
  {
    server.set_max();
  }
  else if (!ip.is_min_value())
  {
    char addr[OB_MAX_ADDR_LEN] = "";
    ObString host;
    ret = ip.get_varchar(host);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get varchar for ip failed:ret[%d]", ret);
    }
    else
    {
      memcpy(addr, host.ptr(), host.length());
      addr[host.length()] = '\0';
    }
    // port 1024 no one can use
    int64_t nport = 1024;
    if ((OB_SUCCESS == ret) && !port.is_min_value())
    {
      ret = port.get_int(nport);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get int for port failed:ret[%d]", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (true != server.set_ipv4_addr(addr, static_cast<int32_t>(nport)))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "set addr failed:addr[%s], port[%ld], ret[%d]",
            addr, nport, ret);
      }
    }
  }
  return ret;
}

void ObRootMonitorTable::print_info(const bool sort) const
{
  ServerVector servers;
  int64_t count = 0;
  init_all_servers(servers, count);
  if (true == sort)
  {
    sort_all_servers(servers);
  }
  TBSYS_LOG(TRACE, "print servers in the cluster:count[%ld], sort[%d]", count, sort);
  for (int32_t i = 0; i < count; ++i)
  {
    TBSYS_LOG(INFO, "index:%d, role:%d, server:%s",
        i, servers[i].role, servers[i].addr.to_cstring());
  }
}
