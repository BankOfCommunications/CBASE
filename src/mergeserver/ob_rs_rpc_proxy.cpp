#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_schema_manager.h"
#include "common/location/ob_tablet_location_cache.h"
#include "common/ob_general_rpc_stub.h"
#include "ob_rs_rpc_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


ObMergerRootRpcProxy::ObMergerRootRpcProxy(const int64_t retry_times,
    const int64_t timeout, const ObServer & root) : ObGeneralRootRpcProxy(retry_times, timeout, root)
{
  // Should have been inited in parent class. change this later
  rpc_retry_times_ = retry_times;
  rpc_timeout_ = timeout;
  root_server_ = root;
  rpc_stub_ = NULL;
}


ObMergerRootRpcProxy::~ObMergerRootRpcProxy()
{
}


int ObMergerRootRpcProxy::init(common::ObGeneralRpcStub *rpc_stub)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_stub)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(ERROR, "check param failed:rpc[%p]", rpc_stub);
  }
  else
  {
    // Should have been inited in parent class. change this later
    rpc_stub_ = rpc_stub;
    ret = ObGeneralRootRpcProxy::init(rpc_stub);
  }
  return ret;
}

int ObMergerRootRpcProxy::async_heartbeat(const ObServer & merge_server, const int32_t sql_port,
    const bool is_listen_ms)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    /// async send heartbeat no need retry
    ret = rpc_stub_->heartbeat_merge_server(rpc_timeout_, root_server_, merge_server,
        OB_MERGESERVER, sql_port, is_listen_ms);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "heartbeat with root server failed:ret[%d]", ret);
    }
  }
  return ret;
}

int ObMergerRootRpcProxy::fetch_newest_schema(common::ObMergerSchemaManager * schema_manager,
    const ObSchemaManagerV2 ** manager)
{
  /// fetch new schema
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 * schema = NULL;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  char * temp = NULL;
  if(OB_SUCCESS == ret)
  {
    temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_MS_RPC);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "%s", "check ob malloc failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      schema = new(temp) ObSchemaManagerV2;
      if (NULL == schema)
      {
        TBSYS_LOG(ERROR, "check replacement new schema failed:schema[%p]", schema);
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ret = rpc_stub_->fetch_schema(rpc_timeout_, root_server_, 0, false, *schema);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "rpc fetch schema failed:ret[%d]", ret);
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = schema_manager->add_schema(*schema, manager);
    // maybe failed because of timer thread fetch and add it already
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
      //mod liumz,
      if (ret == OB_NO_EMPTY_ENTRY)
      //if (OB_SUCCESS != OB_NO_EMPTY_ENTRY)
      //mod:e
      {
        ret = OB_SUCCESS;
        *manager = schema_manager->get_user_schema(0);
        if (NULL == *manager)
        {
          TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
              *manager, schema_manager->get_latest_version());
          ret = OB_INNER_STAT_ERROR;
        }
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
    }
  }

  if (schema != NULL)
  {
    schema->~ObSchemaManagerV2();
  }
  if (temp != NULL)
  {
    ob_free(temp);
    temp = NULL;
  }
  return ret;
}

int ObMergerRootRpcProxy::fetch_schema_version(int64_t & timestamp)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->fetch_schema_version(rpc_timeout_, root_server_, timestamp);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
    }
  }
  return ret;
}

// waring:all return cell in a row must be same as root table's columns,
//        and the second row is this row allocated chunkserver list
int ObMergerRootRpcProxy::scan_root_table(ObTabletLocationCache * cache,
    const uint64_t table_id, const ObRowkey & row_key, const ObServer & addr,
    ObTabletLocationList & location)
{
  assert(location.get_buffer() != NULL);
  int ret = OB_SUCCESS;
  bool find = false;
  ObScanner scanner;
  CharArena allocator;
  // root table id = 0
  ret = rpc_stub_->fetch_tablet_location(rpc_timeout_, root_server_, 0,
      table_id, row_key, scanner
       //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
       , addr.cluster_id_
       //add:e
       );
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fetch tablet location failed:table_id[%lu], rowkey[%s], ret[%d]",
        table_id, to_cstring(row_key), ret);
  }
  else
  {
    // no need deep copy the range buffer
    ObTabletLocationList list;
    ObNewRange range;
    ObRowkey start_key;
    start_key = ObRowkey::MIN_ROWKEY;
    ObRowkey end_key;
    ObServer server;
    ObCellInfo * cell = NULL;
    bool row_change = false;
    TBSYS_LOG(DEBUG, "root server get rpc return succeed, cell num=%ld",
              scanner.get_cell_num());
    oceanbase::common::dump_scanner(scanner, TBSYS_LOG_LEVEL_DEBUG, 0);
    ObScannerIterator iter = scanner.begin();
    // all return cell in a row must be same as root table's columns
    ++iter;
    while ((iter != scanner.end())
        && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change)
    {
      if (NULL == cell)
      {
        ret = OB_INNER_STAT_ERROR;
        break;
      }
      cell->row_key_.deep_copy(start_key, allocator);
      ++iter;
    }

    if (ret == OB_SUCCESS)
    {
      int64_t ip = 0;
      int64_t port = 0;
      int64_t version = 0;//add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
      // next cell
      for (++iter; iter != scanner.end(); ++iter)
      {
        ret = iter.get_cell(&cell, &row_change);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
          break;
        }
        else if (row_change) // && (iter != last_iter))
        {
          find_tablet_item(table_id, row_key, start_key, end_key, addr, range, find, list, location);
          if (OB_SUCCESS != cache->set(range, list))
          {
            TBSYS_LOG(ERROR, "add the range[%s] to cache failed", to_cstring(range));
          }
          list.clear();
          start_key = end_key;
        }
        else
        {
          cell->row_key_.deep_copy(end_key, allocator);
          if ((cell->column_name_.compare("1_port") == 0)
              || (cell->column_name_.compare("2_port") == 0)
              || (cell->column_name_.compare("3_port") == 0)
              //add zhaoqiong[roottable tablet management]20160104:b
              || (cell->column_name_.compare("4_port") == 0)
              || (cell->column_name_.compare("5_port") == 0)
              || (cell->column_name_.compare("6_port") == 0))
            //add:e
          {
            ret = cell->value_.get_int(port);
          }
          else if ((cell->column_name_.compare("1_ipv4") == 0)
                   || (cell->column_name_.compare("2_ipv4") == 0)
                   || (cell->column_name_.compare("3_ipv4") == 0)
                   //add zhaoqiong[roottable tablet management]20160104:b
                   || (cell->column_name_.compare("4_ipv4") == 0)
                   || (cell->column_name_.compare("5_ipv4") == 0)
                   || (cell->column_name_.compare("6_ipv4") == 0))
            //add:e
          {
            ret = cell->value_.get_int(ip);
          }
          //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
          else if ((cell->column_name_.compare("1_tablet_version") == 0)
                   || (cell->column_name_.compare("2_tablet_version") == 0)
                   || (cell->column_name_.compare("3_tablet_version") == 0)
                   || (cell->column_name_.compare("4_tablet_version") == 0)
                   || (cell->column_name_.compare("5_tablet_version") == 0)
                   || (cell->column_name_.compare("6_tablet_version") == 0))
                   //add:e
          {
            ret = cell->value_.get_int(version);
            TBSYS_LOG(DEBUG,"tablet_version is %ld, rowkey:%s",version, to_cstring(cell->row_key_));
            //add:e
            if (OB_SUCCESS == ret)
            {
              if (port == 0)
              {
                TBSYS_LOG(WARN, "check port failed:ip[%ld], port[%ld]", ip, port);
              }
              server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
              //mod zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
              //ObTabletLocation addr(0, server);
              ObTabletLocation addr(version, server);
              //mod:e
              if (OB_SUCCESS != (ret = list.add(addr)))
              {
                TBSYS_LOG(ERROR, "add addr failed:ip[%ld], port[%ld], ret[%d]",
                    ip, port, ret);
                break;
              }
              else
              {
                TBSYS_LOG(DEBUG, "add addr succ:ip[%ld], port[%ld], server:%s", ip, port, to_cstring(server));
              }
              ip = port = 0;
            }
          }

          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
            break;
          }
        }
      }
      // for the last row
      TBSYS_LOG(DEBUG, "get a new tablet start_key[%s], end_key[%s]",
          to_cstring(start_key), to_cstring(end_key));
      if ((OB_SUCCESS == ret) && (start_key != end_key))
      {
        find_tablet_item(table_id, row_key, start_key, end_key, addr, range, find, list, location);
        if (OB_SUCCESS != cache->set(range, list))
        {
          TBSYS_LOG(ERROR, "add the range[%s] to cache failed", to_cstring(range));
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "check get first row cell failed:ret[%d]", ret);
    }
  }
  if ((OB_SUCCESS == ret) && (0 == location.size()))
  {
    TBSYS_LOG(ERROR, "check get location size failed:table_id[%ld], rowkey[%s], count[%ld], find[%d]",
        table_id, to_cstring(row_key), location.size(), find);
    ret = OB_INNER_STAT_ERROR;
  }
  return ret;
}

void ObMergerRootRpcProxy::find_tablet_item(const uint64_t table_id, const ObRowkey & row_key,
    const ObRowkey & start_key, const ObRowkey & end_key, const ObServer & addr, ObNewRange & range,
    bool & find, ObTabletLocationList & list, ObTabletLocationList & location)
{
  range.table_id_ = table_id;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  list.set_timestamp(tbsys::CTimeUtil::getTime());
  list.set_tablet_range(range);
  list.sort(addr);
  // double check add all range->locationlist to cache
  if (!find && (row_key <= range.end_key_) && ((row_key > range.start_key_) || range.start_key_.is_min_row()))
  {
    location = list;
    assert(location.get_buffer() != NULL);
    location.set_tablet_range(range);
    find = true;
  }
  if (range.start_key_ >= range.end_key_)
  {
    TBSYS_LOG(WARN, "check range invalid:start[%s], end[%s]",
        to_cstring(range.start_key_), to_cstring(range.end_key_));
  }
  else
  {
    TBSYS_LOG(DEBUG, "got a tablet:%s, with location list:%ld", to_cstring(range), list.size());
  }
}

int ObMergerRootRpcProxy::create_table(bool if_not_exists, const common::TableSchema & table_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->create_table(CREATE_DROP_TABLE_TIME_OUT, root_server_, if_not_exists, table_schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to create table, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "create table succ, tid=%lu", table_schema.table_id_);
    }
  }
  return ret;
}
//add wenghaixing [secondary index drop index]20141223
int ObMergerRootRpcProxy::drop_index(const common::ObStrings &indexs)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->drop_index(CREATE_DROP_TABLE_TIME_OUT, root_server_, indexs);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "drop indexs succ, tables=%s", to_cstring(indexs));
    }
  }
  return ret;
}
//add e
int ObMergerRootRpcProxy::drop_table(bool if_exists, const common::ObStrings & tables)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->drop_table(CREATE_DROP_TABLE_TIME_OUT, root_server_, if_exists, tables);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "drop table succ, tables=%s", to_cstring(tables));
    }
  }
  return ret;
}

//add zhaoqiong [Truncate Table]:20160318:b
int ObMergerRootRpcProxy::truncate_table(bool if_exists, const common::ObStrings & tables, const common::ObString & user, const common::ObString & comment)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->truncate_table(CREATE_DROP_TABLE_TIME_OUT, root_server_, if_exists, tables, user, comment);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to truncate table, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "truncate table succ, tables=%s", to_cstring(tables));
    }
  }
  return ret;
}
//add:e

int ObMergerRootRpcProxy::alter_table(const common::AlterTableSchema& alter_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->alter_table(CREATE_DROP_TABLE_TIME_OUT, root_server_, alter_schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to alter table, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "alter table succ, table=%s", alter_schema.table_name_);
    }
  }
  return ret;
}
int ObMergerRootRpcProxy::set_obi_role(const ObServer &rs, const int64_t timeout, const ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_->set_obi_role(rs, timeout, obi_role);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "set obi role to server[%s] failed, ret=%d", to_cstring(rs), ret);
  }
  return ret;
}
int ObMergerRootRpcProxy::get_obi_role(const int64_t timeout_us, const common::ObServer& root_server, common::ObiRole &obi_role) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rpc_stub_->get_obi_role(timeout_us, root_server, obi_role)))
  {
    TBSYS_LOG(WARN, "get obi role from rootserver[%s] failed, ret=%d", to_cstring(root_server), ret);
  }
  return ret;
}
int ObMergerRootRpcProxy::set_master_rs_vip_port_to_cluster(const ObServer &rs, const int64_t timeout, const char *new_master_ip, const int32_t new_master_port)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rpc_stub_->set_master_rs_vip_port_to_cluster(rs, timeout, new_master_ip, new_master_port)))
  {
    TBSYS_LOG(WARN, "set new master rs vip port to rs[%s] failed, ret=%d", to_cstring(rs), ret);
  }
  return ret;
}
int ObMergerRootRpcProxy::fetch_master_ups(const ObServer &rootserver, ObServer & master_ups)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rpc_stub_->fetch_update_server(rpc_timeout_, rootserver, master_ups)))
  {
    TBSYS_LOG(ERROR, "fetch master ups in this cluster failed, ret=%d", ret);
  }
  return ret;
}
