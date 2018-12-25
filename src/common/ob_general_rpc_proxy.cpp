#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_schema_manager.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/location/ob_tablet_location_cache.h"
#include "common/ob_general_rpc_stub.h"
#include "common/ob_general_rpc_proxy.h"

using namespace oceanbase::common;

ObGeneralRootRpcProxy::ObGeneralRootRpcProxy(const int64_t retry_times,
    const int64_t timeout, const ObServer & root)
{
  rpc_retry_times_ = retry_times;
  rpc_timeout_ = timeout;
  root_server_ = root;
  rpc_stub_ = NULL;
}


ObGeneralRootRpcProxy::~ObGeneralRootRpcProxy()
{
}


int ObGeneralRootRpcProxy::init(ObGeneralRpcStub * rpc_stub)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_stub)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(ERROR, "check param failed:rpc[%p]", rpc_stub);
  }
  else
  {
    rpc_stub_ = rpc_stub;
  }
  return ret;
}

// waring:all return cell in a row must be same as root table's columns,
//        and the second row is this row allocated chunkserver list
int ObGeneralRootRpcProxy::scan_root_table(ObTabletLocationCache * cache,
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
    TBSYS_LOG(DEBUG, "root server get rpc return succeed, cell num=%ld", scanner.get_cell_num());
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
      int64_t version = 0; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
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
              //add zhaoqiong [roottable tablet management] 20160104:b
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
                   //add zhaoqiong [roottable tablet management] 20160104:b
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

void ObGeneralRootRpcProxy::find_tablet_item(const uint64_t table_id, const ObRowkey & row_key,
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
  TBSYS_LOG(DEBUG, "got a tablet:%s, with location list:%ld", to_cstring(range), list.size());
}


