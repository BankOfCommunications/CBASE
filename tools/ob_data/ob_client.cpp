#include "ob_client.h"
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
using namespace oceanbase::common;
using namespace oceanbase::tools;

int ObClient::prepare()
{
  // get merge server list
  // get update_server

  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;
  if (OB_SUCCESS != (ret = rpc_->get_merge_server(root_server_, timeout, merge_server_, MAX_COUNT, ms_num_)))
  {
    TBSYS_LOG(ERROR, "failed to get mergeserver, ret = %d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_->get_update_server(root_server_, timeout, update_server_)))
  {
    TBSYS_LOG(ERROR, "failed to get updateserver, ret = %d", ret);
  }
  return ret;
}

int ObClient::scan(const common::ObString & table_name, const common::ObRange & range, common::ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  ObScanParam scan_param;
  ObRange tmp = range;
  scan_param.set(OB_INVALID_ID, table_name, tmp);

  ObVersionRange version_range;
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  scan_param.set_version_range(version_range);
  
  ret = rpc_->scan(merge_server_[0], scan_timeout_, scan_param, scanner);
  if (OB_SUCCESS == ret)
  {
    bool is_fullfilled = false;
    int64_t fullfilled_item_num = -1;
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    // mock server just returns a fullfilled obscanner
    if (is_fullfilled == true)
    {
      // do nothing
      //TBSYS_LOG(INFO, "scan finished");
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "scan failed");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "rpc scan table error, ret = %d",ret);
  }
  return ret;
}
void ObClient::setMergeserver(const ObServer & server)
{
  merge_server_[0] = server;
}
int ObClient::del(const ObString & table_name, const ObRange & range, int64_t & row_count)
{
  // while (not finish all the data)
  // modify the next param and scan ms data
  // mutate delete all the row
  // do while
  srand((unsigned int)time(0));
  ObScanner scanner;
  ObString last_row_key;
  int ret = OB_SUCCESS;
  ObScanParam scan_param;
  ObRange tmp = range;
  ret = scan_param.set(OB_INVALID_ID, table_name, tmp);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "set scan_param failed, ret = %d", ret);
    
  }
  else
  {
    ObVersionRange version_range;
    version_range.border_flag_.set_min_value();
    version_range.border_flag_.set_max_value();
    scan_param.set_version_range(version_range);
    int ms_idx = -1;
    while (true)
    {
      scanner.clear();
      int32_t i = 0;
      ms_idx = rand() % ms_num_;
      for(; i < retry_times_ ; ++i)
      {
        ms_idx = (ms_idx + 1) % ms_num_;
        ret = rpc_->scan(merge_server_[ms_idx], scan_timeout_, scan_param, scanner);
        if (OB_SUCCESS == ret)
        {
          break;
        }
        else
        {
          TBSYS_LOG(ERROR, "scan mergeserver[%s] failed, ret = %d", 
              ob_server_to_string(merge_server_[ms_idx]), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "scan mergeserver success");
        bool is_fullfilled = false;
        int64_t fullfilled_item_num = -1;
        scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
        if ( is_fullfilled == true && fullfilled_item_num == 0)
        {
          TBSYS_LOG(INFO, "mergeserver already no data, is_fullfilled = %d, fullfilled_item_num = %ld", is_fullfilled, fullfilled_item_num);
          break;
        }
        const int64_t timeout = 2000000;
        ret = rpc_->del(update_server_, timeout, scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "del row from updateserver[%s] failed, ret = [%d]", ob_server_to_string(update_server_), ret);
          break;
        }
        else
        {
          row_count += fullfilled_item_num;
          ret = scanner.get_last_row_key(last_row_key);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get last row key failed , ret = %d", ret);
            break;
          }
          else
          {
            ret = string_buf_.write_string(last_row_key, &last_row_key);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "write last row key to string buf failed, ret = %d", ret);
              break;
            }
            else
            {
              tmp.border_flag_.unset_min_value();
              tmp.start_key_ = last_row_key;
              tmp.border_flag_.unset_inclusive_start();
              scan_param.set(OB_INVALID_ID, table_name, tmp);
            }

          }
        }
      }
    }
    // check scan the table at last
    if (OB_SUCCESS == ret)
    {
      ret = scan_param.set(OB_INVALID_ID, table_name, range);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "set scan_param failed, ret = %d", ret);
      }
      else
      {
        ms_idx = rand()%ms_num_;
        ret = rpc_->scan(merge_server_[ms_idx], scan_timeout_, scan_param, scanner);
        if (OB_SUCCESS == ret)
        {
          bool is_fullfilled = false;
          int64_t fullfilled_item_num = -1;
          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
          if (is_fullfilled == true && fullfilled_item_num == 0)
          {
          }
          else
          {
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "check scan table error, ret = %d",ret);
        }
      }
    }
  }
  return ret;
}
