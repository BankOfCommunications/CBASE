
#include "ob_ups_info_mgr.h"

using namespace oceanbase;
using namespace common;

bool ObUpsInfoMgr::check_inner_stat(void) const
{
  return (NULL != ups_info_mgr_rpc_stub_) && (rpc_timeout_ > 0);
}

int ObUpsInfoMgr::get_master_ups(ObServer& master_ups, const bool is_update_list, bool& is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;

  if(is_update_list)
  {
    int32_t count = 0;
    ret = fetch_update_server_list(count, is_updated); 
  }
  if(OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard lock(ups_info_mgr_lock_);
    master_ups = update_server_;
  }
  return ret;
}

int ObUpsInfoMgr::init(ObUpsInfoMgrRpcStub* ups_info_mgr_rpc_stub)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_info_mgr_rpc_stub)
  {
    TBSYS_LOG(WARN, "ups_info_mgr_rpc_stub[%p]", ups_info_mgr_rpc_stub);
    ret = OB_INPUT_PARAM_ERROR; 
  }
  else
  {
    this->ups_info_mgr_rpc_stub_ = ups_info_mgr_rpc_stub;
  }
  return ret;
}

int ObUpsInfoMgr::fetch_update_server_list(int32_t& count, bool& is_updated)
{
  int ret = OB_SUCCESS;
  ObUpsList list;
  const ObUpsInfo* master_ups = NULL; 

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
  }

  if(OB_SUCCESS == ret)
  {
    ret = ups_info_mgr_rpc_stub_->fetch_server_list(rpc_timeout_, root_server_, list);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch server list from root server failed:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    count = list.ups_count_;

    if (list.get_sum_percentage(server_type_) <= 0)
    {
      TBSYS_LOG(DEBUG, "reset the percentage for all servers, server_type_[%d], list.ups_count_[%d]", server_type_, list.ups_count_);
      for (int32_t i = 0; i < list.ups_count_; ++i)
      {
        // reset all ms and cs to equal
        list.ups_array_[i].ms_read_percentage_ = 1;
        list.ups_array_[i].cs_read_percentage_ = 1;
      }
      // reset all ms and cs sum percentage to count
      list.sum_ms_percentage_ = list.ups_count_;
      list.sum_cs_percentage_ = list.ups_count_;
    }

    // check finger print changed
    uint64_t finger_print = ob_crc64(&list, sizeof(list));
    if (finger_print != cur_finger_print_)
    {
      is_updated = true;
      TBSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu]", cur_finger_print_, finger_print);
      list.print();
      tbsys::CWLockGuard lock(ups_info_mgr_lock_);
      cur_finger_print_ = finger_print;
      memcpy(&update_server_list_, &list, sizeof(update_server_list_));
      
      if(OB_SUCCESS == ret)
      {
        master_ups = list.select_master_ups();
        if(NULL != master_ups)
        {
          update_server_ = master_ups->addr_;
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "there is no master ups in ups list:count [%d]", count);
        }
      }
    }
    else
    {
      is_updated = false;
      TBSYS_LOG(DEBUG, "ups list not changed:finger[%lu], count[%d]", finger_print, list.ups_count_);
    }
  }
  return ret;
}


int ObUpsInfoMgr::get_ups_list(ObUpsList& ups_list, const bool is_update_list, bool& is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;

  if(is_update_list)
  {
    int32_t count = 0;
    ret = fetch_update_server_list(count, is_updated); 
  }
  if(OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard lock(ups_info_mgr_lock_);
    ups_list = update_server_list_;
  }
  return ret;
}

ObUpsInfoMgr::ObUpsInfoMgr(const ObServerType type, const ObServer& root_server, int64_t rpc_timeout):ups_info_mgr_lock_(tbsys::WRITE_PRIORITY)
{
  rpc_timeout_ = rpc_timeout;
  root_server_ = root_server;
  server_type_ = type;
  cur_finger_print_ = 0;
  ups_info_mgr_rpc_stub_ = NULL;
}

ObUpsInfoMgr::~ObUpsInfoMgr()
{
}


