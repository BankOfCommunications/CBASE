////===================================================================
 //
 // ob_store_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-03-16 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "common/ob_define.h"
#include "ob_store_mgr.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    StoreMgr::StoreMgr() : inited_(false),
                           disk_reserved_ratio_(DEFAULT_DISK_RESERVED_RATIO),
                           disk_reserved_space_(DEFAULT_DISK_RESERVED_SPACE),
                           disk_enfile_interval_time_(DEFAULT_DISK_ENFILE_INTERVAL_TIME)
    {
    }

    StoreMgr::~StoreMgr()
    {
      destroy();
    }

    int StoreMgr::init(const char *store_root, const char *raid_regex, const char *dir_regex)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      if (inited_)
      {
        TBSYS_LOG(WARN, "this=%p already inited", this);
        ret = OB_INIT_TWICE;
      }
      else if (NULL == store_root
              || NULL == raid_regex
              || NULL == dir_regex)
      {
        TBSYS_LOG(WARN, "invalid param store_root=%p dir_regex=%p", store_root, dir_regex);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (MAX_DIR_NAME_LENGTH <= (tmp_ret = snprintf(store_root_str_, MAX_DIR_NAME_LENGTH, "%s", store_root)))
      {
        TBSYS_LOG(WARN, "copy fail store_root=[%s] ret=%d", store_root, tmp_ret);
        ret = OB_ERROR;
      }
      else if (MAX_DIR_NAME_LENGTH <= (tmp_ret = snprintf(raid_regex_str_, MAX_DIR_NAME_LENGTH, "%s", raid_regex)))
      {
        TBSYS_LOG(WARN, "copy fail raid_regex=[%s] ret=%d", raid_regex, tmp_ret);
        ret = OB_ERROR;
      }
      else if (MAX_DIR_NAME_LENGTH <= (tmp_ret = snprintf(dir_regex_str_, MAX_DIR_NAME_LENGTH, "%s", dir_regex)))
      {
        TBSYS_LOG(WARN, "copy fail dir_regex=[%s] ret=%d", dir_regex, tmp_ret);
        ret = OB_ERROR;
      }
      else if (!raid_regex_.init(raid_regex_str_, REG_EXTENDED))
      {
        TBSYS_LOG(WARN, "init raid_regex fail raid_regex=[%s]", raid_regex_str_);
        ret = OB_ERROR;
      }
      else if (!dir_regex_.init(dir_regex_str_, REG_EXTENDED))
      {
        TBSYS_LOG(WARN, "init dir_regex fail dir_regex=[%s]", dir_regex_str_);
        ret = OB_ERROR;
      }
      else if (0 != (tmp_ret = pthread_mutex_init(&mutex_, NULL)))
      {
        TBSYS_LOG(WARN, "init mutex fail errno=%u ret=%d", errno, tmp_ret);
        ret = OB_ERROR;
      }
      else if (0 != (tmp_ret = store_dev_.create(DEVICE_SET_SIZE)))
      {
        TBSYS_LOG(WARN, "create store_dev fail ret=%d", tmp_ret);
        ret = OB_ERROR;
      }
      else
      {
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        pthread_mutex_destroy(&mutex_);
        raid_regex_.destroy();
        dir_regex_.destroy();
      }
      return ret;
    }

    void StoreMgr::destroy()
    {
      if (inited_)
      {
        valid_raid_.clear();
        new_store_.clear();
        broken_store_.clear();
        all_store_.clear();
        store_dev_.destroy();
        pthread_mutex_destroy(&mutex_);
        raid_regex_.destroy();
        dir_regex_.destroy();
        inited_ = false;
      }
    }

    bool StoreMgr::load()
    {
      bool bret = false;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        RaidNodeBuilder raid_node_builder(*this);
        lookup_dir_(store_root_str_, raid_node_builder);
        hash::MutexLocker locker(mutex_);
        RaidList::iterator iter;
        for (iter = valid_raid_.begin(); iter != valid_raid_.end();)
        {
          RaidList::iterator tmp_iter = iter;
          iter++;
          if (0 == (*tmp_iter)->node_list.size())
          {
            valid_raid_.erase(tmp_iter);
            TBSYS_LOG(INFO, "erase empty raid=[%s]", (*tmp_iter)->path);
          }
          else
          {
            StoreList::iterator jter;
            for (jter = (*tmp_iter)->node_list.begin(); jter != (*tmp_iter)->node_list.end(); jter++)
            {
              TBSYS_LOG(INFO, "load succ raid=[%s] store=[%s]", (*tmp_iter)->path, (*jter)->path);
            }
          }
        }
        TBSYS_LOG(INFO, "end load new store_dev_num=%ld", store_dev_.size());
        bret = (0 != store_dev_.size());
      }
      log_store_info();
      return bret;
    }

    StoreMgr::StoreNode *StoreMgr::get_store_node_(const Handle store_handle) const
    {
      StoreNode *ret = NULL;
      if ((uint64_t)all_store_.size() <= store_handle
          || NULL == (ret = all_store_[static_cast<int32_t>(store_handle)]))
      {
        TBSYS_LOG(WARN, "invalid store_handle=%lu all_store_size=%d", store_handle, all_store_.size());
      }
      else if (store_handle != ret->store_handle)
      {
        TBSYS_LOG(WARN, "store_handle=%lu not match store_node=%p handle=%lu", store_handle, ret, ret->store_handle);
        ret = NULL;
      }
      return ret;
    }

    StoreMgr::RaidNode *StoreMgr::get_raid_node_(const char *path) const
    {
      RaidNode *ret = NULL;
      RaidList::const_iterator iter;
      for (iter = valid_raid_.begin(); iter != valid_raid_.end(); iter++)
      {
        if (0 == memcmp(path, (*iter)->path, strlen((*iter)->path)))
        {
          ret = *iter;
          break;
        }
      }
      return ret;
    }

    bool StoreMgr::filt_store_(const RaidNode &raid_node, ObList<Handle> &store_list)
    {
      bool bret = false;
      store_list.clear();
      StoreList::const_iterator iter;
      for (iter = raid_node.node_list.begin(); iter != raid_node.node_list.end(); iter++)
      {
        // 过滤非USING状态的store
        StoreNode *store_node = *iter;
        if (USING != store_node->stat)
        {
          TBSYS_LOG(INFO, "invalid store_node=%p path=[%s] stat=%d", store_node, store_node->path, store_node->stat);
        }
        // 检查是文件系统否打开文件过多
        else if (tbsys::CTimeUtil::getTime() - store_node->last_enfile_time < disk_enfile_interval_time_)
        {
          TBSYS_LOG(INFO, "too many file have been opened store_node=%p path=[%s] last_enfile_time=%ld "
                    "cur_time=%ld disk_enfile_interval_time=%ld",
                    store_node, store_node->path, store_node->last_enfile_time,
                    tbsys::CTimeUtil::getTime(), disk_enfile_interval_time_);
        }
        // 检查剩余空间容量
        else if (get_free(store_node->path) <= disk_reserved_space_
                || get_free(store_node->path) <= static_cast<int64_t>((static_cast<double>(get_total(store_node->path)) * disk_reserved_ratio_)))
        {
          TBSYS_LOG(ERROR, "free space not enough store_node=%p path=[%s] free=%ld total=%ld disk_reserved_ratio=%lf",
                    store_node, store_node->path, get_free(store_node->path), get_total(store_node->path), disk_reserved_ratio_);
        }
        else
        {
          store_list.push_back((*iter)->store_handle);
        }
      }
      bret = (0 != store_list.size());
      return bret;
    }

    int StoreMgr::assign_stores(ObList<Handle> &store_list)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
        ret = OB_NOT_INIT;
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        RaidList::iterator iter;
        for (iter = valid_raid_.begin(); iter != valid_raid_.end(); iter++)
        {
          RaidNode *raid_node = *iter;
          if (!filt_store_(*raid_node, store_list))
          {
            TBSYS_LOG(WARN, "raid_node=%p path=[%s] have not valid store", raid_node, raid_node->path);
          }
          else
          {
            valid_raid_.push_back(raid_node);
            valid_raid_.erase(iter);
            break;
          }
        }
        if (0 == store_list.size())
        {
          TBSYS_LOG(WARN, "not valid store to be assigned");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    void StoreMgr::report_broken(const Handle store_handle)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreNode *store_node = get_store_node_(store_handle);
        if (NULL != store_node)
        {
          if (USING != store_node->stat)
          {
            TBSYS_LOG(INFO, "invalid stat=%d store_node=%p path=[%s]",
                      store_node->stat, store_node, store_node->path);
          }
          else
          {
            store_node->stat = BROKEN;
            broken_store_.push_back(store_node);
          }
        }
      }
    }

    void StoreMgr::report_broken(const char *path)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else if (NULL == path)
      {
        TBSYS_LOG(WARN, "invalid param path=%p", path);
      }
      else
      {
        dev_t dev_no = get_dev(path);
        Handle store_handle = INVALID_HANDLE;
        hash::MutexLocker locker(mutex_);
        if (hash::HASH_EXIST != store_dev_.get(dev_no, store_handle))
        {
          TBSYS_LOG(WARN, "get from store_dev fail dev_no=%lu path=[%s]", dev_no, path);
        }
        else
        {
          StoreNode *store_node = get_store_node_(store_handle);
          if (NULL != store_node)
          {
            if (USING != store_node->stat)
            {
              TBSYS_LOG(INFO, "invalid stat=%d store_node=%p path=[%s]",
                        store_node->stat, store_node, store_node->path);
            }
            else
            {
              store_node->stat = BROKEN;
              broken_store_.push_back(store_node);
            }
          }
        }
      }
    }

    void StoreMgr::report_enfile(const Handle store_handle)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreNode *store_node = get_store_node_(store_handle);
        if (NULL != store_node)
        {
          if (USING != store_node->stat)
          {
            TBSYS_LOG(WARN, "invalid stat=%d store_node=%p path=[%s]",
                      store_node->stat, store_node, store_node->path);
          }
          else
          {
            store_node->last_enfile_time = tbsys::CTimeUtil::getTime();
          }
        }
      }
    }
    
    void StoreMgr::report_fclose(const Handle store_handle)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreNode *store_node = get_store_node_(store_handle);
        if (NULL != store_node)
        {
          if (USING != store_node->stat)
          {
            TBSYS_LOG(WARN, "invalid stat=%d store_node=%p path=[%s]",
                      store_node->stat, store_node, store_node->path);
          }
          else
          {
            store_node->last_enfile_time = 0;
          }
        }
      }
    }

    void StoreMgr::erase_store_(const StoreNode *store_node)
    {
      if (NULL != store_node)
      {
        RaidList::iterator raid_iter;
        for (raid_iter = valid_raid_.begin(); raid_iter != valid_raid_.end();)
        {
          RaidList::iterator tmp_raid_iter = raid_iter;
          raid_iter++;
          RaidNode *raid_node = *tmp_raid_iter;
          if (NULL != raid_node)
          {
            StoreList::iterator store_iter;
            for (store_iter = raid_node->node_list.begin(); store_iter != raid_node->node_list.end();)
            {
              StoreList::iterator tmp_store_iter = store_iter;
              store_iter++;
              if (store_node == *tmp_store_iter)
              {
                raid_node->node_list.erase(tmp_store_iter);
                store_dev_.erase(store_node->dev_no);
              }
            }
            if (0 == raid_node->node_list.size())
            {
              valid_raid_.erase(tmp_raid_iter);
              free_(raid_node);
            }
          }
        }
      }
    }

    void StoreMgr::umount(const Handle store_handle)
    {
      char *path = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreNode *store_node = get_store_node_(store_handle);
        if (NULL != store_node)
        {
          if (BROKEN != store_node->stat)
          {
            TBSYS_LOG(ERROR, "invalid stat=%d can not umount, store_node=%p path=[%s]",
                      store_node->stat, store_node, store_node->path);
          }
          else
          {
            erase_store_(store_node);
            path = store_node->path;
            TBSYS_LOG(INFO, "umount store_node=%p store_handle=%lu path=[%s]", store_node, store_handle, path);
          }
        }
      }
      if (NULL != path)
      {
        char broken_path[MAX_DIR_NAME_LENGTH];
        if (MAX_DIR_NAME_LENGTH <= snprintf(broken_path, MAX_DIR_NAME_LENGTH,
                                            "%s.%010ld.broken", path, tbsys::CTimeUtil::getTime()))
        {
          TBSYS_LOG(ERROR, "build broken path fail");
        }
        else if (0 != rename(path, broken_path))
        {
          TBSYS_LOG(INFO, "rename broken path=[%s] to [%s] fail errno=%u", path, broken_path, errno);
        }
        else
        {
          TBSYS_LOG(INFO, "rename broken path=[%s] to [%s] success", path, broken_path);
        }
      }
      if (0 == store_dev_.size())
      {
        TBSYS_LOG(ERROR, "not valid store");
      }
    }

    const char *StoreMgr::get_dir(const Handle store_handle) const
    {
      const char *ret = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreNode *store_node = get_store_node_(store_handle);
        if (NULL != store_node
            && USING == store_node->stat)
        {
          ret = store_node->path;
        }
      }
      return ret;
    }

    const char *StoreMgr::get_new_dir(Handle &store_handle)
    {
      const char *ret = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreList::iterator iter = new_store_.begin();
        StoreNode *store_node = NULL;
        if (new_store_.end() != iter
            && NULL != (store_node = *iter)
            && USING == store_node->stat)
        {
          ret = store_node->path;
          store_handle = store_node->store_handle;
          new_store_.pop_front();
        }
      }
      return ret;
    }

    const char *StoreMgr::get_broken_dir(Handle &store_handle)
    {
      const char *ret = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreList::iterator iter = broken_store_.begin();
        StoreNode *store_node = NULL;
        if (broken_store_.end() != iter
            && NULL != (store_node = *iter)
            && BROKEN == store_node->stat)
        {
          ret = store_node->path;
          store_handle = store_node->store_handle;
          broken_store_.pop_front();
        }
      }
      return ret;
    }

    void StoreMgr::log_store_info()
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
      }
      else
      {
        hash::MutexLocker locker(mutex_);
        StoreVector::iterator store_iter;
        for (store_iter = all_store_.begin(); store_iter != all_store_.end(); store_iter++)
        {
          StoreNode *store_node = *store_iter;
          if (NULL != store_node)
          {
            TBSYS_LOG(INFO, "all store report path=[%s] handle=%lu stat=%d dev_no=%lu",
                      store_node->path, store_node->store_handle, store_node->stat, store_node->dev_no);
          }
        }
      }
    }

    int StoreMgr::get_all_valid_stores(common::ObList<Handle> &store_list)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "this=%p have not inited", this);
        ret = OB_NOT_INIT;
      }
      else
      {
        store_list.clear();
        RaidList::iterator raid_iter;
        for (raid_iter = valid_raid_.begin(); raid_iter != valid_raid_.end(); raid_iter++)
        {
          RaidNode *raid_node = *raid_iter;
          if (NULL != raid_node)
          {
            StoreList::iterator store_iter;
            for (store_iter = raid_node->node_list.begin(); store_iter != raid_node->node_list.end(); store_iter++)
            {
              StoreNode *store_node = *store_iter;
              if (NULL != store_node)
              {
                store_list.push_back(store_node->store_handle);
              }
            }
          }
        }
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    StoreMgr::StoreNodeBuilder::StoreNodeBuilder(StoreMgr &store_mgr,
                                                RaidNode &raid_node) : store_mgr_(store_mgr),
                                                                       raid_node_(raid_node)
    {
    }

    void StoreMgr::StoreNodeBuilder::handle(const char *path)
    {
      StoreNode *store_node = NULL;
      if (!is_symlink(path))
      {
        TBSYS_LOG(INFO, "store [%s] is not symlink", path);
      }
      else if (!build_dir(path, TRASH_DIR))
      {
        TBSYS_LOG(WARN, "build trash dir fail store=[%s]", path);
      }
      else if (!build_dir(path, BYPASS_DIR))
      {
        TBSYS_LOG(WARN, "build bypass dir fail store=[%s]", path);
      }
      else
      {
        dev_t dev_no = StoreMgr::get_dev(path);
        hash::MutexLocker locker(store_mgr_.mutex_);
        if (hash::HASH_INSERT_SUCC != store_mgr_.store_dev_.set(dev_no, -1))
        {
          TBSYS_LOG(INFO, "store [%s] dev=%lu raid=[%s] have other dir in the same device",
                    path, dev_no, raid_node_.path);
        }
        else if (NULL == (store_node = (StoreNode*)store_mgr_.alloc_(sizeof(StoreNode))))
        {
          TBSYS_LOG(WARN, "alloc store_node fail store=[%s] raid=[%s]", path, raid_node_.path);
        }
        else if (OB_SUCCESS != store_mgr_.all_store_.push_back(store_node))
        {
          TBSYS_LOG(WARN, "push store_node to all_store_vector fail store=[%s] raid=[%s]", path, raid_node_.path);
          store_mgr_.free_(store_node);
        }
        else if (0 != store_mgr_.new_store_.push_back(store_node))
        {
          TBSYS_LOG(WARN, "push store_node to new_store_list fail store=[%s] raid=[%s]", path, raid_node_.path);
        }
        else
        {
          Handle store_handle = store_mgr_.all_store_.size() - 1;
          if (hash::HASH_OVERWRITE_SUCC != store_mgr_.store_dev_.set(dev_no, store_handle, 1))
          {
            TBSYS_LOG(WARN, "set handle=%lu to store_dev fail dev_no=%lu", store_handle, dev_no);
          }
          else
          {
            snprintf(store_node->path, MAX_DIR_NAME_LENGTH, "%s", path);
            store_node->stat = USING;
            store_node->store_handle = store_handle;
            store_node->dev_no = dev_no;
            store_node->last_enfile_time = 0;
            raid_node_.node_list.push_back(store_node);
            TBSYS_LOG(INFO, "load store succ [%s] handle=%lu dev_no=%lu", path, store_node->store_handle, dev_no);
          }
        }
      }
    }

    bool StoreMgr::StoreNodeBuilder::match(const char *path)
    {
      return store_mgr_.dir_regex_.match(path, 0);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    StoreMgr::RaidNodeBuilder::RaidNodeBuilder(StoreMgr &store_mgr) : store_mgr_(store_mgr)
    {
    }

    void StoreMgr::RaidNodeBuilder::handle(const char *path)
    {
      RaidNode *raid_node = NULL;
      if (true)
      {
        hash::MutexLocker locker(store_mgr_.mutex_);
        if (NULL == (raid_node = store_mgr_.get_raid_node_(path)))
        {
          if (NULL == (raid_node = (RaidNode*)store_mgr_.alloc_(sizeof(RaidNode)))
              || NULL == (raid_node = new(raid_node) RaidNode()))
          {
            TBSYS_LOG(WARN, "alloc raid_node fail raid=[%s]", path);
          }
          else if (OB_SUCCESS != store_mgr_.valid_raid_.push_back(raid_node))
          {
            TBSYS_LOG(WARN, "push raid_node to valid_raid_list fail raid=[%s]", path);
            store_mgr_.free_(raid_node);
          }
        }
      }
      if (NULL != raid_node)
      {
        snprintf(raid_node->path, MAX_DIR_NAME_LENGTH, "%s", path);
        StoreNodeBuilder store_node_builder(store_mgr_, *raid_node);
        StoreMgr::lookup_dir_(path, store_node_builder);
      }
    }

    bool StoreMgr::RaidNodeBuilder::match(const char *path)
    {
      return store_mgr_.raid_regex_.match(path, 0);
    }
  }
}

