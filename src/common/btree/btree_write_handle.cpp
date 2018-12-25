#include "btree_alloc.h"
#include "btree_base.h"
#include "btree_node.h"
#include "btree_write_handle.h"
#include "btree_root_pointer.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 构造
     */
    BtreeWriteHandle::BtreeWriteHandle()
    {
      owner_ = NULL;
      mutex_ = NULL;
      error_ = ERROR_CODE_OK;
      next_handle_ = NULL;
      old_value_ = NULL;
      old_key_ = NULL;
      prev_root_pointer_ = NULL;
      key_allocator_ = NULL;
      node_allocator_ = NULL;
      ppr_create_ = 0;
      level_ = 0;
      parent_ = NULL;
    }
    /**
     * 析构
     */
    BtreeWriteHandle::~BtreeWriteHandle()
    {
      end();
    }


    /**
     * 开始
     */
    void BtreeWriteHandle::start(pthread_mutex_t *mutex, BtreeCallback *owner)
    {
      if (mutex) pthread_mutex_lock(mutex);
      mutex_ = mutex;
      owner_ = owner;
      level_ = 0;
      parent_ = NULL;
      new_key_list_.init(BtreeArrayList::NODE_SIZE);
      old_key_list_.init(BtreeArrayList::NODE_SIZE);
    }

    /**
     * 结束
     */
    void BtreeWriteHandle::end()
    {
      // 把下级副本提交
      if (NULL == owner_)
        return;

      if (NULL != next_handle_)
      {
        next_handle_->end();
        char *data = reinterpret_cast<char*>(next_handle_);
        OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator is null.");
        if (node_allocator_)
        {
          node_allocator_->release(data);
        }
        next_handle_ = NULL;
      }
      if (NULL != root_pointer_)
      {
        if (ERROR_CODE_OK == error_)
          error_ = owner_->callback_done(this);

        // 有错误发生. 不要切换, 把node释放掉
        if (ERROR_CODE_OK != error_)
        {
          root_pointer_->release_node_list(static_cast<int64_t>(0), key_allocator_);
          root_pointer_->destroy();
          if (NULL != prev_root_pointer_ && ppr_create_ == 0)
            prev_root_pointer_->clear_list_only();
        }
        root_pointer_ = NULL;
      }
      if (NULL != prev_root_pointer_ && ppr_create_)
      {
        if (ppr_create_ == 2)
        {
          prev_root_pointer_->release_node_list(static_cast<int64_t>(0), key_allocator_);
        }
        prev_root_pointer_->destroy();
      }
      // 需要对key的引用计数进行减－
      if (key_allocator_ && ERROR_CODE_OK == error_)
      {
        int32_t *old_addr = NULL;
        BtreeNodeList list;
        new_key_list_.node_list_first(list);
        // 取出每个key
        while((old_addr = reinterpret_cast<int32_t*>(new_key_list_.node_list_next(list))) != NULL)
        {
          if (parent_)
          {
            parent_->add_key_to_list(reinterpret_cast<char*>(old_addr), 1);
          }
          else if (old_addr)
          {
            OCEAN_BTREE_CHECK_TRUE((*old_addr) >= 0, "old_addr: %d", (*old_addr));
            (*old_addr) ++;
          }
        }

        old_key_list_.node_list_first(list);
        // 取出每个key
        while((old_addr = reinterpret_cast<int32_t*>(old_key_list_.node_list_next(list))) != NULL)
        {
          OCEAN_BTREE_CHECK_TRUE(old_addr, "old_addr is null.");
          if (parent_)
          {
            parent_->add_key_to_list(reinterpret_cast<char*>(old_addr), -1);
          }
          else if (old_addr)
          {
            OCEAN_BTREE_CHECK_TRUE((*old_addr) > 0, "old_addr: %d", (*old_addr));
            (*old_addr) --;
            // 释放掉key
            if (0 == (*old_addr))
            {
              key_allocator_->release(reinterpret_cast<char*>(old_addr));
            }
          }
        }
      }
      old_key_list_.clear_list_only(node_allocator_);
      new_key_list_.clear_list_only(node_allocator_);
      owner_ = NULL;
      key_allocator_ = NULL;
      node_allocator_ = NULL;
      prev_root_pointer_ = NULL;
      ppr_create_ = 0;
      level_ = 0;
      error_ = ERROR_CODE_OK;
      // 解锁
      if (NULL != mutex_)
      {
        pthread_mutex_unlock(mutex_);
        mutex_ = NULL;
      }
    }

    /**
     * 回滚
     */
    void BtreeWriteHandle::rollback()
    {
      error_ = ERROR_CODE_FAIL;
    }

    /**
     * 产生一个下一级的writehandle
     */
    BtreeWriteHandle *BtreeWriteHandle::get_write_handle()
    {
      OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator is null.");

      BtreeWriteHandle *handle = NULL;
      if (NULL != mutex_) pthread_mutex_lock(mutex_);
      // 在本级put过还没commit.
      if (root_pointer_ && root_pointer_->need_set_only())
      {
        root_pointer_->set_read_only(NULL, false);
      }
      // 第一个write_handle
      if (!prev_root_pointer_)
      {
        if (node_allocator_)
        {
          prev_root_pointer_ = reinterpret_cast<BtreeRootPointer*>(node_allocator_->alloc());
          OCEAN_BTREE_CHECK_TRUE(prev_root_pointer_, "prev_root_pointer_ is null.");
          if (prev_root_pointer_) prev_root_pointer_->init(node_allocator_);
        }
        ppr_create_ = 2;
      }

      // 如果next_handle存在并且已经end过, release掉
      if (next_handle_ && NULL == next_handle_->owner_)
      {
        char *data = reinterpret_cast<char*>(next_handle_);
        if (node_allocator_)
        {
          node_allocator_->release(data);
        }
        next_handle_ = NULL;
      }
      // 如果next_handle存在了.就不能再产生了
      if (prev_root_pointer_ && NULL == next_handle_ && NULL != root_pointer_)
      {
        BtreeRootPointer *proot = NULL;
        BtreeRootPointer *prev_proot = NULL;
        handle = reinterpret_cast<BtreeWriteHandle*>(node_allocator_->alloc());
        // 生成一个BtreeRootPointer
        proot = reinterpret_cast<BtreeRootPointer*>(node_allocator_->alloc());
        prev_proot = reinterpret_cast<BtreeRootPointer*>(node_allocator_->alloc());

        OCEAN_BTREE_CHECK_TRUE(handle && proot && prev_proot,
                               "handle: %p, proot: %p, prev_proot: %p", handle, proot, prev_proot);

        if (handle && proot && prev_proot)
        {
          next_handle_ = handle;
          proot->init(node_allocator_);
          // 复制
          for(int32_t i = 0; i < CONST_MAX_BTREE_COUNT; i++)
          {
            proot->tree_depth_[i] = root_pointer_->tree_depth_[i];
            proot->root_[i] = root_pointer_->root_[i];
          }
          proot->total_object_count_ = root_pointer_->total_object_count_;
          proot->node_count_ = root_pointer_->node_count_;
          proot->sequence_ = root_pointer_->sequence_;
          handle->root_pointer_ = proot;
          handle->key_allocator_ = key_allocator_;
          handle->node_allocator_ = node_allocator_;
          handle->owner_ = this;
          handle->mutex_ = mutex_;
          handle->prev_root_pointer_ = prev_proot;
          prev_proot->init(node_allocator_);
          handle->ppr_create_ = 1;
          handle->level_ = static_cast<int16_t>(level_ + 1);
          handle->parent_ = this;
          handle->old_key_list_.init(BtreeArrayList::NODE_SIZE);
          handle->new_key_list_.init(sizeof(BtreeWriteHandle));
        }
      }
      if (NULL != mutex_) pthread_mutex_unlock(mutex_);
      return handle;
    }

    /**
     * 让下级writehandle回调, 把下一级的handle提交上来
     */
    int BtreeWriteHandle::callback_done(void *data)
    {
      BtreeWriteHandle *handle = reinterpret_cast<BtreeWriteHandle*>(data);
      int32_t ret = ERROR_CODE_FAIL;
      OCEAN_BTREE_CHECK_TRUE(handle && root_pointer_ && prev_root_pointer_,
                             "handle: %p, root_pointer_: %p, prev_root_pointer: %p",
                             handle, root_pointer_, prev_root_pointer_);
      if (handle)
      {
        OCEAN_BTREE_CHECK_TRUE(handle->root_pointer_ && handle->prev_root_pointer_,
                               "root_pointer_: %p, prev_root_pointer: %p",
                               handle->root_pointer_, handle->prev_root_pointer_);
      }
      if (handle && root_pointer_ && handle->root_pointer_ &&
          prev_root_pointer_ && handle->prev_root_pointer_)
      {
        // 把下级的rootpointer上的node放在自己的rootpointer上
        BtreeNode *node = NULL;
        BtreeNodeList list;

        handle->root_pointer_->node_list_first(list);
        // 取出每个node
        while((node = handle->root_pointer_->node_list_next(list)) != NULL)
        {
          if (0 == node->is_deleted()) root_pointer_->add_node_list(node);
        }
        handle->root_pointer_->set_read_only(NULL, false);

        // prev_root_pointer
        handle->prev_root_pointer_->node_list_first(list);
        // 取出每个node
        while((node = handle->prev_root_pointer_->node_list_next(list)) != NULL)
        {
          if (node->get_sequence() < handle->root_pointer_->sequence_ && 0 == node->is_deleted())
          {
            prev_root_pointer_->add_node_list(node);
          }
          else
          {
            node->set_deleted(1);
          }
        }

        // root节点
        for(int32_t i = 0; i < CONST_MAX_BTREE_COUNT; i++)
        {
          root_pointer_->tree_depth_[i] = handle->root_pointer_->tree_depth_[i];
          root_pointer_->root_[i] = handle->root_pointer_->root_[i];
        }
        root_pointer_->total_object_count_ = handle->root_pointer_->total_object_count_;
        root_pointer_->node_count_ = handle->root_pointer_->node_count_;

        // 释放掉root_pointer_
        handle->root_pointer_->destroy();
        handle->prev_root_pointer_->destroy();
        handle->root_pointer_ = NULL;
        handle->prev_root_pointer_ = NULL;
        ret = ERROR_CODE_OK;
      }

      return ret;
    }

    /**
     * 得到object数量
     */
    int64_t BtreeWriteHandle::get_object_count()
    {
      return (root_pointer_ ? root_pointer_->total_object_count_ : 0);
    }
    /**
     * 得到node数量
     */
    int32_t BtreeWriteHandle::get_node_count()
    {
      return (root_pointer_ ? root_pointer_->node_count_ : 0);
    }

    /**
    * 得到old_value
    */
    char *BtreeWriteHandle::get_old_value()
    {
      return old_value_;
    }

    /**
    * 得到old_key
    */
    char *BtreeWriteHandle::get_old_key()
    {
      return old_key_;
    }

    /**
     * 加key到list中
     */
    void BtreeWriteHandle::add_key_to_list(char *key, int32_t value)
    {
      if (value > 0)
        new_key_list_.add_node_list(node_allocator_, 0, key);
      else
        old_key_list_.add_node_list(node_allocator_, 0, key);
    }

  } // end namespace common
} // end namespace oceanbase
