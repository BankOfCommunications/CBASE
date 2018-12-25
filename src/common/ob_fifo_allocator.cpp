////===================================================================
 //
 // ob_fifo_allocator.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-31 by Yubai (yubai.lk@taobao.com) 
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

#include "ob_fifo_allocator.h"

//#define ob_malloc(size) ::malloc(size)
//#define ob_free(ptr) ::free(ptr)

namespace oceanbase
{
  namespace common
  {
    FIFOAllocator::FIFOAllocator() : inited_(false),
                                     mod_id_(0),
                                     id_map_(),
                                     free_list_(),
                                     thread_node_key_(0),
                                     thread_node_lock_(),
                                     thread_node_allocator_(),
                                     thread_node_list_(NULL),
                                     thread_num_(0),
                                     total_limit_(0),
                                     hold_limit_(0),
                                     page_size_(0),
                                     allocated_size_(0)
    {
      pthread_key_create(&thread_node_key_, NULL);
      pthread_spin_init(&thread_node_lock_, PTHREAD_PROCESS_PRIVATE);
    }

    FIFOAllocator::~FIFOAllocator()
    {
      destroy();
      pthread_spin_destroy(&thread_node_lock_);
      pthread_key_delete(thread_node_key_);
    }

    int FIFOAllocator::init(const int64_t total_limit,
                            const int64_t hold_limit,
                            const int64_t page_size)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have inited, this=%p", this);
        ret = OB_INIT_TWICE;
      }
      else if (0 >= total_limit
              || 0 >= hold_limit
              || 0 >= page_size
              || total_limit < hold_limit
              || hold_limit < page_size)
      {
        TBSYS_LOG(WARN, "invalid param total_limit=%ld hold_limit=%ld page_size=%ld",
                  total_limit, hold_limit, page_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = id_map_.init(total_limit / page_size * 1000))) // total_limit maybe magnify
      {
        TBSYS_LOG(WARN, "init id_map fail, ret=%d total_limit=%ld page_size=%ld",
                  ret, total_limit, page_size);
      }
      else if (OB_SUCCESS != (ret = free_list_.init(hold_limit / page_size)))
      {
        TBSYS_LOG(WARN, "init free_list fail, ret=%d old_size=%ld page_size=%ld",
                  ret, hold_limit, page_size);
      }
      else
      {
        total_limit_ = total_limit;
        hold_limit_ = hold_limit;
        page_size_ = page_size;
        allocated_size_ = 0;
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void FIFOAllocator::destroy()
    {
      volatile ThreadNode *iter = thread_node_list_;
      while (NULL != iter)
      {
        revert_page_(iter->id, NULL);
        iter = iter->next;
      }
      thread_node_list_ = NULL;
      thread_node_allocator_.reuse();

      if (0 != id_map_.size())
      {
        TBSYS_LOG(ERROR, "still memory have not been free: ref_page_cnt=%d", id_map_.size());
        BACKTRACE(WARN, true, "fifo_allocator not clear");
      }

      Page *page = NULL;
      while (OB_SUCCESS == free_list_.pop(page))
      {
        if (NULL != page)
        {
          ob_free(page);
        }
      }
      free_list_.destroy();

      id_map_.destroy();

      total_limit_ = 0;
      hold_limit_ = 0;
      page_size_ = 0;
      allocated_size_ = 0;
      inited_ = false;
    }

    FIFOAllocator::Page *FIFOAllocator::get_page_(const int64_t require_size, uint64_t &id)
    {
      Page *ret = NULL;
      ThreadNode *thread_node = (ThreadNode*)pthread_getspecific(thread_node_key_);
      if (NULL == thread_node)
      {
        pthread_spin_lock(&thread_node_lock_);
        thread_node = thread_node_allocator_.alloc(sizeof(ThreadNode));
        pthread_spin_unlock(&thread_node_lock_);
        if (NULL != thread_node)
        {
          thread_node->id = UINT64_MAX;
          thread_node->next = NULL;
          int tmp_ret = pthread_setspecific(thread_node_key_, thread_node);
          if (0 != tmp_ret)
          {
            TBSYS_LOG(WARN, "setspecific fail, ret=%d key=%d thread_node=%p",
                tmp_ret, thread_node_key_, thread_node);
          }
          else
          {
            while (true)
            {
              volatile ThreadNode *ov = thread_node_list_;
              thread_node->next = ov;
              if (ov == ATOMIC_CAS(&thread_node_list_, ov, thread_node))
              {
                break;
              }
            }

            int64_t new_thread_num = ATOMIC_ADD(&thread_num_, 1);
            int64_t old_total_limit = total_limit_;
            int64_t new_total_limit = page_size_ * new_thread_num;
            if (new_total_limit > total_limit_
                && old_total_limit == ATOMIC_CAS(&total_limit_, old_total_limit, new_total_limit))
            {
              TBSYS_LOG(WARN, "total_limit cannot support at least one page for each thread, will force to modify total_limit from %ld to %ld",
                  old_total_limit, new_total_limit);
            }
          }
        }
      }
      if (NULL != thread_node)
      {
        Page *tmp_page = NULL;
        id_map_.get(thread_node->id, tmp_page);
        if (NULL != tmp_page
            && page_size_ < (tmp_page->pos + require_size))
        {
          revert_page_(thread_node->id, NULL);
          thread_node->id = UINT64_MAX;
          tmp_page = NULL;
        }
        if (NULL == tmp_page)
        {
          tmp_page = alloc_page_();
          uint64_t tmp_id = UINT64_MAX;
          if (NULL != tmp_page)
          {
            ATOMIC_ADD(&(tmp_page->ref_cnt), 1);
            if (OB_SUCCESS == id_map_.assign(tmp_page, tmp_id))
            {
              thread_node->id = tmp_id;
            }
            else
            {
              TBSYS_LOG(WARN, "assign from id_map_ fail, page=%p", tmp_page);
              free_page_(tmp_page);
              tmp_page = NULL;
            }
          }
        }
        if (NULL != tmp_page
            && page_size_ >= (tmp_page->pos + require_size))
        {
          id = thread_node->id;
          ret = tmp_page;
        }
      }
      return ret;
    }

    void FIFOAllocator::revert_page_(const uint64_t id, void *ptr)
    {
      Page *page = NULL;
      id_map_.get(id, page);
      if (NULL == page)
      {
        TBSYS_LOG(ERROR, "get page fail, maybe double free, ptr=%p id=%lu", ptr, id);
      }
      else
      {
        if (0 == ATOMIC_ADD(&(page->ref_cnt), -1))
        {
          id_map_.erase(id);
          free_page_(page);
        }
      }
    }

    FIFOAllocator::Page *FIFOAllocator::alloc_page_()
    {
      Page *ret = NULL;
      if (total_limit_ >= ATOMIC_ADD(&allocated_size_, page_size_))
      {
        free_list_.pop(ret);
        if (NULL == ret)
        {
          ret = (Page*)ob_malloc(page_size_ + sizeof(Page), mod_id_);
          if (NULL == ret)
          {
            TBSYS_LOG(WARN, "alloc from system fail size=%ld", page_size_ + sizeof(Page));
          }
        }
        if (NULL != ret)
        {
          ret->ref_cnt = 0;
          ret->pos = 0;
        }
        else
        {
          //add zhaoqiong [fifo allocator bug] 20161118:b
          TBSYS_LOG(ERROR, "alloc failed, allocated_size=[%ld] and total_limit=[%ld]", allocated_size_,total_limit_);
          //add:e
          ATOMIC_ADD(&allocated_size_, -page_size_);
        }
        //add zhaoqiong [fifo allocator bug] 20161118:b
        if (allocated_size_ >= 50 * total_limit_ / 100)
        {
          TBSYS_LOG(ERROR, "allocated_size[%ld] has reached the warn threshold, total_limit[%ld]", allocated_size_,total_limit_);
        }
        //add:e
      }
      else
      {
        //add zhaoqiong [fifo allocator bug] 20161118:b
        TBSYS_LOG(ERROR, "alloc failed, allocated_size[%ld] exceeds total_limit[%ld]", allocated_size_,total_limit_);
        //add:e
        ATOMIC_ADD(&allocated_size_, -page_size_);
      }
      return ret;
    }

    void FIFOAllocator::free_page_(Page *ptr)
    {
      if (NULL != ptr)
      {
        ATOMIC_ADD(&allocated_size_, -page_size_);
        if (hold_limit_ <= (free_list_.get_total() * page_size_)
            || OB_SUCCESS != free_list_.push(ptr))
        {
          ob_free(ptr);
        }
      }
    }

    void FIFOAllocator::set_mod_id(const int32_t mod_id)
    {
      mod_id_ = mod_id;
    }

    void *FIFOAllocator::alloc(const int64_t size)
    {
      void *ret = NULL;
      uint64_t id = UINT64_MAX;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited, this=%p", this);
      }
      else if (0 >= size
               || page_size_ < size + (int64_t)sizeof(id))
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
      }
      else
      {
        Page *page = get_page_(size + sizeof(id), id);
        if (NULL != page)
        {
          *(uint64_t*)(page->buf + page->pos) = id;
          ret = page->buf + page->pos + sizeof(id);
          ATOMIC_ADD(&(page->ref_cnt), 1);
          page->pos += (uint32_t)(size + sizeof(id));
        }
      }
      return ret;
    }

    void FIFOAllocator::free(void *ptr)
    {
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited, this=%p", this);
      }
      else if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid param, null pointer");
      }
      else
      {
        uint64_t id = *(uint64_t*)((char*)ptr - sizeof(id));
        *(uint64_t*)((char*)ptr - sizeof(id)) = UINT64_MAX;
        revert_page_(id, ptr);
      }
    }
  }
}

