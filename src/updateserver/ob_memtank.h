////===================================================================
 //
 // ob_memtank.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2010-11-23 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_UPDATESERVER_MEMTANK_H_
#define  OCEANBASE_UPDATESERVER_MEMTANK_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/page_arena.h"
#include "common/utility.h"
#include "common/ob_rowkey.h"
#include "ob_ups_utils.h"
#include "common/ob_simple_tpl.h"
#include "common/ob_stack_allocator.h"

namespace oceanbase
{
  namespace updateserver
  {
    class IExternMemTotal
    {
      public:
        IExternMemTotal() {};
        virtual ~IExternMemTotal() {};
      public:
        virtual int64_t get_extern_mem_total() = 0;
    };

    class DefaultExternMemTotal : public IExternMemTotal
    {
      public:
        virtual int64_t get_extern_mem_total()
        {
          return 0;
        }
    };
    class MemTank
    {
      typedef common::ObHandyAllocatorWrapper<common::TSIStackAllocator> Allocator;
      static const int64_t PAGE_SIZE = 2 * 1024L * 1024L;
      static const int64_t AVAILABLE_WARN_SIZE = 2L * 1024L * 1024L * 1024L; //2G
      static const int64_t BLOCK_ALLOCATOR_LIMIT_DELTA = 2L * 1024L * 1024L * 1024L;
      public:
        static const int64_t REPLAY_MEMTABLE_RESERVE_SIZE_GB = 1; // 1g
      public:
        MemTank(const int32_t mod_id = common::ObModIds::OB_UPS_MEMTABLE)
          : total_limit_(INT64_MAX),
            extern_mem_total_ptr_(&default_extern_mem_total_)
        {
          int64_t block_size = PAGE_SIZE;
          block_allocator_.set_mod_id(mod_id);
          string_buf_.init(&block_allocator_, block_size);
          allocer_.init(&block_allocator_, block_size);
          undo_allocer_.init(&block_allocator_, block_size);
          tevalue_allocer_.init(&block_allocator_, block_size);
          btree_engine_allocer_.init(&block_allocator_, block_size);
          hash_engine_allocer_.init(&block_allocator_, block_size);
        };
        ~MemTank()
        {
          clear();
        };
      public:
        int write_string(const common::ObString& str, common::ObString* stored_str)
        {
          int ret = common::OB_MEM_OVERFLOW;
          if (!mem_over_limit())
          {
            ret = string_buf_.write_string(str, stored_str);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        int write_string(const common::ObRowkey& rowkey, common::ObRowkey* stored_rowkey)
        {
          int ret = common::OB_MEM_OVERFLOW;
          if (!mem_over_limit())
          {
            ret = string_buf_.write_string(rowkey, stored_rowkey);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        int write_obj(const common::ObObj& obj, common::ObObj* stored_obj)
        {
          int ret = common::OB_MEM_OVERFLOW;
          if (!mem_over_limit())
          {
            ret = string_buf_.write_obj(obj, stored_obj);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *node_alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            void *tmp = allocer_.alloc(common::upper_align(sz, sizeof(void*)));
            if (NULL != tmp)
            {
              ret = (void*)common::upper_align((int64_t)tmp, sizeof(void*));
            }
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *undo_node_alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            void *tmp = undo_allocer_.alloc(common::upper_align(sz, sizeof(void*)));
            if (NULL != tmp)
            {
              ret = (void*)common::upper_align((int64_t)tmp, sizeof(void*));
            }
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *tevalue_alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            void *tmp = tevalue_allocer_.alloc(common::upper_align(sz, sizeof(void*)));
            if (NULL != tmp)
            {
              ret = (void*)common::upper_align((int64_t)tmp, sizeof(void*));
            }
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *btree_engine_alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            ret = btree_engine_allocer_.alloc(sz);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *hash_engine_alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            ret = hash_engine_allocer_.alloc(sz);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void clear(const bool slow=true)
        {
          string_buf_.clear(slow);
          allocer_.clear(slow);
          undo_allocer_.clear(slow);
          tevalue_allocer_.clear(slow);
          btree_engine_allocer_.clear(slow);
          hash_engine_allocer_.clear(slow);
        };
      public:
        bool mem_over_limit() const
        {
          bool bret = false;
          int64_t table_total = total() + extern_mem_total_ptr_->get_extern_mem_total();
          int64_t table_available = total_limit_ - table_total;
          int64_t table_available_warn_size = get_table_available_warn_size();
          if (table_available_warn_size >= table_available)
          {
            ups_available_memory_warn_callback(table_available);
          }
          bret = (table_total >= total_limit_);
          if (bret
              && tc_is_replaying_log())
          {
            bret = (table_total >= (total_limit_ + (REPLAY_MEMTABLE_RESERVE_SIZE_GB << 30)));
          }
          return bret;
        };
        int64_t used() const
        {
          return block_allocator_.get_allocated();
        };
        int64_t total() const
        {
          return block_allocator_.get_allocated();
        };
        void set_extern_mem_total(IExternMemTotal *extern_mem_total_ptr)
        {
          if (NULL != extern_mem_total_ptr)
          {
            extern_mem_total_ptr_ = extern_mem_total_ptr;
          }
        };
        IExternMemTotal *get_extern_mem_total()
        {
          return extern_mem_total_ptr_;
        };
        int64_t set_total_limit(const int64_t limit)
        {
          if (0 < limit)
          {
            total_limit_ = limit;
            block_allocator_.set_limit(limit + BLOCK_ALLOCATOR_LIMIT_DELTA);
          }
          return total_limit_;
        };
        int64_t get_total_limit() const
        {
          return total_limit_;
        };
        void log_info() const
        {
          TBSYS_LOG(INFO,  "MemTank report used=%ld total=%ld extern=%ld limit=%ld "
                    "string_buf_used=%ld "
                    "allocer_used=%ld "
                    "undo_allocer_used=%ld "
                    "tevalue_allocer_used=%ld "
                    "btree_engine_allocer_used=%ld "
                    "hash_engine_allocer_used=%ld ",
                    used(), total(), extern_mem_total_ptr_->get_extern_mem_total(), get_total_limit(),
                    string_buf_.get_alloc_size(),
                    allocer_.get_alloc_size(),
                    undo_allocer_.get_alloc_size(),
                    tevalue_allocer_.get_alloc_size(),
                    btree_engine_allocer_.get_alloc_size(),
                    hash_engine_allocer_.get_alloc_size());
        };
      private:
        void log_error_(const char *caller) const
        {
          TBSYS_LOG(ERROR,  "[%s] MemTank report used=%ld total=%ld extern=%ld limit=%ld"
                    "string_buf_used=%ld "
                    "allocer_used=%ld "
                    "undo_allocer_used=%ld "
                    "tevalue_allocer_used=%ld "
                    "btree_engine_allocer_used=%ld "
                    "hash_engine_allocer_used=%ld ",
                    caller, used(), total(), extern_mem_total_ptr_->get_extern_mem_total(), get_total_limit(),
                    string_buf_.get_alloc_size(),
                    allocer_.get_alloc_size(),
                    undo_allocer_.get_alloc_size(),
                    tevalue_allocer_.get_alloc_size(),
                    btree_engine_allocer_.get_alloc_size(),
                    hash_engine_allocer_.get_alloc_size());
        };
      private:
        int64_t total_limit_;
        common::DefaultBlockAllocator block_allocator_;
        Allocator string_buf_;
        Allocator allocer_;
        Allocator undo_allocer_;
        Allocator tevalue_allocer_;
        Allocator btree_engine_allocer_;
        Allocator hash_engine_allocer_;
        IExternMemTotal *extern_mem_total_ptr_;
        DefaultExternMemTotal default_extern_mem_total_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_MEMTANK_H_
