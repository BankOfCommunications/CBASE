////===================================================================
 //
 // ob_id_map.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2012, 2013 Taobao.com, Inc.
 //
 // Created on 2012-03-23 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_COMMON_ID_MAP_H_
#define  OCEANBASE_COMMON_ID_MAP_H_
#include "ob_define.h"
#include "ob_malloc.h"
#include "ob_fixed_queue.h"
#include "ob_spin_rwlock.h"
#include "utility.h"

#define IDMAP_INVALID_ID 0
namespace oceanbase
{
  namespace common
  {
    enum FetchMod
    {
      FM_SHARED = 0,
      FM_MUTEX_BLOCK = 1,
      FM_MUTEX_NONBLOCK = 2,
    };

    template <class T,
              int64_t TSI_HOLD_NUM = 4>
    class ObTCIDFreeList
    {
      typedef ObFixedQueue<T> FreeList;
      public:
        ObTCIDFreeList() : free2glist_(false) {};
        ~ObTCIDFreeList() {};
      public:
        int init(const int64_t max_num)
        {
          return gfree_list_.init(max_num);
        };
        void destroy()
        {
          return gfree_list_.destroy();
        };
      public:
        int push(T *ptr)
        {
          int ret = OB_SUCCESS;
          FreeList &tsi = tsi_array_.get_tsi();
          if (!tsi.inited())
          {
            tsi.init(TSI_HOLD_NUM);
          }
          if (free2glist_
              || OB_SUCCESS != tsi.push(ptr))
          {
            ret = gfree_list_.push(ptr);
            if (TSI_HOLD_NUM < gfree_list_.get_total())
            {
              free2glist_ = false;
            }
          }
          return ret;
        };
        int pop(T *&ptr)
        {
          int ret = OB_SUCCESS;
          FreeList &tsi = tsi_array_.get_tsi();
          if (!tsi.inited())
          {
            tsi.init(TSI_HOLD_NUM);
          }
          if (OB_SUCCESS != tsi.pop(ptr))
          {
            ret = gfree_list_.pop(ptr);
            if (TSI_HOLD_NUM >= gfree_list_.get_total())
            {
              free2glist_ = true;
            }
          }
          if (OB_SUCCESS != ret)
          {
            static __thread uint64_t i = 0;
            for (uint64_t j = 0; j < tsi_array_.get_thread_num(); j++, i++)
            {
              if (i >= tsi_array_.get_thread_num())
              {
                i = 0;
              }
              if (OB_SUCCESS == (ret = tsi_array_.at(i).pop(ptr)))
              {
                break;
              }
            }
          }
          return ret;
        };
        inline int64_t get_total() const
        {
          int64_t ret = gfree_list_.get_total();
          for (uint64_t i = 0; i < tsi_array_.get_thread_num(); i++)
          {
            ret += tsi_array_.at(i).get_total();
          }
          return ret;
        };
        inline int64_t get_free() const
        {
          int64_t ret = gfree_list_.get_free();
          for (uint64_t i = 0; i < tsi_array_.get_thread_num(); i++)
          {
            ret += tsi_array_.at(i).get_free();
          }
          return ret;
        };
      private:
        FreeList gfree_list_;
        ObTSIArray<FreeList> tsi_array_;
        volatile bool free2glist_;
    };

    template <typename T, typename ID_TYPE = uint64_t>
    class ObIDMap
    {
      enum Stat
      {
        ST_FREE = 0,
        ST_USING = 1,
      };
      struct Node
      {
        common::SpinRWLock lock;
        volatile ID_TYPE id;
        volatile Stat stat;
        T *data;
      };
      public:
        ObIDMap();
        ~ObIDMap();
      public:
        int init(const ID_TYPE num);
        void destroy();
      public:
        int assign(T *value, ID_TYPE &id);
        int get(const ID_TYPE id, T *&value) const;
        T *fetch(const ID_TYPE id, const FetchMod mod = FM_SHARED);
        void revert(const ID_TYPE id, const bool erase = false);
        void erase(const ID_TYPE id);
        int size() const;
      public:
        // callback::operator()(const ID_TYPE id)
        template <typename Callback>
        void traverse(Callback &cb) const
        {
          if (NULL != array_)
          {
            for (ID_TYPE i = 0; i < num_; i++)
            {
              if (ST_USING == array_[i].stat)
              {
                cb(array_[i].id);
              }
            }
          }
        };
      private:
        ID_TYPE num_;
        ID_TYPE overflow_limit_;
        Node *array_;
        //common::ObFixedQueue<Node> free_list_;
        ObTCIDFreeList<Node> free_list_;
    };

    inline int calc_clz(const uint32_t s)
    {
      return __builtin_clz(s);
    }

    inline int calc_clz(const uint64_t s)
    {
      return __builtin_clzl(s);
    }

    template <typename T, typename ID_TYPE>
    ObIDMap<T, ID_TYPE>::ObIDMap() : num_(1),
                                     overflow_limit_(0),
                                     array_(NULL),
                                     free_list_()
    {
    }

    template <typename T, typename ID_TYPE>
    ObIDMap<T, ID_TYPE>::~ObIDMap()
    {
      destroy();
    }

    template <typename T, typename ID_TYPE>
    int ObIDMap<T, ID_TYPE>::init(const ID_TYPE num)
    {
      int ret = common::OB_SUCCESS;
      if (NULL != array_)
      {
        TBSYS_LOG(WARN, "have inited");
        ret = common::OB_INIT_TWICE;
      }
      else if (0 >= num)
      {
        TBSYS_LOG(WARN, "invalid param num=%lu", (uint64_t)num);
        ret = common::OB_INVALID_ARGUMENT;
      }
      else if (NULL == (array_ = (Node*)common::ob_malloc((num + 1) * sizeof(Node), ObModIds::ID_MAP)))
      {
        TBSYS_LOG(WARN, "malloc array fail num=%lu", (uint64_t)(num + 1));
        ret = common::OB_MEM_OVERFLOW;
      }
      else if (common::OB_SUCCESS != (ret = free_list_.init(num)))
      {
        TBSYS_LOG(WARN, "free list init fail ret=%d", ret);
      }
      else
      {
        memset(array_, 0, (num + 1) * sizeof(Node));
        for (ID_TYPE i = 0; i < (num + 1); i++)
        {
          array_[i].id = i;
          if (0 != i
              && common::OB_SUCCESS != (ret = free_list_.push(&(array_[i]))))
          {
            TBSYS_LOG(WARN, "push to free list fail ret=%d i=%lu", ret, (uint64_t)i);
            break;
          }
        }
        num_ = num + 1;
        overflow_limit_ = (num_ << (calc_clz(num_) - 1)); // only use 31/63-low-bit, make sure integer multiples of num_
      }
      if (common::OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    template <typename T, typename ID_TYPE>
    void ObIDMap<T, ID_TYPE>::destroy()
    {
      free_list_.destroy();
      if (NULL != array_)
      {
        common::ob_free(array_);
        array_ = NULL;
      }
      num_ = 1;
    }

    template <typename T, typename ID_TYPE>
    int ObIDMap<T, ID_TYPE>::assign(T *value, ID_TYPE &id)
    {
      int ret = common::OB_SUCCESS;
      Node *node = NULL;
      if (NULL == array_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = common::OB_NOT_INIT;
      }
      else if (common::OB_SUCCESS != (ret = free_list_.pop(node))
              || NULL == node)
      {
        TBSYS_LOG(WARN, "no more id free list size=%ld", free_list_.get_total());
        ret = (common::OB_SUCCESS == ret) ? common::OB_MEM_OVERFLOW : ret;
      }
      else
      {
        id = node->id;
        node->data = value;
        node->stat = ST_USING;
      }
      return ret;
    }

    template <typename T, typename ID_TYPE>
    int ObIDMap<T, ID_TYPE>::get(const ID_TYPE id, T *&value) const
    {
      int ret = common::OB_SUCCESS;
      ID_TYPE pos = id % num_;
      if (NULL == array_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = common::OB_NOT_INIT;
      }
      else
      {
        T *retv = NULL;
        if (id != array_[pos].id
            || ST_USING != array_[pos].stat)
        {
          ret = common::OB_ENTRY_NOT_EXIST;
        }
        else
        {
          retv = array_[pos].data;
          // double check
          if (id != array_[pos].id
              || ST_USING != array_[pos].stat)
          {
            ret = common::OB_ENTRY_NOT_EXIST;
            retv = NULL;
          }
          else
          {
            value = retv;
          }
        }
      }
      return ret;
    }

    template <typename T, typename ID_TYPE>
    T *ObIDMap<T, ID_TYPE>::fetch(const ID_TYPE id, const FetchMod mod)
    {
      T *ret = NULL;
      if (NULL != array_)
      {
        ID_TYPE pos = id % num_;
        bool lock_succ = false;
        if (FM_SHARED == mod)
        {
          array_[pos].lock.rdlock();
          lock_succ = true;
        }
        else if (FM_MUTEX_BLOCK == mod)
        {
          array_[pos].lock.wrlock();
          lock_succ = true;
        }
        else if (FM_MUTEX_NONBLOCK == mod)
        {
          if (array_[pos].lock.try_wrlock())
          {
            lock_succ = true;
          }
        }

        if (!lock_succ)
        {
          // do nothing
        }
        else if (id == array_[pos].id
                && ST_USING == array_[pos].stat)
        {
          ret = array_[pos].data;
        }
        else
        {
          array_[pos].lock.unlock();
        }
      }
      return ret;
    }

    template <typename T, typename ID_TYPE>
    void ObIDMap<T, ID_TYPE>::revert(const ID_TYPE id, const bool erase)
    {
      if (NULL != array_)
      {
        ID_TYPE pos = id % num_;
        if (erase
            && id == array_[pos].id
            && ST_USING == array_[pos].stat)
        {
          if ((overflow_limit_ - num_) < array_[pos].id)
          {
            array_[pos].id = pos;
          }
          else
          {
            array_[pos].id += num_;
          }
          array_[pos].stat = ST_FREE;
          array_[pos].data = NULL;
          int tmp_ret = common::OB_SUCCESS;
          if (common::OB_SUCCESS != (tmp_ret = free_list_.push(&(array_[pos]))))
          {
            TBSYS_LOG(ERROR, "push to free list fail ret=%d free list size=%ld", tmp_ret, free_list_.get_total());
          }
        }
        array_[pos].lock.unlock();
      }
    }

    template <typename T, typename ID_TYPE>
    void ObIDMap<T, ID_TYPE>::erase(const ID_TYPE id)
    {
      if (NULL != array_)
      {
        ID_TYPE pos = id % num_;
        common::SpinWLockGuard guard(array_[pos].lock);
        if (id == array_[pos].id
            && ST_USING == array_[pos].stat)
        {
          if ((overflow_limit_ - num_) < array_[pos].id)
          {
            array_[pos].id = pos;
          }
          else
          {
            array_[pos].id += num_;
          }
          array_[pos].stat = ST_FREE;
          array_[pos].data = NULL;
          int tmp_ret = common::OB_SUCCESS;
          if (common::OB_SUCCESS != (tmp_ret = free_list_.push(&(array_[pos]))))
          {
            TBSYS_LOG(ERROR, "push to free list fail ret=%d free list size=%ld", tmp_ret, free_list_.get_total());
          }
        }
      }
    }

    template <typename T, typename ID_TYPE>
    int ObIDMap<T, ID_TYPE>::size() const
    {
      return static_cast<int>(num_ - 1 - free_list_.get_total());
    }
  } // namespace common
} // namespace oceanbase

#endif //OCEANBASE_UPDATESERVER_ID_MAP_H_
