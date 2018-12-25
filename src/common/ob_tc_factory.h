/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tc_factory.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TC_FACTORY_H
#define _OB_TC_FACTORY_H 1
#include "common/dlist.h"
#include "common/ob_define.h"
#include "common/utility.h"
#include "sql/ob_phy_operator_type.h"
#include "ob_global_factory.h"
namespace oceanbase
{
  namespace common
  {
    // T should be a derived class of common::DLink
    template<typename T>
    class ObTCFreeList
    {
      public:
        ObTCFreeList()
          :magic_(0xFBEE1127), max_length_(1), low_water_(0), overlimit_times_(0){}
        virtual ~ObTCFreeList(){};

        void push(T *ptr) {list_.add_last(ptr);};
        T* pop()
        {
          T* ret = dynamic_cast<T*>(list_.remove_last());
          if (list_.get_size() < low_water_)
          {
            low_water_ = list_.get_size();
          }
          return ret;
        };

        void push_range(common::DList &range) {list_.push_range(range);};
        void pop_range(int32_t num, common::DList &range)
        {
          list_.pop_range(num, range);
          if (list_.get_size() < low_water_)
          {
            low_water_ = list_.get_size();
          }
        };

        bool is_empty() const {return list_.is_empty();};
        int32_t get_length() const {return list_.get_size();};

        int32_t get_max_length() const {return max_length_;};
        void inc_max_length() {++max_length_;};
        void set_max_length(int32_t max_length) {max_length_ = max_length;};

        int32_t get_low_water() const {return low_water_;};
        void reset_low_water() {low_water_ = list_.get_size();};

        int32_t get_overlimit_times() const {return overlimit_times_;};
        void set_overlimit_times(int32_t times) {overlimit_times_ = times;};
        void inc_overlimit_times() {++overlimit_times_;};
      private:
        // types and constants
      private:
        // disallow copy
        ObTCFreeList(const ObTCFreeList &other);
        ObTCFreeList& operator=(const ObTCFreeList &other);
        // function members
      private:
        // data members
        int32_t magic_;
        int32_t max_length_;
        int32_t low_water_;
        int32_t overlimit_times_;
        common::DList list_;
    };

    template <typename T>
    void ob_tc_factory_callback_on_put(T *obj, BoolType<true>)
    {
      obj->reset();
    }

    template <typename T>
    void ob_tc_factory_callback_on_put(T *obj, BoolType<false>)
    {
      UNUSED(obj);
    }

    // T should be a derived class of common::DLink
    template <typename T,
              int64_t MAX_CLASS_NUM,
              int32_t MODID,
              int64_t MEM_LIMIT = 1 << 24/*16MB*/,
              int32_t MAX_FREE_LIST_LENGTH = 1024>
    class ObTCFactory
    {
      public:
        static ObTCFactory* get_instance();

        T* get(int32_t type_id);
        void put(T* ptr);

        void stat();
      private:
        // types and constants
        typedef ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH> self_t;
        typedef ObGlobalFactory<T, MAX_CLASS_NUM, MODID> global_factory_t;
        typedef ObTCFreeList<T> freelist_t;
        static const int32_t MAX_OVERLIMIT_TIMES = 3;
      private:
        // disallow copy
        ObTCFactory(const ObTCFactory &other);
        ObTCFactory& operator=(const ObTCFactory &other);
        // function members
        ObTCFactory();
        virtual ~ObTCFactory(){};
        T* get_from_global_factory(int32_t type_id, int32_t obj_size);
        void list_too_long(freelist_t &list, int32_t type_id, int32_t obj_size);
        void garbage_collect();
      private:
        static volatile self_t *TC_FACTORY_LIST_HEAD;
      private:
        // data members
        freelist_t freelists_[MAX_CLASS_NUM];
        int64_t mem_size_;
        int64_t get_count_[MAX_CLASS_NUM];
        int64_t put_count_[MAX_CLASS_NUM];
        volatile self_t *next_;
    };

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    volatile ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>* ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::TC_FACTORY_LIST_HEAD = NULL;

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::ObTCFactory()
      :mem_size_(0),
       next_(NULL)
    {
      memset(get_count_, 0, sizeof(get_count_));
      memset(put_count_, 0, sizeof(put_count_));
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>* ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get_instance()
    {
      static __thread self_t *instance = NULL;
      if (OB_UNLIKELY(NULL == instance))
      {
        instance = new(std::nothrow) self_t();
        if (NULL == instance)
        {
          TBSYS_LOG(ERROR, "failed to init tc_factory");
          abort();
        }
        else
        {
          // link all the tc_factory of all threads
          volatile self_t *old_v = NULL;
          while(true)
          {
            old_v = TC_FACTORY_LIST_HEAD;
            instance->next_ = old_v;
            if (__sync_bool_compare_and_swap(&TC_FACTORY_LIST_HEAD, old_v, instance))
            {
              break;
            }
          }
        }
      }
      return instance;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    T* ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get(int32_t type_id)
    {
      T* ret = NULL;
      if (0 > type_id
          || type_id >= MAX_CLASS_NUM)
      {
        TBSYS_LOG(ERROR, "invalid class type id, id=%d", type_id);
      }
      else
      {
        global_factory_t *gfactory = global_factory_t::get_instance();
        const int32_t obj_size = gfactory->get_obj_size(type_id);
        //OB_PHY_OP_INC(type_id);
        if (freelists_[type_id].is_empty())
        {
          // no item in the local free list
          ret = get_from_global_factory(type_id, obj_size);
        }
        else
        {
          ret = freelists_[type_id].pop();
          mem_size_ -= obj_size;
        }
        if (OB_LIKELY(NULL != ret))
        {
          ++get_count_[type_id];
        }
      }
      return ret;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    T* ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get_from_global_factory(int32_t type_id, int32_t obj_size)
    {
      T* ret = NULL;
      freelist_t &list = freelists_[type_id];
      OB_ASSERT(list.is_empty());
      global_factory_t *gfactory = global_factory_t::get_instance();
      const int32_t batch_count = gfactory->get_batch_count(type_id);
      const int32_t move_num = std::min(batch_count, list.get_max_length());
      common::DList range;
      gfactory->get_objs(type_id, move_num, range);
      ret = dynamic_cast<T*>(range.remove_last());

      if (range.get_size() > 0)
      {
        mem_size_ += range.get_size() * obj_size;
        list.push_range(range);
      }
      // Increase max length slowly up to batch_count.
      if (list.get_max_length() < batch_count)
      {
        list.inc_max_length();
      }
      else
      {
        int32_t new_length = std::min(list.get_max_length() + batch_count, MAX_FREE_LIST_LENGTH);
        new_length -= new_length % batch_count;
        OB_ASSERT(new_length % batch_count == 0);
        list.set_max_length(new_length);
      }
      return ret;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    void ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::put(T* ptr)
    {
      if (OB_LIKELY(NULL != ptr))
      {
        global_factory_t *gfactory = global_factory_t::get_instance();
        const int32_t type_id = static_cast<const int32_t>(ptr->get_type());
        const int32_t obj_size = gfactory->get_obj_size(type_id);
        //OB_PHY_OP_DEC(type_id);
        freelist_t &list = freelists_[type_id];
        ob_tc_factory_callback_on_put(ptr, BoolType<HAS_MEMBER(T, void (T::*)(), reset)>());
        list.push(ptr);
        mem_size_ += obj_size;
        if (list.get_length() > list.get_max_length())
        {
          //TBSYS_LOG(ERROR, "list too long, length=%d max_length=%d", list.get_length(), list.get_max_length());
          list_too_long(list, type_id, obj_size);
        }
        if (mem_size_ > MEM_LIMIT)
        {
          //TBSYS_LOG(ERROR, "cache too large, mem_size_=%ld", mem_size_);
          garbage_collect();
        }
        ++put_count_[type_id];
      }
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    void ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::list_too_long(freelist_t &list, int32_t type_id, int32_t obj_size)
    {
      global_factory_t *gfactory = global_factory_t::get_instance();
      const int32_t batch_count = gfactory->get_batch_count(type_id);
      // release batch_count objs to the global factory
      common::DList range;
      list.pop_range(batch_count, range);
      mem_size_ -= obj_size * range.get_size();
      gfactory->put_objs(type_id, range);

      if (list.get_max_length() < batch_count)
      {
        list.inc_max_length();
      }
      else if (list.get_max_length() > batch_count)
      {
        // shrink the list if we consistently go over max_length
        list.inc_overlimit_times();
        if (list.get_overlimit_times() >= MAX_OVERLIMIT_TIMES)
        {
          list.set_max_length(list.get_max_length() - batch_count);
          list.set_overlimit_times(0);
        }
      }
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    void ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::garbage_collect()
    {
      global_factory_t *gfactory = global_factory_t::get_instance();
      for (int32_t type_id = 0; type_id < MAX_CLASS_NUM; ++type_id)
      {
        const int32_t obj_size = gfactory->get_obj_size(type_id);
        if (0 >= obj_size)
        {
          // invalid class type, skip
          continue;
        }
        freelist_t &list = freelists_[type_id];
        const int32_t low_water = list.get_low_water();
        if (0 < low_water)
        {
          const int32_t drop_num = (low_water > 1) ? (low_water/2) : 1;

          // release drop_num objs to the global factory
          const int32_t batch_count = gfactory->get_batch_count(type_id);
          common::DList range;
          list.pop_range(drop_num, range);
          mem_size_ -= obj_size * range.get_size();
          gfactory->put_objs(type_id, range);

          // shrink the list
          if (list.get_max_length() > batch_count)
          {
            list.set_max_length(list.get_max_length() - batch_count);
          }
        }
        list.reset_low_water();
      } // end for
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID, int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
    void ObTCFactory<T, MAX_CLASS_NUM, MODID, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::stat()
    {
      self_t *tc_factory = const_cast<self_t*>(TC_FACTORY_LIST_HEAD);
      int32_t i = 0;
      int64_t total_mem_size = 0;
      int64_t total_length = 0;
      int64_t total_max_length = 0;
      int32_t length = 0;
      int32_t max_length = 0;
      int32_t low_water = 0;
      int32_t overlimit_times = 0;
      int64_t class_total_length[MAX_CLASS_NUM];
      int64_t class_total_max_length[MAX_CLASS_NUM];
      int64_t allocated_count[MAX_CLASS_NUM];
      memset(class_total_length, 0, sizeof(class_total_length));
      memset(class_total_max_length, 0, sizeof(class_total_max_length));
      memset(allocated_count, 0, sizeof(allocated_count));
      while (NULL != tc_factory)
      {
        TBSYS_LOG(INFO, "[TCFACTORY_STAT] thread=%d type_id=ALL mem_size=%ld", i, tc_factory->mem_size_);
        total_mem_size += tc_factory->mem_size_;
        for (int32_t type_id = 0; type_id < MAX_CLASS_NUM; ++type_id)
        {
          length = tc_factory->freelists_[type_id].get_length();
          max_length = tc_factory->freelists_[type_id].get_max_length();
          low_water = tc_factory->freelists_[type_id].get_low_water();
          overlimit_times = tc_factory->freelists_[type_id].get_overlimit_times();

          total_length += length;
          total_max_length += max_length;

          class_total_length[type_id] += length;
          class_total_max_length[type_id] += max_length;

          allocated_count[type_id] += tc_factory->get_count_[type_id] - tc_factory->put_count_[type_id];
          TBSYS_LOG(INFO, "[TCFACTORY_STAT] thread=%d type_id=%d length=%d "
                    "max_length=%d low_water=%d overlimit_times=%d get_count=%ld put_count=%ld",
                    i, type_id, length, max_length, low_water, overlimit_times,
                    tc_factory->get_count_[type_id], tc_factory->put_count_[type_id]);
        }
        tc_factory = const_cast<self_t*>(tc_factory->next_);
        ++i;
      } // end while
      for (int32_t type_id = 0; type_id < MAX_CLASS_NUM; ++type_id)
      {
        TBSYS_LOG(INFO, "[TCFACTORY_STAT] thread=ALL type_id=%d length=%ld max_length=%ld allocated=%ld",
                  type_id, class_total_length[type_id],
                  class_total_max_length[type_id], allocated_count[type_id]);
      }
      TBSYS_LOG(INFO, "[TCFACTORY_STAT] thread=ALL type_id=ALL mem_size=%ld length=%ld max_length=%ld",
                total_mem_size, total_length, total_max_length);
    }
  } // end namespace common
} // end namespace oceanbase
#endif /* _OB_TC_FACTORY_H */
