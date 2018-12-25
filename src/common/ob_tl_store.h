/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - A thread-local object store
 */
#ifndef OCEANBASE_COMMON_OB_TL_STORE_H__
#define OCEANBASE_COMMON_OB_TL_STORE_H__

#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <vector>
#include <Mutex.h>
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    inline int get_tc_tid()
    {
      static __thread int tid = -1;
      if (OB_UNLIKELY(tid == -1))
      {
        tid = static_cast<int>(syscall(__NR_gettid));
      }
      return tid;
    }

    class DefaultThreadStoreAlloc
    {
      public:
        inline void * alloc(const int64_t sz) { return ::malloc(sz); }
        inline void free(void *p) { ::free(p); }
    };

    template <class Type>
    class DefaultInitializer
    {
      public:
        void operator()(void *ptr)
        {
          new (ptr) Type();
        }
    };

    template <class Type, class Initializer = DefaultInitializer<Type>,
              class Alloc = DefaultThreadStoreAlloc>
    class ObTlStore
    {
      public:
        typedef ObTlStore<Type, Initializer, Alloc> TSelf;
      public:
        struct Item
        {
          TSelf * self;
          int     thread_id;
          Type    obj;
        };

        class SyncVector
        {
          public:
            typedef std::vector<Item *>        PtrArray;
          public:
            inline int      push_back          (Item * ptr);

            template<class Function>
            inline int      for_each           (Function & f) const;
            inline void     destroy            ();
          private:
            PtrArray ptr_array_;
            tbutil::Mutex mutex_;
        };

        template<class Function>
        class ObjPtrAdapter
        {
          public:
            ObjPtrAdapter                      (Function & f)
              : f_(f)
            {
            }
            void operator()                    (const Item * item)
            {
              if (NULL != item)
              {
                f_(&item->obj);
              }
            }
            void operator()                    (Item * item)
            {
              if (NULL != item)
              {
                f_(&item->obj);
              }
            }
          protected:
            Function & f_;
        };

      public:
        static const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
      public:
        ObTlStore                 (Alloc &alloc);

        ObTlStore                 (Initializer & initializer, Alloc &alloc);

        ObTlStore                 (Initializer & initializer);

        ObTlStore                 ();

        virtual ~ObTlStore        ();

        static void destroy_object(Item * item);

        int32_t  init             ();

        void     destroy          ();

        Type *   get              ();

        template<class Function>
        int      for_each_obj_ptr (Function & f) const;

        template<class Function>
        int      for_each_item_ptr(Function & f) const;

      private:
        pthread_key_t key_;
        DefaultInitializer<Type> default_initializer_;
        DefaultThreadStoreAlloc default_alloc_;
        Initializer & initializer_;
        SyncVector ptr_array_;
        Alloc &alloc_;
        bool init_;
    };

    template <class Type, class Initializer, class Alloc>
    int ObTlStore<Type, Initializer, Alloc>::SyncVector::push_back(Item * ptr)
    {
      int ret = OB_SUCCESS;
      mutex_.lock();
      try
      {
        ptr_array_.push_back(ptr);
      }
      catch (std::bad_alloc)
      {
        TBSYS_LOG(ERROR, "memory is not enough when push_back");
        ret = OB_ERR_UNEXPECTED;
      }
      mutex_.unlock();
      return ret;
    }

    template <class Type, class Initializer, class Alloc>
    template<class Function>
    int ObTlStore<Type, Initializer, Alloc>::SyncVector::for_each(Function & f) const
    {
      int ret = OB_SUCCESS;
      mutex_.lock();
      std::for_each(ptr_array_.begin(), ptr_array_.end(), f);
      mutex_.unlock();
      return ret;
    }

    template <class Type, class Initializer, class Alloc>
    void ObTlStore<Type, Initializer, Alloc>::SyncVector::destroy()
    {
      mutex_.lock();
      ptr_array_.clear();
      mutex_.unlock();
    }

    template <class Type, class Initializer, class Alloc>
    ObTlStore<Type, Initializer, Alloc>::ObTlStore(Alloc &alloc)
      : key_(INVALID_THREAD_KEY), initializer_(default_initializer_),
        alloc_(alloc), init_(false)
    {
    }

    template <class Type, class Initializer, class Alloc>
    ObTlStore<Type, Initializer, Alloc>::ObTlStore(
        Initializer & initializer, Alloc &alloc)
      : key_(INVALID_THREAD_KEY), initializer_(initializer),
        alloc_(alloc), init_(false)
    {
    }

    template <class Type, class Initializer, class Alloc>
    ObTlStore<Type, Initializer, Alloc>::ObTlStore(
        Initializer & initializer)
      : key_(INVALID_THREAD_KEY), initializer_(initializer),
        alloc_(default_alloc_), init_(false)
    {
    }

    template <class Type, class Initializer, class Alloc>
    ObTlStore<Type, Initializer, Alloc>::ObTlStore()
      : key_(INVALID_THREAD_KEY), initializer_(default_initializer_),
        alloc_(default_alloc_), init_(false)
    {
    }

    template <class Type, class Initializer, class Alloc>
    ObTlStore<Type, Initializer, Alloc>::~ObTlStore()
    {
      destroy();
    }

    template <class Type, class Initializer, class Alloc>
    void ObTlStore<Type, Initializer, Alloc>::destroy_object(Item * item)
    {
      if (NULL != item)
      {
        item->obj.~Type();
        item->self->alloc_.free(item);
      }
    }

    template <class Type, class Initializer, class Alloc>
    int32_t ObTlStore<Type, Initializer, Alloc>::init()
    {
      int32_t ret = OB_SUCCESS;
      if (init_)
      {
        TBSYS_LOG(ERROR, "ObTlStore has already initialized.");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        if (INVALID_THREAD_KEY == key_)
        {
          int err = pthread_key_create(&key_, NULL);
          if (0 != err)
          {
            TBSYS_LOG(ERROR, "pthread_key_create error: %s",
                strerror(errno));
            if (errno == ENOMEM)
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
            }
          }
          else
          {
            init_ = true;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "key_ should be INVALID_THREAD_KEY");
          ret = OB_ERR_UNEXPECTED;
        }
      }
      return ret;
    }

    template <class Type, class Initializer, class Alloc>
    void ObTlStore<Type, Initializer, Alloc>::destroy()
    {
      if (init_)
      {
        if (INVALID_THREAD_KEY != key_)
        {
          //void* mem = pthread_getspecific(key_);
          //if (NULL != mem) destroy_object(mem);
          pthread_key_delete(key_);
          key_ = INVALID_THREAD_KEY;
        }
        for_each_item_ptr(destroy_object);
        ptr_array_.destroy();
        init_ = false;
      }
    }

    template <class Type, class Initializer, class Alloc>
    Type * ObTlStore<Type, Initializer, Alloc>::get()
    {
      Type * ret = NULL;
      if (OB_UNLIKELY(!init_))
      {
        TBSYS_LOG(ERROR, "ObTlStore has not been initialized");
      }
      else if (INVALID_THREAD_KEY == key_)
      {
        TBSYS_LOG(ERROR, "ObTlStore thread key is invalid");
      }
      else
      {
        Item * item = reinterpret_cast<Item *>(pthread_getspecific(key_));
        if (NULL == item)
        {
          item = reinterpret_cast<Item *>(alloc_.alloc(sizeof(Item)));
          if (NULL != item)
          {
            if (0 != pthread_setspecific(key_, item))
            {
              alloc_.free(item);
              item = NULL;
            }
            else
            {
              initializer_(&item->obj);
              item->self = this;
              item->thread_id = get_tc_tid();
              ptr_array_.push_back(item);
            }
          }
        }
        if (NULL != item)
        {
          ret = &item->obj;
        }
      }
      return ret;
    }

    template <class Type, class Initializer, class Alloc>
    template<class Function>
    int ObTlStore<Type, Initializer, Alloc>::for_each_obj_ptr(Function & f) const
    {
      if (OB_UNLIKELY(!init_))
      {
        TBSYS_LOG(ERROR, "ObTlStore has not been initialized");
        return OB_ERR_UNEXPECTED;
      }
      else
      {
        ObjPtrAdapter<Function> opa(f);
        return ptr_array_.for_each(opa);
      }
    }

    template <class Type, class Initializer, class Alloc>
    template<class Function>
    int ObTlStore<Type, Initializer, Alloc>::for_each_item_ptr(Function & f) const
    {
      if (OB_UNLIKELY(!init_))
      {
        TBSYS_LOG(ERROR, "ObTlStore has not been initialized");
        return OB_ERR_UNEXPECTED;
      }
      else
      {
        return ptr_array_.for_each(f);
      }
    }

  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_TL_STORE_H__
