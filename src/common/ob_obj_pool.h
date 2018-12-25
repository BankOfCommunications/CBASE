#ifndef __COMMON_OB_OBJ_POOL__
#define __COMMON_OB_OBJ_POOL__

#include "common/ob_list.h"
#include "common/page_arena.h"
#include "common/ob_spin_lock.h"
#include "common/ob_tl_store.h"

namespace oceanbase
{
  namespace common
  {
    template <typename T>
      class ObObjPool
      {
        public:
          ObObjPool(int32_t mod);
          ~ObObjPool();
          int init();
          T* alloc();
          int free(T* obj);
        private:
          ObTlStore<common::ObList<T*> > list_;
          common::PageArena<char> allocator_;
          common::ObSpinLock allocator_lock_;
      };

    template <typename T>
    ObObjPool<T>::ObObjPool(int32_t mod)
    {
      allocator_.set_mod_id(mod);
    }
    template <typename T>
      int ObObjPool<T>::init()
      {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS != (ret = list_.init()))
        {
          TBSYS_LOG(WARN, "ObObjPool init failed, ret=%d", ret);
        }
        return ret;
      }
    template <typename T>
    ObObjPool<T>::~ObObjPool()
    {
    }
    template <typename T>
    T* ObObjPool<T>::alloc()
    {
      T *ret = NULL;
      int err = OB_SUCCESS;
      {
        if (list_.get() != NULL)
        {
          if (OB_SUCCESS == (err = list_.get()->pop_front(ret)))
          {
            // nothing
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        common::ObSpinLockGuard guard(allocator_lock_);
        void *buffer = allocator_.alloc(sizeof(T));
        if (NULL != buffer)
        {
          ret = new (buffer) T();
        }
      }
      return ret;
    }
    template <typename T>
    int ObObjPool<T>::free(T* obj)
    {
      int ret = OB_SUCCESS;
      if (list_.get() != NULL)
      {
        if (OB_SUCCESS != (ret = list_.get()->push_back(obj)))
        {
          TBSYS_LOG(INFO, "push to list failed free failed, ret=%d", ret);
        }
      }
      return ret;
    }
  }
}
#endif
