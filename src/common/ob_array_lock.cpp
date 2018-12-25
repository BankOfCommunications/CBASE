#include "ob_define.h"
#include "ob_atomic.h"
#include "ob_array_lock.h"

using namespace oceanbase::common;

ObLockHolder::ObLockHolder()
{
  lock_size_ = 0;
  lock_holder_ = NULL;
}


ObLockHolder::~ObLockHolder()
{
  if (lock_size_ != 0)
  {
    delete []lock_holder_;
    lock_holder_ = NULL;
  }
}

uint64_t ObLockHolder::size(void) const
{
  return lock_size_;
}

int ObLockHolder::init(const uint64_t size)
{
  int ret = OB_SUCCESS;
  if (size <= 0)
  {
    TBSYS_LOG(WARN, "check input lock size failed:size[%lu]", size);
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((lock_size_ != 0) && (lock_size_ != size))
  {
    TBSYS_LOG(WARN, "check init lock size twice:old[%lu], new[%lu]", lock_size_, size);
    ret = OB_INIT_TWICE;
  }
  else if (lock_size_ != size)
  {
    lock_holder_ = new(std::nothrow) tbsys::CThreadMutex[size];
    if (lock_holder_ != NULL)
    {
      lock_size_ = size;
    }
    else
    {
      TBSYS_LOG(ERROR, "malloc the mutex failed:holder[%p], size[%lu]", lock_holder_, size);
      ret = OB_ERROR;
    }
  }
  return ret;
}

ObArrayLock::ObArrayLock()
{
}

ObArrayLock::~ObArrayLock()
{
}

tbsys::CThreadMutex * ObArrayLock::acquire_lock(const uint64_t id)
{
   tbsys::CThreadMutex * ret = NULL;
   if (id < lock_size_)
   {
      ret = &lock_holder_[id];
   }
   else
   {
     TBSYS_LOG(WARN, "acquire lock id failed:holder[%p], size[%lu], id[%lu]", lock_holder_, lock_size_, id);
   }
   return ret;
}

ObSequenceLock::ObSequenceLock()
{
  lock_index_ = 0;
}

ObSequenceLock::~ObSequenceLock()
{
}

tbsys::CThreadMutex * ObSequenceLock::acquire_lock(void)
{
  uint64_t index = 0;
  return acquire_lock(index);
}

tbsys::CThreadMutex * ObSequenceLock::acquire_lock(uint64_t & index)
{
  tbsys::CThreadMutex * ret = NULL;
  if (lock_size_ > 0)
  {
    index = (atomic_inc(&lock_index_) - 1) % lock_size_;
    ret = &lock_holder_[index];
  }
  return ret;
}

