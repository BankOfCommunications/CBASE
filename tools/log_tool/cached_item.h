#ifndef __OB_LOG_TOOL_CACHED_ITEM_H__
#define __OB_LOG_TOOL_CACHED_ITEM_H__
#include "tbsys.h"
#include "pthread.h"

template<typename Item, typename Getter>
class CachedItem
{
  public:
    struct Guard
    {
      Item*& cfg_;
      CachedItem& src_;
      Guard(Item*& cfg, CachedItem& src): cfg_(cfg), src_(src) { src_.get(cfg_); }
      ~Guard() { src_.release(cfg_); }
    };
  public:
    CachedItem(): getter_(NULL), update_interval_us_(0), last_update_time_us_(0), seq_(0) {
      pthread_rwlockattr_t attr;
      if (0 != pthread_rwlockattr_init(&attr))
      {
        TBSYS_LOG(ERROR, "rwlockattr_init() failed");
      }
      else if (0 != pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
      {
        TBSYS_LOG(ERROR, "rwlockattr_setkind_np() failed");
      }
      else if (0 != pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE))
      {
        TBSYS_LOG(ERROR, "rwlockattr_setpshared() failed");
      }
      else if (0 != pthread_rwlock_init(&lock_, &attr))
      {
        TBSYS_LOG(ERROR, "pthread_rwlock_init() failed.");
        abort();
      }
    }
    ~CachedItem() {
      int err = 0;
      if (0 != (err = pthread_rwlock_trywrlock(&lock_)))
      {
        TBSYS_LOG(ERROR, "pthread_rwlock_trywrlock()=>%d", err);
        abort();
      }
      else
      {
        pthread_rwlock_wrlock(&lock_);
        pthread_rwlock_unlock(&lock_);
        pthread_rwlock_destroy(&lock_);
      }
    }
  public:
    int init(Getter* getter, int64_t update_interval_us){
      int err = OB_SUCCESS;
      if (NULL == getter || 0 >= update_interval_us)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        getter_ = getter;
        update_interval_us_ = update_interval_us;
        TBSYS_LOG(INFO, "init(update_interval_us=%ld)", update_interval_us);
      }
      return err;
    }
    
    int get(Item*& item){
      int err = OB_SUCCESS;
      if (NULL == getter_)
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = update()))
      {
        TBSYS_LOG(ERROR, "update()=>%d", err);
      }
      else
      {
        pthread_rwlock_rdlock(&lock_);
        item = &cur_item_;
      }
      return err;
    }
    int release(Item* item) {
      int err= OB_SUCCESS;
      if (NULL == item)
      {}
      else if (NULL == getter_)
      {
        err = OB_NOT_INIT;
      }
      else if (item != &cur_item_)
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "release(): NOT MATCH");
      }
      else
      {
        pthread_rwlock_unlock(&lock_);
      }
      return err;
    }
    int update(){
      int err = OB_SUCCESS;
      int64_t cur_time = tbsys::CTimeUtil::getTime(); 
      int64_t seq = 0;
      if (NULL == getter_)
      {
        err = OB_NOT_INIT;
      }
      else if (last_update_time_us_ + update_interval_us_ > cur_time)
      {}
      else if (0 != ((seq = seq_) & 1) || !__sync_bool_compare_and_swap(&seq_, seq, seq+1))
      {
        TBSYS_LOG(DEBUG, "update(%ld): already in process", seq);
      }
      else
      {
        pthread_rwlock_wrlock(&lock_);
        cur_time = tbsys::CTimeUtil::getTime(); 
        if (last_update_time_us_ + update_interval_us_ >= cur_time)
        {}
        else if (OB_SUCCESS != (err = getter_->get(cur_item_)))
        {
          TBSYS_LOG(ERROR, "update_cfg())=>%d", err);
        }
        else
        {
          last_update_time_us_ = tbsys::CTimeUtil::getTime();
        }
        pthread_rwlock_unlock(&lock_);
        __sync_fetch_and_add(&seq_, 1);
      }
      return err;
    }
    
  private:
    Getter* getter_;
    int64_t update_interval_us_;
    int64_t last_update_time_us_;
    Item cur_item_;
    volatile int64_t seq_;
    pthread_rwlock_t lock_;
};

#endif /* __OB_LOG_TOOL_CACHED_ITEM_H__ */
