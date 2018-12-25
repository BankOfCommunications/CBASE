#ifndef __OB_COMMON_RWT_H__
#define __OB_COMMON_RWT_H__
#include "thread_worker.h"

class RWT: public Callable
{
  typedef void*(*pthread_handler_t)(void*);
  struct Thread
  {
    int set(RWT* self, int64_t idx){ self_ = self, idx_ = idx; return 0; }
    pthread_t thread_;
    RWT* self_;
    int64_t idx_;
  };
  public:
    RWT(): n_read_thread_(0), n_write_thread_(0), n_admin_thread_(0){}
    virtual ~RWT(){}
  public:
    int64_t get_thread_num() { return 1 + n_read_thread_ + n_write_thread_ + n_admin_thread_; }
    RWT& set(const int64_t n_read, const int64_t n_write, const int64_t n_admin = 0) {
      n_read_thread_ = n_read;
      n_write_thread_ = n_write;
      n_admin_thread_ = n_admin;
      return *this;
    }
    int report_loop() {
      int err = 0;
      int64_t report_interval = 1000 * 1000;
      while(!stop_ && 0 == err) {
        usleep(static_cast<__useconds_t>(report_interval));
        err = report();
      }
      return err;
    }
    virtual int call(pthread_t thread, const int64_t idx_) {
      int err = 0;
      int64_t idx = idx_;
      (void)(thread);
      fprintf(stderr, "rwt.start(idx=%ld)\n", idx_);
      if (idx < 0)
      {
        err = -EINVAL;
      }
      if (0 == err && idx >= 0)
      {
        if (idx == 0)
        {
          err = report_loop();
        }
        idx -= 1;
      }
      if (0 == err && idx >= 0)
      {
        if (idx < n_read_thread_)
        {
          err = read(idx);
        }
        idx -= n_read_thread_;
      }
      if (0 == err && idx >= 0)
      {
        if (idx < n_write_thread_)
        {
          err = write(idx);
        }
        idx -= n_write_thread_;
      }
      if (0 == err && idx >= 0)
      {
        if (idx < n_admin_thread_)
        {
          err = admin(idx);
        }
        idx -= n_admin_thread_;
      }
      if (0 == err && idx >= 0)
      {
        err = -EINVAL;
      }
      if (0 != err)
      {
        stop_ = true;
      }
      fprintf(stderr, "rwt.start(idx=%ld)=>%d\n", idx_, err);
      return err;
    }
    virtual int report(){ return 0; }
    virtual int read(const int64_t idx) = 0;
    virtual int write(const int64_t idx) = 0;
    virtual int admin(const int64_t idx){ (void)(idx);  return 0; }
  protected:
    int64_t n_read_thread_;
    int64_t n_write_thread_;
    int64_t n_admin_thread_;
};
#endif /* __OB_COMMON_RWT_H__ */

