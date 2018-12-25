#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>

int64_t get_usec()
{
  struct timeval time_val;
  gettimeofday(&time_val, NULL);
  return time_val.tv_sec*1000000 + time_val.tv_usec;
}

#define profile(expr, n) { \
  int64_t start = get_usec(); \
  expr;\
  int64_t end = get_usec();\
  printf("%s: 1000000*%ld/%ld=%ld\n", #expr, n, end - start, 1000000 * n / (end - start)); \
}

struct Callable
{
  Callable(): stop_(false) {}
  virtual ~Callable() {}
  virtual int call(pthread_t thread, int64_t idx) = 0;
  volatile bool stop_;
};

typedef void*(*pthread_handler_t)(void*);
class BaseWorker
{
  public:
    static const int64_t MAX_N_THREAD = 16;
    struct WorkContext
    {
      WorkContext() : callable_(NULL), idx_(0) {}
      ~WorkContext() {}
      WorkContext& set(Callable* callable, int64_t idx) {
        callable_ = callable;
        idx_ = idx;
        return *this;
      }
      Callable* callable_;
      pthread_t thread_;
      int64_t idx_;
    };
  public:
    BaseWorker(): n_thread_(0) {}
    ~BaseWorker(){}
  public:
    BaseWorker& set_thread_num(int64_t n){ n_thread_ = n; return *this; }
    int start(Callable* callable, int64_t idx=-1) {
      int err = 0;
      for(int64_t i = 0; i < n_thread_; i++){
        if (idx > 0 && idx != i)continue;
        fprintf(stderr, "worker[%ld] start.\n", i);
        pthread_create(&ctx_[i].thread_, NULL, (pthread_handler_t)do_work, (void*)(&ctx_[i].set(callable, i)));
      }
      return err;
    }

    int wait(int64_t idx=-1) {
      int err = 0;
      int64_t ret = 0;
      for(int64_t i = 0; i < n_thread_; i++) {
        if (idx > 0 && idx != i)continue;
        pthread_join(ctx_[i].thread_, (void**)&ret);
        if (ret != 0) {
          fprintf(stderr, "thread[%ld] => %ld\n", i, ret);
        } else {
          fprintf(stderr, "thread[%ld] => OK.\n", i);
        }
      }
      return err;
    }

    static int do_work(WorkContext* ctx) {
      int err = 0;
      if (NULL == ctx || NULL == ctx->callable_) {
        err = -EINVAL;
      } else {
        err = ctx->callable_->call(ctx->thread_, ctx->idx_);
      }
      return err;
    }
    int par_do(Callable* callable, int64_t duration) {
      int err = 0;
      if (0 != (err = start(callable)))
      {
        fprintf(stderr, "start()=>%d\n", err);
      }
      else
      {
        usleep(static_cast<__useconds_t>(duration));
        callable->stop_ = true;
      }
      if (0 != (err = wait()))
      {
        fprintf(stderr, "wait()=>%d\n", err);
      }
      return err;
    }
  protected:
    int64_t n_thread_;
    WorkContext ctx_[MAX_N_THREAD];
};

int PARDO(int64_t thread_num, Callable* call, int64_t duration)
{
  BaseWorker worker;
  fprintf(stderr, "thread_num=%ld\n", thread_num);
  return worker.set_thread_num(thread_num).par_do(call, duration);
}
#if 0
struct SimpleCallable: public Callable
{
  int64_t n_items_;
  SimpleCallable& set(int64_t n_items) {
    n_items_ = n_items;
    return *this;
  }
  int call(pthread_t thread, int64_t idx) {
    int err = 0;
    fprintf(stdout, "worker[%ld] run\n", idx);
    if (idx % 2)
      err = -EPERM;
    return err;
  }
};

int main(int argc, char** argv)
{
  int err = 0;
  BaseWorker worker;
  SimpleCallable callable;
  int64_t n_thread = 0;
  int64_t n_items = 0;
  if (argc != 3)
  {
    err = -EINVAL;
    fprintf(stderr, "%s n_thread n_item\n", argv[0]);
  }
  else
  {
    n_thread = atoll(argv[1]);
    n_items = atoll(argv[2]);
    profile(worker.set_thread_num(n_thread).par_do(&callable.set(n_items), 10000000), n_items);
  }
}
#endif
