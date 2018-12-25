#ifndef _THREAD_H_
#define _THREAD_H_

#include <pthread.h>

class ThreadException
{
};

class Thread
{
  public:
    Thread();
    Thread(void * arg);
    virtual ~Thread();
    virtual void *  run(void * arg) = 0;
    void    start();
    void    start(void * arg);
    void    wait();
    void    stop();
  protected:
    static void * run_(void * arg);
    void *  arg_;
    pthread_t pid_;
    volatile bool stop_;
};

Thread::Thread() : arg_(NULL), stop_(false)
{
}

Thread::Thread(void * arg) : arg_(arg), stop_(false)
{
}

Thread::~Thread()
{
}

void Thread::start()
{
  start(arg_);
}

void Thread::start(void * arg)
{
  arg_ = arg;
  int err = pthread_create(&pid_, NULL, &run_, this);
  if (0 != err)
  {
    throw ThreadException();
  }
}

void Thread::wait()
{
  void * thread_ret = NULL;
  int err = pthread_join(pid_, &thread_ret);
  if (0 != err)
  {
    throw ThreadException();
  }
}

void Thread::stop()
{
  stop_ = true;
  //int err = pthread_cancel(pid_);
  //if (0 != err)
  //{
  //  throw ThreadException();
  //}
}

void * Thread::run_(void * arg)
{
  Thread * t = reinterpret_cast<Thread *>(arg);
  return t->run(t->arg_);
}

#endif // _THREAD_H_
