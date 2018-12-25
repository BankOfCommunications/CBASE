#ifndef __OB_EXPORT_QUEUE_H__
#define __OB_EXPORT_QUEUE_H__

#include <list>
#include <pthread.h>
#include <semaphore.h>
#include <limits.h>

#include "common/utility.h"
#include "tbsys.h"
template <class T>
class QueueComsumer {
  public:
    virtual ~QueueComsumer() { }
    virtual int init() { return 0; }
    virtual int comsume(T &obj) = 0;
    virtual int init_data_file(std::string &file_name, int n) = 0;//add qianzm [export_by_tablets] 20160415
};

template<class T>
class QueueProducer {
  public:
    virtual ~QueueProducer() { }
    virtual int init() { return 0; }
    virtual int produce(T &obj) = 0;
    virtual int produce_v2(T &obj) = 0;
    //add qianzm [export_by_tablets] 20160415:b
    virtual int get_tablets_count() = 0;
    virtual int print_tablet_info_cache() = 0;
    //add 20160415:e
};

template<class T>
class ComsumerQueue : public tbsys::CDefaultRunnable {
  public:
    static const int QUEUE_ERROR = -1;
    static const int QUEUE_SUCCESS = 0;
    static const int QUEUE_QUITING = 1;
  public:
    ComsumerQueue() { 
      cap_ = 0;
      producer_ = NULL;
      comsumer_ = NULL;
      nr_producer_ = nr_comsumer_ = 0;
      running_ = false;
      producer_quiting_ = false;
      atomic_set(&producer_quiting_nr_, 0);
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
      is_multi_tables_ = false;
    }

    ComsumerQueue(QueueProducer<T> *producer, QueueComsumer<T> *comsumer, size_t cap = LONG_MAX) { 
      cap_ = cap;
      comsumer_ = comsumer;
      producer_ = producer;
      nr_producer_ = nr_comsumer_ = 0;
      running_ = false;
      producer_quiting_ = false;
      atomic_set(&producer_quiting_nr_, 0);
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
      is_multi_tables_ = false;
    }

    ~ComsumerQueue() { if (running_) dispose(); }

    void attach_comsumer(QueueComsumer<T> *comsumer) {
      comsumer_ = comsumer;
    }

    void attach_producer(QueueProducer<T> *producer) {
      producer_ = producer;
    }

    //start produce and comsume process
    int produce_and_comsume(int nr_producer, int nr_comsumer);

    //(long)arg is thread id, id < nr_producer_ is producer thread 
    //or is comsume thread
    virtual void run(tbsys::CThread *thread, void *arg);

    void produce(long id);

    void comsume(long id);

    //maybe block
    void push(T &obj);

    //maybe sleep, ms
    int pop(T &obj);

    //dispose queue
    void dispose();

    //set queue size
    void set_capacity(int64_t cap) { cap_ = cap; }

    //return size
    size_t size() const { return queue_.size(); }

    //return capacity of the queue
    size_t capacity() const { return cap_; }

    void finalize_queue(long id);

    void sleep_when_full();

    void wakeup_push();

    void set_is_multi_tables() {is_multi_tables_ = true;}
	
    void set_data_file_name(std::string &file_name){file_name_ = file_name;}//add qianzm [export_by_tablets] 20160415

  private:
    std::list<T> queue_;

    size_t cap_;
    tbsys::CThreadCond queue_cond_;
    tbsys::CThreadCond queue_cond_full_;
    QueueComsumer<T> *comsumer_;
    QueueProducer<T> *producer_;
    bool running_;
    long nr_comsumer_;
    long nr_producer_;
    bool producer_quiting_;
    bool push_waiting_;
    atomic_t producer_quiting_nr_;
    atomic_t queue_size_;
    bool is_multi_tables_;
    std::string file_name_;//add qianzm [export_by_tablets] 20160415
};

template <class T>
void ComsumerQueue<T>::sleep_when_full()
{
  if (cap_ != 0) {                              /* cap_ == 0, no cap limit */
    queue_cond_full_.lock();

    while (static_cast<size_t>(atomic_read(&queue_size_)) >= cap_ && running_) {
      push_waiting_ = true;
      queue_cond_full_.wait(1000);
    }
    push_waiting_ = false;
    queue_cond_full_.unlock();
  }
}

template <class T>
void ComsumerQueue<T>::wakeup_push()
{
  if (push_waiting_)
    queue_cond_full_.signal();
}

template <class T>
void ComsumerQueue<T>::push(T &obj)
{
  sleep_when_full();                           /* sleep if queue cap reaches */

  if (running_ == false)                        /* no more obj, if quiting */
    return;

  atomic_inc(&queue_size_);

  queue_cond_.lock();
  queue_.push_back(obj);
  queue_cond_.unlock();

  queue_cond_.signal();
}

template <class T>
int ComsumerQueue<T>::pop(T &obj)
{
  int ret = 0;

  queue_cond_.lock();
  while (queue_.empty() && !producer_quiting_) {
    queue_cond_.wait(1000);
  }

  if (!queue_.empty()) {
    obj = queue_.front();
    queue_.pop_front();
    atomic_dec(&queue_size_);
  } else if (producer_quiting_) {
    ret = QUEUE_QUITING;
  }

  queue_cond_.unlock();

  /* wake up sleeping push thread if needed */
  wakeup_push();

  return ret;
}

template <class T>
int ComsumerQueue<T>::produce_and_comsume(int nr_producer, int nr_comsumer)
{
  int ret = QUEUE_SUCCESS;

  if (producer_ == NULL) {
    nr_producer_ = 0;
  } else {
    nr_producer_ = nr_producer;
  }

  if (comsumer_ == NULL) {
    nr_comsumer_ = 0;
  } else {
    nr_comsumer_ = nr_comsumer;
  }

  if (producer_->init() || comsumer_->init()) {
    TBSYS_LOG(ERROR, "can't init producer/comsumer, quiting");
    ret = QUEUE_ERROR;
  }
  //add qianzm [export_by_tablets] 20160415:b
  else if (comsumer_->init_data_file(file_name_, producer_->get_tablets_count()))
  {
    TBSYS_LOG(ERROR, "can't init comsumer file writers, quiting");
    ret = QUEUE_ERROR;
  }
  //add 20160415:e
  else {
    running_ = true;
    TBSYS_LOG(INFO, "CQ:producer = %ld, comsumer = %ld, cap=%ld", nr_producer_, nr_comsumer_, cap_);
    setThreadCount(static_cast<int32_t>(nr_comsumer_ + nr_producer_));
    start();
    wait();
  }

  return ret;
}

template <class T>
void ComsumerQueue<T>::run(tbsys::CThread *thread, void *arg)
{
  long id = (long)arg;
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(DEBUG, "CQ:in run, id=%ld", id);
  if (id < nr_producer_) {
    produce(id);
  } else {
    comsume(id);
  }
}

template <class T>
void ComsumerQueue<T>::produce(long id)
{
  TBSYS_LOG(DEBUG, "producer id = %ld", id);

  int ret = 0;
  while (running_)
  {
    T obj;
    //add qianzm [multi-tables] 20151228
    if (is_multi_tables_)
    {
      ret = producer_->produce_v2(obj);
    }
    //add e
    else
    {
      ret = producer_->produce(obj);
    }
    if (ret == QUEUE_ERROR)
    {
      TBSYS_LOG(WARN, "can't produce object, producer id=%ld", id);
    }
    else if (ret == QUEUE_QUITING)
    {
      atomic_add(1, &producer_quiting_nr_);
      if (atomic_read(&producer_quiting_nr_) == nr_producer_)
      {
        TBSYS_LOG(INFO, "all producer quit");
        producer_quiting_ = true;
        if (!is_multi_tables_)
            producer_->print_tablet_info_cache();//add qianzm [export_by_tablets] 20160415
      }
      break;
    }
    push(obj);
  }
}

template <class T>
void ComsumerQueue<T>::comsume(long id)
{
  int ret = 0;

  TBSYS_LOG(INFO, "in comsume thread, id=%ld", id);
  while (running_) {
    T obj;
    ret = pop(obj);
    if (ret == 0) {
      ret = comsumer_->comsume(obj);
      if (ret != 0) {
        TBSYS_LOG(WARN, "can't comsume object, comsumer id=%ld", id);
      }
    } else if (ret == QUEUE_QUITING) {
      TBSYS_LOG(INFO, "producer quiting, quit comsumer id=%ld", id);
      break;
    } else if (ret == QUEUE_ERROR) {
      TBSYS_LOG(WARN, "can't pop queue err");
    }
  }

  /* comsume the remaining objs */
  finalize_queue(id);
}

template <class T>
void ComsumerQueue<T>::finalize_queue(long id)
{
  queue_cond_.lock();
  while (!queue_.empty()) {
    T &obj = queue_.front();
    int ret = comsumer_->comsume(obj);
    if (ret != 0) {
      TBSYS_LOG(WARN, "can't comsume object, comsumer id=%ld", id);
    }
    queue_.pop_front();
  }
  queue_cond_.unlock();
}

template <class T>
void ComsumerQueue<T>::dispose()
{
  running_ = false;
  stop();
}

#endif
