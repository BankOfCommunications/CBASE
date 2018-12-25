/*
 * =====================================================================================
 *
 *       Filename:  db_export_queue.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月26日 14时37分40秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef __DB_EXPORT_QUEUE_H__
#define __DB_EXPORT_QUEUE_H__

template <class T>
class ExportQueueConsumer {
public:
  virtual ~ExportQueueConsumer() { }
  virtual int init() { return 0; }
  virtual int consume(T &obj) = 0;
};

template <class T>
class ExportQueueProducer {
public:
  virtual ~ExportQueueProducer() { }
  virtual int init() { return 0; }
  virtual int produce(T &obj) = 0;
};

template <class T>
class ExportQueue : public tbsys::CDefaultRunnable {
private:
  std::list<T> queue_;
  size_t cap_;
  tbsys::CThreadCond queue_cond_;
  tbsys::CThreadCond queue_cond_full_;//如果队列装满且producer正在运行,该条件量阻塞等待
  ExportQueueConsumer<T> *consumer_;
  ExportQueueProducer<T> *producer_;
  long nr_consumer_;
  long nr_producer_;
  bool producer_quiting_;
  bool push_waiting_;//producer压队列时是否等待
  atomic_t producer_quiting_nr_;
  atomic_t queue_size_;
};

template <class T>
void ExportQueue<T>::sleep_when_full()
{
  if (cap_ != 0)//如果没有指定cap_,producer没有等待限制
  {
     queue_cond_full_.lock();
     
     while (static_cast<size_t>(atomic_read(&queue_size_)) >= cap_ && running_)
     {
        push_waiting_ = true;
        queue_cond_full_.wait(1000);
     }
     push_waiting_ = false;
     queue_cond_full_.unlock();
  }
}

template <class T>
void ExportQueue<T>::wakeup_push()
{
  if (push_waiting_)
     queue_cond_full_.signal();
}

template <class T>
void ExportQueue<T>::push(T &obj)
{
  sleep_when_full();
  
  if (false == running_)
     return;

  atomic_inc(&queue_size_);
  queue_cond_.lock();
  queue_.push_back(obj);
  queue_cond_.unlock();

  queue_cond_.signal();
}

template <class T>
void ExportQueue<T>::pop(T &obj)
{
  int ret = 0;

  queue_cond_.lock();
  while (queue_.empty() && !producer_quiting_)
  {
     queue_cond_.wait(1000);
  }
  
  if (!queue_.empty())
  {
     obj = queue_.front();
     queue_.pop_front();
     atomic_dec(&queue_size_);
  }
  else if (producer_quiting_)
  {
     ret = QUEUE_QUITING;
  }

  queue_cond_.unlock();
  
  /* wake up sleeping producer thread if needed */
  wakeup_push();

  return ret;
}

template <class T>
int ExportQueue<T>::produce_and_comsume(int nr_producer, int nr_consumer)
{
  int ret = QUEUE_SUCCESS;
  if (NULL == producer_)
  {
     nr_producer_ = 0;
     TBSYS_LOG(ERROR, "export producer is NULL");
     return QUEUE_ERROR;
  }
  else
  {
     nr_producer_ = nr_producer;
  }

  if (NULL == consumer_)
  {
     nr_consumer_ = 0;
     TBSYS_LOG(ERROR, "export producer is NULL");
     return QUEUE_ERROR;
  }
  else
  {
     nr_consumer_ = nr_consumer;
  }

  if (procuder_->init() || consumer_->init())
  {
     TBSYS_LOG(ERROR, "can't init producer/consumer, quiting");
     ret = QUEUE_ERROR;
  }
  else
  {
     running_ = true;
     TBSYS_LOG(INFO, "ExportQueue: producer = %ld, consumer = %ld, cap = %ld", nr_producer_, nr_consumer_, cap_);
     setThreadCount(static_cast<int32_t>(nr_producer_ + nr_consumer_));
     start();
     wait();
  }

  return ret;
}

template <class T>
void ExportQueue<T>::run(tbsys::CThread *thread, void *arg)
{
  long id = (long)arg;
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "ExportQueue: is running, id=%ld", id);
  if (id < nr_producer_)
  {
     produce(id);
  }
  else
  {
     consume(id);
  }  
}

template <class T>
void ExportQueue<T>::produce(long id)
{
  TBSYS_LOG(INFO, "producer id = %ld", id);
  
  int ret = 0;
  while (running_)
  {
     T obj;
     ret = producer_->produce(obj);

  }
}
#endif
