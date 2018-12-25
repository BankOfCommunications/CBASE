#ifndef __COMSUMER_QUEUE_H__
#define  __COMSUMER_QUEUE_H__

#include <list>
#include <pthread.h>
#include <semaphore.h>
#include <limits.h>

#include "common/utility.h"
#include "tbsys.h"
#include "oceanbase_db.h"

//add by liyongfeng:20141020 
extern bool g_cluster_state;//rootserver状态,初始值为true,如果monitor发现异常状态,置为false
const int64_t l_merge_sleep = 30;//monitor线程,每间隔30s获取一次合并状态
const int64_t l_ups_sleep = 20;//monitor线程,每间隔20s获取一次最新主UPS
const int64_t l_gcd_sleep = 60;//上面两个时间的最大公约数
//add:end

template <class T>
class QueueComsumer {
  public:
    virtual ~QueueComsumer() { }
    virtual int init() { return 0; }
    virtual int comsume(T &obj) = 0;
};

template<class T>
class QueueProducer {
  public:
    virtual ~QueueProducer() { }
    virtual int init() { return 0; }
    virtual int produce(T &obj) = 0;
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
	  atomic_set(&consumer_quiting_nr_, 0);//add by liyongfeng:20141016 初始值为0
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
	  atomic_set(&import_state_, 0);//add by liyongfeng:20141017 初始值为0
	  db_ = NULL;//add by liyongfeng:20141017 初始值为NULL
      wait_time_sec_ = 0; // add by zhangcd 20150804
    }

	//mod by liyongfeng:20141017 add oceanbase::api::OceanbaseDb *db
    ComsumerQueue(oceanbase::api::OceanbaseDb *db, QueueProducer<T> *producer, QueueComsumer<T> *comsumer, size_t cap = LONG_MAX) { 
      cap_ = cap;
      comsumer_ = comsumer;
      producer_ = producer;
      nr_producer_ = nr_comsumer_ = 0;
      running_ = false;
      producer_quiting_ = false;
      atomic_set(&producer_quiting_nr_, 0);
	  atomic_set(&consumer_quiting_nr_, 0);//add by liyongfeng:20141016 初始值为0
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
	  atomic_set(&import_state_, 0);//add by liyongfeng:20141017 初始值为0
	  db_ = db;//add by liyongfeng:20141017
	  assert(db_ != NULL);//add by liyongfeng:20141017
    wait_time_sec_ = 0; // add by zhangcd 20150804
    }
	//mod:end

    ~ComsumerQueue() { if (running_) dispose(); }

    void attach_comsumer(QueueComsumer<T> *comsumer) {
      comsumer_ = comsumer;
    }

    void attach_producer(QueueProducer<T> *producer) {
      producer_ = producer;
    }

	//mod by liyongfeng:20141017 add nr_monitor
    //start produce and comsume process
    int produce_and_comsume(int nr_producer, int nr_monitor, int nr_comsumer);
	//mod:end

    //(long)arg is thread id, id < nr_producer_ is producer thread 
    //or is comsume thread
    virtual void run(tbsys::CThread *thread, void *arg);

    void produce(long id);

    void comsume(long id);

	void monitor(long id);//add by liyongfeng, 20141014, monitor the state of daily merge

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

    // add by zhangcd 20150804:b
    int64_t get_waittime_sec();
    // add:e

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
	long nr_monitor_;//add by liyongfeng:20141017 监控线程数量
    int64_t wait_time_sec_; // add by zhangcd 20150804
    bool producer_quiting_;
    bool push_waiting_;
    atomic_t producer_quiting_nr_;
	atomic_t consumer_quiting_nr_;//add by liyongfeng 20141016: for notify monitor exit
    atomic_t queue_size_;
	atomic_t import_state_;//add by liyongfeng 20141016: ob_import state(1--continue;0--stop;-1--forbid)
	oceanbase::api::OceanbaseDb *db_;//add by liyongfeng:20141017
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

//mod by liyongfeng:20141017 add nr_monitor
template <class T>
int ComsumerQueue<T>::produce_and_comsume(int nr_producer, int nr_monitor, int nr_comsumer)
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

  nr_monitor_ = nr_monitor;

  // add zhangcd [ob_import.ignore_merge] 20150721:b
  if(nr_monitor == 0)
  {
    atomic_set(&import_state_, 1);
  }
  // add:e
  if (producer_->init() || comsumer_->init()) {
    TBSYS_LOG(ERROR, "can't init producer/comsumer, quiting");
    ret = QUEUE_ERROR;
  } else {
    running_ = true;
    TBSYS_LOG(INFO, "CQ:producer = %ld, monitor = %ld, comsumer = %ld, cap=%ld", nr_producer_, nr_monitor_, nr_comsumer_, cap_);//mod by liyongfeng:20141017 add nr_monitor_
    setThreadCount(static_cast<int32_t>(nr_comsumer_ + nr_monitor_ + nr_producer_));//mod by liyongfeng:20141017 add nr_monitor_	
    start();
    wait();
  }

  return ret;
}
//mod:end

template <class T>
void ComsumerQueue<T>::run(tbsys::CThread *thread, void *arg)
{
  long id = (long)arg;
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(DEBUG, "CQ:in run, id=%ld", id);
  if (id < nr_producer_) {//if thread id(0) < nr_producer_(1) means this thread is producer thread
    produce(id);
  //add by liyongfeng, 20141014, monitor thread for monitoring the state of daily merge 
  }
  // mod zhangcd [ob_import.ignore_merge] 20150721:b
  else if ((id >= nr_producer_ && id < nr_producer_ + nr_comsumer_)) {//if thread id(0) = nr_producer_(1) means this thread is monitor thread
    comsume(id);
  //add:end
  } else { //if thread id(2,3,...) > nr_producer_(1) means these threas are consumer threads
  	monitor(id);
  }
  // mod:e
}

template <class T>
void ComsumerQueue<T>::produce(long id)
{
  TBSYS_LOG(DEBUG, "producer id = %ld", id);

  int ret = 0;
  while (running_) {
	  //mod by liyongfeng:20141016 ob_import的producer线程针对不同import状态进行不同处理
	  //import_state_=-1  RS切换或UPS切换,ob_import禁止生产导入数据
	  //否则, 继续生产数据,提供给ob_import的consumer进行消费
	  if (0 != atomic_read(&import_state_) && 1 != atomic_read(&import_state_)) {
		  TBSYS_LOG(ERROR, "error import state, quit producer id=%ld", id);
		  atomic_add(1, &producer_quiting_nr_);
		  if (atomic_read(&producer_quiting_nr_) == nr_producer_) {
			  TBSYS_LOG(INFO, "all producer quit");
			  producer_quiting_ = true;
		  }
		  break;
	  } else {
		  T obj;
		  ret = producer_->produce(obj);
		  if (ret == QUEUE_ERROR) {
			  TBSYS_LOG(WARN, "can't produce object, producer id=%ld", id);
		  } else if (ret == QUEUE_QUITING) {
			  atomic_add(1, &producer_quiting_nr_);
			  if (atomic_read(&producer_quiting_nr_) == nr_producer_) {
				  TBSYS_LOG(INFO, "all producer quit");
				  producer_quiting_ = true;
			  }
			  break;
		  }
		  push(obj);
	  }
	  //mod:end
  }
}

template <class T>
void ComsumerQueue<T>::comsume(long id)
{
  int ret = 0;

  TBSYS_LOG(INFO, "in comsume thread, id=%ld", id);
  while (running_) {
	  //mod by liyongfeng:20141016 ob_import的consumer线程针对不同的import状态进行不同处理
	  //import_state_=1  ob_import允许consumer继续导入数据
	  //import_state_=0  ob_import停止consumer导入数据,等待合并结束
	  //import_state_=-1  RS切换或UPS切换,ob_import的producer禁止生产数据,但是允许consumer消费余下的已生产的数据
	  if (0 == atomic_read(&import_state_)) {
		  //TBSYS_LOG(WARN, "merge: DOING or merge: TIMEOUT, pause comsumer id=%ld", id);
		  sleep(1);
		  continue;
	  } else {
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
      }//mod:end
  }

  /* comsume the remaining objs */
  finalize_queue(id);

  atomic_add(1, &consumer_quiting_nr_);//add by liyongfeng:20141016
}

//add by liyongfeng, 20141014, monitor the state of daily merge
template <class T>
void ComsumerQueue<T>::monitor(long id)
{
	int ret = 0;
	int32_t state = 0;//记录每次获取到merge状态
	int32_t ups_switch = 0;//记录每次判断UPS是否切换
	int64_t count = 0;//记录sleep(1)的次数

	TBSYS_LOG(INFO, "in monitor thread, id=%ld", id);
	while (running_) {
    // add zhangcd [ob_import.ignore_merge] 20150721:b
    TBSYS_LOG(INFO, "Monitor is running...");
    // add:e
		if (atomic_read(&consumer_quiting_nr_) == nr_comsumer_) {
			TBSYS_LOG(INFO, "all consumer quit, quit monitor id=%ld", id);
			break;
		} 
		if (0 == (count % l_merge_sleep)) {//需要获取每日合并状态
			//send request to get the state of daily merge
			RPC_WITH_RETRIES_SLEEP(db_->get_daily_merge_state(state), 3, ret);//重试3次,每次间隔5s
			if(ret != 0) {
				//send request failed
				TBSYS_LOG(ERROR, "failed to get the state of daily merge, all producer forbid, err=%d", ret);
				//将import_state_置为-1,禁止ob_import导入数据
				/*
				if (1 == atomic_read(&is_merge_)) {
					atomic_sub(2, &is_merge_);
				} else if (0 == atomic_read(&is_merge_)) {
					atomic_sub(1, &is_merge_);
				} else {
					//TBSYS_LOG(INFO, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
				}
				*/
				atomic_set(&import_state_, -1);
				TBSYS_LOG(INFO, "quit monitor id=%ld", id);
				//修改全局状态,通知主线程异常状态,程序会异常退出
				g_cluster_state = false;
				break;
			} else {
				//根据获取到当前状态,修改import_state_
				if (1 == state) {//已经合并完成
					TBSYS_LOG(INFO, "daily merge has done, all consumer continue, state=%d", state);
					atomic_set(&import_state_, 1);
				} else if (0 == state) {//正在合并
          // add by zhangcd 20150804:b
          wait_time_sec_ += l_merge_sleep;
          fprintf(stdout, "daily merge is doing, all consumer pause...\n");
          // add:e
					TBSYS_LOG(INFO, "daily merge is doing, all consumer pause, state=%d", state);
					atomic_set(&import_state_, 0);
				} else if (-1 == state) {//获取合并失败
					TBSYS_LOG(WARN, "error merge state, all producer forbid, state=%d", state);
					atomic_set(&import_state_, -1);
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					//修改全局状态,通知主线程异常状态,程序会异常退出
					g_cluster_state = false;
					break;
				} else {//错误状态码
					TBSYS_LOG(ERROR, "invalid merge state, all producer forbid, state=%d", state);
					atomic_set(&import_state_, -1);
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					//修改全局状态,通知主线程异常状态,程序会异常退出
					g_cluster_state = false;
					break;
				}
				/*
				if(1 == atomic_read(&is_merge_)) {//上一次是merge: DONE
					if (1 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DONE]");
					} else if (0 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DOING]");
						atomic_sub(1, &is_merge_);
					} else if (-1 == state) {
						TBSYS_LOG(DEBUG, "can't get merge state");
						atomic_sub(2, &is_merge_);
					} else {
						TBSYS_LOG(ERROR, "invalid merge state, state=%d", state);
						atomic_sub(2, &is_merge_);
					}
				} else if (0 == atomic_read(&is_merge_)) {//上一次是merge: DOING or merge: TIMEOUT
					if (1 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DONE]");
						atomic_add(1, &is_merge_);
					} else if (0 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DOING]");
					} else if (-1 == state) {
						TBSYS_LOG(DEBUG, "can't get merge state");
						atomic_sub(1, &is_merge_);
					} else {
						TBSYS_LOG(ERROR, "invalid merge state, state=%d", state);
						atomic_sub(1, &is_merge_);
					}
				} else {//上一次获取merge状态失败,ob_import过程已经被禁止,正在准备退出,不管本次获取到状态
					TBSYS_LOG(WARN, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
					//修改全局状态,通知主线程异常状态,程序会异常退出
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					g_cluster_state = false;
					break;
				}
				*/
			}
		}
		if (0 == (count % l_ups_sleep)) {//需要获取最新主UPS
			RPC_WITH_RETRIES_SLEEP(db_->get_latest_update_server(ups_switch), 3, ret);
			if (ret != 0) {//获取主UPS失败,
				TBSYS_LOG(ERROR, "failed to get latest update server, err=%d", ret);
				atomic_set(&import_state_, -1);
				TBSYS_LOG(WARN, "forbid producer running, import_state_=%d", atomic_read(&import_state_));
				TBSYS_LOG(INFO, "quit monitor id=%ld", id);
				//修改全局状态,通知主线程异常状态,程序会异常退出
				g_cluster_state = false;
				break;
			} else {
				if (0 == ups_switch) {//主UPS未发生切换
					TBSYS_LOG(DEBUG, "master update server not switch");
				} else if (1 == ups_switch) {//主UPS发生切换
					atomic_set(&import_state_, -1);
					TBSYS_LOG(WARN, "forbid producer running, import_state_=%d", atomic_read(&import_state_));
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					//修改全局状态,通知主线程异常状态,程序会异常退出
					g_cluster_state = false;
					break;
				} else {//错误状态
					TBSYS_LOG(ERROR, "invalid ups switch state, ups_switch=%d", ups_switch);
					atomic_set(&import_state_, -1);
					TBSYS_LOG(WARN, "forbid producer running, import_state_=%d", atomic_read(&import_state_));
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					//修改全局状态,通知主线程异常状态,程序会异常退出
					g_cluster_state = false;
					break;
				}
			}
		}

		count++;
		count = count % l_gcd_sleep;
		sleep(1);
	}
}
//add:end

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

// add by zhangcd 20150804:b
template <class T>
long ComsumerQueue<T>::get_waittime_sec()
{
  return wait_time_sec_;
}
// add:e

#endif
