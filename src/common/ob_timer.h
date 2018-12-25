#ifndef OCEANBASE_COMMON_OB_TIMER_H_
#define OCEANBASE_COMMON_OB_TIMER_H_

#include <pthread.h>
#include <tbsys.h>
#include <Time.h>
#include <Mutex.h>
#include <Monitor.h>
#include "common/ob_define.h"

namespace oceanbase 
{
  namespace common 
  {

    class ObTimerTask 
    {
      public:
        virtual ~ObTimerTask() {};
        virtual void runTimerTask() = 0;
    };

    class ObTimer
    {
      public:
        ObTimer():tid_(0),tasks_num_(0),wakeup_time_(0),inited_(false),destroyed_(false) {}
        ~ObTimer();

        int init();
        void destroy();

        int schedule(ObTimerTask& task,const int64_t delay,bool repeate = false);
        void cancel(const ObTimerTask& task);
        
        int32_t get_tasks_num() const { return tasks_num_; }
        void dump() const;
      
      private:
       
        struct Token
        {
          Token():scheduled_time(0),delay(0),task(NULL){};
          Token(const int64_t st, const int64_t dt, ObTimerTask *task):scheduled_time(st),delay(dt),task(task) {}
        
          int64_t scheduled_time;
          int64_t delay;
          ObTimerTask *task;
        };
        
        static const int32_t MAX_TASK_NUM = 16;
        
        int insert_token(const Token& token);
        static void *run_thread(void *arg);
        void run();
        
        pthread_t tid_;
        int32_t tasks_num_;
        int64_t wakeup_time_;
        bool inited_;
        bool destroyed_;
        tbutil::Monitor<tbutil::Mutex> monitor_;
        Token tokens_[MAX_TASK_NUM];
    };

  } /* common */

} /* oceanbase */


#endif
