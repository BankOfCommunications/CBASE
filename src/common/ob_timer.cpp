#include "common/ob_timer.h"
namespace oceanbase
{
  namespace common
  {
    using namespace tbutil;
    int ObTimer::init()
    {
      int ret = OB_ERROR;
      if (inited_)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        if (pthread_create(&tid_,NULL,ObTimer::run_thread,this) != 0)
        {
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_SUCCESS;
          inited_ = true;
          destroyed_ = false;
        }
      }
      return ret;
    }

    ObTimer::~ObTimer()
    {
      destroy();
    }

    void ObTimer::destroy()
    {
      if( !destroyed_ && inited_)
      {
        Monitor<Mutex>::Lock guard(monitor_);
        destroyed_ = true;
        inited_ = false;
        monitor_.notifyAll();
        tasks_num_ = 0;
      }

      if (tid_ != 0)
      {
        pthread_join(tid_,NULL);
        tid_ = 0;
      }
    }

    int ObTimer::schedule(ObTimerTask& task,const int64_t delay,bool repeate /*=false*/)
    {
      int ret = OB_ERROR;
      if (delay < 0 || (tasks_num_ >= MAX_TASK_NUM) || !inited_ || destroyed_)
      {
        TBSYS_LOG(WARN, "check schedule param failed:delay[%ld], task[%d], max[%d], init[%d], destroy[%d]",
            delay, tasks_num_, MAX_TASK_NUM, inited_, destroyed_);
        ret = OB_ERROR;
      }
      else
      {
        Monitor<Mutex>::Lock guard(monitor_);
        int64_t time = Time::now(Time::Monotonic).toMicroSeconds() + delay;
        ret = insert_token(Token(time,repeate ? delay : 0,&task));
        if ( OB_SUCCESS == ret )
        {
          if (0 == wakeup_time_ || wakeup_time_ >= time)
          {
            monitor_.notify();
          }
        }
        else
        {
          TBSYS_LOG(WARN, "insert token failed:ret[%d]", ret);
        }
      }
      return ret;
    }

    int ObTimer::insert_token(const Token& token)
    {
      int ret = OB_ERROR;
      if (tasks_num_ >= MAX_TASK_NUM)
      {
        ret = OB_ERROR;
      }
      else
      {
        int32_t pos = 0;
        for(pos = 0;pos < tasks_num_;++pos)
        {
          if (token.scheduled_time <= tokens_[pos].scheduled_time)
          {
            break;
          }
        }

        for(int32_t i=tasks_num_; i > pos; --i)
        {
          tokens_[i] = tokens_[i - 1];
        }
        tokens_[pos] = token;
        ret = OB_SUCCESS;
        ++tasks_num_;
      }
      return OB_SUCCESS;
    }

    void ObTimer::cancel(const ObTimerTask& task)
    {
      Monitor<Mutex>::Lock guard(monitor_);
      if (inited_ && !destroyed_ && tasks_num_ > 0)
      {
        int32_t pos = -1;
        for(int32_t i = 0;i < tasks_num_;++i)
        {
          if (&task == tokens_[i].task)
          {
            pos = i;
            break;
          }
        }

        if (pos != -1)
        {
          memmove(&tokens_[pos], &tokens_[pos + 1],
                  sizeof (tokens_[0]) * (tasks_num_ - pos - 1));
          --tasks_num_;
        }
      }
      return;
    }

    void *ObTimer::run_thread(void *arg)
    {
      if (NULL == arg)
      {
        return NULL;
      }
      ObTimer *t = reinterpret_cast<ObTimer *>(arg);
      t->run();
      return NULL;
    }

    void ObTimer::run()
    {
      Token token(0,0,NULL);
      while(true)
      {
        {
          Monitor<Mutex>::Lock guard(monitor_);
          if (destroyed_)
          {
            break;
          }
          //add repeated task to tasks_ again
          if (token.delay != 0)
          {
            token.scheduled_time = Time::now(Time::Monotonic).toMicroSeconds() + token.delay;
            insert_token(Token(token.scheduled_time,token.delay,token.task));
          }

          if ( 0 == tasks_num_)
          {
            wakeup_time_ = 0;
            monitor_.wait();
          }

          if (destroyed_)
          {
            break;
          }

          while(tasks_num_ > 0 && !destroyed_)
          {
            int64_t now = Time::now(Time::Monotonic).toMicroSeconds();
            if (tokens_[0].scheduled_time <= now)
            {
              token = tokens_[0];
              memmove(tokens_,tokens_+1,(tasks_num_ - 1) * sizeof(tokens_[0]));
              --tasks_num_;
              break;
            }

            if (destroyed_)
            {
              break;
            }
            else
            {
              wakeup_time_ = tokens_[0].scheduled_time;
              monitor_.timedWait(Time(wakeup_time_ - now));
            }
          }
        }
        if (token.task != NULL && !destroyed_)
          token.task->runTimerTask();
      }
    }

    void ObTimer::dump() const
    {
      for(int32_t i=0;i<tasks_num_;++i)
      {
        printf("%d : %ld %ld %p\n",i,tokens_[i].scheduled_time,tokens_[i].delay,tokens_[i].task);
      }
    }
  } /* common */
} /* chunkserver */
