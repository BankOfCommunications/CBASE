/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_STRESS_H__
#define __OB_UPDATESERVER_STRESS_H__
#include "common/ob_pcap.h"
#include "ob_trans_executor.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    class StressRunnable: public tbsys::CDefaultRunnable
    {
      public:
        StressRunnable(TransExecutor& handler, const char* pf_cmd):  pf_(pf_cmd, 21, 16), handler_(handler){}
        virtual ~StressRunnable(){}
        virtual void run(tbsys::CThread* thread, void* arg) {
          int err = OB_SUCCESS;
          ObPacket pkt;
          UNUSED(thread);
          TBSYS_LOG(INFO, "StressRunnable.start(arg=%ld)", (int64_t)arg);
          ThreadSpecificBuffer::Buffer* buffer = thread_buffer_.get_buffer();
          while(!_stop && (OB_EAGAIN == err || OB_SUCCESS == err))
          {
            if (handler_.get_session_mgr().get_flying_rwsession_num() + handler_.TransHandlePool::get_queued_num() > 40000)
            {
              PAUSE();
              continue;
            }
            pkt.get_buffer()->set_data(buffer->current(), buffer->remain());
            if (OB_SUCCESS != (err = pf_.get_match_packet(&pkt, OB_PHY_PLAN_EXECUTE))
                && OB_EAGAIN != err)
            {
              TBSYS_LOG(ERROR, "get_match_packet(PHY_PLAN_EXECUTE)=>%d, stop client", err);
              err = OB_CANCELED;
            }
            else if (OB_EAGAIN == err)
            {}
            else
            {
              pkt.get_buffer()->set_data(pkt.get_buffer()->get_data(), pkt.get_buffer()->get_position());
              handler_.handle_packet(pkt);
            }
          }
          TBSYS_LOG(INFO, "StressRunnable.stop(arg=%ld)=>%d", (int64_t)arg, err);
        }

        static StressRunnable& get_stress_runnable(TransExecutor* trans_executor = NULL, const char* pfetch_cmd = NULL) {
          static StressRunnable stress(*trans_executor, pfetch_cmd);
          return stress;
        }

        static int start_stress(TransExecutor& trans_executor, const char* pfetch_cmd=NULL, int thread_count = 4) {
          StressRunnable& stress = get_stress_runnable(&trans_executor, pfetch_cmd?:getenv("pfetch_cmd"));
          stress.setThreadCount(thread_count);
          stress.start();
          return 0;
        }

        static int stop_stress() {
          get_stress_runnable().stop();
          get_stress_runnable().wait();
          return 0;
        }
      private:
        ObPFetcher pf_;
        TransExecutor& handler_;
        ThreadSpecificBuffer thread_buffer_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_STRESS_H__ */
