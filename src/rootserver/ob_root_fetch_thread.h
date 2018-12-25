#ifndef OCEANBASE_ROOTSERVER_FETCH_THREAD_H_
#define OCEANBASE_ROOTSERVER_FETCH_THREAD_H_

#include "common/ob_fetch_runnable.h"
#include "rootserver/ob_root_log_manager.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootFetchThread : public common::ObFetchRunnable
    {
      const static int64_t WAIT_INTERVAL;

      public:
        ObRootFetchThread();

        int wait_recover_done();

        int got_ckpt(uint64_t ckpt_id);

        void set_log_manager(ObRootLogManager* log_manager);
    
      private:
        int recover_ret_;
        bool is_recover_done_;
        ObRootLogManager* log_manager_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOTSERVER_FETCH_THREAD_H_ */
#include "common/ob_fetch_runnable.h"

