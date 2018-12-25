/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_write_worker.h for define write worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_WRITE_WORKER_H_
#define OCEANBASE_SYSCHECKER_WRITE_WORKER_H_

#include <tbsys.h>
#include "client/ob_client.h"
#include "ob_syschecker_rule.h"
#include "ob_syschecker_stat.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    class ObWriteWorker : public tbsys::CDefaultRunnable
    {
    public:
      ObWriteWorker(client::ObClient& client, ObSyscheckerRule& rule,
                    ObSyscheckerStat& stat);
      ~ObWriteWorker();

    public:
      virtual void run(tbsys::CThread* thread, void* arg);

    private:
      int init(ObOpParam** write_param);
      int run_mix_op(const ObOpParam& write_param, common::ObMutator& mutator);
      void set_obj(const ObOpCellParam& cell_param, common::ObObj& obj);
      int set_mutator_add(const ObOpParam& write_param, 
                          common::ObMutator& mutator,
                          const ObOpCellParam& cell_param, 
                          const common::ObRowkey& row_key);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObWriteWorker);

      client::ObClient& client_;
      ObSyscheckerRule& write_rule_;
      ObSyscheckerStat& stat_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_WRITE_WORKER_H_
