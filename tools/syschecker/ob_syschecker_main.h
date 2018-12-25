/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_main.h for define syschecker main scheduler. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_MAIN_H_
#define OCEANBASE_SYSCHECKER_MAIN_H_

#include "common/base_main.h"
#include "ob_syschecker.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    class ObSyscheckerMain : public common::BaseMain
    {
    protected:
      ObSyscheckerMain();

      virtual int do_work();
      virtual void do_signal(const int sig);

    public:
      static ObSyscheckerMain* get_instance();

      const ObSyschecker& get_syschecker() const 
      {
        return checker_; 
      }

      ObSyschecker& get_syschecker() 
      {
        return checker_; 
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSyscheckerMain);

      ObSyschecker checker_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_MAIN_H_

