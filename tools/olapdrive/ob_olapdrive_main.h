/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_main.h for define olapdrive main olapdrive. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_MAIN_H
#define OCEANBASE_OLAPDRIVE_MAIN_H

#include "common/base_main.h"
#include "ob_olapdrive.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObOlapdriveMain : public common::BaseMain
    {
    protected:
      ObOlapdriveMain();

      virtual int do_work();
      virtual void do_signal(const int sig);

    public:
      static ObOlapdriveMain* get_instance();

      const ObOlapdrive& get_olapdrive() const 
      {
        return drive_; 
      }

      ObOlapdrive& get_olapdrive() 
      {
        return drive_; 
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObOlapdriveMain);

      ObOlapdrive drive_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_MAIN_H

