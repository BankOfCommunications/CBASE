/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_cell_array_helper.h for help get cell from cell array or 
 * get param easier. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CELL_ARRAY_HELPER_H_ 
#define OCEANBASE_CHUNKSERVER_CELL_ARRAY_HELPER_H_

#include "common/ob_cell_array.h"
#include "common/ob_read_common_data.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObCellArrayHelper
    {
    public:
      ObCellArrayHelper();
      ObCellArrayHelper(common::ObCellArray & cell_array);
      ObCellArrayHelper(const common::ObGetParam & get_param);
      virtual ~ObCellArrayHelper()
      {
      }

      // get cell number
      int64_t get_cell_size()const;
      // get cell according to operator []
      const common::ObCellInfo & operator[](int64_t offset)const;

    private:
      mutable common::ObCellInfo ugly_temp_cell_;
      common::ObCellArray *cell_array_;
      const common::ObGetParam *get_param_;
      common::ObCellInfo default_cell_;
    };
  }
}

#endif //OCEANBASE_CHUNKSERVER_CELL_ARRAY_HELPER_H_ 
