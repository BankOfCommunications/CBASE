/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_param_cell_iterator.h for iterate cell in get param. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_GET_PARAM_CELL_ITERATOR_H_ 
#define OCEANBASE_CHUNKSERVER_GET_PARAM_CELL_ITERATOR_H_

#include "common/ob_get_param.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObGetParamCellIterator
    {
    public:
      ObGetParamCellIterator();
      ~ObGetParamCellIterator();
      int init(const common::ObGetParam & get_param, int64_t cell_idx);
      ObGetParamCellIterator &operator++();
      ObGetParamCellIterator operator++(int);
      bool operator==(const ObGetParamCellIterator& other);
      bool operator!=(const ObGetParamCellIterator& other);
      common::ObCellInfo *operator->();
      common::ObCellInfo &operator*();

    private:
      static common::ObCellInfo fake_cell_;

      const common::ObGetParam *get_param_;
      int64_t           cell_size_;
      int64_t cell_idx_;
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_GET_PARAM_CELL_ITERATOR_H_
