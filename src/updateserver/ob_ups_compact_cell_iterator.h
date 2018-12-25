////===================================================================
 //
 // ob_ups_compact_cell_iterator.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-09 by Jianming (jianming.cjq@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef OCEANBASE_UPDATESERVER_COMPACT_CELL_ITERATOR_H_
#define OCEANBASE_UPDATESERVER_COMPACT_CELL_ITERATOR_H_

#include "common/ob_compact_cell_iterator.h"

namespace oceanbase
{
  using namespace common;

  namespace updateserver
  {
    class ObUpsCompactCellIterator : public ObCompactCellIterator
    {
      public:
        virtual int parse_varchar(ObBufferReader &buf_reader, ObObj &value) const;
    };
  }
}

#endif /* OCEANBASE_UPDATESERVER_COMPACT_CELL_ITERATOR_H_ */


