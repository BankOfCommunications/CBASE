/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_CHUNKSERVER_H_
#define OCEANBASE_CHUNKSERVER_H_

#include "common/ob_iterator.h"
#include "sstable/ob_sstable_scanner.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {

    class ObMultiVersionTabletImage;
    class ObTablet;
    class ObTabletManager;

    class ObMergeReader : public common::ObIterator
    {
      public:
        ObMergeReader(ObTabletManager& manager);
        virtual ~ObMergeReader() ;
      public:
        // Moves the cursor to next cell.
        // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
        virtual int next_cell();
        // Gets the current cell.
        virtual int get_cell(common::ObCellInfo** cell);
        virtual int get_cell(common::ObCellInfo** cell, bool* is_row_changed);
      public:
        int scan(const oceanbase::common::ObScanParam &scan_param);
        void reset();
      private:
        bool initialized_;
        ObTablet* tablet_;
        ObMultiVersionTabletImage& tablet_image_;
        ObTabletManager& manager_;
        sstable::ObSSTableScanner scanner_;
    };
  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_H_

