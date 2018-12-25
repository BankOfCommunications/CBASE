/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compactsstable_cache.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_COMPACTSSTABLE_CACHE_H
#define OB_COMPACTSSTABLE_CACHE_H

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_scan_param.h"
#include "compactsstable/ob_compactsstable_mem.h"
#include "ob_tablet.h"
#include "ob_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;
    
    /** 
     * get frozen data of a tablet
     * 
     */
    class ObCompacSSTableMemGetter
    {
    public:
      ObCompacSSTableMemGetter(ObMergerRpcProxy& proxy);
      ~ObCompacSSTableMemGetter();

      /** 
       * get forzen data of *tablet*
       * 
       * @param tablet the tablet
       * @param data_version  the version of ups active mem tablet
       * 
       * @return 0 on success,otherwise on failure
       */
      int get(ObTablet* tablet,int64_t data_version);

      void reset();
      
    private:
      int fill_scan_param(ObTablet& tablet,int64_t data_version);
      void fill_version_range(const ObTablet& tablet,const int64_t data_version,
                              common::ObVersionRange& version_range) const;
      
      /** 
       * convert scanner to CompactSSTableMem
       * 
       * @param cell_strem the scanner
       * @param sstable
       * 
       * @return 0 on success,otherwise on failure
       */
      int fill_compact_sstable(ObScanCellStream& cell_strem,compactsstable::ObCompactSSTableMem& sstable);
      int save_row(uint64_t table_id,compactsstable::ObCompactSSTableMem& sstable);

    private:
      ObMergerRpcProxy&                 rpc_proxy_;
      common::ObScanParam               scan_param_;
      ObScanCellStream                  cell_stream_;
      compactsstable::ObCompactRow      row_;
      int64_t                           row_cell_num_;
    };

    class ObCompactSSTableMemThread : public tbsys::CDefaultRunnable
    {
    public:
      ObCompactSSTableMemThread();
      ~ObCompactSSTableMemThread();

      int init(ObTabletManager* tablet_manager);
      void destroy();

      int push(ObTablet* tablet,int64_t data_version);
      /*virtual*/ void run(tbsys::CThread *thread, void *arg);

    private:
      static const int32_t MAX_TABLETS_NUM = 1024;
      static const int64_t MAX_THREAD_COUNT = 16;
      ObTablet* pop();
    private:
      tbsys::CThreadCond       mutex_;
      ObTabletManager*         manager_;
      bool                     inited_;
      ObTablet*                tablets_[MAX_TABLETS_NUM];
      int32_t                  read_idx_;
      int32_t                  write_idx_;
      int32_t                  tablets_num_;
      int64_t                  data_version_;
    };
  } //end namespace chunkserver
} //end namespace oceanbase

#endif
