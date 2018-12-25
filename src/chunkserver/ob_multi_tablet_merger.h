/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_multi_tablet_merger.h for merge multi-tablet. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_MULTI_TALBET_MERGER_H_
#define OCEANBASE_CHUNKSERVER_OB_MULTI_TALBET_MERGER_H_

#include "common/ob_tablet_info.h"
#include "sstable/ob_sstable_merger.h"
#include "ob_tablet.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    class ObTabletManager;

    class ObMultiTabletMerger
    {
      public:
        ObMultiTabletMerger();
        ~ObMultiTabletMerger();

        int merge_tablets(ObTabletManager& manager, 
          common::ObTabletReportInfoList& tablet_list, 
          const int64_t serving_version);

        void cleanup();

      private:
        void reset();
        int check_range_list_param(const common::ObTabletReportInfoList& tablet_list, 
          const int64_t serving_version);
        int acquire_tablets_and_readers(const common::ObTabletReportInfoList& tablet_list);
        int release_tablets();
        int get_new_sstable_path(common::ObString& sstable_path);
        int do_merge_sstable(const common::ObNewRange& new_range);
        int get_new_tablet_range(common::ObNewRange& new_range);
        int create_new_tablet(const common::ObNewRange& new_range, ObTablet*& new_tablet);
        int update_tablet_image(ObTablet* tablet);
        int fill_return_tablet_list(common::ObTabletReportInfoList& tablet_list, 
          const ObTablet& tablet);

      private:
        static const int64_t MAX_MERGE_TABLET_NUM = 64;

        DISALLOW_COPY_AND_ASSIGN(ObMultiTabletMerger);

        ObTabletManager* manager_;
        char             path_[common::OB_MAX_FILE_NAME_LENGTH];
        common::ObString sstable_path_;
        sstable::ObSSTableId sstable_id_;
        int64_t sstable_size_;

        ObTablet* merge_tablets_[MAX_MERGE_TABLET_NUM];
        common::ObArrayHelper<ObTablet*> tablet_array_;
        sstable::ObSSTableReader* sstable_readers_[MAX_MERGE_TABLET_NUM];
        common::ObArrayHelper<sstable::ObSSTableReader*> sstable_array_;
        int64_t max_tablet_seq_;

        sstable::ObSSTableMerger sstable_merger_;
    };
  } // namespace oceanbase::chunkserver
} // namespace Oceanbase

#endif // OCEANBASE_CHUNKSERVER_OB_MULTI_TALBET_MERGER_H_
