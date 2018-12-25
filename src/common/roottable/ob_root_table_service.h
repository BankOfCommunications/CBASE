/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_table_service.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_TABLE_SERVICE_H
#define _OB_ROOT_TABLE_SERVICE_H 1
#include "ob_root_table3.h"
#include "common/ob_define.h"
namespace oceanbase
{
  namespace common
  {
    class ObServer;
    class ObTabletReportInfoList;
    template<typename T, typename BlockAllocatorT, typename CallBack> class ObArray;
    class ObScanner;
    class ObTabletReportInfo;
  } // end namespace common

  namespace common
  {
    /// @note thread-safe, use as a singleton
    class ObRootTableService
    {
      public:
        /// the_meta and schema_service should be thread-safe
        ObRootTableService(ObFirstTabletEntryMeta& the_meta, ObSchemaService &schema_service);
        virtual ~ObRootTableService();

        /// @param scan_helper must be a thread-safe or thread-local object
        int report_tablets(ObScanHelper &scan_helper, const ObServer& cs, const ObTabletReportInfoList& tablets);
        int remove_replicas(ObScanHelper &scan_helper, const ObServer &cs);
        int migrate_replica(ObScanHelper &scan_helper, const ObNewRange &tab, const int64_t version,
                            const ObServer &from, const ObServer &to, bool keep_src);
        int remove_tables(ObScanHelper &scan_helper, const ObArray<uint64_t> &tables);
        int new_table(ObScanHelper &scan_helper, const uint64_t tid, const int64_t tablet_version,
                      const ObArray<ObServer> &chunkservers);
        //
        int search_range(ObScanHelper &scan_helper, const ObNewRange& range, ObScanner& scanner);
        //
        int aquire_root_table(ObScanHelper &scan_helper, ObRootTable3 *&root_table);
        void release_root_table(ObRootTable3 *&root_table);
      private:
        int check_integrity() const;
        int report_tablet(ObScanHelper &scan_helper, const ObTabletReportInfo& tablet);
        int add_new_tablet(ObRootTable3 &root_table, const ObTabletReportInfo& tablet);
        int tablet_one_to_n(ObRootTable3 &root_table, const ObRootTable3::Value& row, const ObTabletReportInfo& tablet);
        int tablet_one_to_one(ObRootTable3 &root_table, const ObRootTable3::Value& row, const ObTabletReportInfo& tablet);
        int tablet_one_to_two_same_endkey(ObRootTable3 &root_table, const ObRootTable3::Value& row,
                                          const ObTabletReportInfo& tablet);
        int tablet_one_to_two_same_startkey(ObRootTable3 &root_table, const ObRootTable3::Value& row,
                                            const ObTabletReportInfo& tablet);
        bool did_range_contain(const ObTabletMetaTableRow &row, const ObTabletReportInfo& tablet) const;
        bool is_same_range(const ObTabletMetaTableRow &row, const ObNewRange &range) const;
        int remove_replicas_in_table(ObScanHelper &scan_helper, const ObServer &cs, const uint64_t tid);
        int remove_table(ObScanHelper &scan_helper, const uint64_t &tid);
        int add_row_to_scanner(const ObRootTable3::Value& row, ObScanner& scanner) const;
        // disallow copy
        ObRootTableService(const ObRootTableService &other);
        ObRootTableService& operator=(const ObRootTableService &other);
      private:
        // data members
        ObFirstTabletEntryMeta& the_meta_;
        ObSchemaService &schema_service_;
        ObCachedAllocator<ObScanner> scanner_allocator_;
        ObCachedAllocator<ObScanParam> scan_param_allocator_;
        ObLockedPool root_table_pool_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROOT_TABLE_SERVICE_H */
