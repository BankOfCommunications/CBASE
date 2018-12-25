/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_table_service.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_table_service.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_tablet_info.h"
#include "common/ob_array.h"
#include "common/ob_range2.h"
using namespace oceanbase::common;

ObRootTableService::ObRootTableService(ObFirstTabletEntryMeta& the_meta, ObSchemaService &schema_service)
  :the_meta_(the_meta), schema_service_(schema_service), root_table_pool_(sizeof(ObRootTable3))
{
}

ObRootTableService::~ObRootTableService()
{
}

int ObRootTableService::check_integrity() const
{
  int ret = OB_SUCCESS;
  
  return ret;
}

int ObRootTableService::aquire_root_table(ObScanHelper &scan_helper, ObRootTable3 *&root_table)
{
  int ret = OB_SUCCESS;
  if (NULL != root_table)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "root table is not NULL");
  }
  else
  {
    void* p = root_table_pool_.alloc();
    if (NULL == p)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "no memory");
    }
    else
    {
      root_table = new(p) ObRootTable3(the_meta_, schema_service_, scan_helper, 
                                       scanner_allocator_, scan_param_allocator_);
    }
  }
  return ret;
}

void ObRootTableService::release_root_table(ObRootTable3 *&root_table)
{
  if (NULL != root_table)
  {
    root_table->~ObRootTable3();
    root_table_pool_.free(root_table);
    root_table = NULL;
  }
}

int ObRootTableService::report_tablets(ObScanHelper &scan_helper, const common::ObServer& cs, const common::ObTabletReportInfoList& rtablets)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "integrity error");
  }
  else
  {
    TBSYS_LOG(INFO, "cs report tablets, cs=%s tablets_num=%ld", cs.to_cstring(), rtablets.get_tablet_size());
    const ObTabletReportInfo* tablets = rtablets.get_tablet();
    for (int64_t i = 0; i < rtablets.get_tablet_size(); ++i)
    {
      const ObTabletReportInfo &tablet = tablets[i];
      if (OB_SUCCESS != (ret = report_tablet(scan_helper, tablet)))
      {
        TBSYS_LOG(WARN, "failed to report");
      }
    }
  }
  return ret;
}

bool ObRootTableService::did_range_contain(const ObTabletMetaTableRow &row, const ObTabletReportInfo& tablet) const
{
  return row.get_start_key() <= tablet.tablet_info_.range_.start_key_
    && row.get_end_key() >= tablet.tablet_info_.range_.end_key_;
}

int ObRootTableService::report_tablet(ObScanHelper &scan_helper, const ObTabletReportInfo& tablet)
{
  int ret = OB_SUCCESS;
  ObRootTable3::ConstIterator* first = NULL;
  ObRootTable3 *root_table = NULL;
  if (0 >= tablet.tablet_location_.tablet_version_
      || 0 == tablet.tablet_location_.chunkserver_.get_port()
      || NULL == tablet.tablet_info_.range_.start_key_.ptr()
      || NULL == tablet.tablet_info_.range_.end_key_.ptr())
  {
    TBSYS_LOG(WARN, "invalid tablet");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
  {
    TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = root_table->search(tablet.tablet_info_.range_, first)))
  {
    TBSYS_LOG(WARN, "failed to search tablet, err=%d range=%s", ret, to_cstring(tablet.tablet_info_.range_));
  }
  else
  {
    const ObTabletMetaTableRow *row = NULL;
    if (OB_ITER_END == (ret = first->next(row)))
    {
      // no tablet
      ret = add_new_tablet(*root_table, tablet);
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
    }
    else if (did_range_contain(*row, tablet))
    {
      // one tablet
      ret = tablet_one_to_n(*root_table, *row, tablet);
    }
    else
    {
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_ERR_UNEXPECTED;
    }
  }
   release_root_table(root_table);
   return ret;
 }

 int ObRootTableService::add_new_tablet(ObRootTable3& root_table, const ObTabletReportInfo& tablet)
 {
   int ret = OB_SUCCESS;

   ObTabletMetaTableRow row;
   row.set_tid(tablet.tablet_info_.range_.table_id_);
   row.set_start_key(tablet.tablet_info_.range_.start_key_);
   row.set_end_key(tablet.tablet_info_.range_.end_key_);
   ObTabletReplica replica;
   replica.version_ = tablet.tablet_location_.tablet_version_;
   replica.cs_ = tablet.tablet_location_.chunkserver_;
   replica.row_count_ = tablet.tablet_info_.row_count_;
   replica.occupy_size_ = tablet.tablet_info_.occupy_size_;
   replica.checksum_ = tablet.tablet_info_.crc_sum_;
   row.set_replica(0, replica);

   if (OB_SUCCESS != (ret = root_table.insert(row)))
   {
     TBSYS_LOG(WARN, "failed to insert row, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table.commit()))
   {
     TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
   }
   return ret;
 }

 int ObRootTableService::tablet_one_to_n(ObRootTable3& root_table, const ObRootTable3::Value& row, 
                                         const ObTabletReportInfo& tablet)
 {
   int ret = OB_SUCCESS;
   // @todo split one to N(>2)
   if (tablet.tablet_info_.range_.start_key_ == row.get_start_key())
   {
     if (tablet.tablet_info_.range_.end_key_ == row.get_end_key())
     {
       ret = tablet_one_to_one(root_table, row, tablet);
     }
     else if (tablet.tablet_info_.range_.end_key_ < row.get_end_key())
     {
       ret = tablet_one_to_two_same_startkey(root_table, row, tablet);
     }
     else
     {
       TBSYS_LOG(ERROR, "unexpected branch");
       ret = OB_ERR_UNEXPECTED;
     }
   }
   else if (tablet.tablet_info_.range_.end_key_ == row.get_end_key())
   {
     if (tablet.tablet_info_.range_.start_key_ == row.get_start_key())
     {
       ret = tablet_one_to_one(root_table, row, tablet);
     }
     else if (tablet.tablet_info_.range_.start_key_ > row.get_start_key())
     {
       ret = tablet_one_to_two_same_endkey(root_table, row, tablet);
     }
     else
     {
       TBSYS_LOG(ERROR, "unexpected branch");
       ret = OB_ERR_UNEXPECTED;
     }
   }
   else
   {
     TBSYS_LOG(ERROR, "unexpected branch");
     ret = OB_ERR_UNEXPECTED;
   }
   return ret;
 }

 int ObRootTableService::tablet_one_to_one(ObRootTable3& root_table, const ObRootTable3::Value& crow, 
                                           const ObTabletReportInfo& tablet)
 {
   int ret = OB_SUCCESS;
   bool found = false;
   ObRootTable3::Value row = crow; // copy
   for (int i = 0; i < row.get_max_replica_count(); ++i)
   {
     const ObTabletReplica& replica = row.get_replica(i);
     if (replica.cs_ == tablet.tablet_location_.chunkserver_)
     {
       found = true;
       if (replica.version_ < tablet.tablet_location_.tablet_version_)
       {
         ObTabletReplica replica2;
         replica2.version_ = tablet.tablet_location_.tablet_version_;
         replica2.row_count_ = tablet.tablet_info_.row_count_;
         replica2.occupy_size_ = tablet.tablet_info_.occupy_size_;
         replica2.checksum_ = tablet.tablet_info_.crc_sum_;
         row.set_replica(i, replica2);
       }
       else if (replica.version_ > tablet.tablet_location_.tablet_version_)
       {
         TBSYS_LOG(WARN, "cs reported an old replica, cs=%s version=%ld report_v=%ld",
                   to_cstring(tablet.tablet_location_.chunkserver_), replica.version_,
                   tablet.tablet_location_.tablet_version_);
       }
       else
       {
         // same version, just check the checksum
         if (0 == replica.checksum_)
         {
           ObTabletReplica replica2;
           replica2.row_count_ = tablet.tablet_info_.row_count_;
           replica2.occupy_size_ = tablet.tablet_info_.occupy_size_;
           replica2.checksum_ = tablet.tablet_info_.crc_sum_;
           row.set_replica(i, replica2);
         }
         else if (replica.checksum_ != tablet.tablet_info_.crc_sum_)
         {
           TBSYS_LOG(ERROR, "check sum error, ver=%ld old_crc=%lu new_crc=%lu cs=%s endkey=%s", 
                     replica.version_, replica.checksum_, tablet.tablet_info_.crc_sum_, 
                     tablet.tablet_location_.chunkserver_.to_cstring(), 
                     to_cstring(tablet.tablet_info_.range_.end_key_));
         }
       }
       break;
     }
   } // end for
   if (!found)
   {
     // insert new replica
     for (int i = 0; i < row.get_max_replica_count(); ++i)
     {
       const ObTabletReplica& replica = row.get_replica(i);
       if (0 == replica.cs_.get_port())
       {
         found = true;
         // found an invalid entry
         ObTabletReplica replica2(tablet);
         row.set_replica(i, replica2);
         break;
       }
     }
     if (!found)
     {
       TBSYS_LOG(WARN, "no entry for replica, cs=%s endkey=%s",
                 tablet.tablet_location_.chunkserver_.to_cstring(),
                 to_cstring(tablet.tablet_info_.range_.end_key_));
     }
   }
   if (row.has_changed())
   {
     if (OB_SUCCESS != (ret = root_table.update(row)))
     {
       TBSYS_LOG(ERROR, "failed to update root table");
     }
     else if (OB_SUCCESS != (ret = root_table.commit()))
     {
       TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
     }
   }
   return ret;
 }

 int ObRootTableService::tablet_one_to_two_same_endkey(ObRootTable3& root_table, const ObRootTable3::Value& crow, 
                                                       const ObTabletReportInfo& tablet)
 {
   int ret = OB_SUCCESS;
   ObTabletMetaTableRow prev_row = crow;
   ObTabletMetaTableRow row = crow;
   row.set_start_key(tablet.tablet_info_.range_.start_key_);
   for (int32_t i = 0; i < row.get_max_replica_count(); ++i)
   {
     const ObTabletReplica& replica = row.get_replica(i);
     if (0 != replica.cs_.get_port())
     {
       ObTabletReplica replica2(tablet);
       replica2.cs_ = replica.cs_;
       row.set_replica(i, replica2);
     }
   }
   prev_row.set_end_key(tablet.tablet_info_.range_.start_key_);
   for (int32_t i = 0; i < prev_row.get_max_replica_count(); ++i)
   {
     const ObTabletReplica& replica = prev_row.get_replica(i);
     if (0 != replica.cs_.get_port())
     {
       ObTabletReplica replica2;
       replica2.cs_ = replica.cs_;
       replica2.version_ = replica.version_;
       replica2.row_count_ = 0;   // unknown
       replica2.occupy_size_ = 0; // unknown
       replica2.checksum_ = 0;    // unknown
       prev_row.set_replica(i, replica2);
     }
   }
   if (OB_SUCCESS != (ret = root_table.insert(prev_row)))
   {
     TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table.update(row)))
   {
     TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table.commit()))
   {
     TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
   }
   return ret;
 }

 int ObRootTableService::tablet_one_to_two_same_startkey(ObRootTable3 &root_table, const ObRootTable3::Value &crow, 
                                                         const ObTabletReportInfo& tablet)
 {
   int ret = OB_SUCCESS;
   ObTabletMetaTableRow next_row = crow;
   ObTabletMetaTableRow row = crow;
   row.set_end_key(tablet.tablet_info_.range_.end_key_);
   for (int32_t i = 0; i < row.get_max_replica_count(); ++i)
   {
     const ObTabletReplica& replica = row.get_replica(i);
     if (0 != replica.cs_.get_port())
     {
       ObTabletReplica replica2(tablet);
       replica2.cs_ = replica.cs_;
       row.set_replica(i, replica2);
     }
   }
   next_row.set_start_key(tablet.tablet_info_.range_.end_key_);
   for (int32_t i = 0; i < next_row.get_max_replica_count(); ++i)
   {
     const ObTabletReplica& replica = next_row.get_replica(i);
     if (0 != replica.cs_.get_port())
     {
       ObTabletReplica replica2;
       replica2.cs_ = replica.cs_;
       replica2.version_ = replica.version_;
       replica2.row_count_ = 0;   // unknown
       replica2.occupy_size_ = 0; // unknown
       replica2.checksum_ = 0;    // unknown
       next_row.set_replica(i, replica2);
     }
   }
   if (OB_SUCCESS != (ret = root_table.insert(row)))
   {
     TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table.update(next_row)))
   {
     TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table.commit()))
   {
     TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
   }
   return ret;
 }

 int ObRootTableService::remove_replicas(ObScanHelper &scan_helper, const common::ObServer &cs)
 {
   int ret = OB_SUCCESS;
   if (0 == cs.get_port())
   {
     TBSYS_LOG(WARN, "invalid cs port");
     ret = OB_INVALID_ARGUMENT;
   }
   else if (OB_SUCCESS != (ret = check_integrity()))
   {
     TBSYS_LOG(ERROR, "integrity error");
   }
   else
   {
     // @todo for each table
     remove_replicas_in_table(scan_helper, cs, 0);
   }
   return ret;
 }

 int ObRootTableService::remove_replicas_in_table(ObScanHelper &scan_helper, const common::ObServer &cs, const uint64_t tid)
 {
   int ret = OB_SUCCESS;
   ObRootTable3 *root_table = NULL;
   ObNewRange range_min_max;
   range_min_max.table_id_ = tid;
   range_min_max.set_whole_range();
   ObRootTable3::ConstIterator* it = NULL;
   const ObRootTable3::Value* crow = NULL;
   if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
   {
     TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table->search(range_min_max, it)))
   {
     TBSYS_LOG(WARN, "failed to search tablet, err=%d tid=%lu", ret, tid);
   }
   else
   {
     int32_t deleted_replicas = 0;
     int32_t updated_count = 0;
     const int32_t batch_count = 512;
     while(OB_SUCCESS == ret && OB_SUCCESS == (ret = it->next(crow)))
     {
       for (int32_t i = 0; i < crow->get_max_replica_count(); ++i)
       {
         const ObTabletReplica& replica = crow->get_replica(i);
         if (replica.cs_ == cs)
         {
           ObTabletReplica replica2; // remove the replica
           ObRootTable3::Value mrow = *crow;
           mrow.set_replica(i, replica2);
           if (OB_SUCCESS != (ret = root_table->update(mrow)))
           {
             TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
           }
           else
           {
             updated_count++;
             if (updated_count % batch_count == 0)
             {
               if (OB_SUCCESS != (ret = root_table->commit()))
               {
                 TBSYS_LOG(ERROR, "failed to commit change, err=%d", ret);
               }
               else
               {
                 deleted_replicas += batch_count;
               }
             }
           }
           break;
         }
       } // end for
     } // end while

     if (OB_ITER_END != ret)
     {
       TBSYS_LOG(WARN, "failed to remove replicas, err=%d", ret);
     }
     else
     {
       ret = OB_SUCCESS;
     }
     if (updated_count != deleted_replicas)
     {
       if (OB_SUCCESS != (ret = root_table->commit()))
       {
         TBSYS_LOG(ERROR, "failed to commit change, err=%d", ret);
       }
     }
     TBSYS_LOG(INFO, "delete replicas by cs, cs=%s replicas_num=%d", 
               cs.to_cstring(), deleted_replicas);
   }
   release_root_table(root_table);
   return ret;
 }

 bool ObRootTableService::is_same_range(const ObTabletMetaTableRow &row, const ObNewRange &range) const
 {
   return row.get_start_key() == range.start_key_
     && row.get_end_key() == range.end_key_;
 }

int ObRootTableService::migrate_replica(ObScanHelper &scan_helper, const ObNewRange &range, const int64_t version, 
                                         const common::ObServer &from, const common::ObServer &to, bool keep_src)
 {
   int ret = OB_SUCCESS;
   ObRootTable3::ConstIterator *first;
   ObRootTable3 *root_table = NULL;
   const ObRootTable3::Value* crow = NULL;

   if (NULL == range.start_key_.ptr()
       || NULL == range.end_key_.ptr()
       || 0 >= version
       || 0 == from.get_port()
       || 0 == to.get_port())
   {
     TBSYS_LOG(WARN, "invalid tablet");
     ret = OB_INVALID_ARGUMENT;
   }
   else if (OB_SUCCESS != (ret = check_integrity()))
   {
     TBSYS_LOG(ERROR, "integrity error");
   }
   else if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
   {
     TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = root_table->search(range, first)))
   {
     TBSYS_LOG(WARN, "failed to search tablet, err=%d", ret);
   }
   else if (OB_SUCCESS != (ret = first->next(crow)))
   {
     TBSYS_LOG(WARN, "tablet not exist, err=%d range=%s", ret, to_cstring(range));
     ret = OB_ENTRY_NOT_EXIST;
   }
   else if (!is_same_range(*crow, range))
   {
     ret = OB_ENTRY_NOT_EXIST;
     TBSYS_LOG(WARN, "tablet not exist, range=%s", to_cstring(range));
   }
   else
   {
     for (int32_t i = 0; i < crow->get_max_replica_count(); ++i)
     {
       const ObTabletReplica& replica = crow->get_replica(i);
       if (from == replica.cs_)
       {
         if (replica.version_ != version)
         {
           TBSYS_LOG(WARN, "migrate tablet with wrong version, old_v=%ld new_v=%ld cs=%s range=%s",
                     replica.version_, version, from.to_cstring(), to_cstring(range));
           ret = OB_CONFLICT_VALUE;
         }
         else
         {
           ObTabletMetaTable::Value new_row = *crow; // copy
           ObTabletReplica replica2(replica);
           replica2.cs_ = to;
           if (keep_src)
           {
             if (OB_SUCCESS != (ret = new_row.add_replica(replica2)))
             {
               TBSYS_LOG(WARN, "failed to add replica, err=%d", ret);
             }
           }
           else
           {
             new_row.set_replica(i, replica2);
           }
           if (OB_SUCCESS == ret)
           {
             if (OB_SUCCESS != (ret = root_table->update(new_row)))
             {
               TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
             }
             else if (OB_SUCCESS != (ret = root_table->commit()))
             {
               TBSYS_LOG(ERROR, "failed to commit change, err=%d", ret);
             }
           }
         }
         break;
      }
     } // end for
   }
  release_root_table(root_table);
  return ret;
}

int ObRootTableService::remove_tables(ObScanHelper &scan_helper, const common::ObArray<uint64_t> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "integrity error");
  }
  else
  {
    for (int32_t i = 0; i < tables.count(); ++i)
    {
      const uint64_t &tid = tables.at(i);
      if (OB_SUCCESS != (ret = this->remove_table(scan_helper, tid)))
      {
        TBSYS_LOG(ERROR, "failed to remove table, err=%d tid=%ld",
                  ret, tid);
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "removed table, tid=%ld", tid);
      }
    }
  }
  return ret;
}

int ObRootTableService::remove_table(ObScanHelper &scan_helper, const uint64_t &tid)
{
  int ret = OB_SUCCESS;
  ObRootTable3 *root_table = NULL;
  const ObRootTable3::Value* crow = NULL;
  ObNewRange range_min_max;
  range_min_max.table_id_ = tid;
  range_min_max.set_whole_range();
  ObRootTable3::ConstIterator* first = NULL;
  if (OB_INVALID_ID == tid)
  {
    TBSYS_LOG(WARN, "invalid table id");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
  {
    TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = root_table->search(range_min_max, first)))
  {
    TBSYS_LOG(WARN, "failed to search tablet, err=%d", ret);
  }
  else
  {
    const int32_t batch_count = 512;
    int32_t erased_count = 0;
    int32_t updated_count = 0;
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = first->next(crow)))
    {
      ObRootTable3::Key key;
      key.table_id_ = crow->get_tid();
      key.rowkey_ = crow->get_end_key();
      if (OB_SUCCESS != (ret = root_table->erase(key)))
      {
        TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
        break;
      }
      else
      {
        updated_count++;
        if (updated_count % batch_count == 0)
        {
          if (OB_SUCCESS != (ret = root_table->commit()))
          {
            TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
          }
          else
          {
            erased_count += batch_count;
          }
        }
      }
    } // end while
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to remove table, err=%d range=%s", ret, to_cstring(range_min_max));
    }
    else
    {
      ret = OB_SUCCESS;
    }
    if (updated_count != erased_count)
    {
      if (OB_SUCCESS != (ret = root_table->commit()))
      {
        TBSYS_LOG(WARN, "failed to commit change, err=%d", ret);
      }
    }
    TBSYS_LOG(INFO, "remove table, tid=%lu removed_tablets_count=%d", tid, updated_count);
  }
  release_root_table(root_table);
  // @todo remove my meta table
  return ret;
}

int ObRootTableService::search_range(ObScanHelper &scan_helper, const common::ObNewRange& range, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  ObRootTable3 *root_table = NULL;
  const ObRootTable3::Value* crow = NULL;
  ObRootTable3::ConstIterator *first = NULL;
  if (NULL == range.start_key_.ptr()
      || NULL == range.end_key_.ptr())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid range");
  }
  else if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "integrity error");
  }
  else if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
  {
    TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = root_table->search(range, first)))
  {
    TBSYS_LOG(WARN, "failed to search range, err=%d range=%s", ret, to_cstring(range));
  }
  else
  {
    while(OB_SUCCESS == ret 
          && (OB_SUCCESS == (ret = first->next(crow))))
    {
      if (OB_SUCCESS != (ret = this->add_row_to_scanner(*crow, scanner)))
      {
        TBSYS_LOG(WARN, "failed to add row into scanner, err=%d", ret);
        break;
      }
    }
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  release_root_table(root_table);
  return ret;
}

int ObRootTableService::add_row_to_scanner(const ObRootTable3::Value& row, common::ObScanner& scanner) const
{
  int ret = OB_SUCCESS;
  ret = row.add_into_scanner_as_old_roottable(scanner);
  return ret;
}

int ObRootTableService::new_table(ObScanHelper &scan_helper, const uint64_t tid, const int64_t tablet_version, const ObArray<ObServer> &chunkservers)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tid
      || OB_FIRST_META_VIRTUAL_TID == tid)
  {
    TBSYS_LOG(WARN, "invalid table id=%lu", tid);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > tablet_version)
  {
    TBSYS_LOG(WARN, "invalid tablet version=%ld", tablet_version);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 >= chunkservers.count())
  {
    TBSYS_LOG(WARN, "invalid chunkserver count=%ld", chunkservers.count());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootTable3 *root_table = NULL;
    const ObRootTable3::Value* crow = NULL;
    ObNewRange range_min_max;
    range_min_max.table_id_ = tid;
    range_min_max.set_whole_range();
    ObRootTable3::ConstIterator* first = NULL;

    if (OB_INVALID_ID == tid)
    {
      TBSYS_LOG(WARN, "invalid table id");
      ret = OB_INVALID_ARGUMENT;
    }
    else if (OB_SUCCESS != (ret = aquire_root_table(scan_helper, root_table)))
    {
      TBSYS_LOG(ERROR, "failed to aquire root table, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = root_table->search(range_min_max, first)))
    {
      TBSYS_LOG(WARN, "failed to search tablet, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = first->next(crow)))
    {
      TBSYS_LOG(ERROR, "no row found for the new table, err=%d", ret);
    }
    else
    {
      ObTabletMetaTable::Value new_row = *crow; // copy
      for (int i = 0; i < chunkservers.count() && i < ObTabletMetaTable::Value::get_max_replica_count(); ++i)
      {
        const ObTabletReplica& replica = crow->get_replica(i);
        ObTabletReplica new_replica(replica);
        new_replica.cs_ = chunkservers.at(i);
        new_replica.version_ = tablet_version;
        new_replica.row_count_ = 0;
        new_replica.occupy_size_ = 0;
        new_replica.checksum_ = 0;
        new_row.set_replica(i, new_replica);
      }
      if (OB_SUCCESS != (ret = root_table->update(new_row)))
      {
        TBSYS_LOG(ERROR, "failed to update root table, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_table->commit()))
      {
        TBSYS_LOG(ERROR, "failed to commit change, err=%d", ret);
      }
    }
    release_root_table(root_table);  
  }
  return ret;
}
