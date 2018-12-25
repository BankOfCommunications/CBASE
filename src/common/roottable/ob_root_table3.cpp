/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_table3.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_table3.h"
#include "common/utility.h"
using namespace oceanbase::common;

ObRootTable3::ObRootTable3(common::ObFirstTabletEntryMeta& the_meta, ObSchemaService &schema_service,
                           ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                           ObCachedAllocator<ObScanParam> &scan_param_allocator)
  :the_meta_(the_meta), scan_helper_(scan_helper),
   mutator_(), 
   first_table_(schema_service, the_meta_, mutator_, scan_helper, scanner_allocator, scan_param_allocator), 
   meta_table_(schema_service, mutator_, scan_helper, scanner_allocator, scan_param_allocator)
{
}

ObRootTable3::~ObRootTable3()
{
}

int ObRootTable3::search(const KeyRange& key, ConstIterator *&first)
{
  return search(key, first, 0);
}

int ObRootTable3::search(const KeyRange& key, ConstIterator *&first, int32_t recursive_level)
{
  int ret = OB_SUCCESS;
  ObString tname;
  uint64_t meta_tid = OB_INVALID_ID;
  ObString meta_tname;
  if (OB_SUCCESS != (ret = check_range(key)))
  {
    TBSYS_LOG(ERROR, "invalid search range, range=%s", to_cstring(key));
  }
  else if (recursive_level > MAX_RECURSIVE_LEVEL)
  {
    ret = OB_MAYBE_LOOP;
    TBSYS_LOG(ERROR, "too many recursive funcation call");
  }
  else if (OB_SUCCESS != (ret = first_table_.get_meta_tid(key.table_id_, tname, meta_tid, meta_tname))
           || OB_INVALID_ID == meta_tid)
  {
    TBSYS_LOG(DEBUG, "table not exist, tid=%lu", key.table_id_);
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get meta table, tid=%lu meta_tid=%lu", key.table_id_, meta_tid);
    if (OB_FIRST_META_VIRTUAL_TID == meta_tid)
    {
      the_meta_.search(key, first);
    }
    else if (OB_FIRST_TABLET_ENTRY_TID == meta_tid)
    {
      first_table_.search(key, first);
    }
    else
    {
      KeyRange meta_range = key;
      meta_range.table_id_ = meta_tid;
      ConstIterator *meta_it = NULL;
      ret = this->search(meta_range, meta_it, recursive_level + 1); // recursive
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to read meta table, tid=%lu", meta_tid);
      }
      else
      {
        meta_table_.reset(meta_it, meta_tid, meta_tname, key.table_id_, tname);
        ret = meta_table_.search(key, first);
      }
    }
  }
  return ret;
}

int ObRootTable3::aquire_meta_table_for_mutation(const uint64_t tid, common::ObTabletMetaTable* &meta_table)
{
  int ret = OB_SUCCESS;
  ObString tname;
  uint64_t meta_tid = OB_INVALID_ID;
  ObString meta_tname;
  if (OB_SUCCESS != (ret = first_table_.get_meta_tid(tid, tname, meta_tid, meta_tname))
           || OB_INVALID_ID == meta_tid)
  {
    TBSYS_LOG(DEBUG, "table not exist, tid=%lu", tid);
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get meta table, tid=%lu meta_tid=%lu", tid, meta_tid);
    if (OB_FIRST_META_VIRTUAL_TID == meta_tid)
    {
      meta_table = &the_meta_;
    }
    else if (OB_FIRST_TABLET_ENTRY_TID == meta_tid)
    {
      meta_table = &first_table_;
    }
    else
    {
      meta_table_.reset(NULL, meta_tid, meta_tname, tid, tname);
      meta_table = &meta_table_;
    }
  }
  return ret;
}

int ObRootTable3::insert(const Value &v)
{
  int ret = OB_SUCCESS;
  int loop = 0;
  ObTabletMetaTable *meta_table = NULL;
  do
  {
    if (OB_SUCCESS != (ret = aquire_meta_table_for_mutation(v.get_tid(), meta_table)))
    {
      TBSYS_LOG(WARN, "failed to aquire meta table for mutation, err=%d tid=%lu",
                ret, v.get_tid());
    }
    else
    {
      ret = meta_table->insert(v);
    }
    loop++;
    if (loop > 2)
    {
      TBSYS_LOG(ERROR, "egain too many times, loop=%d", loop);
      ret = OB_MAYBE_LOOP;
      break;
    }
  } 
  while (OB_EAGAIN == ret);
  
  return ret;
}

int ObRootTable3::erase(const Key &k)
{
  int ret = OB_SUCCESS;
  ObTabletMetaTable *meta_table = NULL;
  if (OB_SUCCESS != (ret = aquire_meta_table_for_mutation(k.table_id_, meta_table)))
  {
    TBSYS_LOG(WARN, "failed to aquire meta table for mutation, err=%d tid=%lu",
              ret, k.table_id_);
  }
  else
  {
    ret = meta_table->erase(k);
  }
  return ret;
}

int ObRootTable3::update(const Value &v)
{
  int ret = OB_SUCCESS;
  ObTabletMetaTable *meta_table = NULL;
  if (OB_SUCCESS != (ret = aquire_meta_table_for_mutation(v.get_tid(), meta_table)))
  {
    TBSYS_LOG(WARN, "failed to aquire meta table for mutation, err=%d tid=%lu",
              ret, v.get_tid());
  }
  else
  {
    ret = meta_table->update(v);
  }
  return ret;
}

int ObRootTable3::commit()
{
  int ret = OB_SUCCESS;
  mutator_.reset_iter();
  if (OB_ITER_END != mutator_.next_cell())
  {
    // mutator is not empty
    mutator_.reset_iter();
    if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator_)))
    {
      TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = the_meta_.commit()))
    {
      TBSYS_LOG(WARN, "failed to commit the meta");
    }
    else
    {
      mutator_.reset();
    }
  }
  return ret;
}
