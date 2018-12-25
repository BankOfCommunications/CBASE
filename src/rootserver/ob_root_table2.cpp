/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-10-21
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 *
 ================================================================*/
#include <algorithm>
#include <stdlib.h>
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "common/ob_record_header.h"
#include "common/file_utils.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "ob_root_balancer.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace
{
  const int SPLIT_TYPE_ERROR = -1;
  const int SPLIT_TYPE_TOP_HALF = 1;
  const int SPLIT_TYPE_BOTTOM_HALF = 2;
  const int SPLIT_TYPE_MIDDLE_HALF = 3;
}

ObRootTable2::ObRootTable2(ObTabletInfoManager* tim):tablet_info_manager_(tim), sorted_count_(0)
{
  meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_);
}

ObRootTable2::~ObRootTable2()
{
  tablet_info_manager_ = NULL;
  sorted_count_ = 0;
}
 void ObRootTable2::clear()
    {
      meta_table_.clear();
      sorted_count_ = 0;
      tablet_info_manager_->clear();
    }

bool ObRootTable2::has_been_sorted() const
{
  return 0 != sorted_count_;
}

bool ObRootTable2::internal_check() const
{
  bool ret = true;
  if (NULL == tablet_info_manager_)
  {
    TBSYS_LOG(ERROR, "tablet_info_manager_ not inited");
    ret = false;
  }
//  else if (!has_been_sorted())
//  {
//    TBSYS_LOG(INFO, "not sorted");
//    ret = false;
//  }
  return ret;
}

int ObRootTable2::find_range(const common::ObNewRange& range,
    const_iterator& first,
    const_iterator& last) const
{
  int ret = OB_NOT_INIT;
  if (!internal_check())
  {
    ret = OB_ERROR;
  }
  else if (begin() == end())
  {
    TBSYS_LOG(INFO, "root table is empty");
    first = begin();
    last = begin();
    ret = OB_FIND_OUT_OF_RANGE;
  }
  else
  {
    // find start_key in whole range scope
    ObNewRange start_range;
    start_range.table_id_  = range.table_id_;
    start_range.end_key_ = range.start_key_;
    const common::ObTabletInfo* tablet_info = NULL;
    ObRootMeta2RangeLessThan meta_comp(tablet_info_manager_); // compare_with_endkey
    //const_iterator start_pos = std::lower_bound(begin(), sorted_end(), start_range, meta_comp);
    const_iterator start_pos = std::lower_bound(begin(), end(), start_range, meta_comp);
    //if (NULL == start_pos || start_pos == sorted_end())
    if (NULL == start_pos || start_pos == end())
    {
      TBSYS_LOG(DEBUG, "find start pos error find_start_pos = %p end = %p", start_pos, end());
      ret = OB_FIND_OUT_OF_RANGE;
      first = end();
      last = end();
    }
    else if (NULL == (tablet_info = get_tablet_info(start_pos)))
    {
      TBSYS_LOG(DEBUG, "get_table_info null");
      ret = OB_ERROR_OUT_OF_RANGE;
    }
    else
    {
      first = start_pos;
      // right on one of range's end boundary? skip this tablet
      if (!range.start_key_.is_min_row()
          && !tablet_info->range_.end_key_.is_max_row()
          && 0 == range.start_key_.compare(tablet_info->range_.end_key_)
          && !range.border_flag_.inclusive_start())
      {
        ++first;
        //if (first >= sorted_end())
        if (first >= end())
        {
          TBSYS_LOG(DEBUG, "first[%p] >= end[%p] ", first, end());
          ret = OB_FIND_OUT_OF_RANGE;
        }
        else if (NULL == (tablet_info = get_tablet_info(first)))
        {
          TBSYS_LOG(DEBUG, "get_tablet_info null");
          ret = OB_ERROR_OUT_OF_RANGE;
        }
      }
      // find the last range
      if (OB_NOT_INIT == ret)
      {
        //last = std::lower_bound(begin(), sorted_end(), range, meta_comp);
        last = std::lower_bound(begin(), end(), range, meta_comp);
        //if (NULL == last || last == sorted_end())
        if (NULL == last || last == end())
        {
          TBSYS_LOG(DEBUG, "last[%p] == end[%p] ", last, end());
          last = end();
          ret = OB_FIND_OUT_OF_RANGE;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCCESS == ret && first > last)
      {
        TBSYS_LOG(DEBUG, "first[%p] > last[%p] ", first, last);
        ret = OB_ERROR_OUT_OF_RANGE;
      }
    }
  }
  if (OB_NOT_INIT == ret)
  {
    ret = OB_ERROR_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "no proper range found");
  }
  return ret;
}


int ObRootTable2::find_key(const uint64_t table_id,
    const common::ObRowkey& key,
    int32_t adjacent_offset,
    const_iterator& first,
    const_iterator& last,
    const_iterator& ptr) const
{
  int ret = OB_SUCCESS;
  if (!internal_check())
  {
    ret = OB_ERROR;
  }
  else
  {
    if (adjacent_offset < 0) adjacent_offset = 0;

    // find start_key in whole range scope
    common::ObNewRange find_range;
    find_range.table_id_ = table_id;
    find_range.end_key_ = key;
    const common::ObTabletInfo* tablet_info = NULL;
    ObRootMeta2RangeLessThan meta_comp(tablet_info_manager_); // compare_with_endkey
    //const_iterator find_pos = std::lower_bound(begin(), sorted_end(), find_range, meta_comp);
    const_iterator find_pos = std::lower_bound(begin(), end(), find_range, meta_comp);
   // if (table_id == 3)
   //   assert(false);
    if (find_pos == NULL || find_pos == end())
    {
      TBSYS_LOG(WARN, "not find. find_range=%s, find_pos=%p, begin()=%p, end()=%p",
          common::to_cstring(find_range), find_pos, begin(), end());
      ret = OB_ERROR_OUT_OF_RANGE;
    }
    else if (NULL == (tablet_info = get_tablet_info(find_pos)))
    {
      ret = OB_ERROR_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "get tablet_info fail. err =%d", ret);
    }
    else
    {
      ptr = find_pos;
      int64_t max_left_offset = ptr - begin();
      int64_t max_right_offset = end() - ptr - 1;
      if (max_left_offset > 1)
        max_left_offset = 1;
      if (max_right_offset > adjacent_offset)
        max_right_offset = adjacent_offset;

      // return pointers of tablets.
      first = ptr - max_left_offset;
      const common::ObTabletInfo* tablet_info = NULL;

      if (OB_SUCCESS == ret)
      {
        tablet_info = get_tablet_info(first);
        if (NULL == tablet_info || tablet_info->range_.table_id_ != table_id)
        {
          first = ptr; //previes is not the same table
        }
      }
      if (OB_SUCCESS == ret)
      {
        tablet_info = get_tablet_info(first);
        ObNewRange start_range;
        start_range.table_id_  = table_id;
        start_range.start_key_ = key;
        if (NULL == tablet_info || tablet_info->range_.compare_with_startkey(start_range) > 0)
        {
          static char rowkey[2 * OB_MAX_ROW_KEY_LENGTH];
          tablet_info->range_.to_string(rowkey, 2 * OB_MAX_ROW_KEY_LENGTH);
          //tablet_info->range_.start_key_.to_string(rowkey, OB_MAX_ROW_KEY_LENGTH);
          static char start_key[OB_MAX_ROW_KEY_LENGTH];
          key.to_string(start_key, OB_MAX_ROW_KEY_LENGTH);
          TBSYS_LOG(WARN, "find range not exist.key=%s, find_pos.range=%s", start_key, rowkey);
          ret = OB_FIND_OUT_OF_RANGE;
        }
      }
      if (OB_SUCCESS == ret)
      {
        last = ptr + max_right_offset;
        while (last >= first)
        {
          tablet_info = get_tablet_info(last);
          if (NULL != tablet_info && tablet_info->range_.table_id_ == table_id)
          {
            break;
          }
          last--;
        }
        if (last < first)
        {
          TBSYS_LOG(WARN, "table not exist in root_table. table_id=%lu", table_id);
          ret = OB_FIND_OUT_OF_RANGE;
        }
      }
    }
  }
  return ret;
}


int ObRootTable2::get_table_row_checksum(const int64_t tablet_version, const uint64_t table_id, uint64_t &row_checksum)
{
  int ret = OB_SUCCESS;
  common::ObNewRange range;
  range.table_id_ = table_id;
  const common::ObTabletInfo *tablet_info = NULL;
  const common::ObTabletInfo *last_tablet_info = NULL;
  // if table is exist, we should find at lest one range with this table id
  ObRootMeta2TableIdLessThan meta_table_id_comp(tablet_info_manager_); // compare with table id
  const_iterator start_pos = std::lower_bound(begin(), end(), range, meta_table_id_comp);

  if (start_pos != end())
  {
    const ObTabletCrcHistoryHelper* crc_helper = NULL;
    uint64_t tmp_row_checksum = 0;
    for(const_iterator it = start_pos; it != end() && OB_SUCCESS == ret; ++it)
    {
      tmp_row_checksum = 0;
      tablet_info = get_tablet_info(it);
      if(NULL != tablet_info)
      {
        if(table_id != tablet_info->range_.table_id_)
        {
          break;
        }
        else if(NULL != last_tablet_info && tablet_info->range_.start_key_ != last_tablet_info->range_.end_key_)
        {
          TBSYS_LOG(WARN, "bug, table_id:%lu miss some range, last_range:%s now_range:%s", table_id, to_cstring(last_tablet_info->range_), to_cstring(tablet_info->range_));
        }
        else
        {
          crc_helper = get_crc_helper(it);
          if(NULL != crc_helper)
          {
            if(OB_SUCCESS != (ret = crc_helper->get_row_checksum(tablet_version, tmp_row_checksum)))
            {
              TBSYS_LOG(WARN, "range:%s tablet_version:%ld get row_checksum fail ret:%d", to_cstring(tablet_info->range_), tablet_version, ret);
              break;
            }
            else
            {
              row_checksum += tmp_row_checksum;
            }
          }
          last_tablet_info = tablet_info;
        }
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "not found tablet_version:%ld table_id:%lu", tablet_version, table_id);
  }

  return ret;
}

bool ObRootTable2::table_is_exist(const uint64_t table_id) const
{
  common::ObNewRange range;
  range.table_id_ = table_id;
  const common::ObTabletInfo *tablet_info = NULL;
  // if table is exist, we should find at lest one range with this table id
  bool ret = false;
  ObRootMeta2TableIdLessThan meta_table_id_comp(tablet_info_manager_); // compare with table id
  const_iterator start_pos = std::lower_bound(begin(), end(), range, meta_table_id_comp);
  if (start_pos != end())
  {
    tablet_info = get_tablet_info(start_pos);
    if (tablet_info == NULL)
    {
      TBSYS_LOG(ERROR, "get_tablet_info NULL");
    }
    else
    {
      if (tablet_info->range_.table_id_ == table_id)
      {
        ret = true;
      }
    }
  }

  return ret;
}

void ObRootTable2::server_off_line(const int32_t server_index, const int64_t time_stamp)
{
  int64_t count = 0;
  for (int32_t i = 0; i < (int32_t)meta_table_.get_array_index(); ++i)
  {
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
    for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
    //add e
    {
      if (data_holder_[i].server_info_indexes_[j] == server_index)
      {
        atomic_exchange((uint32_t*) &(data_holder_[i].server_info_indexes_[j]), (uint32_t)OB_INVALID_INDEX);
        if (time_stamp > 0 ) atomic_exchange((uint64_t*) &(data_holder_[i].last_dead_server_time_), time_stamp);
        count++;
      }
    }
  }
  TBSYS_LOG(INFO, "delete replicas, server_index=%d count=%ld", server_index, count);
  return;
}

void ObRootTable2::get_cs_version(const int64_t index, int64_t &version)
{
  if (tablet_info_manager_ != NULL)
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
      for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
      //add e
      {
        if (index == data_holder_[i].server_info_indexes_[j]) //the server is down
        {
          version = data_holder_[i].tablet_version_[j];
          break;
        }
      }
    }
  }
}

void ObRootTable2::dump() const
{
  TBSYS_LOG(INFO, "meta_table_.size():%d", (int32_t)meta_table_.get_array_index());
  if (tablet_info_manager_ != NULL)
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      TBSYS_LOG(DEBUG, "dump the %d-th tablet info, index=%d", i, data_holder_[i].tablet_info_index_);
      tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
      TBSYS_LOG(DEBUG, "dump the %d-th tablet info end", i);
      data_holder_[i].dump();
      TBSYS_LOG(DEBUG, "dump the %d-th server info end", i);
    }
  }
  TBSYS_LOG(INFO, "dump meta_table_ complete");
  return;
}

//add liumz, [secondary index static_index_build] 20150601:b
void ObRootTable2::dump(const int32_t index) const
{
  if (tablet_info_manager_ != NULL)
  {
    if (index < meta_table_.get_array_index() && index >=0)
    {
      tablet_info_manager_->hex_dump(data_holder_[index].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
      data_holder_[index].dump();
    }
  }
  return;
}
//add:e

void ObRootTable2::dump_cs_tablet_info(const int server_index, int64_t &tablet_num)const
{
  if (NULL != tablet_info_manager_)
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      data_holder_[i].dump(server_index, tablet_num);
    }
  }
}

void ObRootTable2::dump_unusual_tablets(int64_t current_version, int32_t replicas_num, int32_t &num) const
{
  num = 0;
  TBSYS_LOG(INFO, "meta_table_.size():%d", (int32_t)meta_table_.get_array_index());
  TBSYS_LOG(INFO, "current_version=%ld replicas_num=%d", current_version, replicas_num);
  if (tablet_info_manager_ != NULL)
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      int32_t new_num = 0;
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
      for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
	  //add e
      {
        if (OB_INVALID_INDEX == data_holder_[i].server_info_indexes_[j]) //the server is down
        {
        }
        else if (data_holder_[i].tablet_version_[j] == current_version)
        {
          ++new_num;
        }
      }
      if (new_num < replicas_num)
      {
        // is an unusual tablet
        num++;
        tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
        data_holder_[i].dump();
      }
    }
  }
  TBSYS_LOG(INFO, "dump meta_table_ complete, num=%d", num);
  return;
}

int ObRootTable2::check_tablet_version_merged(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const
{
  int err = OB_SUCCESS;
  is_merged = true;
  int32_t fail_count = 0;
  int32_t merging_count = 0;
  int32_t succ_count = 0;
  if (NULL == tablet_info_manager_)
  {
    TBSYS_LOG(WARN, "tablet_info_manager is null");
    is_merged = false;
    err = OB_ERROR;
  }
  else
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      succ_count = 0;
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
      for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
	  //add e
      {
        //all online cs should be merged
        if (data_holder_[i].server_info_indexes_[j] != OB_INVALID_INDEX)
        {
          if (data_holder_[i].tablet_version_[j] < tablet_version)
          {
            TBSYS_LOG(TRACE, "root_table[%p] server[%d]'s tablet version[%ld] less than required[%ld]",
                this, data_holder_[i].server_info_indexes_[j], data_holder_[i].tablet_version_[j],
                tablet_version);
            // tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
            is_merged = false;
            ++merging_count;
          }
          else
          {
            ++succ_count;
          }
        }
      }
      if (succ_count < safe_count)
      {
        is_merged = false;
        ++fail_count;
        TBSYS_LOG(TRACE, "check tablet not all safe copy merged to this version:succ[%d], safe[%ld]",
            succ_count, safe_count);
      }
    }
  }
  if (!is_merged)
  {
    TBSYS_LOG(INFO, "there are %d tablet not satisfy the version[%ld] and safe_copy_count[%ld], the merging tablet count=%d",
        fail_count, tablet_version, safe_count, merging_count);

  }
  return err;
}

//add liumz, [secondary index static_index_build] 20150422:b
/*
 * if index table's stat is not AVALIBALE or NOT_AVALIBALE, ignore it, invalid index does not affect merge done
 */
int ObRootTable2::check_tablet_version_merged_v2(const int64_t tablet_version, const int64_t safe_count, bool &is_merged, const common::ObSchemaManagerV2 &schema_mgr) const
{
  int err = OB_SUCCESS;
  is_merged = true;
  int32_t fail_count = 0;
  int32_t merging_count = 0;
  int32_t succ_count = 0;
  if (NULL == tablet_info_manager_)
  {
    TBSYS_LOG(WARN, "tablet_info_manager is null");
    is_merged = false;
    err = OB_ERROR;
  }
  else
  {
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      succ_count = 0;
      //add liumz, check if index table's stat is AVALIBALE or NOT_AVALIBALE, ignore error
      ObTabletInfoManager::const_iterator tablet_info = tablet_info_manager_->get_tablet_info(data_holder_[i].tablet_info_index_);
      if (NULL != tablet_info)
      {
        const ObTableSchema *table_schema = NULL;
        table_schema = schema_mgr.get_table_schema(tablet_info->range_.table_id_);
        if (NULL == table_schema)
        {
          TBSYS_LOG(TRACE, "get table schema failed, ignore it, table_id = [%lu]", tablet_info->range_.table_id_);
          continue;
        }
        else if (OB_INVALID_ID != table_schema->get_index_helper().tbl_tid
                 && AVALIBALE != table_schema->get_index_helper().status
                 /*&& NOT_AVALIBALE != table_schema->get_index_helper().status*/)
        {
          TBSYS_LOG(TRACE, "unavailable index, ignore it, table_id = [%lu]", tablet_info->range_.table_id_);
          continue;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "tablet_info is NULL, tablet info index = [%d]", data_holder_[i].tablet_info_index_);
        continue;
      }
      //add:e
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
      for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
      //add e
      {
        //all online cs should be merged
        if (data_holder_[i].server_info_indexes_[j] != OB_INVALID_INDEX)
        {          
          if (data_holder_[i].tablet_version_[j] < tablet_version)
          {
            TBSYS_LOG(TRACE, "root_table[%p] server[%d]'s tablet version[%ld] less than required[%ld]",
                this, data_holder_[i].server_info_indexes_[j], data_holder_[i].tablet_version_[j],
                tablet_version);
            // tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
            is_merged = false;
            ++merging_count;
          }
          else
          {
            ++succ_count;
          }
        }
      }
      if (succ_count < safe_count)
      {
        is_merged = false;
        ++fail_count;
        TBSYS_LOG(TRACE, "check tablet not all safe copy merged to this version:succ[%d], safe[%ld]",
            succ_count, safe_count);
      }
    }
  }
  if (!is_merged)
  {
    TBSYS_LOG(INFO, "there are %d tablet not satisfy the version[%ld] and safe_copy_count[%ld], the merging tablet count=%d",
        fail_count, tablet_version, safe_count, merging_count);

  }
  return err;
}
//add:e

//add liumz, [secondary index static_index_build] 20150529:b
int ObRootTable2::check_tablet_version_merged_v3(const uint64_t table_id, const int64_t tablet_version, const int64_t safe_count, bool &is_merged)
{
  int ret = OB_SUCCESS;
  is_merged = true;
  int32_t fail_count = 0;
  int32_t merging_count = 0;
  int32_t succ_count = 0;
  common::ObRowkey min_rowkey;
  min_rowkey.set_min_row();
  common::ObRowkey max_rowkey;
  max_rowkey.set_max_row();
  common::ObNewRange start_range;
  start_range.table_id_ = table_id;
  start_range.end_key_ = min_rowkey;
  common::ObNewRange end_range;
  end_range.table_id_ = table_id;
  end_range.end_key_ = max_rowkey;

  const_iterator begin_pos = find_pos_by_range(start_range);
  const_iterator end_pos = find_pos_by_range(end_range);

  if (NULL == begin_pos || NULL == end_pos)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "find_pos_by_range failed, begin_pos or end_pos is NULL. ret=%d", ret);
  }
  else if (end() == begin_pos || end() == end_pos || begin_pos > end_pos)
  {
    ret = OB_ARRAY_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "find_pos_by_range failed, out of range. begin():[%p], end():[%p], begin:[%p], end:[%p], ret=%d",
              begin(), end(), begin_pos, end_pos, ret);
  }
  for (const_iterator it = begin_pos; it <= end_pos && OB_LIKELY(OB_SUCCESS == ret); it++)
  {
    succ_count = 0;
    for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
    {
      //all online cs should be merged
      if (it->server_info_indexes_[j] != OB_INVALID_INDEX)
      {
        if (it->tablet_version_[j] < tablet_version)
        {
          TBSYS_LOG(TRACE, "root_table[%p] server[%d]'s tablet version[%ld] less than required[%ld]",
              this, it->server_info_indexes_[j], it->tablet_version_[j],
              tablet_version);
          // tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
          is_merged = false;
          ++merging_count;
        }
        else
        {
          ++succ_count;
        }
      }
    }//end inner for
    if (succ_count < safe_count)
    {
      is_merged = false;
      ++fail_count;
      TBSYS_LOG(TRACE, "check tablet not all safe copy merged to this version:succ[%d], safe[%ld], tid[%lu]",
          succ_count, safe_count, table_id);
    }
  }//end outer for
  if (!is_merged)
  {
    TBSYS_LOG(INFO, "table[%lu] has %d tablet not safity the version[%ld] and safe_copy_count[%ld], the merging tablet count=%d",
        table_id, fail_count, tablet_version, safe_count, merging_count);
  }
  return ret;
}
//add:e

void ObRootTable2::get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & data_size) const
{
  if (tablet_info_manager_ != NULL)
  {
    const common::ObTabletInfo * tablet_info = NULL;
    for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
    {
      ++tablet_count;
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
      for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
      //add e
      {
        if (OB_INVALID_INDEX != data_holder_[i].server_info_indexes_[j])
        {
          tablet_info = tablet_info_manager_->get_tablet_info(data_holder_[i].tablet_info_index_);
          if (tablet_info != NULL)
          {
            data_size += tablet_info->occupy_size_;
            row_count += tablet_info->row_count_;
            break;
          }
        }
      }
    }
  }
}

//add liumz, [secondary index static_index_build] 20150601:b
const ObTabletInfo* ObRootTable2::get_tablet_info(const int32_t meta_index) const
{
  int32_t tablet_index = 0;
  const common::ObTabletInfo* tablet_info = NULL;
  if (meta_index < meta_table_.get_array_index() && meta_index >=0 && NULL != tablet_info_manager_)
  {
    tablet_index = data_holder_[meta_index].tablet_info_index_;
    tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
    if (NULL == tablet_info)
    {
      TBSYS_LOG(ERROR, "no tablet info found in tablet_info_manager_ with tablet_index=%d", tablet_index);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "no tablet info found, meta_index=%d rt_size=%ld tablet_info_manager_=%p",
        meta_index, meta_table_.get_array_index(), tablet_info_manager_);
  }
  return tablet_info;
}

ObTabletInfo* ObRootTable2::get_tablet_info(const int32_t meta_index)
{
  int32_t tablet_index = 0;
  common::ObTabletInfo* tablet_info = NULL;
  if (meta_index < meta_table_.get_array_index() && meta_index >=0 && NULL != tablet_info_manager_)
  {
    tablet_index = data_holder_[meta_index].tablet_info_index_;
    tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
    if (NULL == tablet_info)
    {
      TBSYS_LOG(ERROR, "no tablet info found in tablet_info_manager_ with tablet_index=%d", tablet_index);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "no tablet info found, meta_index=%d rt_size=%ld tablet_info_manager_=%p",
        meta_index, meta_table_.get_array_index(), tablet_info_manager_);
  }
  return tablet_info;
}
//add:e

const ObTabletInfo* ObRootTable2::get_tablet_info(const const_iterator& it) const
{
  int32_t tablet_index = 0;
  const common::ObTabletInfo* tablet_info = NULL;
  if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
  {
    tablet_index = it->tablet_info_index_;
    tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
    if (NULL == tablet_info)
    {
      TBSYS_LOG(ERROR, "no tablet info found in tablet_info_manager_ with tablet_index=%d", tablet_index);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "no tablet info found it=%p end()=%p begin()=%p tablet_info_manager_=%p",
        it, end(), begin(), tablet_info_manager_);
  }
  return tablet_info;
}

ObTabletInfo* ObRootTable2::get_tablet_info(const const_iterator& it)
{
  int32_t tablet_index = 0;
  common::ObTabletInfo* tablet_info = NULL;
  if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
  {
    tablet_index = it->tablet_info_index_;
    tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
    if (NULL == tablet_info)
    {
      TBSYS_LOG(ERROR, "no tablet info found in tablet_info_manager_ with tablet_index=%d", tablet_index);
    }
  }
  return tablet_info;
}


bool ObRootTable2::move_back(const int32_t from_index_inclusive, const int32_t move_step)
{
  bool ret = false;
  int32_t data_count = static_cast<int32_t>(meta_table_.get_array_index());
  if ((move_step + data_count < ObTabletInfoManager::MAX_TABLET_COUNT) && from_index_inclusive <= data_count)
  {
    ret = true;
    memmove(&(data_holder_[from_index_inclusive + move_step]), //dest
        &(data_holder_[from_index_inclusive]), //src
        (data_count - from_index_inclusive) * sizeof(ObRootMeta2) ); //size
    meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, data_count + move_step);
    sorted_count_ += move_step;
  }
  if (!ret)
  {
    TBSYS_LOG(ERROR, "too much data, not enought sapce");
  }
  return ret;

}
int64_t ObRootTable2::get_max_tablet_version()
{
  int64_t max_tablet_version = 0;
  int64_t tmp_version = 0;
  if (NULL != tablet_info_manager_)
  {
    for (const_iterator it = begin(); it != end(); it++)
    {
      tmp_version = get_max_tablet_version(it);
      if (tmp_version > max_tablet_version)
      {
        max_tablet_version = tmp_version;
      }

    }
  }
  return max_tablet_version;
}

int64_t ObRootTable2::get_max_tablet_version(const const_iterator& it)
{
  int64_t max_tablet_version = 0;
  //add zhaoqiong[roottable tablet management]20150127:b
  //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
  for (int32_t i = 0 ; i < OB_MAX_COPY_COUNT; i++)
  //add e
  {
    if (it->tablet_version_[i] > max_tablet_version)
    {
      max_tablet_version = it->tablet_version_[i];
    }
  }
  return max_tablet_version;
}

//add liumz, [secondary index static_index_build] 20150319:b
//modify liumz, 20150324
int ObRootTable2::get_root_meta(const ObTabletInfo &tablet_info, const_iterator &root_meta)
{
  int ret = OB_SUCCESS;
  const common::ObTabletInfo* p_tablet_info = NULL;
  common::ObNewRange find_range;
  find_range.table_id_ = tablet_info.range_.table_id_;
  find_range.end_key_ = tablet_info.range_.end_key_;
  //mod liumz, bugfix: check tablet_info_manager_, 20150511:b
  //ObRootMeta2RangeLessThan meta_comp(tablet_info_manager_); // compare_with_endkey
  //const_iterator find_pos = std::lower_bound(begin(), end(), find_range, meta_comp);
  const_iterator find_pos = find_pos_by_range(find_range);
  //mod:e
  if (find_pos == NULL || find_pos == end())
  {
    TBSYS_LOG(WARN, "not find. find_range=%s, find_pos=%p, begin()=%p, end()=%p",
        common::to_cstring(find_range), find_pos, begin(), end());
    ret = OB_ERROR_OUT_OF_RANGE;
  }
  else if (NULL == (p_tablet_info = get_tablet_info(find_pos)))
  {
    ret = OB_ERROR_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "get tablet_info fail. err =%d", ret);
  }
  else if (!p_tablet_info->equal(tablet_info))
  {
    ret = OB_ERROR_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "tablet info is not equal. err =%d", ret);
  }
  else
  {
    root_meta = find_pos;
  }
//  root_meta = NULL;
//  if (NULL != tablet_info_manager_)
//  {
//    int32_t tablet_info_index = tablet_info_manager_->get_index(tablet_info);
//    if (OB_INVALID_INDEX != tablet_info_index)
//    {
//      for (const_iterator it = begin(); it != end(); it++)
//      {
//        if (tablet_info_index == it->tablet_info_index_)
//        {
//          root_meta = it;
//          break;
//        }
//      }
//    }
//  }
//  else if (NULL == root_meta)
//  {
//    ret = OB_ERROR;
//  }
  return ret;
}

int ObRootTable2::get_root_meta(const int32_t meta_index, const_iterator &root_meta)
{
  int ret = OB_SUCCESS;
  if (meta_index < meta_table_.get_array_index() && meta_index >=0)
  {
    root_meta = &data_holder_[meta_index];
  }
  else
  {
    ret = OB_ARRAY_OUT_OF_RANGE;
  }
  return ret;
}

//add:e

//add liumz, [secondary index static_index_build] 20150601:b
int ObRootTable2::get_root_meta_index(const ObTabletInfo &tablet_info, int32_t &meta_index)
{
  int ret = OB_SUCCESS;
  const common::ObTabletInfo* p_tablet_info = NULL;
  common::ObNewRange find_range;
  find_range.table_id_ = tablet_info.range_.table_id_;
  find_range.end_key_ = tablet_info.range_.end_key_;
  //mod liumz, bugfix: check tablet_info_manager_, 20150511:b
  //ObRootMeta2RangeLessThan meta_comp(tablet_info_manager_); // compare_with_endkey
  //const_iterator find_pos = std::lower_bound(begin(), end(), find_range, meta_comp);
  const_iterator find_pos = find_pos_by_range(find_range);
  //mod:e
  if (find_pos == NULL || find_pos == end())
  {
    TBSYS_LOG(WARN, "not find. find_range=%s, find_pos=%p, begin()=%p, end()=%p",
        common::to_cstring(find_range), find_pos, begin(), end());
    ret = OB_ERROR_OUT_OF_RANGE;
  }
  else if (NULL == (p_tablet_info = get_tablet_info(find_pos)))
  {
    ret = OB_ERROR_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "get tablet_info fail. err =%d", ret);
  }
  else if (!p_tablet_info->equal(tablet_info))
  {
    ret = OB_ERROR_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "tablet info is not equal. err =%d", ret);
  }
  else
  {
    meta_index = static_cast<int32_t>(find_pos - begin());
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150324:b
ObRootTable2::const_iterator ObRootTable2::find_pos_by_range(const common::ObNewRange &range)
{
  const_iterator find_pos = NULL;
  if (internal_check())
  {
    ObRootMeta2RangeLessThan meta_comp(tablet_info_manager_); // compare_with_endkey
    find_pos = std::lower_bound(begin(), end(), range, meta_comp);
  }
  return find_pos;
}
//add:e

int ObRootTable2::remove(const const_iterator& it, const int32_t safe_count, const int32_t server_index)
{
  int ret = OB_SUCCESS;
  if (safe_count <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check safe count failed:safe_count[%d]", safe_count);
  }
  else
  {
    int32_t current_count = 0;
    int32_t found_it_index = OB_INVALID_INDEX;
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
    //add e
    {
      if (it->server_info_indexes_[i] != OB_INVALID_INDEX)
      {
        ++current_count;
      }
      if (it->server_info_indexes_[i] == server_index)
      {
        found_it_index = i;
      }
    }
    // find and remove the replica
    if ((OB_INVALID_INDEX != found_it_index) && (current_count - 1 > safe_count))
    {
      it->server_info_indexes_[found_it_index] = OB_INVALID_INDEX;
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "not find the server or safe count failed:server[%d], safe[%d], current[%d]",
          server_index, safe_count, current_count);
    }
  }
  return ret;
}

int ObRootTable2::modify(const const_iterator& it, const int32_t dest_server_index, const int64_t tablet_version)
{
  int32_t found_it_index = OB_INVALID_INDEX;
  found_it_index = find_suitable_pos(it, dest_server_index, tablet_version);
  if (found_it_index != OB_INVALID_INDEX)
  {
    it->server_info_indexes_[found_it_index] = dest_server_index;
    it->tablet_version_[found_it_index] = tablet_version;
  }
  return OB_SUCCESS;
}

int ObRootTable2::replace(const const_iterator& it,
    const int32_t src_server_index, const int32_t dest_server_index,
    const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX != src_server_index)
  {
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
    //add e
    {
      if (it->server_info_indexes_[i] == src_server_index)
      {
        it->server_info_indexes_[i] = OB_INVALID_INDEX;
        break;
      }
    }
  }
  if (OB_INVALID_INDEX != dest_server_index)
  {
    ret = modify(it, dest_server_index, tablet_version);
  }
  return ret;
}

//only used in root server's init process
int ObRootTable2::add(const common::ObTabletInfo& tablet, const int32_t server_index,
    const int64_t tablet_version, const int64_t seq)
{
  int ret = OB_ERROR;
  UNUSED(seq);
  if (tablet_info_manager_ != NULL)
  {
    ret = OB_SUCCESS;
    int32_t tablet_index = OB_INVALID_INDEX;
    ret = tablet_info_manager_->add_tablet_info(tablet, tablet_index);
    ObTabletCrcHistoryHelper *crc_helper = NULL;
    if (OB_SUCCESS == ret)
    {
      crc_helper = tablet_info_manager_->get_crc_helper(tablet_index);
      if (crc_helper != NULL)
      {
        crc_helper->reset();
        crc_helper->check_and_update(tablet_version, tablet.crc_sum_, tablet.row_checksum_);
      }
      ObRootMeta2 meta;
      meta.tablet_info_index_ = tablet_index;
      meta.server_info_indexes_[0] = server_index;
      meta.tablet_version_[0] = tablet_version;
      meta.last_dead_server_time_ = 0;
      if (!meta_table_.push_back(meta))
      {
        TBSYS_LOG(ERROR, "add new tablet error have no enough space");
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}
int ObRootTable2::create_table(const ObTabletInfo &tablet, const ObArray<int32_t> &chunkservers, const int64_t tablet_version)
{
  int ret = OB_ERROR;
  int32_t insert_pos = -1;
  if (tablet_info_manager_ != NULL)
  {
    ret = OB_SUCCESS;
    const_iterator first;
    const_iterator last;
    ret = find_range(tablet.range_, first, last);
    if (OB_SUCCESS == ret && first == last)
    {
      insert_pos = static_cast<int32_t>(first-begin());
    }
    if (OB_FIND_OUT_OF_RANGE == ret && -1 == insert_pos)
    {
      ret =OB_SUCCESS;
      insert_pos = static_cast<int32_t>(end() - begin());
    }
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "insert pos = %d", insert_pos);
      if(!move_back(insert_pos, 1))
      {
        TBSYS_LOG(ERROR, "too many tablet!!!");
        ret = OB_ERROR;
      }
    }
    int32_t tablet_index = OB_INVALID_INDEX;
    if (OB_SUCCESS == ret)
    {
      ret = tablet_info_manager_->add_tablet_info(tablet, tablet_index);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "too many tablet info!!!");
      }
      else
      {
        TBSYS_LOG(INFO, "add tablet info succ. tablet_index=%d", tablet_index);
        ObTabletCrcHistoryHelper *crc_helper = NULL;
        crc_helper = tablet_info_manager_->get_crc_helper(tablet_index);
        if (crc_helper != NULL)
        {
          crc_helper->reset();
          crc_helper->check_and_update(tablet_version, tablet.crc_sum_, tablet.row_checksum_);
        }
      }
    }
    ObRootMeta2 meta;
    if (OB_SUCCESS == ret)
    {
      meta.tablet_info_index_ = tablet_index;
      meta.last_dead_server_time_ = 0;
      //add zhaoqiong[roottable tablet management]20150127:b
      //for (int32_t i = 0; i < chunkservers.count() && i < OB_SAFE_COPY_COUNT; i++)
      for (int32_t i = 0; i < chunkservers.count() && i < OB_MAX_COPY_COUNT; i++)
      //add e
      {
        meta.server_info_indexes_[i] = chunkservers.at(i);
        meta.tablet_version_[i] = tablet_version;
      }
    }
    if (OB_SUCCESS == ret)
    {
      data_holder_[insert_pos] = meta;
    }
  }
  return ret;
}
bool ObRootTable2::add_lost_range()
{
  bool ret = true;
  ObNewRange last_range;
  last_range.table_id_ = OB_INVALID_ID;
  last_range.set_whole_range();
  const ObTabletInfo* tablet_info = NULL;
  ObTabletInfo will_add;
  int32_t i = 0;
  while (i < (int32_t)meta_table_.get_array_index())
  {
    tablet_info = get_tablet_info(begin() + i);
    if (tablet_info == NULL)
    {
      TBSYS_LOG(ERROR, "invalid tablet info index is %d", i);
      ret = false;
      break;
    }
    will_add.range_ = tablet_info->range_;
    if (tablet_info->range_.table_id_ != OB_INVALID_ID)
    {
      if (tablet_info->range_.table_id_ != last_range.table_id_ )
      {
        if (!last_range.end_key_.is_max_row())
        {
          TBSYS_LOG(ERROR, "table %lu not end correctly", last_range.table_id_);
          will_add.range_ = last_range;
          will_add.range_.start_key_ = last_range.end_key_;
          will_add.range_.end_key_.set_max_row();
          will_add.range_.border_flag_.unset_inclusive_start();
          will_add.range_.border_flag_.set_inclusive_end();
          if (OB_SUCCESS != add_range(will_add, begin() + i, FIRST_TABLET_VERSION, OB_INVALID_INDEX))
          {
            ret = false;
            break;
          }
        }
        if (!tablet_info->range_.start_key_.is_min_row())
        {
          TBSYS_LOG(ERROR, "table %lu not start correctly", tablet_info->range_.table_id_);
        }
      }
      else
      {
        if (tablet_info->range_.start_key_.compare(last_range.end_key_) != 0)
        {
          TBSYS_LOG(ERROR, "table %lu have a hole last range is ", last_range.table_id_);
          //last_range.hex_dump(TBSYS_LOG_LEVEL_INFO);
          static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
          last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          TBSYS_LOG(INFO, "now is table %lu  range is ", tablet_info->range_.table_id_);
          //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
          tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          will_add.range_ = last_range;
          will_add.range_.start_key_ = last_range.end_key_;
          will_add.range_.end_key_ = tablet_info->range_.start_key_;
          will_add.range_.border_flag_.unset_inclusive_start();
          will_add.range_.border_flag_.set_inclusive_end();
          if (OB_SUCCESS != add_range(will_add, begin() + i, FIRST_TABLET_VERSION, OB_INVALID_INDEX))
          {
            ret = false;
            break;
          }
        }
      }
    }
    last_range = will_add.range_;
    ++i;
  }
  if (ret && meta_table_.get_array_index() > 0)
  {
    if (!last_range.end_key_.is_max_row())
    {
      TBSYS_LOG(ERROR, "table %lu not end correctly", last_range.table_id_);
      will_add.range_ = last_range;
      will_add.range_.border_flag_.unset_inclusive_start();
      will_add.range_.border_flag_.set_inclusive_end();
      will_add.range_.start_key_ = last_range.end_key_;
      will_add.range_.end_key_.set_max_row();
      if (OB_SUCCESS != add_range(will_add, end() , FIRST_TABLET_VERSION, OB_INVALID_INDEX))
      {
        ret = false;
      }
    }
  }
  return ret;
}
bool ObRootTable2::check_lost_data(const int64_t server_index)const
{
  bool ret = true;
  int64_t count = 0;
  //add zhaoqiong[roottable tablet management]20150127:b
  //int64_t single_copy_count = 0;
  //int64_t double_copy_count = 0;
  //int64_t three_copy_count = 0;
  int64_t copy_count_array[OB_SAFE_COPY_COUNT];
  //add e
  int64_t i = 0;
  const_iterator it = begin();
  const ObTabletInfo* tablet_info = NULL;
  while (i < meta_table_.get_array_index())
  {
    int copy_count = 0;
    bool have_hole = true;
    it = begin() + i;
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
    for (int j = 0; j < OB_MAX_COPY_COUNT; j++)
    //add e
    {
      if (server_index != it->server_info_indexes_[j]
          && OB_INVALID_INDEX != it->server_info_indexes_[j])
      {
        copy_count++;
        have_hole = false;
      }
    }
    if (have_hole)
    {
      ret = false;
      count ++;
      tablet_info = get_tablet_info(it);
      if (NULL != tablet_info)
      {
        TBSYS_LOG(ERROR, "lost data, range=%s", to_cstring(tablet_info->range_));
      }
      else
      {
        TBSYS_LOG(WARN, "can't be here. i = %ld, it=%p", i, it);
      }
    }
    else
    {
      //add zhaoqiong[roottable tablet management]20150127:b
      /*if (1 == copy_count)
      {
        single_copy_count ++;
      }
      else if (2 == copy_count)
      {
        double_copy_count ++;
      }
      else
      {
        three_copy_count ++;
      }
      */
      if (copy_count < OB_SAFE_COPY_COUNT)
      {
          copy_count_array[copy_count-1] ++;
      }
      else
      {
          copy_count_array[OB_SAFE_COPY_COUNT -1] ++;
      }
      //add e

    }
    i++;
  }
  if (count > 0)
  {
    TBSYS_LOG(ERROR, "check lost data over, there are %ld range are lost.", count);
  }
  else
  {
    TBSYS_LOG(INFO, "check lost data over, all data are exist expect cs_index=%ld.", server_index);
  }
  //add zhaoqiong[roottable tablet management]20150127:b
  //TBSYS_LOG(INFO, "travel around roottable. there are %ld single_copy tablet, %ld double_copy tablet, %ld three_copy tablet",
     // single_copy_count, double_copy_count, three_copy_count);
  TBSYS_LOG(INFO, "travel around roottable. there are %ld single_copy tablet",
      copy_count_array[0]);
  //add e
  return ret;
}
bool ObRootTable2::check_lost_range(const common::ObSchemaManagerV2 *schema_mgr)
{
  bool ret = true;
  ObNewRange last_range;
  last_range.table_id_ = OB_INVALID_ID;
  last_range.set_whole_range();
  const ObTabletInfo* tablet_info = NULL;
  const ObTableSchema *table_schema = NULL;//add liumz, [bugfix: ignore error index when check lost range]20151229
  bool is_invalid_index = false;
  int32_t i = 0;
  while (i < (int32_t)meta_table_.get_array_index())
  {
    tablet_info = get_tablet_info(begin() + i);
    if (tablet_info == NULL)
    {
      TBSYS_LOG(ERROR, "invalid tablet info index is %d", i);
      ret = false;
      break;
    }
    if (tablet_info->range_.table_id_ != OB_INVALID_ID)
    {
      //add liumz, [bugfix: ignore invalid index when check lost range]20151229:b
      if (NULL != schema_mgr)
      {
        table_schema = schema_mgr->get_table_schema(tablet_info->range_.table_id_);
        if (NULL == table_schema)
        {
          TBSYS_LOG(WARN, "get table schema failed, ignore it, table_id = [%lu]", tablet_info->range_.table_id_);
        }
        else if (OB_INVALID_ID != table_schema->get_index_helper().tbl_tid
                 && AVALIBALE != table_schema->get_index_helper().status)
        {
          TBSYS_LOG(WARN, "invalid index, ignore it, table_id = [%lu]", tablet_info->range_.table_id_);
          is_invalid_index = true;
          ++i;
          continue;
        }
        else
        {
          is_invalid_index = false;
        }
      }
      //add:e
      if (tablet_info->range_.table_id_ != last_range.table_id_ )
      {
        if (!last_range.end_key_.is_max_row())
        {
          TBSYS_LOG(WARN, "table %lu not end correctly", last_range.table_id_);
          ret = false;
          //break;
        }
        if (!tablet_info->range_.start_key_.is_min_row())
        {
          TBSYS_LOG(WARN, "table %lu not start correctly", tablet_info->range_.table_id_);
          ret = false;
          //break;
        }
      }
      else
      {
        if (tablet_info->range_.start_key_.compare(last_range.end_key_) != 0)
        {
          TBSYS_LOG(WARN, "table not complete, table_id=%lu last_range=%s",
              last_range.table_id_, to_cstring(last_range));
          TBSYS_LOG(WARN, "cur_table_id=%lu  curr_range=%s",
              tablet_info->range_.table_id_, to_cstring(tablet_info->range_));
          ret = false;
          //break;
        }
      }
    }
    if (!is_invalid_index)
    {
      last_range = tablet_info->range_;
    }
    ++i;
  }
  if (ret && meta_table_.get_array_index() > 0)
  {
    if (!last_range.end_key_.is_max_row())
    {
      TBSYS_LOG(WARN, "table %lu not end correctly", last_range.table_id_);
      ret = false;
    }
  }
  return ret;
}
void ObRootTable2::sort()
{
  if (tablet_info_manager_ != NULL)
  {
    ObRootMeta2CompareHelper sort_helper(tablet_info_manager_);
    std::sort(begin(), end(), sort_helper);
    sorted_count_ = static_cast<int32_t>(end() - begin());
  }
  return;
}

int ObRootTable2::shrink_to(ObRootTable2* shrunk_table, ObTabletReportInfoList &delete_list)
{
  int ret = OB_ERROR;
  static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
  if (shrunk_table != NULL &&
      shrunk_table->tablet_info_manager_ != NULL &&
      end() == sorted_end())
  {
    ret = OB_SUCCESS;
    ObNewRange last_range;
    last_range.table_id_ = OB_INVALID_ID;
    ObTabletInfo* tablet_info = NULL;
    ObTabletInfo new_tablet_info;
    int32_t last_range_index = OB_INVALID_INDEX;
    int32_t last_tablet_index = OB_INVALID_INDEX;
    ObRootMeta2 root_meta;
    const_iterator it = begin();
    const_iterator it2 = begin();
    int cmp_ret = 0;
    ObTabletReportInfo to_delete;
    while (OB_SUCCESS == ret && it < end() )
    {
      tablet_info = get_tablet_info(it);
      if (NULL == tablet_info)
      {
        TBSYS_LOG(ERROR, "should never reach this bug occurs");
        ret = OB_ERROR;
        break;
      }
      last_range = tablet_info->range_;
      ret = shrunk_table->tablet_info_manager_->add_tablet_info(*tablet_info, last_range_index);
      ObTabletCrcHistoryHelper *last_tablet_crc_helper = NULL;
      if (OB_SUCCESS == ret)
      {
        last_tablet_crc_helper = shrunk_table->tablet_info_manager_->get_crc_helper(last_range_index);
        if (last_tablet_crc_helper != NULL)
        {
          last_tablet_crc_helper->reset();
          last_tablet_crc_helper->check_and_update(it->tablet_version_[0], tablet_info->crc_sum_, tablet_info->row_checksum_);
        }
      }
      if (OB_SUCCESS == ret)
      {
        root_meta = *it;
        root_meta.tablet_info_index_ = last_range_index;
        last_tablet_index = static_cast<int32_t>(shrunk_table->meta_table_.get_array_index());
        if (!shrunk_table->meta_table_.push_back(root_meta))
        {
          TBSYS_LOG(ERROR, "too much tablet");
          ret = OB_ERROR;
          break;
        }
      }
      //merge the same range
      while(tablet_info->range_.equal(last_range))
      {
        TBSYS_LOG(DEBUG, "shrink idx=%ld range=%s", it-begin(), to_cstring(tablet_info->range_));

        if (OB_SUCCESS != last_tablet_crc_helper->check_and_update(it->tablet_version_[0], tablet_info->crc_sum_, tablet_info->row_checksum_))
        {
          TBSYS_LOG(ERROR, "crc check error,  crc=%lu last_crc=%lu last_cs=%d",
              tablet_info->crc_sum_, it->tablet_version_[0], it->server_info_indexes_[0]);
          TBSYS_LOG(ERROR, "error tablet is %s", to_cstring(last_range));
          TBSYS_LOG(ERROR, "tabletinfo in shrink table is");
          shrunk_table->data_holder_[last_tablet_index].dump();
          //last_range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
        }
        else
        {
          to_delete.tablet_location_.chunkserver_.set_port(0);
          merge_one_tablet(shrunk_table, last_tablet_index, it, to_delete);
          if (0 != to_delete.tablet_location_.chunkserver_.get_port())
          {
            if (OB_SUCCESS != delete_list.add_tablet(to_delete))
            {
              TBSYS_LOG(WARN, "failed to add delete tablet");
            }
          }
        }
        it++;
        if (it >= end())
        {
          break;
        }
        tablet_info = get_tablet_info(it);
        if (NULL == tablet_info)
        {
          TBSYS_LOG(ERROR, "should never reach this bug occurs");
          ret = OB_ERROR;
          break;
        }
      } // end while same range
      //split next OB_SAFE_COPY_COUNT * 2 is enough
      if (OB_SUCCESS == ret)
      {
        for (it2 = it; it2 < it + OB_SAFE_COPY_COUNT * 2 && it2 < end() && OB_SUCCESS == ret; it2++)
        {
          tablet_info = get_tablet_info(it2);
          if (NULL == tablet_info)
          {
            TBSYS_LOG(ERROR, "should never reach this bug occurs");
            ret = OB_ERROR;
            break;
          }
          //compare start key;
          cmp_ret = last_range.compare_with_startkey(tablet_info->range_);
          if (cmp_ret > 0)
          {
            last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(ERROR, "lost some tablet, last_range=%s", row_key_dump_buff);
            tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);
            ret = OB_ERROR;
            break;
          }
          if (cmp_ret < 0)
          {
            //no need split
            continue;
          }
          // same start key, compare end key
          cmp_ret = last_range.compare_with_endkey(tablet_info->range_);
          if (cmp_ret > 0)
          {
            TBSYS_LOG(ERROR, "you have not sorted it? bugs!!");
            ret = OB_ERROR;
          }
          if (cmp_ret == 0)
          {
            TBSYS_LOG(ERROR, "we should skip this , bugs!!");
            last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(ERROR, "last_range=%s", row_key_dump_buff);
            tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);

            //no need split
            continue;
          }
          //split it2;
          TBSYS_LOG(ERROR, "in this version we sould not reach this init data have some error");
          last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(ERROR, "last_range=%s", row_key_dump_buff);
          tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);
          ret = OB_ERROR;
          break;
          merge_one_tablet(shrunk_table, last_tablet_index, it2, to_delete);
          tablet_info->range_.start_key_ = last_range.end_key_;
        } //end for next OB_SAFE_COPY_COUNT * 2
      }
    } //end while
  }
  return ret;
}

int32_t ObRootTable2::find_suitable_pos(const const_iterator& it, const int32_t server_index, const int64_t tablet_version, common::ObTabletReportInfo *to_delete/*=NULL*/)
{
  int32_t found_it_index = OB_INVALID_INDEX;
  //add zhaoqiong[roottable tablet management]20150127:b
  //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
  for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
  //add e
  {
    if (it->server_info_indexes_[i] == server_index)
    {
      found_it_index = i;
      break;
    }
  }
  if (OB_INVALID_INDEX == found_it_index)
  {
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
    //add e
    {
      if (it->server_info_indexes_[i] == OB_INVALID_INDEX)
      {
        found_it_index = i;
        break;
      }
    }
  }
  if (OB_INVALID_INDEX == found_it_index)
  {
    int64_t min_tablet_version = tablet_version;
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
    //add e
    {
      if (it->tablet_version_[i] < min_tablet_version)
      {
        min_tablet_version = it->tablet_version_[i];
        found_it_index = i;
      }
    }
    if (OB_INVALID_INDEX != found_it_index && NULL != to_delete)
    {
      to_delete->tablet_location_.tablet_version_ = it->tablet_version_[found_it_index];
      to_delete->tablet_location_.chunkserver_.set_port(it->server_info_indexes_[found_it_index]);
    }
  }
  return found_it_index;
}

int ObRootTable2::check_tablet_version(const int64_t tablet_version, int safe_copy_count) const
{
  int ret = OB_SUCCESS;
  int tablet_ok = 0;
  const common::ObTabletInfo* tablet_info = NULL;
  for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
  {
    tablet_ok = 0;
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
    for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
    //add e
    {
      if (data_holder_[i].tablet_version_[j] >= tablet_version)
      {
        ++tablet_ok;
      }
    }
    if (tablet_ok < safe_copy_count)
    {
      tablet_info = get_tablet_info(begin() + i);
      if (tablet_info != NULL)
      {
        TBSYS_LOG(WARN, "tablet version error now version is %ld, tablet is", tablet_version);
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(INFO, "%s", row_key_dump_buff);
        //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);

        //add zhaoqiong[roottable tablet management]20150127:b
        //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
        for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
        //add e
        {
          TBSYS_LOG(INFO, "root table's tablet version is %ld", data_holder_[i].tablet_version_[j]);
        }
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}

ObTabletCrcHistoryHelper* ObRootTable2::get_crc_helper(const const_iterator& it)
{
  ObTabletCrcHistoryHelper *ret = NULL;
  if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
  {
    int32_t tablet_index = it->tablet_info_index_;
    ret = tablet_info_manager_->get_crc_helper(tablet_index);
  }
  return ret;
}

bool ObRootTable2::check_tablet_copy_count(const int32_t copy_count) const
{
  bool ret = true;
  int tablet_ok = 0;
  const common::ObTabletInfo* tablet_info = NULL;
  for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
  {
    tablet_ok = 0;
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
    for (int32_t j = 0; j < OB_MAX_COPY_COUNT; j++)
    //add e
    {
      if (data_holder_[i].server_info_indexes_[j] != OB_INVALID_INDEX)
      {
        ++tablet_ok;
      }
    }
    if (tablet_ok < copy_count)
    {
      tablet_info = get_tablet_info(begin() + i);
      if (tablet_info != NULL)
      {
        TBSYS_LOG(WARN, "tablet copy count less than %d", copy_count);
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(INFO, "%s", row_key_dump_buff);
        //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
      }
      ret = false;
    }
  }
  return ret;
}


void ObRootTable2::merge_one_tablet(ObRootTable2* shrunk_table, int32_t last_tablet_index, const_iterator it, ObTabletReportInfo &to_delete)
{
  to_delete.tablet_location_.chunkserver_.set_port(0);
  int32_t i = 0;
  //add zhaoqiong[roottable tablet management]20150127:b
  //for (i = 0; i < OB_SAFE_COPY_COUNT; ++i)
  for (i = 0; i < OB_MAX_COPY_COUNT; ++i)
  //add e
  {
    if (shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] == OB_INVALID_INDEX ||
        shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] == it->server_info_indexes_[0])
    {
      shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] = it->server_info_indexes_[0];
      shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] = it->tablet_version_[0];
      break;
    }
  }
  //add zhaoqiong[roottable tablet management]20150127:b
  //if (OB_SAFE_COPY_COUNT == i)
  if (OB_MAX_COPY_COUNT == i)
  //add e
  {
    const ObTabletInfo *tablet_info = shrunk_table->get_tablet_info(&shrunk_table->data_holder_[last_tablet_index]);
    //add zhaoqiong[roottable tablet management]20150127:b
    //for (i = 0; i < OB_SAFE_COPY_COUNT; ++i)
    for (i = 0; i < OB_MAX_COPY_COUNT; ++i)
    //add e
    {
      if (shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] < it->tablet_version_[0])
      {
        to_delete.tablet_location_.tablet_version_ = shrunk_table->data_holder_[last_tablet_index].tablet_version_[i];
        // reinterprete port_ to present server_index
        to_delete.tablet_location_.chunkserver_.set_port(shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i]);

        shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] = it->server_info_indexes_[0];
        shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] = it->tablet_version_[0];
        break;
      }
    }
    //add zhaoqiong[roottable tablet management]20150127:b
    //if (OB_SAFE_COPY_COUNT == i)
    if (OB_MAX_COPY_COUNT == i)
    //add e
    {
      to_delete.tablet_location_.tablet_version_ = it->tablet_version_[0];
      to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[0]);
    }
    TBSYS_LOG(DEBUG, "delete replica, cs_idx=%d", to_delete.tablet_location_.chunkserver_.get_port());
    to_delete.tablet_info_ = *tablet_info;
  }
  return;
}

/*
 * 得到range会对root table产生的影响
 * 1个range可能导致root table分裂 合并 无影响等
 */
int ObRootTable2::get_range_pos_type(const common::ObNewRange& range, const const_iterator& first, const const_iterator& last) const
{
  int ret = POS_TYPE_UNINIT;
  const common::ObTabletInfo* tablet_info = NULL;
  char row_key_dump_buff[OB_RANGE_STR_BUFSIZ];

  range.to_string(row_key_dump_buff, OB_RANGE_STR_BUFSIZ);
  tablet_info = this->get_tablet_info(first);

  if (tablet_info == NULL)
  {
    ret = POS_TYPE_ADD_RANGE;
    TBSYS_LOG(DEBUG, "maybe a new range, range=%s", row_key_dump_buff);
  }
  else if (first == end())
  {
    ret = POS_TYPE_ADD_RANGE;
    TBSYS_LOG(DEBUG, "add in the last, range=%s", row_key_dump_buff);
  }
  else if (last == end())
  {
    if (range.start_key_ == tablet_info->range_.start_key_)
    {
      ret = POS_TYPE_MERGE_RANGE;
    }
    else
    {
      TBSYS_LOG(WARN, "can't decide what type this range is[%s]", row_key_dump_buff);
    }
  }
  else
  {
    if (range.compare_with_startkey(tablet_info->range_) < 0 )
    {
      if (range.table_id_ < tablet_info->range_.table_id_)
      {
        ret = POS_TYPE_ADD_RANGE;
        TBSYS_LOG(DEBUG, "add new range %s", row_key_dump_buff);
      }

      if (POS_TYPE_UNINIT == ret)
      {
        if (!range.end_key_.is_max_row() && range.end_key_ <= tablet_info->range_.start_key_)
        {
          ret = POS_TYPE_ADD_RANGE;
        }
      }

      if (POS_TYPE_ADD_RANGE == ret)
      {
        //range is less than first, we should check if range is large than prev
        if (first != begin())
        {
          const const_iterator prev = first - 1;
          const common::ObTabletInfo* tablet_info = NULL;
          tablet_info = this->get_tablet_info(prev);
          if (tablet_info == NULL)
          {
            ret = POS_TYPE_ERROR;
            TBSYS_LOG(ERROR, "no tablet_info found in table_info_manager.");
          }
          else
          {
            if (range.table_id_ == tablet_info->range_.table_id_)
            {
              if (tablet_info->range_.end_key_.is_max_row() ||
                  range.start_key_.is_min_row() ||
                  range.start_key_ < tablet_info->range_.end_key_)
              {
                ret = POS_TYPE_ERROR;
                TBSYS_LOG(ERROR, "wrong range position");
              }
            }
          }
        }
      }
    }
    else if (first == last)
    {
      //range in one tablet
      if (range.equal(tablet_info->range_))
      {
        ret = POS_TYPE_SAME_RANGE;
      }
      else
      {
        ret = POS_TYPE_SPLIT_RANGE;
      }
    }
    else
    {
      if (range.compare_with_startkey(tablet_info->range_) == 0)
      {
        tablet_info = this->get_tablet_info(last);
        if (tablet_info == NULL)
        {
          ret = POS_TYPE_ERROR;
          TBSYS_LOG(ERROR, "failed to get tablet info");
        }
        else if (range.compare_with_endkey(tablet_info->range_) == 0)
        {
          ret = POS_TYPE_MERGE_RANGE;
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to get tablet info");
        }
      }
    }
    if (ret == POS_TYPE_UNINIT)
    {
      TBSYS_LOG(ERROR, "No proper type is found");
      ret = POS_TYPE_ERROR;
    }
    if (ret == POS_TYPE_ERROR)
    {
      TBSYS_LOG(ERROR, "error range found");
      TBSYS_LOG(INFO, "tablet range is %s", to_cstring(tablet_info->range_));
      //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
      TBSYS_LOG(INFO, "reported range is %s", to_cstring(range));
      //range.hex_dump(TBSYS_LOG_LEVEL_INFO);
    }
  }

  return ret;
}
int ObRootTable2::split_range(const common::ObTabletInfo& reported_tablet_info, const const_iterator& pos,
    const int64_t tablet_version, const int32_t server_index)
{
  int ret = OB_ERROR;
  ObNewRange range = reported_tablet_info.range_;
  ObTabletInfo will_add_table;
  will_add_table = reported_tablet_info;
  ObTabletCrcHistoryHelper* crc_helper = NULL;
  int pos_type = get_range_pos_type(range, pos, pos);
  if (pos_type != POS_TYPE_SPLIT_RANGE)
  {
    TBSYS_LOG(ERROR, "pos_type[%d] is not POS_TYPE_SPLIT_RANGE[%d]", pos_type, POS_TYPE_SPLIT_RANGE);
    ret = OB_ERROR;
  }
  else
  {
    ret = OB_SUCCESS;
    common::ObTabletInfo* tablet_info = NULL;
    tablet_info = get_tablet_info(pos);
    if (tablet_info == NULL || tablet_info_manager_ == NULL)
    {
      TBSYS_LOG(ERROR, "tablet_info[%p] or tablet_info_manager_[%p] should not be NULL",
          tablet_info, tablet_info_manager_);
      ret = OB_ERROR;
    }
    else
    {
      int split_type = SPLIT_TYPE_ERROR;
      int cmp =range.compare_with_startkey(tablet_info->range_);
      if (cmp == 0)
      {
        //new range is top half of root table range
        //will split to two ranges
        split_type = SPLIT_TYPE_TOP_HALF;
      }
      else if (cmp > 0)
      {
        cmp = range.compare_with_endkey(tablet_info->range_);
        if (cmp == 0)
        {
          //this range is bottom half of root table range
          //will split to two ranges;
          split_type = SPLIT_TYPE_BOTTOM_HALF;
        }
        else if (cmp < 0 )
        {
          //this range is middle half of root table range
          //will split to three ranges;
          split_type = SPLIT_TYPE_MIDDLE_HALF;
        }
      }
      int32_t from_pos_inclusive = static_cast<int32_t>(pos - begin());
      if (split_type == SPLIT_TYPE_TOP_HALF || split_type == SPLIT_TYPE_BOTTOM_HALF)
      {
        if (from_pos_inclusive <  meta_table_.get_array_index() )
        {
          if (!move_back(from_pos_inclusive, 1))
          {
            TBSYS_LOG(ERROR, "move back error");
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "why you reach this bugss!!");
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ObNewRange splited_range_top = tablet_info->range_;
          splited_range_top.border_flag_.unset_max_value();
          ObNewRange splited_range_bottom = tablet_info->range_;
          if (split_type == SPLIT_TYPE_TOP_HALF)
          {
            splited_range_top.end_key_ = range.end_key_;
          }
          else
          {
            splited_range_top.end_key_ = range.start_key_;
          }
          // set inclusive end anyway
          splited_range_top.border_flag_.set_inclusive_end();
          splited_range_bottom.border_flag_.set_inclusive_end();
          int32_t out_index_top = OB_INVALID_INDEX;
          int32_t out_index_bottom = OB_INVALID_INDEX;
          //not clone start key clone end key
          will_add_table.range_ = splited_range_top;

          ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_top);
          crc_helper = tablet_info_manager_->get_crc_helper(out_index_top);
          if (crc_helper != NULL)
          {
            crc_helper->reset();
            if (split_type == SPLIT_TYPE_TOP_HALF)
            {
              crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_, reported_tablet_info.row_checksum_);
            }
          }

          const ObTabletInfo* tablet_info_added = NULL;
          if (OB_SUCCESS == ret)
          {
            tablet_info_added = tablet_info_manager_->get_tablet_info(out_index_top);
            splited_range_bottom.start_key_ = tablet_info_added->range_.end_key_;//so we do not need alloc mem for key
          }
          if (OB_SUCCESS == ret)
          {
            will_add_table.range_ = splited_range_bottom;
            ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_bottom);
            crc_helper = tablet_info_manager_->get_crc_helper(out_index_bottom);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
              if (split_type != SPLIT_TYPE_TOP_HALF)
              {
                crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_, reported_tablet_info.row_checksum_);
              }
            }
          }
          if (OB_SUCCESS == ret)
          {
            data_holder_[from_pos_inclusive].tablet_info_index_ = out_index_top;
            data_holder_[from_pos_inclusive + 1].tablet_info_index_ = out_index_bottom;
            int32_t new_range_index = 0;
            if (split_type == SPLIT_TYPE_TOP_HALF)
            {
              new_range_index = from_pos_inclusive;
            }
            else
            {
              new_range_index = from_pos_inclusive + 1;
            }
            int32_t found_index = find_suitable_pos(begin() + new_range_index, server_index, tablet_version);
            if (OB_INVALID_INDEX != found_index)
            {
              data_holder_[new_range_index].server_info_indexes_[found_index] = server_index;
              data_holder_[new_range_index].tablet_version_[found_index] = tablet_version;
            }
            else
            {
              TBSYS_LOG(ERROR, "found_index should not be OB_INVALID_INDEX");
            }
          }
        }
      }
      else if (split_type == SPLIT_TYPE_MIDDLE_HALF)
      {
        //will split to three range
        if (from_pos_inclusive <  meta_table_.get_array_index() )
        {
          if (!move_back(from_pos_inclusive, 2))
          {
            TBSYS_LOG(ERROR, "too many tablet");
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "why we got this bugss!");
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          data_holder_[from_pos_inclusive+1] = data_holder_[from_pos_inclusive];
          data_holder_[from_pos_inclusive+2] = data_holder_[from_pos_inclusive];

          const ObTabletInfo* tablet_info_added = NULL;
          ObNewRange splited_range_middle = range;
          ObNewRange splited_range_bottom = tablet_info->range_;
          ObNewRange splited_range_top = tablet_info->range_;
          splited_range_top.border_flag_.unset_max_value();
          int32_t out_index_top = OB_INVALID_INDEX;
          int32_t out_index_middle = OB_INVALID_INDEX;
          int32_t out_index_bottom = OB_INVALID_INDEX;
          // set inclusive end anyway
          splited_range_top.border_flag_.set_inclusive_end();
          splited_range_bottom.border_flag_.set_inclusive_end();
          splited_range_middle.border_flag_.set_inclusive_end();

          if (OB_SUCCESS == ret)
          {
            //clone end key, not clone start key
            will_add_table.range_ = splited_range_middle;
            will_add_table.crc_sum_ = reported_tablet_info.crc_sum_; //no use any more
            ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_middle);
            crc_helper = tablet_info_manager_->get_crc_helper(out_index_middle);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
              crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_, reported_tablet_info.row_checksum_);
            }
          }
          if (OB_SUCCESS == ret)
          {
            tablet_info_added = tablet_info_manager_->get_tablet_info(out_index_middle);
            splited_range_top.end_key_ = tablet_info_added->range_.start_key_;
            will_add_table.range_ = splited_range_top;
            will_add_table.crc_sum_ = 0; //no use any more
            ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_top);
            crc_helper = tablet_info_manager_->get_crc_helper(out_index_top);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
            }
          }
          if (OB_SUCCESS == ret)
          {
            splited_range_bottom.start_key_ = tablet_info_added->range_.end_key_;
            will_add_table.range_ = splited_range_bottom;
            will_add_table.crc_sum_ = 0; //no use any more
            ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_bottom);
            crc_helper = tablet_info_manager_->get_crc_helper(out_index_bottom);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
            }
          }
          if (OB_SUCCESS == ret)
          {
            data_holder_[from_pos_inclusive].tablet_info_index_ = out_index_top;
            data_holder_[from_pos_inclusive + 1].tablet_info_index_ = out_index_middle;
            data_holder_[from_pos_inclusive + 2].tablet_info_index_ = out_index_bottom;
            int32_t new_range_index = from_pos_inclusive + 1;
            int32_t found_index = find_suitable_pos(begin() + from_pos_inclusive + 1, server_index, tablet_version);
            if (OB_INVALID_INDEX != found_index)
            {
              data_holder_[new_range_index].server_info_indexes_[found_index] = server_index;
              data_holder_[new_range_index].tablet_version_[found_index] = tablet_version;
            }
            else
            {
              TBSYS_LOG(ERROR, "found_index should not be OB_INVALID_INDEX");
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "you should not reach this bugs!!");
        ret = OB_ERROR;
      }

    }
  }
  return ret;
}
int ObRootTable2::add_range(const common::ObTabletInfo& reported_tablet_info, const const_iterator& pos,
    const int64_t tablet_version, const int32_t server_index)
{
  int ret = OB_ERROR;
  ObNewRange range = reported_tablet_info.range_;
  ObTabletInfo will_add_table;
  will_add_table = reported_tablet_info;
  if (pos == end() || get_range_pos_type(range, pos, pos) == POS_TYPE_ADD_RANGE)
  {
    ret = OB_SUCCESS;
    int32_t from_pos_inclusive = static_cast<int32_t>(pos - begin());
    if (!move_back(from_pos_inclusive, 1))
    {
      ret = OB_ERROR;
    }
    if (OB_SUCCESS == ret)
    {
      int32_t out_index = OB_INVALID_INDEX;
      ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index);
      if (OB_SUCCESS == ret)
      {
        ObTabletCrcHistoryHelper *crc_helper = NULL;
        crc_helper = tablet_info_manager_->get_crc_helper(out_index);
        if (crc_helper != NULL)
        {
          crc_helper->reset();
          crc_helper->check_and_update(tablet_version, will_add_table.crc_sum_, will_add_table.row_checksum_);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ObRootMeta2 meta;
        meta.tablet_info_index_ = out_index;
        meta.server_info_indexes_[0] = server_index;
        meta.tablet_version_[0] = tablet_version;
        meta.last_dead_server_time_ = 0;
        data_holder_[from_pos_inclusive] = meta;
      }
    }
  }
  return ret;
}

int ObRootTable2::write_to_file(const char* filename)
{
  int ret = OB_SUCCESS;
  if (filename == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "file name can not be NULL");
  }

  if (tablet_info_manager_ == NULL)
  {
    TBSYS_LOG(ERROR, "tablet_info_manager should not be NULL bugs!!");
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
  {
    int64_t total_size = 0;
    int64_t total_count = meta_table_.get_array_index();
    // cal total size
    total_size += tablet_info_manager_->get_serialize_size();
    total_size += serialization::encoded_length_vi64(total_count);
    total_size += serialization::encoded_length_vi32(sorted_count_);
    for(int64_t i=0; i<total_count; ++i)
    {
      total_size += data_holder_[i].get_serialize_size();
    }

    // allocate memory
    char* data_buffer = static_cast<char*>(ob_malloc(total_size, ObModIds::OB_RS_ROOT_TABLE));
    if (data_buffer == NULL)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "allocate memory failed.");
    }

    // serialization
    if (ret == OB_SUCCESS)
    {
      common::ObDataBuffer buffer(data_buffer, total_size);
      ret = tablet_info_manager_->serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), total_count);
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), sorted_count_);
      }
      if (ret == OB_SUCCESS)
      {
        for(int64_t i=0; i<total_count; ++i)
        {
          ret = data_holder_[i].serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialize tablet failed:index[%ld], total[%ld], buff_size[%ld], buff_pos[%ld]",
                i, total_count, total_size, buffer.get_position());
            break;
          }
        }
      }
    }

    // header
    int64_t header_length = sizeof(common::ObRecordHeader);
    int64_t pos = 0;
    char header_buffer[header_length];
    if (ret == OB_SUCCESS)
    {
      common::ObRecordHeader header;

      header.set_magic_num(ROOT_TABLE_MAGIC);
      header.header_length_ = static_cast<int16_t>(header_length);
      header.version_ = 0;
      header.reserved_ = 0;

      header.data_length_ = static_cast<int32_t>(total_size);
      header.data_zlength_ = static_cast<int32_t>(total_size);

      header.data_checksum_ = common::ob_crc64(data_buffer, total_size);
      header.set_header_checksum();

      ret = header.serialize(header_buffer, header_length, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialization file header failed");
      }
    }

    // write into file
    if (ret == OB_SUCCESS)
    {
      // open file
      common::FileUtils fu;
      int32_t rc = fu.open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
      if (rc < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "create file [%s] failed", filename);
      }

      if (ret == OB_SUCCESS)
      {
        int64_t wl = fu.write(header_buffer, header_length);
        if (wl != header_length)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "write header into [%s] failed", filename);
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t wl = fu.write(data_buffer, total_size);
        if (wl != total_size)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "write data into [%s] failed", filename);
        }
        else
        {
          TBSYS_LOG(DEBUG, "root table has been write into [%s]", filename);
        }
      }
      fu.close();
    }
    if (data_buffer != NULL)
    {
      ob_free(data_buffer);
      data_buffer = NULL;
    }
  }

  return ret;
}
int ObRootTable2::read_from_file(const char* filename)
{
  int ret = OB_SUCCESS;
  if (filename == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "filename can not be NULL");
  }
  if (tablet_info_manager_ == NULL)
  {
    TBSYS_LOG(ERROR, "tablet_info_manager should not be NULL bugs!!");
    ret = OB_ERROR;
  }

  common::FileUtils fu;
  if (ret == OB_SUCCESS)
  {
    int32_t rc = fu.open(filename, O_RDONLY);
    if (rc < 0)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "open file [%s] failed", filename);
    }
  }

  TBSYS_LOG(DEBUG, "read root table from [%s]", filename);
  int64_t header_length = sizeof(common::ObRecordHeader);
  common::ObRecordHeader header;
  if (ret == OB_SUCCESS)
  {
    char header_buffer[header_length];
    int64_t rl = fu.read(header_buffer, header_length);
    if (rl != header_length)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "read header from [%s] failed", filename);
    }

    if (ret == OB_SUCCESS)
    {
      int64_t pos = 0;
      ret = header.deserialize(header_buffer, header_length, pos);
    }

    if (ret == OB_SUCCESS)
    {
      ret = header.check_header_checksum();
    }
  }
  TBSYS_LOG(DEBUG, "header read success ret: %d", ret);

  char* data_buffer = NULL;
  int64_t size = 0;
  if (ret == OB_SUCCESS)
  {
    size = header.data_length_;
    data_buffer = static_cast<char*>(ob_malloc(size, ObModIds::OB_RS_ROOT_TABLE));
    if (data_buffer == NULL)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "allocate memory failed");
    }
  }

  if (ret == OB_SUCCESS)
  {
    int64_t rl = fu.read(data_buffer, size);
    if (rl != size)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "read data from file [%s] failed", filename);
    }
  }

  if (ret == OB_SUCCESS)
  {
    int cr = common::ObRecordHeader::check_record(header, data_buffer, size, ROOT_TABLE_MAGIC);
    if (cr != OB_SUCCESS)
    {
      ret = OB_DESERIALIZE_ERROR;
      TBSYS_LOG(ERROR, "data check failed, rc: %d", cr);
    }
  }

  int64_t count = 0;
  common::ObDataBuffer buffer(data_buffer, size);
  if (ret == OB_SUCCESS)
  {
    ret = tablet_info_manager_->deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
  }
  if (ret == OB_SUCCESS)
  {
    ret = serialization::decode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &count);
  }
  if (ret == OB_SUCCESS)
  {
    ret = serialization::decode_vi32(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &sorted_count_);
  }

  if (ret == OB_SUCCESS)
  {
    ObRootMeta2 record;
    for (int64_t i=0; i<count; ++i)
    {
      data_holder_[i].deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "record deserialize failed");
        break;
      }
    }
    meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, count);

  }

  fu.close();
  if (data_buffer != NULL)
  {
    ob_free(data_buffer);
    data_buffer = NULL;
  }

  return ret;
}
void ObRootTable2::dump_as_hex(FILE* stream) const
{
  fprintf(stream, "root_table_size %ld sorted_count_ %d\n", meta_table_.get_array_index(), sorted_count_);
  ObRootTable2::const_iterator it = this->begin();
  for (; it != this->end(); it++)
  {
    it->dump_as_hex(stream);
  }
}
void ObRootTable2::read_from_hex(FILE* stream)
{
  int64_t size = 0;
  fscanf(stream, "root_table_size %ld sorted_count_ %d", &size, &sorted_count_);
  fscanf(stream, "\n");
  for (int64_t i = 0 ; i < size; i++)
  {
    data_holder_[i].read_from_hex(stream);
  }
  meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, size);
}

int ObRootTable2::shrink()
{
  int ret = OB_SUCCESS;
  int32_t new_count = 0;
  iterator it_invalid = NULL;
  for (iterator it = begin(); sorted_end() != it; ++it)
  {
    if (OB_INVALID_INDEX == it->tablet_info_index_)
    {
      if (NULL == it_invalid)
      {
        it_invalid = it;
      }
    }
    else
    {
      new_count++;
      if (NULL != it_invalid)
      {
        *it_invalid = *it;
        it_invalid++;
      }
    }
  } // end for
  sorted_count_ = new_count;
  meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, sorted_count_);
  TBSYS_LOG(INFO, "tablet num after shrink, num=%d", sorted_count_);
  return ret;
}

int ObRootTable2::get_deleted_table(const common::ObSchemaManagerV2 & schema, const ObRootBalancer& balancer, common::ObArray<uint64_t>& deleted_tables) const
{
  int ret = OB_SUCCESS;
  ObRootTable2::const_iterator it;
  const ObTabletInfo* tablet = NULL;
  uint64_t last_deleted_table_id = OB_INVALID_ID;
  uint64_t cur_table_id = OB_INVALID_ID;
  for (it = this->begin(); it != this->end(); ++it)
  {
    tablet = this->get_tablet_info(it);
    OB_ASSERT(NULL != tablet);
    cur_table_id = tablet->range_.table_id_;
    if (cur_table_id != last_deleted_table_id &&
        NULL == schema.get_table_schema(cur_table_id) &&
        !balancer.is_table_loading(cur_table_id))
    {
      if (cur_table_id <= OB_APP_MIN_TABLE_ID)
      {
        TBSYS_LOG(ERROR, "delete table id is less than OB_APP_MIN_TABLE_ID:table_id[%lu]", cur_table_id);
        break;
      }
      else if (cur_table_id == OB_INVALID_ID)
      {
        TBSYS_LOG(ERROR, "table_id must not be OB_INVALID_ID=%lu", OB_INVALID_ID);
        break;
      }
      else
      {
        ret = deleted_tables.push_back(cur_table_id);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "push deleted table failed:table_id[%lu], ret[%d]", cur_table_id, ret);
          break;
        }
        else
        {
          last_deleted_table_id = cur_table_id;
          TBSYS_LOG(INFO, "find a table not in schema and not a loading table:table_id[%lu]", cur_table_id);
        }
      }
    }
  }
  return ret;
}

int ObRootTable2::delete_tables(const common::ObArray<uint64_t> &deleted_tables)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_ROW_KEY_LENGTH * 2];
  // mark deleted
  ObRootTable2::iterator it;
  ObTabletInfo* tablet = NULL;
  //for (it = this->begin(); it != this->sorted_end(); ++it)
  for (it = this->begin(); it != this->end(); ++it)
  {
    tablet = this->get_tablet_info(it);
    OB_ASSERT(NULL != tablet);
    for (int j = 0; j < deleted_tables.count(); ++j)
    {
      // for each deleted table
      if (tablet->range_.table_id_ == deleted_tables.at(j))
      {
        it->tablet_info_index_ = OB_INVALID_INDEX;
        tablet->range_.to_string(buf, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(INFO, "delete tablets from roottable, range=%s", buf);
        break;
      }
    }
  } // end for
  if (OB_SUCCESS == ret
      && OB_SUCCESS != (ret = this->shrink()))
  {
    TBSYS_LOG(WARN, "failed to shrink table, err=%d", ret);
  }
  return ret;
}
