/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_cache_join.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_tablet_cache_join.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "common/ob_ups_row_util.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletCacheJoin::ObTabletCacheJoin()
  :ups_row_cache_(NULL),
  cache_handle_(NULL),
  handle_count_(0)
{
}

ObTabletCacheJoin::~ObTabletCacheJoin()
{
}

void ObTabletCacheJoin::set_table_id(uint64_t table_id)
{
  table_id_ = table_id;
}

void ObTabletCacheJoin::set_cache(ObTabletJoinCache &ups_row_cache)
{
  ups_row_cache_ = &ups_row_cache;
}

int ObTabletCacheJoin::open()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id_ || NULL == ups_row_cache_)
  {
    TBSYS_LOG(WARN, "not inited: table_id_[%lu], ups_row_cache_[%p]",
      table_id_, ups_row_cache_);
  }
  else if (OB_SUCCESS != (ret = ObTabletJoin::open()))
  {
    TBSYS_LOG(WARN, "ObTabletJoin open fail:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret)
  {
    handle_count_ = 0;
  }

  if(OB_SUCCESS == ret)
  {
    void *tmp = ob_malloc(sizeof(CacheHandle) * batch_count_, ObModIds::OB_SQL_TABLET_CACHE_JOIN);
    if(NULL == tmp)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate mem for cache handle fail");
    }
    else
    {
      cache_handle_ = new(tmp)CacheHandle[batch_count_];
    }
  }
  return ret;
}

int ObTabletCacheJoin::close()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObTabletJoin::close()))
  {
    TBSYS_LOG(WARN, "ObTabletJoin close fail:ret[%d]", ret);
  }

  if(OB_SUCCESS != (ret = revert_cache_handle()))
  {
    TBSYS_LOG(WARN, "revert cache handle fail:ret[%d]", ret);
  }

  if(NULL != cache_handle_)
  {
    for(int64_t i=0;i<batch_count_;i++)
    {
      cache_handle_[i].~CacheHandle();
    }
    ob_free(cache_handle_);
    cache_handle_ = NULL;
    fused_row_ = NULL;
  }
  return ret;
}

void ObTabletCacheJoin::reset()
{
  ObTabletJoin::reset();
  revert_cache_handle();
}

int ObTabletCacheJoin::fetch_fused_row_prepare()
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = revert_cache_handle()))
  {
    TBSYS_LOG(WARN, "revert cache handle fail:ret[%d]", ret);
  }
  return ret;
}

int ObTabletCacheJoin::get_ups_row(const ObRowkey &rowkey, ObUpsRow &ups_row, const ObGetParam &get_param)
{
  int ret = OB_SUCCESS;
  ObString value;

  if(OB_SUCCESS == ret)
  {
    ObTabletJoinCacheKey cache_key;
    cache_key.table_id_ = table_id_;
    cache_key.rowkey_ = rowkey;
    ret = ups_row_cache_->get(cache_key, value);
    if(OB_ENTRY_NOT_EXIST == ret)
    {
      if(OB_SUCCESS != (ret = fetch_next_ups_row(get_param, fused_row_idx_)))
      {
        TBSYS_LOG(WARN, "fetch next ups row fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = ups_row_cache_->get(cache_key, value)))
      {
        TBSYS_LOG(WARN, "get rowkey %s from cache not exist.ret[%d]", to_cstring(rowkey), ret);
      }
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get rowkey %s from cache not exist.", to_cstring(rowkey));
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = ObUpsRowUtil::convert(table_join_info_.right_table_id_, value, ups_row)))
    {
      TBSYS_LOG(WARN, "convert to ups row fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletCacheJoin::fetch_ups_row(const ObGetParam &get_param)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const ObUpsRow *ups_row = NULL;
  ObString compact_row; //用于存储ObRow的紧缩格式，内存已经分配好
  const ObRowkey *rowkey = NULL;
  ThreadSpecificBuffer::Buffer *buffer = NULL;

  TBSYS_LOG(DEBUG, "fetch join ups_row param: %ld, %s", get_param.get_cell_size(),
      get_param.get_cell_size() < 10 ? to_cstring(get_param) : "..");

  if(OB_SUCCESS == ret)
  {
    buffer = thread_buffer_.get_buffer();
    if(NULL == buffer)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific buffer fail:ret[%d]", ret);
    }
    else
    {
      buffer->reset();
    }
  }

  if(OB_SUCCESS == ret && get_param.get_cell_size() > 0)
  {
    ups_multi_get_.reuse();
    ups_multi_get_.set_get_param(get_param);
    ups_multi_get_.set_row_desc(ups_row_desc_);

    if(OB_SUCCESS != (ret = ups_multi_get_.open()))
    {
      TBSYS_LOG(WARN, "open ups multi get fail:ret[%d]", ret);
    }

    int64_t row_in_cache_num = 0;
    while(OB_SUCCESS == ret)
    {
      const ObRow *tmp_row_ptr = NULL;
      ret = ups_multi_get_.get_next_row(rowkey, tmp_row_ptr);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ups multi get next row fail:ret[%d]", ret);
      }
      else
      {
        if (row_count++ < 10)
        {
          TBSYS_LOG(DEBUG, "fetch join ups row(%ld): key:%s, row:%s",
              row_count, to_cstring(*rowkey), to_cstring(*tmp_row_ptr));
        }
        ups_row = dynamic_cast<const ObUpsRow *>(tmp_row_ptr);
        if(NULL == ups_row)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "shoud be ObUpsRow");
        }
      }

      if(OB_SUCCESS == ret)
      {
        compact_row.assign_buffer(buffer->current(), buffer->remain());
        if(OB_SUCCESS != (ret = ObUpsRowUtil::convert(*ups_row, compact_row)))
        {
          TBSYS_LOG(WARN, "convert ups row to compact row fail:ret[%d]", ret);
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(handle_count_ >= batch_count_)
        {
          ret = OB_SIZE_OVERFLOW;
          TBSYS_LOG(WARN, "handle_count_ is overflow[%ld]", handle_count_);
        }
      }

      if(OB_SUCCESS == ret)
      {
        ObString value;
        ObTabletJoinCacheKey cache_key;
        cache_key.table_id_ = table_id_;
        cache_key.rowkey_ = *rowkey;
        ret = ups_row_cache_->put_and_fetch(cache_key, compact_row, value, cache_handle_[handle_count_]);
        if(OB_BUF_NOT_ENOUGH == ret)
        {
          if(row_in_cache_num <= 0)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "row put in join cache [%ld]", row_in_cache_num);
          }
          else
          {
            TBSYS_LOG(DEBUG, "join cache size overflow [%ld] row add to cache", row_in_cache_num);
            ret = OB_SUCCESS;
          }
          break;
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "join cache put fail:ret[%d], rowkey[%s]", ret, to_cstring(*rowkey));
        }
        else
        {
          row_in_cache_num ++;
          handle_count_ ++;
        }
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = ups_multi_get_.close()))
      {
        TBSYS_LOG(WARN, "close ups multi get fail:ret[%d]", ret);
      }
    }
  }

  return ret;
}

int ObTabletCacheJoin::compose_next_get_param(ObGetParam &next_get_param, int64_t fused_row_idx)
{
  int ret = OB_SUCCESS;
  const ObRowStore::StoredRow *stored_row = NULL;
  ObRow row;
  const ObRowDesc *row_desc = NULL;

  if (OB_SUCCESS != (ret = fused_scan_->get_row_desc(row_desc) ))
  {
    TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
  }
  else
  {
    row.set_row_desc(*row_desc);
  }

  if(OB_SUCCESS == ret)
  {
    next_get_param.reset(true);
    next_get_param.set_version_range(version_range_);

    for(int64_t i=fused_row_idx;OB_SUCCESS == ret && i<fused_row_array_.count();i++)
    {
      if(OB_SUCCESS != (ret = fused_row_array_.at(i, stored_row)))
      {
        TBSYS_LOG(WARN, "get stored row from fused row array fail:ret[%d], i[%ld]", ret, i);
      }
      else if(OB_SUCCESS != (ret = ObRowUtil::convert(stored_row->get_compact_row(), row)))
      {
        TBSYS_LOG(WARN, "convert row fail:ret[%d], i[%ld], fused_row_array_count[%ld]", ret, i, fused_row_array_.count());
      }
      else if(OB_SUCCESS != (ret = gen_get_param(next_get_param, row)))
      {
        TBSYS_LOG(WARN, "gen get param fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObTabletCacheJoin::revert_cache_handle()
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    for(int64_t i=0;OB_SUCCESS == ret && i<handle_count_;i++)
    {
      if(OB_SUCCESS != (ret = ups_row_cache_->revert(cache_handle_[i])))
      {
        TBSYS_LOG(WARN, "revert cache handle fail:ret[%d], [%ld]", ret, i);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    handle_count_ = 0;
  }
  return ret;
}

int ObTabletCacheJoin::fetch_next_ups_row(const ObGetParam &get_param, int64_t fused_row_idx)
{
  int ret = OB_SUCCESS;
  const ObGetParam *next_get_param = NULL;

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = revert_cache_handle()))
    {
      TBSYS_LOG(WARN, "revert cache handle fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(0 == fused_row_idx)
    {
      next_get_param = &get_param;
    }
    else
    {
      ObGetParam *tmp_get_param = NULL;
      if(NULL == (tmp_get_param = GET_TSI_MULT(ObGetParam, common::TSI_SQL_GET_PARAM_2)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "get tsi get_param fail:ret[%d]", ret);
      }
      else
      {
        tmp_get_param->reset(true);
        tmp_get_param->set_is_read_consistency(is_read_consistency_);
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = compose_next_get_param(*tmp_get_param, fused_row_idx)))
        {
          TBSYS_LOG(WARN, "compose next get param fail:ret[%d]", ret);
        }
        else
        {
          next_get_param = tmp_get_param;
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = fetch_ups_row(*next_get_param)))
    {
      TBSYS_LOG(WARN, "fetch_ups_row fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletCacheJoin::gen_get_param(ObGetParam &get_param, const ObRow &fused_row)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObString value;

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = get_right_table_rowkey(fused_row, rowkey, rowkey_obj)))
    {
      TBSYS_LOG(WARN, "get rowkey from tmp_row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(handle_count_ >= batch_count_)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "handle_count_ is overflow[%ld]", handle_count_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ObTabletJoinCacheKey cache_key;
    cache_key.table_id_ = table_id_;
    cache_key.rowkey_ = rowkey;
    ret = ups_row_cache_->get(cache_key, value, cache_handle_[handle_count_]);
    if(OB_ENTRY_NOT_EXIST == ret)
    {
      if(OB_SUCCESS != (ret = compose_get_param(table_join_info_.right_table_id_, rowkey, get_param)))
      {
        if (OB_SIZE_OVERFLOW != ret)
        {
          TBSYS_LOG(WARN, "compose get param fail:ret[%d]", ret);
        }
      }
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "join cache get fail:ret[%d]", ret);
    }
    else
    {
      handle_count_ ++;
    }
  }
  return ret;
}
