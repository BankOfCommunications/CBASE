/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_sstable_scan.h"
#include "common/utility.h"
#include "sstable/ob_block_index_cache.h"
#include "chunkserver/ob_tablet_image.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "compactsstablev2/ob_sstable_block_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::chunkserver;

ObSSTableScan::ObSSTableScan()
  : iterator_(NULL), last_rowkey_(NULL), sstable_version_(0), row_counter_(0)
{
}

ObSSTableScan::~ObSSTableScan()
{
}

void ObSSTableScan::reset()
{
  iterator_ = NULL;
  last_rowkey_ = NULL;
  sstable_version_ = 0;
  row_counter_ = 0;
}

void ObSSTableScan::reuse()
{
  iterator_ = NULL;
  last_rowkey_ = NULL;
  sstable_version_ = 0;
  row_counter_ = 0;
}


int ObSSTableScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSSTableScan, PHY_SSTABLE_SCAN);
  }
}

int64_t ObSSTableScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "SSTableScan()\n");
  return pos;
}

int ObSSTableScan::init_sstable_scanner()
{
  int ret = OB_SUCCESS;
  int32_t size = 1;
  ObTablet* tablet = scan_context_.tablet_;

  if (NULL == tablet)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((sstable_version_ = tablet->get_sstable_version()) < SSTableReader::COMPACT_SSTABLE_VERSION)
  {
    if (OB_SUCCESS != (ret = tablet->find_sstable(
            scan_param_.get_range(), &scan_context_.sstable_reader_, size)) )
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else if (OB_SUCCESS != (ret = scanner_.set_scan_param(scan_param_, &scan_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else
    {
      iterator_ = &scanner_;
    }
  }
  else
  {
    if (OB_SUCCESS != (ret = tablet->find_sstable(
            scan_param_.get_range(), &scan_context_.compact_context_.sstable_reader_, size)) )
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else if (OB_SUCCESS != (ret = compact_scanner_.set_scan_param(&scan_param_, &scan_context_.compact_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else
    {
      iterator_ = &compact_scanner_;
    }
  }

  return ret;
}

int ObSSTableScan::open()
{
  // set row description base on query columns;
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = init_sstable_scanner()))
  {
    TBSYS_LOG(WARN, "fail to set scan param for scanner");
  }
  // dispatch column groups according input query columns
  row_counter_ = 0;
  return ret;
}

int ObSSTableScan::close()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "sstable scan row count=%ld", row_counter_);
  scanner_.cleanup();
  // release tablet object.
  if (NULL != scan_context_.tablet_)
  {
    ret = scan_context_.tablet_image_->release_tablet(scan_context_.tablet_);
    scan_context_.tablet_ = NULL;
  }
  return ret;
}
//add wenghaixing [secondary index static_index_build.cs_scan]20150416
int ObSSTableScan::open_scan_context_local_idx(const sstable::ObSSTableScanParam &param, const ScanContext &context, ObNewRange &fake_range)
{
  int ret = OB_SUCCESS;
  scan_param_ = param;
  scan_context_ = context;
  // acquire tablet object.
  int64_t query_version = 0;
  if ((query_version = scan_param_.get_version_range().get_query_version()) < 0)
  {
    TBSYS_LOG(ERROR, "empty version range to scan, version_range=%s",
        to_cstring(scan_param_.get_version_range()));
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = scan_context_.tablet_image_->acquire_tablet(fake_range,
          ObMultiVersionTabletImage::SCAN_FORWARD, query_version, scan_context_.tablet_)))
  {
    TBSYS_LOG(WARN, "cannot acquire tablet with scan range: %s, version: %ld",
        to_cstring(fake_range), query_version);
  }
  //add liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
  else if (NULL == scan_context_.tablet_)
  {
    TBSYS_LOG(WARN, "tablet is null point!");
    ret = OB_ERROR;
  }
  //add e
  else if (NULL != scan_context_.tablet_ && (scan_context_.tablet_->is_removed()
           || scan_context_.tablet_->get_range().compare_with_startkey2(fake_range) > 0))
  {
    TBSYS_LOG(WARN, "tablet is removed or tablet_range.start_key > scan_range.start_key, "
                    "can't scan, tablet_range=%s, scan_range=%s",
        to_cstring(scan_context_.tablet_->get_range()), to_cstring(fake_range));
    ret = scan_context_.tablet_image_->release_tablet(scan_context_.tablet_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, range:%s",
        scan_context_.tablet_, to_cstring(scan_context_.tablet_->get_range()));
    }
    scan_context_.tablet_ = NULL;
    ret = OB_CS_TABLET_NOT_EXIST;
  }
  return ret;
}

int ObSSTableScan::init_sstable_scanner_for_local_idx(ObNewRange &fake_range)
{
  int ret = OB_SUCCESS;
  int32_t size = 1;
  ObTablet* tablet = scan_context_.tablet_;

  if (NULL == tablet)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((sstable_version_ = tablet->get_sstable_version()) < SSTableReader::COMPACT_SSTABLE_VERSION)
  {
   // TBSYS_LOG(ERROR, "test::whx scan_param = (%s)",to_cstring(scan_param_));
    if (OB_SUCCESS != (ret = tablet->find_loc_idx_sstable(
            fake_range, &scan_context_.sstable_reader_, size)) )
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(fake_range), sstable_version_);
    }
    else if (OB_SUCCESS != (ret = scanner_.set_scan_param(scan_param_, &scan_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else
    {
      iterator_ = &scanner_;

    }
  }
  else
  {
    if (OB_SUCCESS != (ret = tablet->find_loc_idx_sstable(
            fake_range, &scan_context_.compact_context_.sstable_reader_, size)) )
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(fake_range), sstable_version_);
    }
    else if (OB_SUCCESS != (ret = compact_scanner_.set_scan_param(&scan_param_, &scan_context_.compact_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(scan_param_.get_range()), sstable_version_);
    }
    else
    {
      iterator_ = &compact_scanner_;
    }
  }

  return ret;
}

int ObSSTableScan::reset_scanner()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "sstable scan row count=%ld", row_counter_);
  scanner_.clear();//LMZ, free thread local memory
  // release tablet object.
  if (NULL != scan_context_.tablet_)
  {
    ret = scan_context_.tablet_image_->release_tablet(scan_context_.tablet_);
    scan_context_.tablet_ = NULL;
  }
  return ret;
}

//add e
int ObSSTableScan::open_scan_context(const sstable::ObSSTableScanParam& param, const ScanContext& context)
{
  int ret = OB_SUCCESS;
  scan_param_ = param;
  scan_context_ = context;
  // acquire tablet object.
  int64_t query_version = 0;
  if ((query_version = scan_param_.get_version_range().get_query_version()) < 0)
  {
    TBSYS_LOG(ERROR, "empty version range to scan, version_range=%s",
        to_cstring(scan_param_.get_version_range()));
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = scan_context_.tablet_image_->acquire_tablet(scan_param_.get_range(),
          ObMultiVersionTabletImage::SCAN_FORWARD, query_version, scan_context_.tablet_)))
  {
    TBSYS_LOG(WARN, "cannot acquire tablet with scan range: %s, version: %ld",
        to_cstring(scan_param_.get_range()), query_version);
  }
  else if (NULL != scan_context_.tablet_ && (scan_context_.tablet_->is_removed()
           || scan_context_.tablet_->get_range().compare_with_startkey2(scan_param_.get_range()) > 0))
  {
    /**
     * if tablet is removed or tablet_range.start_key > scan_range.start_key, cs 
     * can't scan this tablet and return data to ms. for example: 
     * tablet C (0,10] splits to tablet A (0,5] and tablet B (5, 10], and tablet 
     * A is migrated to other cs, then if ms scan range (0,10] from this cs, 
     * this cs can't scan tablet B (5,10] and return data to ms, otherwise ms 
     * can't scan the rest range (0,5] from other cs, the result is incorrect. 
     * for this case, cs just refuse this scan, and return error code
     * OB_CS_TABLET_NOT_EXIST, and ms must scan the range (0,5] first.
     */
    TBSYS_LOG(WARN, "tablet is removed or tablet_range.start_key > scan_range.start_key, "
                    "can't scan, tablet_range=%s, scan_range=%s",
        to_cstring(scan_context_.tablet_->get_range()), to_cstring(scan_param_.get_range()));
    ret = scan_context_.tablet_image_->release_tablet(scan_context_.tablet_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, range:%s",
        scan_context_.tablet_, to_cstring(scan_context_.tablet_->get_range()));
    }
    scan_context_.tablet_ = NULL;
    ret = OB_CS_TABLET_NOT_EXIST;
  }
  return ret;
}

int ObSSTableScan::get_next_row(const ObRowkey* &row_key, const ObRow *&row_value)
{
  int ret = OB_SUCCESS;
  if (NULL == iterator_)
  {
    TBSYS_LOG(ERROR, "internal error, iterator_ not set, call init_sstable_scanner");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = iterator_->get_next_row(row_key, row_value);
  }

  if (OB_SUCCESS == ret)
  {
    last_rowkey_ = row_key;
    ++row_counter_;
  }
  return ret;
}

int ObSSTableScan::get_tablet_data_version(int64_t &version)
{
  int ret = OB_SUCCESS;
  if(NULL == scan_context_.tablet_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invoke this func after open");
  }
  else
  {
    version = scan_context_.tablet_->get_data_version();
  }
  return ret;
}

int ObSSTableScan::get_tablet_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if(NULL == scan_context_.tablet_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invoke this func after open");
  }
  else
  {
    range = scan_context_.tablet_->get_range();
  }
  return ret;
}

int ObSSTableScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == iterator_)
  {
    ret = OB_NOT_INIT;
  }
  else if(OB_SUCCESS != (ret = iterator_->get_row_desc(row_desc)))
  {
    TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObSSTableScan::get_last_rowkey(const ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = last_rowkey_;
  return ret;
}
