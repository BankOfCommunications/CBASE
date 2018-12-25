#include "common/utility.h"
#include "ob_tablet_location_list.h"
#include "ob_tablet_location_range_iterator.h"

using namespace oceanbase::common;

ObTabletLocationRangeIterator::ObTabletLocationRangeIterator()
{
  init_ = false;
  is_iter_end_ = false;
}

ObTabletLocationRangeIterator::~ObTabletLocationRangeIterator()
{
}

int ObTabletLocationRangeIterator::initialize(ObTabletLocationCacheProxy * cache_proxy,
    const ObNewRange * iter_range, ScanFlag::Direction scan_direction, ObStringBuf * range_rowkey_buf)
{
  int ret = OB_SUCCESS;
  if (NULL == cache_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cache_proxy is null");
  }

  if (OB_SUCCESS == ret && NULL == range_rowkey_buf)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "range_rowkey_buf is null");
  }

  if (OB_SUCCESS == ret && NULL == iter_range)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "scan_param_in is null");
  }

  if (OB_SUCCESS == ret)
  {
    is_iter_end_ = false;
    cache_proxy_ = cache_proxy;
    range_rowkey_buf_ = range_rowkey_buf;
    scan_direction_ = scan_direction;
    org_scan_range_ = iter_range;
    next_range_ = *iter_range;
  }

  if (OB_SUCCESS == ret)
  {
    ret = range_rowkey_buf_->write_string(org_scan_range_->start_key_, &(next_range_.start_key_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "string buf write fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = range_rowkey_buf_->write_string(org_scan_range_->end_key_, &(next_range_.end_key_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "string buf write fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    init_ = true;
  }

  return ret;
}


int ObTabletLocationRangeIterator::range_intersect(const ObNewRange &r1, const ObNewRange& r2,
  ObNewRange& r3, ObStringBuf& range_rowkey_buf) const
{
  int ret = OB_SUCCESS;
  if (r1.table_id_ != r2.table_id_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "table id is not equal:r1.table_id_[%ld], r2.table_id_[%ld]",
      r1.table_id_, r2.table_id_);
  }

  if (OB_SUCCESS == ret)
  {
    if (r1.empty() || r2.empty())
    {
      TBSYS_LOG(DEBUG, "range r1 or range r2 is emptry:r1 empty[%d], r2 empty[%d]",
        (int)r1.empty(), (int)r2.empty());
      ret = OB_EMPTY_RANGE;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (r1.intersect(r2)) // if r1 inersect r2 is not empty
    {
      r3.reset();

      r3.table_id_ = r1.table_id_;

      const ObNewRange * inner_range = NULL;

      if ( r1.compare_with_startkey2(r2) >= 0) // if r1 start key is greater than or equal to r2 start key
      {
        inner_range = &r1;
      }
      else // if r1 start key less than r2 start key
      {
        inner_range = &r2;
      }

      if (inner_range->start_key_.is_min_row())
      {
        r3.start_key_.set_min_row();
      }

      if (inner_range->border_flag_.inclusive_start())
      {
        r3.border_flag_.set_inclusive_start();
      }
      else
      {
        r3.border_flag_.unset_inclusive_start();
      }

      ret = range_rowkey_buf.write_string(inner_range->start_key_, &(r3.start_key_));
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write start key fail:ret[%d]", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if ( r1.compare_with_endkey2(r2) <= 0 ) // if r1 end key is less than or equal to r2 end key
        {
          inner_range = &r1;
        }
        else // if r1 end key is greater than r2 end key
        {
          inner_range = &r2;
        }

        if (inner_range->end_key_.is_max_row())
        {
          r3.end_key_.set_max_row();
        }

        if (inner_range->border_flag_.inclusive_end())
        {
          r3.border_flag_.set_inclusive_end();
        }
        else
        {
          r3.border_flag_.unset_inclusive_end();
        }

        ret = range_rowkey_buf.write_string(inner_range->end_key_, &(r3.end_key_));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write end key fail:ret[%d]", ret);
        }
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "dump range r1:%s, r2:%s", to_cstring(r1), to_cstring(r2));
    }
  }

  return ret;
}

int ObTabletLocationRangeIterator::next(common::ObChunkServerItem * replicas_out,
  int32_t & replica_count_in_out, ObNewRange & tablet_range_out)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTabletLocationRangeIterator not init yet");
  }

  if (OB_SUCCESS == ret && NULL == replicas_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replicas_out is null");
  }

  if (OB_SUCCESS == ret && replica_count_in_out <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in_out should be positive: replica_count_in_out[%d]", replica_count_in_out);
  }

  if (OB_SUCCESS == ret && is_iter_end_)
  {
    ret = OB_ITER_END;
  }

  if (OB_SUCCESS == ret && next_range_.empty())
  {
    ret = OB_ITER_END;
    is_iter_end_ = true;
  }

  ObTabletLocationList location_list;
  location_list.set_buffer(*range_rowkey_buf_);
  if (OB_SUCCESS == ret &&
    OB_SUCCESS != (ret = cache_proxy_->get_tablet_location(scan_direction_, &next_range_, location_list)))
  {
    TBSYS_LOG(WARN, "get tablet location fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t size = location_list.size();
    if (size > replica_count_in_out)
    {
      TBSYS_LOG(INFO, "replica count get from rootserver is greater than the expectation:replica_count_in_out[%d],\
        location_list.size()[%ld]", replica_count_in_out, size);
    }
    int32_t fill_count = 0;
    for (int i = 0; (i < size) && (fill_count < replica_count_in_out); ++i)
    {
      // no used
      if (location_list[i].err_times_ < ObTabletLocationItem::MAX_ERR_TIMES)
      {
        replicas_out[fill_count].addr_ = location_list[i].server_.chunkserver_;
        //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
        replicas_out[fill_count].tablet_version_ = location_list[i].server_.tablet_version_;
        //add:e
        replicas_out[fill_count].status_ = common::ObChunkServerItem::UNREQUESTED;
        ++fill_count;
      }
    }
    replica_count_in_out = fill_count;
    ret = range_intersect(location_list.get_tablet_range(), next_range_, tablet_range_out, *range_rowkey_buf_);
    if(OB_SUCCESS != ret && OB_EMPTY_RANGE != ret)
    {
      TBSYS_LOG(DEBUG, "dump location_list.get_tablet_range(%s)", to_cstring(location_list.get_tablet_range()));
      TBSYS_LOG(DEBUG, "dump next_range_(%s)", to_cstring(next_range_));
      TBSYS_LOG(WARN, "intersect next range and location list range fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = next_range_.trim(tablet_range_out, *range_rowkey_buf_);
    if (OB_EMPTY_RANGE == ret)
    {
      ret = OB_SUCCESS;
      is_iter_end_ = true;
    }

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "trim next range fail:ret[%d]", ret);
    }
  }

  /// ugly implimentation to process the last rowkey of last tablet, which is all '0xff'
  if ((OB_SUCCESS == ret)
    &&  (tablet_range_out.compare_with_endkey2(location_list.get_tablet_range()) == 0)
    &&  (next_range_.end_key_.is_max_row())
    &&  (tablet_range_out.end_key_.length() > 0)
    &&  (tablet_range_out.end_key_.ptr()[0].is_max_value()))
  {
    bool is_max_row_key = true;
    if(is_max_row_key)
    {
      tablet_range_out.end_key_.set_max_row();
      is_iter_end_ = true;
    }
  }

  /// using serialized access cs when scan backward, we can add more serializing access strategy here
  if((OB_SUCCESS == ret)
     && (scan_direction_ == ScanFlag::BACKWARD))
  {
    tablet_range_out = next_range_;
    is_iter_end_ = true;
    TBSYS_LOG(INFO, "trigger seralizing access cs when backward scan");
  }
  return ret;
}
