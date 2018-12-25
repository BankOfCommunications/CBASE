/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_tablet_location_list.h,v 0.1 2010/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_COMMON_TABLET_LOCATION_ITEM_H_
#define OB_COMMON_TABLET_LOCATION_ITEM_H_

#include "common/ob_string_buf.h"
#include "common/ob_tablet_info.h" // ObTabletLocation

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    /// server and access err times
    struct ObTabletLocationItem
    {
      static const int64_t MAX_ERR_TIMES = 10;  //mod bingo [Paxos performance] 20170427:b:e
      int64_t err_times_;
      common::ObTabletLocation server_;
      ObTabletLocationItem() : err_times_(0) {}
    };
    
    /// tablet location item info
    class ObTabletLocationList 
    {
    public:
      ObTabletLocationList();
      virtual ~ObTabletLocationList();

    public:

      //mod zhaoqiong[roottable tablet management]20160104:b
      //static const int32_t MAX_REPLICA_COUNT = 3;
      static const int32_t MAX_REPLICA_COUNT = 6;
      //mod:e

      /// operator = the static content not contain the range buffer
      ObTabletLocationList & operator = (const ObTabletLocationList & other);

      /// add a tablet location to item list
      int add(const common::ObTabletLocation & location);

      /// del the index pos TabletLocation
      int del(const int64_t index, ObTabletLocationItem & location);

      /// set item invalid status
      int set_item_invalid(const ObTabletLocationItem & location);

      /// set item valid status
      void set_item_valid(const int64_t timestamp);

      /// get valid item count
      int64_t get_valid_count(void) const;

      /// operator random access
      ObTabletLocationItem & operator[] (const uint64_t index);

      /// sort the server list
      int sort(const common::ObServer & server);

      /// current tablet locastion server count
      int64_t size(void) const;

      /// clear all items
      void clear(void);

      /// get modify timestamp
      int64_t get_timestamp(void) const;
      /// set timestamp
      void set_timestamp(const int64_t timestamp);

      /// dump all info
      void print_info(void) const;
      
      int set_tablet_range(const common::ObNewRange & tablet_range);
      const common::ObNewRange & get_tablet_range() const;

      void set_buffer(common::ObStringBuf & buf);
      const common::ObStringBuf * get_buffer(void) const;
      /// serailize or deserialization
      NEED_SERIALIZE_AND_DESERIALIZE;

    private:
      int64_t cur_count_;
      int64_t timestamp_;
      common::ObNewRange tablet_range_;
      common::ObStringBuf *tablet_range_rowkey_buf_;
      ObTabletLocationItem locations_[MAX_REPLICA_COUNT];
    };

    inline void ObTabletLocationList::set_buffer(common::ObStringBuf & buf)
    {
      tablet_range_rowkey_buf_ = &buf;
    }
    
    inline const common::ObStringBuf * ObTabletLocationList::get_buffer(void) const
    {
      return tablet_range_rowkey_buf_;
    }

    inline int ObTabletLocationList::set_tablet_range(const common::ObNewRange & tablet_range)
    {
      int ret = OB_SUCCESS;

      if(&tablet_range != &(tablet_range_))
      {
        this->tablet_range_ = tablet_range;
      }
      
      /// ret = tablet_range_rowkey_buf_.reset();
      /// if(OB_SUCCESS != ret)
      /// {
      ///   TBSYS_LOG(WARN, "reset tablet range rowkey fail:ret[%d]", ret);
      /// }

      if((OB_SUCCESS == ret) && (NULL != tablet_range_rowkey_buf_))
      {
        ret = tablet_range_rowkey_buf_->write_string(tablet_range.start_key_, &(tablet_range_.start_key_));
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write tablet range start key fail:ret[%d]", ret);
        }
      }

      if((OB_SUCCESS == ret) && (NULL != tablet_range_rowkey_buf_))
      {
        ret = tablet_range_rowkey_buf_->write_string(tablet_range.end_key_, &(tablet_range_.end_key_));
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write tablet range end key fail:ret[%d]", ret);
        }
      }
      return ret;
    }

    inline const common::ObNewRange & ObTabletLocationList::get_tablet_range() const
    {
      return tablet_range_;
    }
    
    inline int64_t ObTabletLocationList::size(void) const
    {
      return cur_count_;
    }

    inline void ObTabletLocationList::clear(void)
    {
      timestamp_ = 0;
      cur_count_ = 0;
      tablet_range_rowkey_buf_ = NULL;
    }

    inline int64_t ObTabletLocationList::get_timestamp(void) const
    {
      return timestamp_;
    }

    inline void ObTabletLocationList::set_timestamp(const int64_t timestamp)
    {
      timestamp_ = timestamp;
    }

    inline ObTabletLocationItem & ObTabletLocationList::operator[] (const uint64_t index)
    {
      return locations_[index];
    }
  }
}



#endif // OB_COMMON_TABLET_LOCATION_ITEM_H_


