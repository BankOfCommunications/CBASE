
/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - ob range class, 
 *               
 */
#ifndef OCEANBASE_COMMON_OB_RANGE2_H_
#define OCEANBASE_COMMON_OB_RANGE2_H_

#include <tbsys.h>
#include "ob_define.h"
#include "ob_rowkey.h"
#include "ob_range.h"
#include "ob_malloc.h"
#include "common/utility.h"

namespace oceanbase 
{ 
  namespace common
  {
    struct ObNewRange : public RowkeyInfoHolder
    {
      uint64_t table_id_;
      ObBorderFlag border_flag_;
      ObRowkey start_key_;
      ObRowkey end_key_;


      ObNewRange()
      {
        reset();
      }

      ~ObNewRange()
      {
        reset();
      }

      inline void reset()
      {
        table_id_ = OB_INVALID_ID;
        border_flag_.set_data(0);
        start_key_.assign(NULL, 0);
        end_key_.assign(NULL, 0);
      }

      inline const ObRowkeyInfo* fetch_rowkey_info(const ObNewRange & l, const ObNewRange & r) const
      {
        const ObRowkeyInfo * ri = NULL;
        if (NULL != l.rowkey_info_)
        {
          ri = l.rowkey_info_;
        }
        else if (NULL != r.rowkey_info_)
        {
          ri = r.rowkey_info_;
        }
        return ri;
      }

      // new compare func for tablet.range and scan_param.range
      inline int compare_with_endkey2(const ObNewRange & r) const
      {
        int cmp = 0;
        ObRowkeyLess less(fetch_rowkey_info(*this, r));
        if (end_key_.is_max_row())
        {
          if (!r.end_key_.is_max_row())
          {
            cmp = 1;
          }
        }
        else if (r.end_key_.is_max_row())
        {
          cmp = -1;
        }
        else
        {
          cmp = less.compare(end_key_, r.end_key_);
          if (0 == cmp)
          {
            if (border_flag_.inclusive_end() && !r.border_flag_.inclusive_end())
            {
              cmp = 1;
            }
            else if (!border_flag_.inclusive_end() && r.border_flag_.inclusive_end())
            {
              cmp = -1;
            }
          }
        }
        return cmp;
      }

      inline int compare_with_endkey(const ObNewRange & r) const 
      {
        int cmp = 0;
        ObRowkeyLess less(fetch_rowkey_info(*this, r));
        if (table_id_ != r.table_id_) 
        {
          cmp = (table_id_ < r.table_id_) ? -1 : 1;
        } 
        else 
        {
          if (end_key_.is_max_row())
          {
            // MAX_VALUE == MAX_VALUE;
            if (r.end_key_.is_max_row())
            {
              cmp = 0;
            }
            else
            {
              cmp = 1;
            }
          }
          else
          {
            if (r.end_key_.is_max_row())
            {
              cmp = -1;
            }
            else
            {
              cmp = less.compare(end_key_, r.end_key_);
            }
          }
        }
        return cmp;
      }

      inline int compare_with_startkey(const ObNewRange & r) const 
      {
        int cmp = 0;
        ObRowkeyLess less(fetch_rowkey_info(*this, r));
        if (table_id_ != r.table_id_) 
        {
          cmp = (table_id_ < r.table_id_) ? -1 : 1;
        } 
        else 
        {
          if (start_key_.is_min_row())
          {
            // MIN_VALUE == MIN_VALUE;
            if (r.start_key_.is_min_row())
            {
              cmp = 0;
            }
            else
            {
              cmp = -1;
            }
          }
          else
          {
            if (r.start_key_.is_min_row())
            {
              cmp = 1;
            }
            else
            {
              cmp = less.compare(start_key_, r.start_key_);
            }
          }
        }
        return cmp;
      }

      inline int compare_with_startkey2(const ObNewRange & r) const 
      {
        int cmp = 0;
        ObRowkeyLess less(fetch_rowkey_info(*this, r));
        if (start_key_.is_min_row())
        {
          if (!r.start_key_.is_min_row())
          {
            cmp = -1;
          }
        }
        else if (r.start_key_.is_min_row())
        {
          cmp = 1;
        }
        else
        {
          cmp = less.compare(start_key_, r.start_key_);
          if (0 == cmp)
          {
            if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start())
            {
              cmp = -1;
            }
            else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start())
            {
              cmp = 1;
            }
          }
        }
        return cmp;
      }

      inline bool empty() const 
      {
        bool ret = false;
        if (start_key_.is_min_row() || end_key_.is_max_row())
        {
          ret = false;
        }
        else 
        {
          ret  = end_key_ < start_key_ 
            || ((end_key_ == start_key_) 
                && !((border_flag_.inclusive_end()) 
                  && border_flag_.inclusive_start()));
        }
        return ret;
      }

      // from MIN to MAX, complete set.
      inline bool is_whole_range() const 
      {
        return (start_key_.is_min_row()) && (end_key_.is_max_row());
      }

      // from MIN to MAX, complete set.
      inline void set_whole_range()
      {
        start_key_.set_min_row();
        end_key_.set_max_row();
        border_flag_.unset_inclusive_start();
        border_flag_.unset_inclusive_end();
      }

      inline bool is_left_open_right_closed() const 
      {
        return (!border_flag_.inclusive_start() && border_flag_.inclusive_end()) || end_key_.is_max_row();
      }


      inline bool equal(const ObNewRange& r) const
      {
        return (compare_with_startkey(r) == 0) && (compare_with_endkey(r) == 0);
      }

      inline bool equal2(const ObNewRange& r) const
      {
        return (table_id_ == r.table_id_) && (compare_with_startkey2(r) == 0) && (compare_with_endkey2(r) == 0);
      }

      bool intersect(const ObNewRange& r) const;

      // cut out the range r from this range, this range will be changed
      // r should be included by this range
      // one of condition below should be satisfied
      // 1. the left end of r should equal to the left end of this range
      // 2. the right end of r should equal to the right end of this range
      // 
      // @param r ObRange 
      // @param string_buf string memory for changed this range rowkey
      //
      // if r equal to this range, than will return OB_EMPTY_RANGE, this range will NOT CHANGED
      int trim(const ObNewRange& r, ObStringBuf & string_buf);

      bool check(void) const;

      void hex_dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
      int64_t to_string(char* buffer, const int64_t length) const;
      int64_t hash() const;
      inline bool operator == (const ObNewRange &other) const
      {
        return equal(other);
      };

      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
      int64_t get_serialize_size(void) const;

      template <typename Allocator>
      int deserialize(Allocator& allocator, const char* buf, const int64_t data_len, int64_t& pos);

    };

    template <typename Allocator>
      int ObNewRange::deserialize(Allocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
      {
        int ret = OB_SUCCESS;
        ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        ObNewRange copy_range;
        copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        ret = copy_range.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = deep_copy_range(allocator, copy_range, *this);
        }

        return ret;
      }

    template <typename Allocator>
      inline int deep_copy_range(Allocator& allocator, const ObNewRange &src, ObNewRange &dst)
      {
        int ret = OB_SUCCESS;
        if ( OB_SUCCESS == (ret = src.start_key_.deep_copy(dst.start_key_, allocator)) 
            && OB_SUCCESS == (ret = src.end_key_.deep_copy(dst.end_key_, allocator)) )
        {
          dst.table_id_ = src.table_id_;
          dst.border_flag_ = src.border_flag_;
        }
        return ret;
      }

    template <typename Allocator>
    inline int deep_copy_range(Allocator& start_key_allocator,
        Allocator& end_key_allocator, const ObNewRange& src,
        ObNewRange& dst)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = src.start_key_.deep_copy(dst.start_key_, start_key_allocator)))
      {
        char buf[1024];
        dst.start_key_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "start key deep copy error:start_key_=%s", buf);
      }
      else if (OB_SUCCESS != (ret = src.end_key_.deep_copy(dst.end_key_, end_key_allocator)))
      {
        char buf[1024];
        dst.end_key_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "end key deep copy error:end_key_=%s", buf);
      }
      else
      {
        dst.table_id_ = src.table_id_;
        dst.border_flag_ = src.border_flag_;
      }

      return ret;
    }
  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_

