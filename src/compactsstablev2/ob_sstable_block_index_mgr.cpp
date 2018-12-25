#include "ob_sstable_block_index_mgr.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockIndexMgr::search_batch_blocks_by_key(
        const ObRowkey& key, const SearchMode mode,
        ObBlockPositionInfos& pos_info) const
    {
      int ret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;
      ObRowkey end_key;

      if (is_regular_mode(mode) && NULL == key.ptr())
      {
        TBSYS_LOG(ERROR, "is regular mode, but key==NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get bound error");
      }
      else if (OB_SUCCESS != (ret = find_by_key(key, mode, bound, find_it)))
      {
        TBSYS_LOG(ERROR, "find by key error");
      }
      else if (OB_SUCCESS != (ret = store_block_position_info(
              find_it, bound, mode, end_key, pos_info)))
      {
        TBSYS_LOG(ERROR, "store block position info error");
      }
      else
      {
      }

      return ret;
    }

    int ObSSTableBlockIndexMgr::search_batch_blocks_by_range(
        const ObNewRange& range, const bool is_reverse_scan,
        ObBlockPositionInfos& pos_info) const
    {
      int ret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;
      common::ObRowkey search_key;
      common::ObRowkey end_key;
      SearchMode mode = OB_SEARCH_MODE_MIN_VALUE;

      if (OB_SUCCESS != (ret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get bound error");
      }
      else if (OB_SUCCESS != (ret = trans_range_to_search_key(
              range, is_reverse_scan, search_key, mode)))
      {//基准key
        TBSYS_LOG(ERROR, "trans range to search key error");
      }
      else if (OB_SUCCESS != (ret = find_by_key(search_key, mode,
              bound, find_it)))
      {
        //TBSYS_LOG(ERROR, "find by key error");
      }
      else
      {
        if (is_reverse_scan && (!range.border_flag_.is_min_value()))
        {
          end_key = range.start_key_;
        }
        else if (!is_reverse_scan && (!range.border_flag_.is_max_value()))
        {
          end_key = range.end_key_;
        }

        if (OB_SUCCESS != (ret = store_block_position_info(
                find_it, bound, mode, end_key, pos_info)))
        {
          TBSYS_LOG(WARN, "stroe block position info error:ret=%d",
              ret);
        }
      }

      return ret;
    }


    int ObSSTableBlockIndexMgr::search_one_block_by_key(
        const ObRowkey& key, const SearchMode mode,
        ObBlockPositionInfo& pos_info) const
    {
      int ret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;

      if (is_regular_mode(mode) && NULL == key.ptr())
      {
        TBSYS_LOG(ERROR, "INVALID ARGUMENT");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get bound error");
      }
      else if (OB_SUCCESS != (ret = find_by_key(key, mode, bound, find_it)))
      {
        TBSYS_LOG(ERROR, "find by key error");
      }
      else
      {
        if (NULL != find_it && find_it < bound.end_ && find_it >= bound.begin_)
        {
          pos_info.offset_ = find_it->block_data_offset_;
          pos_info.size_ = (find_it + 1)->block_data_offset_
            - find_it->block_data_offset_;
        }
      }
      
      return ret;
    }

    int ObSSTableBlockIndexMgr::search_batch_blocks_by_offset(
        const int64_t offset, const SearchMode mode,
        ObBlockPositionInfos& pos_info) const
    {
      int ret = OB_SUCCESS;
      Bound bound;
      ObRowkey key;

      if (0 > offset)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument");
      }
      else if (OB_SUCCESS != (ret = get_bound(bound)))
      {
        TBSYS_LOG(WARN, "get bound error:ret=%d", ret);
      }
      else
      {
        ObSSTableBlockIndex item;
        item.block_data_offset_ = offset;
        item.block_endkey_offset_ = 0;
        const_iterator find_it = std::lower_bound(bound.begin_, 
            bound.end_, item);
        if (OB_SUCCESS != (ret = check_border(find_it, bound, mode)))
        {//check right border
          //TBSYS_LOG(WARN, "check border error");
        }
        else
        {
          switch (mode)
          {
            case OB_SEARCH_MODE_GREATER_THAN:
              if (*find_it == item)
              {
                ++ find_it;
              }

              if (find_it >= bound.end_)
              {
                ret = OB_BEYOND_THE_RANGE;
              }
              break;

            case OB_SEARCH_MODE_EQUAL:
              if (*find_it == item)
              {
                pos_info.block_count_ = 1;
              }
              else
              {
                ret = OB_BEYOND_THE_RANGE;
              }
              break;

            case OB_SEARCH_MODE_GREATER_EQUAL:
              break;

            case OB_SEARCH_MODE_LESS_THAN:
              find_it --;

            case OB_SEARCH_MODE_LESS_EQUAL:
              if (*find_it != item)
              {
                find_it --;
              }
              if (find_it < bound.begin_)
              {
                ret = OB_BEYOND_THE_RANGE;
              }
              break;

            default:
              ret = OB_SEARCH_MODE_NOT_IMPLEMENT;
              break;
          }//end switch
        }

        if (OB_SUCCESS == ret)
        {
          ret = store_block_position_info(find_it, bound, mode, key, pos_info);
        }
      }
      
      return ret;
    }

    ObSSTableBlockIndexMgr* ObSSTableBlockIndexMgr::copy(char* buffer) const
    {
      ObSSTableBlockIndexMgr* ret = reinterpret_cast<ObSSTableBlockIndexMgr*>(buffer);

      ret->block_index_base_ = buffer + sizeof(ObSSTableBlockIndexMgr);
      memcpy(ret->block_index_base_, block_index_base_, 
          block_index_length_ + block_endkey_length_);
      ret->block_index_length_ = block_index_length_;
      ret->block_endkey_base_ = ret->block_index_base_ + block_index_length_;
      ret->block_endkey_length_ = block_endkey_length_;
      ret->block_count_ = block_count_;

      return ret;
    }

    int ObSSTableBlockIndexMgr::find_by_key(
        const ObRowkey& key, const SearchMode mode,
        const Bound& bound, const_iterator& find) const
    {
      int ret = OB_SUCCESS;

      if (OB_SEARCH_MODE_MIN_VALUE == mode)
      {
        find = bound.begin_;
      }
      else if (OB_SEARCH_MODE_MAX_VALUE == mode)
      {
        find = bound.end_;
        find --;
      }
      else
      {
        find = std::lower_bound(bound.begin_, bound.end_,
            key, Compare(*this));

        if (OB_SUCCESS != (ret = check_border(find, bound, mode)))
        {
          //TBSYS_LOG(WARN, "check border error");
        }
        else
        {
          ObRowkey compare_key;
          if (OB_SUCCESS != (ret = get_row_key(*find, compare_key)))
          {
            TBSYS_LOG(WARN, "get row key error:ret=%d", ret);
          }
          else
          {
            switch (mode)
            {
              case OB_SEARCH_MODE_EQUAL:
              case OB_SEARCH_MODE_GREATER_EQUAL:
                break;
              case OB_SEARCH_MODE_GREATER_THAN:
                if (key == compare_key)
                {
                  find ++;
                }
                if (find >= bound.end_)
                {
                  ret = OB_BEYOND_THE_RANGE;
                }
                break;
              case OB_SEARCH_MODE_LESS_THAN:
              case OB_SEARCH_MODE_LESS_EQUAL:
                break;
              default:
                ret = OB_SEARCH_MODE_NOT_IMPLEMENT;
                break;
            }
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockIndexMgr::store_block_position_info(
        const_iterator find, const ObSSTableBlockIndexMgr::Bound& bound,
        const SearchMode mode, const ObRowkey& key,
        ObBlockPositionInfos& pos_info) const
    {
      int ret = OB_SUCCESS;
      ObRowkey cur_key;

      if (find < bound.begin_ || find >= bound.end_)
      {
        TBSYS_LOG(WARN, "beyond the range");
        ret = OB_ERROR;
      }
      else
      {
        int64_t need_block_count = pos_info.block_count_;
        pos_info.block_count_ = 0;
        int64_t count = 0;
        if (is_looking_forward_mode(mode))
        {
          for (; find < bound.end_ && count < need_block_count; find ++)
          {
            pos_info.position_info_[count].offset_ = find->block_data_offset_;
            pos_info.position_info_[count].size_ = 
              (find + 1)->block_data_offset_ - find->block_data_offset_;
            count ++;
            if (OB_SUCCESS != (ret = get_row_key(*find, cur_key)))
            {
              TBSYS_LOG(WARN, "get row key error:ret=%d", ret);
            }
            else if (NULL != key.ptr() && key.compare(cur_key) <= 0)
            {
              break;
            }
          }
        }
        else
        {
          int64_t max_count = find - bound.begin_ + 1;
          if (max_count > need_block_count)
          {
            max_count = need_block_count;
          }
          const_iterator start = find - max_count + 1;

          for (; start <= find && count < max_count; ++ start)
          {
            if (OB_SUCCESS != (ret = get_row_key(*start, cur_key)))
            {
              TBSYS_LOG(WARN, "get row key error:ret=%d", ret);
            }
            else if (NULL != key.ptr() && key.compare(cur_key) > 0)
            {
              continue;
            }

            pos_info.position_info_[count].offset_ = start->block_data_offset_;
            pos_info.position_info_[count].size_ = 
              (start + 1)->block_data_offset_ - start->block_data_offset_;
            count ++;
          }
        }
        pos_info.block_count_ = count;

        if (0 == pos_info.block_count_)
        {
          TBSYS_LOG(WARN, "BEYOND THE RANGE");
          ret = OB_BEYOND_THE_RANGE;
        }
      }

      return ret;
    }

    int ObSSTableBlockIndexMgr::trans_range_to_search_key(
        const ObNewRange& range, const bool is_reverse_scan,
        ObRowkey& search_key, SearchMode& mode) const
    {
      ObBorderFlag border_flag = range.border_flag_;

      if (!is_reverse_scan)
      {
        if (border_flag.is_min_value())
        {
          mode = OB_SEARCH_MODE_MIN_VALUE;
        }
        else if (border_flag.inclusive_start())
        {
          mode = OB_SEARCH_MODE_GREATER_EQUAL;
        }
        else
        {
          mode = OB_SEARCH_MODE_GREATER_THAN;
        }
        search_key = range.start_key_;
      }
      else
      {
        if (border_flag.is_max_value())
        {
          mode = OB_SEARCH_MODE_MAX_VALUE;
        }
        else if (border_flag.inclusive_end())
        {
          mode = OB_SEARCH_MODE_LESS_EQUAL;
        }
        else
        {
          mode = OB_SEARCH_MODE_LESS_THAN;
        }
        search_key = range.end_key_;
      }

      return OB_SUCCESS;
    }

    int ObSSTableBlockIndexMgr::get_row_key(const ObSSTableBlockIndex& index, 
        common::ObRowkey& key) const
    {
      int ret = OB_SUCCESS;
      ObCompactCellIterator row;
      char* key_ptr = block_endkey_base_ + index.block_endkey_offset_;
      int rowkey_obj_count = 0;
      ObObj* rowkey_buf_array = NULL;
      common::ModuleArena* arena = GET_TSI_MULT(ModuleArena,
          TSI_SSTABLE_MODULE_ARENA_1);

      if (NULL == (rowkey_buf_array = reinterpret_cast<ObObj*>(arena->alloc(
              sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER))))
      {
        TBSYS_LOG(WARN, "alloc error");
      }
      else if (OB_SUCCESS != (ret = row.init(key_ptr, DENSE)))
      {
        TBSYS_LOG(WARN, "row init error");
      }
      else
      {
        const ObObj* obj = NULL;
        bool is_row_finished = false;
        
        do
        {
          if (OB_SUCCESS != (ret = row.next_cell()))
          {
            TBSYS_LOG(WARN, "row next cell error");
            break;
          }
          else
          {
            if (OB_SUCCESS != (ret = row.get_cell(obj, &is_row_finished)))
            {
              TBSYS_LOG(WARN, "row get cell error");
            }
            else
            {
              rowkey_buf_array[rowkey_obj_count] = *obj;
              if (is_row_finished)
              {
                break;
              }
              else
              {
                rowkey_obj_count ++;
              }
            }
          }
        }while(true);
      }

      if (OB_SUCCESS == ret)
      {
        key.assign(rowkey_buf_array, rowkey_obj_count);
      }

      return ret;
    }

    int ObSSTableBlockIndexMgr::check_border(
        ObSSTableBlockIndexMgr::const_iterator& find, 
        const Bound& bound, const SearchMode mode) const
    {
      int ret = OB_SUCCESS;

      if (find >= bound.end_)
      {//大于sstable中的所有rowkey
        if (is_looking_forward_mode(mode))
        {//min, ==, >=, >
          ret = OB_BEYOND_THE_RANGE;
        }
        else
        {//max, <=, <
          //上一个block
          find = bound.end_;
          find --;
          if (find < bound.begin_)
          {//只有一个block
            ret = OB_BEYOND_THE_RANGE;
          }
        }
      }

      return ret;
    }

  }//end namespace compactsstblev2
}//end namespace oceanbase
