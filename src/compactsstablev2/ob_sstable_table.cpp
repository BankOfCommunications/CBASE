#include "ob_sstable_table.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableTable::switch_table_reset()
    {
      int ret = OB_SUCCESS;

      if (0 == table_range_array_flag_)
      {
        table_range_array_flag_ = 1;
        table_range_array_[1].reset();
      }
      else if (1 == table_range_array_flag_)
      {
        table_range_array_flag_ = 0;
        table_range_array_[0].reset();
      }
      else
      {
        TBSYS_LOG(ERROR, "can not go herer");
      }

      table_bloomfilter_.clear();
      //block_index_.reset();
      memset(&block_index_, 0, sizeof(block_index_));
      block_index_builder_.reset();
      block_endkey_builder_.reset();
      table_range_builder_.reset();

      return ret;
    }

    int ObSSTableTable::switch_sstable_reset()
    {
      int ret = OB_SUCCESS;
      int64_t old_index = 0;
      int64_t cur_index = 0;

      if (0 == table_range_array_flag_)
      {
        old_index = 0;
        cur_index = 1;
      }
      else if (1 == table_range_array_flag_)
      {
        old_index = 1;
        cur_index = 0;
      }
      else
      {
        TBSYS_LOG(ERROR, "can not go here");
      }

      TableRangeStruct& old_range = table_range_array_[old_index];
      TableRangeStruct& cur_range = table_range_array_[cur_index];
      cur_range.reset();
      cur_range.table_range_.table_id_
        = old_range.table_range_.table_id_;
      if (old_range.table_range_.border_flag_.inclusive_end())
      {
        cur_range.table_range_.border_flag_.set_inclusive_end();
      }
      else
      {
        cur_range.table_range_.border_flag_.unset_inclusive_end();
      }

      if (old_range.table_range_.border_flag_.is_max_value())
      {
        cur_range.table_range_.border_flag_.set_max_value();
      }
      else
      {
        cur_range.table_range_.border_flag_.unset_max_value();
      }

      cur_range.table_range_.border_flag_.unset_min_value();
      cur_range.table_range_.border_flag_.unset_inclusive_start();
      old_range.table_range_.border_flag_.unset_max_value();
      old_range.table_range_.border_flag_.set_inclusive_end();

      ObMemBufAllocatorWrapper cur_start_key_allocator(
          cur_range.start_key_buf_);
      ObMemBufAllocatorWrapper cur_end_key_allocator(
          cur_range.end_key_buf_);
      ObMemBufAllocatorWrapper old_end_key_allocator(
          old_range.end_key_buf_);

      if (OB_SUCCESS != (ret
            = old_range.table_range_.end_key_.deep_copy(
              cur_range.table_range_.end_key_, cur_end_key_allocator)))
      {
        char buf[1024];
        old_range.table_range_.end_key_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "old range table range end key deep copy error:"
            "ret=%d,old_end_key_=%s", ret, buf);
      }
      else if (OB_SUCCESS != (ret = block_endkey_.deep_copy(
              cur_range.table_range_.start_key_,
              cur_start_key_allocator)))
      {
        char buf[1024];
        block_endkey_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "block endkey deep copy error:ret=%d,"
            "block_endkey_=%s", ret, buf);
      }
      else if (OB_SUCCESS != (ret = block_endkey_.deep_copy(
              old_range.table_range_.end_key_, old_end_key_allocator)))
      {
        char buf[1024];
        block_endkey_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "block endkey deep copy error:ret=%d,"
            "block_endkey_=%s", ret, buf);
      }
      else
      {
        cur_range.available_ = true;
        table_range_array_flag_ = cur_index;
      }

      table_bloomfilter_.clear();
      block_index_.reset();
      block_index_builder_.reset();
      block_endkey_builder_.reset();
      table_range_builder_.reset();

      return ret;
    }

    int ObSSTableTable::init(const int64_t bloomfilter_nhash, 
        const int64_t bloomfilter_nbyte)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = table_bloomfilter_.init(
              bloomfilter_nhash, bloomfilter_nbyte)))
      {
        TBSYS_LOG(WARN, "init error:ret=%d", ret);
      }
      
      return ret;
    }

    bool ObSSTableTable::check_rowkey_range(const::ObRowkey& rowkey, 
        const bool not_table_first_row)
    {
      bool ret = false;
      TableRangeStruct& range
        = table_range_array_[table_range_array_flag_];

      if (not_table_first_row)
      {
        //not the first row of table; should only check the end_key_, 
        //becouse the rowkey bas been 
        //compared with the last rowkey
        //必须是前开后闭
        if (rowkey <= range.table_range_.end_key_)
        {
          ret = true;
        }
        else
        {
          ret = false;
        }
      }
      else
      {
        //the firest rowkey of the table,
        //should check both the start_key and the endkey
        if (rowkey > range.table_range_.start_key_
            && rowkey <= range.table_range_.end_key_)
        {
          ret = true;
        }
        else
        {
          ret = false;
        }
      }

      return ret;
    }

    bool ObSSTableTable::check_rowkey_range(const::ObRow& row,
        const bool not_table_first_row)
    {
      bool ret = false;
      const ObRowkey* rowkey = NULL;
      TableRangeStruct& range
        = table_range_array_[table_range_array_flag_];

      if (OB_SUCCESS != (ret = row.get_rowkey(rowkey)))
      {
        TBSYS_LOG(WARN, "row get rowkey error:ret=%d,row=%s,rowkey=%p",
            ret, to_cstring(row), rowkey);
      }
      else
      {
        if (not_table_first_row)
        {
          //not the first row of table; should only check the end_key_, 
          //becouse the rowkey bas been 
          //compared with the last rowkey
          //必须是前开后闭
          if ((*rowkey) <= range.table_range_.end_key_)
          {
            ret = true;
          }
          else
          {
            TBSYS_LOG(WARN, "cur rowkey > the end_key_ of range:"
                "*rowkey=%s,table_range_.end key=%s",
                to_cstring(*rowkey),
                to_cstring(range.table_range_.end_key_));
            ret = false;
          }
        }
        else
        {
          //the firest rowkey of the table, 
          //should check both the start_key and the endkey
          if ((*rowkey) > range.table_range_.start_key_
              && (*rowkey) <= range.table_range_.end_key_)
          {
            ret = true;
          }
          else
          {
            TBSYS_LOG(WARN, "cur rowkey > the end_key_ of range or "
                "cur rowkey < range.table_range_.start_key_:"
                "*rowkey=%s,table_range.start_key=%s"
                "table_range_.end key=%s",
                to_cstring(*rowkey),
                to_cstring(range.table_range_.start_key_),
                to_cstring(range.table_range_.end_key_));
            ret = false;
          }
        }
      }

      return ret;
    }
    
    int ObSSTableTable::set_table_range(const common::ObNewRange& range)
    {
      int ret = common::OB_SUCCESS;
      TableRangeStruct& table_range
        = table_range_array_[table_range_array_flag_];

      ObMemBufAllocatorWrapper start_key_allocator(
          table_range.start_key_buf_);
      ObMemBufAllocatorWrapper end_key_allocator(
          table_range.end_key_buf_);

      if (OB_SUCCESS != (ret = deep_copy_range(
              start_key_allocator, end_key_allocator, range,
              table_range.table_range_)))
      {
        TBSYS_LOG(WARN, "failed copy range");
      }
      else
      {
        table_range.available_ = true;
      }

      return ret;
    }

    int ObSSTableTable::set_block_index(const int64_t data_offset)
    {
      int ret = OB_SUCCESS;
      
      if (data_offset < 0)
      {
        TBSYS_LOG(WARN, "invalid argument:data_offset=%ld", data_offset);
        ret = OB_ERROR;
      }
      else
      {
        block_index_.block_data_offset_ = data_offset;
        if (OB_SUCCESS != (ret = block_index_builder_.add_item(block_index_)))
        {
          TBSYS_LOG(WARN, "add entry error:ret=%d", ret);
        }
      }
      
      return ret;
    }

    int ObSSTableTable::set_block_endkey(const ObRowkey& block_endkey)
    {
      int ret = OB_SUCCESS;
      int64_t endkey_offset = block_endkey_builder_.get_length();

      block_index_.block_endkey_offset_ = endkey_offset;
      if (OB_SUCCESS != (ret = block_endkey_builder_.add_item(
              block_endkey)))
      {
        TBSYS_LOG(WARN, "add entry error:ret=%d", ret);
      }
      else
      {
        ObMemBufAllocatorWrapper allocator(block_endkey_buf_);
        block_endkey.deep_copy(block_endkey_, allocator);
      }
      
      return ret;
    }

    int ObSSTableTable::finish_last_block(const int64_t data_offset)
    {
      int ret = OB_SUCCESS;
      int64_t index_offset = data_offset;
      int64_t endkey_offset = block_endkey_builder_.get_length();

      block_index_.block_data_offset_ = index_offset;
      block_index_.block_endkey_offset_ = endkey_offset;

      if (OB_SUCCESS != (ret = block_index_builder_.add_item(block_index_)))
      {
        TBSYS_LOG(WARN, "add entry error:ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableTable::finish_table_range(const bool is_table_finish)
    {
      int ret = OB_SUCCESS;
      TableRangeStruct& range
        = table_range_array_[table_range_array_flag_];
      ObBorderFlag border_flag;
      border_flag.set_data(range.table_range_.border_flag_.get_data());

      table_range_builder_.reset();

      if (!is_table_finish)
      {
        if (range.table_range_.border_flag_.is_max_value())
        {
          border_flag.set_inclusive_end();
          border_flag.unset_max_value();
        }
      }

      if (OB_SUCCESS != (ret = table_range_builder_.add_item_border_flag(
              border_flag)))
      {
        TBSYS_LOG(WARN, "table range builder add item border flag:"
            "ret=%d,range.border_flag_=%d", ret, border_flag.get_data());
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = table_range_builder_.add_item(
                range.table_range_.start_key_)))
        {
          char buf[1024];
          range.table_range_.start_key_.to_string(buf, 1024);
          TBSYS_LOG(WARN, "table range builder add_item error:ret=%d,"
              "range.start_key_=%s", ret, buf);
        }
        else
        {
          range.startkey_length_ = table_range_builder_.get_length()
            - sizeof(int8_t);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (is_table_finish)
        {
          if (OB_SUCCESS != (ret = table_range_builder_.add_item(
                  range.table_range_.end_key_)))
          {
            char buf[1024];
            range.table_range_.end_key_.to_string(buf, 1024);
            TBSYS_LOG(WARN, "table range builder add_item error:ret=%d,"
                "table_range_.end_key_=%s", ret, buf);
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = table_range_builder_.add_item(
                  block_endkey_)))
          {
            char buf[1024];
            block_endkey_.to_string(buf, 1024);
            TBSYS_LOG(WARN, "table range builder add_item error:ret=%d,"
                "block_endkey_=%s", ret, buf);
          }
          /*
          else
          {
            ObMemBufAllocatorWrapper start_key_allocator(range.start_key_buf_);
            if (OB_SUCCESS != (ret = block_endkey_.deep_copy(
                    range.table_range_.start_key_, start_key_allocator)))
            {
              char buf[1024];
              block_endkey_.to_string(buf, 1024);
              TBSYS_LOG(WARN, "block endkey deep copy error:ret=%d,"
                  "block_endkey_=%s", ret, buf);
            }
          }
          */
        }

        if (OB_SUCCESS == ret)
        {
          range.endkey_length_ = table_range_builder_.get_length() 
            - range.startkey_length_ - sizeof(int8_t);
        }
      }

      return ret;
    }


    int ObSSTableTable::build_block_index(char* buf, const int64_t buf_size, 
        int64_t& index_length)
    {
      int ret = OB_SUCCESS;
      
      if (OB_SUCCESS != (ret = block_index_builder_.build_block_index(
              buf, buf_size, index_length)))
      {
        TBSYS_LOG(WARN, "build block index error:ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableTable::build_block_index(const char*& buf, int64_t& index_length)
    {
      int ret = OB_SUCCESS;
      
      if (OB_SUCCESS != (ret = block_index_builder_.build_block_index(
              buf, index_length)))
      {
        TBSYS_LOG(WARN, "build block index error:ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableTable::build_block_endkey(char* buf, const int64_t buf_size, 
        int64_t& endkey_length)
    {
      int ret = OB_SUCCESS;
      
      if (OB_SUCCESS != (ret = block_endkey_builder_.build_block_endkey(
              buf, buf_size, endkey_length)))
      {
        TBSYS_LOG(WARN, "build block endkey error:ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableTable::build_block_endkey(const char*& buf, 
        int64_t& endkey_length)
    {
      int ret = OB_SUCCESS;
      
      if (OB_SUCCESS != (ret = block_endkey_builder_.build_block_endkey(
              buf, endkey_length)))
      {
        TBSYS_LOG(WARN, "build block endkey error:ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableTable::build_table_range(char* buf, const int64_t buf_size,
        int64_t& startkey_length, int64_t& endkey_length)
    {
      int ret = OB_SUCCESS;
      int64_t length = 0;
      TableRangeStruct& range
        = table_range_array_[table_range_array_flag_];

      if (OB_SUCCESS != (ret = table_range_builder_.build_table_range(
              buf, buf_size, length)))
      {
        TBSYS_LOG(WARN, "table range builder build table range error:ret=%d,"
            "buf=%p,buf_size=%ld,length=%ld", ret, buf, buf_size, length);
      }
      else
      {
        startkey_length = range.startkey_length_;
        endkey_length = range.endkey_length_;
      }

      return ret;
    }

    int ObSSTableTable::build_table_range(const char*& buf, 
        int64_t& startkey_length, int64_t& endkey_length)
    {
      int ret = OB_SUCCESS;
      int64_t length = 0;
      TableRangeStruct& range 
        = table_range_array_[table_range_array_flag_];
      
      if (OB_SUCCESS != (ret = table_range_builder_.build_table_range(
              buf, length)))
      {
        TBSYS_LOG(WARN, "table range builder build table range error:"
            "ret=%d,buf=%p,length=%ld", ret, buf, length);
      }
      else
      {
        startkey_length = range.startkey_length_;
        endkey_length = range.endkey_length_;
      }

      return ret;
    }

    int ObSSTableTable::build_table_bloomfilter(const char*& buf, 
        int64_t& bloomfilter_length)
    {
      int ret = OB_SUCCESS;
      
      buf = reinterpret_cast<const char*>(table_bloomfilter_.get_buffer());
      bloomfilter_length = table_bloomfilter_.get_nbyte();
      
      return ret;
    }

    const ObNewRange* ObSSTableTable::get_table_range(int64_t flag)
    {
      ObNewRange* ret = NULL;
      TableRangeStruct* range = NULL;

      if (0 == flag)
      {
        int64_t i = (table_range_array_flag_ + 1) % 2;
        range = &table_range_array_[i];
        if (range->available_)
        {
          ret = &range->table_range_;
        }
      }
      else if (1 == flag)
      {
        int64_t i = table_range_array_flag_;
        range = &table_range_array_[i];
        if (range->available_)
        {
          ret = &range->table_range_;
        }
      }
      else
      {
        //do nothing
      }

      return  ret;
    }
  }//end namespace compactsstablev2
}
