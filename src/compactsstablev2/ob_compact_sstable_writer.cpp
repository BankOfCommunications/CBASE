#include "ob_compact_sstable_writer.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObCompactSSTableWriter::set_sstable_param(
        const ObFrozenMinorVersionRange& version_range,
        const ObCompactStoreType& row_store_type,
        const int64_t table_count,
        const int64_t blocksize,
        const ObString& compressor_name,
        const int64_t def_sstable_size,
        const int64_t min_split_sstable_size/*=0*/)
    {
      int ret = OB_SUCCESS;

      if (sstable_inited_)
      {
        TBSYS_LOG(WARN, "sstable reinited");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (DENSE_SPARSE != row_store_type
          && DENSE_DENSE != row_store_type)
      {
        TBSYS_LOG(WARN, "invalid row_stroe_type:row_store_type=%d",
            row_store_type);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!version_range.is_legal(row_store_type))
      {
        TBSYS_LOG(WARN, "version_range is invalid:row_store_type=%d,"
            "version_range=%s",
            row_store_type, to_cstring(version_range));
        ret = OB_INVALID_ARGUMENT;
      }
      else if (table_count <= 0)
      {
        TBSYS_LOG(WARN, "table count is error:table_count=%ld",
            table_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (blocksize <= 0)
      {
        TBSYS_LOG(WARN, "block size is error:block_size=%ld", blocksize);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (def_sstable_size < 0
          || min_split_sstable_size < 0
          || (def_sstable_size != 0 && min_split_sstable_size != 0
            && min_split_sstable_size > def_sstable_size)
          || (def_sstable_size == 0 && min_split_sstable_size != 0)
          || (def_sstable_size != 0 && min_split_sstable_size == 0))
      {
        TBSYS_LOG(WARN, "split size set error:def_sstable_size=%ld,"
            "min_split_sstable_size=%ld",
            def_sstable_size, min_split_sstable_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        sstable_.set_frozen_minor_version_range(version_range);
        sstable_.set_row_store_type(row_store_type);
        sstable_.set_blocksize(blocksize);
        block_.set_row_store_type(row_store_type);
        sstable_table_init_count_ = table_count;

        //split
        if (DENSE_SPARSE == row_store_type)
        {
          if (0 != def_sstable_size || 0 != min_split_sstable_size)
          {
            TBSYS_LOG(WARN, "do not support the split of multible table:"
                "table_count=%ld,def_sstable_size=%ld,"
                "min_split_sstable_size=%ld",
                table_count, def_sstable_size, min_split_sstable_size);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            def_sstable_size_ = 0;
            min_split_sstable_size_ = 0;
            split_flag_ = false;
          }
        }
        else if (DENSE_DENSE == row_store_type)
        {
          if (1 != table_count)
          {
            TBSYS_LOG(ERROR, "DENSE_DENSE do not support the mult table");
            ret = OB_ERROR;
          }
          else if (0 == def_sstable_size)
          {
            def_sstable_size_ = 0;
            min_split_sstable_size_ = 0;
            split_flag_ = false;
          }
          else
          {
            def_sstable_size_ = def_sstable_size;
            min_split_sstable_size_ = min_split_sstable_size;
            split_flag_ = true;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "invalid store type:row_store_type=%d",
              row_store_type);
          ret = OB_ERROR;
        }

        //compress
        if (OB_SUCCESS == ret)
        {
          if (NULL == compressor_name.ptr())
          {
            compressor_ = NULL;
            sstable_.set_compressor_name(compressor_name);
          }
          else if (NULL == compressor_)
          {
            sstable_.set_compressor_name(compressor_name);
            if (NULL == (compressor_ = create_compressor(
                    compressor_name.ptr())))
            {
              TBSYS_LOG(ERROR, "create commpressor error:"
                  "compressor name=%s", compressor_name.ptr());
              ret = OB_ERROR;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "ObCompactSSTableWriter set sstable param"
            "success");
        sstable_inited_ = true;
      }

      return ret;
    }

    int ObCompactSSTableWriter::set_table_info(const uint64_t table_id,
        const ObSSTableSchema& schema, const ObNewRange& table_range)
    {
      int ret = OB_SUCCESS;
      bool is_sstable_split = false;

      if (!sstable_inited_)
      {
        TBSYS_LOG(WARN, "sstable inited donot inited:sstable_inited_=%d",
            sstable_inited_);
        ret = OB_NOT_INIT;
      }
      else if (table_inited_)
      {//switch table
        if (split_flag_)
        {
          TBSYS_LOG(WARN, "don not support the split of mult"
              "table:split_flag_=%d", table_inited_);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = finish_current_block(
                is_sstable_split)))
        {
          TBSYS_LOG(WARN, "finish the current block error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = table_.add_table_bloomfilter(
                sstable_writer_buffer_.get_list_bloomfilter())))
        {
          TBSYS_LOG(WARN, "add table bloomfilter error:ret=%d", ret);
        }
        else
        {
          sstable_writer_buffer_.reset_list_bloomfilter();
        }

        if (OB_SUCCESS == ret)
        {
          sstable_.add_row_count(
              sstable_writer_buffer_.get_list_row_count());
          sstable_writer_buffer_.reset_list_row_count();

          if (OB_SUCCESS != (ret = finish_current_table(true)))
          {
            TBSYS_LOG(WARN, "finish current table error:ret=%d", ret);
          }
          else
          {//switch table
            sstable_.switch_table_reset();
            sstable_.set_table_id(table_id);
            sstable_.set_table_schema(schema);
            table_.set_table_range(table_range);
            table_inited_ = true;
            cur_table_offset_ = cur_offset_;
          }
        }
      }
      else
      {//table not init
        if (!sstable_first_table_)
        {
          sstable_first_table_ = true;
          sstable_.set_first_table_id(table_id);
        }
        sstable_.set_table_id(table_id);
        sstable_.set_table_schema(schema);
        table_.set_table_range(table_range);
        table_inited_ = true;
        sstable_first_table_ = true;
      }

      return ret;
    }

    int ObCompactSSTableWriter::set_sstable_filepath(
        const ObString& filepath)
    {
      int ret = OB_SUCCESS;

      if (!sstable_inited_)
      {
        TBSYS_LOG(WARN, "sstable has not been inited");
        ret = OB_NOT_INIT;
      }
      else if(!table_inited_)
      {
        TBSYS_LOG(WARN, "table has not been inited");
        ret = OB_NOT_INIT;
      }
      else if (sstable_file_inited_)
      {
        TBSYS_LOG(WARN, "sstable file inited twice");
        ret = OB_INIT_TWICE;
      }
      else if (0 == filepath.length())
      {
        TBSYS_LOG(ERROR, "filepath is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = filesys_->create(filepath, dio_flag_)))
      {
        TBSYS_LOG(ERROR, "create file appender error:ret=%d,filepath=%s,"
            "dio_flag_=%d", ret, to_cstring(filepath), dio_flag_);
      }
      else if (OB_SUCCESS != (ret = flush_mem_block_list()))
      {
        TBSYS_LOG(WARN, "flush mem block list error:ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "set sstable file path success");
        sstable_file_inited_ = true;
      }

      //if faild, close the file
      if (OB_SUCCESS != ret)
      {
        filesys_->close();
      }

      return ret;
    }

    int ObCompactSSTableWriter::append_row(const ObRowkey& row_key,
        const ObRow& row_value, bool& is_sstable_split)
    {
      int ret = OB_SUCCESS;
      is_sstable_split = false;

      if (!sstable_inited_)
      {
        TBSYS_LOG(WARN, "sstable has not been inited");
        ret = OB_NOT_INIT;
      }
      else if (!table_inited_)
      {
        TBSYS_LOG(WARN, "table has not been inited");
        ret = OB_NOT_INIT;
      }
      else if (!sstable_file_inited_)
      {
        TBSYS_LOG(WARN, "sstable file has not been inited");
        ret = OB_NOT_INIT;
      }

      //use the schema to check the row
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = sstable_.check_row(row_key, row_value)))
        {
          TBSYS_LOG(WARN, "check row error:row_key=%s,row_value=%s",
              to_cstring(row_key), to_cstring(row_value));
        }
      }

      //check row_rowkey
      if (OB_SUCCESS == ret)
      {//check the rowkey in the table range
        if (true == table_.check_rowkey_range(row_key, not_table_first_row_))
        {
          ret = OB_SUCCESS;
        }
        else
        {//todo:the output of error meesage
          TBSYS_LOG(WARN, "check rowkey range error:row_key=%s,"
              "not_table_first_row_=%d", to_cstring(row_key),
              not_table_first_row_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (not_table_first_row_)
        {
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.check_rowkey(
                  row_key)))
          {
            TBSYS_LOG(WARN, "sstable writer buffer check row error:"
                "ret=%d,rowkey=%s", ret, to_cstring(row_key));
          }
        }
      }

      //switch block or not
      if (OB_SUCCESS == ret)
      {
        if (need_switch_block())
        {
          if(OB_SUCCESS != (ret = finish_current_block(is_sstable_split)))
          {
            TBSYS_LOG(WARN, "finish current block error:ret=%d", ret);
          }
        }
      }

      //add row
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = block_.add_row(row_key, row_value)))
        {//add the row
          TBSYS_LOG(WARN, "add row error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = sstable_writer_buffer_.set_cur_key(row_key)))
        {//cache the current key
          TBSYS_LOG(WARN, "soft copy the cur_key error:ret=%d", ret);
        }
        else
        {
          if (!not_table_first_row_)
          {
            not_table_first_row_ = true;
          }

          //add list row count
          sstable_writer_buffer_.inc_list_row_count();

          //update bloom filter
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.update_list_bloomfilter(sstable_.get_table_id(), row_key)))
          {
            TBSYS_LOG(WARN, "update list bloom filter error:ret=%d", ret);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::append_row(const ObRow& row,
        bool& is_sstable_split)
    {
      int ret = OB_SUCCESS;
      is_sstable_split = false;

      if (!sstable_inited_)
      {
        TBSYS_LOG(WARN, "sstable has not been inited");
        ret = OB_NOT_INIT;
      }
      else if (!table_inited_)
      {
        TBSYS_LOG(WARN, "table has not been inited");
        ret = OB_NOT_INIT;
      }
      else if (!sstable_file_inited_)
      {
        TBSYS_LOG(WARN, "sstable file has not been inited");
        ret = OB_NOT_INIT;
      }

#ifdef OB_COMPACT_SSTABLE_ALLOW_SCHEMA_CHECK_ROWKEY_
      //use the schema to check the row
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = sstable_.check_row(row)))
        {
          TBSYS_LOG(WARN, "sstable check row error:ret=%d,row=%s",
              ret, to_cstring(row));
        }
      }
#endif

#ifdef OB_COMPACT_SSTABLE_ALLOW_TABLE_RANGE_CHECK_ROWKEY_
      //use the table range to check row_rowkey
      if (OB_SUCCESS == ret)
      {
        if (true == table_.check_rowkey_range(row, not_table_first_row_))
        {
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "check rowkey range error:row=%s,"
              "not_table_first_row=%d",
              to_cstring(row), not_table_first_row_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
#endif

#ifdef OB_COMPACT_SSTABLE_ALLOW_LAST_CHECK_ROWKEY_
      //is larger than before rowkey
      if (OB_SUCCESS == ret)
      {
        if (not_table_first_row_)
        {
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.check_rowkey(row)))
          {
            TBSYS_LOG(WARN, "sstable writer buffer check row error:"
                "ret=%d,row=%s", ret, to_cstring(row));
          }
        }
      }
#endif

      //switch block or not
      if (OB_SUCCESS == ret)
      {
        if (need_switch_block())
        {
          if(OB_SUCCESS != (ret = finish_current_block(is_sstable_split)))
          {
            TBSYS_LOG(WARN, "finish current block error:ret=%d", ret);
          }
        }
      }

      //add row
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = block_.add_row(row)))
        {//add the row
          TBSYS_LOG(WARN, "add row error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = sstable_writer_buffer_.set_cur_key(row)))
        {//cache the current key
          TBSYS_LOG(WARN, "soft copy the cur_key error:ret=%d", ret);
        }
        else
        {
          if (!not_table_first_row_)
          {
            not_table_first_row_ = true;
          }

          //add list row count
          sstable_writer_buffer_.inc_list_row_count();

#ifdef OB_COMPACT_SSTABLE_ALLOW_BLOOMFILTER_
          //update bloom filter
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.update_list_bloomfilter(sstable_.get_table_id(), row)))
          {
            TBSYS_LOG(WARN, "update list bloom filter error:ret=%d", ret);
          }
#endif
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish()
    {
      int ret = OB_SUCCESS;
      bool is_sstable_split = false;

      if (OB_SUCCESS != (ret = finish_current_block(is_sstable_split)))
      {
        TBSYS_LOG(WARN, "finish current block error:ret=%d", ret);
      }
      else if (!sstable_writer_buffer_.mem_block_list_empty())
      {//mem list is not empty
        if (OB_SUCCESS != (ret = flush_mem_block_list()))
        {
          TBSYS_LOG(WARN, "flush mem block list error:ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = table_.add_table_bloomfilter(
                sstable_writer_buffer_.get_list_bloomfilter())))
        {
          TBSYS_LOG(WARN, "add table bloomfilter error:ret=%d", ret);
        }
        else
        {
          sstable_writer_buffer_.reset_list_bloomfilter();
        }
      }

      if (OB_SUCCESS == ret)
      {
        sstable_.add_row_count(
            sstable_writer_buffer_.get_list_row_count());
        sstable_writer_buffer_.reset_list_row_count();
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = finish_current_table(true)))
        {
          TBSYS_LOG(WARN, "finish current talbe errot:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = finish_current_sstable()))
        {
          TBSYS_LOG(WARN, "finish_current_sstable error:ret=%d", ret);
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::get_table_range(
        const ObNewRange*& table_range, const int64_t flag)
    {
      int ret = OB_SUCCESS;

      if (NULL == (table_range = table_.get_table_range(flag)))
      {
        TBSYS_LOG(WARN, "get table range error:flag=%ld,"
            "table_range=%p", flag, table_range);
        ret = OB_ERROR;
      }
      else
      {
        //do nothing
      }

      return ret;
    }

    void ObCompactSSTableWriter::reset()
    {
      sstable_inited_ = false;
      table_inited_ = false;
      sstable_file_inited_ = false;
      sstable_first_table_ = false;
      not_table_first_row_ = false;

      if (NULL != compressor_)
      {
        destroy_compressor(compressor_);
        compressor_ = NULL;
      }
      dio_flag_ = true;
      //default_filesys_(construct)
      filesys_->close();
      filesys_ = &default_filesys_;

      split_buf_inited_ = false;
      split_flag_ = false;
      def_sstable_size_ = 0;
      min_split_sstable_size_ = 0;

      cur_offset_ = 0;
      cur_table_offset_ = 0;
      sstable_checksum_ = 0;
      sstable_table_init_count_ = 0;

      sstable_writer_buffer_.reset();

      sstable_.reset();
      sstable_.set_bloomfilter_hashcount(SSTABLE_BLOOMFILTER_HASH_COUNT);
      table_.reset();
      table_.init(SSTABLE_BLOOMFILTER_HASH_COUNT,
          SSTABLE_BLOOMFILTER_SIZE);
      block_.reset();

      sstable_trailer_offset_.reset();
      query_struct_.reset();
    }

    void ObCompactSSTableWriter::switch_sstable_reset()
    {
      filesys_->close();
      sstable_file_inited_ = false;
      cur_offset_ = 0;
      cur_table_offset_ = 0;

      sstable_checksum_ = 0;
      //table_.switch_sstable_reset();
      sstable_.switch_sstable_reset();
    }

    int ObCompactSSTableWriter::flush_mem_block_list()
    {
      int ret = OB_SUCCESS;
      const ObList<BlockListNode*>& mem_block_list = sstable_writer_buffer_.get_mem_block_list();
      ObList<BlockListNode*>::const_iterator start_iter = mem_block_list.begin();
      ObList<BlockListNode*>::const_iterator end_iter = mem_block_list.end();
      ObList<BlockListNode*>::const_iterator cur_iter = start_iter;
      int64_t prev_offset = 0;

      while (cur_iter != end_iter)
      {
        prev_offset = cur_offset_;
        sstable_checksum_ = ob_crc64(sstable_checksum_,
            &(*cur_iter)->record_header_, sizeof(ObRecordHeaderV2));
        if (OB_SUCCESS != (ret = write_record_header((*cur_iter)->record_header_)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = write_record_body(
                (*cur_iter)->block_buf_->get_buffer(),
                (*cur_iter)->data_len_)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
          break;
        }
        else
        {
          table_.set_block_endkey((*cur_iter)->block_endkey_);
          table_.set_block_index(static_cast<int32_t>(prev_offset - cur_table_offset_));
          sstable_.inc_block_count();
        }
        cur_iter ++;
      }

      if (OB_SUCCESS == ret)
      {
        table_.add_table_bloomfilter(sstable_writer_buffer_.get_list_bloomfilter());
        sstable_writer_buffer_.reset_list_bloomfilter();
      }

      if (OB_SUCCESS == ret)
      {
        sstable_.add_row_count(sstable_writer_buffer_.get_list_row_count());
        sstable_writer_buffer_.reset_list_row_count();
      }

      if (OB_SUCCESS == ret)
      {
        sstable_writer_buffer_.clear_mem_block_list();
        split_buf_inited_ = false;
      }
      return ret;
    }

    int ObCompactSSTableWriter::finish_current_block(bool& is_sstable_split)
    {
      int ret = OB_SUCCESS;
      is_sstable_split = false;

      char* buf_ptr = NULL;
      int64_t data_len = 0;

      if (0 != block_.get_row_count())
      {
        if (OB_SUCCESS != (ret = block_.build_block(buf_ptr, data_len)))
        {
          TBSYS_LOG(WARN, "build block error:ret=%d", ret);
        }
        else
        {
          sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(
                  compressor_)))
          {//compress
            TBSYS_LOG(WARN, "compress error:ret=%d", ret);
          }
          else
          {//select data
            sstable_writer_buffer_.select_buf();
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (split_flag_)
          {//allow split
            if (OB_SUCCESS != (ret = finish_current_block_split(
                    is_sstable_split)))
            {
              TBSYS_LOG(WARN, "finish current block split error:ret=%d", ret);
            }
          }
          else
          {//don not allow split
            if (OB_SUCCESS != (ret = finish_current_block_nosplit()))
            {
              TBSYS_LOG(WARN, "finish current block nosplit error:"
                  "ret=%d", ret);
            }
          }
        }
      }

      //reset block_
      block_.reset();

      return ret;
    }



    int ObCompactSSTableWriter::finish_current_block_split(
        bool& is_sstable_split)
    {
      int ret = OB_SUCCESS;
      is_sstable_split = false;
      int64_t prev_offset = 0;

      sstable_writer_buffer_.update_record_header(
          OB_SSTABLE_BLOCK_DATA_MAGIC);

      if (split_buf_inited_)
      {
        if (OB_SUCCESS != (ret = sstable_writer_buffer_.cur_node_prepare()))
        {
          TBSYS_LOG(WARN, "cur node prepare error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = sstable_writer_buffer_.build_cur_node()))
        {
          TBSYS_LOG(WARN, "build current node error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = sstable_writer_buffer_.push_cur_node()))
        {
          TBSYS_LOG(WARN, "push cur node error:ret=%d", ret);
        }
        else if (true == sstable_writer_buffer_.check_reach_min_split_sstable_size(min_split_sstable_size_))
        {//reach the condition of split
          if (OB_SUCCESS != (ret = finish_current_table(false)))
          {
            TBSYS_LOG(WARN, "finish current table error:ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = finish_current_sstable()))
          {
            TBSYS_LOG(WARN, "finish current sstable error:ret=%d", ret);
          }
          else
          {
            is_sstable_split = true;
          }
        }
        else
        {
        }
      }//split_buf_inited_ == false
      else
      {//mem_block_list_ has not been started
        prev_offset = cur_offset_;

        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        sstable_checksum_ = ob_crc64(sstable_checksum_,
            &record_header, sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          //update the block_endkey_ , the block_index_ , block_count_
          table_.set_block_endkey(sstable_writer_buffer_.get_cur_key());
          table_.set_block_index(prev_offset - cur_table_offset_);
          sstable_.inc_block_count();

          if (OB_SUCCESS == ret)
          {
            if (cur_offset_ >= def_sstable_size_)
            {//the size of file reach the top
              sstable_.add_row_count(sstable_writer_buffer_.get_list_row_count());
              sstable_writer_buffer_.reset_list_row_count();
              split_buf_inited_ = true;

              if (OB_SUCCESS != (ret = table_.add_table_bloomfilter(
                      sstable_writer_buffer_.get_list_bloomfilter())))
              {
                TBSYS_LOG(WARN, "add table bloomfilter error:ret=%d", ret);
              }
              else
              {
                sstable_writer_buffer_.reset_list_bloomfilter();
              }
            }
          }
        }
      }//end split_buf_inited==false

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_block_nosplit()
    {
      int ret = OB_SUCCESS;
      int64_t prev_offset = 0;

      prev_offset = cur_offset_;

      sstable_writer_buffer_.update_record_header(
          OB_SSTABLE_BLOCK_DATA_MAGIC);
      const ObRecordHeaderV2& record_header =
        sstable_writer_buffer_.get_record_header();
      sstable_checksum_ = ob_crc64(sstable_checksum_,
          &record_header, sizeof(ObRecordHeaderV2));

      const char* buf = NULL;
      int64_t len = 0;

      sstable_writer_buffer_.get_output_buf(buf, len);

      if (OB_SUCCESS != (ret = write_record_header(record_header)))
      {
        TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
      {
        TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
      }
      else
      {
        table_.set_block_endkey(sstable_writer_buffer_.get_cur_key());
        table_.set_block_index(prev_offset - cur_table_offset_);
        sstable_.inc_block_count();
      }

      return ret;
    }

    bool ObCompactSSTableWriter::need_switch_block()
    {
      bool ret;
      if (block_.get_block_size() >=  sstable_.get_block_size())
      {
        ret = true;
      }
      else
      {
        ret = false;
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_table(const bool finish_flag)
    {
      int ret = OB_SUCCESS;


      sstable_.set_block_data_record(cur_table_offset_,
          cur_offset_ - cur_table_offset_);
      if (OB_SUCCESS != (ret = finish_current_block_index()))
      {
        TBSYS_LOG(WARN, "finish current block index error:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_block_endkey()))
      {
        TBSYS_LOG(WARN, "finish current block endkey error:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_table_range(finish_flag)))
      {
        TBSYS_LOG(WARN, "finish current table range error:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_table_bloomfilter()))
      {
        TBSYS_LOG(WARN, "finish current table bloomfilter error:ret=%d", ret);
      }
      else
      {
        sstable_.finish_one_table();
        //sstable_.set_block_data_record(cur_table_offset_,
        //    cur_offset_ - cur_table_offset_);
        //sstable_.set_block_data_record(cur_table_offset_,
        //    cur_offset_ - cur_table_offset_);

        if (finish_flag)
        {
          table_.switch_table_reset();
          table_inited_ = false;
          not_table_first_row_ = false;
        }
        else
        {
          table_.switch_sstable_reset();
        }
      }

      return ret;
    }


    int ObCompactSSTableWriter::finish_current_block_index()
    {//ok
      int ret = OB_SUCCESS;
      int64_t index_len = 0;
      int64_t prev_offset = 0;

      if (cur_offset_ == cur_table_offset_
          && 0 == table_.get_block_index_length())
      {
      }
      else
      {
        if (OB_SUCCESS != (ret = table_.finish_last_block(
                cur_offset_ - cur_table_offset_)))
        {//最后一个block index
          TBSYS_LOG(WARN, "finish last block error:ret=%d", ret);
        }
        else
        {//block index的总大小
          index_len = table_.get_block_index_length();
        }

        if (OB_SUCCESS == ret)
        {
          if (table_.is_alloc_block_index_buf())
          {//需要分配空间
            char* buf_ptr = NULL;
            int64_t buf_size = 0;
            int64_t data_len = 0;

            if (OB_SUCCESS != (ret = sstable_writer_buffer_.ensure_uncomp_buf(
                    index_len)))
            {//分配空间
              TBSYS_LOG(WARN, "ensure space error:ret=%d", ret);
            }
            else
            {
              buf_ptr = sstable_writer_buffer_.get_uncomp_buf_ptr();
              buf_size = sstable_writer_buffer_.get_uncomp_buf_size();
            }

            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = table_.build_block_index(
                      buf_ptr, buf_size, data_len)))
              {//build block index
                TBSYS_LOG(WARN, "build block index error:ret=%d", ret);
              }
              else
              {//设置block index len
                sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
              }
            }
          }
          else
          {//不需要分配空间
            const char* buf_ptr = NULL;
            int64_t data_len = 0;

            if (OB_SUCCESS != (ret = table_.build_block_index(
                    buf_ptr, data_len)))
            {
              TBSYS_LOG(WARN, "build block index error:ret=%d", ret);
            }
            else
            {
              sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
          {//compress
            TBSYS_LOG(WARN, "compress error:ret=%d", ret);
          }
          else
          {
            sstable_writer_buffer_.select_buf();
          }
        }

        if (OB_SUCCESS == ret)
        {
          prev_offset = cur_offset_;

          sstable_writer_buffer_.update_record_header(
              OB_SSTABLE_BLOCK_INDEX_MAGIC);
          const ObRecordHeaderV2& record_header =
            sstable_writer_buffer_.get_record_header();
          sstable_checksum_ = ob_crc64(sstable_checksum_,
              &record_header, sizeof(ObRecordHeaderV2));

          const char* buf = NULL;
          int64_t len = 0;
          sstable_writer_buffer_.get_output_buf(buf, len);

          if (OB_SUCCESS != (ret = write_record_header(record_header)))
          {
            TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
          {
            TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
          }
          else
          {
            sstable_.set_block_index_record(prev_offset,
                cur_offset_ - prev_offset);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_block_endkey()
    {
      int ret = OB_SUCCESS;
      int64_t endkey_len = 0;
      int64_t prev_offset = 0;

      endkey_len = table_.get_block_endkey_length();

      if (0 != endkey_len)
      {
        if (table_.is_alloc_block_endkey_buf())
        {
          char* buf_ptr = NULL;
          int64_t buf_size = 0;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret = sstable_writer_buffer_.ensure_uncomp_buf(endkey_len)))
          {
            TBSYS_LOG(WARN, "sstable writer buffer ensure uncomp buf error:"
                "ret=%d", ret);
          }
          else
          {
            buf_ptr = sstable_writer_buffer_.get_uncomp_buf_ptr();
            buf_size = sstable_writer_buffer_.get_uncomp_buf_size();
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = table_.build_block_endkey(
                    buf_ptr, buf_size, data_len)))
            {
              TBSYS_LOG(WARN, "build block endkey error:ret=%d", ret);
            }

          }
          else
          {
            sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
          }
        }
        else
        {
          const char* buf_ptr = NULL;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret = table_.build_block_endkey(
                  buf_ptr, data_len)))
          {
            TBSYS_LOG(WARN, "block block endkey error:ret=%d", ret);
          }
          else
          {
            sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(
                  NULL)))
          {//compress
            TBSYS_LOG(WARN, "compress error:ret=%d", ret);
          }
          else
          {
            sstable_writer_buffer_.select_buf();
          }
        }

        if (OB_SUCCESS == ret)
        {
          prev_offset = cur_offset_;
          sstable_writer_buffer_.update_record_header(
              OB_SSTABLE_BLOCK_ENDKEY_MAGIC);
          const ObRecordHeaderV2& record_header =
            sstable_writer_buffer_.get_record_header();
          sstable_checksum_ = ob_crc64(sstable_checksum_, &record_header,
              sizeof(ObRecordHeaderV2));

          const char* buf = NULL;
          int64_t len = 0;
          sstable_writer_buffer_.get_output_buf(buf, len);

          if (OB_SUCCESS != (ret = write_record_header(record_header)))
          {
            TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
          {
            TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
          }
          else
          {
            sstable_.set_block_endkey_record(prev_offset,
                cur_offset_ - prev_offset);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_table_range(
        const bool finish_flag)
    {
      int ret = OB_SUCCESS;
      int64_t range_len = 0;
      int64_t startkey_len = 0;
      int64_t endkey_len = 0;
      int64_t prev_offset = 0;

      if (OB_SUCCESS != (ret = table_.finish_table_range(finish_flag)))
      {
        TBSYS_LOG(WARN, "table finish table range error:ret=%d", ret);
      }
      else
      {
        range_len = table_.get_table_range_length();
      }

      if (OB_SUCCESS == ret)
      {
        if (table_.is_alloc_table_range_buf())
        {
          char* buf_ptr = NULL;
          int64_t buf_size = 0;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret
                = sstable_writer_buffer_.ensure_uncomp_buf(range_len)))
          {
            TBSYS_LOG(WARN, "ensure uncomp buf error:ret=%d", ret);
          }
          else
          {
            buf_ptr = sstable_writer_buffer_.get_uncomp_buf_ptr();
            buf_size = sstable_writer_buffer_.get_uncomp_buf_size();
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = table_.build_table_range(
                    buf_ptr, buf_size, startkey_len, endkey_len)))
            {
              TBSYS_LOG(WARN, "build table range error:ret=%d", ret);
            }
            else
            {
              data_len = startkey_len + endkey_len;
              sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
            }
          }
        }
        else
        {
          const char* buf_ptr = NULL;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret = table_.build_table_range(
                  buf_ptr, startkey_len, endkey_len)))
          {
            TBSYS_LOG(WARN, "build table range error:ret=%d", ret);
          }
          else
          {
            data_len = sizeof(int8_t) + startkey_len + endkey_len;
            sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
      {//compress
        TBSYS_LOG(WARN, "compress error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.select_buf();
      }

      if (OB_SUCCESS == ret)
      {
        prev_offset = cur_offset_;

        sstable_writer_buffer_.update_record_header(
            OB_SSTABLE_TABLE_RANGE_MAGIC);
        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        sstable_checksum_ = ob_crc64(sstable_checksum_, &record_header,
            sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          sstable_.set_table_range_record(prev_offset,
              startkey_len, endkey_len);
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_table_bloomfilter()
    {
      int ret = OB_SUCCESS;
      int64_t prev_offset;

      const char* buf_ptr = NULL;
      int64_t data_len = 0;

      if (OB_SUCCESS != (ret = table_.build_table_bloomfilter(
              buf_ptr, data_len)))
      {
        TBSYS_LOG(WARN, "build table range error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
      {//compress
        TBSYS_LOG(WARN, "compress error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.select_buf();
      }

      if (OB_SUCCESS == ret)
      {
        prev_offset = cur_offset_;
        sstable_writer_buffer_.update_record_header(
            OB_SSTABLE_TABLE_BLOOMFILTER_MAGIC);
        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        sstable_checksum_ = ob_crc64(sstable_checksum_, &record_header,
            sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          sstable_.set_bloomfilter_record(prev_offset,
              static_cast<int32_t>(cur_offset_ - prev_offset));
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_sstable()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = finish_current_table_index()))
      {
        TBSYS_LOG(WARN, "finish current table index error:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_table_schema()))
      {
        TBSYS_LOG(WARN, "finish current table schema:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_sstable_header()))
      {
        TBSYS_LOG(WARN, "finish current sstable header:ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_current_sstable_trailer_offset()))
      {
        TBSYS_LOG(WARN, "finish current sstable trailer offset:ret=%d", ret);
      }
      else
      {
        switch_sstable_reset();
        TBSYS_LOG(DEBUG, "finish_current_sstable:ret=%d", ret);
      }
      return ret;
    }

    int ObCompactSSTableWriter::finish_current_table_index()
    {
      int ret = OB_SUCCESS;
      int64_t index_len = 0;
      int64_t prev_offset = 0;

      index_len = sstable_.get_table_index_length();

      if (OB_SUCCESS == ret)
      {
        if (sstable_.is_alloc_table_index_buf())
        {
          char* buf_ptr = NULL;
          int64_t buf_size = 0;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret
                = sstable_writer_buffer_.ensure_uncomp_buf(index_len)))
          {
            TBSYS_LOG(WARN, "sstable writer buffer ensure uncomp buf"
                "error:ret=%d", ret);
          }
          else
          {
            buf_ptr = sstable_writer_buffer_.get_uncomp_buf_ptr();
            buf_size = sstable_writer_buffer_.get_uncomp_buf_size();
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = sstable_.build_table_index(
                    buf_ptr, buf_size, data_len)))
            {
              TBSYS_LOG(WARN, "build table index error:ret=%d", ret);
            }
            else
            {
              sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
            }
          }
        }
        else
        {
          const char* buf_ptr = NULL;
          int64_t data_len = 0;
          if (OB_SUCCESS != (ret = sstable_.build_table_index(
                  buf_ptr, data_len)))
          {
            TBSYS_LOG(WARN, "sstable build table index error:ret=%d", ret);
          }
          else
          {
            sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
      {//compress
        TBSYS_LOG(WARN, "compress error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.select_buf();
      }

      if (OB_SUCCESS == ret)
      {
        prev_offset = cur_offset_;
        sstable_writer_buffer_.update_record_header(
            OB_SSTABLE_TABLE_INDEX_MAGIC);
        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        sstable_checksum_ = ob_crc64(sstable_checksum_,
            &record_header, sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          sstable_.set_table_index_record(prev_offset,
             static_cast<int64_t>(sizeof(ObSSTableTableIndex)));
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_table_schema()
    {
      int ret = OB_SUCCESS;
      int64_t schema_len = 0;
      int64_t prev_offset = 0;

      schema_len = sstable_.get_table_schema_length();

      if (OB_SUCCESS == ret)
      {
        if (sstable_.is_alloc_table_schema_buf())
        {
          char* buf_ptr = NULL;
          int64_t buf_size = 0;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret = sstable_writer_buffer_.ensure_uncomp_buf(schema_len)))
          {
            TBSYS_LOG(WARN, "ensure_uncomp_buf error:ret=%d", ret);
          }
          else
          {
            buf_ptr = sstable_writer_buffer_.get_uncomp_buf_ptr();
            buf_size = sstable_writer_buffer_.get_uncomp_buf_size();
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = sstable_.build_table_schema(
                    buf_ptr, buf_size, data_len)))
            {
              TBSYS_LOG(WARN, "build table schema error:ret=%d", ret);
            }
            else
            {
              sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
            }
          }
        }
        else
        {
          const char* buf_ptr = NULL;
          int64_t data_len = 0;

          if (OB_SUCCESS != (ret = sstable_.build_table_schema(buf_ptr, data_len)))
          {
            TBSYS_LOG(WARN, "build table schema error:ret=%d", ret);
          }
          else
          {
            sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
      {//compress
        TBSYS_LOG(WARN, "compress error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.select_buf();
      }

      if (OB_SUCCESS == ret)
      {
        prev_offset = cur_offset_;
        sstable_writer_buffer_.update_record_header(
            OB_SSTABLE_SCHEMA_MAGIC);
        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        sstable_checksum_ = ob_crc64(sstable_checksum_,
            &record_header, sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          sstable_.set_table_schema_record(prev_offset,
              static_cast<int64_t>(sizeof(ObSSTableTableSchemaItem)));
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_sstable_header()
    {
      int ret = OB_SUCCESS;
      int64_t prev_offset;

      const char* buf_ptr = NULL;
      int64_t data_len = 0;
      sstable_.set_checksum(sstable_checksum_);

      if (OB_SUCCESS != (ret = sstable_.build_sstable_header(
              buf_ptr, data_len)))
      {
        TBSYS_LOG(WARN, "build table range error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.set_uncomp_buf(buf_ptr, data_len);
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = sstable_writer_buffer_.compress(NULL)))
      {//compress
        TBSYS_LOG(WARN, "compress error:ret=%d", ret);
      }
      else
      {
        sstable_writer_buffer_.select_buf();
      }

      if (OB_SUCCESS == ret)
      {
        prev_offset = cur_offset_;
        sstable_writer_buffer_.update_record_header(
            OB_SSTABLE_HEADER_MAGIC);
        const ObRecordHeaderV2& record_header =
          sstable_writer_buffer_.get_record_header();
        //sstable_checksum_ = ob_crc64(sstable_checksum_, &record_header,
        //    sizeof(ObRecordHeaderV2));

        const char* buf = NULL;
        int64_t len = 0;
        sstable_writer_buffer_.get_output_buf(buf, len);

        if (OB_SUCCESS != (ret = write_record_header(record_header)))
        {
          TBSYS_LOG(WARN, "write record header error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = write_record_body(buf, len)))
        {
          TBSYS_LOG(WARN, "write record body error:ret=%d", ret);
        }
        else
        {
          sstable_trailer_offset_.offset_ = prev_offset;
        }
      }

      return ret;
    }

    int ObCompactSSTableWriter::finish_current_sstable_trailer_offset()
    {
      int ret = OB_SUCCESS;
      char* buf = reinterpret_cast<char*>(&sstable_trailer_offset_);
      int64_t buf_len = static_cast<int64_t>(sizeof(ObSSTableTrailerOffset));
      if (OB_SUCCESS != (ret = write_record_body(buf, buf_len)))
      {
        TBSYS_LOG(WARN, "writer record body error:ret=%d", ret);
      }
      else
      {
        query_struct_.sstable_size_ = cur_offset_;
      }

      return ret;
    }

    int ObCompactSSTableWriter::write_record_header(
        const ObRecordHeaderV2& record_header)
    {
      int ret = OB_SUCCESS;

      if (NULL == filesys_)
      {
        TBSYS_LOG(WARN, "filesys == NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = filesys_->append(&record_header,
              sizeof(record_header), false)))
      {
        TBSYS_LOG(WARN, "append to file error:ret=%d", ret);
      }
      else
      {
        cur_offset_ += sizeof(ObRecordHeaderV2);
      }

      return ret;
    }

    int ObCompactSSTableWriter::write_record_body(const char* block_buf,
        const int64_t block_buf_len)
    {
      int ret = OB_SUCCESS;

      if (NULL == filesys_)
      {
        TBSYS_LOG(WARN, "filesys == NULL");
        ret = OB_ERROR;
      }
      else if(block_buf == NULL || block_buf_len < 0)
      {
        TBSYS_LOG(WARN, "block buf == NULL or block buf len <= 0:block_buf_len=%ld", block_buf_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 == block_buf_len)
      {
        //do nothing
      }
      else if (OB_SUCCESS != (ret = filesys_->append(block_buf,
              block_buf_len, false)))
      {
        TBSYS_LOG(WARN, "file append error:ret=%d", ret);
      }
      else
      {
        cur_offset_ += block_buf_len;
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase


