#include "ob_compact_sstable_writer_buffer.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    void ObCompactSSTableWriterBuffer::reset()
    {   
      //cur_key_
      cur_rowkey_.assign(NULL, 0);

      //buffer
      output_buf_flag_ = false;
      
      if (NULL != uncomp_buf_)
      {
        uncomp_buf_->~ObMemBuf();
        ob_free(uncomp_buf_);
        uncomp_buf_ = NULL;
      }
      uncomp_buf_ptr_ = NULL;
      uncomp_buf_data_len_ = 0;

      if (NULL != comp_buf_)
      {
        comp_buf_->~ObMemBuf();
        ob_free(comp_buf_);
        comp_buf_ = NULL;
      }
      comp_buf_ptr_ = NULL;
      comp_buf_data_len_ = 0;

      record_header_.reset();

      //cur_node_
      if (NULL != cur_node_)
      {
        cur_node_->clear();
        cur_node_ = NULL;
      }

      if (!mem_block_list_empty())
      {
        clear_mem_block_list();
      }

      list_bloomfilter_.init(SSTABLE_BLOOMFILTER_HASH_COUNT, 
          SSTABLE_BLOOMFILTER_SIZE);
      mem_block_list_total_len_  = 0;
      list_row_count_ = 0;
    }

    void ObCompactSSTableWriterBuffer::clear()
    {
      if (NULL != uncomp_buf_)
      {
        uncomp_buf_->~ObMemBuf();
        ob_free(uncomp_buf_);
        uncomp_buf_ = NULL;
      }

      if (NULL != comp_buf_)
      {
        comp_buf_->~ObMemBuf();
        ob_free(comp_buf_);
        comp_buf_ = NULL;
      }

      if (NULL != cur_node_)
      {
        cur_node_->clear();
        cur_node_ = NULL;
      }

      if (!mem_block_list_empty())
      {
        clear_mem_block_list();
      }      
    }

    int ObCompactSSTableWriterBuffer::set_cur_key(
        const ObRowkey& rowkey)
    {
      int ret = OB_SUCCESS;

      ObMemBufAllocatorWrapper allocator(cur_rowkey_buf_);
      if (OB_SUCCESS != (ret = rowkey.deep_copy(cur_rowkey_, allocator)))
      {
        TBSYS_LOG(WARN, "rowkey deep copy error:ret=%d", ret);
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::set_cur_key(const ObRow& row)
    {
      int ret = OB_SUCCESS;
      const ObRowkey* rowkey = NULL;
      ObMemBufAllocatorWrapper allocator(cur_rowkey_buf_);

      if (OB_SUCCESS != (ret = row.get_rowkey(rowkey)))
      {
        char buf[1024];
        (*rowkey).to_string(buf, 1024);
        TBSYS_LOG(WARN, "row get rowkey error:ret=%d,rowkey=%s", ret, buf);
      }
      else if (OB_SUCCESS != (ret = rowkey->deep_copy(cur_rowkey_, allocator)))
      {
        char buf[1024];
        cur_rowkey_.to_string(buf, 1024);
        TBSYS_LOG(WARN, "rowkey deep copy error:ret=%d,"
            "cur_rowkey_buf_.buf_ptr_=%p,cur_rowkey_buf_.buf_size_=%ld,",
            ret, cur_rowkey_buf_.get_buffer(),
            cur_rowkey_buf_.get_buffer_size());
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::ensure_uncomp_buf(
        const int64_t buf_size, const bool ptr_flag/*=true*/)
    {
      int ret = OB_SUCCESS;
      char* tmp_buf = NULL;

      if (NULL == uncomp_buf_)
      {
        tmp_buf = static_cast<char*>(ob_malloc(sizeof(ObMemBuf),
                ObModIds::OB_SSTABLE_WRITER));
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(ERROR, "malloc memorry error");
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          if (DEFAULT_UNCOMP_BUF_SIZE < buf_size)
          {
            uncomp_buf_ = new(tmp_buf)ObMemBuf(buf_size);
          }
          else
          {
            uncomp_buf_ = new(tmp_buf)ObMemBuf(DEFAULT_UNCOMP_BUF_SIZE);
          }

          if (NULL == uncomp_buf_)
          {
            TBSYS_LOG(WARN, "new struct error");
            ret = OB_MEM_OVERFLOW;
            ob_free(tmp_buf);
          }
          else
          {
            if (ptr_flag)
            {
              uncomp_buf_ptr_ = uncomp_buf_->get_buffer();
            }
          }
        }
      }
      
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = uncomp_buf_->ensure_space(buf_size, 
              ObModIds::OB_SSTABLE_WRITER)))
        {
          TBSYS_LOG(WARN, "ensure space error:ret=%d", ret);
        }
        else
        {
          if (ptr_flag)
          {
            uncomp_buf_ptr_ = uncomp_buf_->get_buffer();
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::ensure_comp_buf(
        const int64_t buf_size)
    {
      int ret = OB_SUCCESS;
      char* tmp_buf = NULL;

      if (NULL == comp_buf_)
      {
        tmp_buf = static_cast<char*>(ob_malloc(sizeof(ObMemBuf),
                ObModIds::OB_SSTABLE_WRITER));
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(ERROR, "malloc memory error");
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          if (DEFAULT_COMP_BUF_SIZE < buf_size)
          {
            comp_buf_ = new(tmp_buf)ObMemBuf(buf_size);
          }
          else
          {
            comp_buf_ = new(tmp_buf)ObMemBuf(DEFAULT_UNCOMP_BUF_SIZE);
          }

          if (NULL == comp_buf_)
          {
            TBSYS_LOG(WARN, "new struct error");
            ret = OB_MEM_OVERFLOW;
            ob_free(tmp_buf);
          }
          else
          {
            comp_buf_ptr_ = comp_buf_->get_buffer();
          }
        }
      }
      
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = comp_buf_->ensure_space(buf_size, 
              ObModIds::OB_SSTABLE_WRITER)))
        {
          TBSYS_LOG(WARN, "ensure space error:ret=%d", ret);
        }
        else
        {
          comp_buf_ptr_ = comp_buf_->get_buffer();
        }
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::cur_node_prepare()
    {
      int ret = OB_SUCCESS;
      char* tmp_buf = NULL;

      //cur_node_ == NULL
      if (NULL == cur_node_)
      {
        tmp_buf = static_cast<char*>(ob_malloc(sizeof(BlockListNode), 
              ObModIds::OB_SSTABLE_WRITER));
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(ERROR, "alloc mem error");
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          cur_node_ = new(tmp_buf)BlockListNode;
          if (NULL == cur_node_)
          {
            TBSYS_LOG(WARN, "create BlockListNode error");
            ret = OB_MEM_OVERFLOW;
            ob_free(tmp_buf);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::build_cur_node()
    {
      int ret = OB_SUCCESS;

      if (!output_buf_flag_)
      {//uncomp
        if (OB_SUCCESS != (ret = ensure_uncomp_buf(uncomp_buf_data_len_, false)))
        {
          TBSYS_LOG(WARN, "ensure uncomp buf error:ret=%d", ret);
        }
        else
        {
          memcpy(uncomp_buf_->get_buffer(), uncomp_buf_ptr_,
              uncomp_buf_data_len_);
          cur_node_->block_buf_ = uncomp_buf_;
          cur_node_->data_len_ = uncomp_buf_data_len_;

          if (OB_SUCCESS != (ret = cur_node_->set_block_endkey(cur_rowkey_)))
          {
            TBSYS_LOG(WARN, "set block endkey error:ret=%d", ret);
          }

          uncomp_buf_ = NULL;
          uncomp_buf_ptr_ = NULL;
          uncomp_buf_data_len_ = 0;
        }
      }
      else if (output_buf_flag_)
      {//comp
        cur_node_->block_buf_ = comp_buf_;
        cur_node_->data_len_ = comp_buf_data_len_;

        if (OB_SUCCESS != (ret = cur_node_->set_block_endkey(cur_rowkey_)))
        {
          TBSYS_LOG(WARN, "set block endkey error:ret=%d", ret);
        }

        comp_buf_ = NULL;
        comp_buf_ptr_ = NULL;
        comp_buf_data_len_ = 0;
      }

      if (OB_SUCCESS == ret)
      {
        cur_node_->set_record_header(record_header_);
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::update_list_bloomfilter(
        uint64_t table_id, const ObRowkey& row_key)
    {
      int ret = OB_SUCCESS;

      UNUSED(table_id);
      if (OB_SUCCESS != (ret = list_bloomfilter_.insert(row_key)))
      {
        TBSYS_LOG(WARN, "list bloomfilter insert error:ret=%d", ret);
      }

      return ret;
    }

    int ObCompactSSTableWriterBuffer::update_list_bloomfilter(
        uint64_t table_id, const ObRow& row)
    {
      int ret = OB_SUCCESS;
      UNUSED(table_id);
      const ObRowkey* rowkey = NULL;

      if (OB_SUCCESS != (ret = row.get_rowkey(rowkey)))
      {
        char buf[1024];
        row.to_string(buf, 1024);
        TBSYS_LOG(WARN, "row get rowkey error:ret=%d,row=%s,rowkey=%p",
            ret, buf, rowkey);
      }
      else if (OB_SUCCESS != (ret = list_bloomfilter_.insert(*rowkey)))
      {
        TBSYS_LOG(WARN, "list bloomfilter insert error:ret=%d", ret);
      }

      return ret;
    }
   
    void ObCompactSSTableWriterBuffer::clear_mem_block_list()
    {
      ObList<BlockListNode*>::iterator cur_iter = mem_block_list_.begin();
      ObList<BlockListNode*>::iterator end_iter = mem_block_list_.end();
      ObList<BlockListNode*>::iterator tmp_iter = cur_iter;

      while (cur_iter != end_iter)
      {
        (*cur_iter)->~BlockListNode();
        ob_free(*cur_iter);
        tmp_iter = cur_iter;
        cur_iter ++;
        mem_block_list_.erase(tmp_iter);
      }

      mem_block_list_total_len_ = 0;
    }

    int ObCompactSSTableWriterBuffer::compress(ObCompressor* compressor)
    {
      int ret = OB_SUCCESS;
      int64_t comp_buf_len = 0;

      if (NULL == compressor)
      {
        comp_buf_data_len_ =  0;
      }
      else 
      {
        comp_buf_len = uncomp_buf_data_len_ + 
          compressor->get_max_overflow_size(uncomp_buf_data_len_);
        if (OB_SUCCESS != (ret = ensure_comp_buf(comp_buf_len)))
        {
          TBSYS_LOG(WARN, "ensure after compress buf space error:ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = compressor->compress(
                uncomp_buf_ptr_, uncomp_buf_data_len_,
                comp_buf_ptr_, comp_buf_len, 
                comp_buf_data_len_)))
        {
          TBSYS_LOG(WARN, "compress error:ret=%d", ret);
        }
      }

      return ret;
    }
   
    void ObCompactSSTableWriterBuffer::update_record_header(
        const int16_t magic)
    {
      char* output_buf = NULL;
      int64_t output_len = 0;
      
      if (!output_buf_flag_)
      {
        output_buf = uncomp_buf_ptr_;
        output_len = uncomp_buf_data_len_;
      }
      else
      {
        output_buf = comp_buf_ptr_;
        output_len = comp_buf_data_len_;
      }

      record_header_.magic_ = magic;
      record_header_.header_length_ = static_cast<int16_t>(
          sizeof(ObRecordHeaderV2));
      record_header_.version_ = 0x2;
      record_header_.header_checksum_ = 0;
      record_header_.reserved16_ = 0;
      record_header_.data_length_ = uncomp_buf_data_len_;
      record_header_.data_zlength_ = output_len;
      record_header_.data_checksum_ = ob_crc64(output_buf, output_len);
      record_header_.set_header_checksum();
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
