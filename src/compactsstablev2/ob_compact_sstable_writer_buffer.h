#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_WRITER_BUFFER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_WRITER_BUFFER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/bloom_filter.h"
#include "common/ob_list.h"
#include "common/ob_record_header_v2.h"
#include "common/compress/ob_compressor.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_rowkey.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    static const int64_t SSTABLE_BLOOMFILTER_HASH_COUNT = 2;
    static const int64_t SSTABLE_BLOOMFILTER_SIZE = 2 * 1024 * 1024;
    static const int64_t DEFAULT_UNCOMP_BUF_SIZE = 128 * 1024;
    static const int64_t DEFAULT_COMP_BUF_SIZE = 1024 * 1024;

    struct BlockListNode
    {
      common::ObRecordHeaderV2 record_header_;
      common::ObMemBuf* block_buf_;
      int64_t data_len_;
      common::ObRowkey block_endkey_;
      common::ObMemBuf block_endkey_buf_;

      BlockListNode()
      : block_buf_(NULL),
        data_len_(0)
      {
        record_header_.reset();
        block_endkey_.assign(NULL, 0);
      }

      ~BlockListNode()
      {
        clear();
      }

      inline void reset()
      {
        record_header_.reset();

        if (NULL != block_buf_)
        {
          block_buf_->~ObMemBuf();
          ob_free(block_buf_);
          block_buf_ = NULL;
        }

        block_endkey_.assign(NULL, 0);
        data_len_  = 0;
      }

      inline void clear()
      {
        if (NULL != block_buf_)
        {
          block_buf_->~ObMemBuf();
          ob_free(block_buf_);
          block_buf_ = NULL;
        }
      }

      inline int set_block_endkey(common::ObRowkey& rowkey)
      {
        int ret = common::OB_SUCCESS;

        common::ObMemBufAllocatorWrapper allocator(block_endkey_buf_);
        if (common::OB_SUCCESS != (rowkey.deep_copy(block_endkey_, 
                allocator)))
        {
          TBSYS_LOG(WARN, "rowkey deep copy error");
        }

        return ret;
      }

      inline void set_record_header(const common::ObRecordHeaderV2& record_header)
      {
        record_header_.magic_ = record_header.magic_;
        record_header_.header_length_ = record_header.header_length_;
        record_header_.version_ = record_header.version_;
        record_header_.header_checksum_ = record_header.header_checksum_;
        record_header_.reserved16_ = record_header.reserved16_;
        record_header_.data_length_ = record_header.data_length_;
        record_header_.data_zlength_ = record_header.data_zlength_;
        record_header_.data_checksum_ = record_header.data_checksum_;
      }
    };//end  struct BlockListNode

    class ObCompactSSTableWriterBuffer
    {
    public:
      ObCompactSSTableWriterBuffer()
        : uncomp_buf_(NULL),
          uncomp_buf_ptr_(NULL),
          comp_buf_(NULL),
          comp_buf_ptr_(NULL),
          cur_node_(NULL)
      {
        reset();
      }

      ~ObCompactSSTableWriterBuffer()
      {
        clear();
      }

      void reset();

      void clear();

      inline const common::ObRowkey& get_cur_key() const
      {
        return cur_rowkey_;
      }

      int set_cur_key(const common::ObRowkey& rowkey);

      int set_cur_key(const common::ObRow& row);

      inline const char* get_uncomp_buf_ptr() const
      {
        return uncomp_buf_ptr_;
      }

      inline char* get_uncomp_buf_ptr()
      {
        return uncomp_buf_ptr_;
      }

      inline const char* get_comp_buf_ptr() const
      {
        return comp_buf_ptr_;
      }

      inline char* get_comp_buf_ptr()
      {
        return comp_buf_ptr_;
      }

      inline int64_t get_uncomp_buf_size() const
      {
        return uncomp_buf_->get_buffer_size();
      }

      inline int64_t get_comp_buf_size() const
      {
        return comp_buf_->get_buffer_size();
      }

      inline void set_uncomp_buf(char* buf, const int64_t len)
      {
        uncomp_buf_ptr_ = buf;
        uncomp_buf_data_len_ = len;
      }

      inline void set_uncomp_buf(const char* buf, const int64_t len)
      {
        uncomp_buf_ptr_ = const_cast<char*>(buf);
        uncomp_buf_data_len_ = len;
      }

      int ensure_uncomp_buf(const int64_t buf_size, 
          const bool ptr_flag = true);

      int ensure_comp_buf(const int64_t buf_size);

      inline void select_buf()
      {
        if (0 == comp_buf_data_len_)
        {
          output_buf_flag_ = false;
        }
        else if (uncomp_buf_data_len_ <= comp_buf_data_len_)
        {
          output_buf_flag_ = false;
        }
        else if (uncomp_buf_data_len_ > comp_buf_data_len_)
        {
          output_buf_flag_ = true;
        }
      }

      inline void get_output_buf(const char*& buf, int64_t& len) const
      {
        if (output_buf_flag_)
        {
          buf = comp_buf_ptr_;
          len = comp_buf_data_len_;
        }
        else
        {
          buf = uncomp_buf_ptr_;
          len = uncomp_buf_data_len_;
        }
      }

      int cur_node_prepare();

      inline const BlockListNode& get_cur_node() const
      {
        return *cur_node_;
      }

      int build_cur_node();

      inline int push_cur_node()
      {
        int ret = common::OB_SUCCESS;

        if (common::OB_SUCCESS != (ret = mem_block_list_.push_back(cur_node_)))
        {
          TBSYS_LOG(WARN, "mem block list push back error:ret=%d", ret);
        }
        else
        {
          mem_block_list_total_len_ += cur_node_->data_len_ 
            + sizeof(common::ObRecordHeaderV2);
          cur_node_ = NULL;
        }

        return ret;
      }

      //mem block list
      inline const common::ObList<BlockListNode*>& get_mem_block_list() const
      {
        return mem_block_list_;
      }

      inline bool mem_block_list_empty() const
      {
        return mem_block_list_.empty();
      }

      void clear_mem_block_list();

      inline int64_t get_list_row_count() const
      {
        return list_row_count_;
      }

      inline void reset_list_row_count()
      {
        list_row_count_ = 0;
      }

      inline void inc_list_row_count()
      {
        list_row_count_ ++;
      }

      inline const common::ObBloomFilterV1& get_list_bloomfilter() const
      {
        return list_bloomfilter_;
      }

      inline const common::ObRecordHeaderV2& get_record_header() const
      {
        return record_header_;
      }

      inline void reset_list_bloomfilter()
      {
        list_bloomfilter_.clear();
      }

      int update_list_bloomfilter(uint64_t table_id,
          const common::ObRowkey& row_key);

      int update_list_bloomfilter(uint64_t table_id,
          const common::ObRow& row);

      //compress
      int compress(ObCompressor* compressor);

      void update_record_header(const int16_t magic);

      //split flag
      inline bool check_reach_min_split_sstable_size(
          const int64_t min_split_sstable_size)
      {
        if (mem_block_list_total_len_ >= min_split_sstable_size)
        {
          return true;
        }
        else
        {
          return false;
        }
      }

      /**
       * this function is not already used
       * @param rowkey: the inserted rowkey of row
       */
      int check_rowkey(const common::ObRowkey& rowkey);

      /**
       * compare the row with the current row key
       * --if larger return OB_SUCCESS
       * --else return OB_ERROR
       * @param row: the inserted row
       */
      int check_rowkey(const common::ObRow& row);

    private:
      common::ObRowkey cur_rowkey_;
      common::ObMemBuf cur_rowkey_buf_;
      bool output_buf_flag_; //ture:压缩  false：非压缩

      common::ObMemBuf* uncomp_buf_;
      char* uncomp_buf_ptr_;
      int64_t uncomp_buf_data_len_;

      common::ObMemBuf* comp_buf_;
      char* comp_buf_ptr_;
      int64_t comp_buf_data_len_;

      common::ObRecordHeaderV2 record_header_;

      BlockListNode *cur_node_;
      common::ObList<BlockListNode*> mem_block_list_;
      int64_t mem_block_list_total_len_;
      int64_t list_row_count_;
      common::ObBloomFilterV1 list_bloomfilter_;
    };

    inline int ObCompactSSTableWriterBuffer::check_rowkey(
        const common::ObRowkey& rowkey)
    {
      int ret = common::OB_SUCCESS;

      if (!(rowkey > cur_rowkey_))
      {
        TBSYS_LOG(ERROR, "the inserted rowkey is not larger than"
            "current rowkey:*rowkey=[%s],cur_rowkey_=[%s]",
            to_cstring(rowkey), to_cstring(cur_rowkey_));
        ret = common::OB_INVALID_ARGUMENT;
      }

      return ret;
    }

    inline int ObCompactSSTableWriterBuffer::check_rowkey(
        const common::ObRow& row)
    {
      int ret = common::OB_SUCCESS;
      const common::ObRowkey* rowkey = NULL;

      if (common::OB_SUCCESS != (ret = row.get_rowkey(rowkey)))
      {
        TBSYS_LOG(ERROR, "row get rowkey error:ret=[%d],row=[%s]",
            ret, to_cstring(row));
      }
      else if (!(*rowkey > cur_rowkey_))
      {
        TBSYS_LOG(ERROR, "the rowkey of inserted row is not larger than"
            "current rowkey:*rowkey=[%s],cur_rowkey_=[%s]",
            to_cstring(*rowkey), to_cstring(cur_rowkey_));
        ret = common::OB_INVALID_ARGUMENT;
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end oceanbase

#endif
