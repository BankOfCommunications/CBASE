#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_record_header_v2.h"
#include "ob_sstable_aio_buffer_mgr.h"
#include "ob_sstable_block_index_mgr.h"
#include "ob_sstable_block_cache.h"

namespace oceanbase 
{
  namespace compactsstablev2
  {
    using namespace common;

    ObAIOBuffer::ObAIOBuffer() 
      : inited_(false), state_(FREE), fd_(-1), 
        sstable_id_(OB_INVALID_ID), fileinfo_cache_(NULL), 
        file_info_(NULL), file_offset_(-1), 
        misalign_buf_(NULL), buffer_(NULL), buf_size_(0), 
        buf_read_(0), buf_to_read_(0), buf_pos_(0), 
        block_(NULL), block_count_(0), 
        cur_block_idx_(0)
    {
    }

    ObAIOBuffer::~ObAIOBuffer()
    {
      if (NULL != misalign_buf_)
      {
        ob_free(misalign_buf_);
        misalign_buf_ = NULL;
        buffer_ = NULL;
      }

      //revert file info if the aio buffer is in WAIT state
      if (NULL != file_info_ && WAIT == state_)
      {
        fileinfo_cache_->revert_fileinfo(file_info_);
      }
    }

    int ObAIOBuffer::init()
    {
      int ret = OB_SUCCESS;

      if (NULL == misalign_buf_)
      {
        misalign_buf_ = static_cast<char*>(ob_malloc(
              OB_DIRECT_IO_ALIGN + DEFAULT_AIO_BUFFER_SIZE,
              ObModIds::OB_SSTABLE_AIO));
        if (NULL == misalign_buf_)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for aio read buffer");
          ret = OB_ERROR;
        }
        else
        {
          buffer_ = reinterpret_cast<char*>(upper_align(
                reinterpret_cast<int64_t>(misalign_buf_), 
                OB_DIRECT_IO_ALIGN));
          buf_size_ = DEFAULT_AIO_BUFFER_SIZE;
          inited_ = true;
        }
      }

      return ret;
    }

    int ObAIOBuffer::reset()
    {
      int ret = OB_SUCCESS;

      if (FREE != state_)
      {
        TBSYS_LOG(WARN, "state is not FREE, can't reset aio buffer");
        ret = OB_ERROR;
      }
      else
      {
        reverse_scan_ = false;
        fd_ = -1;
        sstable_id_ = OB_INVALID_ID;
        fileinfo_cache_ = NULL;
        file_info_ = NULL;
        file_offset_ = -1;
        buf_read_ = 0;
        buf_to_read_ = 0;
        buf_pos_ = 0;
        block_ = NULL;
        block_count_ = 0;
        cur_block_idx_ = 0;
      }

      return ret;
    }

    void ObAIOBuffer::aio_finished(const int64_t ret_size, const int ret_code)
    {
      if (NULL != file_info_ && WAIT == state_)
      {
        fileinfo_cache_->revert_fileinfo(file_info_);
      }

      if (0 == ret_code && WAIT == state_)
      {
        state_ = READY;
        if (ret_size == buf_to_read_)
        {
          buf_read_ = buf_to_read_;
        }
        else
        {
          //end of file
          buf_read_ = ret_size;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed aio read, ret_size=%ld, buf_to_read_=%ld, "
            "ret_code=%d, state=%d, error=%s", 
            ret_size, buf_to_read_, ret_code, state_, strerror(ret_code));
      }
    }

    int64_t ObAIOBuffer::lower_align(int64_t input, int64_t align)
    {
      int64_t ret = input;
      ret = (input + align - 1) & ~(align - 1);
      ret = ret - ((ret - input + align - 1) & ~(align - 1));
      return ret;
    };

    int64_t ObAIOBuffer::upper_align(int64_t input, int64_t align)
    {
      int64_t ret = input;
      ret = (input + align - 1) & ~(align - 1);
      return ret;
    };

    int ObAIOBuffer::set_read_info(uint64_t sstable_id, 
            IFileInfoMgr& fileinfo_cache,
            ObBlockInfo* block, 
            const int64_t block_count,
            const bool reverse_scan)
    {
      int ret = OB_SUCCESS;
      int64_t read_size = 0;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't init");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id 
          || NULL == block || block_count <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, sstable_id=%lu, "
            "block=%p, block_count=%ld", 
            sstable_id, block, block_count);
        ret = OB_ERROR;
      }
      else
      {
        file_offset_ = lower_align(block[0].offset_, OB_DIRECT_IO_ALIGN);
        for (int64_t i = 0; i < block_count; ++i)
        {
          read_size += block[i].size_;
        }
        buf_to_read_ = upper_align(block[0].offset_ + read_size, 
            OB_DIRECT_IO_ALIGN) - file_offset_;
        if (buf_to_read_ > buf_size_)
        {
          TBSYS_LOG(WARN, "too many block to read, buf_to_read_=%ld, "
              "buf_size=%ld", buf_to_read_, buf_size_);
          file_offset_ = -1;
          buf_to_read_ = 0;
          ret = OB_ERROR;
        }
        else
        {
          if (NULL == (file_info_ = fileinfo_cache.get_fileinfo(sstable_id)))
          {
            TBSYS_LOG(WARN, "get file info fail sstable_id=%lu", sstable_id);
            ret = OB_ERROR;
          }
          else
          {
            fileinfo_cache_ = &fileinfo_cache;
            reverse_scan_ = reverse_scan;
            fd_ = file_info_->get_fd();
            sstable_id_ = sstable_id;
            block_ = block;
            block_count_ = block_count;
            
            if (reverse_scan_)
            {
              cur_block_idx_ = block_count_ - 1;
              buf_pos_ = block[0].offset_ - file_offset_ 
                + read_size - block[cur_block_idx_].size_;
            }
            else
            {
              buf_pos_ = block[0].offset_ - file_offset_;
            }
          }
        }
      }

      return ret;
    }

    int ObAIOBuffer::get_block(const uint64_t sstable_id, 
        const int64_t offset, const int64_t size, char*& buffer)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't init");
        ret = OB_ERROR;
      }
      else if (READY != state_)
      {
        TBSYS_LOG(WARN, "aio buffer isn't ready, state=%d", state_);
        ret = OB_ERROR;
      }
      else //READY == state_
      {
        if (cur_block_idx_ >= block_count_ || cur_block_idx_ < 0)
        {
          TBSYS_LOG(WARN, "current block is out of range in aio buffer, "
              "reverse_scan_=%d, cur_block_idx_=%ld, block_count_=%ld", 
              reverse_scan_, cur_block_idx_, block_count_);
          ret = OB_ERROR;
        }
        else if (!is_data_valid(sstable_id, offset, size ))
        {
          TBSYS_LOG(WARN, "get block not in order, sstable_id=%lu, "
              "expected_sstable_id=%lu, offset=%ld, expected_offset=%ld, "
              "size=%ld, expected_size=%ld", 
              sstable_id, sstable_id_, offset, block_[cur_block_idx_].offset_,
              size, block_[cur_block_idx_].size_);
          ret = OB_ERROR;
        }
        else
        {
          if (buf_read_ < buf_to_read_ && buf_read_ < buf_pos_ + size)
          {
            //end of file, don't return the part of data
            ret = OB_AIO_EOF;
          }
          else
          {
            buffer = buffer_ + buf_pos_;
            if (!reverse_scan_)
            {
              buf_pos_ += size;
              if (++cur_block_idx_ >= block_count_)
              {
                ret = OB_AIO_BUF_END_BLOCK;
              }
            }
            else
            {
              if (--cur_block_idx_ < 0)
              {
                ret = OB_AIO_BUF_END_BLOCK;
              }
              else
              {
                buf_pos_ -= block_[cur_block_idx_].size_;
              }
            }
          }
        }
      }

      return ret;
    }

    int ObAIOBuffer::get_next_block(uint64_t& sstable_id, int64_t& offset,
                                    int64_t& size, char*& buffer, bool& cached_)
    {
      int ret     = OB_SUCCESS;
      sstable_id  = OB_INVALID_ID;
      offset      = 0;
      size        = 0;
      buffer      = NULL;
      cached_     = false;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't init");
        ret = OB_ERROR;
      }
      else if (READY != state_)
      {
        TBSYS_LOG(WARN, "aio buffer isn't ready, state=%d", state_);
        ret = OB_ERROR;
      }
      else //READY == state_
      {
        if (cur_block_idx_ >= block_count_ || cur_block_idx_ < 0)
        {
          ret = OB_AIO_BUF_END_BLOCK;
        }
        else
        {
          if (buf_read_ < buf_to_read_ 
              && buf_read_ < buf_pos_ + block_[cur_block_idx_].size_)
          {
            //end of file, don't return the part of data
            ret = OB_AIO_EOF;
          }
          else
          {
            sstable_id = sstable_id_;
            offset = block_[cur_block_idx_].offset_;
            size = block_[cur_block_idx_].size_;
            buffer = buffer_ + buf_pos_;
            cached_ = block_[cur_block_idx_].cached_;
            if (!reverse_scan_)
            {
              buf_pos_ += size;
              ++cur_block_idx_;
            }
            else
            {
              if (--cur_block_idx_ >= 0)
              {
                buf_pos_ -= block_[cur_block_idx_].size_;
              }
            }
          }
        }
      }

      return ret;
    }

    int ObAIOBuffer::put_cache_block(const char* buffer, const int64_t size, 
                                     const bool reverse_scan)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't init");
        ret = OB_ERROR;
      }
      else if (NULL == buffer || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, buffer=%p, size=%ld", buffer, size);
        ret = OB_ERROR;
      }
      else
      {
        if (reverse_scan)
        {
          //reverse store block data
          buf_read_ += size;
          memcpy(buffer_ + buf_size_ - buf_read_, buffer, size);
        }
        else
        {
          memcpy(buffer_ + buf_read_, buffer, size);
          buf_read_ += size;
        }
      }

      return ret;
    }

    int ObAIOBuffer::set_cache_block_info(uint64_t sstable_id, 
                                          ObBlockInfo* block, 
                                          const int64_t block_count,
                                          const bool reverse_scan)
    {
      int ret           = OB_SUCCESS;
      int64_t read_size = 0;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't init");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id || NULL == block || block_count <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, sstable_id=%lu, "
            "block=%p, block_count=%ld", 
            sstable_id, block, block_count);
        ret = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; i < block_count; ++i)
        {
          read_size += block[i].size_;
        }
        buf_to_read_ = read_size;
        if (buf_to_read_ > buf_size_)
        {
          TBSYS_LOG(WARN, "too many blocks to read, buf_to_read_=%ld, "
              "buf_size=%ld", buf_to_read_, buf_size_);
          buf_to_read_ = 0;
          ret = OB_ERROR;
        }
        else if (buf_to_read_ != buf_read_)
        {
          TBSYS_LOG(WARN, "buf_read_ is inconsistent with buf_to_read_, "
              "buf_to_read_=%ld, buf_read_=%ld",
              buf_to_read_, buf_read_);
          buf_to_read_ = 0;
          buf_read_ = 0;
          ret = OB_ERROR;
        }
        else
        {
          state_ = READY; //set aio buffer state ready
          reverse_scan_ = reverse_scan;
          sstable_id_ = sstable_id;
          block_ = block;
          block_count_ = block_count;
          
          if (reverse_scan_)
          {
            cur_block_idx_ = block_count_ - 1;
            buf_pos_ = buf_size_ - buf_read_ 
              + read_size - block[cur_block_idx_].size_;
          }
          else
          {
            cur_block_idx_ = 0;
            buf_pos_ = 0;
          }
        }
      }

      return ret;
    }

    ObAIOBufferMgr::ObAIOBufferMgr() 
      : inited_(false), state_(FREE_FREE), reverse_scan_(false), 
        copy2cache_(false), get_first_block_(false), copy_from_cache_(false), 
        update_buf_range_(false), sstable_id_(OB_INVALID_ID), 
        block_cache_(NULL), block_(NULL), 
        block_buf_size_(DEFAULT_BLOCK_BUF_SIZE), block_count_(0), 
        cur_block_idx_(0), cur_read_block_idx_(0), end_curread_block_idx_(0),
        preread_block_idx_(0), end_preread_block_idx_(0), cur_buf_idx_(0)
    {

    }

    ObAIOBufferMgr::~ObAIOBufferMgr()
    {
      io_context_t ctx = get_io_context();

      if (NULL != ctx)
      {
        io_destroy(ctx);
      }

      if (NULL != block_)
      {
        ob_free(block_);
        block_ = NULL;
      }
    }

    io_context_t ObAIOBufferMgr::get_io_context()
    {
      static __thread io_context_t ctx = NULL;
      int64_t max_events = AIO_BUFFER_COUNT * OB_MAX_COLUMN_GROUP_NUMBER;

      if (NULL == ctx)
      {
        if (0 != io_setup(static_cast<int>(max_events), &ctx))
        {
          TBSYS_LOG(WARN, "failed to setup io context, ctx=%p, error:%s", 
              ctx, strerror(errno));
          ctx = NULL;
        }
      }

      return ctx;
    }

    int ObAIOBufferMgr::init()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = ensure_block_buf_space(DEFAULT_BLOCK_BUF_SIZE);
        for (int64_t i = 0; i < AIO_BUFFER_COUNT && OB_SUCCESS == ret; ++i)
        {
          ret = buffer_[i].init();
          if (OB_SUCCESS == ret)
          {
            ret = event_mgr_[i].init(get_io_context());
          }
        }
  
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::ensure_block_buf_space(const int64_t size)
    {
      int ret           = OB_SUCCESS;
      char* new_buf     = NULL;
      int64_t buf_size  = size > block_buf_size_ ? size : block_buf_size_;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid block info buffer size, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == block_ || (NULL != block_ && size > block_buf_size_))
      {
        new_buf = static_cast<char*>(ob_malloc(buf_size, 
              ObModIds::OB_SSTABLE_AIO));
        if (NULL == new_buf)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for block info buffer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != block_)
          {
            ob_free(block_);
            block_ = NULL;
          }
          block_buf_size_ = buf_size;
          block_ = reinterpret_cast<ObBlockInfo*>(new_buf);
          memset(block_, 0, block_buf_size_);
        }
      }

      return ret;
    }

    void ObAIOBufferMgr::reset()
    {
      reverse_scan_ = false;
      copy2cache_ = false;
      get_first_block_ = false;
      copy_from_cache_ = false;
      update_buf_range_ = false;
      sstable_id_ = OB_INVALID_ID;
      block_cache_ = NULL;
      block_count_ = 0;
      cur_block_idx_ = 0;
      cur_read_block_idx_ = 0;
      end_curread_block_idx_ = 0;
      preread_block_idx_ = 0;
      end_preread_block_idx_ = 0;
      cur_buf_idx_ = 0;
      aio_stat_.reset();
    }

    int ObAIOBufferMgr::advise(ObSSTableBlockCache& block_cache,
        const uint64_t sstable_id,
        const ObBlockPositionInfos& block_infos, 
        const bool copy2cache,
        const bool reverse_scan)
    {
      int ret = OB_SUCCESS;
      int64_t next_offset = 0;

      if (OB_INVALID_ID == sstable_id || block_infos.block_count_ <= 0)
      {
        TBSYS_LOG(WARN, "Invalid parameter, sstable_id=%lu, block_count=%ld",
            sstable_id, block_infos.block_count_);
        ret = OB_ERROR;
      }
      else if (!inited_)
      {
        //initialize aio buffer manager 
        ret = init();
      }

      if (OB_SUCCESS == ret && inited_ && !check_aio_buf_free())
      {
        TBSYS_LOG(WARN, "two aio buffer aren't in FREE state, "
            "cur_aio_state=%s", get_state_str(get_state()));
        ret = OB_AIO_BUSY;
      }

      if (OB_SUCCESS == ret && inited_)
      {
        reset();
        reverse_scan_ = reverse_scan;
        copy2cache_ = copy2cache;
        get_first_block_ = true;
        sstable_id_ = sstable_id;
        block_cache_ = &block_cache;

        ret = ensure_block_buf_space(block_infos.block_count_ 
            * sizeof(ObBlockInfo));
      }

      if (OB_SUCCESS == ret && inited_)
      {
        next_offset = block_infos.position_info_[0].offset_;
        for (int64_t i = 0; i < block_infos.block_count_ 
            && OB_SUCCESS == ret; ++i)
        {
          if (block_infos.position_info_[i].offset_ < 0 
              || block_infos.position_info_[i].size_ <= 0)
          {
            TBSYS_LOG(WARN, "invalid block info, block_offset=%ld, "
                "block_size=%ld, block_idx=%ld", 
                block_infos.position_info_[i].offset_, 
                block_infos.position_info_[i].size_, i);
            ret = OB_ERROR;
            break;
          }
          else if (!can_fit_aio_buffer(block_infos.position_info_[i].size_))
          {
            //block size too big(more than 1M)
            TBSYS_LOG(WARN, "block size is too big, block_size=%ld, "
                "block_idx=%ld, max_block_size=%ld", 
                block_infos.position_info_[i].size_, i, 
                ObAIOBuffer::DEFAULT_AIO_BUFFER_SIZE);
            ret = OB_ERROR;
            break;
          }
          else if (next_offset == block_infos.position_info_[i].offset_)
          {
            //blocks must be continuous
            block_count_++;
            block_[i].offset_ = block_infos.position_info_[i].offset_;
            block_[i].size_ = block_infos.position_info_[i].size_;
            block_[i].cached_ = false;  //default block isn't in block cache

            next_offset += block_[i].size_;
          }
          else
          {
            reset();
            TBSYS_LOG(WARN, "blocks are not continuous, pre_offset=%ld, "
                "pre_size=%ld, cur_offset=%ld, expected_offset=%ld, "
                "block_idx=%ld", 
                i == 0 ? 0 : block_[i - 1].offset_, i == 0 
                ? 0 : block_[i - 1].size_,
                block_infos.position_info_[i].offset_, next_offset, i);
            ret = OB_ERROR;
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          aio_stat_.sstable_id_ = sstable_id_;
          aio_stat_.total_blocks_ = block_count_;
          aio_stat_.total_size_ = block_[block_count_ - 1].offset_ 
            + block_[block_count_ - 1].size_ - block_[0].offset_;
        }
      }

      if (OB_SUCCESS == ret && inited_ && reverse_scan_)
      {
        cur_block_idx_ = block_count_ - 1;
      }

      return ret;
    }

    int ObAIOBufferMgr::set_aio_buf_block_info(const int64_t aio_buf_idx, 
                                               const bool check_cache)
    {
      int ret = OB_SUCCESS;

      if (aio_buf_idx >= AIO_BUFFER_COUNT || aio_buf_idx < 0)
      {
        TBSYS_LOG(WARN, "invalid aio buffer index, aio_buf_idx=%ld", 
            aio_buf_idx);
        ret = OB_ERROR;
      }
      else if (check_cache)
      {
        if (aio_buf_idx == cur_buf_idx_)
        {
          ret = buffer_[aio_buf_idx].set_cache_block_info(sstable_id_, 
              &block_[cur_read_block_idx_], 
              end_curread_block_idx_ - cur_read_block_idx_, reverse_scan_);
        }
        else
        {
          ret = buffer_[aio_buf_idx].set_cache_block_info(sstable_id_, 
              &block_[preread_block_idx_], 
              end_preread_block_idx_ - preread_block_idx_, reverse_scan_);
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::reset_both_aio_buffer()
    {
      int ret = OB_SUCCESS;

      if (!check_aio_buf_free())
      {
        TBSYS_LOG(WARN, "two aio buffer aren't in FREE state, "
            "cur_aio_state=%s", get_state_str(get_state()));
        ret = OB_AIO_BUSY;
      }
      else
      {
        ret = buffer_[cur_buf_idx_].reset();
        if (OB_SUCCESS == ret)
        {
          ret = buffer_[(cur_buf_idx_ + 1) % AIO_BUFFER_COUNT].reset();
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::cur_aio_buf_add_block(
      bool& cur_range_assign, const int64_t block_idx, 
      const int64_t block_size, const int64_t total_size, 
      const bool check_cache, const bool reverse_scan,
      ObSSTableBlockBufferHandle& buffer_handle)
    {
      int ret = OB_SUCCESS;

      if (!cur_range_assign)
      {
        if (!can_fit_aio_buffer(total_size))
        {
          /**
           * currnet aio buffer can't store all the data, asign the block 
           * range of current aio buffer to read, and assign the start 
           * block of preread 
           */
          if (!reverse_scan)
          {
            cur_read_block_idx_ = 0;
            end_curread_block_idx_ = block_idx;
            preread_block_idx_ = block_idx;
          }
          else
          {
            cur_read_block_idx_ = block_idx + 1;
            end_curread_block_idx_ = block_count_;
            end_preread_block_idx_ = block_idx + 1;
          }
          cur_range_assign = true;
          ret = set_aio_buf_block_info(cur_buf_idx_, check_cache);
        }
        else if (check_cache)
        {
          //copy block to aio buffer from block cache
          block_[block_idx].cached_ = true;
          copy_from_cache_ = true;
          aio_stat_.total_cached_size_ += block_size;
          ret = buffer_[cur_buf_idx_].put_cache_block(
            buffer_handle.get_buffer(), block_size, reverse_scan);
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::preread_aio_buf_add_block(
      const bool cur_range_assign, bool& preread_range_assign,
      const int64_t block_idx, const int64_t block_size, 
      int64_t& preread_size, const bool check_cache, 
      const bool reverse_scan, ObSSTableBlockBufferHandle& buffer_handle)
    {
      int ret = OB_SUCCESS;
      int64_t preread_buf_idx = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;

      if (OB_SUCCESS == ret && cur_range_assign && !preread_range_assign)
      {
        preread_size += block_size;
        if (!can_fit_aio_buffer(preread_size))
        {
          /**
           * preread aio buffer is also can't store the left data, set the 
           * end block of preread aio buffer. 
           */
          if (!reverse_scan)
          {
            end_preread_block_idx_ = block_idx;
          }
          else
          {
            preread_block_idx_ = block_idx + 1;
          }
          preread_range_assign = true;
          ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
        }
        else if (check_cache)
        {
          //copy block to aio buffer from block cache
          block_[block_idx].cached_ = true;
          copy_from_cache_ = true;
          aio_stat_.total_cached_size_ += block_size;
          ret = buffer_[preread_buf_idx].put_cache_block(
            buffer_handle.get_buffer(), block_size, reverse_scan);
        }
      }

      return ret;
    }
    
    int ObAIOBufferMgr::do_first_block_noexist_in_cache(
      bool& cur_range_assign, bool& preread_range_assign,
      bool& check_cache, const int64_t block_idx, 
      const bool reverse_scan)
    {
      int ret                       = OB_SUCCESS;
      int64_t cur_buf_read_size     = 0;
      int64_t preread_buf_read_size = 0;
      int64_t preread_buf_idx       = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;

      /**
       * the first block not in block cache, we read the block and 
       * follwing block from sstable file. so reset copy_from_cache_ 
       * flag.
       */
      copy_from_cache_ = false;
      cur_buf_read_size = buffer_[cur_buf_idx_].get_read_size();
      preread_buf_read_size = buffer_[preread_buf_idx].get_read_size();
      if (!cur_range_assign && !preread_range_assign)
      {
        if (cur_buf_read_size == 0 && 0 == preread_buf_read_size)
        {
          //no block data in cache, try without check cache 
          check_cache = false;
          ret = OB_CS_EAGAIN;
        }
        else if (cur_buf_read_size > 0 && 0 == preread_buf_read_size)
        {
          /**
           * current aio buffer has blocks, preread aio buffer has no 
           * block. if there is one block at least copied from cache, we 
           * don't use this aio buffer to read block from sstable file 
           * before the blocks are getten by user. 
           */
          if (!reverse_scan)
          {
            cur_read_block_idx_ = 0;
            end_curread_block_idx_ = block_idx;
            preread_block_idx_ = block_idx;
          }
          else
          {
            cur_read_block_idx_ = block_idx + 1;
            end_curread_block_idx_ = block_count_;
            end_preread_block_idx_ = block_idx + 1;
          }
          cur_range_assign = true;
          ret = set_aio_buf_block_info(cur_buf_idx_, check_cache);
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected stauts, cur_range_assign=%d, "
              "preread_range_assign=%d, cur_buf_read_size=%ld, "
              "preread_buf_read_size=%ld, block_index=%ld", 
              cur_range_assign, preread_range_assign, cur_buf_read_size,
              preread_buf_read_size, block_idx);
          ret = OB_ERROR;
        }
      }
      else if (cur_range_assign && !preread_range_assign)
      {
        //both aio buffer has blocks
        if (cur_buf_read_size > 0 && preread_buf_read_size > 0)
        {
          if (!reverse_scan)
          {
            end_preread_block_idx_ = block_idx;
          }
          else
          {
            preread_block_idx_ = block_idx + 1;
          }
          preread_range_assign = true;
          ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected stauts, cur_range_assign=%d, "
              "preread_range_assign=%d, cur_buf_read_size=%ld, "
              "preread_buf_read_size=%ld, block_index=%ld", 
              cur_range_assign, preread_range_assign, cur_buf_read_size,
              preread_buf_read_size, block_idx);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::do_all_blocks_in_cache(
      const bool cur_range_assign, const bool preread_range_assign,
      const bool check_cache, const bool reverse_scan)
    {
      int ret = OB_SUCCESS;
      int64_t preread_buf_idx = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;

      /**
       * all blocks copy from block cache
       */
      if (!cur_range_assign && !preread_range_assign)
      {
        cur_read_block_idx_ = 0;
        end_curread_block_idx_ = block_count_;
        if (!reverse_scan)
        {
          preread_block_idx_ = block_count_;
          end_preread_block_idx_ = block_count_;
        }
        else
        {
          preread_block_idx_ = 0;
          end_preread_block_idx_ = 0;
        }
        ret = set_aio_buf_block_info(cur_buf_idx_, check_cache);
      }
      else if (cur_range_assign && !preread_range_assign)
      {
        if (!reverse_scan)
        {
          end_preread_block_idx_ = block_count_;
        }
        else
        {
          preread_block_idx_ = 0;
        }
        ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
      }
      else
      {
        //both aio buffer read range are assigned, do nothing.
      }

      return ret;
    }

    void ObAIOBufferMgr::do_either_aio_buf_assigned(
      const int64_t total_size, const bool reverse_scan)
    {
      int64_t i             = 0;
      int64_t start         = 0;
      int64_t end           = 0;
      int64_t preread_size  = 0;

      //total blocks size is less than 512K, only use one aio buffer to read
      if (total_size <= MIN_PREREAD_SIZE)
      {
        if (!reverse_scan)
        {
          cur_read_block_idx_ = 0;
          end_curread_block_idx_ = block_count_;
          preread_block_idx_ = block_count_;
          end_preread_block_idx_ = block_count_;
        }
        else
        {
          cur_read_block_idx_ = 0;
          end_curread_block_idx_ = block_count_;
          preread_block_idx_ = 0;
          end_preread_block_idx_ = 0;
        }
      }
      else if (can_fit_aio_buffer(total_size))
      {
        /**
         * if 512K < total_size < 1M, and there are more than 1 block, 
         * use two aio buffer to read, each aio buffer read about half 
         * of data. if there is only 1 block, use one aio buffer to 
         * read. 
         */
        if (!reverse_scan)
        {
          cur_read_block_idx_ = 0;
          end_preread_block_idx_ = block_count_;
        }
        else
        {
          preread_block_idx_ = 0;
          end_curread_block_idx_ = block_count_;
        }

        start = reverse_scan ? block_count_ - 1 : 0;
        end   = reverse_scan ? 0 : block_count_;
        if (block_count_ > 1)
        {
          //more than one block, each aio buffer read half of blocks
          for (i = start; reverse_scan ? (i >= end) : (i < end);)
          {
            preread_size += block_[i].size_;
            if (preread_size > total_size / AIO_BUFFER_COUNT)
            {
              if (!reverse_scan)
              {
                end_curread_block_idx_ = i;
                preread_block_idx_ = i;
              }
              else
              {
                cur_read_block_idx_ = i + 1;
                end_preread_block_idx_ = i + 1;
              }
              break;
            }

            //update index
            reverse_scan ? (--i) : (++i);
          }
        }
        else
        {
          //only one block, just use the current aio buffer to read
          if (!reverse_scan)
          {
            end_curread_block_idx_ = block_count_;
            preread_block_idx_ = block_count_;
          }
          else
          {
            cur_read_block_idx_ = 0;
            end_preread_block_idx_ = 0;
          }
        }
      }
    }

    void ObAIOBufferMgr::do_only_cur_aio_buf_assigned(
      const bool check_cache, const bool reverse_scan)
    {
      int64_t i               = 0;
      int64_t preread_size    = 0;
      int64_t preread_buf_idx = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;

      /**
       * current aio buffer is filled some block data, but preread aio 
       * buffer is null, so preread aio buffer need read block data 
       * from sstable file. the following code caculate the preread 
       * block range if necessary. 
       */
      if (check_cache && 0 == buffer_[preread_buf_idx].get_read_size()
          && !reverse_scan && preread_block_idx_ < block_count_)
      {
        for (i = preread_block_idx_; i < block_count_; ++i)
        {
          preread_size += block_[i].size_;
          if (!can_fit_aio_buffer(preread_size))
          {
            end_preread_block_idx_ = i;
            break;
          }
        }

        if (block_count_ == i)
        {
          end_preread_block_idx_ = block_count_;
        }
      }
      else if (check_cache && 0 == buffer_[preread_buf_idx].get_read_size()
               && reverse_scan && end_preread_block_idx_ > 0)
      {
        for (i = end_preread_block_idx_ - 1; i >= 0; --i)
        {
          preread_size += block_[i].size_;
          if (!can_fit_aio_buffer(preread_size))
          {
            preread_block_idx_ = i + 1; 
            break;
          }
        }

        if (i < 0)
        {
          preread_block_idx_ = 0;
        }
      }
      else
      {
        /**
         * blocks range of current aio buffer is assigned, but blocks 
         * range of preread aio buffer isn't assigned. it means that the 
         * total block size is less than 2M and greater than 1M. if 
         * enable cache check, can't run into this path. for cache 
         * check, this case is handled in for loop. 
         */
        if (!reverse_scan)
        {
          end_preread_block_idx_ = block_count_;
        }
        else
        {
          preread_block_idx_ = 0;
        }
      }
    }

    int ObAIOBufferMgr::internal_init_read_range(const bool read_cache,
                                                 const bool reverse_scan)
    {
      int ret                   = OB_SUCCESS;
      int64_t total_size        = 0;
      int64_t preread_size      = 0;
      bool cur_range_assign     = false;
      bool preread_range_assign = false;
      bool check_cache          = read_cache;
      int32_t block_size        = -1;
      int64_t i                 = 0;
      int64_t start             = reverse_scan ? block_count_ - 1 : 0;
      int64_t end               = reverse_scan ? 0 : block_count_;
      int64_t preread_buf_idx   = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;

      ret = reset_both_aio_buffer();

      for (i = start; reverse_scan ? (i >= end) : (i < end) && OB_SUCCESS == ret;)
      {
        ObSSTableBlockBufferHandle buffer_handle;
        block_size = static_cast<int32_t>(block_[i].size_);
        if (check_cache)
        {
          block_size = block_cache_->get_cache_block(
              sstable_id_, block_[i].offset_,
              block_[i].size_, buffer_handle);
        }

        if (block_size == block_[i].size_)
        {
          total_size += block_size;
          ret = cur_aio_buf_add_block(cur_range_assign, i, 
              block_size, total_size,
              check_cache, reverse_scan, buffer_handle);
  
          if (OB_SUCCESS == ret)
          {
            ret = preread_aio_buf_add_block(cur_range_assign, 
                preread_range_assign, i, block_size, preread_size, 
                check_cache, reverse_scan, buffer_handle);
            if (cur_range_assign && preread_range_assign)
            {
              break;  //both aio buffer is full, break loop
            }
          }
        }
        else if (check_cache) //get block from block cache 
        {
          ret = do_first_block_noexist_in_cache(
              cur_range_assign, preread_range_assign,
              check_cache, i, reverse_scan);
          if (OB_CS_EAGAIN == ret)
          {
            ret = OB_SUCCESS;
            continue;
          }
          else
          {
            break;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "get block from block cache failed, block_size=%d," 
              "block_[%ld].size_=%ld, check_cache=%d", 
              block_size, i, block_[i].size_, check_cache);
          ret = OB_ERROR;
        }

        //update index
        reverse_scan ? (--i) : (++i);
      }

      if (OB_SUCCESS == ret)
      {
        if (copy_from_cache_)
        {
          ret = do_all_blocks_in_cache(cur_range_assign, 
              preread_range_assign,
              check_cache, reverse_scan);
        }
        else
        {
          /**
           * both blocks range of curret aio buffer and blocks range of 
           * preread aio buffer aren't assigned. it means that total block 
           * size is less than 1M. this path if only for no cache check.
           */
          if (!cur_range_assign && !preread_range_assign)
          {
            do_either_aio_buf_assigned(total_size, reverse_scan);
          }
          else if (cur_range_assign && !preread_range_assign)
          {
            do_only_cur_aio_buf_assigned(check_cache, reverse_scan);
          }
          else
          {
            //both aio buffer read range are assigned, do nothing.
          }
        }
      }

      //failed to init range, reset aio buffer
      if (OB_SUCCESS != ret && OB_AIO_BUSY != ret)
      {
        buffer_[cur_buf_idx_].reset();
        buffer_[cur_buf_idx_].set_state(FREE);
        buffer_[preread_buf_idx].reset();
        buffer_[preread_buf_idx].set_state(FREE);
      }

      return ret;
    }

    int ObAIOBufferMgr::init_read_range()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't initialize");
        ret = OB_ERROR;
      }
      else if(block_count_ <= 0 || NULL == block_)
      {
        TBSYS_LOG(WARN, "block of aio buffer manager doesn't initialize, "
            "block_=%p, block_count_=%ld", block_, block_count_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && get_first_block_)
      {
        ret = internal_init_read_range(copy2cache_, reverse_scan_);
      }

      return ret;
    }

    ObDoubleAIOBufferState ObAIOBufferMgr::get_state() const
    {
      ObAIOBufferState aiobuf1_state = buffer_[0].get_state();
      ObAIOBufferState aiobuf2_state = buffer_[1].get_state();
      static const ObDoubleAIOBufferState state_array[3][3] ={
        {FREE_FREE, WAIT_FREE, READY_FREE}, 
        {WAIT_FREE, WAIT_WAIT, WAIT_READY},
        {READY_FREE, WAIT_READY, READY_READY}
      };

      return state_array[aiobuf1_state][aiobuf2_state];
    }

    const char* ObAIOBufferMgr::get_state_str(ObDoubleAIOBufferState state) const
    {
      static const char* state_strs[] = {
        "FREE_FREE", "READY_READY", "WAIT_WAIT", 
        "WAIT_READY", "WAIT_FREE", "READY_FREE" 
      };

      return state_strs[state];
    }

    inline void ObAIOBufferMgr::set_state(ObDoubleAIOBufferState state)
    {
      TBSYS_LOG(DEBUG, "switch state from %s to %s", 
                get_state_str(state_), get_state_str(state));
      state_ = state;
    }

    inline void ObAIOBufferMgr::set_stop(bool& stop, const int ret)
    {
      TBSYS_LOG(DEBUG, "stop state machine, state: %s, ret=%d", 
                get_state_str(state_), ret);
      stop = true;
    }

    int ObAIOBufferMgr::update_read_range(const bool check_cache,
                                          const bool reverse_scan)
    {
      int ret                       = OB_SUCCESS;
      int64_t preread_size          = 0;
      int64_t i                     = 0;
      int64_t block_size            = -1;
      int64_t start                 = 0;
      int64_t end                   = 0;
      int64_t preread_buf_idx       = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      ObAIOBuffer& preread_aio_buf  = buffer_[preread_buf_idx];
      int64_t preread_buf_read_size = 0;

      if (FREE != preread_aio_buf.get_state())
      {
        TBSYS_LOG(WARN, "preread aio buffer isn't in FREE state, state=%s",
                  get_state_str(get_state()));
        ret = OB_ERROR;
      }
      else
      {
        ret = preread_aio_buf.reset();
      }

      if (OB_SUCCESS == ret)
      {
        if (!reverse_scan)
        {
          cur_read_block_idx_ = preread_block_idx_;
          end_curread_block_idx_ = end_preread_block_idx_;
          preread_block_idx_ = end_preread_block_idx_;
        }
        else
        {
          cur_read_block_idx_ = preread_block_idx_;
          end_curread_block_idx_ = end_preread_block_idx_;
          end_preread_block_idx_ = preread_block_idx_;
        }
      }

      if (OB_SUCCESS == ret 
          && reverse_scan ? preread_block_idx_ > 0 : preread_block_idx_ < block_count_)
      {
        start = reverse_scan ? preread_block_idx_ - 1 : preread_block_idx_;
        end   = reverse_scan ? 0 : block_count_;
        for (i = start; reverse_scan ? (i >= end) : (i < end) && OB_SUCCESS == ret;)
        {
          ObSSTableBlockBufferHandle buffer_handle;
          block_size = block_[i].size_;
          if (check_cache && copy_from_cache_)
          {
            block_size = block_cache_->get_cache_block(sstable_id_, block_[i].offset_,
                                                       block_[i].size_, buffer_handle);
          }
  
          if (block_size == block_[i].size_)
          {
            preread_size += block_[i].size_;
            if (!can_fit_aio_buffer(preread_size))
            {
              if (!reverse_scan)
              {
                end_preread_block_idx_ = i;
              }
              else
              {
                preread_block_idx_ = i + 1;
              }
              if (check_cache && copy_from_cache_)
              {
                //set preread aio buffer block range
                ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
              }
              break;
            }
            else if (check_cache && copy_from_cache_)
            {
              //copy block to preread aio buffer from block cache
              block_[i].cached_ = true;
              aio_stat_.total_cached_size_ += block_[i].size_;
              ret = preread_aio_buf.put_cache_block(buffer_handle.get_buffer(), 
                                                    block_[i].size_, reverse_scan);
            }
          }
          else if (check_cache && copy_from_cache_)
          {
            copy_from_cache_ = false;
            preread_buf_read_size = preread_aio_buf.get_read_size();
            if (0 == preread_buf_read_size)
            {
              //recovery index, redo without copy block from cache
              continue;
            }
            else if (preread_buf_read_size > 0)
            {
              if (!reverse_scan)
              {
                end_preread_block_idx_ = i;
              }
              else
              {
                preread_block_idx_ = i + 1;
              }
              ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
            }
            break;
          }

          //update index
          reverse_scan ? (--i) : (++i);
        }
  
        if (OB_SUCCESS == ret)
        {
          if (!reverse_scan && block_count_ == i)
          {
            end_preread_block_idx_ = block_count_;
          }
          else if (reverse_scan && i < 0)
          {
            preread_block_idx_ = 0;
          }
    
          if (check_cache && copy_from_cache_ && (block_count_ == i || i < 0))
          {
            //set preread aio buffer block range
            ret = set_aio_buf_block_info(preread_buf_idx, check_cache);
          }
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::aio_read_blocks(ObAIOBuffer& aio_buf, ObAIOEventMgr& event_mgr, 
                                        const int64_t aio_buf_idx, int64_t& timeout_us)
    {
      int ret = OB_SUCCESS;

      if (timeout_us <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, timeout_us=%ld", timeout_us);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS == (ret = aio_buf.reset()))
      {
        ret = aio_buf.set_read_info(sstable_id_, 
                                    block_cache_->get_fileinfo_cache(),
                                    &block_[cur_read_block_idx_],
                                    end_curread_block_idx_ - cur_read_block_idx_,
                                    reverse_scan_);
        if (OB_SUCCESS == ret)
        {
          ret = event_mgr.aio_submit(aio_buf.get_fd(), aio_buf.get_file_offset(),
                                     aio_buf.get_toread_size(), aio_buf);
          if (OB_SUCCESS == ret)
          {
            aio_buf.set_state(WAIT);
            cur_buf_idx_ = aio_buf_idx;
            aio_stat_.total_read_size_ += aio_buf.get_toread_size();
            aio_stat_.total_read_times_ += 1;
            aio_stat_.total_read_blocks_ += end_curread_block_idx_ - cur_read_block_idx_;
            ret = event_mgr.aio_wait(timeout_us);
          }
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::preread_blocks()
    {
      int ret                           = OB_SUCCESS;
      int64_t preread_buf_idx           = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      ObAIOBuffer* preread_aio_buf      = &buffer_[preread_buf_idx];
      ObAIOEventMgr* preread_event_mgr  = &event_mgr_[preread_buf_idx];

      //preread if necessary
      if (end_preread_block_idx_ > preread_block_idx_
          && FREE == preread_aio_buf->get_state() && !copy_from_cache_)
      {
        if (OB_SUCCESS == (ret = preread_aio_buf->reset()))
        {
          ret = preread_aio_buf->set_read_info(sstable_id_, 
                                               block_cache_->get_fileinfo_cache(),
                                               &block_[preread_block_idx_], 
                                               end_preread_block_idx_ - preread_block_idx_,
                                               reverse_scan_);
        }

        if (OB_SUCCESS == ret)
        {
          ret = preread_event_mgr->aio_submit(preread_aio_buf->get_fd(), 
                                              preread_aio_buf->get_file_offset(),
                                              preread_aio_buf->get_toread_size(), 
                                              *preread_aio_buf);
          if (OB_SUCCESS == ret)
          {
            aio_stat_.total_read_size_ += preread_aio_buf->get_toread_size();
            aio_stat_.total_read_times_ += 1;
            aio_stat_.total_read_blocks_ += end_preread_block_idx_ - preread_block_idx_;
            preread_aio_buf->set_state(WAIT);
          }
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_free_free(int64_t& timeout_us, bool& stop)
    {
      int ret = OB_SUCCESS;

      if (update_buf_range_)
      {
        cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
        update_buf_range_ = false;
        ret = update_read_range(copy2cache_, reverse_scan_);
      }

      if (OB_SUCCESS == ret)
      {
        if (FREE_FREE != get_state())
        {
          set_state(get_state());
        }
        else
        {
          ret = aio_read_blocks(buffer_[cur_buf_idx_], event_mgr_[cur_buf_idx_], 
                                cur_buf_idx_, timeout_us);
          if (OB_SUCCESS == ret)
          {
            //switch state READY_FREE
            set_state(get_state());
          }
          else
          {
            //aio read timeout or something wrong, stop state machine
            set_stop(stop, ret);
          }
        }
      }
      else
      {
        set_stop(stop, ret);
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_ready_ready(const uint64_t sstable_id,
                                           const int64_t offset,
                                           const int64_t size,
                                           char*& buffer, bool& stop)
    {
      int ret = OB_SUCCESS;

      //ensure the current aio buffer is valid
      if (!buffer_[cur_buf_idx_].is_data_valid(sstable_id, offset, size))
      {
        cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      }

      int64_t preread_buf_idx       = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      ObAIOBuffer* cur_aio_buf      = &buffer_[cur_buf_idx_];
      ObAIOBuffer* preread_aio_buf  = &buffer_[preread_buf_idx];

      if (READY == cur_aio_buf->get_state() 
          && cur_aio_buf->is_data_valid(sstable_id, offset, size))
      {
        ret = cur_aio_buf->get_block(sstable_id, offset, size, buffer);
        if (OB_AIO_BUF_END_BLOCK == ret || OB_AIO_EOF == ret)
        {
          cur_aio_buf->set_state(FREE);
          update_buf_range_ = true;
          if (OB_AIO_BUF_END_BLOCK == ret)
          {
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (!reverse_scan_)
          {
            cur_block_idx_++;
          }
          else
          {
            cur_block_idx_--;
          }

          if (!is_preread_data_valid())
          {
            TBSYS_LOG(WARN, "handle_ready_ready(), ready data is unavailable");
            preread_aio_buf->set_state(FREE);
            //preread if necessary
            ret = preread_blocks();
          }
        }

        set_stop(stop, ret);  //has gotten block, stop state machine
      }
      else
      {
        /**
         * all aio buffer data is invalid, change aio buffer state to 
         * FREE, and switch FREE_FREE state 
         */
        cur_aio_buf->set_state(FREE);
        preread_aio_buf->set_state(FREE);
        set_state(get_state());
        TBSYS_LOG(WARN, "handle_ready_ready(), both ready data is unavailable");
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_wait_wait(int64_t& timeout_us, bool& stop)
    {
      int ret = OB_SUCCESS;

      if (timeout_us <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, timeout_us=%ld", timeout_us);
        set_stop(stop, ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = event_mgr_[cur_buf_idx_].aio_wait(timeout_us);
        if (OB_AIO_TIMEOUT == ret)
        {
          set_stop(stop, ret);
        }
        else if (OB_SUCCESS == ret)
        {
          //one aio buffer state is READY at least, switch to next state to get block
          set_state(get_state());
          ret = OB_SUCCESS;
        }
        else
        {
          set_stop(stop, ret);
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_wait_ready(const uint64_t sstable_id,
                                          const int64_t offset,
                                          const int64_t size,
                                          int64_t& timeout_us, 
                                          char*& buffer, bool& stop)
    {
      int ret = OB_SUCCESS;

      //ensure the current aio buffer is READY
      if (WAIT == buffer_[cur_buf_idx_].get_state())
      {
        cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      }

      int64_t preread_buf_idx           = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      ObAIOBuffer* cur_aio_buf          = &buffer_[cur_buf_idx_];
      ObAIOBuffer* preread_aio_buf      = &buffer_[preread_buf_idx];
      ObAIOEventMgr* preread_event_mgr  = &event_mgr_[preread_buf_idx];

      if (timeout_us <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, timeout_us=%ld", timeout_us);
        set_stop(stop, ret);
        ret = OB_ERROR;
      }
      else
      {
        if (READY == cur_aio_buf->get_state() 
            && cur_aio_buf->is_data_valid(sstable_id, offset, size))
        {
          ret = cur_aio_buf->get_block(sstable_id, offset, size, buffer);
          if (OB_AIO_BUF_END_BLOCK == ret || OB_AIO_EOF == ret)
          {
            cur_aio_buf->set_state(FREE);
            update_buf_range_ = true;
            if (OB_AIO_BUF_END_BLOCK == ret)
            {
              ret = OB_SUCCESS;
            }
          }
  
          if (OB_SUCCESS == ret)
          {
            if (!reverse_scan_)
            {
              cur_block_idx_++;
            }
            else
            {
              cur_block_idx_--;
            }
          }
  
          set_stop(stop, ret);  //has gotten block, stop state machine
        }
        else
        {
          /**
           * current aio buffer data is invalid, change aio buffer state 
           * to FREE, check the preread aio buffer 
           */
          cur_aio_buf->set_state(FREE);
          if (preread_aio_buf->is_data_valid(sstable_id, offset, size))
          {
            TBSYS_LOG(WARN, "handle_wait_ready(), ready data is unavailable");
            //wait aio read return valid data
            ret = preread_event_mgr->aio_wait(timeout_us);
            if (OB_AIO_TIMEOUT == ret || OB_ERROR == ret)
            {
              //aio read timeout, stop state machine 
              set_stop(stop, ret);
            }
            else
            {
              //switch state to READY_FREE
            }
            cur_buf_idx_ = preread_buf_idx; //switch current aio buffer
          }
          else
          {
            //switch state to WAIT_FREE
            TBSYS_LOG(WARN, "handle_wait_ready(), wait and ready data is unavailable");
          }
  
          if (!stop)
          {
            //switch state to WAIT_FREE or READY_FREE
            set_state(get_state());
          }
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_wait_free(const uint64_t sstable_id,
                                         const int64_t offset,
                                         const int64_t size, 
                                         int64_t& timeout_us, 
                                         bool& stop)
    {
      int ret = OB_SUCCESS;

      if (timeout_us <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, timeout_us=%ld", timeout_us);
        set_stop(stop, ret);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && update_buf_range_)
      {
        cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
        update_buf_range_ = false;
        ret = update_read_range(copy2cache_, reverse_scan_);
      }

      if (OB_SUCCESS == ret)
      {
        if (WAIT_FREE != get_state())
        {
          set_state(get_state());
        }
        else
        {
          //ensure the current aio buffer is WAIT
          if (FREE == buffer_[cur_buf_idx_].get_state())
          {
            cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
          }
    
          int64_t preread_buf_idx       = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
          ObAIOBuffer* cur_aio_buf      = &buffer_[cur_buf_idx_];
          ObAIOEventMgr* cur_event_mgr  = &event_mgr_[cur_buf_idx_];
    
          if (WAIT == cur_aio_buf->get_state() 
              && cur_aio_buf->is_data_valid(sstable_id, offset, size))
          {
            //wait aio read return valid data
            ret = cur_event_mgr->aio_wait(timeout_us);
          }
          else
          {
            ret = aio_read_blocks(buffer_[preread_buf_idx], event_mgr_[preread_buf_idx], 
                                  preread_buf_idx, timeout_us);
            TBSYS_LOG(WARN, "handle_wait_free(), wait data is unavailable");
          }
    
          if (OB_SUCCESS == ret)
          {
            set_state(get_state());
          }
          else
          {
            //aio read timeout or something wrong, stop state machine
            set_stop(stop, ret);
          }
        }
      }
      else
      {
        set_stop(stop, ret);
      }

      return ret;
    }

    int ObAIOBufferMgr::handle_ready_free(const uint64_t sstable_id,
                                          const int64_t offset,
                                          const int64_t size,
                                          char*& buffer, bool& stop)
    {
      int ret = OB_SUCCESS;

      if (update_buf_range_)
      {
        cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
        update_buf_range_ = false;
        ret = update_read_range(copy2cache_, reverse_scan_);
      }

      if (OB_SUCCESS == ret)
      {
        if (READY_FREE != get_state())
        {
          set_state(get_state());
        }
        else
        {
          //ensure the current aio buffer is READY
          if (FREE == buffer_[cur_buf_idx_].get_state())
          {
            cur_buf_idx_ = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
          }
    
          ObAIOBuffer* cur_aio_buf  = &buffer_[cur_buf_idx_];
    
          if (READY == cur_aio_buf->get_state() 
              && cur_aio_buf->is_data_valid(sstable_id, offset, size))
          {
            ret = cur_aio_buf->get_block(sstable_id, offset, size, buffer);
            if (OB_AIO_BUF_END_BLOCK == ret || OB_AIO_EOF == ret)
            {
              cur_aio_buf->set_state(FREE);
              update_buf_range_ = true;
              if (OB_AIO_BUF_END_BLOCK == ret)
              {
                ret = OB_SUCCESS;
              }
            }
    
            if (OB_SUCCESS == ret)
            {
              if (!reverse_scan_)
              {
                cur_block_idx_++;
              }
              else
              {
                cur_block_idx_--;
              }
    
              //preread if necessary
              ret = preread_blocks();
            }
    
            set_stop(stop, ret);  //has gotten block, stop state machine
          }
          else
          {
            /**
             * current aio buffer data is invalid, change aio buffer state 
             * to FREE, and switch FREE_FREE state 
             */
            cur_aio_buf->set_state(FREE);
            set_state(get_state());
            TBSYS_LOG(WARN, "handle_ready_free(), ready data is unavailable");
          }
        }
      }
      else
      {
        set_stop(stop, ret);
      }

      return ret;
    }

    int ObAIOBufferMgr::get_block_state_machine(const uint64_t sstable_id,
                                                const int64_t offset,
                                                const int64_t size,
                                                int64_t& timeout_us, 
                                                char*& buffer)
    {
      int ret   = OB_SUCCESS;
      bool stop = false;
      set_state(get_state());

      //assumpt that all the parameters are checked in get_block function
      while (!stop)
      {
        switch (state_)
        {
        case FREE_FREE:
          if (OB_SUCCESS == ret)
          {
            ret = handle_free_free(timeout_us, stop);
          }
          else
          {
            stop = true;
          }
          break;

        case READY_READY:
          if (OB_SUCCESS == ret)
          {
            ret = handle_ready_ready(sstable_id, offset, size, buffer, stop);
          }
          else
          {
            stop = true;
          }
          break;
                    
        case WAIT_WAIT:
          if (OB_SUCCESS == ret)
          {
            ret = handle_wait_wait(timeout_us, stop);
          }
          else
          {
            stop = true;
          }
          break;

        case WAIT_READY:
          if (OB_SUCCESS == ret)
          {
            ret = handle_wait_ready(sstable_id, offset, size, timeout_us, buffer, stop);
          }
          else
          {
            stop = true;
          }
          break;

        case WAIT_FREE:
          if (OB_SUCCESS == ret)
          {
            ret = handle_wait_free(sstable_id, offset, size, timeout_us, stop);
          }
          else
          {
            stop = true;
          }
          break;

        case READY_FREE:
          if (OB_SUCCESS == ret)
          {
            ret = handle_ready_free(sstable_id, offset, size, buffer, stop);
          }
          else
          {
            stop = true;
          }
          break;

        default:
          TBSYS_LOG(WARN, "unexpected internal error");
          stop = true;
          break;
        }
      }

      return ret;
    }

    int ObAIOBufferMgr::copy2cache(const uint64_t sstable_id, 
                                   const int64_t offset, 
                                   const int64_t nbyte, 
                                   const char* buffer)
    {
      int ret = OB_SUCCESS;
      ObSSTableBlockCacheValue value;
      ObSSTableBlockCacheKey dataindex_key;

      value.nbyte_ = nbyte;
      value.buffer_ = const_cast<char*>(buffer);
      dataindex_key.sstable_id_ = sstable_id;
      dataindex_key.offset_ = offset;
      dataindex_key.size_ = nbyte;

      ret = block_cache_->get_kv_cache().put(dataindex_key, value, false);

      return  ret;
    }

    int ObAIOBufferMgr::get_block(ObSSTableBlockCache& block_cache,
                                  const uint64_t sstable_id,
                                  const int64_t offset,
                                  const int64_t size,
                                  const int64_t timeout_us, 
                                  char*& buffer, bool& from_cache,
                                  const bool check_crc)
    {
      int ret                 = OB_SUCCESS;
      int64_t cur_timeout_us  = timeout_us;
      bool copy_to_cache      = false;
      buffer  = NULL;
      from_cache = false;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "aio buffer manager doesn't initialize");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id || offset < 0 || size <= 0
               || timeout_us <= 0 || !can_fit_aio_buffer(size))
      {
        TBSYS_LOG(WARN, "Invalid parameter, sstable_id=%lu, offset=%ld, size=%ld, "
                        "timeout_us=%ld", 
                  sstable_id, offset, size, timeout_us);
        ret = OB_ERROR;
      }
      else if ((!reverse_scan_ && cur_block_idx_ >= block_count_)
               || (reverse_scan_ && cur_block_idx_ < 0))
      {
        TBSYS_LOG(WARN, "current block is out of range in aio buffer, "
                        "reverse_scan_=%d, cur_block_idx_=%ld, block_count_=%ld", 
                  reverse_scan_, cur_block_idx_, block_count_);
        ret = OB_ERROR;
      }
      else if (sstable_id != sstable_id_ || offset != block_[cur_block_idx_].offset_ ||
               size != block_[cur_block_idx_].size_)
      {
        TBSYS_LOG(WARN, "get block not in order, sstable_id=%lu, expected_sstable_id=%lu, "
                        "offset=%ld, expected_offset=%ld, size=%ld, expected_size=%ld", 
                  sstable_id, sstable_id_, offset, block_[cur_block_idx_].offset_,
                  size, block_[cur_block_idx_].size_);
        ret = OB_ERROR;
      }
      else if (get_first_block_)
      {
        ret = init_read_range();
        get_first_block_ = false;
      }

      if (OB_SUCCESS == ret)
      {
        block_cache_ = &block_cache;
        if (copy2cache_)
        {
          //only if the block isn't in block cache, copy it into block cache
          copy_to_cache = !block_[cur_block_idx_].cached_;
          from_cache = block_[cur_block_idx_].cached_;
        }
        ret = get_block_state_machine(sstable_id, offset, size, cur_timeout_us, buffer);
      }

      if (OB_SUCCESS == ret && NULL != buffer)
      {
        if (check_crc && !from_cache)
        {
          ret = ObRecordHeaderV2::check_record(buffer, 
            size, OB_SSTABLE_BLOCK_DATA_MAGIC);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to check block record, sstable_id=%lu "
                            "offset=%ld nbyte=%ld",
                      sstable_id, offset, size);   
          }
        }

        //copy to block cache if necessary
        if (OB_SUCCESS == ret && copy_to_cache)
        {
          //ignore the return value of copy2cache
          copy2cache(sstable_id, offset, size, buffer);
        }
      }

      return ret;
    }

    void ObAIOBufferMgr::copy_remain_block_to_cache(ObAIOBuffer& aio_buf)
    {
      int status    = OB_SUCCESS;
      bool cached_  = false;
      ObSSTableBlockCacheValue value;
      ObSSTableBlockCacheKey dataindex_key;

      /**
       * no matter what this function success or fail, ignore the 
       * return value 
       */
      while (true)
      {
        status = aio_buf.get_next_block(dataindex_key.sstable_id_,
                                        dataindex_key.offset_,
                                        dataindex_key.size_, 
                                        value.buffer_, cached_);
        if (OB_SUCCESS == status)
        {
          //if the block is not in block cache, copy it into block cache
          if (!cached_)
          {
            status = ObRecordHeaderV2::check_record(value.buffer_, 
              dataindex_key.size_, OB_SSTABLE_BLOCK_DATA_MAGIC);
            if (OB_SUCCESS != status)
            {
              TBSYS_LOG(WARN, "failed to check block record, sstable_id=%lu "
                              "offset=%ld nbyte=%ld",
                        dataindex_key.sstable_id_, dataindex_key.offset_, 
                        dataindex_key.size_);
              break;   
            }
            else
            {
              value.nbyte_ = dataindex_key.size_;
              status = block_cache_->get_kv_cache().put(dataindex_key, value);
              if (OB_SUCCESS != status && OB_ENTRY_EXIST != status)
              {
                TBSYS_LOG(WARN, "failed to copy block data to cache, status=%d", status);
                break;
              }
            }
          }
        }
        else
        {
          break;
        }
      }
    }

    int ObAIOBufferMgr::wait_aio_buf_free(int64_t& timeout_us)
    {
      int ret                             = OB_SUCCESS;
      int64_t preread_buf_idx             = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
      ObAIOBuffer& cur_aio_buf            = buffer_[cur_buf_idx_];
      ObAIOBuffer& preread_aio_buf        = buffer_[preread_buf_idx];
      ObAIOBufferState cur_aio_state      = cur_aio_buf.get_state();
      ObAIOBufferState prepread_aio_state = preread_aio_buf.get_state();
      ObAIOEventMgr& cur_event_mgr        = event_mgr_[cur_buf_idx_];
      ObAIOEventMgr& preread_event_mgr    = event_mgr_[preread_buf_idx];

      if (timeout_us > 0 && WAIT == cur_aio_state)
      {
        ret = cur_event_mgr.aio_wait(timeout_us);
      }

      if (OB_AIO_TIMEOUT != ret && timeout_us > 0 && WAIT == prepread_aio_state)
      {
        ret = preread_event_mgr.aio_wait(timeout_us);
      }

      if (OB_SUCCESS == ret)
      {
        if (READY == cur_aio_buf.get_state())
        {
          //copy to cache
          if (copy2cache_)
          {
            copy_remain_block_to_cache(cur_aio_buf);
          }
          cur_aio_buf.set_state(FREE);
        }

        if (READY == preread_aio_buf.get_state())
        {
          //copy to cache
          if (copy2cache_)
          {
            copy_remain_block_to_cache(preread_aio_buf);
          }
          preread_aio_buf.set_state(FREE);
        }
        reset();
      }

      return ret;
    }

    ObThreadAIOBufferMgrArray::ObThreadAIOBufferMgrArray() : item_count_(0)
    {

    }

    ObThreadAIOBufferMgrArray::~ObThreadAIOBufferMgrArray()
    {
      destroy();
    }

    void ObThreadAIOBufferMgrArray::destroy()
    {
      ObAIOBufferMgr* aio_buf_mgr = NULL;

      for (int64_t i = 0; i < item_count_; ++i)
      {
        aio_buf_mgr = item_array_[i].aio_buf_mgr_;
        if (NULL == aio_buf_mgr)
        {
          aio_buf_mgr->~ObAIOBufferMgr();
          item_array_[i].aio_buf_mgr_ = NULL;
        }
        item_array_[i].sstable_id_ = OB_INVALID_ID;
        item_array_[i].table_id_ = OB_INVALID_ID;
      }
    }

    ObAIOBufferMgr* ObThreadAIOBufferMgrArray::create_aio_buf_mgr()
    {
      ObAIOBufferMgr* aio_buf_mgr = NULL;

      void* buffer = alloc_.alloc(sizeof(ObAIOBufferMgr));
      if (NULL != buffer)
      {
        aio_buf_mgr = new (buffer) ObAIOBufferMgr();
        if (NULL == aio_buf_mgr)
        {
          TBSYS_LOG(WARN, "new aio buffer manager failed");
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "failed to allocate memory for aio buffer manager");
        aio_buf_mgr = NULL;
      }

      return aio_buf_mgr;
    }

    ObAIOBufferMgr* ObThreadAIOBufferMgrArray::get_aio_buf_mgr(const uint64_t sstable_id,
                                                               const uint64_t table_id, 
                                                               const bool free_mgr)
    {
      ObAIOBufferMgr* aio_buf_mgr = NULL;
      int64_t i                   = 0;

      //first try to find a exact fit item
      for (i = 0; i < item_count_; ++i)
      {
        if (sstable_id == item_array_[i].sstable_id_ 
            && table_id == item_array_[i].table_id_)
        {
          aio_buf_mgr = item_array_[i].aio_buf_mgr_;
          if (NULL == aio_buf_mgr)
          {
            /**
             * actually, this can't happen, if the thread aio buffer manager 
             * item is assigned, the aio buffer manager isn't NULL. just for 
             * safe 
             */
            TBSYS_LOG(WARN, "aio buffer manager is NULL in thread aio buffer "
                            "manager array, item_count=%ld, index=%ld, "
                            "table_id=%lu",
                      item_count_, i, table_id);
            item_array_[i].sstable_id_ = OB_INVALID_ID;
            item_array_[i].table_id_ = OB_INVALID_ID;
            if (i < item_count_ -1)
            {
              memmove(&item_array_[i], &item_array_[i + 1], 
                      sizeof(ObThreadAIOBufferMgrItem) * (item_count_ - 1 - i));
              --item_count_;
            }
          }
          break;
        }
      }

      /**
       * if need a free aio buffer manager, but doesn't find a exact 
       * fit item, try to find an aio buffer manager in free state 
       */
      if (free_mgr && NULL == aio_buf_mgr)
      {
        for (i = 0; i < item_count_; ++i)
        {
          if (NULL != item_array_[i].aio_buf_mgr_ 
              && FREE_FREE == item_array_[i].aio_buf_mgr_->get_state())
          {
            aio_buf_mgr = item_array_[i].aio_buf_mgr_;
            item_array_[i].sstable_id_ = sstable_id;
            item_array_[i].table_id_ = table_id;
            break;
          }
          else if (NULL == item_array_[i].aio_buf_mgr_)
          {
            /**
             * actually, this can't happen, if the thread aio buffer manager 
             * item is assigned, the aio buffer manager isn't NULL. just for 
             * safe 
             */
            TBSYS_LOG(WARN, "aio buffer manager is NULL in thread aio buffer "
                            "manager array, item_count=%ld, index=%ld, "
                            "table_id=%lu",
                      item_count_, i, table_id);
            item_array_[i].sstable_id_ = OB_INVALID_ID;
            item_array_[i].table_id_ = OB_INVALID_ID;
            if (i < item_count_ -1)
            {
              memmove(&item_array_[i], &item_array_[i + 1], 
                      sizeof(ObThreadAIOBufferMgrItem) * (item_count_ - 1 - i));
              --item_count_;
            }
          }
        }
      }

      //create a new  aio buffer manager if necessary
      if (NULL == aio_buf_mgr)
      {
        if (item_count_ < MAX_THREAD_AIO_BUFFER_MGR)
        {
          aio_buf_mgr = create_aio_buf_mgr();
          if (NULL != aio_buf_mgr)
          {
            item_array_[item_count_].sstable_id_ = sstable_id;
            item_array_[item_count_].table_id_ = table_id;
            item_array_[item_count_].aio_buf_mgr_ = aio_buf_mgr;
            ++item_count_;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "thread aio buffer manager count reach the maximum=%ld, "
                          "item_count=%ld", 
                    OB_MAX_COLUMN_GROUP_NUMBER, item_count_);
          aio_buf_mgr = NULL;
        }
      }
      
      if (free_mgr && NULL != aio_buf_mgr)
      {
        thread_aio_stat_ += aio_buf_mgr->get_aio_stat();
      }

      return aio_buf_mgr;
    }

    int ObThreadAIOBufferMgrArray::wait_all_aio_buf_mgr_free(const int64_t timeout_us)
    {
      int ret                 = OB_SUCCESS;
      int64_t cur_timeout_us  = timeout_us;

      for (int64_t i = 0; 
           i < item_count_ && OB_SUCCESS == ret && cur_timeout_us > 0; ++i)
      {
        ret = item_array_[i].aio_buf_mgr_->wait_aio_buf_free(cur_timeout_us);
        if (OB_SUCCESS == ret)
        {
          item_array_[i].sstable_id_ = OB_INVALID_ID;
        }
      }

      if (OB_AIO_TIMEOUT == ret || cur_timeout_us <= 0)
      {
        TBSYS_LOG(WARN, "wait all thread aio buffer manager free time out");
        ret = OB_AIO_TIMEOUT;
      }
      thread_aio_stat_.reset();

      return ret;
    }

    bool ObThreadAIOBufferMgrArray::check_all_aio_buf_free()
    {
      bool bret = true;

      for (int64_t i = 0; i < item_count_ && bret; ++i)
      {
        bret = item_array_[i].aio_buf_mgr_->check_aio_buf_free();
      }

      return bret;
    }

    void ObThreadAIOBufferMgrArray::add_stat(const ObIOStat& stat)
    {
      thread_aio_stat_ += stat;
    }

    const char* ObThreadAIOBufferMgrArray::get_thread_aio_stat_str()
    {
      static __thread char buffer[512];
      ObIOStat stat = thread_aio_stat_;

      for (int64_t i = 0; i < item_count_; ++i)
      {
        stat += item_array_[i].aio_buf_mgr_->get_aio_stat();
      }

      snprintf(buffer, 512, "read_size=%ld, read_times=%ld, "
                      "read_blocks=%ld, total_blocks=%ld, "
                      "total_size=%ld, cached_size=%ld, sstable_id=%lu",
        stat.total_read_size_, stat.total_read_times_, stat.total_read_blocks_,
        stat.total_blocks_, stat.total_size_, stat.total_cached_size_,
        stat.sstable_id_);

      return buffer;
    }

    int wait_aio_buffer()
    {
      int ret = OB_SUCCESS;
      int status = 0;

      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = 
        GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_COMPACTSSTABLEV2_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if (NULL == aio_buf_mgr_array)
      {
        ret = OB_ERROR;
      }
      else if (OB_AIO_TIMEOUT == (status = 
            aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000)))
      {    
        TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, "
                        "stop current thread");
        ret = OB_ERROR;
      }    

      return ret;
    }

    const char* get_io_stat_str()
    {
      const char* stat_str = "";

      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = 
        GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_COMPACTSSTABLEV2_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if (NULL != aio_buf_mgr_array)
      {
        stat_str = aio_buf_mgr_array->get_thread_aio_stat_str();
      }
  
      return stat_str;
    }

    void add_io_stat(const ObIOStat& stat)
    {
      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = 
        GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_COMPACTSSTABLEV2_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if (NULL != aio_buf_mgr_array)
      {
        aio_buf_mgr_array->add_stat(stat);
      }
    }
  } // end namespace sstable 
} // end namespace oceanbase
