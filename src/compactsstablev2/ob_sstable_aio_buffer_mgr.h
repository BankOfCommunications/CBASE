#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_AIO_BUFFER_MGR_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_AIO_BUFFER_MGR_H_

#include "common/page_arena.h"
#include "common/ob_fileinfo_manager.h"
#include "ob_sstable_aio_event_mgr.h"
#include "ob_sstable_store_struct.h"

namespace oceanbase 
{
  namespace compactsstablev2
  {
    struct ObBlockPositionInfos;
    class ObSSTableBlockCache;
    class ObSSTableBlockBufferHandle;

    enum ObAIOBufferState 
    {
      FREE,
      WAIT,
      READY
    };

    struct ObBlockInfo
    {
      int64_t offset_;    //block offset in sstable
      int64_t size_;      //block size
      bool cached_;       //whether this block is in block cache
    };

    class ObAIOBufferInterface
    {
      public:
        ObAIOBufferInterface()
        {
        }

        virtual ~ObAIOBufferInterface()
        {
        }

        //return internal buffer to store read data
        virtual char* get_buffer() const = 0;

        //callback after aio return
        virtual void aio_finished(const int64_t ret_size, const int ret_code) = 0;
    };
    
    class ObAIOBuffer : public ObAIOBufferInterface
    {
    public:
      static const int64_t DEFAULT_AIO_BUFFER_SIZE  = 1024 * 1024; //1M

    public:
      ObAIOBuffer();
      virtual ~ObAIOBuffer();

      int init();
      int reset();

      inline const int get_fd() const
      {
        return fd_;
      }

      inline const int64_t get_file_offset() const
      {
        return file_offset_;
      }

      //get size to read
      inline const int64_t get_toread_size() const
      {
        return buf_to_read_;
      }

      //get size read in aio buffer
      inline const int64_t get_read_size() const
      {
        return buf_read_;
      }

      inline const ObAIOBufferState get_state() const
      {
        return state_;
      }

      inline void set_state(ObAIOBufferState state)
      {
        state_ = state;
      }

      virtual inline char* get_buffer() const
      {
        return buffer_;
      }

      inline bool is_data_valid(const uint64_t sstable_id,
                                const int64_t offset,
                                const int64_t size)
      {
        return (sstable_id == sstable_id_ 
                && offset == block_[cur_block_idx_].offset_ 
                && size == block_[cur_block_idx_].size_);
      }

      /**
       * when aio returns, this function is called to set the return 
       * size and return error code.
       * 
       * @param ret_size return size by aio
       * @param ret_code return code by aio
       */
      virtual void aio_finished(const int64_t ret_size, const int ret_code);

      /**
       * before use this aio buffer to read data, first call this 
       * function to set read info 
       * 
       * @param sstable_id sstable id of file to read 
       * @param fileinfo_cache file info cache  
       * @param block block array to read
       * @param block_count block count of block array 
       * @param reverse_scan whether get block reversely 
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR
       */
      int set_read_info(uint64_t sstable_id, common::IFileInfoMgr& fileinfo_cache,
                        ObBlockInfo* block, const int64_t block_count,
                        const bool reverse_scan = false);

      /**
       * get block from local buffer if local buffer is ready.
       * 
       * @param sstable_id sstable id of read file
       * @param offset offset of block in file 
       * @param size block size
       * @param buffer the return buffer which includes the data
       * 
       * @return int if success, returns OB_SUCCESS or 
       *         OB_AIO_BUF_END_BLOCK, else returns OB_ERROR or
       *         OB_AIO_EOF
       */
      int get_block(const uint64_t sstable_id, const int64_t offset,
                    const int64_t size, char*& buffer);

      /**
       * traverse all the blocks in aio buffer, it's used to get the 
       * remain block data from aio buffer and then we can copy the 
       * block data in to block cache if necessary 
       * 
       * @param sstable_id [out] sstable id of read file
       * @param offset [out] offset of block in file
       * @param size [out] current block size
       * @param buffer [out] the return buffer which includes the 
       *               block data
       * @param cached_ [out] whether the current block is in block 
       *                cache
       * 
       * @return int if success, returns OB_SUCCESS or 
       *         OB_AIO_BUF_END_BLOCK, else returns OB_ERROR or
       *         OB_AIO_EOF
       */
      int get_next_block(uint64_t& sstable_id, int64_t& offset,
                         int64_t& size, char*& buffer, bool& cached_);

      /**
       * put block data into aio buffer, the block data is getten from 
       * block cache. 
       * 
       * @param buffer block buffer which stores block data from block 
       *               cache
       * @param size block data size 
       * @param reverse_scan whether get block reversely
       * 
       * @return int if success, returns OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int put_cache_block(const char* buffer, const int64_t size, 
                          const bool reverse_scan = false);

      /**
       * after copy block data from block cache, use this function to 
       * set the block range of aio buffer. 
       * 
       * @param sstable_id sstable id of read file
       * @param block block array to read
       * @param block_count block count to read
       * @param reverse_scan whether get block reversely 
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR
       */
      int set_cache_block_info(uint64_t sstable_id, 
                               ObBlockInfo* block, 
                               const int64_t block_count,
                               const bool reverse_scan = false);

      static int64_t lower_align(int64_t input, int64_t align);
      static int64_t upper_align(int64_t input, int64_t align);
      
    private:
      DISALLOW_COPY_AND_ASSIGN(ObAIOBuffer);

      bool inited_;                 //whether aio buffer is initialized
      ObAIOBufferState state_;      //aio buffer state(free, wait, ready)
      bool reverse_scan_;           //reverse to get block

      int32_t fd_;                  //fd of file to read
      uint64_t sstable_id_;         //sstable id of file
      common::IFileInfoMgr* fileinfo_cache_;  //file info cache
      const common::IFileInfo* file_info_;    //current reading file info
      int64_t file_offset_;         //the real aligned offset of file to read

      char* misalign_buf_;          //misalign buffer 
      char* buffer_;                //the real buffer to store data
      int64_t buf_size_;            //buffer size
      int64_t buf_read_;            //how much data is ready in buffer
      int64_t buf_to_read_;         //how much need read
      int64_t buf_pos_;             //current valid data position in buffer

      ObBlockInfo* block_;          //block array which this buffer belong to
      int64_t block_count_;         //block count of block array
      int64_t cur_block_idx_;       //which block is reading in crrent block array
    };

    struct ObIOStat
    {
      ObIOStat()
      {
        reset();
      }

      void reset()
      {
        memset(this, 0, sizeof(ObIOStat));
      }

      ObIOStat& operator += (const ObIOStat& stat)
      {
        total_read_size_ += stat.total_read_size_;
        total_read_times_ += stat.total_read_times_;
        total_read_blocks_ += stat.total_read_blocks_;
        total_blocks_ += stat.total_blocks_;
        total_size_ += stat.total_size_;
        total_cached_size_ += stat.total_cached_size_;
        if (stat.sstable_id_ > 0)
        {
          sstable_id_ = stat.sstable_id_;
        }

        return *this;
      }

      int64_t total_read_size_;
      int64_t total_read_times_;
      int64_t total_read_blocks_;
      int64_t total_blocks_;
      int64_t total_size_;
      int64_t total_cached_size_;
      uint64_t sstable_id_;
    };

    enum ObDoubleAIOBufferState
    {
      FREE_FREE,
      READY_READY,
      WAIT_WAIT,
      WAIT_READY,
      WAIT_FREE,
      READY_FREE
    };
    
    /**
     * each thread has one or more ObAIOBufferMgr instances, each 
     * column group has one ObAIOBufferMgr instance. each instance 
     * has two aio buffers, one aio buffer is used to read the 
     * current blocks data, the other aio buffer is used to preread 
     * the next blocks data. because each thread has one io_context 
     * instance, and each thread detect the state of io_context 
     * instance, the application get block serially, but we can do 
     * preread parallel. 
     */
    class ObAIOBufferMgr 
    {
    public:
      //there are only two aio buffer for each thread
      static const int64_t AIO_BUFFER_COUNT = 2;  

    public:
      ObAIOBufferMgr();
      ~ObAIOBufferMgr();

      int init();
      ObDoubleAIOBufferState get_state() const;

      /**
       * when reading multiple blocks, first call this function to 
       * tell aio buffer which continuous blocks need read. it make 
       * it's easy to preread blocks for aio buffer. and this function 
       * will caculate the blocks to read by current aio buffer and 
       * the blocks to read by preread aio buffer. 
       *  
       * @param block_cache block cache 
       * @param sstable_id sstable id of sstable to read 
       * @param block_infos block position infos array
       * @param copy2cache whether copy block into block cache if the 
       *                   blocks aren't existent in block cache
       * @param reverse_scan whether get block reversely
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR or OB_AIO_BUSY
       */
      int advise(ObSSTableBlockCache& block_cache, 
                 const uint64_t sstable_id, 
                 const ObBlockPositionInfos& block_infos, 
                 const bool copy2cache = false,
                 const bool reverse_scan = false);

      /**
       * get continuous block from aio buffer, the order of blocks to 
       * get must be consistent with the order of blocks which 
       * advise() function specified. and if get_block success, don't 
       * get the same block again, otherwise, get_block() will return 
       * error. 
       *  
       * @param block_cache block cache  
       * @param sstable_id sstable id of sstable file to read
       * @param offset block offset in sstable file
       * @param size block size 
       * @param timeout_us time out in us 
       * @param buffer buffer to return, buffer stores the data to 
       *               read
       * @param from_cache whether the block data is from cache 
       * @param check_crc whether check the block data record 
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR or OB_AIO_TIMEOUT or OB_AIO_EOF
       */
      int get_block(ObSSTableBlockCache& block_cache, 
                    const uint64_t sstable_id,
                    const int64_t offset,
                    const int64_t size,
                    const int64_t timeout_us, 
                    char*& buffer, bool& from_cache,
                    const bool check_crc = true);


      /**
       * wait all the aio buffer are in free state, copy the ready 
       * data to block cache if necessary 
       * 
       * @param timeout_us [in/out] timeout in us, when return, it 
       *                   storee the left timeout in us
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR or OB_AIO_TIMEOUT
       */
      int wait_aio_buf_free(int64_t& timeout_us);

      /**
       * check whether all the aio buffer is in FREE state 
       * 
       * @return bool if success, return true, else return false
       */
      inline bool check_aio_buf_free()
      {
        return (get_state() == FREE_FREE);
      }

      inline const ObIOStat& get_aio_stat() const
      {
        return aio_stat_;
      }

    private:
      inline bool can_fit_aio_buffer(const int64_t size)
      {
        return (size + 2 * common::OB_DIRECT_IO_ALIGN 
                <= ObAIOBuffer::DEFAULT_AIO_BUFFER_SIZE);
      }

      inline bool is_preread_data_valid()
      {
        int64_t preread_buf_idx = (cur_buf_idx_ + 1) % AIO_BUFFER_COUNT;
        int64_t block_index = 
          reverse_scan_ ? end_preread_block_idx_ - 1 : preread_block_idx_;

        return (end_preread_block_idx_ > preread_block_idx_ 
                && buffer_[preread_buf_idx].is_data_valid(sstable_id_, 
                   block_[block_index].offset_, block_[block_index].size_));
      }

      void reset();
      io_context_t get_io_context();
      int ensure_block_buf_space(const int64_t size);
      const char* get_state_str(ObDoubleAIOBufferState state) const;
      void set_state(ObDoubleAIOBufferState state);
      void set_stop(bool& stop, const int ret);
      
      int reset_both_aio_buffer();
      int set_aio_buf_block_info(const int64_t aio_buf_idx, const bool check_cache);
      int cur_aio_buf_add_block(
        bool& cur_range_assign, const int64_t block_idx, 
        const int64_t block_size, const int64_t total_size, 
        const bool check_cache, const bool reverse_scan,
        ObSSTableBlockBufferHandle& buffer_handle);
      int preread_aio_buf_add_block(
        const bool cur_range_assign, bool& preread_range_assign,
        const int64_t block_idx, const int64_t block_size, 
        int64_t& preread_size, const bool check_cache, 
        const bool reverse_scan, ObSSTableBlockBufferHandle& buffer_handle);
      int do_first_block_noexist_in_cache(
        bool& cur_range_assign, bool& preread_range_assign,
        bool& check_cache, const int64_t block_idx, 
        const bool reverse_scan);
      int do_all_blocks_in_cache(const bool cur_range_assign, 
                                 const bool preread_range_assign,
                                 const bool check_cache, const bool reverse_scan);
      void do_either_aio_buf_assigned(const int64_t total_size, const bool reverse_scan);
      void do_only_cur_aio_buf_assigned(const bool check_cache, const bool reverse_scan);
      int internal_init_read_range(const bool read_cache = false, 
                                   const bool reverse_scan = false);
      int init_read_range();
      int update_read_range(const bool check_cache, const bool reverse_scan);

      int preread_blocks();
      int aio_read_blocks(ObAIOBuffer& aio_buf, ObAIOEventMgr& event_mgr, 
                          const int64_t aio_buf_idx, int64_t& timeout_us);

      int handle_free_free(int64_t& timeout_us, bool& stop);
      int handle_ready_ready(const uint64_t sstable_id, const int64_t offset,
                             const int64_t size, char*& buffer, bool& stop);
      int handle_wait_wait(int64_t& timeout_us, bool& stop);
      int handle_wait_ready(const uint64_t sstable_id, const int64_t offset,
                            const int64_t size, int64_t& timeout_us, 
                            char*& buffer, bool& stop);
      int handle_wait_free(const uint64_t sstable_id, const int64_t offset,
                           const int64_t size, int64_t& timeout_us, bool& stop);
      int handle_ready_free(const uint64_t sstable_id, const int64_t offset,
                            const int64_t size, char*& buffer, bool& stop);
      int get_block_state_machine(const uint64_t sstable_id, const int64_t offset,
                                  const int64_t size, int64_t& timeout_us, 
                                  char*& buffer);

      int copy2cache(const uint64_t sstable_id, const int64_t offset,
                     const int64_t nbyte, const char* buffer);
      void copy_remain_block_to_cache(ObAIOBuffer& aio_buf);
  
    private:
      static const int64_t DEFAULT_BLOCK_BUF_SIZE = 10000 * sizeof(ObBlockInfo);
      static const int64_t MIN_PREREAD_SIZE = 512 * 1024; //512k

      DISALLOW_COPY_AND_ASSIGN(ObAIOBufferMgr);

      bool inited_;                   //whether it's initialized
      ObDoubleAIOBufferState state_;  //double aio buffer state
      bool reverse_scan_;             //reverse to get block
      bool copy2cache_;               //whether copy block to block cache
      bool get_first_block_;          //whether it gets the first block
      bool copy_from_cache_;          //whether copy block from block cache
      bool update_buf_range_;         //whether update aio buffer read range

      uint64_t sstable_id_;           //sstable id to read
      ObSSTableBlockCache* block_cache_;     //block cache

      ObBlockInfo* block_;            //store block collection to read
      int64_t block_buf_size_;        //block buffer size 
      int64_t block_count_;           //block count in block array to read
      int64_t cur_block_idx_;         //current reading block index in block array
      int64_t cur_read_block_idx_;    //start block index for current aio buffer
      int64_t end_curread_block_idx_; //end block index for current aio buffer
      int64_t preread_block_idx_;     //start block index to preread
      int64_t end_preread_block_idx_; //end block index to preread

      ObAIOBuffer buffer_[AIO_BUFFER_COUNT];      //two aio buffer
      ObAIOEventMgr event_mgr_[AIO_BUFFER_COUNT]; //two aio event manager
      int64_t cur_buf_idx_;           //current aio buffer to use to read block

      ObIOStat aio_stat_;
    };

    class ObThreadAIOBufferMgrArray
    {
    public:
      ObThreadAIOBufferMgrArray();
      ~ObThreadAIOBufferMgrArray();

      ObAIOBufferMgr* get_aio_buf_mgr(const uint64_t sstable_id, 
                                      const uint64_t table_id, 
                                      const bool free_mgr = false);
      int wait_all_aio_buf_mgr_free(const int64_t timeout_us);
      bool check_all_aio_buf_free();
      void add_stat(const ObIOStat& stat);
      const char* get_thread_aio_stat_str();

    private:
      struct ObThreadAIOBufferMgrItem
      {
        ObThreadAIOBufferMgrItem() 
        : sstable_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID), 
          column_group_id_(common::OB_INVALID_ID), aio_buf_mgr_(NULL)
        {

        }

        uint64_t sstable_id_;
        uint64_t table_id_;
        uint64_t column_group_id_;
        ObAIOBufferMgr* aio_buf_mgr_;
      };

    private:
      ObAIOBufferMgr* create_aio_buf_mgr();
      void destroy();

    private:
      static const int64_t MAX_THREAD_AIO_BUFFER_MGR = 
        common::OB_MAX_COLUMN_GROUP_NUMBER * common::OB_MAX_THREAD_READ_SSTABLE_NUMBER;

      DISALLOW_COPY_AND_ASSIGN(ObThreadAIOBufferMgrArray);
      ObThreadAIOBufferMgrItem item_array_[MAX_THREAD_AIO_BUFFER_MGR];
      int64_t item_count_;
      common::PageArena<char> alloc_;
      ObIOStat thread_aio_stat_;
    };

    int wait_aio_buffer();
    void add_io_stat(const ObIOStat& stat);
    const char* get_io_stat_str();
  } // namespace oceanbase::sstable
} // namespace Oceanbase
#endif //OCEANBASE_SSTABLE_OB_AIO_BUFFER_MGR_H_
