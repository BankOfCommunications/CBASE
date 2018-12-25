#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_READER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_READER_H_
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_fileinfo_manager.h"
#include "common/bloom_filter.h"
#include "common/page_arena.h"
#include "common/ob_file.h"
#include "common/hash/ob_hashmap.h"
#include "common/hash/ob_hashutils.h"
#include "common/compress/ob_compressor.h"
#include "common/ob_range2.h"
#include "common/ob_record_header_v2.h"
#include "common/ob_compact_store_type.h"
#include "common/ob_mod_define.h"
#include "sstable/ob_sstable_reader_i.h"
#include "ob_sstable_disk_path.h"
#include "ob_sstable_store_struct.h"
#include "ob_sstable_schema_cache.h"
#include "ob_sstable_schema.h"

class TestCompactSSTableReader_construct_Test;
class TestCompactSSTableReader_init_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactSSTableReader : public sstable::SSTableReader
    {
      class ObFileInfo : public common::IFileInfo
      {
      private:
        static const int FILE_OPEN_NORMAL_RFLAG = O_RDONLY;
        static const int FILE_OPEN_DIRECT_RFLAG = O_RDONLY | O_DIRECT;

      public:
        ObFileInfo()
        {
          fd_ = -1;
        }

        virtual ~ObFileInfo()
        {
          destroy();
        }

        int init(const char* sstable_fname, const bool use_dio = true);

        inline void destroy()
        {
          if (-1 != fd_)
          {
            close(fd_);
            fd_ = -1;
          }
        }

        inline virtual int get_fd() const
        {
          return fd_;
        }

      private:
        int fd_;
      };

    public:
      friend class ::TestCompactSSTableReader_construct_Test;
      friend class ::TestCompactSSTableReader_init_Test;

    private:
      typedef common::hash::ObHashMap<uint64_t, int64_t,
              common::hash::NoPthreadDefendMode> HashMap;

    public:
      static const int64_t TRAILER_OFFSET_SIZE
        = static_cast<int64_t>(sizeof(ObSSTableTrailerOffset));
      static const int64_t SSTABLE_HEADER_SIZE
        = static_cast<int64_t>(sizeof(ObSSTableHeader));

    public:
      ObCompactSSTableReader(common::IFileInfoMgr& fileinfo_cache)
        : is_inited_(false),
          fileinfo_cache_(fileinfo_cache),
          sstable_size_(0),
          sstable_id_(0),
          table_index_(NULL),
          table_range_(NULL),
          bloom_filter_(NULL),
          sstable_schema_(NULL),
          enable_bloomfilter_(false),
          compressor_(NULL),
          allocator_(common::ObModIds::OB_CS_SSTABLE_READER),
          arena_(common::ModuleArena::DEFAULT_PAGE_SIZE, allocator_)
      {
        memset(&sstable_header_, 0, sizeof(sstable_header_));
      }


      ~ObCompactSSTableReader()
      {
        reset();
      }

      int init(const uint64_t sstable_id);
      inline int open(const int64_t sstable_id, int64_t version) { UNUSED(version); return init(sstable_id); }

      void reset();
      inline const common::ObNewRange& get_range() const { return table_range_[0]; }
      inline int64_t get_row_count() const
      {
        int64_t count = 0;
        if (is_inited_)
        {
          count = table_index_[0].row_count_;
        }
        return count;
      }

      inline int64_t get_sstable_checksum() const
      {
        int64_t checksum = 0;
        if (is_inited_)
        {
          checksum = sstable_header_.checksum_;
        }
        return checksum;
      }

      inline int64_t get_sstable_size() const
      {
        int64_t size = 0;
        if (is_inited_)
        {
          size = sstable_size_;
        }
        return size;
      }

      inline const ObSSTableSchema* get_schema() const
      {
        return sstable_schema_;
      }

      inline const ObSSTableHeader* get_sstable_header() const
      {
        return &sstable_header_;
      }

      inline common::ObCompactStoreType get_row_store_type() const
      {
        common::ObCompactStoreType row_store_type;
        row_store_type = static_cast<common::ObCompactStoreType>(
            sstable_header_.row_store_type_);
        return row_store_type;
      }

      inline uint64_t get_sstable_id() const
      {
        return sstable_id_;
      }

      inline const int64_t get_table_cnt() const
      {
        return sstable_header_.table_count_;
      }

      inline const common::ObNewRange* get_table_range() const
      {
        return table_range_;
      }

      inline int64_t get_row_count(const uint64_t table_id) const
      {
        int64_t ret = 0;
        int64_t table_count = sstable_header_.table_count_;
        if (is_inited_)
        {
          for (int64_t i = 0; i < table_count; i ++)
          {
            if (table_index_[i].table_id_ == table_id)
            {
              ret = table_index_[i].row_count_;
              break;
            }
          }
        }
        return ret;
      }

      inline const ObSSTableTableIndex* get_table_index(
          const uint64_t table_id) const
      {
        ObSSTableTableIndex* index = NULL;
        int64_t table_count = sstable_header_.table_count_;
        for (int64_t i = 0; i < table_count; i ++)
        {
          if (table_index_[i].table_id_ == table_id)
          {
            index = &table_index_[i];
            break;
          }
        }
        return index;
      }

      inline const ObSSTableTableIndex* get_table_index() const
      {
        const ObSSTableTableIndex* ret = table_index_;
        return ret;
      }

      inline const common::BloomFilter*  get_bloom_filter() const
      {
        if (sstable_header_.table_count_ > 0)
        {
          return &bloom_filter_[0];
        }
        return NULL;
      }

      inline const common::ObBloomFilterV1* get_table_bloomfilter(
          const uint64_t table_id) const
      {
        common::ObBloomFilterV1* bloom_filter = NULL;               
        int64_t table_count = sstable_header_.table_count_;
        for (int64_t i = 0; i < table_count; i ++)
        {
          if (table_index_[i].table_id_ == table_id)
          {
            bloom_filter = &bloom_filter_[i];
            break;
          }
        }
        return bloom_filter;
      }

      ObCompressor* get_decompressor();

    private:
      static int fetch_sstable_size(const common::IFileInfo& file_info,
          int64_t& sstable_size);

      static int load_trailer_offset(const common::IFileInfo& file_info,
          const int64_t sstable_size,
          ObSSTableTrailerOffset& trailer_offset);

      static int read_record(const common::IFileInfo& file_info,
          const int64_t offset,
          const int64_t size,
          const char*& out_buf);

      int load_trailer(const common::IFileInfo& file_info,
          ObSSTableHeader& sstable_header,
          const ObSSTableTrailerOffset& trailer_offset,
          const int64_t sstable_size);

      int load_schema(const common::IFileInfo& file_info, 
          const ObSSTableHeader& sstable_header,
          ObSSTableSchema*& schema);

      int load_table_index(const common::IFileInfo& file_info, 
          const ObSSTableHeader& sstable_header,
          ObSSTableTableIndex*& table_index);

      int load_table_bloom_filter(const common::IFileInfo& file_info, 
          const ObSSTableHeader& sstable_header,
          const ObSSTableTableIndex* table_index,
          common::ObBloomFilterV1*& bloom_filter);

      int load_table_range(const common::IFileInfo& file_info, 
          const ObSSTableHeader& sstable_header,
          const ObSSTableTableIndex* table_index,
          common::ObNewRange*& table_range);

      inline static ObSSTableSchemaCache& get_sstable_schema_cache()
      {
        static ObSSTableSchemaCache s_schema_cache;
        return s_schema_cache;
      }

      int trans_to_sstable_schema(ObSSTableSchema& schema,
          const ObSSTableTableSchemaItem* item_array, int64_t item_count);

      int trans_to_table_range(common::ObNewRange& range,
          const common::ObCompactStoreType store_type, const char* buf,
          const int64_t start_key_length, const int64_t end_key_length);

    private:
      //file
      bool is_inited_;
      common::IFileInfoMgr& fileinfo_cache_;
      int64_t sstable_size_;
      uint64_t sstable_id_;

      //table
      ObSSTableTableIndex* table_index_;
      common::ObNewRange* table_range_;
      common::ObBloomFilterV1* bloom_filter_;

      //sstable
      ObSSTableSchema* sstable_schema_;
      ObSSTableHeader sstable_header_;

      //table id--->table index, bloomfilter, table range
      HashMap tableid_tablepos_hashmap_;

      //config
      bool enable_bloomfilter_;
      ObCompressor* compressor_;

      //alloc
      common::ModulePageAllocator allocator_;
      common::ModuleArena arena_;
    };//end class ObCompactSSTableReader
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif
