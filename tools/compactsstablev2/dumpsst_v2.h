#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "compactsstablev2/ob_sstable_block_reader.h"
#include "compactsstablev2/ob_sstable_block_index_mgr.h"
#include "ob_fileinfo_cache.cpp"
#include "common/file_utils.h"
#include <getopt.h>

namespace oceanbase
{
  namespace compactsstablev2
  {
    struct CmdLineParam
    {
      const char *cmd_type_;
      const char *idx_file_name_;
      const char *dump_content_;
      int32_t block_id_;
      int32_t block_n_;//if not 0 , from block_id to block_n
      int32_t disk_no_;
      int64_t table_version_;
      const char *search_range_;
      const char *new_range_;
      int64_t table_id_;
      const char *app_name_;
      int32_t hex_format_;
      int32_t quiet_;
      int64_t file_id_;
      int64_t dst_file_id_;
      const char *compressor_name_;
      const char *output_sst_dir_;
      const char *schema_file_;

      CmdLineParam()
        : cmd_type_(NULL),
          idx_file_name_(NULL),
          dump_content_(NULL),
          block_id_(0),
          block_n_(0),
          disk_no_(0),
          table_version_(0),
          search_range_(NULL),
          new_range_(NULL),
          table_id_(0),
          app_name_(NULL),
          hex_format_(0),
          quiet_(0),
          file_id_(0),
          dst_file_id_(0),
          compressor_name_(NULL),
          output_sst_dir_(NULL),
          schema_file_(NULL)
      {
      }
    };

    class DumpSSTableV2
    {
    public:
      DumpSSTableV2()
        : opened_(false),
          reader_(fileinfo_cache_),
          block_index_mgr_(NULL)

      {
        fileinfo_cache_.init(1024);
      }

      ~DumpSSTableV2()
      {
        const int64_t table_cnt = reader_.get_table_cnt();
        for (int64_t i = 0; i < table_cnt; i ++)
        {
          free(block_index_mgr_[i]);
        }
      }

      int open(const int64_t sstable_file_id);

      int load_block_index();

      int load_display_block(const ObSSTableBlockIndexMgr& index,
          const int64_t block_id);

      int load_display_blocks(const uint64_t table_id,
          const int64_t start_block_id,
          const int64_t end_block_id);

      int display_sstable_size();

      int display_sstable_trailer();

      int display_sstable_schema();

      int display_table_index();

      int display_table_bloomfilter();

      int display_table_range();

      int display_block_index();

      void display_block_index_array(ObBlockPositionInfos& pos_info);

    private:
      bool opened_;
      common::FileUtils util_;
      chunkserver::FileInfoCache fileinfo_cache_;
      ObCompactSSTableReader reader_;
      ObSSTableBlockIndexMgr** block_index_mgr_;
      //ObSSTableBlockIndex** block_index_array_;
      //char** block_endkey_array_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase

