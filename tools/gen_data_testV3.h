/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * gen_data_testV3.h is for what ...
 *
 * Version: ***: gen_data_testV3.h  Sat Feb 18 10:06:35 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm<fangji.hcm@taobao.com>
 *          -some work detail if you want 
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_GEN_DATA_TESTV3_H_
#define OCEANBASE_CHUNKSERVER_GEN_DATA_TESTV3_H_

#include <map>
#include <string.h>
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "chunkserver/ob_tablet.h"
#include "chunkserver/ob_tablet_image.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_disk_path.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::sstable;
    struct Args
    {
      int64_t disk_num_;   //number of disk the data distrbute on
      int64_t block_size_; //number of kbytes of data block
      char *schema_file_;  //schema file path
      char *data_dest_;    //data dir
    };

    class GenDataTestV3 {
    public:
      static const int64_t MAX_KEY_LEN = 1024;
      static const int64_t MAX_PATH_LEN = 512;
    public:
      GenDataTestV3();
      int init(Args* arg);
      int generate();

      typedef int (GenDataTestV3::*generate_table)();

    private:
      int create_sstable(uint64_t table_id, int32_t disk_no);
      int close_sstable(bool set_min = false, bool set_max = false);

      const common::ObString& get_compressor() const;
      int fill_sstable_schema(const common::ObSchemaManagerV2& schema,uint64_t table_id,ObSSTableSchema& sstable_schema);      

      int generate_table_search();
      int generate_table_campaign();
      int generate_table_cust();
      int generate_table_jdatetime();
      int generate_table_jvarchar();
      int generate_table_jint();
      

      int fill_row_search(uint64_t group_id, int32_t index);
      int fill_row_campaign(uint64_t group_id, int32_t index);
      int fill_row_cust(uint64_t group_id, int32_t index);
      int fill_row_jdatetime(uint64_t group_id, int32_t index);
      int fill_row_jvarchar(uint64_t group_id, int32_t index);
      int fill_row_jint(uint64_t group_id, int32_t index);

      int gen_rowkey_search();
      int gen_rowkey_campaign();
      int gen_rowkey_cust();
      int gen_rowkey_join(int32_t index);
      int gen_rowkey_joinint(int32_t index);

    public:
      void set_rowkey_var_search(int32_t index);
      void set_rowkey_var_campaign(int32_t index);
      void set_rowkey_var_cust(int32_t index);
      void set_rowkey_var_join(int32_t index);
      void reset_rowkey_var();
    private:
        //rowkey member v3
      int64_t customid_;
      int64_t thedate_;
      int64_t campaignid_;
      int64_t adgroupid_;
      int64_t bidwordid_;
      int64_t network_;
      char     matchscope_;
      const char* searchtype_;
      const char* searchtypereverse_;
      int64_t creativeid_;
      int64_t isshop_;

      int64_t curr_sstable_id_;
      int64_t curr_sstable_size_;
      ObSSTableId sstable_id_;
      ObSSTableSchema table_schema_;
      ObSSTableWriter writer_;
      ObSSTableRow sstable_row_;
      ObTabletImage* tablet_image_[10];

      uint64_t table_id_; //current table id
      int32_t disk_no_;  //current disk No.
      int32_t str_index_; //str data cursor
      int32_t char_index_; //char data cursor

      common::ObNewRange range_;
      Args* arg_;
      const common::ObSchemaManagerV2 *schema_mgr_;

      common::ObRowkey curr_rowkey_;
      common::ObRowkey last_sstable_end_key_;//record last key of last sstable;
      common::CharArena rowkey_allocator_;
      common::ObObj rowkey_object_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      common::ObObj start_key_object_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      //char last_key_buf_[MAX_KEY_LEN];
      //char rowkey_buf_[MAX_KEY_LEN];
      //char start_key_buf_[MAX_KEY_LEN];

      common::ObString compressor_name_;
      char compressor_buf_[MAX_KEY_LEN];
      
      common::ObString data_file_;
      char data_file_buf_[MAX_PATH_LEN];

      std::map<int32_t, generate_table>generate_table_; //table function map
    };
  }//namespace chunkserver
}//namespace oceanbase
#endif
