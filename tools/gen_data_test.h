/*
 * (C) 2007-2010 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * gen_data_test.h is for what ...
 *
 * Version: ***: gen_data_test.h  Thu Apr 14 16:20:33 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want 
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_GEN_DATA_TEST_H_
#define OCEANBASE_CHUNKSERVER_GEN_DATA_TEST_H_
#include <errno.h>
#include <string.h>
#include <map>
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_range2.h"
#include "common/page_arena.h"
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
    struct TableArgs
    {
      int32_t table_id_;
      int32_t row_count_;
      int32_t disk_no_;
      int32_t data_type_;
      int32_t sstable_num_;
      int64_t block_size_;
      int64_t start_uid_;
      int64_t start_item_id_;
      const char* schema_file_;
      const char* data_dest_;
      const char* data_file_;
      bool    set_max_;
      bool    set_min_;
    };
    
    class GenDataTest
    {
    public:
      static const int64_t MAX_KEY_LEN     = 1024;
      static const int64_t SSTABLE_SIZE    = 256 * 1024 * 1024; //256M per sstable
      static const int64_t MAX_PATH        = 256;
      static const int64_t SSTABLE_ID_BASE = 1000;
      static const int64_t FIR_MULTI       = 256;
      static const int64_t SEC_MULTI       = 100;
      static const int32_t ARR_SIZE        = 62;
      static const int32_t JOIN_DIFF       = 9;
      static const int32_t ITEM_PER_USER   = 2000;
      static const int32_t ITEM_SIZE       = 100000000;
      static const int64_t crtime          = 1289915563;
      static const int64_t MICRO_PER_SEC   = 1000000;
      
    public:
      GenDataTest();
      int init(TableArgs * args, int32_t table_num);
      int generate();
      
      //function pointer
      typedef int (GenDataTest::*fill_row_func)(uint64_t);
      
    private:
      int create_sstable(uint64_t table_id);
      int finish_sstable(bool set_max = false);
      int fill_sstable_schema(const common::ObSchemaManagerV2& schema,uint64_t table_id,ObSSTableSchema& sstable_schema);      
      const common::ObString& get_compressor() const;
      int fill_row_press_info(uint64_t group_id);
      int fill_row_recovery_info(uint64_t group_id);
      int fill_row_press_item(uint64_t group_id);
      int fill_row_recovery_item(uint64_t group_id);
      int fill_row_with_data_file(uint64_t group_id, char* line);

      int gen_rowkey_press_info();
      int gen_rowkey_press_item();
      int gen_rowkey_recovery_info();
      int gen_rowkey_recovery_item();
      
    private:
      //rowkey member
      int64_t user_id_;
      int64_t item_type_;
      int64_t item_id_;

      bool first_row_disk_;
      bool first_row_sstable_;
      int32_t table_index_;
      int64_t curr_sstable_id_;
      int64_t curr_sstable_size_;
      ObSSTableId sstable_id_;
      ObSSTableSchema table_schema_;
      ObSSTableWriter writer_;
      ObSSTableRow sstable_row_;
      ObTabletImage tablet_image_;
      common::ObNewRange range_;
      TableArgs * table_args_;
      int32_t table_num_;
      const common::ObSchemaManagerV2 *schema_mgr_;

      common::ObRowkey curr_rowkey_;
      common::ObRowkey last_sstable_end_key_;
      common::CharArena rowkey_allocator_;
      common::ObObj rowkey_object_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      common::ObObj start_key_object_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      char varchar_buf_[MAX_KEY_LEN];

      std:: map<int32_t, fill_row_func> func_table_;
      common::ObString sstable_file_name_;
      char dest_file_[MAX_PATH];
      char compressor_name_[MAX_PATH];
      common::ObString compressor_string_;
    };
  }//namespace chunkserver
}//namespace oceanbase
#endif
