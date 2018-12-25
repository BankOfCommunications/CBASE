/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_sstable_writer.cpp for test sstable writer
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "common/file_utils.h"
#include "sstable/ob_sstable_writer.h"
#include "file_directory_utils.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_row.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      #define COMPRESSOR_NAME "none"
      
      static char sstable_path[OB_MAX_FILE_NAME_LENGTH];
      static const int64_t table_id = 128;
      static const int64_t column_scheme_def_size = 100;
      static const int64_t sstable_id = 1026;
      static const int32_t objs_per_row = 3;
      static const int64_t TABLE_COUNT = 4;
      static const int64_t ROWS_PER_TABLE = 5000;
      static const uint64_t TABLE_ID_BASE = 1025;
      
      void test_write_one_file()
      {
        ObSSTableSchemaColumnDef column_def;
        ObSSTableSchema schema;
        ObSSTableRow row;
        char *compressor_name = (char*)COMPRESSOR_NAME;
        ObSSTableWriter writer;
        ObString file_name(static_cast<int32_t>(strlen(sstable_path) + 1),
                      static_cast<int32_t>(strlen(sstable_path) + 1), sstable_path);
        ObString compressor(static_cast<int32_t>(strlen(compressor_name) + 1),
                            static_cast<int32_t>(strlen(compressor_name) + 1), compressor_name);
        int64_t disk_usage = 0;
        int64_t trailer_offset = 0;
        ObObj obj;
        ObObj key_obj;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        char value_data[1024 + 1];
        char rowkey_str[32];
        ObString row_key;
        ObRowkey key;
        char *ptr;
        int ret;
      
        // init schema
        for (int64_t i = 0; i < 5; ++i)
        {
          column_def.table_id_ = static_cast<uint32_t>(1025 + i);

          // add rowkey column
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = 1;
          column_def.column_value_type_ = ObVarcharType;
          column_def.rowkey_seq_ = 1;
          schema.add_column_def(column_def);

          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObDoubleType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
      
            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
      
            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
          }
        }
      
        //build data
        ptr = value_data;
        for (int64_t i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);
        writer.set_dio(true);
      
        int64_t start_time = tbsys::CTimeUtil::getTime();
        // create sstable file
        if (OB_ERROR == (ret = writer.create_sstable(schema, file_name,
                                  compressor, 2)))
        {
          TBSYS_LOG(ERROR, "Failed to create sstable file: %s", sstable_path);
        }
      
        for (int64_t i = 0; i < 5000000; ++i) 
        {
          row.clear();
          table_id = i / 1000000 + 1025;
          column_group_id = i % 1000000 / 100000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          sprintf(rowkey_str, "row_key_%08ld", i);
          row_key.assign(rowkey_str, 16);
          key_obj.set_varchar(row_key);
          key.assign(&key_obj, 1);
          row.set_rowkey(key);
      
          obj.set_double((double)i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);
      
          if (OB_ERROR == (ret = writer.append_row(row, disk_usage)))
          {
            TBSYS_LOG(ERROR, "add row failed, i=%ld", i);
            return;
          }
        }
      
        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset)))
        {
          TBSYS_LOG(ERROR, "close sstable failed ------------------");
        }
      
        printf("test_write_one_file, run_time=%ld\n", 
          tbsys::CTimeUtil::getTime() - start_time);
        //remove(sstable_path);
      }
      
      void test_write_patch_file_with_check()
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        int64_t sstable_size = 0;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        ObObj key_obj;
        ObObj obj;
        ObString row_key;
        ObRowkey key;
        char value_data[1024 + 1];
        char rowkey_str[32];
        char *ptr;
        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        // init schema
        for (int64_t i = 0; i < 5; ++i)
        {
          column_def.table_id_ = static_cast<uint32_t>(1025 + i);

          // add rowkey column
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = 1;
          column_def.column_value_type_ = ObVarcharType;
          column_def.rowkey_seq_ = 1;
          schema.add_column_def(column_def);

          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObDoubleType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
      
            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
      
            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            column_def.rowkey_seq_ = 0;
            schema.add_column_def(column_def);
          }
        }
      
        //build data
        ptr = value_data;
        for (int64_t i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);
        writer.set_dio(true);
       
        int64_t start_time = tbsys::CTimeUtil::getTime();
        ret = writer.create_sstable(schema, file_name, compressor, 2, 
                                    OB_SSTABLE_STORE_SPARSE);
      
        for (int64_t i = 0; i < 5000000; ++i) {
          row.clear();
          table_id = i / 1000000 + 1025;
          column_group_id = i % 1000000 / 100000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          sprintf(rowkey_str, "row_key_%08ld", i);
          row_key.assign(rowkey_str, 16);          
          key_obj.set_varchar(row_key);
          key.assign(&key_obj, 1);
          row.set_rowkey(key);
      
          obj.set_double((double)i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);
      
          if (OB_ERROR == (ret = writer.append_row(row, space_usage)))
          {
            TBSYS_LOG(ERROR, "add row failed");
            return;
          }
        }
      
        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset, sstable_size)))
        {
          TBSYS_LOG(ERROR, "close sstable failed ------------------");
        }
      
        printf("test_write_patch_file_with_check(), run_time=%ld\n", 
          tbsys::CTimeUtil::getTime() - start_time);
        //remove(sstable_path);
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

using namespace oceanbase::tests::sstable;
int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  ObSSTableId sst_id(sstable_id);
  get_sstable_path(sst_id, sstable_path, OB_MAX_FILE_NAME_LENGTH);

//char* buf1 = (char*)malloc(1024 * 1024);
//char* buf2 = (char*)malloc(1024 * 1024);
//
//int64_t start_time = tbsys::CTimeUtil::getTime();
//for (int64_t i = 0; i < 100000; ++i)
//{
//  memset(buf1, 0xFF, 1024 * 1024);
//}
//printf("memset, run_time=%ld\n", tbsys::CTimeUtil::getTime() - start_time);
//
//start_time = tbsys::CTimeUtil::getTime();
//for (int64_t i = 0; i < 100000; ++i)
//{
//  memcpy(buf2, buf1, 1024 * 1024);
//}
//printf("memcpy, run_time=%ld\n", tbsys::CTimeUtil::getTime() - start_time);

  //test_write_one_file();
  test_write_patch_file_with_check();
}
