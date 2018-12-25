/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_sstable_reader.cpp is for what ...
 *
 *  Authors:
 *     qushan<qushan@taobao.com>
 *        
 */
#include <gtest/gtest.h>
#include "common/file_directory_utils.h"
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_row.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_schema.h"
#include "common/ob_schema_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

static const char* g_key[] = { "aoo", "boo", "coo", "doo", "foo", "koo" };
const int64_t TABLE_ID = 1;

void create_row_key(
    CharArena& allocator,
    ObRowkey& rowkey,
    const char* sk)
{
  int32_t sz = static_cast<int32_t>(strlen(sk));
  char* msk = allocator.alloc(sz);
  memcpy(msk, sk, sz);
  char* obj_ptr = allocator.alloc(sizeof(ObObj));
  ObObj* obj = new (obj_ptr) ObObj();
  ObString str_obj(sz, sz, msk);
  obj->set_varchar(str_obj);
  rowkey.assign(obj, 1);
}

void create_row(
    CharArena& allocator,
    ObSSTableRow& row,
    const char* key,
    int64_t f1,
    double f2,
    char* f3
    )
{
  ObRowkey row_key;
  create_row_key(allocator, row_key, key);
  row.set_rowkey(row_key);
  ObObj obj1;
  obj1.set_int(f1);
  ObObj obj2;
  obj2.set_double(f2);
  ObObj obj3;
  ObString temp;
  temp.assign_ptr(f3, static_cast<int32_t>(strlen(f3)));
  obj3.set_varchar(temp);

  row.set_column_group_id(0);
  row.set_table_id(TABLE_ID);
  row.add_obj(obj1);
  row.add_obj(obj2);
  row.add_obj(obj3);
}

void create_schema(ObSSTableSchema& schema)
{
  ObSSTableSchemaColumnDef def0, def1, def2, def3;
  def0.column_name_id_ = 2;
  def0.column_value_type_ = ObVarcharType;
  def0.column_group_id_ = 0;
  def0.rowkey_seq_ = 1;
  def0.table_id_ = TABLE_ID;

  def1.column_name_id_ = 5;
  def1.column_value_type_ = ObIntType;
  def1.column_group_id_ = 0;
  def1.rowkey_seq_ = 0;
  def1.table_id_ = TABLE_ID;

  def2.column_name_id_ = 6;
  def2.column_value_type_ = ObDoubleType;
  def2.column_group_id_ = 0;
  def2.rowkey_seq_ = 0;
  def2.table_id_ = TABLE_ID;

  def3.column_name_id_ = 7;
  def3.column_value_type_ = ObVarcharType;
  def3.column_group_id_ = 0;
  def3.rowkey_seq_ = 0;
  def3.table_id_ = TABLE_ID;

  schema.add_column_def(def0);
  schema.add_column_def(def1);
  schema.add_column_def(def2);
  schema.add_column_def(def3);
}

int write_sstable_file(const ObString& path, const ObString& compress, const ObNewRange range, const int row_count)
{
  CharArena allocator;
  ObSSTableWriter writer;
  ObSSTableSchema schema;
  create_schema(schema);
  remove(path.ptr());
  int ret = writer.create_sstable(schema, path, compress, 0);
  if (ret) return ret;
  for (int i = 0; i < row_count; ++i)
  {
    ObSSTableRow row;
    create_row(allocator, row, g_key[i], i, i, (char*)g_key[i]);
    int64_t approx_usage = 0;
    ret = writer.append_row(row, approx_usage);
    if (ret) return ret;
  }

  ret = writer.set_tablet_range(range);
  int64_t trailer_offset = 0;
  ret = writer.close_sstable(trailer_offset);
  return ret;
}


TEST(ObTestObSSTableReader, read)
{
  ObSSTableId sstable_id;
  sstable_id.sstable_file_id_ = 1234;
  sstable_id.sstable_file_offset_ = 0;
   
  char path[OB_MAX_FILE_NAME_LENGTH];
  char* compress = (char*)"lzo_1.0";
  CharArena char_allocator;
  ObNewRange range;
  range.table_id_ = TABLE_ID;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.inclusive_end();
  create_row_key(char_allocator, range.start_key_, g_key[0]);
  create_row_key(char_allocator, range.end_key_, g_key[5]);

  int ret = 0;
  char sstable_file_dir[OB_MAX_FILE_NAME_LENGTH];
  ret = get_sstable_directory((sstable_id.sstable_file_id_ & 0xFF), 
                              sstable_file_dir, OB_MAX_FILE_NAME_LENGTH);
  EXPECT_EQ(0, ret);
  bool ok = FileDirectoryUtils::exists(sstable_file_dir);
  if (!ok)
  {
    ok = FileDirectoryUtils::create_full_path(sstable_file_dir);
    if (!ok)
      TBSYS_LOG(ERROR, "create sstable path:%s failed", sstable_file_dir); 
  }

  get_sstable_path(sstable_id, path, OB_MAX_FILE_NAME_LENGTH);
  ObString ob_path(0, static_cast<int32_t>(strlen(path)), path);
  ObString ob_compress(0, static_cast<int32_t>(strlen(compress)), compress);

  ret = write_sstable_file(ob_path, ob_compress, range, 6);
  EXPECT_EQ(0, ret);

  FileInfoCache fileinfo_cache;
  fileinfo_cache.init(10);

  ModulePageAllocator mod(0);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObSSTableReader reader(allocator, fileinfo_cache);

  ret = reader.open(sstable_id, 0);
  EXPECT_EQ(0, ret);
  unlink(path);
  allocator.free();
  fileinfo_cache.destroy();
}

TEST(ObTestObSSTableReader, read_0_3_1_1212_version)
{
  unlink("tmp/1/1");
  EXPECT_EQ(0, system("mkdir -p tmp/1 && ln -s ../../data/sst_0.3.1_1212 tmp/1/1"));
  ObSSTableId sstable_id;
  sstable_id.sstable_file_id_ = 1;
  sstable_id.sstable_file_offset_ = 0;
  ObNewRange range;
  range.reset();
  range.start_key_.set_min_row();
  range.border_flag_.unset_inclusive_start();
  range.end_key_.set_max_row();
  range.border_flag_.unset_inclusive_end();
  range.table_id_ = 1001;

  FileInfoCache fileinfo_cache;
  fileinfo_cache.init(10);
  ModulePageAllocator mod(0);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObSSTableReader reader(allocator, fileinfo_cache);
  ObSchemaManagerV2* schema = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  tbsys::CConfig c1;
  if (!schema->parse_from_file("data/test_sstable_reader.schema", c1))
  {
    printf("failed to load schema\n");
    FAIL();
  }
  ObMergerSchemaManager* sstable_schema_manager = new ObMergerSchemaManager();
  if (0 != sstable_schema_manager->init(false, *schema))
  {
    printf("failed to init global_sstable_schema_manager\n");
    FAIL();
  }
  set_global_sstable_schema_manager(sstable_schema_manager);
  int ret = reader.open(sstable_id, 0);
  EXPECT_EQ(0, ret);
  TBSYS_LOG(INFO, "range %s", to_cstring(reader.get_range()));
  EXPECT_TRUE(range == reader.get_range());
  allocator.free();
  fileinfo_cache.destroy();
  unlink("tmp/1/1");
}

TEST(ObTestObSSTableReader, read_0_3_1_with_range_version)
{
  unlink("tmp/1/257");
  EXPECT_EQ(0, system("mkdir -p tmp/1 && ln -s ../../data/sst_0.3.1_with_range tmp/1/257"));
  ObSSTableId sstable_id;
  sstable_id.sstable_file_id_ = 257;
  sstable_id.sstable_file_offset_ = 0;
  CharArena char_allocator;

  FileInfoCache fileinfo_cache;
  fileinfo_cache.init(10);
  ModulePageAllocator mod(0);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObSSTableReader reader(allocator, fileinfo_cache);
  ObSchemaManagerV2* schema = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  tbsys::CConfig c1;
  if (!schema->parse_from_file("data/test_sstable_reader.schema", c1))
  {
    printf("failed to load schema\n");
    FAIL();
  }
  ObMergerSchemaManager* sstable_schema_manager = new ObMergerSchemaManager();
  if (0 != sstable_schema_manager->init(false, *schema))
  {
    printf("failed to init global_sstable_schema_manager\n");
    FAIL();
  }
  set_global_sstable_schema_manager(sstable_schema_manager);
  int ret = reader.open(sstable_id, 0);
  EXPECT_EQ(0, ret);
  TBSYS_LOG(INFO, "range %s", to_cstring(reader.get_range()));

  ObNewRange expect_range;
  expect_range.reset();
  expect_range.border_flag_.unset_min_value();
  expect_range.border_flag_.unset_inclusive_start();
  expect_range.border_flag_.unset_max_value();
  expect_range.border_flag_.set_inclusive_end();
  expect_range.table_id_ = 1001;
  ObObj start_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj end_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  start_key_obj_array[0].set_int(599);
  start_key_obj_array[1].set_int(-1);
  end_key_obj_array[0].set_int(604);
  end_key_obj_array[1].set_int(-1);
  expect_range.start_key_.assign(start_key_obj_array, 2);
  expect_range.end_key_.assign(end_key_obj_array, 2);
  EXPECT_TRUE(expect_range == reader.get_range());

  allocator.free();
  fileinfo_cache.destroy();
  unlink("tmp/1/257");
}

TEST(ObTestObSSTableReader, read_0_4)
{
  unlink("tmp/1/513");
  EXPECT_EQ(0, system("mkdir -p tmp/1 && ln -s ../../data/sst_0.4 tmp/1/513"));
  ObSSTableId sstable_id;
  sstable_id.sstable_file_id_ = 513;
  sstable_id.sstable_file_offset_ = 0;
  CharArena char_allocator;

  FileInfoCache fileinfo_cache;
  fileinfo_cache.init(10);
  ModulePageAllocator mod(0);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObSSTableReader reader(allocator, fileinfo_cache);
  ObSchemaManagerV2* schema = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  tbsys::CConfig c1;
  if (!schema->parse_from_file("data/test_sstable_reader.schema", c1))
  {
    printf("failed to load schema\n");
    FAIL();
  }
  ObMergerSchemaManager* sstable_schema_manager = new ObMergerSchemaManager();
  if (0 != sstable_schema_manager->init(false, *schema))
  {
    printf("failed to init global_sstable_schema_manager\n");
    FAIL();
  }
  set_global_sstable_schema_manager(sstable_schema_manager);
  int ret = reader.open(sstable_id, 0);
  EXPECT_EQ(0, ret);
  TBSYS_LOG(INFO, "range %s", to_cstring(reader.get_range()));

  ObNewRange expect_range;
  expect_range.reset();
  expect_range.border_flag_.unset_min_value();
  expect_range.border_flag_.unset_inclusive_start();
  expect_range.border_flag_.unset_max_value();
  expect_range.border_flag_.set_inclusive_end();
  expect_range.table_id_ = 1001;
  ObObj start_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj end_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  start_key_obj_array[0].set_int(619);
  start_key_obj_array[1].set_int(INT64_MAX);
  end_key_obj_array[0].set_int(624);
  end_key_obj_array[1].set_int(INT64_MAX);
  expect_range.start_key_.assign(start_key_obj_array, 2);
  expect_range.end_key_.assign(end_key_obj_array, 2);
  EXPECT_TRUE(expect_range == reader.get_range());
  allocator.free();
  fileinfo_cache.destroy();
  unlink("tmp/1/513");
}
int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

