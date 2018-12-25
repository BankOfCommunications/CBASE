/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_helper.cpp
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   qushan<qushan@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "test_helper.h"
#include "sstable/ob_disk_path.h"
#include "common/file_directory_utils.h"
#include "common/file_directory_utils.h"
#include "chunkserver/ob_tablet.h"
#include "chunkserver/ob_chunk_server_config.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;

static const char* DEF_SSTABLE_PATH = "/tmp/ko/100";
static CharArena allocator_;

void check_string(const ObString& expected, const ObString& real)
{
  EXPECT_EQ(expected.length(), real.length());
  if (NULL != expected.ptr() && NULL != real.ptr())
  {
    EXPECT_EQ(0, memcmp(expected.ptr(), real.ptr(), expected.length()));
  }
  else
  {
    EXPECT_EQ((const char*) NULL, expected.ptr());
    EXPECT_EQ((const char*) NULL, real.ptr());
  }
  //printf("expected_row_key=%.*s, real_row_key=%.*s\n",
  //    expected.length(), expected.ptr(), real.length(), real.ptr());
}

void check_obj(const ObObj& expected, const ObObj& real)
{
  // TODO

  EXPECT_EQ(expected.get_type(), real.get_type());
  int64_t expected_val =0;
  int64_t real_val = 0;
  int ret = 0;
  ret = expected.get_int(expected_val);
  EXPECT_EQ(0, ret);
  ret = real.get_int(real_val);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(expected_val, real_val);
}

void check_cell(const ObCellInfo& expected, const ObCellInfo& real)
{
  EXPECT_EQ(expected.column_id_, real.column_id_);
  EXPECT_EQ(expected.table_id_, real.table_id_);
  EXPECT_EQ(expected.row_key_, real.row_key_);
  check_obj(expected.value_, real.value_);
}

void check_cell_with_name(const ObCellInfo& expected, const ObCellInfo& real)
{
  check_string(expected.table_name_, real.table_name_);
  EXPECT_EQ(expected.row_key_, real.row_key_);
  check_string(expected.column_name_, real.column_name_);
}

int prepare_sstable_directroy(const int64_t disk_num)
{
  FileDirectoryUtils::delete_directory_recursively("tmp");
  char sstable_path[OB_MAX_FILE_NAME_LENGTH];
  int ret = 0;
  for (int i = 0; i < disk_num && OB_SUCCESS == ret; ++i)
  {
    ret = get_sstable_directory(i+1, sstable_path, OB_MAX_FILE_NAME_LENGTH);
    if (OB_SUCCESS == ret)
    {
      FileDirectoryUtils::create_full_path(sstable_path);
    }
  }
  return ret;
}

void generate_cg_schema(
    ObSSTableSchema& sstable_schema, const ObCellInfo** cell_infos,
    const int64_t column_group_id,
    const int64_t start_col, const int64_t end_col)
{
  ObSSTableSchemaColumnDef column_def;

  EXPECT_TRUE(NULL != cell_infos);

  uint64_t table_id = cell_infos[0][0].table_id_;

  for (int64_t i = start_col; i < end_col; ++i)
  {
    column_def.table_id_ = static_cast<uint32_t>(table_id);
    column_def.column_group_id_ = static_cast<uint16_t>(column_group_id);
    column_def.column_name_id_ = static_cast<uint16_t>(cell_infos[0][i].column_id_);
    column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
    sstable_schema.add_column_def(column_def);
  }
}

int append_cg_rows(
    ObSSTableWriter& writer, const ObCellInfo** cell_infos,
    const int64_t column_group_id, const int64_t row_num,
    const int64_t start_col, const int64_t end_col)
{
  int err = 0;
  uint64_t table_id = cell_infos[0][0].table_id_;

  for (int64_t i = 0; i < row_num; ++i)
  {
    ObSSTableRow row;
    err = row.set_rowkey(cell_infos[i][0].row_key_);
    EXPECT_EQ(0, err);
    row.set_table_id(table_id);
    row.set_column_group_id(column_group_id);
    for (int64_t j = start_col; j < end_col; ++j)
    {
      err = row.add_obj(cell_infos[i][j].value_);
      EXPECT_EQ(0, err);
    }

    int64_t space_usage = 0;
    err = writer.append_row(row, space_usage);
    EXPECT_EQ(0, err);
  }
  return err;
}

int write_cg_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path )
{
  int err = OB_SUCCESS;
  EXPECT_TRUE(NULL != cell_infos);
  EXPECT_TRUE(row_num > 0);
  EXPECT_TRUE(col_num > 0);

  if (sstable_file_path) unlink(sstable_file_path);

  ObSSTableSchema sstable_schema;
  generate_cg_schema(sstable_schema, cell_infos, 12, 0, 1);
  generate_cg_schema(sstable_schema, cell_infos, 13, 1, 3);
  generate_cg_schema(sstable_schema, cell_infos, 15, 3, col_num);

  ObString path;
  ObString compress_name;
  const char* path_str = NULL;
  if (NULL != sstable_file_path)
  {
    path_str = sstable_file_path;
  }
  else
  {
    path_str = DEF_SSTABLE_PATH;
  }
  path.assign((char*) path_str, static_cast<int32_t>(strlen(path_str)));
  compress_name.assign((char*)"lzo", static_cast<int32_t>(strlen("lzo")));

  ObSSTableWriter writer;
  uint64_t table_id = cell_infos[0][0].table_id_;
  err = writer.create_sstable(sstable_schema, path, compress_name, table_id);
  EXPECT_EQ(0, err);

  err = append_cg_rows(writer, cell_infos, 12, row_num, 0, 1);
  EXPECT_EQ(0, err);
  err = append_cg_rows(writer, cell_infos, 13, row_num, 1, 3);
  EXPECT_EQ(0, err);
  err = append_cg_rows(writer, cell_infos, 15, row_num, 3, col_num);
  EXPECT_EQ(0, err);

  int64_t offset = 0;
  err = writer.close_sstable(offset);
  EXPECT_EQ(0, err);

  return err;
}

int write_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path )
{
  int err = OB_SUCCESS;

  if (sstable_file_path) unlink(sstable_file_path);

  ObSSTableSchema sstable_schema;
  ObSSTableSchemaColumnDef column_def;

  EXPECT_TRUE(NULL != cell_infos);
  EXPECT_TRUE(row_num > 0);
  EXPECT_TRUE(col_num > 0);

  uint64_t table_id = cell_infos[0][0].table_id_;

  // add rowkey column
  column_def.reserved_ = 0;
  column_def.rowkey_seq_ = 1;
  column_def.column_group_id_= 0;
  column_def.column_name_id_ = 1;
  column_def.column_value_type_ = ObVarcharType;
  column_def.table_id_ = static_cast<uint32_t>(table_id);
  sstable_schema.add_column_def(column_def);

  for (int64_t i = 0; i < col_num; ++i)
  {
    column_def.reserved_ = 0;
    column_def.rowkey_seq_ = 0;
    column_def.table_id_ = static_cast<uint32_t>(table_id);
    column_def.column_group_id_ = 0;
    column_def.column_name_id_ = static_cast<uint16_t>(cell_infos[0][i].column_id_);
    column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
    sstable_schema.add_column_def(column_def);
  }

  ObString path;
  ObString compress_name;
  const char* path_str = NULL;

  if (NULL != sstable_file_path)
  {
    path_str = sstable_file_path;
  }
  else
  {
    path_str = DEF_SSTABLE_PATH;
  }
  path.assign((char*) path_str, static_cast<int32_t>(strlen(path_str)));
  compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));

  ObSSTableWriter writer;
  err = writer.create_sstable(sstable_schema, path, compress_name, table_id);
  EXPECT_EQ(0, err);

  for (int64_t i = 0; i < row_num; ++i)
  {
    ObSSTableRow row;
    row.set_table_id(table_id);
    row.set_column_group_id(0);
    err = row.set_rowkey(cell_infos[i][0].row_key_);
    EXPECT_EQ(0, err);
    for (int64_t j = 0; j < col_num; ++j)
    {
      err = row.add_obj(cell_infos[i][j].value_);
      EXPECT_EQ(0, err);
    }

    int64_t space_usage = 0;
    err = writer.append_row(row, space_usage);
    EXPECT_EQ(0, err);
  }

  int64_t offset = 0;
  err = writer.close_sstable(offset);
    EXPECT_EQ(0, err);

  return err;
}

int write_empty_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path )
{
  int err = OB_SUCCESS;

  if (sstable_file_path) unlink(sstable_file_path);

  ObSSTableSchema sstable_schema;
  ObSSTableSchemaColumnDef column_def;

  EXPECT_TRUE(NULL != cell_infos);
  EXPECT_TRUE(row_num > 0);
  EXPECT_TRUE(col_num > 0);

  uint64_t table_id = cell_infos[0][0].table_id_;

  // add rowkey column
  column_def.reserved_ = 0;
  column_def.rowkey_seq_ = 1;
  column_def.column_group_id_= 0;
  column_def.column_name_id_ = 1;
  column_def.column_value_type_ = ObVarcharType;
  column_def.table_id_ = static_cast<uint32_t>(table_id);
  sstable_schema.add_column_def(column_def);

  for (int64_t i = 0; i < col_num; ++i)
  {
    column_def.reserved_ = 0;
    column_def.rowkey_seq_ = 0;
    column_def.table_id_ = static_cast<uint32_t>(table_id);
    column_def.column_group_id_ = 0;
    column_def.column_name_id_ = static_cast<uint16_t>(cell_infos[0][i].column_id_);
    column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
    sstable_schema.add_column_def(column_def);
  }

  ObString path;
  ObString compress_name;
  const char* path_str = NULL;

  if (NULL != sstable_file_path)
  {
    path_str = sstable_file_path;
  }
  else
  {
    path_str = DEF_SSTABLE_PATH;
  }
  path.assign((char*) path_str, static_cast<int32_t>(strlen(path_str)));
  compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));

  ObSSTableWriter writer;
  err = writer.create_sstable(sstable_schema, path, compress_name, table_id);
  EXPECT_EQ(0, err);

  int64_t offset = 0;
  err = writer.close_sstable(offset);
  EXPECT_EQ(0, err);

  return err;
}

GFactory GFactory::instance_;

void GFactory::init()
{
  const ObChunkServerConfig config;

  ob_init_memory_pool(64 * 1024 * 1024);
  fic_.init(config.file_info_cache_num);
  int ret = block_cache_.init(config.block_cache_size);
  ASSERT_EQ(0, ret);
  ret = block_index_cache_.init(config.block_index_cache_size);
  ASSERT_EQ(0, ret);
}

void GFactory::destroy()
{
  block_cache_.destroy();
  block_index_cache_.clear();
}

SSTableBuilder::SSTableBuilder()
{
  //malloc
  cell_infos = new ObCellInfo*[ROW_NUM];
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    cell_infos[i] = new ObCellInfo[COL_NUM];
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      row_key_strs[i][j] = new char[50];
    }
  }
}

SSTableBuilder::~SSTableBuilder()
{
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      if (NULL != row_key_strs[i][j])
      {
        delete[] row_key_strs[i][j];
        row_key_strs[i][j] = NULL;
      }
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    if (NULL != cell_infos[i])
    {
      delete[] cell_infos[i];
      cell_infos[i] = NULL;
    }
  }
  if (NULL != cell_infos)
  {
    delete[] cell_infos;
  }
}

int SSTableBuilder::generate_sstable_file(WRITE_SSTABLE_FUNC write_func,
    const ObSSTableId& sstable_id, const int64_t start_index)
{
  int err = OB_SUCCESS;
  static CharArena allocator;

  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_id_ = table_id;
      sprintf(row_key_strs[i][j], "row_key_%08ld", i + start_index);
      cell_infos[i][j].row_key_ = make_rowkey(row_key_strs[i][j], &allocator);
      cell_infos[i][j].column_id_ = j + 2;
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
  char sstable_file_dir[OB_MAX_FILE_NAME_LENGTH];
  err = get_sstable_directory((sstable_id.sstable_file_id_ & 0xFF), sstable_file_dir, OB_MAX_FILE_NAME_LENGTH);
  if (err) return err;
  else
  {
    bool ok = FileDirectoryUtils::exists(sstable_file_dir);
    if (!ok)
    {
      ok = FileDirectoryUtils::create_full_path(sstable_file_dir);
      if (!ok)
        TBSYS_LOG(ERROR, "create sstable path:%s failed", sstable_file_dir);
    }
  }
  err = get_sstable_path(sstable_id, sstable_file_path, OB_MAX_FILE_NAME_LENGTH);
  if (err) return err;
  //init sstable
  err = write_func((const ObCellInfo**) cell_infos, ROW_NUM, COL_NUM, sstable_file_path);
  return err;
}

int MultSSTableBuilder::generate_sstable_files(bool empty_sstable)
{
  int ret = OB_SUCCESS;
  int64_t start_index = 0;
  ObSSTableId sst_id;

  for (int i = 0; i < SSTABLE_NUM; i++)
  {
    ret = get_sstable_id(sst_id, i);
    if (OB_SUCCESS == ret)
    {
      start_index = i * SSTableBuilder::ROW_NUM * SSTableBuilder::COL_NUM;
      if (empty_sstable)
      {
        ret = builder_[i].generate_sstable_file(write_empty_sstable, sst_id, start_index);
      }
      else
      {
        ret = builder_[i].generate_sstable_file(write_sstable, sst_id, start_index);
      }
      if (OB_SUCCESS != ret)
      {
        break;
      }
    }
  }

  return ret;
}

int TabletManagerIniter::create_tablet(const ObNewRange& range,
                                       const ObSSTableId& sst_id, bool serving,
                                       bool add_sst_id,
                                       const int64_t version)
{
  int ret = OB_SUCCESS;
  UNUSED(serving);

  if (range.empty())
  {
    TBSYS_LOG(ERROR, "create_tablet error, input range is empty.");
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    ObTablet* tablet = NULL;
    // find tablet if exist?
    ObMultiVersionTabletImage &image = tablet_mgr_.get_serving_tablet_image();
    ret = image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, version, tablet);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(ERROR, "tablet already exists! dump input and exist:");
      range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
      tablet->get_range().hex_dump(TBSYS_LOG_LEVEL_ERROR);
      ret = OB_ERROR;
      tablet = NULL;
      image.release_tablet(tablet);
    }
    else
    {
      ret = image.alloc_tablet_object(range, version, tablet);
      if (OB_SUCCESS == ret && add_sst_id)
      {
        ret = tablet->add_sstable_by_id(sst_id);
      }
      if (OB_SUCCESS == ret)
      {
        tablet->set_disk_no(sst_id.sstable_file_id_ & DISK_NO_MASK);
        ret = image.add_tablet(tablet, true, true);
      }
    }
  }

  return ret;
}

int TabletManagerIniter::create_tablets(bool empty_tablet)
{
  int ret = OB_SUCCESS;
  ObNewRange ranges[MultSSTableBuilder::SSTABLE_NUM];
  ObSSTableId sst_ids[MultSSTableBuilder::SSTABLE_NUM];
  ObCellInfo** cell_infos;
  ObCellInfo** prev_cell_infos;

  ret = builder_.generate_sstable_files(empty_tablet);
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < MultSSTableBuilder::SSTABLE_NUM; ++i)
    {
      ranges[i].table_id_ = SSTableBuilder::table_id;
      cell_infos = get_mult_sstable_builder().get_cell_infos(i);
      ranges[i].end_key_ = cell_infos[SSTableBuilder::ROW_NUM - 1][0].row_key_;
      ranges[i].border_flag_.set_inclusive_end();
      ranges[i].border_flag_.unset_inclusive_start();

      if (0 == i)
      {
        ranges[i].start_key_.set_min_row();
      }
      else
      {
        prev_cell_infos = get_mult_sstable_builder().get_cell_infos(i - 1);
        ranges[i].start_key_ = prev_cell_infos[SSTableBuilder::ROW_NUM - 1][0].row_key_;
      }

      ret = get_mult_sstable_builder().get_sstable_id(sst_ids[i], i);
      if (OB_SUCCESS == ret)
      {
        if (empty_tablet)
        {
          if (i < MultSSTableBuilder::SSTABLE_NUM / 2)
          {
            ret = create_tablet(ranges[i], sst_ids[i], true, !empty_tablet);
          }
          else
          {
            ret = create_tablet(ranges[i], sst_ids[i], true);
          }
        }
        else
        {
          ret = create_tablet(ranges[i], sst_ids[i], true);
        }
        if (OB_SUCCESS != ret)
        {
          ret = OB_ERROR;
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      int32_t disk_no_array[MultSSTableBuilder::DISK_NUM];
      char path[OB_MAX_FILE_NAME_LENGTH];

      ObMultiVersionTabletImage &image = tablet_mgr_.get_serving_tablet_image();
      for (int i = 0; i < MultSSTableBuilder::DISK_NUM && OB_SUCCESS == ret; i++)
      {
        disk_no_array[i] = i + 1;
        ret = get_meta_path(1, disk_no_array[i], true, path, OB_MAX_FILE_NAME_LENGTH);
        if (OB_SUCCESS != ret)
        {
          break;
        }
        remove(path);
        if (OB_SUCCESS == ret)
        {
          ret = image.write(1, i + 1);
        }
      }
    }
  }

  return ret;
}


int TabletManagerIniter::init(bool create, bool empty_tablet)
{
  int ret = OB_SUCCESS;
  static ObChunkServerConfig config;
  static ObClientManager client_mgr; //add dragon [repair_test] 2016-12-30
  config.datadir.set_value("./tmp");

  if (inited_)
  {
    ret = OB_ERROR;
  }
  else
  {
    //mod dragon [repair_test] 2016-12-30
    ret = tablet_mgr_.init(&config, &client_mgr);
    //ret = tablet_mgr_.init(&config);
    //mod e
    if (OB_SUCCESS == ret && create)
    {
      ret = create_tablets(empty_tablet);
    }

    if (OB_SUCCESS == ret)
    {
      inited_ = true;
    }
  }

  return ret;
}
