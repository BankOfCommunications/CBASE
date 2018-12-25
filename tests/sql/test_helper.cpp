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
#include "chunkserver/ob_tablet.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;


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
  int64_t expected_val = 0;
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
  EXPECT_TRUE(expected.row_key_ ==  real.row_key_) ;
  check_obj(expected.value_, real.value_);
}

void check_cell_with_name(const ObCellInfo& expected, const ObCellInfo& real)
{
  check_string(expected.table_name_, real.table_name_);
  EXPECT_TRUE(expected.row_key_ ==  real.row_key_);
  check_string(expected.column_name_, real.column_name_);
}

/*
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
    column_def.column_name_id_ = static_cast<uint32_t>(cell_infos[0][i].column_id_);
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
    err = row.set_row_key(cell_infos[i][0].row_key_);
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
  compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));

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

  for (int64_t i = 0; i < col_num; ++i)
  {
    column_def.table_id_ = static_cast<uint32_t>(table_id);
    column_def.column_group_id_ = 0;
    column_def.column_name_id_ = static_cast<uint32_t>(cell_infos[0][i].column_id_);
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
    err = row.set_row_key(cell_infos[i][0].row_key_);
    EXPECT_EQ(0, err);
    row.set_table_id(table_id);
    row.set_column_group_id(0);
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
*/

GFactory GFactory::instance_;

void GFactory::init()
{
  //ObBlockCacheConf bc_conf_;
  //ObBlockIndexCacheConf bic_conf;

  //bc_conf_.block_cache_memsize_mb = 1024;
  //bc_conf_.ficache_max_num = 1024;

  //bic_conf.cache_mem_size = 1024*1024*200;

  ob_init_memory_pool(64 * 1024);
  fic_.init(1024);
  int ret = block_cache_.init(1024 * 1024);
  ASSERT_EQ(0, ret);
  block_cache_.set_fileinfo_cache(fic_);
  ret = block_index_cache_.init(1024 * 1024 * 200);
  ASSERT_EQ(0, ret);
}

void GFactory::destroy()
{
  block_cache_.destroy();
  block_index_cache_.clear();
}

/*
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


  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_id_ = table_id;
      sprintf(row_key_strs[i][j], "row_key_%08ld", i + start_index);
      cell_infos[i][j].row_key_.assign(row_key_strs[i][j], static_cast<int32_t>(strlen(row_key_strs[i][j])));
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

int MultSSTableBuilder::generate_sstable_files()
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
      ret = builder_[i].generate_sstable_file(write_sstable, sst_id, start_index);
      if (OB_SUCCESS != ret)
      {
        break;
      }
    }
  }

  return ret;
}

int TabletManagerIniter::create_tablet(const ObRange& range,
                                       const ObSSTableId& sst_id, bool serving,
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
    ObMultiVersionTabletImage &image =  tablet_mgr_.get_serving_tablet_image() ;
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
      if (OB_SUCCESS == ret)
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

int TabletManagerIniter::create_tablets()
{
  int ret = OB_SUCCESS;
  ObRange ranges[MultSSTableBuilder::SSTABLE_NUM];
  ObSSTableId sst_ids[MultSSTableBuilder::SSTABLE_NUM];
  ObCellInfo** cell_infos;
  ObCellInfo** prev_cell_infos;

  ret = builder_.generate_sstable_files();
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
        ranges[i].start_key_.assign(NULL, 0);
        ranges[i].border_flag_.set_min_value();
      }
      else
      {
        prev_cell_infos = get_mult_sstable_builder().get_cell_infos(i - 1);
        ranges[i].start_key_ = prev_cell_infos[SSTableBuilder::ROW_NUM - 1][0].row_key_;
      }

      ret = get_mult_sstable_builder().get_sstable_id(sst_ids[i], i);
      if (OB_SUCCESS == ret)
      {
        ret = create_tablet(ranges[i], sst_ids[i], true);
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


int TabletManagerIniter::init(bool create)
{
  int ret = OB_SUCCESS;
  ObBlockCacheConf bc_conf;
  ObBlockIndexCacheConf bic_conf;

  if (inited_)
  {
    ret = OB_ERROR;
  }
  else
  {
    bc_conf.block_cache_memsize_mb = 1024;
    bc_conf.ficache_max_num = 1024;

    bic_conf.cache_mem_size = 128 * 1024 * 1024;

    ret = tablet_mgr_.init(bc_conf, bic_conf);
    if (OB_SUCCESS == ret && create)
    {
      ret = create_tablets();
    }

    if (OB_SUCCESS == ret)
    {
      inited_ = true;
    }
  }

  return ret;
}
*/



CellInfoGen::CellInfoGen(const int64_t tr, const int64_t tc)
  : total_rows(tr), total_columns(tc)
{
  //malloc
  cell_infos = new ObCellInfo*[total_rows];
  for (int64_t i = 0; i < total_rows; ++i)
  {
    cell_infos[i] = new ObCellInfo[total_columns];
  }

}

CellInfoGen::~CellInfoGen()
{
  for (int64_t i = 0; i < total_rows; ++i)
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

int CellInfoGen::gen(const Desc *desc, const int64_t size)
{
  int err = OB_SUCCESS;

  EXPECT_GT(size, 0);

  const Desc& key_desc = desc[0];
  EXPECT_EQ(key_desc.column_group_id, 0);
  EXPECT_LT(desc[0].end_column, total_columns);

  for (int64_t i = 0; i < total_rows; ++i)
  {
    for (int64_t j = 0; j < total_columns; ++j)
    {
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].column_id_ = j + START_ID;
      cell_infos[i][j].value_.set_int(i * total_columns + j);
    }
  }


  ObSSTableSchemaColumnDef column_def;
  for (int di = 1; di < size; ++di)
  {
    const Desc& d = desc[di];
    for(int col_index = 0; col_index < total_columns; ++col_index)
    {
      memset(&column_def,0,sizeof(column_def));
      column_def.table_id_ = table_id;
      column_def.column_group_id_ = static_cast<uint16_t>(d.column_group_id);
      if (col_index >= key_desc.start_column && col_index <= key_desc.end_column)
      {
        column_def.rowkey_seq_ = static_cast<uint16_t>(col_index + 1);
      }
      else if (col_index >= d.start_column && col_index <= d.end_column)
      {
        column_def.rowkey_seq_ = 0;
      }
      else
      {
        continue;
      }

      column_def.column_name_id_ = static_cast<uint16_t>(col_index + START_ID);
      column_def.column_value_type_ = cell_infos[0][col_index].value_.get_type();
      err = schema.add_column_def(column_def);

    }
  }

  return err;
}

struct WriterAdapter
{
  WriterAdapter(ObSSTableWriter& writer)
    : writer_(writer) {}

  int operator()(const ObSSTableRow& row)
  {
    int64_t space_usage = 0;
    int err = writer_.append_row(row, space_usage);
    return err;
  }
  ObSSTableWriter &writer_;
};

int write_sstable(const ObSSTableId & sstable_id, const int64_t row_num, const int64_t col_num, const CellInfoGen::Desc* desc , const int64_t size)
{
  char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
  char sstable_file_dir[OB_MAX_FILE_NAME_LENGTH];
  int err = get_sstable_directory((sstable_id.sstable_file_id_ & 0xFF), sstable_file_dir, OB_MAX_FILE_NAME_LENGTH);
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
  err = write_sstable(sstable_file_path, row_num, col_num, desc, size);
  return err;
}

int write_sstable(const char* sstable_file_path, const int64_t row_num, const int64_t col_num, const CellInfoGen::Desc* desc , const int64_t size)
{
  int err = OB_SUCCESS;

  if (sstable_file_path) unlink(sstable_file_path);


  ObString path;
  ObString compress_name;
  path.assign((char*) sstable_file_path, static_cast<int32_t>( strlen(sstable_file_path)));
  compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));

  CellInfoGen cgen(row_num, col_num);
  ObSSTableWriter writer;

  if (OB_SUCCESS != (err = cgen.gen(desc, size)))
  {
  }
  else if (OB_SUCCESS != (err = writer.create_sstable(cgen.schema,
          path, compress_name, CellInfoGen::table_id)))
  {
  }
  else
  {

    err = map_row(WriterAdapter(writer), cgen, desc, size);
    if (OB_SUCCESS != err) return err;

    int64_t offset = 0;
    err = writer.close_sstable(offset);
    EXPECT_EQ(0, err);

  }

  return err;
}

struct BuilderAdapter
{
  BuilderAdapter(ObSSTableBlockBuilder& writer)
    : writer_(writer) {}

  int operator()(const ObSSTableRow& row)
  {
    uint64_t row_checksum = 0;
    int err = writer_.add_row(row, row_checksum);
    return err;
  }
  ObSSTableBlockBuilder &writer_;
};

int write_block(ObSSTableBlockBuilder& builder, const CellInfoGen& cgen , const CellInfoGen::Desc* desc , const int64_t size)
{
  int ret = builder.init();
  if (OB_SUCCESS != ret) return ret;

  ret = map_row(BuilderAdapter(builder), cgen, desc, size);
  if (OB_SUCCESS != ret) return ret;

  ret = builder.build_block();
  return ret;
}

void create_rowkey(const int64_t row, const int64_t col_num, const int64_t rowkey_col_num, ObRowkey &rowkey)
{
  ObObj* obj_array  = new ObObj[rowkey_col_num];
  for (int64_t i = 0; i < rowkey_col_num; ++i)
  {
    obj_array[i].set_int(row * col_num + i);
  }
  rowkey.assign(obj_array, rowkey_col_num);
}

void create_new_range(ObNewRange& range,
    const int64_t col_num, const int64_t rowkey_col_num,
    const int64_t start_row, const int64_t end_row,
    const ObBorderFlag& border_flag)
{

  range.table_id_ = CellInfoGen::table_id;
  create_rowkey(start_row, col_num, rowkey_col_num, range.start_key_);
  create_rowkey(end_row, col_num, rowkey_col_num, range.end_key_);
  if (border_flag.is_min_value()) range.start_key_.set_min_row();
  if (border_flag.is_max_value()) range.end_key_.set_max_row();
  range.border_flag_ = border_flag;
}


int check_rowkey(const ObRowkey& rowkey, const int64_t row, const int64_t col_num)
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  for (int i = 0; i < rowkey.length(); ++i)
  {
    ret = rowkey.ptr()[i].get_int(val);
    if (OB_SUCCESS != ret || val != row * col_num + i) return OB_ERROR;
  }
  return ret;
}

int check_rows(const ObObj* columns, const int64_t col_size, const int64_t row, const int64_t col_num, const int64_t start_col)
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  for (int64_t i = 0; i < col_size; ++i)
  {
    ret = columns[i].get_int(val);
    if (OB_SUCCESS != ret || val != row * col_num + start_col + i) return OB_ERROR;
  }
  return ret;
}

int64_t random_number(int64_t min, int64_t max)
{
  static int dev_random_fd = -1;
  char *next_random_byte;
  int bytes_to_read;
  int64_t random_value = 0;

  assert(max >= min);

  if (dev_random_fd == -1)
  {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char *)&random_value;
  bytes_to_read = sizeof(random_value);

  do
  {
    int bytes_read;
    bytes_read = static_cast<int32_t>( read(dev_random_fd, next_random_byte, bytes_to_read));
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while(bytes_to_read > 0);

  return min + (abs(random_value) % (max - min + 1));
}



void set_obj_array(ObObj array[], const int types[], const int64_t values[],  const int64_t size)
{
  const int64_t MAX_BUFSIZ = 64;
  char * buf = NULL;
  ObString ob_string;
  for (int64_t i = 0; i < size; ++i)
  {
    switch (types[i])
    {
      case ObIntType:
        array[i].set_int(values[i]);
        break;
      case ObFloatType:
        array[i].set_float((float)values[i]);
        break;
      case ObDoubleType:
        array[i].set_double((double)values[i]);
        break;
      case ObDateTimeType:
        array[i].set_datetime(values[i]);
        break;
      case ObPreciseDateTimeType:
        array[i].set_precise_datetime(values[i]);
        break;
      case ObCreateTimeType:
        array[i].set_createtime(values[i]);
        break;
      case ObModifyTimeType:
        array[i].set_modifytime(values[i]);
        break;
      case ObVarcharType:
        buf = (char*)ob_malloc(MAX_BUFSIZ, ObModIds::TEST);
        snprintf(buf, MAX_BUFSIZ, "%ld", values[i]);
        ob_string.assign(buf, static_cast<int32_t>(strlen(buf)));
        array[i].set_varchar(ob_string);
        break;
      default:
        fprintf(stderr, "not support type %d\n", types[i]);
        break;
    }
  }
}

void set_rnd_obj_array(ObObj array[], const int64_t size, const int64_t min, const int64_t max)
{
  int64_t objval[size];
  int     types[size];
  for (int64_t i = 0; i < size ; ++i)
  {
    objval[i] = random_number(min, max);
    types[i] = ObIntType;
  }
  set_obj_array(array, types, objval, size);
}
