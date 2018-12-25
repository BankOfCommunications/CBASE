#include <bitset>
#include "updateserver/ob_memtank.h"
#include "utils.h"
#include "mutator_builder.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

MutatorBuilder::MutatorBuilder()
{
  memset(cb_array_, 0, sizeof(CellinfoBuilder*) * OB_MAX_TABLE_NUMBER);
  memset(rb_array_, 0, sizeof(RowkeyBuilder*) * OB_MAX_TABLE_NUMBER);
  memset(prefix_end_array_, 0, sizeof(int64_t) * OB_MAX_TABLE_NUMBER);
  max_rownum_per_mutator_ = 0;
  table_num_ = 0;
  table_start_version_ = 0;
}

MutatorBuilder::~MutatorBuilder()
{
  destroy();
}

void MutatorBuilder::destroy()
{
  client_.destroy();
  for (int64_t i = 0; i < table_num_; i++)
  {
    if (NULL != cb_array_[i])
    {
      delete cb_array_[i];
      cb_array_[i] = NULL;
    }
    if (NULL != rb_array_[i])
    {
      delete rb_array_[i];
      rb_array_[i] = NULL;
    }
  }
  table_num_ = 0;
}

RowkeyBuilder &MutatorBuilder::get_rowkey_builder(const int64_t schema_pos)
{
  return *(rb_array_[schema_pos]);
}

CellinfoBuilder &MutatorBuilder::get_cellinfo_builder(const int64_t schema_pos)
{
  return *(cb_array_[schema_pos]);
}

const ObSchema &MutatorBuilder::get_schema(const int64_t schema_pos) const
{
  return schema_mgr_.begin()[schema_pos];
}

int64_t MutatorBuilder::get_schema_num() const
{
  return table_num_;
}

int MutatorBuilder::init4read(const ObSchemaManager &schema_mgr,
                              const int64_t prefix_start,
                              const char *serv_addr,
                              const int serv_port,
                              const int64_t table_start_version,
                              const bool using_id)
{
  return init4write(schema_mgr, prefix_start, serv_addr, serv_port, table_start_version, using_id, 0, 0, 0, 0, NULL);
}

int MutatorBuilder::init4write(const ObSchemaManager &schema_mgr,
                              const int64_t prefix_start,
                              const char *serv_addr,
                              const int serv_port,
                              const int64_t table_start_version,
                              const bool using_id,
                              const int64_t suffix_length,
                              const int64_t suffix_num,
                              const int64_t prefix_end,
                              const int64_t max_rownum_per_mutator,
                              ClientWrapper *client4write)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = CellinfoBuilder::check_schema_mgr(schema_mgr)))
  {
    TBSYS_LOG(WARN, "schema cannot pass check");
  }
  else
  {
    client_.init(serv_addr, serv_port);

    table_num_ = schema_mgr.end() - schema_mgr.begin();
    srand48_r(time(NULL), &table_rand_seed_);
    schema_mgr_ = schema_mgr;
    max_rownum_per_mutator_ = max_rownum_per_mutator;
    table_start_version_ = table_start_version;
    //seed_map_.create(2000000);
    const ObSchema *schema_array = schema_mgr_.begin();
    for (int64_t i = 0; i < table_num_; i++)
    {
      int64_t tmp_suffix_length = get_suffix_length_(schema_array[i], using_id, prefix_start);
      tmp_suffix_length = (0 == tmp_suffix_length) ? suffix_length : tmp_suffix_length;
      int64_t tmp_suffix_num = get_suffix_num_(schema_array[i], using_id, prefix_start);
      tmp_suffix_num = (0 == tmp_suffix_num) ? suffix_num : tmp_suffix_num;
      int64_t tmp_prefix_end = get_prefix_end_(schema_array[i], using_id, prefix_start);
      tmp_prefix_end = std::max(prefix_end, tmp_prefix_end);
      if (0 == tmp_suffix_length
          || 0 == tmp_suffix_num
          || 0 == tmp_prefix_end)
      {
        TBSYS_LOG(WARN, "suffix_length=%ld suffix_num=%ld prefix_end=%ld",
                  tmp_suffix_length, tmp_suffix_num, tmp_prefix_end);
        ret = OB_ERROR;
        break;
      }
      if (NULL == (cb_array_[i] = new(std::nothrow) CellinfoBuilder())
          || NULL == (rb_array_[i] = new(std::nothrow) RowkeyBuilder(prefix_start, tmp_suffix_num, tmp_suffix_length)))
      {
        ret = OB_ERROR;
        break;
      }
      prefix_end_array_[i] = tmp_prefix_end;
      if (NULL == client4write)
      {
        continue;
      }
      if (OB_SUCCESS != (ret = put_suffix_length_(*client4write, schema_array[i], prefix_start, tmp_suffix_length)))
      {
        break;
      }
      if (OB_SUCCESS != (ret = put_suffix_num_(*client4write, schema_array[i], prefix_start, tmp_suffix_num)))
      {
        break;
      }
      if (OB_SUCCESS != (ret = put_prefix_end_(*client4write, schema_array[i], prefix_start, tmp_prefix_end)))
      {
        break;
      }
    }
  }
  assert(OB_SUCCESS == ret);
  return ret;
}

int MutatorBuilder::build_total_scan_param(ObScanParam &scan_param, PageArena<char> &allocer,
                                          const int64_t table_start_version,
                                          const bool using_id, const int64_t table_pos)
{
  int ret = OB_SUCCESS;
  const ObSchema &schema = schema_mgr_.begin()[table_pos];

  int64_t cur_rowkey_info = get_cur_rowkey_info_(schema, using_id, rb_array_[table_pos]->get_prefix_start());
  std::pair<ObString,ObString> key_range = rb_array_[table_pos]->get_rowkey4total_scan(cur_rowkey_info, allocer);
  ObNewRange new_range;
  new_range.start_key_ = TestRowkeyHelper(key_range.first, &allocer);
  new_range.end_key_ = TestRowkeyHelper(key_range.second, &allocer);

  const ObColumnSchema *iter = NULL;
  for (iter = schema.column_begin(); iter != schema.column_end(); iter++)
  {
    if (using_id)
    {
      scan_param.add_column(iter->get_id());
    }
    else
    {
      ObString column_name;
      column_name.assign_ptr(const_cast<char*>(iter->get_name()), static_cast<int32_t>(strlen(iter->get_name())));
      scan_param.add_column(column_name);
    }
  }

  if (using_id)
  {
    scan_param.set(schema.get_table_id(), ObString(), new_range);
  }
  else
  {
    ObString table_name;
    table_name.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
    scan_param.set(OB_INVALID_ID, table_name, new_range);
  }

  ObVersionRange version_range;
  version_range.start_version_ = table_start_version;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  scan_param.set_version_range(version_range);
  return ret;
}

int MutatorBuilder::build_scan_param(ObScanParam &scan_param, PageArena<char> &allocer,
                                    const int64_t table_start_version, const bool using_id, int64_t &table_pos, int64_t &prefix)
{
  int ret = OB_SUCCESS;
  scan_param.reset();

  static int64_t row_info_array[OB_MAX_TABLE_NUMBER];
  static int64_t counter = 0;
  static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 100;
  //static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 1000000000;
  if (0 == (counter++ % ROW_INFO_CACHE_FLUSH_PERIOD))
  {
    memset(row_info_array, 0, sizeof(row_info_array));
  }

  int64_t rand = 0;
  lrand48_r(&table_rand_seed_, &rand);
  table_pos = range_rand(0, table_num_ - 1, rand);
  const ObSchema &schema = schema_mgr_.begin()[table_pos];

  if (0 == row_info_array[table_pos]
      && 0 == (row_info_array[table_pos] = get_cur_rowkey_info_(schema, using_id, rb_array_[table_pos]->get_prefix_start())))
  {
    ret = OB_ERROR;
  }
  else
  {
    std::pair<ObString, ObString> key_range = rb_array_[table_pos]->get_rowkey4scan(row_info_array[table_pos], allocer, prefix);
    ObNewRange new_range;
    new_range.start_key_ = TestRowkeyHelper(key_range.first, &allocer);
    new_range.end_key_ = TestRowkeyHelper(key_range.second, &allocer);

    if (using_id)
    {
      scan_param.add_column(SEED_COLUMN_ID);
    }
    else
    {
      ObString column_name;
      column_name.assign_ptr((char*)SEED_COLUMN_NAME, static_cast<int32_t>(strlen(SEED_COLUMN_NAME)));
      scan_param.add_column(column_name);
    }
    if (using_id)
    {
      scan_param.add_column(CELL_NUM_COLUMN_ID);
    }
    else
    {
      ObString column_name;
      column_name.assign_ptr((char*)CELL_NUM_COLUMN_NAME, static_cast<int32_t>(strlen(CELL_NUM_COLUMN_NAME)));
      scan_param.add_column(column_name);
    }

    lrand48_r(&table_rand_seed_, &rand);
    int64_t column_num = schema.column_end() - schema.column_begin();
    for (int64_t i = 0; i < column_num; i++)
    {
      lrand48_r(&table_rand_seed_, &rand);
      int64_t column_pos = range_rand(0, column_num - 1, rand);
      //int64_t column_pos = 0;
      const ObColumnSchema &column_schema = schema.column_begin()[column_pos];
      if (using_id)
      {
        scan_param.add_column(column_schema.get_id());
      }
      else
      {
        ObString column_name;
        column_name.assign_ptr(const_cast<char*>(column_schema.get_name()), static_cast<int32_t>(strlen(column_schema.get_name())));
        scan_param.add_column(column_name);
      }
    }

    if (using_id)
    {
      scan_param.set(schema.get_table_id(), ObString(), new_range);
    }
    else
    {
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
      scan_param.set(OB_INVALID_ID, table_name, new_range);
    }

    ObVersionRange version_range;
    version_range.start_version_ = table_start_version;
    version_range.border_flag_.set_max_value();
    version_range.border_flag_.set_inclusive_start();
    scan_param.set_version_range(version_range);
    char* is_read_consistency_cfg = getenv("is_read_consistency");
    bool is_read_consistency = (NULL != is_read_consistency_cfg && 0 == strcmp(is_read_consistency_cfg, "true"));
    scan_param.set_is_read_consistency(is_read_consistency);
  }
  return ret;
}

int MutatorBuilder::build_get_param(ObGetParam &get_param, PageArena<char> &allocer,
                                    const int64_t table_start_version, const bool using_id,
                                    const int64_t row_num, const int64_t cell_num_per_row)
{
  int ret = OB_SUCCESS;
  get_param.reset();
  UNUSED(table_start_version);

  static int64_t row_info_array[OB_MAX_TABLE_NUMBER];
  static int64_t suffix_num_array[OB_MAX_TABLE_NUMBER];
  static int64_t counter = 0;
  static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 100;
  //static const int64_t ROW_INFO_CACHE_FLUSH_PERIOD = 1000000000;
  if (0 == (counter++ % ROW_INFO_CACHE_FLUSH_PERIOD))
  {
    memset(row_info_array, 0, sizeof(row_info_array));
    memset(suffix_num_array, 0, sizeof(suffix_num_array));
  }
  for (int64_t i = 0; i < row_num; i++)
  {
    int64_t rand = 0;
    lrand48_r(&table_rand_seed_, &rand);
    int64_t table_pos = range_rand(0, table_num_ - 1, rand);
    const ObSchema &cur_schema = schema_mgr_.begin()[table_pos];

    if (0 == row_info_array[table_pos]
        && 0 == (row_info_array[table_pos] = get_cur_rowkey_info_(cur_schema, using_id, rb_array_[table_pos]->get_prefix_start())))
    {
      assert(OB_SUCCESS == ret);
      ret = OB_ERROR;
      break;
    }
    if (0 == suffix_num_array[table_pos]
        && 0 == (suffix_num_array[table_pos] = get_suffix_num_(cur_schema, using_id, rb_array_[table_pos]->get_prefix_start())))
    {
      assert(OB_SUCCESS == ret);
      ret = OB_ERROR;
      break;
    }
    ObCellInfo ci;
    ci.row_key_ = TestRowkeyHelper(rb_array_[table_pos]->get_random_rowkey(row_info_array[table_pos], suffix_num_array[table_pos], allocer), &allocer);
    if (using_id)
    {
      ci.table_id_ = cur_schema.get_table_id();
    }
    else
    {
      ci.table_name_.assign_ptr(const_cast<char*>(cur_schema.get_table_name()), static_cast<int32_t>(strlen(cur_schema.get_table_name())));
    }
    if (using_id)
    {
      ci.column_id_ = SEED_COLUMN_ID;
      ci.column_name_.assign_ptr(NULL, 0);
    }
    else
    {
      ci.column_id_ = OB_INVALID_ID;
      ci.column_name_.assign_ptr((char*)SEED_COLUMN_NAME, static_cast<int32_t>(strlen(SEED_COLUMN_NAME)));
    }
    get_param.add_cell(ci);
    if (using_id)
    {
      ci.column_id_ = CELL_NUM_COLUMN_ID;
      ci.column_name_.assign_ptr(NULL, 0);
    }
    else
    {
      ci.column_id_ = OB_INVALID_ID;
      ci.column_name_.assign_ptr((char*)CELL_NUM_COLUMN_NAME, static_cast<int32_t>(strlen(CELL_NUM_COLUMN_NAME)));
    }
    get_param.add_cell(ci);

    lrand48_r(&table_rand_seed_, &rand);
    int64_t cell_num = range_rand(1, cell_num_per_row, rand);
    int64_t column_num = cur_schema.column_end() - cur_schema.column_begin();
    for (int64_t j = 0; j < cell_num; j++)
    {
      lrand48_r(&table_rand_seed_, &rand);
      int64_t column_pos = range_rand(0, column_num - 1, rand);
      //int64_t column_pos = 0;
      const ObColumnSchema &column_schema = cur_schema.column_begin()[column_pos];
      if (using_id)
      {
        ci.column_id_ = column_schema.get_id();
        ci.column_name_.assign_ptr(NULL, 0);
      }
      else
      {
        ci.column_id_ = OB_INVALID_ID;
        ci.column_name_.assign_ptr(const_cast<char*>(column_schema.get_name()), static_cast<int32_t>(strlen(column_schema.get_name())));
      }
      get_param.add_cell(ci);
    }
  }
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version_;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);
  char* is_read_consistency_cfg = getenv("is_read_consistency");
  bool is_read_consistency = (NULL != is_read_consistency_cfg && 0 == strcmp(is_read_consistency_cfg, "true"));
  get_param.set_is_read_consistency(is_read_consistency);

  assert(get_param.get_cell_size() > 0);
  return ret;
}

int64_t MutatorBuilder::query_prefix_meta_(const ObSchema &schema, const bool using_id, const int64_t prefix_start, const char *column_name)
{
  int64_t ret = 0;

  ObGetParam get_param;
  ObScanner scanner;
  ObCellInfo cell_info;
  if (using_id)
  {
    cell_info.table_id_ = schema.get_table_id();
    cell_info.column_id_ = schema.find_column_info(column_name)->get_id();
  }
  else
  {
    cell_info.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
    cell_info.column_name_.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));
  }
  char rowkey_info_buffer[1024];
  sprintf(rowkey_info_buffer, "%s%020ld", ROWKEY_INFO_ROWKEY, prefix_start);
  CharArena allocer;
  cell_info.row_key_ = make_rowkey(rowkey_info_buffer, &allocer);
  get_param.add_cell(cell_info);
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version_;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);

  if (OB_SUCCESS == (ret = client_.get(get_param, scanner)))
  {
    CellinfoBuilder::result_set_t result_set;
    ::MemTank mem_tank;
    while (OB_SUCCESS == scanner.next_cell())
    {
      ObCellInfo *ci = NULL;
      if (OB_SUCCESS != scanner.get_cell(&ci))
      {
        break;
      }
      if (!using_id)
      {
        ::trans_name2id(*ci, schema);
      }
      ObCellInfo *new_ci = copy_cell(mem_tank, ci);
      CellinfoBuilder::merge_obj(new_ci, result_set);
    }
    if (0 != result_set.size())
    {
      ObCellInfo *ret_ci = NULL;
      result_set.get(schema.find_column_info(column_name)->get_id(), ret_ci);
      if (NULL != ret_ci)
      {
        ret_ci->value_.get_int(ret);
      }
    }
  }

  return ret;
}

int64_t MutatorBuilder::get_cur_rowkey_info_(const ObSchema &schema, const bool using_id, const int64_t prefix_start)
{
  return query_prefix_meta_(schema, using_id, prefix_start, ROWKEY_INFO_COLUMN_NAME);
}

int64_t MutatorBuilder::get_suffix_length_(const ObSchema &schema, const bool using_id, const int64_t prefix_start)
{
  return query_prefix_meta_(schema, using_id, prefix_start, SUFFIX_LENGTH_COLUMN_NAME);
}

int64_t MutatorBuilder::get_prefix_end_(const ObSchema &schema, const bool using_id, const int64_t prefix_start)
{
  return query_prefix_meta_(schema, using_id, prefix_start, PREFIX_END_COLUMN_NAME);
}

int64_t MutatorBuilder::get_suffix_num_(const ObSchema &schema, const bool using_id, const int64_t prefix_start)
{
  return query_prefix_meta_(schema, using_id, prefix_start, SUFFIX_NUM_COLUMN_NAME);
}

int MutatorBuilder::put_suffix_length_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t suffix_length)
{
  return write_prefix_meta_(client, schema, prefix_start, suffix_length, SUFFIX_LENGTH_COLUMN_NAME);
}

int MutatorBuilder::put_prefix_end_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t prefix_end)
{
  return write_prefix_meta_(client, schema, prefix_start, prefix_end, PREFIX_END_COLUMN_NAME);
}

int MutatorBuilder::put_suffix_num_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t suffix_num)
{
  return write_prefix_meta_(client, schema, prefix_start, suffix_num, SUFFIX_NUM_COLUMN_NAME);
}

int MutatorBuilder::write_prefix_meta_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start,
                                      const int64_t param, const char *column_name)
{
  int ret = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
  ObString meta_column_name;
  meta_column_name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));
  ObString rowkey_info_rowkey;
  char rowkey_info_buffer[1024];
  sprintf(rowkey_info_buffer, "%s%020ld", ROWKEY_INFO_ROWKEY, prefix_start);
  rowkey_info_rowkey.assign_ptr(rowkey_info_buffer, static_cast<int32_t>(strlen(rowkey_info_buffer)));
  ObObj meta_obj;
  meta_obj.set_int(param);
  CharArena allocer;
  if (OB_SUCCESS == (ret = mutator.update(table_name, TestRowkeyHelper(rowkey_info_rowkey, &allocer), meta_column_name, meta_obj)))
  {
    ret = client.apply(mutator);
  }
  return ret;
}

int MutatorBuilder::build_mutator(ObMutator &mutator, PageArena<char> &allocer, const bool using_id, const int64_t cell_num)
{
  int ret = OB_SUCCESS;
  mutator.reset();
  const ObSchema *schema_array = schema_mgr_.begin();

  //seed_map_t &seed_map = seed_map_;
  //PageArena<char> &map_allocer = allocer_;
  seed_map_t seed_map;
  PageArena<char> map_allocer;
  int64_t rand = 0;
  lrand48_r(&table_rand_seed_, &rand);
  int64_t row_num = range_rand(1, max_rownum_per_mutator_, rand);
  seed_map.create(row_num);
  for (int64_t i = 0; i < row_num; i++)
  {
    lrand48_r(&table_rand_seed_, &rand);
    int64_t table_pos = range_rand(0, table_num_ - 1, rand);

    ObString row_key = rb_array_[table_pos]->get_rowkey4apply(map_allocer, prefix_end_array_[table_pos]);
    int64_t cur_seed = get_cur_seed_(seed_map, schema_array[table_pos], row_key, using_id);
    if (OB_SUCCESS != (ret = cb_array_[table_pos]->get_mutator(row_key, schema_array[table_pos], cell_num, cur_seed, mutator, allocer)))
    {
      break;
    }
    TEKey te_key;
    te_key.table_id = schema_array[table_pos].get_table_id();
    te_key.row_key = TestRowkeyHelper(row_key, &allocer);
    seed_map.set(te_key, cur_seed, 1);

    ObString table_name;
    table_name.assign_ptr(const_cast<char*>(schema_array[table_pos].get_table_name()), static_cast<int32_t>(strlen(schema_array[table_pos].get_table_name())));
    ObString rowkey_info_column_name;
    rowkey_info_column_name.assign_ptr((char*)ROWKEY_INFO_COLUMN_NAME, static_cast<int32_t>(strlen(ROWKEY_INFO_COLUMN_NAME)));
    ObString rowkey_info_rowkey;
    char rowkey_info_buffer[1024];
    sprintf(rowkey_info_buffer, "%s%020ld", ROWKEY_INFO_ROWKEY, rb_array_[table_pos]->get_prefix_start());
    rowkey_info_rowkey.assign_ptr(rowkey_info_buffer, static_cast<int32_t>(strlen(rowkey_info_buffer)));
    ObObj rowkey_info_obj;
    rowkey_info_obj.set_int(rb_array_[table_pos]->get_cur_prefix_end());
    if (OB_SUCCESS != (ret = mutator.update(table_name, TestRowkeyHelper(rowkey_info_rowkey, &allocer), rowkey_info_column_name, rowkey_info_obj)))
    {
      break;
    }
  }
  return ret;
}

int64_t MutatorBuilder::get_cur_seed_(const seed_map_t &seed_map, const ObSchema &schema, const ObString &row_key, const bool using_id)
{
  int64_t cur_seed = 0;

  TEKey te_key;
  te_key.table_id = schema.get_table_id();
  CharArena allocer;
  te_key.row_key = TestRowkeyHelper(row_key, &allocer);

  int hash_ret = 0;
  if (HASH_EXIST != (hash_ret = seed_map.get(te_key, cur_seed)))
  {
    ObGetParam get_param;
    ObScanner scanner;
    ObCellInfo cell_info;
    cell_info.row_key_ = TestRowkeyHelper(row_key, &allocer);
    if (using_id)
    {
      cell_info.table_id_ = schema.get_table_id();
      cell_info.column_id_ = SEED_COLUMN_ID;
    }
    else
    {
      cell_info.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
      cell_info.column_name_.assign_ptr((char*)SEED_COLUMN_NAME, static_cast<int32_t>(strlen(SEED_COLUMN_NAME)));
    }
    get_param.add_cell(cell_info);
    ObVersionRange version_range;
    version_range.start_version_ = table_start_version_;
    version_range.border_flag_.set_max_value();
    version_range.border_flag_.set_inclusive_start();
    get_param.set_version_range(version_range);
    if (OB_SUCCESS == client_.get(get_param, scanner))
    {
      CellinfoBuilder::result_set_t result_set;
      MemTank mem_tank;
      while (OB_SUCCESS == scanner.next_cell())
      {
        ObCellInfo *ci = NULL;
        if (OB_SUCCESS != scanner.get_cell(&ci))
        {
          break;
        }
        if (!using_id)
        {
          ::trans_name2id(*ci, schema);
        }
        ObCellInfo *new_ci = copy_cell(mem_tank, ci);
        CellinfoBuilder::merge_obj(new_ci, result_set);
      }
      if (0 != result_set.size())
      {
        ObCellInfo *ret_ci = NULL;
        result_set.get(SEED_COLUMN_ID, ret_ci);
        if (NULL != ret_ci)
        {
          ret_ci->value_.get_int(cur_seed);
        }
        TBSYS_LOG(DEBUG, "seed get from oceanbase %s", print_cellinfo(ret_ci));
      }
    }
  }
  if (0 == cur_seed)
  {
    cur_seed = SEED_START;
    TBSYS_LOG(DEBUG, "first seed from cur_seed=%ld row_key=[%.*s]", cur_seed, row_key.length(), row_key.ptr());
  }
  else
  {
    cur_seed += 1;
  }
  TBSYS_LOG(DEBUG, "get cur_seed=%ld hash_ret=%d row_key=[%.*s]", cur_seed, hash_ret, row_key.length(), row_key.ptr());
  return cur_seed;
}

int64_t MutatorBuilder::get_schema_pos(const uint64_t table_id) const
{
  int64_t ret = -1;
  for (int64_t i = 0; i < table_num_; i++)
  {
    if (schema_mgr_.begin()[i].get_table_id() == table_id)
    {
      ret = i;
      break;
    }
  }
  return ret;
}

int64_t MutatorBuilder::get_schema_pos(const ObString &table_name) const
{
  int64_t ret = -1;
  for (int64_t i = 0; i < table_num_; i++)
  {
    if (0 == memcmp(schema_mgr_.begin()[i].get_table_name(), table_name.ptr(), table_name.length()))
    {
      ret = i;
      break;
    }
  }
  return ret;
}

