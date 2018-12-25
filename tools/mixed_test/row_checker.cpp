#include "common/ob_crc64.h"
#include "common/ob_define.h"
#include "updateserver/ob_ups_utils.h"
#include "cellinfo_builder.h"
#include "utils.h"
#include "row_checker.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

RowChecker::RowChecker()
{
  last_is_del_row_ = false;
  rowkey_read_set_.create(RK_SET_SIZE);
  add_rowkey_times_ = 0;
}

RowChecker::~RowChecker()
{
}

void RowChecker::add_cell(const ObCellInfo *ci)
{
  if (0 != memcmp(ROWKEY_INFO_ROWKEY, ci->row_key_.ptr(), strlen(ROWKEY_INFO_ROWKEY)))
  {
    ObCellInfo *new_ci = (ObCellInfo*)ci_mem_tank_.node_alloc(sizeof(ObCellInfo));
    *new_ci = *ci;
    ci_mem_tank_.write_obj(ci->value_, &(new_ci->value_));
    if (NULL == cur_row_key_.ptr())
    {
      cur_row_key_.assign(NULL, 0);
      ci_mem_tank_.write_string(ci->row_key_, &cur_row_key_);
    }
    CellinfoBuilder::merge_obj(new_ci, read_result_);
    last_is_del_row_ = (ObActionFlag::OP_DEL_ROW == ci->value_.get_ext());
  }
}

int64_t RowChecker::cell_num() const
{
  return read_result_.size();
}

bool RowChecker::check_row(CellinfoBuilder &cb, const ObSchema &schema)
{
  bool bret = true;
  ObCellInfo *seed_ci = NULL;
  ObCellInfo *cell_num_ci = NULL;
  read_result_.get(SEED_COLUMN_ID, seed_ci);
  read_result_.get(CELL_NUM_COLUMN_ID, cell_num_ci);
  if (NULL == seed_ci
      || NULL == cell_num_ci)
  {
    TBSYS_LOG(WARN, "not seed column or cell_num column cellinfo [%s] [%s]",
              ::print_cellinfo(seed_ci), ::print_cellinfo(cell_num_ci));
    bret = false;
    abort();
  }
  else
  {
    int64_t seed_end = 0;
    int64_t cell_num = 0;
    seed_ci->value_.get_int(seed_end);
    cell_num_ci->value_.get_int(cell_num);
    CellinfoBuilder::result_set_t check_result;
    PageArena<char> allocer;
    cb.get_result(TestRowkeyHelper(cur_row_key_), schema, SEED_START, seed_end, cell_num, check_result, allocer);

    CellinfoBuilder::result_iter_t iter;
    for (iter = read_result_.begin(); iter != read_result_.end(); ++iter)
    {
      if (SEED_COLUMN_ID == iter->second->column_id_
          || CELL_NUM_COLUMN_ID == iter->second->column_id_
          || ROWKEY_INFO_COLUMN_ID == iter->second->column_id_
          || SUFFIX_LENGTH_COLUMN_ID == iter->second->column_id_
          || C_TIME_COLUMN_ID == iter->second->column_id_
          || M_TIME_COLUMN_ID == iter->second->column_id_
          || schema.is_rowkey_column(iter->second->column_id_))
      {
        continue;
      }
      else
      {
        ObCellInfo *tmp_ci = NULL;
        check_result.get(iter->first, tmp_ci);
        if (NULL == tmp_ci)
        {
          if (ObNullType != iter->second->value_.get_type())
          {
            TBSYS_LOG(WARN, "value not equal [%s] [%s]", ::print_cellinfo(iter->second), ::print_cellinfo(tmp_ci));
            bret = false;
            break;
          }
        }
        else if (tmp_ci->value_.get_type() == ObExtendType
                && tmp_ci->value_.get_ext() == iter->second->value_.get_ext())
        {
          continue;
        }
        else if (!(tmp_ci->value_ == iter->second->value_))
        {
          TBSYS_LOG(WARN, "value not equal [%s] [%s]", ::print_cellinfo(iter->second), ::print_cellinfo(tmp_ci));
          bret = false;
          abort();
          break;
        }
      }
    }
    if (last_is_del_row_)
    {
      TBSYS_LOG(INFO, "[row_is_deleted]");
    }
  }
  read_result_.clear();
  ci_mem_tank_.clear();
  cur_row_key_.assign(NULL, 0);
  last_is_del_row_ = false;
  return bret;
}

const ObRowkey &RowChecker::get_cur_rowkey() const
{
  return cur_row_key_;
}

bool RowChecker::is_prefix_changed(const ObRowkey &row_key)
{
  bool bret = false;
  if (NULL == last_row_key_.ptr() && NULL != row_key.ptr())
  {
    bret = true;
  }
  else if (NULL != last_row_key_.ptr() && NULL == row_key.ptr())
  {
    bret = true;
  }
  else if (0 != memcmp(to_obstr(TestRowkeyHelper(last_row_key_)).ptr(), to_obstr(TestRowkeyHelper(row_key)).ptr(), RowkeyBuilder::I64_STR_LENGTH))
  {
    bret = true;
  }
  return bret;
}

void RowChecker::add_rowkey(const ObRowkey &row_key)
{
  if (0 != memcmp(ROWKEY_INFO_ROWKEY, row_key.ptr(), strlen(ROWKEY_INFO_ROWKEY)))
  {
    ObRowkey new_rk;
    rk_mem_tank_.write_string(row_key, &new_rk);
    rowkey_read_set_.set(new_rk, 1, 1);
    last_row_key_ = new_rk;
    add_rowkey_times_++;
  }
}

bool RowChecker::check_rowkey(RowkeyBuilder &rb, const int64_t *prefix_ptr)
{
  bool bret = true;
  PageArena<char> allocer;
  ObHashMap<ObRowkey, uint64_t> rowkey_check_set;
  rowkey_check_set.create(RK_SET_SIZE);
  bool prefix_changed = true;
  while (add_rowkey_times_ > rowkey_check_set.size())
  {
    ObRowkey rowkey_check = TestRowkeyHelper(rb.get_rowkey4checkall(allocer, prefix_changed, prefix_ptr), &allocer);
    prefix_changed = false;
    rowkey_check_set.set(rowkey_check, 1, 1);
  }

  ObHashMap<ObRowkey, uint64_t>::iterator iter;
  for (iter = rowkey_read_set_.begin(); iter != rowkey_read_set_.end(); iter++)
  {
    uint64_t tmp = 0;
    if (HASH_EXIST != rowkey_check_set.get(iter->first, tmp))
    {
      bret = false;
      TBSYS_LOG(WARN, "read_rowkey=[%s] not found in check_set", to_cstring(iter->first));
    }
    else
    {
      rowkey_check_set.erase(iter->first);
    }
  }
  if (0 != rowkey_check_set.size())
  {
    bret = false;
    for (iter = rowkey_check_set.begin(); iter != rowkey_check_set.end(); iter++)
    {
      TBSYS_LOG(WARN, "rowkey=[%s] not in result_set", to_cstring(iter->first));
    }
  }
  assert(bret);
  rowkey_read_set_.clear();
  add_rowkey_times_ = 0;
  return bret;
}

const ObRowkey &RowChecker::get_last_rowkey() const
{
  return last_row_key_;
}

int64_t RowChecker::rowkey_num() const
{
  return rowkey_read_set_.size();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

RowExistChecker::RowExistChecker()
{
  hash_map_.create(DEFAULT_ROW_NUM_PER_MGET * DEFAULT_SUFFIX_NUM_PER_PREFIX);
  hash_set_.create(DEFAULT_ROW_NUM_PER_MGET);
}

RowExistChecker::~RowExistChecker()
{
}

bool RowExistChecker::check_row_not_exist(ClientWrapper &client, const ObCellInfo *ci,
                                          const bool using_id, const int64_t table_start_version, const int64_t timestamp)
{
  bool bret = true;
  int tmp_ret = OB_SUCCESS;

  int64_t prefix = RowkeyBuilder::get_prefix(TestRowkeyHelper(ci->row_key_));
  if (HASH_EXIST != hash_set_.exist(prefix))
  {
    PageArena<char> allocator;
    ObScanParam scan_param;

    if (using_id)
    {
      scan_param.add_column(C_TIME_COLUMN_ID);
    }
    else
    {
      scan_param.add_column(ObString(0, static_cast<int32_t>(strlen(C_TIME_COLUMN_NAME)), (char*)C_TIME_COLUMN_NAME));
    }

    std::pair<ObString,ObString> key_range = RowkeyBuilder::get_rowkey4scan(prefix, RowkeyBuilder::get_suffix_length(TestRowkeyHelper(ci->row_key_)), allocator);
    ObNewRange new_range;
    new_range.start_key_ = TestRowkeyHelper(key_range.first, &allocator);
    new_range.end_key_ = TestRowkeyHelper(key_range.second, &allocator);

    if (using_id)
    {
      scan_param.set(ci->table_id_, ObString(), new_range);
    }
    else
    {
      scan_param.set(OB_INVALID_ID, ci->table_name_, new_range);
    }

    ObVersionRange version_range;
    version_range.start_version_ = table_start_version;
    version_range.border_flag_.set_max_value();
    version_range.border_flag_.set_inclusive_start();
    scan_param.set_version_range(version_range);

    ObScanner scanner;
    tmp_ret = client.scan(scan_param, scanner);
    if (OB_SUCCESS == tmp_ret)
    {
      if (0 == scanner.get_row_num())
      {
        TBSYS_LOG(ERROR, "scan get nothing (%s)", to_cstring(new_range));
        tmp_ret = OB_ERROR;
      }
      else
      {
        while (OB_SUCCESS == (tmp_ret = scanner.next_cell()))
        {
          ObCellInfo *tmp_ci;
          if (OB_SUCCESS == (tmp_ret = scanner.get_cell(&tmp_ci)))
          {
            ObCreateTime tmp_c_time = 0;
            ObRowkey tmp_row_key;
            mem_tank_.write_string(tmp_ci->row_key_, &tmp_row_key);
            tmp_ci->value_.get_createtime(tmp_c_time);
            hash_map_.set(tmp_row_key, tmp_c_time);
          }
        }
        if (OB_ITER_END == tmp_ret)
        {
          hash_set_.set(prefix);
          tmp_ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_SUCCESS == tmp_ret)
  {
    int64_t c_time = 0;
    bret = (hash::HASH_EXIST == hash_map_.get(ci->row_key_, c_time));
    bret = bret ? (timestamp > c_time) : bret;
  }

  return bret;
}
