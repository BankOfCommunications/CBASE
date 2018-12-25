#include <vector>
#include "common/ob_list.h"
#include "test_utils.h"

using namespace oceanbase;
using namespace common;
using namespace common::hash;

const char *CELL_INFOS_INI = "./test_cases/cell_infos.ini";
const char *CELL_INFOS_SECTION = "CELL_INFOS_SECTION";
static const char *CELL_INFOS_NUM = "CELL_INFOS_NUM";
static const char *OP_TYPE_KEY_FORMAT = "OP_TYPE_%d";
static const char *TABLE_NAME_KEY_FORMAT = "TABLE_NAME_%d";
static const char *TABLE_ID_KEY_FORMAT = "TABLE_ID_%d";
static const char *ROW_KEY_DESC_KEY_FORMAT = "ROW_KEY_DESC_%d";
static const char *ROW_KEY_KEY_FORMAT = "ROW_KEY_%d";
static const char *COLUMN_NAME_KEY_FORMAT = "COLUMN_NAME_%d";
static const char *COLUMN_ID_KEY_FORMAT = "COLUMN_ID_%d";
static const char *VALUE_TYPE_KEY_FORMAT = "VALUE_TYPE_%d";
static const char *VALUE_KEY_FORMAT = "VALUE_%d";
static const char *RETURN_UPDATE_RESULT_FORMAT = "RETURN_UPDATE_RESULT_%d";

const char *SCAN_PARAM_INI = "./test_cases/scan_param.ini";
const char *SCAN_PARAM_SECTION = "SCAN_PARAM_SECTION";
static const char *SCAN_TABLE_ID = "SCAN_TABLE_ID";
static const char *SCAN_TABLE_NAME = "SCAN_TABLE_NAME";
static const char *SCAN_COLUMNS_NUM = "SCAN_COLUMNS_NUM";
static const char *SCAN_COLUMN_ID_KEY_FORMAT = "SCAN_COLUMN_ID_%d";
static const char *SCAN_COLUMN_NAME_KEY_FORMAT = "SCAN_COLUMN_NAME_%d";
static const char *SCAN_START_KEY = "SCAN_START_KEY";
static const char *SCAN_END_KEY = "SCAN_END_KEY";
static const char *SCAN_INCLUSIVE_START = "SCAN_INCLUSIVE_START";
static const char *SCAN_INCLUSIVE_END = "SCAN_INCLUSIVE_END";
static const char *SCAN_FROZEN_ONLY = "SCAN_FROZEN_ONLY";
static const char *SCAN_REVERSE = "SCAN_REVERSE";

const char *GET_PARAM_INI = "./test_cases/get_param.ini";
const char *GET_PARAM_SECTION = "GET_PARAM_SECTION";
static const char *GET_FROZEN_ONLY = "GET_FROZEN_ONLY";

static const int64_t BUFFER_SIZE = 1024;
static char buffer[BUFFER_SIZE];
static ObHashMap<const char*, int8_t> opt_map;
static ObHashMap<const char*, int> obj_type_map;

static void init_obj_type_map_()
{
  static bool inited = false;
  if (!inited)
  {
    obj_type_map.create(16);
    obj_type_map.set("null", ObNullType);
    obj_type_map.set("int", ObIntType);
    obj_type_map.set("float", ObFloatType);
    obj_type_map.set("double", ObDoubleType);
    obj_type_map.set("date_time", ObDateTimeType);
    obj_type_map.set("precise_date_time", ObPreciseDateTimeType);
    obj_type_map.set("var_char", ObVarcharType);
    obj_type_map.set("seq", ObSeqType);
    obj_type_map.set("create_time", ObCreateTimeType);
    obj_type_map.set("modify_time", ObModifyTimeType);
    obj_type_map.set("extend", ObExtendType);
    inited = true;
  }
}

static void init_opt_map_()
{
  static bool inited = false;
  if (!inited)
  {
    opt_map.create(16);
    opt_map.set("update", +ObActionFlag::OP_UPDATE, 1);
    opt_map.set("add", +ObActionFlag::OP_UPDATE, 1);
    opt_map.set("insert", +ObActionFlag::OP_INSERT, 1);
    //opt_map.set("read", +ObActionFlag::OP_READ, 1);
    //opt_map.set("delete", +ObActionFlag::OP_DEL, 1);
    opt_map.set("delete_row", +ObActionFlag::OP_DEL_ROW, 1);
    opt_map.set("db_sem", +ObActionFlag::OP_USE_DB_SEM, 1);
    opt_map.set("ob_sem", +ObActionFlag::OP_USE_OB_SEM, 1);
    inited = true;
  }
}

static size_t strlen_(const char *s)
{
  if (NULL == s)
  {
    return 0;
  }
  else
  {
    return strlen(s);
  }
}

static const char *getAndCopyString_(PageArena<char> &allocer, const char *section, const char *key, int32_t &len)
{
  char *ret = NULL;
  const char *str = TBSYS_CONFIG.getString(section, key);
  if (NULL == str)
  {
    len = 0;
  }
  else if (0 == strncmp("0x", str, 2))
  {
    const int64_t BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];
    int64_t pos = 0;
    const char *iter = str;
    while (0 != *iter)
    {
      char *next = NULL;
      buffer[pos++] = static_cast<char>(strtol(iter, &next, 16));
      iter = next;
    }
    buffer[pos] = 0;
    ret = allocer.alloc(pos + 1);
    memcpy(ret, buffer, pos + 1);
    len = static_cast<int32_t>(pos);
  }
  else
  {
    ret = allocer.alloc(strlen_(str) + 1);
    memcpy(ret, str, strlen_(str) + 1);
    len = static_cast<int32_t>(strlen_(str));
  }
  ret = (0 == len) ? NULL : ret;
  return ret;
}

static int parse_rowkey_type_array(const char* rowkey_type_str, int* array, int32_t& size)
{
  int ret = OB_SUCCESS;
  int type = ObNullType;
  std::vector<char*> list;
  tbsys::CStringUtil::split(const_cast<char*>(rowkey_type_str), ",", list);
  int32_t cnt = 0;
  for (size_t i = 0; i < list.size(); ++i)
  {
    if (hash::HASH_EXIST == obj_type_map.get(list[i], type))
    {
      if (cnt < size) array[cnt++] = type;
      else ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      ret = OB_ERROR;
      fprintf(stderr, "unrecognize type :(%s)\n", list[i]);
    }
  }
  size = cnt;
  return ret;
}

static int parse_rowkey_obj_array(PageArena<char> &allocer, const int* type_array, const int32_t size, const char* rowkey_value_str, ObObj* obj_array)
{
  int ret = OB_SUCCESS;
  int64_t tmp_value = 0;
  std::vector<char*> list;
  tbsys::CStringUtil::split(const_cast<char*>(rowkey_value_str), ",", list);
  if ((int32_t)list.size() != size)
  {
    ret = OB_ERROR;
    fprintf(stderr, "type array size = %d not match with value size =%d\n", size, (int32_t)list.size());
  }
  else
  {
    for (int i = 0; i < size; ++i)
    {
      tmp_value = strtoll(list[i], NULL, 10);
      switch (type_array[i])
      {
        case ObNullType:
          obj_array[i].set_null();
          break;
        case ObIntType:
          obj_array[i].set_int(tmp_value);
          break;
        case ObFloatType:
          tmp_value = strtoll(list[i], NULL, 10);
          obj_array[i].set_int(tmp_value);
          break;
        case ObDoubleType:
          obj_array[i].set_double(static_cast<double>(tmp_value));
          break;
        case ObDateTimeType:
          obj_array[i].set_datetime(tmp_value);
          break;
        case ObPreciseDateTimeType:
          obj_array[i].set_precise_datetime(tmp_value);
          break;
        case ObCreateTimeType:
          obj_array[i].set_createtime(tmp_value);
          break;
        case ObModifyTimeType:
          obj_array[i].set_modifytime(tmp_value);
          break;
        case ObVarcharType:
          if (0 == strncmp("0x", list[i], 2))
          {
            const int64_t BUFFER_SIZE = 1024;
            char buffer[BUFFER_SIZE];
            int64_t pos = 0;
            const char *iter = list[i];
            while (0 != *iter)
            {
              char *next = NULL;
              buffer[pos++] = static_cast<char>(strtol(iter, &next, 16));
              iter = next;
            }
            buffer[pos] = 0;
            char *str = allocer.alloc(pos + 1);
            memcpy(str, buffer, pos + 1);
            int32_t len = static_cast<int32_t>(pos);
            ObString str_value(0, len, str);
            obj_array[i].set_varchar(str_value);
          }
          else
          {
            ObString str_value(0, static_cast<int32_t>(strlen(list[i])), list[i]);
            obj_array[i].set_varchar(str_value);
          }
        case ObSeqType:
          break;
        case ObExtendType:
          if (strcmp(list[i], "min") == 0)
          {
            obj_array[i].set_min_value();
          }
          else if (strcmp(list[i], "max") == 0)
          {
            obj_array[i].set_max_value();
          }
          else
          {
            obj_array[i].set_ext(tmp_value);
          }
          break;
        default:
          break;
      }
    }
  }

  return ret;
}

static int parse_rowkey(const char* rowkey_type_str, const char* rowkey_value_str, PageArena<char>& allocer, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int size = OB_MAX_ROWKEY_COLUMN_NUMBER;
  int type_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey tmp_rowkey;

  ret = parse_rowkey_type_array(rowkey_type_str, type_array, size);
  if (OB_SUCCESS == ret)
  {
    ret = parse_rowkey_obj_array(allocer, type_array, size, rowkey_value_str, obj_array);
  }

  if (OB_SUCCESS == ret)
  {
    tmp_rowkey.assign(obj_array, size);
    ret = tmp_rowkey.deep_copy(rowkey, allocer);
  }

  return ret;
}

void read_get_param(const char *fname, const char *section, PageArena<char> &allocer, ObGetParam &get_param)
{
  init_opt_map_();
  init_obj_type_map_();
  TBSYS_CONFIG.load(fname);
  const char *str = NULL;

  int32_t cell_infos_num = TBSYS_CONFIG.getInt(section, CELL_INFOS_NUM);
  str = TBSYS_CONFIG.getString(section, GET_FROZEN_ONLY);
  if (0 == strcmp("YES", str))
  {
    //get_param.set_is_read_frozen_only(true);
  }
  for (int32_t i = 0; i < cell_infos_num; i++)
  {
    const char *str = NULL;
    int32_t len = 0;
    ObCellInfo *ci = (ObCellInfo*)allocer.alloc(sizeof(ObCellInfo));
    ci->reset();

    // table name
    sprintf(buffer, TABLE_NAME_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    ci->table_name_.assign_ptr(const_cast<char*>(str), len);

    // table id
    if (NULL == str)
    {
      sprintf(buffer, TABLE_ID_KEY_FORMAT, i);
      ci->table_id_ = TBSYS_CONFIG.getInt(section, buffer);
    }
    else
    {
      ci->table_id_ = OB_INVALID_ID;
    }

    // row key
    const char* rowkey_desc = NULL;
    sprintf(buffer, ROW_KEY_DESC_KEY_FORMAT, i);
    rowkey_desc = TBSYS_CONFIG.getString(section, buffer);

    sprintf(buffer, ROW_KEY_KEY_FORMAT, i);
    str = TBSYS_CONFIG.getString(section, buffer);
    parse_rowkey(rowkey_desc, str, allocer, ci->row_key_);

    //ci->row_key_.assign_ptr(const_cast<char*>(str), len);

    // column name
    sprintf(buffer, COLUMN_NAME_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    ci->column_name_.assign_ptr(const_cast<char*>(str), len);

    // column id
    if (NULL == str)
    {
      sprintf(buffer, COLUMN_ID_KEY_FORMAT, i);
      ci->column_id_ = TBSYS_CONFIG.getInt(section, buffer);
    }
    else
    {
      ci->column_id_ = OB_INVALID_ID;
    }

    fprintf(stderr, "get rowkey=%s,column_name=%.*s\n", to_cstring(ci->row_key_), len, ci->column_name_.ptr());
    get_param.add_cell(*ci);
  }
}

void read_scan_param(const char *fname, const char *section, PageArena<char> &allocer, ObScanParam &scan_param)
{
  init_obj_type_map_();
  TBSYS_CONFIG.load(fname);
  const char *str = NULL;
  int32_t len = 0;

  str = TBSYS_CONFIG.getString(section, SCAN_FROZEN_ONLY);
  if (0 == strcmp("YES", str))
  {
    //scan_param.set_is_read_frozen_only(true);
  }

  str = TBSYS_CONFIG.getString(section, SCAN_REVERSE);
  if (NULL == str
      || 0 != strcmp("YES", str))
  {
    scan_param.set_scan_direction(ScanFlag::FORWARD);
  }
  else
  {
    scan_param.set_scan_direction(ScanFlag::BACKWARD);
  }

  ObString table_name;
  sprintf(buffer, SCAN_TABLE_NAME);
  str = getAndCopyString_(allocer, section, buffer, len);
  table_name.assign_ptr(const_cast<char*>(str), len);

  uint64_t table_id = OB_INVALID_ID;
  if (NULL == str)
  {
    table_id = TBSYS_CONFIG.getInt(section, SCAN_TABLE_ID);
  }
  else
  {
    table_id = OB_INVALID_ID;
  }

  int32_t columns_num = TBSYS_CONFIG.getInt(section, SCAN_COLUMNS_NUM);
  for (int32_t i = 0; i < columns_num; i++)
  {
    ObString column_name;
    sprintf(buffer, SCAN_COLUMN_NAME_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    column_name.assign_ptr(const_cast<char*>(str), len);
    if (NULL != str)
    {
      scan_param.add_column(column_name);
    }
    else
    {
      sprintf(buffer, SCAN_COLUMN_ID_KEY_FORMAT, i);
      uint64_t column_id = TBSYS_CONFIG.getInt(section, buffer);
      scan_param.add_column(column_id);
    }
  }

  const char* rowkey_desc_start = TBSYS_CONFIG.getString(section, "ROW_KEY_DESC");
  char* rowkey_desc_end = strdup(rowkey_desc_start);


  ObNewRange range;
  range.table_id_ = table_id;
  str = TBSYS_CONFIG.getString(section, SCAN_START_KEY);
  parse_rowkey(rowkey_desc_start, str, allocer, range.start_key_);
  //str = getAndCopyString_(allocer, section, SCAN_START_KEY, len);
  //range.start_key_.assign_ptr(const_cast<char*>(str), len);
  if (0 == strcmp("MIN_KEY", str))
  {
    range.start_key_.set_min_row();
  }
  if (0 == strcmp("MAX_KEY", str))
  {
    range.end_key_.set_max_row();
  }

  str = TBSYS_CONFIG.getString(section, SCAN_END_KEY);
  parse_rowkey(rowkey_desc_end, str, allocer, range.end_key_);
  //str = getAndCopyString_(allocer, section, SCAN_END_KEY, len);
  //range.end_key_.assign_ptr(const_cast<char*>(str), len);
  if (0 == strcmp("MIN_KEY", str))
  {
    range.start_key_.set_min_row();
  }
  if (0 == strcmp("MAX_KEY", str))
  {
    range.end_key_.set_max_row();
  }

  str = getAndCopyString_(allocer, section, SCAN_INCLUSIVE_START, len);
  if (0 == strcmp("YES", str))
  {
    range.border_flag_.set_inclusive_start();
  }
  str = getAndCopyString_(allocer, section, SCAN_INCLUSIVE_END, len);
  if (0 == strcmp("YES", str))
  {
    range.border_flag_.set_inclusive_end();
  }

  scan_param.set(table_id, table_name, range);
}

void read_cell_infos(const char *fname, const char *section, PageArena<char> &allocer, ObMutator &mutator, ObMutator &result)
{
  init_opt_map_();
  init_obj_type_map_();
  TBSYS_CONFIG.load(fname);
  int32_t cell_infos_num = TBSYS_CONFIG.getInt(section, CELL_INFOS_NUM);
  std::vector<int64_t> table_ids;
  std::vector<int64_t> column_ids;
  for (int32_t i = 0; i < cell_infos_num; i++)
  {
    const char *str = NULL;
    int32_t len = 0;
    ObCellInfo *ci = (ObCellInfo*)allocer.alloc(sizeof(ObCellInfo));
    //ObCellInfo *op = (ObCellInfo*)allocer.alloc(sizeof(ObCellInfo));
    ci->reset();

    sprintf(buffer, OP_TYPE_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    int8_t op_type = 0;
    bool is_add = (0 == strncmp("add", str, 3));
    opt_map.get(str, op_type);

    sprintf(buffer, RETURN_UPDATE_RESULT_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    str = (NULL == str) ? "NO" : str;
    int return_flag = (0 == strncmp("YES", str, 3)) ? ObMutator::RETURN_UPDATE_RESULT : ObMutator::RETURN_NO_RESULT;

    // table name
    sprintf(buffer, TABLE_NAME_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    ci->table_name_.assign_ptr(const_cast<char*>(str), len);

    // table id
    sprintf(buffer, TABLE_ID_KEY_FORMAT, i);
    ci->table_id_ = TBSYS_CONFIG.getInt(section, buffer);

    // row key
    const char* rowkey_desc = NULL;
    sprintf(buffer, ROW_KEY_DESC_KEY_FORMAT, i);
    rowkey_desc = TBSYS_CONFIG.getString(section, buffer);

    sprintf(buffer, ROW_KEY_KEY_FORMAT, i);
    str = TBSYS_CONFIG.getString(section, buffer);
    parse_rowkey(rowkey_desc, str, allocer, ci->row_key_);

    //sprintf(buffer, ROW_KEY_KEY_FORMAT, i);
    //str = getAndCopyString_(allocer, section, buffer, len);
    //ci->row_key_.assign_ptr(const_cast<char*>(str), len);

    // column name
    sprintf(buffer, COLUMN_NAME_KEY_FORMAT, i);
    str = getAndCopyString_(allocer, section, buffer, len);
    ci->column_name_.assign_ptr(const_cast<char*>(str), len);

    // column id
    sprintf(buffer, COLUMN_ID_KEY_FORMAT, i);
    ci->column_id_ = TBSYS_CONFIG.getInt(section, buffer);

    //*op = *ci;
    //op->value_.set_ext(op_type);
    //mutator.add_cell(*op);
    //table_ids.push_back(op->table_id_);
    //column_ids.push_back(op->column_id_);
    //if (ObActionFlag::OP_DEL_ROW == op_type)
    //{
    //  result.add_cell(*op);
    //  continue;
    //}

    // value type
    sprintf(buffer, VALUE_TYPE_KEY_FORMAT, i);
    str = TBSYS_CONFIG.getString(section, buffer);
    int value_type = 0;
    obj_type_map.get(str, value_type);

    // value
    sprintf(buffer, VALUE_KEY_FORMAT, i);
    if (ObVarcharType == value_type)
    {
      str = getAndCopyString_(allocer, section, buffer, len);
      ObString varchar;
      varchar.assign_ptr(const_cast<char*>(str), len);
      //char *tmp = (char*)malloc(1024 * 1024 + 512 * 1024);
      //memset(tmp, 'a', 1024 * 1024 + 512 * 1024);
      //varchar.assign_ptr(tmp, 1024 * 1024 + 512 * 1024);
      ci->value_.set_varchar(varchar);
    }
    else if (ObIntType == value_type)
    {
      int64_t value = TBSYS_CONFIG.getInt(section, buffer);
      ci->value_.set_int(value, is_add);
    }
    else if (ObFloatType == value_type)
    {
      str = TBSYS_CONFIG.getString(section, buffer);
      float value = static_cast<float>(atof(str));
      ci->value_.set_float(value, is_add);
    }
    else if (ObDoubleType == value_type)
    {
      str = TBSYS_CONFIG.getString(section, buffer);
      double value = atof(str);
      ci->value_.set_double(value, is_add);
    }
    else if (ObDateTimeType == value_type)
    {
      int64_t value = TBSYS_CONFIG.getInt(section, buffer);
      ci->value_.set_datetime(value);
    }
    else if(ObPreciseDateTimeType == value_type)
    {
      int64_t value = TBSYS_CONFIG.getInt(section, buffer);
      ci->value_.set_precise_datetime(value);
    }
    else if (ObSeqType == value_type)
    {
      ci->value_.set_seq();
    }
    else if (ObCreateTimeType == value_type)
    {
      int64_t value = TBSYS_CONFIG.getInt(section, buffer);
      ci->value_.set_createtime(value);
    }
    else if(ObModifyTimeType == value_type)
    {
      int64_t value = TBSYS_CONFIG.getInt(section, buffer);
      ci->value_.set_modifytime(value);
    }
    else if (ObNullType == value_type)
    {
      ci->value_.set_null();
    }
    else
    {
      fprintf(stderr, "value_type=%d is not supported\n", value_type);
    }

    fprintf(stderr, "rowkey=%s,value=%s\n", to_cstring(ci->row_key_), to_cstring(ci->value_));

    switch (op_type)
    {
      case ObActionFlag::OP_UPDATE:
        mutator.update(ci->table_name_, ci->row_key_, ci->column_name_, ci->value_, return_flag);
        break;
      case ObActionFlag::OP_INSERT:
        mutator.insert(ci->table_name_, ci->row_key_, ci->column_name_, ci->value_, return_flag);
        break;
      case ObActionFlag::OP_DEL_ROW:
        mutator.del_row(ci->table_name_, ci->row_key_);
        break;
      case ObActionFlag::OP_USE_DB_SEM:
        mutator.use_db_sem();
        break;
      case ObActionFlag::OP_USE_OB_SEM:
        mutator.use_ob_sem();
        break;
      default:
        fprintf(stderr, "op_type not support %d\n", op_type);
        break;
    }
    table_ids.push_back(ci->table_id_);
    column_ids.push_back(ci->column_id_);
    ObMutatorCellInfo mutator_ci;
    mutator_ci.cell_info = *ci;
    result.add_cell(mutator_ci);
  }

  for (int32_t i = 0; i < cell_infos_num; i++)
  {
    result.next_cell();
    ObMutatorCellInfo *mutator_ci = NULL;
    ObCellInfo *ci = NULL;
    result.get_cell(&mutator_ci);
    ci = &(mutator_ci->cell_info);

    // table id
    sprintf(buffer, TABLE_ID_KEY_FORMAT, i);
    ci->table_id_ = TBSYS_CONFIG.getInt(section, buffer);

    // column id
    sprintf(buffer, COLUMN_ID_KEY_FORMAT, i);
    ci->column_id_ = TBSYS_CONFIG.getInt(section, buffer);
  }
  result.reset_iter();

  for (uint64_t i = 0; i < table_ids.size(); i++)
  {
    mutator.next_cell();
    ObMutatorCellInfo *mutator_ci = NULL;
    ObCellInfo *ci = NULL;
    mutator.get_cell(&mutator_ci);
    ci = &(mutator_ci->cell_info);

    // table id
    ci->table_id_ = table_ids[i];

    // column id
    ci->column_id_ = column_ids[i];
  }
  mutator.reset_iter();

  int64_t pos = 0;
  int64_t size = 4 * 1024 * 1024;
  char *buffer = new char[size];
  mutator.serialize(buffer, size, pos);
  mutator.reset();
  pos = 0;
  mutator.deserialize(buffer, size, pos);
  delete[] buffer;

  reverse_col_in_row(result);
}

const char *print_obj(const ObObj &obj)
{
  static __thread char buffer[1024 * 10];
  switch (obj.get_type())
  {
    case ObNullType:
      sprintf(buffer, "obj_type=null");
      break;
    case ObIntType:
      {
        bool is_add = false;
        int64_t tmp = 0;
        obj.get_int(tmp, is_add);
        sprintf(buffer, "obj_type=int value=%ld is_add=%s", tmp, STR_BOOL(is_add));
      }
      break;
    case ObFloatType:
      {
        bool is_add = false;
        float tmp = 0.0;
        obj.get_float(tmp, is_add);
        sprintf(buffer, "obj_type=float value=%f is_add=%s", tmp, STR_BOOL(is_add));
      }
      break;
    case ObDoubleType:
      {
        bool is_add = false;
        double tmp = 0.0;
        obj.get_double(tmp, is_add);
        sprintf(buffer, "obj_type=double value=%lf is_add=%s", tmp, STR_BOOL(is_add));
      }
      break;
    case ObDateTimeType:
      {
        bool is_add = false;
        ObDateTime tmp = 0;
        obj.get_datetime(tmp, is_add);
        sprintf(buffer, "obj_type=data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
      }
      break;
    case ObPreciseDateTimeType:
      {
        bool is_add = false;
        ObDateTime tmp = 0;
        obj.get_precise_datetime(tmp, is_add);
        sprintf(buffer, "obj_type=precise_data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
      }
      break;
    case ObVarcharType:
      {
        ObString tmp;
        obj.get_varchar(tmp);
        sprintf(buffer, "obj_type=var_char value=[%.*s]", tmp.length(), tmp.ptr());
      }
      break;
    case ObSeqType:
      {
        sprintf(buffer, "obj_type=seq");
      }
      break;
    case ObCreateTimeType:
      {
        ObCreateTime tmp = 0;
        obj.get_createtime(tmp);
        snprintf(buffer, BUFFER_SIZE, "obj_type=create_time value=%s", time2str(tmp));
      }
      break;
    case ObModifyTimeType:
      {
        ObModifyTime tmp = 0;
        obj.get_modifytime(tmp);
        snprintf(buffer, BUFFER_SIZE, "obj_type=modify_time value=%s", time2str(tmp));
      }
      break;
    case ObExtendType:
      {
        int64_t tmp = 0;
        obj.get_ext(tmp);
        sprintf(buffer, "obj_type=extend value=%ld", tmp);
      }
      break;
    default:
      break;
  }
  return buffer;
}

void print_cellinfo(const ObCellInfo *ci, const char *ext_info/* = NULL*/)
{
  char row_key[2048];
  row_key[0] = '\0';
  //hex_to_str(ci->row_key_.ptr(), ci->row_key_.length(), row_key, sizeof(row_key));
  if (NULL != ci)
  {
    if (NULL == ext_info)
    {
      fprintf(stderr, "table_id=%lu table_name=[%.*s] row_key=[%s] column_id=%lu column_name=[%.*s] %s\n",
          ci->table_id_,
          ci->table_name_.length(), ci->table_name_.ptr(),
          row_key,
          ci->column_id_,
          ci->column_name_.length(), ci->column_name_.ptr(),
          ::print_obj(ci->value_));
    }
    else
    {
      fprintf(stderr, "%s table_id=%lu table_name=[%.*s] row_key=[%s] column_id=%lu column_name=[%.*s] %s\n",
          ext_info,
          ci->table_id_,
          ci->table_name_.length(), ci->table_name_.ptr(),
          row_key,
          ci->column_id_,
          ci->column_name_.length(), ci->column_name_.ptr(),
          ::print_obj(ci->value_));
    }
  }
}

bool equal(const ObCellInfo &a, const ObCellInfo &b)
{
  bool bret = false;
  if (ObExtendType == a.value_.get_type()
      && ObExtendType == b.value_.get_type()
      && a.value_.get_ext() == b.value_.get_ext())
  {
    bret = true;
  }
  else if (a.table_id_ == b.table_id_
      && a.row_key_ == b.row_key_
      && a.column_id_ == b.column_id_)
  {
    bret = (const_cast<ObCellInfo&>(a).value_ == b.value_);
  }
  if (!bret)
  {
    fprintf(stderr, "[%s] [%s] not equal\n", common::print_cellinfo(&a), common::print_cellinfo(&b));
  }
  return bret;
}

//bool operator == (const UpsCellInfo &a, const UpsCellInfo &b)
//{
//  bool bret = false;
//  if (ObExtendType == a.value_.get_type()
//      && ObExtendType == b.value_.get_type()
//      && a.value_.get_ext() == b.value_.get_ext())
//  {
//    bret = true;
//  }
//  else if (a.column_id_ == b.column_id_)
//  {
//    bret = (const_cast<UpsCellInfo&>(a).value_ == b.value_);
//  }
//  return bret;
//}

ObVersionRange str2range(const char *str)
{
  ObVersionRange ret;
  char *iter = (char*)alloca(strlen(str) + 1);
  strcpy(iter, str);
  if ('[' == *iter)
  {
    ret.border_flag_.set_inclusive_start();
  }

  iter++;
  if (0 == strncmp("MIN", iter, 3))
  {
    ret.border_flag_.set_min_value();
    while (',' != *iter
          && '\0' != *iter)
    {
      iter++;
    }
  }
  else
  {
    const char *tmp = iter;
    while (',' != *iter)
    {
      iter++;
    }
    *iter = '\0';
    ret.start_version_ = atol(tmp);
    *iter = ',';
  }

  iter++;

  if (0 == strncmp("MAX", iter, 3))
  {
    ret.border_flag_.set_max_value();
    while (']' != *iter
          && ')' != *iter)
    {
      iter++;
    }
  }
  else
  {
    const char *tmp = iter;
    while (']' != *iter
          && ')' != *iter
          && '\0' != *iter)
    {
      iter++;
    }
    char r = *iter;
    *iter = '\0';
    ret.end_version_ = atol(tmp);
    *iter = r;
  }

  if (']' == *iter)
  {
    ret.border_flag_.set_inclusive_end();
  }
  return ret;
}

void reverse_col_in_row(ObMutator &mutator)
{
  ObMutator tmp_mutator;
  ObList<ObMutatorCellInfo> tmp_list;
  prepare_mutator(mutator);
  while (OB_SUCCESS == mutator.next_cell())
  {
    ObMutatorCellInfo *mci = NULL;
    bool irc = false;
    if (OB_SUCCESS == mutator.get_cell(&mci, &irc, NULL)
        && NULL != mci)
    {
      if (irc && 0 != tmp_list.size())
      {
        ObList<ObMutatorCellInfo>::iterator iter;
        for (iter = tmp_list.begin(); iter != tmp_list.end(); iter++)
        {
          tmp_mutator.add_cell(*iter);
        }
        tmp_list.clear();
      }
      tmp_list.push_front(*mci);
    }
  }
  if (0 != tmp_list.size())
  {
    ObList<ObMutatorCellInfo>::iterator iter;
    for (iter = tmp_list.begin(); iter != tmp_list.end(); iter++)
    {
      tmp_mutator.add_cell(*iter);
    }
    tmp_list.clear();
  }
  dup_mutator(tmp_mutator, mutator);
}

void prepare_mutator(ObMutator &mutator)
{
  int64_t pos = 0;
  int64_t size = 2 * 1024 * 1024;
  char *buffer = new char[size];
  mutator.serialize(buffer, size, pos);
  pos = 0;
  mutator.deserialize(buffer, size, pos);
  delete[] buffer;
}

void prepare_cell_new_scanner(ObCellNewScanner &scanner)
{
  int64_t pos = 0;
  int64_t size = 2 * 1024 * 1024;
  char *buffer = new char[size];
  int err = scanner.serialize(buffer, size, pos);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "ObCellNewScanner serialize error, err: %d", err);
  }
  else
  {
    pos = 0;
    scanner.reuse();
    err = scanner.deserialize(buffer, size, pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObCellNewScanner deserialize error, err: %d", err);
    }
  }
  delete[] buffer;
}

void prepare_cell_new_scanner(const ObCellNewScanner &scanner, ObCellNewScanner &out_scanner)
{
  int64_t pos = 0;
  int64_t size = 2 * 1024 * 1024;
  char *buffer = new char[size];
  int err = scanner.serialize(buffer, size, pos);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "ObCellNewScanner serialize error, err: %d", err);
  }
  else
  {
    pos = 0;
    err = out_scanner.deserialize(buffer, size, pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObCellNewScanner deserialize error, err: %d", err);
    }
  }
  delete[] buffer;
}

void dup_mutator(const ObMutator &src, ObMutator &dest)
{
  int64_t pos = 0;
  int64_t size = 2 * 1024 * 1024;
  char *buffer = new char[size];
  src.serialize(buffer, size, pos);
  pos = 0;
  dest.deserialize(buffer, size, pos);
  delete[] buffer;
}

/*
int main(int argc, char **argv)
{
  init_opt_map_();
  ObMutator mutator;
  PageArena<char> allocer;
  read_cell_infos(CELL_INFOS_INI, CELL_INFOS_SECTION, allocer, mutator);
  ObScanParam scan_param;
  read_scan_param(SCAN_PARAM_INI, SCAN_PARAM_SECTION, allocer, scan_param);

  while (OB_SUCCESS == mutator.next_cell())
  {
    ObCellInfo *ci = NULL;
    mutator.get_cell(&ci);
    print_cellinfo(ci);
  }
}
*/
