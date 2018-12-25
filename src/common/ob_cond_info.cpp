#include "ob_action_flag.h"
#include "ob_string_buf.h"
#include "ob_cond_info.h"
#include "ob_rowkey_helper.h"
#include "utility.h"

using namespace oceanbase::common;

ObCondInfo::ObCondInfo()
{
  op_type_ = NIL;
  rowkey_info_ = NULL;
  memset(rowkey_col_buf_, 0, sizeof(ObObj)*OB_MAX_ROWKEY_COLUMN_NUMBER);
}


ObCondInfo::~ObCondInfo()
{
}

void ObCondInfo::reset()
{
  op_type_ = NIL;
  rowkey_info_ = NULL;
  memset(rowkey_col_buf_, 0, sizeof(ObObj)*OB_MAX_ROWKEY_COLUMN_NUMBER);
  cell_.reset();
}

void ObCondInfo::set(const ObLogicOperator op_type, const ObString & table_name,
    const ObRowkey& row_key, const ObString & column_name, const ObObj & value)
{
  op_type_ = op_type;
  cell_.table_name_ = table_name;
  cell_.row_key_ = row_key;
  cell_.column_name_ = column_name;
  cell_.value_ = value;
}


void ObCondInfo::set(const ObString & table_name, const ObRowkey& row_key, const bool is_exist)
{
  op_type_ = NIL;
  cell_.table_name_ = table_name;
  cell_.row_key_ = row_key;
  cell_.column_name_.assign(NULL, 0);
  if (!is_exist)
  {
    cell_.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  }
  else
  {
    cell_.value_.set_ext(ObActionFlag::OP_ROW_EXIST);
  }
}

void ObCondInfo::set(const ObLogicOperator op_type, const uint64_t table_id, const ObRowkey& row_key,
    const uint64_t column_id, const ObObj & value)
{
  op_type_ = op_type;
  cell_.table_id_ = table_id;
  cell_.row_key_ = row_key;
  cell_.column_id_ = column_id;
  cell_.value_ = value;
}

void ObCondInfo::set(const uint64_t table_id, const ObRowkey& row_key, const bool is_exist)
{
  op_type_ = NIL;
  cell_.table_id_ = table_id;
  cell_.row_key_ = row_key;
  // WARNING: must use OB_ALL_MAX_COLUMN_ID for check row exist
  cell_.column_id_ = OB_ALL_MAX_COLUMN_ID;
  if (!is_exist)
  {
    cell_.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  }
  else
  {
    cell_.value_.set_ext(ObActionFlag::OP_ROW_EXIST);
  }
}

void ObCondInfo::set(const ObLogicOperator op_type, const ObCellInfo & cell)
{
  op_type_ = op_type;
  cell_ = cell;
}

DEFINE_SERIALIZE(ObCondInfo)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_int(op_type_);
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize op type failed:type[%d], ret[%d]", op_type_, ret);
  }
  else if (OB_INVALID_ID != cell_.table_id_)
  {
    obj.set_int(cell_.table_id_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize talbe id failed:table_id[%lu], ret[%d]", cell_.table_id_, ret);
    }
    else
    {
      obj.set_int(cell_.column_id_);
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize column id failed:column_id[%lu], ret[%d]",
            cell_.column_id_, ret);
      }
    }
  }
  else
  {
    obj.set_varchar(cell_.table_name_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize talbe name failed:table_name[%.*s], ret[%d]",
          cell_.table_name_.length(), cell_.table_name_.ptr(), ret);
    }
    else
    { 
      obj.set_varchar(cell_.column_name_);
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize column name failed:column_name[%.*s], ret[%d]",
            cell_.column_name_.length(), cell_.column_name_.ptr(), ret);
      }
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = cell_.row_key_.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize rowkey failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = cell_.value_.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize cell value failed:ret[%d]", ret);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObCondInfo)
{
  ObObj obj;
  int ret = obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize op type failed:ret[%d]", ret);
  }
  else if (ObIntType == obj.get_type())
  {
    int64_t type = 0;
    ret = obj.get_int(type);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get op type int value failed:ret[%d]", ret);
    }
    else
    {
      op_type_ = (ObLogicOperator)type;
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check op type obj type failed:type[%d]", obj.get_type());
  }

  if (OB_SUCCESS == ret)
  {
    ret = obj.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize table failed:ret[%d]", ret);
    }
    else if (ObIntType == obj.get_type())
    {
      ret = obj.get_int((int64_t &)cell_.table_id_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check get int table id failed:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "deserialize column id failed:table_id[%lu], ret[%d]",
            cell_.table_id_, ret);
      }
      else if (OB_SUCCESS != (ret = obj.get_int((int64_t &)cell_.column_id_)))
      {
        TBSYS_LOG(WARN, "check get int column id failed:table_id[%lu], ret[%d]",
            cell_.table_id_, ret);
      }
    }
    else
    {
      ret = obj.get_varchar(cell_.table_name_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check get varchar table name failed:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "deserialize column name failed:table_name[%.*s], ret[%d]",
            cell_.table_name_.length(), cell_.table_name_.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = obj.get_varchar(cell_.column_name_)))
      {
        TBSYS_LOG(WARN, "check get varchar column name failed:table_name[%.*s], ret[%d]",
            cell_.table_name_.length(), cell_.table_name_.ptr(), ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t obj_cnt  = OB_MAX_ROWKEY_COLUMN_NUMBER;
    bool    is_binary_rowkey = false;
    ObRowkeyInfo info;
    if (NULL != rowkey_info_) { info = *rowkey_info_; }
    if (OB_SUCCESS != (ret = get_rowkey_compatible(buf, data_len, pos, 
            info, rowkey_col_buf_, obj_cnt, is_binary_rowkey)))
    {
      TBSYS_LOG(WARN, "deserialize rowkey failed:ret[%d]", ret);
    }
    else if (is_binary_rowkey && NULL == rowkey_info_)
    {
      TBSYS_LOG(WARN, "have not set rowkey_info cannot deserialize rowkey of string type");
    }
    else
    {
      cell_.row_key_.assign(rowkey_col_buf_, obj_cnt);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = cell_.value_.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize cond value failed:ret[%d]", ret);
    }
  }
  return ret;
}

// no check id type or name type
DEFINE_GET_SERIALIZE_SIZE(ObCondInfo)
{
  ObObj obj;
  obj.set_int(op_type_);
  int64_t total_size = obj.get_serialize_size();
  // id type
  if (OB_INVALID_ID != cell_.table_id_)
  {
    obj.set_int(cell_.table_id_);
    total_size += obj.get_serialize_size();
    obj.set_int(cell_.column_id_);
    total_size += obj.get_serialize_size();
  }
  else
  {
    obj.set_varchar(cell_.table_name_);
    total_size += obj.get_serialize_size();
    obj.set_varchar(cell_.column_name_);
    total_size += obj.get_serialize_size();
  }
  total_size += cell_.row_key_.get_serialize_size();
  total_size += cell_.value_.get_serialize_size();
  return total_size;
}

bool ObCondInfo::operator == (const ObCondInfo & other) const
{
  return ((op_type_ == other.op_type_) && (cell_ == other.cell_));
}

int ObCondInfo::deep_copy(ObCondInfo & dest, ObStringBuf & buffer) const
{
  int ret = OB_SUCCESS;
  dest.op_type_ = op_type_;
  if (OB_INVALID_ID != cell_.table_id_)
  {
    dest.cell_.table_id_ = cell_.table_id_;
    dest.cell_.column_id_ = cell_.column_id_;
  }
  else
  {
    ret = buffer.write_string(cell_.table_name_, &dest.cell_.table_name_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "write string for table name failed:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = buffer.write_string(cell_.column_name_, &dest.cell_.column_name_)))
    {
      TBSYS_LOG(WARN, "write string for column name failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = buffer.write_string(cell_.row_key_, &dest.cell_.row_key_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "write string for row key failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = buffer.write_obj(cell_.value_, &dest.cell_.value_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "write obj for value failed:ret[%d]", ret);
    }
  }
  return ret;
}

