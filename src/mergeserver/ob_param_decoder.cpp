#include "ob_param_decoder.h"
#include "common/ob_schema.h"
#include "common/ob_cond_info.h"
#include "common/ob_common_param.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int ObParamDecoder::decode_cell(const bool is_ext, const ObCellInfo & org_cell,
    const ObSchemaManagerV2 & schema, ObCellInfo & decoded_cell)
{
  int ret = OB_SUCCESS;
  if (false == is_ext)
  {
    const ObColumnSchemaV2 * column_schema = schema.get_column_schema(org_cell.table_name_,
        org_cell.column_name_);
    if (NULL == column_schema)
    {
      ret = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "check column failed:table[%.*s], column[%.*s], ret[%d]",
          org_cell.table_name_.length(), org_cell.table_name_.ptr(),
          org_cell.column_name_.length(), org_cell.column_name_.ptr(), ret);
    }
    else
    {
      decoded_cell.table_id_ = column_schema->get_table_id();
      decoded_cell.column_id_ = column_schema->get_id();
      decoded_cell.row_key_ = org_cell.row_key_;
      decoded_cell.value_ = org_cell.value_;
    }
  }
  else
  {
    const ObTableSchema * table_schema = schema.get_table_schema(org_cell.table_name_);
    if (NULL == table_schema)
    {
      ret = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "check table failed:table[%.*s], ret[%d]", org_cell.table_name_.length(),
          org_cell.table_name_.ptr(), ret);
    }
    else
    {
      decoded_cell.table_id_ = table_schema->get_table_id();
      // delete row is already using this column id
      // using OB_ALL_MAX_COLUMN_ID for exist type
      decoded_cell.column_id_ = OB_ALL_MAX_COLUMN_ID;
      decoded_cell.row_key_ = org_cell.row_key_;
      decoded_cell.value_ = org_cell.value_;
    }
  }
  return ret;
}

// not deep copy cell content to decoded_cell
int ObParamDecoder::decode_cond_cell(const ObCondInfo & org_cond, const ObSchemaManagerV2 & schema,
    ObCellInfo & decoded_cell)
{
  int ret = OB_SUCCESS;
  const ObCellInfo & org_cell = org_cond.get_cell();
  if (org_cell.table_name_.length() <= 0) // || (org_cell.row_key_.length() <= 0))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check cell table name or column name failed:table[%.*s], column[%.*s]",
        org_cell.table_name_.length(), org_cell.table_name_.ptr(),
        org_cell.column_name_.length(), org_cell.column_name_.ptr());
  }
  else if (false == org_cond.is_exist_type())
  {
    ret = decode_cell(false, org_cell, schema, decoded_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "decode cond cell failed:ret[%d]", ret);
    }
  }
  else
  {
    ret = decode_cell(true, org_cell, schema, decoded_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "decode exist cond cell failed:ret[%d]", ret);
    }
  }
  // TODO check op type suitable
  return ret;
}

// not deep copy cell content to decoded_cell
int ObParamDecoder::decode_org_cell(const ObCellInfo & org_cell, const ObSchemaManagerV2 & schema,
    ObCellInfo & decoded_cell)
{
  int ret = OB_SUCCESS;
  if (org_cell.table_name_.length() <= 0) // || (org_cell.column_name_.length() <= 0))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check cell table name or column name failed:table[%.*s], column[%.*s]",
        org_cell.table_name_.length(), org_cell.table_name_.ptr(), org_cell.column_name_.length(),
        org_cell.column_name_.ptr());
  }
  // delete row mutator cell op
  else if (ObActionFlag::OP_DEL_ROW != org_cell.value_.get_ext())
  {
    ret = decode_cell(false, org_cell, schema, decoded_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "decode org cell failed:ret[%d]", ret);
    }
  }
  else
  {
    ret = decode_cell(true, org_cell, schema, decoded_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "decode ext cell failed:ret[%d]", ret);
    }
    else
    {
      decoded_cell.column_id_ = OB_INVALID_ID;
    }
  }
  return ret;
}

