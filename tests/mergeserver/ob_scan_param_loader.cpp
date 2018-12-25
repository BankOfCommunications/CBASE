#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "ob_scan_param_loader.h"
#include "ob_read_param_decoder.h"
#include <unistd.h>

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

namespace
{
  const char* OBSP_SCHEMA_FILE_NAME = "test_schema.ini";
  //const char* OB_SCAN_PARAM_SECTIO= "scan_param";
  const char* OBSP_TABLE_ID = "table_id";
  const char* OBSP_TABLE_NAME = "table_name";
  const char* OBSP_RANGE_START = "range_start";
  const char* OBSP_RANGE_START_INCLUSIVE = "range_start_inclusive";
  const char* OBSP_RANGE_START_MIN = "range_start_min";
  const char* OBSP_RANGE_END = "range_end";
  const char* OBSP_RANGE_END_INCLUSIVE = "range_end_inclusive";
  const char* OBSP_RANGE_END_MAX = "range_end_max";
  const char* OBSP_SCAN_DIRECTION = "scan_direction";
  const char* OBSP_LIMIT_OFFSET = "limit_offset";
  const char* OBSP_LIMIT_COUNT = "limit_count";
  const char* OBSP_COLUMN_ID = "column_name_";
  const char* OBSP_COLUMN_IS_RETURN = "column_is_return_";
  const char* OBSP_COMPLEX_COLUMN_ID = "complex_column_expr_";
  const char* OBSP_COMPLEX_COLUMN_IS_RETURN = "complex_column_is_return_";
  const char* OBSP_GROUPBY_COLUMN_ID = "groupby_column_name_";
  const char* OBSP_GROUPBY_COLUMN_IS_RETURN = "groupby_column_is_return_";
  const char* OBSP_AGG_COLUMN_ID = "agg_column_name_";
  const char* OBSP_AGG_COLUMN_AS_NAME = "agg_column_as_name_";
  const char* OBSP_AGG_COLUMN_OP = "agg_column_op_";
  const char* OBSP_AGG_COLUMN_IS_RETURN = "agg_column_is_return_";
  const char* OBSP_GROUPBY_COMPLEX_COLUMN_ID = "groupby_complex_column_expr_";
  const char* OBSP_GROUPBY_COMPLEX_COLUMN_IS_RETURN = "groupby_complex_column_is_return_";
  const char* OBSP_ORDERBY_COLUMN_ID = "orderby_column_name_";
  const char* OBSP_ORDERBY_COLUMN_ORDER = "orderby_column_order_";
  const char* OBSP_WHERE_COND = "where_cond";
  const char* OBSP_HAVING_COND = "having_cond";
  /*
  const int64_t OB_MS_MAX_MEMORY_SIZE_LIMIT = 80;
  const int64_t OB_MS_MIN_MEMORY_SIZE_LIMIT = 10;
  */
}

ObScanParamLoader::ObScanParamLoader()
{
  table_id_ = 0;
  range_start_inclusive_ = false;
  range_start_min_ = false;
  range_end_inclusive_ = false;
  range_end_max_ = false;
  scan_direction_ = 0;
  limit_offset_ = 0;
  limit_count_ = 0;
  column_id_count_ = 0;
  complex_column_id_count_ = 0;
  groupby_column_id_count_ = 0;
  agg_column_id_count_ = 0;
  orderby_column_id_count_ = 0;
  config_loaded_ = false;
}

int ObScanParamLoader::load(const char *config_file_name, const char *section_name)
{
  int ret = config_.load(config_file_name);
  if (ret == EXIT_SUCCESS)
  {
    ret = load_from_config(section_name);
    if (ret == OB_SUCCESS)
    {
      config_loaded_ = true;
    }
  }
  else
  {
    TBSYS_LOG(WARN, "fail to load config file [%s]", config_file_name);
    ret = OB_ERROR;
  }
  return ret;
}

int ObScanParamLoader::get_org_scan_param(ObScanParam &param)
{
  ObNewRange range;
  ObString column;
  ObString table_name;
  ObPostfixExpression comp;

  if (config_loaded_)
  {
    TBSYS_LOG(INFO, "creating org scan param");

    param.reset();

    range.reset();
    range.table_id_ = table_id_;
    range.start_key_ = range_start_;
    if (range_start_inclusive_)
      range.border_flag_.set_inclusive_start();
    else
      range.border_flag_.unset_inclusive_start();
    if (range_start_min_)
      range.start_key_.set_min_row();

    range.end_key_ = range_end_;
    if (range_end_inclusive_)
      range.border_flag_.set_inclusive_end();
    else
      range.border_flag_.unset_inclusive_end();
    if (range_end_max_)
      range.end_key_.set_max_row();

    param.set(table_id_, table_name_, range, true);
    param.set_scan_direction(scan_direction_==0?ObScanParam::FORWARD:ObScanParam::BACKWARD);
    param.set_limit_info(limit_offset_, limit_count_);

    int i = 0;
    for (i = 0; i < column_id_count_; i++)
    {
      param.add_column(column_id_[i], column_is_return_[i]);
    }

    for (i = 0; i < complex_column_id_count_; i++)
    {
      param.add_column(complex_column_id_[i],complex_column_id_[i], complex_column_is_return_[i]);
    }

    for (i = 0; i < orderby_column_id_count_; i++)
    {
      param.add_orderby_column(orderby_column_id_[i],
          orderby_column_order_[i]==0?ObScanParam::ASC:ObScanParam::DESC);
    }

    if (where_cond_.length() > 0 &&  where_cond_.ptr() != NULL)
    {
      param.add_where_cond(where_cond_);
    }

    // groupby param
    for (i = 0; i < groupby_column_id_count_; i++)
    {
      param.get_group_by_param().add_groupby_column(groupby_column_id_[i], groupby_column_is_return_[i]);
    }

    for (i = 0; i < groupby_complex_column_id_count_; i++)
    {
      param.get_group_by_param().add_column(groupby_complex_column_id_[i],
          groupby_complex_column_id_[i],
          groupby_complex_column_is_return_[i]);
    }

    if (having_cond_.length() > 0 &&  having_cond_.ptr() != NULL)
    {
      param.get_group_by_param().add_having_cond(having_cond_);
    }

    /// FIXME: SUM占位, 需要添加OP域
    for (i = 0; i < agg_column_id_count_; i++)
    {
      param.get_group_by_param().add_aggregate_column(agg_column_id_[i],
          agg_column_as_name_[i],
          (ObAggregateFuncType)agg_column_op_[i],
          agg_column_is_return_[i]);
    }
  }
  return OB_SUCCESS;
}

int ObScanParamLoader::decoded_scan_param(ObScanParam &org_param, ObScanParam &decoded_param)
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 schema_mgr;
  tbsys::CConfig config;
  /* read from Schema file */
  if (!schema_mgr.parse_from_file(OBSP_SCHEMA_FILE_NAME, config))
  {
    TBSYS_LOG(ERROR, "fail to load schema file [test_schema.ini]");
    ret = OB_ERROR;
  }
  /* decode the SQL to scan param */
  if (OB_SUCCESS == ret)
  {
    dump_param(org_param);
    decoded_param.reset();
    ret = ob_decode_scan_param(org_param, schema_mgr, decoded_param);
  }
  return ret;
}

void ObScanParamLoader::dump_param(ObScanParam &param)
{
  TBSYS_LOG(INFO, "DUMP ObScanParam:");
  TBSYS_LOG(INFO, "table_name:[%.*s]", param.get_table_name().length(), param.get_table_name().ptr());
  TBSYS_LOG(INFO, "range:%s", to_cstring(*param.get_range()));
}

int ObScanParamLoader::load_from_config(const char *scan_param_section_name)
{
  int ret = OB_SUCCESS;
  ObString tmpstr ;
  int tmpint ;
  int i = 0;

  char buf[OB_MAX_ROW_KEY_LENGTH];

  if (ret == OB_SUCCESS)
  {
    table_id_ = config_.getInt(scan_param_section_name, OBSP_TABLE_ID, 0);
    if (table_id_ <= 0)
    {
      TBSYS_LOG(ERROR, "invalid table id (%d)", table_id_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, OBSP_TABLE_NAME, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty table name. err=%d", ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &table_name_);
    }
  }


  ObRowkey tmp_rowkey;
  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, OBSP_RANGE_START, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty range start. err=%d", ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      // TODO assign tmp_rowkey;
      strbuf.write_string(tmp_rowkey, &range_start_);
    }
  }

  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, OBSP_RANGE_END, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty %s. err=%d", OBSP_RANGE_END, ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      // TODO assign tmp_rowkey
      strbuf.write_string(tmp_rowkey, &range_end_);
    }
  }

  if(OB_SUCCESS == (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
          scan_param_section_name, OBSP_WHERE_COND, 0)))
  {
    tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
    strbuf.write_string(tmpstr, &where_cond_);
  }


  if(OB_SUCCESS == (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
          scan_param_section_name, OBSP_HAVING_COND, 0)))
  {
    tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
    strbuf.write_string(tmpstr, &having_cond_);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_RANGE_START_INCLUSIVE, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_START_INCLUSIVE, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_start_inclusive_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_RANGE_START_MIN, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_START_MIN, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_start_min_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_RANGE_END_INCLUSIVE, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_END_INCLUSIVE, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_end_inclusive_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_RANGE_END_MAX, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_END_MAX, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_end_max_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_SCAN_DIRECTION, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_SCAN_DIRECTION, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    scan_direction_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_LIMIT_OFFSET, 0);
  if (tmpint < 0)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_LIMIT_OFFSET, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    limit_offset_ = tmpint;
  }

  tmpint = config_.getInt(scan_param_section_name, OBSP_LIMIT_COUNT, 0);
  if (tmpint < 0)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_LIMIT_COUNT, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    limit_count_ = tmpint;
  }

  char name_buf[1024];
  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &column_id_[i]);
      column_is_return_[column_id_count_] = 1;
      column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_COLUMN_IS_RETURN, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      column_is_return_[column_id_count_ - 1] = 0;
    }
  }



  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_COMPLEX_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &complex_column_id_[i]);
      complex_column_is_return_[complex_column_id_count_] = 1; // default
      complex_column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_COMPLEX_COLUMN_IS_RETURN, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      complex_column_is_return_[complex_column_id_count_ - 1] = 0;
    }
  }


  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_GROUPBY_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &groupby_column_id_[i]);
      groupby_column_is_return_[groupby_column_id_count_] = 1;
      groupby_column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_GROUPBY_COLUMN_IS_RETURN, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      groupby_column_is_return_[groupby_column_id_count_ - 1] = 0;
    }
  }


  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_AGG_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &agg_column_id_[i]);
      agg_column_is_return_[agg_column_id_count_] = 1;
      agg_column_op_[agg_column_id_count_] = 0;
      agg_column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_AGG_COLUMN_AS_NAME, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      TBSYS_LOG(WARN, "no as name found for aggregate column %.*s",
          agg_column_id_[i].length(), agg_column_id_[i].ptr());
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &agg_column_as_name_[i]);
    }

    sprintf(name_buf, "%s%d", OBSP_AGG_COLUMN_IS_RETURN, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      agg_column_is_return_[agg_column_id_count_ - 1] = 0;
    }
    sprintf(name_buf, "%s%d", OBSP_AGG_COLUMN_OP, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint > 0)
    {
      agg_column_op_[agg_column_id_count_ - 1] = tmpint;
    }
  }


  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_GROUPBY_COMPLEX_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &groupby_complex_column_id_[i]);
      groupby_complex_column_is_return_[groupby_complex_column_id_count_] = 1;
      groupby_complex_column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_GROUPBY_COMPLEX_COLUMN_IS_RETURN, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      groupby_complex_column_is_return_[agg_column_id_count_ - 1] = 0;
    }
  }



  for (i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_ORDERBY_COLUMN_ID, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scan_param_section_name, name_buf, 0)))
    {
      continue;
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmpstr, &orderby_column_id_[i]);
      orderby_column_order_[orderby_column_id_count_] = 1;
      orderby_column_id_count_++;
    }

    sprintf(name_buf, "%s%d", OBSP_ORDERBY_COLUMN_ORDER, i);
    tmpint = config_.getInt(scan_param_section_name, name_buf, -1);
    if (tmpint == 0)
    {
      orderby_column_order_[orderby_column_id_count_ - 1] = 0;
    }
  }
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(INFO, "load config return value not SUCCESS. rewrite to success!");
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObScanParamLoader::load_string(char* dest, const int32_t size,
    const char* section, const char* name, bool require)
{
  int ret = OB_SUCCESS;
  if (NULL == dest || 0 >= size || NULL == section || NULL == name)
  {
    ret = OB_ERROR;
  }

  const char* value = NULL;
  if (OB_SUCCESS == ret)
  {
    value = config_.getString(section, name);
    if (require && (NULL == value || 0 >= strlen(value)))
    {
      TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
      ret = OB_ERROR;
    }
    else if (NULL == value || 0 >= strlen(value))
    {
      ret = OB_ITER_END;
    }
  }

  if (OB_SUCCESS == ret && NULL != value)
  {
    if ((int32_t)strlen(value) >= size)
    {
      TBSYS_LOG(ERROR, "%s.%s too long, length (%ld) > %d",
          section, name, strlen(value), size);
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      //strncpy(dest, value, strlen(value));
      strcpy(dest, value);
    }
  }

  return ret;
}

void ObScanParamLoader::dump_config()
{
  TBSYS_LOG(INFO, "%s", "scan param config:");

  TBSYS_LOG(INFO, "%s => %d", OBSP_TABLE_ID, table_id_);
  TBSYS_LOG(INFO, "%s => %.*s", OBSP_TABLE_NAME, table_name_.length(), table_name_.ptr());

  if (range_start_min_)
  {
    TBSYS_LOG(INFO, "%s => %s:%s:%s", OBSP_RANGE_START, "",
        range_start_inclusive_?"inclusive":"exclusive",
        range_start_min_?"MIN":""
        );
  }
  else
  {
    TBSYS_LOG(INFO, "%s => %s:%s:%s", OBSP_RANGE_START, to_cstring(range_start_),
        range_start_inclusive_?"inclusive":"exclusive",
        range_start_min_?"MIN":"");
  }

  if (range_end_max_)
  {
    TBSYS_LOG(INFO, "%s => %s:%s:%s", OBSP_RANGE_END, "",
        range_end_inclusive_?"inclusive":"exclusive",
        range_end_max_?"MAX":""
        );
  }
  else
  {
    TBSYS_LOG(INFO, "%s => %s:%s:%s", OBSP_RANGE_END, to_cstring(range_end_),
        range_end_inclusive_?"inclusive":"exclusive",
        range_end_max_?"MAX":"");
  }

  TBSYS_LOG(INFO, "%s => %.*s", OBSP_WHERE_COND, where_cond_.length(), where_cond_.ptr());
  TBSYS_LOG(INFO, "%s => %.*s", OBSP_HAVING_COND, having_cond_.length(), having_cond_.ptr());
  TBSYS_LOG(INFO, "%s => %s", OBSP_SCAN_DIRECTION, scan_direction_==0?"forward":"backward");
  TBSYS_LOG(INFO, "%s:%s => %d:%d", OBSP_LIMIT_OFFSET, OBSP_LIMIT_COUNT, limit_offset_, limit_count_);

  int i = 0;
  for (i = 0; i < column_id_count_; i++)
  {
    TBSYS_LOG(INFO, "%s%d => %.*s:%s", OBSP_COLUMN_ID, i, column_id_[i].length(), column_id_[i].ptr(),
        column_is_return_[i]?"return":"not return");
  }
  for (i = 0; i < complex_column_id_count_; i++)
  {
    TBSYS_LOG(INFO, "%s%d => %.*s:%s", OBSP_COMPLEX_COLUMN_ID, i,
        complex_column_id_[i].length(), complex_column_id_[i].ptr(),
        complex_column_is_return_[i]?"return":"not return");
  }
  for (i = 0; i < agg_column_id_count_; i++)
  {
    TBSYS_LOG(INFO, "%s%d => %.*s:%s", OBSP_AGG_COLUMN_ID, i,
        agg_column_id_[i].length(), agg_column_id_[i].ptr(),
        agg_column_is_return_[i]?"return":"not return");
  }
  for (i = 0; i < orderby_column_id_count_; i++)
  {
    TBSYS_LOG(INFO, "%s%d => %.*s:%s", OBSP_ORDERBY_COLUMN_ID, i,
        orderby_column_id_[i].length(), orderby_column_id_[i].ptr(),
        orderby_column_order_[i]?"DESC":"ASC");
  }

}

