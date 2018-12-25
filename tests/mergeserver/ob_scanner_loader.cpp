#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "ob_scanner_loader.h"
#include <unistd.h>

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

namespace
{
  //const char* OB_SCANNER_SECTION = "scanner";
  const char* OBSP_TABLE_ID = "table_id";
  const char* OBSP_TABLE_NAME = "table_name";
  const char* OBSP_RANGE_START = "range_start";
  const char* OBSP_RANGE_START_INCLUSIVE = "range_start_inclusive";
  const char* OBSP_RANGE_START_MIN = "range_start_min";
  const char* OBSP_RANGE_END = "range_end";
  const char* OBSP_RANGE_END_INCLUSIVE = "range_end_inclusive";
  const char* OBSP_RANGE_END_MAX = "range_end_max";
  const char* OBSP_IS_FULLFILLED = "is_fullfilled";
  const char* OBSP_FULLFILLED_ITEM_NUM = "fullfilled_item_num";
  const char* OBSP_DATA_VERSION = "data_version";
  const char* OB_DATA_FILE_NAME = "scanner_data";
  /*
  const int64_t OB_MS_MAX_MEMORY_SIZE_LIMIT = 80;
  const int64_t OB_MS_MIN_MEMORY_SIZE_LIMIT = 10;
  */
}

ObScannerLoader::ObScannerLoader()
{
  table_id_ = 0;
  range_start_inclusive_ = false;
  range_start_min_ = false;
  range_end_inclusive_ = false;
  range_end_max_ = false;
  is_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  data_version_ = 0;
  actual_fullfilled_item_num_ = 0;
  config_loaded_ = false;
}

int ObScannerLoader::load(const char *config_file_name, const char *section_name)
{
  int ret = config_.load(config_file_name);
  if (ret == EXIT_SUCCESS)
  {
    TBSYS_LOG(DEBUG, "succ to load config file [%s][%s]", config_file_name, section_name);
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

int ObScannerLoader::get_decoded_scanner(ObScanner &param)
{
  ObNewRange range;
  ObString column;
  ObString table_name;
  char value[4096];

  if (config_loaded_)
  {
    TBSYS_LOG(INFO, "creating scanner");

    param.reset();

    range.reset();
    range.table_id_ = table_id_;

    TBSYS_LOG(DEBUG, "range_start_=%s", to_cstring(range_start_));
    TBSYS_LOG(DEBUG, "range_end_=%s", to_cstring(range_end_));

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

    param.set_range(range);
    //param.set_is_req_fullfilled(is_fullfilled_, actual_fullfilled_item_num_);
    param.set_is_req_fullfilled(is_fullfilled_, fullfilled_item_num_);
    param.set_data_version(data_version_);

    FILE *fp = NULL;
    if ((fp = fopen(data_file_name_, "rb")) == NULL) {
      TBSYS_LOG(ERROR, "Fail to open %s", data_file_name_);
    }
    else
    {
      while(fgets(value, 4096, fp))
      {
        /* only digit and string accepted. if not dig, then string */
        int start = 0;
        int end = 0;
        int cnt = 0;
        char buf[4096];
        int table_id = 0;
        int column_cnt = 0;
        ObRowkey row_key;

        while(true)
        {
          while(isspace(value[start]))
          {
            start++;
          }

          end = start;
          while(!isspace(value[end]) && value[end] != '\0')
          {
            end++;
          }


          if (start != end)
          {
            memset(buf, 0, 4096);
            cnt = end - start;
            strncpy(buf, &value[start], cnt);
            buf[cnt] = '\0';
            if (column_cnt == 0)
            {
              table_id = atoi(buf);
            }
            else if(column_cnt == 1)
            {
              // TODO build rowkey
              //strcpy(row_key_buf, buf);
              //row_key.assign(row_key_buf, cnt);
            }
            else
            {
              build_cell_info(table_id, row_key, buf, param);
            }
            column_cnt++;
            start = end;
          }

          if (value[end] == '\0')
          {
            break;
          }
        }
      }
      fclose(fp);
    }
  }
  return OB_SUCCESS;
}

int ObScannerLoader::build_cell_info(int table_id, const ObRowkey& row_key, char *buf, ObScanner &param)
{
  ObCellInfo info;
  ObString tmpstr;
  uint64_t column_id = OB_INVALID_ID;
  char *p = NULL;

  info.reset();
  //info.table_name_ = table_name_;
  info.table_id_ = table_id;
  info.row_key_ = row_key;
  info.column_id_ = column_id;

  if(NULL == (p = strchr(buf, ':')))
  {
    p = buf;
    auto_set_val(info.value_, p);
  }
  else
  {
    *p = '\0';
    p++;
    info.column_id_ = atoi(buf);
    auto_set_val(info.value_, p);
  }
  TBSYS_LOG(DEBUG, "[%lu][%s] [%lu]:[%s]", info.table_id_,
      to_cstring(info.row_key_),  info.column_id_, p);

  actual_fullfilled_item_num_++;
  return param.add_cell(info);
}

/* only support integer and string */
void ObScannerLoader::auto_set_val(ObObj &val, char *buf)
{
  if (NULL == buf)
  {
    TBSYS_LOG(ERROR, "invalid scanner data input");
  }
  else
  {
    int len = static_cast<int32_t>(strlen(buf));
    int i = 0;
    ObString tmpstr;
    bool is_num = true;

    for(i = 0; i < len; i++)
    {
      if (!isdigit(buf[i]))
      {
        is_num = false;
        break;
      }
    }

    if (is_num)
    {
      val.set_int(atoi(buf));
    }
    else
    {
      tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      val.set_varchar(tmpstr);
    }
    //val.dump();
  }
}

void ObScannerLoader::dump_param(ObScanner &param)
{
  UNUSED(param);
  TBSYS_LOG(INFO, "DUMP ObScanner:");
}

int ObScannerLoader::load_from_config(const char* scanner_section_name)
{
  int ret = OB_SUCCESS;
  ObString tmpstr ;
  ObRowkey tmp_rowkey;
  int tmpint ;

  char buf[OB_MAX_ROW_KEY_LENGTH];

  if (ret == OB_SUCCESS)
  {
    table_id_ = config_.getInt(scanner_section_name, OBSP_TABLE_ID, 0);
    if (table_id_ <= 0)
    {
      TBSYS_LOG(ERROR, "invalid table id (%d)", table_id_);
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scanner_section_name, OBSP_TABLE_NAME, 0)))
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


  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scanner_section_name, OBSP_RANGE_START, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty range start. err=%d", ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      // TODO
      //tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmp_rowkey, &range_start_);
    }
  }


  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            scanner_section_name, OBSP_RANGE_END, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty %s. err=%d", OBSP_RANGE_END, ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      // TODO
      //tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
      strbuf.write_string(tmp_rowkey, &range_end_);
      TBSYS_LOG(DEBUG, "************%.*s**************", tmpstr.length(), tmpstr.ptr());
    }
  }


  if (ret == OB_SUCCESS)
  {
    if(OB_SUCCESS != (ret = load_string((char*)data_file_name_, 4096,
            scanner_section_name, OB_DATA_FILE_NAME, 0)))
    {
      TBSYS_LOG(ERROR, "invalid/empty %s. err=%d", OB_DATA_FILE_NAME, ret);
      ret = OB_INVALID_ARGUMENT;
    }
  }


  tmpint = config_.getInt(scanner_section_name, OBSP_RANGE_START_MIN, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_START_MIN, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_start_min_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scanner_section_name, OBSP_RANGE_START_INCLUSIVE, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_START_INCLUSIVE, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_start_inclusive_ = (tmpint == 1);
  }


  tmpint = config_.getInt(scanner_section_name, OBSP_RANGE_END_INCLUSIVE, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_END_INCLUSIVE, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_end_inclusive_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scanner_section_name, OBSP_RANGE_END_MAX, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_RANGE_END_MAX, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range_end_max_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scanner_section_name, OBSP_IS_FULLFILLED, 0);
  if (tmpint != 0 && tmpint != 1)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_IS_FULLFILLED, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    is_fullfilled_ = (tmpint == 1);
  }

  tmpint = config_.getInt(scanner_section_name, OBSP_FULLFILLED_ITEM_NUM, 0);
  if (tmpint < 0)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_FULLFILLED_ITEM_NUM, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    fullfilled_item_num_ = tmpint;
  }

  tmpint = config_.getInt(scanner_section_name, OBSP_DATA_VERSION, 0);
  if (tmpint < 0)
  {
    TBSYS_LOG(ERROR, "invalid %s (%d)", OBSP_DATA_VERSION, tmpint);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    data_version_ = tmpint;
  }
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(INFO, "load config return value not SUCCESS. rewrite to success!");
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObScannerLoader::load_string(char* dest, const int32_t size,
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

void ObScannerLoader::dump_config()
{
  TBSYS_LOG(INFO, "%s", "scan param config:");

  TBSYS_LOG(INFO, "%s => %d", OBSP_TABLE_ID, table_id_);
  TBSYS_LOG(INFO, "%s => %.*s", OBSP_TABLE_NAME, table_name_.length(), table_name_.ptr());

  TBSYS_LOG(INFO, "%s:%s => %d:%ld", OBSP_IS_FULLFILLED, OBSP_FULLFILLED_ITEM_NUM, is_fullfilled_, fullfilled_item_num_);
  TBSYS_LOG(INFO, "%s => %ld", OBSP_DATA_VERSION, data_version_);

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
}


