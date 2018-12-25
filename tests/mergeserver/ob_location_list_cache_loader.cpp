#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/location/ob_tablet_location_cache.h"
#include "ob_location_list_cache_loader.h"
#include <unistd.h>

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace
{
  //const char* location_cache_section_name = "tablet_location_cache";
  const char* OBSP_TABLE_ID = "table_id_";
  const char* OBSP_RANGE_START = "range_start_";
  const char* OBSP_RANGE_END = "range_end_";
  const char* OBSP_SERVER_LIST = "server_list_";
}

ObLocationListCacheLoader::ObLocationListCacheLoader()
{
  //memset(this, 0, sizeof(ObLocationListCacheLoader));
  memset(fake_max_row_key_buf_, 0xff, sizeof(fake_max_row_key_buf_));
  fake_max_row_key_.assign(fake_max_row_key_buf_, sizeof(fake_max_row_key_buf_));
}

int ObLocationListCacheLoader::load(const char *config_file_name, const char *section_name)
{
  int ret = config_.load(config_file_name);
  config_loaded_ = false;
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

int ObLocationListCacheLoader::get_decoded_location_list_cache(ObTabletLocationCache & cache)
{
  ObNewRange range;
  ObServer server;
  char value[4096];
  char ipbuf[64];
  char *start = NULL;
  char *end = NULL;
  int i = 0;

  ObTabletLocationList list;

  if (config_loaded_)
  {
    TBSYS_LOG(INFO, "decoding table location list");
    /*
       if(OB_SUCCESS != cache.init(50000 * 5, 1000, 100000))
       {
       TBSYS_LOG(WARN, "fail to init cache");
       }

       else 
     */
    cache.init(50000 * 5, 1000, 100000);
    {
      cache.clear();

      for (i = 0; i < range_num_; i++)
      {
        memset(value, 0, 4096);
        strncpy(value, server_list_[i].ptr(), server_list_[i].length());          
        // set ip list
        end = start = value;          
        list.clear();
        while(true)
        {
          memset(ipbuf, 0, 64);
          while( isdigit(*end) || *end == '.') {
            ipbuf[end - start] = *end;
            end++;
          }

          if (end > start)
          {
            TBSYS_LOG(DEBUG, "ipbuf=%s", ipbuf);
            server.set_ipv4_addr(ipbuf, 19000); // any port
            ObTabletLocation addr(1, server);
            list.add(addr);
          }

          if (end == '\0') break;
          while(!(isdigit(*end) || *end == '.') && *end != '\0') end++;
          if (*end == '\0') break;
          start = end;
        }

        if (strncasecmp(range_start_[i].ptr(), "MIN", 3) == 0)
        {
          range.start_key_.set_min_row();
        }
        else
        {
          range_start_obj_[i].set_varchar(range_start_[i]);
          range.start_key_.assign(&range_start_obj_[i], 1);
        }

        if (strncasecmp(range_end_[i].ptr(), "MAX", 3) == 0)
        {
          range.end_key_.set_max_row();
        }
        else
        {
          range_end_obj_[i].set_varchar(range_end_[i]);
          range.end_key_.assign(&range_end_obj_[i], 1);
        }
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.set_inclusive_end();
        range.table_id_ = table_id_[i];
        list.set_tablet_range(range);
        list.set_timestamp(tbsys::CTimeUtil::getTime());
        cache.set(range, list);
      }

      TBSYS_LOG(INFO, "%ld ranges added to the location cache", range_num_);
    }
  }
  return OB_SUCCESS;
}

int ObLocationListCacheLoader::load_from_config(const char *location_cache_section_name)
{
  int ret = OB_SUCCESS;
  ObString tmpstr;

  char buf[OB_MAX_ROW_KEY_LENGTH];
  char buf2[OB_MAX_ROW_KEY_LENGTH];
  char buf3[OB_MAX_ROW_KEY_LENGTH];
  int table_id = 0;      
  int i = 0;
  char name_buf[1024];
  range_num_=0;
  int max_range_count = 5000;
  for (i = 0; i < max_range_count; i++)
  {
    sprintf(name_buf, "%s%d", OBSP_RANGE_START, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf, OB_MAX_ROW_KEY_LENGTH,
            location_cache_section_name, name_buf, 0)))
    {
      continue;
    } 
    sprintf(name_buf, "%s%d", OBSP_RANGE_END, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf2, OB_MAX_ROW_KEY_LENGTH,
            location_cache_section_name, name_buf, 0)))
    {
      TBSYS_LOG(WARN, "should set range_end_");
      continue;
    }
    sprintf(name_buf, "%s%d", OBSP_SERVER_LIST, i);
    if(OB_SUCCESS != (ret = load_string((char*)buf3, OB_MAX_ROW_KEY_LENGTH,
            location_cache_section_name, name_buf, 0)))
    { 
      TBSYS_LOG(WARN, "should set server_list_");
      continue;
    }

    sprintf(name_buf, "%s%d", OBSP_TABLE_ID, i);
    table_id = config_.getInt(location_cache_section_name, name_buf, 0);
    if (table_id <= 0)
    {
      TBSYS_LOG(WARN, "should set table_id");
      continue;
    }

    tmpstr.assign(buf, static_cast<int32_t>(strlen(buf)));
    strbuf.write_string(tmpstr, &range_start_[range_num_]);
    tmpstr.assign(buf2, static_cast<int32_t>(strlen(buf2)));
    strbuf.write_string(tmpstr, &range_end_[range_num_]);
    tmpstr.assign(buf3, static_cast<int32_t>(strlen(buf3)));
    strbuf.write_string(tmpstr, &server_list_[range_num_]);
    table_id_[range_num_] = table_id;

    range_num_++;
  }  

  if (range_num_ > 0)
  {
    TBSYS_LOG(INFO, "%ld ranges loaded", range_num_);
    ret = OB_SUCCESS;
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(INFO, "no range loaded. Fail!");
  }
  return ret;
}

int ObLocationListCacheLoader::load_string(char* dest, const int32_t size,
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

void ObLocationListCacheLoader::dump_config()
{
  int i = 0;
  TBSYS_LOG(INFO, "%s", "location list config:");
  for (i = 0; i < range_num_; i++)
  {
    TBSYS_LOG(INFO, "[%d][%d][%.*s][%.*s][%.*s]", 
        i, 
        table_id_[i], 
        range_start_[i].length(), range_start_[i].ptr(), 
        range_end_[i].length(), range_end_[i].ptr(), 
        server_list_[i].length(), server_list_[i].ptr()
        );
  }
}

