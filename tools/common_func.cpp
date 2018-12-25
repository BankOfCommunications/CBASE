/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test.cpp is for what ...
 *
 *  Version: $Id: test.cpp 2010年11月17日 16时12分46秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include "common_func.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "test_utils.h"
#include "sstable/ob_sstable_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::chunkserver;

hash::ObHashMap<const char*, int> obj_type_map;

void init_obj_type_map_()
{
  static bool inited = false;
  if (!inited)
  {
    obj_type_map.create(16);
    obj_type_map.set("null", ObNullType);
    obj_type_map.set("int", ObIntType);
    //add lijianqiang [INT_32] 20151008:b
    obj_type_map.set("int32", ObInt32Type);
    //add 20151008:e
    obj_type_map.set("float", ObFloatType);
    obj_type_map.set("double", ObDoubleType);
    obj_type_map.set("date_time", ObDateTimeType);
    //add peiouya [DATE_TIME] 20150906:b
    obj_type_map.set("date", ObDateType);
    obj_type_map.set("time", ObTimeType);
    //add 20150906:e
    obj_type_map.set("precise_date_time", ObPreciseDateTimeType);
    obj_type_map.set("var_char", ObVarcharType);
    obj_type_map.set("seq", ObSeqType);
    obj_type_map.set("create_time", ObCreateTimeType);
    obj_type_map.set("modify_time", ObModifyTimeType);
    inited = true;
  }
}

/* 从min和max中返回一个随机值 */

int64_t random_number(int64_t min, int64_t max)
{
  static int dev_random_fd = -1;
  char *next_random_byte;
  int bytes_to_read;
  int64_t random_value = 0;

  assert(max > min);

  if (dev_random_fd == -1)
  {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char *)&random_value;
  bytes_to_read = sizeof(random_value);

  /* 因为是从/dev/random中读取，read可能会被阻塞，一次读取可能只能得到一个字节，
   * 循环是为了让我们读取足够的字节数来填充random_value.
   */
  do
  {
    int bytes_read;
    bytes_read = static_cast<int32_t>(read(dev_random_fd, next_random_byte, bytes_to_read));
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while(bytes_to_read > 0);

  return min + (abs(static_cast<int32_t>(random_value)) % (max - min + 1));
}


int parse_number_range(const char *number_string,
    int32_t *number_array, int32_t &number_array_size)
{
  int ret = OB_ERROR;
  if (NULL != strstr(number_string, ","))
  {
    ret = parse_string_to_int_array(number_string, ',', number_array, number_array_size);
    if (ret) return ret;
  }
  else if (NULL != strstr(number_string, "~"))
  {
    int32_t min_max_array[2];
    min_max_array[0] = 0;
    min_max_array[1] = 0;
    int tmp_size = 2;
    ret = parse_string_to_int_array(number_string, '~', min_max_array, tmp_size);
    if (ret) return ret;
    int32_t min = min_max_array[0];
    int32_t max = min_max_array[1];
    int32_t index = 0;
    for (int i = min; i <= max; ++i)
    {
      if (index > number_array_size) return OB_SIZE_OVERFLOW;
      number_array[index++] = i;
    }
    number_array_size = index;
  }
  else
  {
    number_array[0] = static_cast<int32_t>(strtol(number_string, NULL, 10));
    number_array_size = 1;
  }
  return OB_SUCCESS;
}

int number_to_hex_buf(const char* sp, const int len, char* hex, const int64_t hexlen, int64_t &pos)
{
  const int max_len = 32;
  assert(len < max_len-1);
  char number[max_len];
  memset(number, 0, max_len);
  strncpy(number, sp, len);
  int64_t value = strtoll(number, NULL, 10);
  return encode_i64(hex, hexlen, pos, value);
}

int parse_string(const char* src, const char del, const char* dst[], int64_t& size)
{
  int ret = OB_SUCCESS;
  int64_t obj_index = 0;

  char *str = (char*)ob_malloc(OB_MAX_FILE_NAME_LENGTH, ObModIds::TEST);
  strcpy(str, src);
  str[strlen(src)] = 0;

  const char* st_ptr = str;
  char* et_ptr = NULL;
  char* last_ptr = str + strlen(str) - 1;

  //skip white space;
  while (*st_ptr != 0 && *st_ptr == ' ') ++st_ptr;

  while (NULL != st_ptr && NULL != (et_ptr = strchr((char*)st_ptr, del)))
  {
    //set del character to '\0'
    *et_ptr = 0;

    if (obj_index >= size)
    {
      ret = OB_SIZE_OVERFLOW;
      break;
    }
    else
    {
      dst[obj_index++] = st_ptr;
      st_ptr = et_ptr + 1;
      //skip white space;
      while (*st_ptr != 0 && *st_ptr == ' ') ++st_ptr;
    }
  }

  // last item;
  if (OB_SUCCESS == ret && *st_ptr != 0 && obj_index < size)
  {
    while (last_ptr > st_ptr && *last_ptr == ' ')
    {
      *last_ptr = 0;
      --last_ptr;
    }
    dst[obj_index++] = st_ptr;
  }

  if (OB_SUCCESS == ret)
  {
    size = obj_index;
  }
  return ret;
}

int parse_object(const char* object_str, ObObj& obj)
{
  int ret = OB_SUCCESS;
  // int:10
  int64_t size = 2;
  const char* dst[size];
  static const char* obj_type_name[] =
  {
    "null",
    "int",
    //del hongchen [INT_32_BUG_FIX] 20161118:b
    ////add lijianqiang [INT_32] 20151008:b
    //"int32",
    ////add 20151008:e
    //del hongchen [INT_32_BUG_FIX] 20161118:e
    "float",
    "double",
    "datetime",
    "precisedatetime",
    "varchar",
    "seq",
    "createtime",
    "modifytime",
    "extend",
    "bool",
    "decimal",
    //add peiouya [DATE_TIME_FIX] 20161118:b 
    "date",
    "time",
    "interval",
    //add peiouya [DATE_TIME_FIX] 20161118:e
    "int32",  //add hongchen [INT_32_BUG_FIX] 20161118
  };

  if (OB_SUCCESS != (ret = parse_string(object_str, ':', dst, size)))
  {
    return ret;
  }

  if (size == 1)
  {
    if (strcasecmp(dst[0], "MIN") == 0)
    {
      obj.set_min_value();
    }
    else if (strcasecmp(dst[0], "MAX") == 0)
    {
      obj.set_max_value();
    }
    else
    {
      fprintf(stderr, "incorrect format [%s], not min or max value.\n", object_str);
      ret = OB_ERROR;
    }
  }
  else if (size == 2)
  {
    int type = 0;
    bool found = false;
    for (; type < (int)sizeof(obj_type_name); ++type)
    {
      const char* type_name = obj_type_name[type];
      if (strcasecmp(dst[0], type_name) == 0)
      {
        found = true;
        break;
      }
    }

    if (found)
    {
      ObString varchar;

      switch(type)
      {
        case ObNullType:
          obj.set_null();
          break;
        case ObIntType:
          obj.set_int(strtoll(dst[1], NULL, 10));
          break;
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          obj.set_int32(atoi(dst[1]));
          break;
        //add 20151008:e
        case ObVarcharType:
          varchar.assign((char*)dst[1], static_cast<int32_t>(strlen(dst[1])));
          obj.set_varchar(varchar);
          break;
        case ObFloatType:
          obj.set_float(static_cast<float>(atof(dst[1])));
          break;
        case ObDoubleType:
          obj.set_double(atof(dst[1]));
          break;
        case ObDateTimeType:
          obj.set_datetime(strtoll(dst[1], NULL, 10));
          break;
        case ObPreciseDateTimeType:
          obj.set_precise_datetime(strtoll(dst[1], NULL, 10));
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
          obj.set_date(strtoll(dst[1], NULL, 10));
          break;
        case ObTimeType:
          obj.set_time(strtoll(dst[1], NULL, 10));
          break;
        //add 20150906:e
        case ObSeqType:
          //TODO
          break;
        case ObCreateTimeType:
          obj.set_createtime(strtoll(dst[1], NULL, 10));
          break;
        case ObModifyTimeType:
          obj.set_modifytime(strtoll(dst[1], NULL, 10));
          break;
        case ObExtendType:
          if (strcasecmp(dst[1], "min") == 0)
          {
            obj.set_min_value();
          }
          else if (strcasecmp(dst[1], "max") == 0)
          {
            obj.set_max_value();
          }
          else
          {
            obj.set_ext(strtoll(dst[1], NULL, 10));
          }
          break;
        case ObBoolType:
          obj.set_bool(strtoll(dst[1], NULL, 10));
          break;
        case ObDecimalType:
          // TODO
          break;
        default:
          break;
      }
    }
    else
    {
      fprintf(stderr, "unknown type %s\n", dst[0]);
    }
  }
  else
  {
    fprintf(stderr, "incorrect format [%s]\n", object_str);
    ret = OB_ERROR;
  }

  return ret;
}

int parse_rowkey(const char* strkey, ObObj* array, int64_t& size)
{
  // int:value,varchar:cadf,float:0.332
  const char* dst[size];
  int ret = parse_string(strkey, ',', dst, size);
  for (int i = 0; i < size && OB_SUCCESS == ret; ++i)
  {
    ret = parse_object(dst[i], array[i]);
  }
  return ret;
}

int parse_range_str(const char* range_str, int hex_format, oceanbase::common::ObNewRange &range)
{
  UNUSED(hex_format);
  //[int:1,varchar:aaa; int:2,varchar:bbb]
  int ret = OB_SUCCESS;
  int64_t len = 0;

  if(NULL == range_str)
  {
    ret = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == ret)
  {
    len = static_cast<int>(strlen(range_str));
    if (len < 5)
      ret = OB_ERROR;
  }

  if(OB_SUCCESS == ret)
  {
    const char start_border = range_str[0];
    if (start_border == '[')
      range.border_flag_.set_inclusive_start();
    else if (start_border == '(')
      range.border_flag_.unset_inclusive_start();
    else
    {
      fprintf(stderr, "start char of range_str(%c) must be [ or (\n", start_border);
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    const char end_border = range_str[len - 1];
    if (end_border == ']')
      range.border_flag_.set_inclusive_end();
    else if (end_border == ')')
      range.border_flag_.unset_inclusive_end();
    else
    {
      fprintf(stderr, "end char of range_str(%c) must be [ or (\n", end_border);
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t obj_array_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
    ObObj * sk_objs = new ObObj[obj_array_size];
    ObObj * ek_objs = new ObObj[obj_array_size];
    int64_t start_key_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
    int64_t end_key_size = OB_MAX_ROWKEY_COLUMN_NUMBER;

    int64_t key_size = 2;
    const char* dst[key_size];
    char rowkey_str[OB_MAX_FILE_NAME_LENGTH];
    memset(rowkey_str, 0, OB_MAX_FILE_NAME_LENGTH);
    memcpy(rowkey_str, range_str + 1, strlen(range_str) - 2 );
    if (OB_SUCCESS != (ret = parse_string(rowkey_str, ';', dst, key_size)))
    {
      fprintf(stderr, "incorrect format [%s], ret=%d.\n", rowkey_str, ret);
    }
    else if (key_size != 2)
    {
      fprintf(stderr, "incorrect format [%s], has %ld rowkey \n", rowkey_str, key_size);
      ret = OB_ERROR;
    }
    else if (OB_SUCCESS != (ret = parse_rowkey(dst[0], sk_objs, start_key_size)))
    {
      fprintf(stderr, "incorrect start rowkey [%s], ret=%d.\n", dst[0], ret);
    }
    else if (OB_SUCCESS != (ret = parse_rowkey(dst[1], ek_objs, end_key_size)))
    {
      fprintf(stderr, "incorrect end rowkey [%s], ret=%d.\n", dst[1], ret);
    }
    else
    {
      range.start_key_.assign(sk_objs, start_key_size);
      range.end_key_.assign(ek_objs, end_key_size);
    }
  }

  return ret;
}

/**
 * hex_format:
 * 0 : plain string
 * 1 : hex format string "FACB012D"
 * 2 : int64_t number "1232"
 */
void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  int total_count = 0;
  int row_count = 0;
  bool is_row_changed = false;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed)
    {
      ++row_count;
      fprintf(stderr,"table_id:%lu, rowkey:\n", cell_info->table_id_);
      hex_dump(cell_info->row_key_.ptr(),static_cast<int32_t>(cell_info->row_key_.length()));
    }
    fprintf(stderr, "%s\n", common::print_cellinfo(cell_info, "CLI_GET"));
    ++total_count;
  }
  fprintf(stderr, "row_count=%d,total_count=%d\n", row_count, total_count);
}

int dump_tablet_info(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t port = 0;
  int64_t version = 0;
  int64_t occupy_size = 0;
  int64_t total_row_count = 0;

  ObServer server;
  char tmp_buf[32];
  ObRowkey start_key;
  ObRowkey end_key;
  ObMemBuf start_key_buf;
  ObMemBufAllocatorWrapper start_key_allocator(start_key_buf);
  ObCellInfo * cell = NULL;
  ObScannerIterator iter;
  bool row_change = false;
  int row_count = 0;

  start_key.set_min_row();

  iter = scanner.begin();
  for (; OB_SUCCESS == ret && iter != scanner.end() ; ++iter)
  {
    ret = iter.get_cell(&cell, &row_change);
    //fprintf(stderr, "row_change=%d, column_name=%.*s\n", row_change,
     //   cell->column_name_.length(), cell->column_name_.ptr());
    //cell->value_.dump();

    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
      break;
    }

    if (row_change && OB_SUCCESS == ret)
    {
      end_key = cell->row_key_;
      row_count++;
      ip = port = version = 0;
      fprintf(stderr, "\nrange:(%s; %s]\n", to_cstring(start_key), to_cstring(end_key));
      if (OB_SUCCESS != (ret = end_key.deep_copy(start_key, start_key_allocator)))
      {
        TBSYS_LOG(ERROR, "failed to copy end key, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret && cell != NULL)
    {
      if ((cell->column_name_.compare("1_port") == 0)
          || (cell->column_name_.compare("2_port") == 0)
          || (cell->column_name_.compare("3_port") == 0))
      {
        ret = cell->value_.get_int(port);
        //TBSYS_LOG(DEBUG,"port is %ld",port);
      }
      else if ((cell->column_name_.compare("1_ipv4") == 0)
          || (cell->column_name_.compare("2_ipv4") == 0)
          || (cell->column_name_.compare("3_ipv4") == 0))
      {
        ret = cell->value_.get_int(ip);
        //TBSYS_LOG(DEBUG,"ip is %ld",ip);
      }
      else if (cell->column_name_.compare("1_tablet_version") == 0 ||
          cell->column_name_.compare("2_tablet_version") == 0 ||
          cell->column_name_.compare("3_tablet_version") == 0)
      {
        ret = cell->value_.get_int(version);
        //hex_dump(cell->row_key_.ptr(),cell->row_key_.length(),false,TBSYS_LOG_LEVEL_INFO);
        //TBSYS_LOG(DEBUG,"tablet_version is %d",version);
      }
      else if (cell->column_name_.compare("occupy_size") == 0 )
      {
        ret = cell->value_.get_int(occupy_size);
        fprintf(stderr, "occupy_size=%ld\n", occupy_size);
      }
      else if (cell->column_name_.compare("record_count") == 0 )
      {
        ret = cell->value_.get_int(total_row_count);
        fprintf(stderr, "record_count=%ld\n", total_row_count);
      }

      //fprintf(stderr, "%.*s\n", cell->column_name_.length(), cell->column_name_.ptr());

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
        break;
      }
    }

    if (port != 0 && ip != 0 && version != 0)
    {
      server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
      server.to_string(tmp_buf,sizeof(tmp_buf));
      if (row_count > 0) fprintf(stderr, "server= %s ,version= %ld\n", tmp_buf, version);
      ip = port = version = 0;
    }
  }

  return ret;
}



int parse_rowkey_type_array(const char* rowkey_type_str, int* array, int32_t& size)
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

int parse_rowkey_obj_array(const int* type_array, const int32_t size, const char* rowkey_value_str, ObObj* obj_array)
{
  int ret = OB_SUCCESS;
  int64_t tmp_value = 0;
  int32_t tmp_value32 = 0;
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
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          tmp_value32 = atoi(list[i]);
          obj_array[i].set_int32(tmp_value32);
          break;
        //add 20151008:e
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
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
          obj_array[i].set_date(tmp_value);
          break;
        case ObTimeType:
          obj_array[i].set_time(tmp_value);
          break;
        //add 20150906:e
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
          {
            ObString str_value(0, static_cast<ObString::obstr_size_t>(strlen(list[i])), list[i]);
            obj_array[i].set_varchar(str_value);
          }
        case ObSeqType:
          break;
        case ObExtendType:
          obj_array[i].set_ext(tmp_value);
          break;
        default:
          break;
      }
    }
  }

  return ret;
}

int parse_rowkey(const char* rowkey_type_str, const char* rowkey_value_str, PageArena<char>& allocer, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int size = OB_MAX_ROWKEY_COLUMN_NUMBER;
  int type_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey tmp_rowkey;

  ret = parse_rowkey_type_array(rowkey_type_str, type_array, size);
  if (OB_SUCCESS == ret)
  {
    ret = parse_rowkey_obj_array(type_array, size, rowkey_value_str, obj_array);
  }

  if (OB_SUCCESS == ret)
  {
    tmp_rowkey.assign(obj_array, size);
    ret = tmp_rowkey.deep_copy(rowkey, allocer);
  }

  return ret;
}

void dump_tablet_image(ObTabletImage & image, bool load_sstable)
{
  ObTablet *tablet  = NULL;
  int tablet_index = 0;

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret)
    {
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, to_cstring(tablet->get_range()));
      dump_tablet(*tablet, load_sstable);

      ++tablet_index;
    }
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();
}

void dump_multi_version_tablet_image(ObMultiVersionTabletImage & image, bool load_sstable)
{
  ObTablet *tablet  = NULL;
  int tablet_index = 0;

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret)
    {
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, to_cstring(tablet->get_range()));
      dump_tablet(*tablet, load_sstable);

      ++tablet_index;
    }
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();
}

int dump_tablet(const ObTablet & tablet, const bool dump_sstable)
{
  // dump sstable info
  const common::ObSEArray<sstable::ObSSTableId, 2>& sstable_id_list_
    = tablet.get_sstable_id_list();
  int64_t size = sstable_id_list_.count();

  // dump tablet basic info
  fprintf(stderr, "range=%s, data version=%ld, disk_no=%d, "
      "merged=%d, removed=%d, last do expire version=%ld, seq num=%ld, sstable version=%d,"
      "row count=%ld, occupy_size=%ld, data_checksum=%ld, row_checksum=%ld",
      to_cstring(tablet.get_range()), tablet.get_data_version(), tablet.get_disk_no(),
      tablet.is_merged(), tablet.is_removed(), tablet.get_last_do_expire_version(), tablet.get_sequence_num(),
      tablet.get_sstable_version(), tablet.get_row_count(), 
      tablet.get_occupy_size(), tablet.get_checksum(), tablet.get_row_checksum());

  if (dump_sstable)
  {
    const_cast<ObTablet&>(tablet).load_sstable(tablet.get_data_version());
    const common::ObSEArray<sstable::SSTableReader*, 2> & sstable_reader_list_
      = tablet.get_sstable_reader_list();
    for (int64_t i = 0; i < sstable_reader_list_.count() ; ++i)
    {
      const sstable::SSTableReader* reader = sstable_reader_list_.at(i);
      if (NULL != reader)
      {
        fprintf(stderr, "sstable [%ld]: id=%ld, row count=%ld, sstable size=%ld,"
            "checksum=%ld\n", i, sstable_id_list_.at(i).sstable_file_id_,
            reader->get_row_count(), reader->get_sstable_size(), reader->get_sstable_checksum());
      }
    }
  }
  else
  {
    for (int64_t i = 0; i < size; ++i)
    {
      fprintf(stderr, "sstable [%ld]: id=%ld\n",
          i, sstable_id_list_.at(i).sstable_file_id_) ;
    }
  }
  return OB_SUCCESS;
}
