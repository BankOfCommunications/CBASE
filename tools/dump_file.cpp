/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include <ctype.h>
#include <tbsys.h>
#include <dirent.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_crc64.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "common/ob_array_helper.h"
#include "ob_tablet_meta.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_disk_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;

using oceanbase::obsolete::chunkserver::ObTabletMeta;


static void hex_dump_nice(const void* data, const int32_t size, const bool char_type)
{
  /* dumps size bytes of *data to stdout. Looks like:
   * [0000] 75 6E 6B 6E 6F 77 6E 20
   * 30 FF 00 00 00 00 39 00 unknown 0.....9.
   * (in a single line of course)
   */

  unsigned const char *p = (unsigned char*)data;
  unsigned char c = 0;
  int n = 0;
  char bytestr[4] = {0};
  char addrstr[10] = {0};
  char hexstr[ 16*3 + 5] = {0};
  char charstr[16*1 + 5] = {0};

  for(n = 1; n <= size; n++) 
  {
    if (n%16 == 1) 
    {
      /* store address for this line */
      snprintf(addrstr, sizeof(addrstr), "%.4x",
          (int)((unsigned long)p-(unsigned long)data) );
    }

    c = *p;
    if (isprint(c) == 0) 
    {
      c = '.';
    }

    /* store hex str (for left side) */
    snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
    strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

    /* store char str (for right side) */
    snprintf(bytestr, sizeof(bytestr), "%c", c);
    strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

    if (n % 16 == 0)
    { 
      /* line completed */
      if (char_type) 
        fprintf(stderr, "[%4.4s]   %-50.50s  %s\n", 
            addrstr, hexstr, charstr);
      else 
        fprintf(stderr, "[%4.4s]   %-50.50s\n", 
            addrstr, hexstr);
      hexstr[0] = 0;
      charstr[0] = 0;
    } 
    else if(n % 8 == 0) 
    {
      /* half line: add whitespaces */
      strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
      strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
    }
    p++; /* next byte */
  }

  if (strlen(hexstr) > 0) 
  {
    /* print rest of buffer if not empty */
    if (char_type) 
      fprintf(stderr, "[%4.4s]   %-50.50s  %s\n", 
          addrstr, hexstr, charstr);
    else 
      fprintf(stderr, "[%4.4s]   %-50.50s\n", 
          addrstr, hexstr);
  }
}

static void hex_dump_range(const ObRange& range)
{
  fprintf(stderr, "table id:%ld,border flag:%d\n", range.table_id_, range.border_flag_.get_data());
  fprintf(stderr, "dump start key with hex format:\n");
  hex_dump(range.start_key_.ptr(), range.start_key_.length(), true);
  fprintf(stderr, "dump end key with hex format:\n");
  hex_dump(range.end_key_.ptr(), range.end_key_.length(), true);
}


int dump_tablet(const char* idx_path)
{
  int err = OB_SUCCESS;

  if (NULL == idx_path)
  {
    fprintf(stderr, "invalid param, idx_path=%p\n", idx_path);
    err = OB_SUCCESS;
  }
  else
  {
    ObTabletMeta tablet_meta;
    err = tablet_meta.init(idx_path);
    if (OB_SUCCESS != err)
    {
      fprintf(stderr, "failed to init tablet meta, idx_path=%s, err=%d\n",
          idx_path, err);
    }
    else
    {
      err = tablet_meta.load_meta_info();
      if (OB_SUCCESS != err)
      {
        fprintf(stderr, "failed to load meta info, err=%d\n", err);
      }
    }

    if (OB_SUCCESS == err)
    {
      // TODO (rizhao) refine: appends tablet to tablet array and qsort
      for (ObTabletMeta::iterator it = tablet_meta.begin(); OB_SUCCESS == err && it != tablet_meta.end(); ++it)
      {
        /*
           err = dump_tablet(
           it->second.range_,
           it->second.path_.ptr(),
           it->second.sstable_id_.sstable_file_id_,
           it->second.sstable_id_.sstable_file_offset_);
           */
        fprintf(stderr, "\n---------------------begin dump-----------------------------\n");
        fprintf(stderr, "sstable file id:%ld, file offset:%ld, data path:%s \n", 
            it->second.sstable_id_.sstable_file_id_,
            it->second.sstable_id_.sstable_file_offset_, 
            it->second.path_.ptr());
        hex_dump_range(it->second.range_);
        fprintf(stderr, "---------------------end dump  -----------------------------\n");
      }
    }
  }

  return err;
}

int dump_sstable_file(const char* filename, const uint64_t sstable_file_id)
{
  ModuleArena arena(64*1024,ModulePageAllocator());
  FileInfoCache fileinfo_cache_;
  ObSSTableReader reader(arena, fileinfo_cache_);
  ObSSTableId id;
  id.sstable_file_id_ = sstable_file_id;
  int ret = reader.open(id);
  if (OB_SUCCESS != ret) return ret;

//fprintf(stderr, "total count:%ld\n", reader.get_row_count());
  /*
  ObString start_key = reader.get_start_key(reader.get_trailer().get_table_id(0));
  ObString end_key = reader.get_end_key(reader.get_trailer().get_table_id(0));
  hex_dump(start_key.ptr(), start_key.length(), true);
  hex_dump(end_key.ptr(), end_key.length(), true);
  */

  const ObSSTableTrailer & trailer = reader.get_trailer();
  fprintf(stderr, "block count:%ld, block index record offset :%ld, "
      "block index record size:%ld\n",
      trailer.get_block_count(), 
      trailer.get_block_index_record_offset(), 
      trailer.get_block_index_record_size());

  const ObSSTableSchema & schema = *reader.get_schema();

  for (int i = 0; i < schema.get_column_count(); ++i)
  {
    const ObSSTableSchemaColumnDef* def = schema.get_column_def(i);
    fprintf(stderr, "column:%d, id:(%u), type(%d)\n", i, 
        def->column_name_id_, def->column_value_type_);
  }
  return 0;
}

int dump_new_index(const char* filename, const int32_t disk_no)
{
  ObTabletImage image;
  int err = image.read(filename, disk_no);
  if (OB_SUCCESS == err)
  {
    image.dump(true);
  }
  else
  {
    fprintf(stderr, "read image file failed\n");
  }
  return err;
}

int parse_range_str(const char* range_str, int hex_format, ObRange &range)
{
  int len = strlen(range_str);
  if (len < 5) return OB_ERROR;
  const char start_border = range_str[0];
  if (start_border == '[')
    range.border_flag_.set_inclusive_start();
  else if (start_border == '(')
    range.border_flag_.unset_inclusive_start();
  else
  {
    fprintf(stderr, "start char of range_str(%c) must be [ or (\n", start_border);
    return OB_ERROR;
  }
  const char end_border = range_str[len - 1];
  if (end_border == ']')
    range.border_flag_.set_inclusive_end();
  else if (end_border == ')')
    range.border_flag_.unset_inclusive_end();
  else
  {
    fprintf(stderr, "end char of range_str(%c) must be [ or (\n", end_border);
    return OB_ERROR;
  }

  const char* sp = range_str + 1;
  char* del = strstr(sp, ",");
  if (NULL == del)
  {
    fprintf(stderr, "cannot find , in range_str(%s)\n", range_str);
    return OB_ERROR;
  }

  while (*sp == ' ') ++sp; // skip space
  int start_len = del - sp;
  if (start_len <= 0) 
  {
    fprintf(stderr, "start key cannot be NULL\n");
    return OB_ERROR;
  }

  if (strncasecmp(sp, "min", 3) == 0 )
  {
    range.border_flag_.set_min_value();
  }
  else 
  {
    if (hex_format == 0)
    {
      range.start_key_.assign_ptr((char*)sp, start_len);
    }
    else
    {
      char *hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
      int hexlen = str_to_hex(sp, start_len, hexkey, OB_RANGE_STR_BUFSIZ);
      range.start_key_.assign_ptr(hexkey, hexlen/2);
    }
  }

  const char* ep = del + 1;
  while (*ep == ' ') ++ep;
  int end_len = range_str + len - 1 - ep;
  if (end_len <= 0)
  {
    fprintf(stderr, "end key cannot be NULL\n");
    return OB_ERROR;
  }

  if (strncasecmp(ep, "max", 3) == 0)
  {
    range.border_flag_.set_max_value();
  }
  else
  {
    if (hex_format == 0)
    {
      range.end_key_.assign_ptr((char*)ep, end_len);
    }
    else
    {
      char *hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
      int hexlen = str_to_hex(ep, end_len, hexkey, OB_RANGE_STR_BUFSIZ);
      range.end_key_.assign_ptr(hexkey, hexlen/2);
    }
  }

  return OB_SUCCESS;
}

int meta_filter(const struct dirent *d)
{
  int ret = 0;
  if (NULL != d)
  {
    ret = (0 == strncmp(d->d_name,"idx_",4)) ? 1 : 0;
  }
  return ret;
}

int find_tablet(const char* data_dir, const char* app_name, 
    int32_t disk_no, const ObRange &range)
{
  char sstable_dir[OB_MAX_FILE_NAME_LENGTH];
  snprintf(sstable_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%d/%s/sstable", data_dir, disk_no, app_name);
  struct dirent **result = NULL;
  int meta_files = scandir(sstable_dir, &result, meta_filter,::versionsort);
  if (meta_files < 0) return OB_ERROR;
  char sstable_idx_file[OB_MAX_FILE_NAME_LENGTH];
  ObTablet* dest_tablet = NULL;
  for (int i = 0; i < meta_files; ++i)
  {
    ObTabletImage image;
    snprintf(sstable_idx_file, OB_MAX_FILE_NAME_LENGTH, 
        "%s/%s", sstable_dir, result[i]->d_name);
    int ret = image.read(sstable_idx_file, disk_no, false);
    if (ret) return ret;
    ret = image.acquire_tablet(range, 1, dest_tablet);
    if (OB_SUCCESS == ret)
    {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      dest_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      fprintf(stderr, "----------------------------------\n");
      fprintf(stderr, "found in tablet idx=%s, disk_no=%d,"
          "version=%ld, range=<%s>\n", 
          sstable_idx_file, disk_no, dest_tablet->get_data_version(), range_buf);
      const ObArrayHelper<ObSSTableId>& sstable_id_list = 
        dest_tablet->get_sstable_id_list();
      for (int j = 0; j < sstable_id_list.get_array_index(); ++j)
      {
        ObSSTableId * id = sstable_id_list.at(j);
        char sstable_file[OB_MAX_FILE_NAME_LENGTH];
        snprintf(sstable_file, OB_MAX_FILE_NAME_LENGTH, "%s/%ld", 
            sstable_dir, id->sstable_file_id_);
        ModuleArena arena(64*1024,ModulePageAllocator());
        FileInfoCache fileinfo_cache_;
        ObSSTableReader reader(arena, fileinfo_cache_);
        ret = reader.open(*id);
        if (OB_SUCCESS != ret) continue;
        fprintf(stderr, "sstable[%d] id=%ld, file=%s, "
            "row_count=%ld, occupy_size=%ld, crcsum=%lu\n", 
            j, id->sstable_file_id_, sstable_file, 
            reader.get_trailer().get_row_count(),
            reader.get_sstable_size(),
            reader.get_trailer().get_sstable_checksum());
      }
      fprintf(stderr, "----------------------------------\n");
      image.release_tablet(dest_tablet);
    }
  }
  free(result);
  return OB_SUCCESS;
}

int search_index(const char* data_dir, const char* app_name, 
    const int64_t table_id, const char* range_str, int hex_format)
{
  ObRange range;
  int ret = parse_range_str(range_str, hex_format, range);
  if (OB_SUCCESS != ret) return ret;
  range.table_id_ = table_id;
  char buf[OB_RANGE_STR_BUFSIZ*2];
  ret = range.to_string(buf, OB_RANGE_STR_BUFSIZ*2);
  if (OB_SUCCESS == ret) fprintf(stderr, "search range:%s\n", buf);
  else fprintf(stderr, "range to_string failed\n");
  //range.hex_dump();

  ObDiskManager disk_manager;
  int64_t max_sstable_size = 256 * 1024 * 1024;
  ret = disk_manager.scan(data_dir, max_sstable_size);
  if (OB_SUCCESS != ret) return ret;
  int32_t disk_num = 0;
  const int32_t * disk_array = disk_manager.get_disk_no_array(disk_num);

  for (int i = 0; i < disk_num; ++i)
  {
    find_tablet(data_dir, app_name, disk_array[i], range);
  }

  return OB_SUCCESS;
}

int main (int argc, char* argv[])
{
  int type = 1;
  int disk_no = 0;
  const char* file_name = 0;
  const char* range_str = 0;
  const char* app_name = 0;
  int i = 0;
  int64_t table_id = 0;
  int silent = 0;
  int hex_format = 0;
  while ((i = getopt(argc, argv, "a:b:r:t:f:i:qx")) != EOF) 
  {
    switch (i) 
    {
      case 't':
        type = atoi(optarg);
        break;
      case 'f':
        file_name = optarg;
        break;
      case 'i':
        disk_no = atoi(optarg);
        break;
      case 'r':
        range_str = optarg;
        break;
      case 'b':
        table_id = strtoll(optarg, NULL, 10);
        break;
      case 'a':
        app_name = optarg;
        break;
      case 'q':
        silent = 1;
        break;
      case 'x':
        hex_format = 1;
        break;
      case 'h':
        fprintf(stderr, "Usage: %s "
            "-t file_type [1|2|3|4] -f file_path [-i disk_no] [-r range_str]\n", argv[0]);
        return OB_ERROR;
    }
  }

  if (silent) TBSYS_LOGGER.setLogLevel("ERROR");
  if (NULL == file_name)
  {
    fprintf(stderr, "-f file_name cannot be NULL\n");
    return -1;
  }

  if (4 == type && (NULL == range_str || NULL == app_name || 0 == table_id) )
  {
    fprintf(stderr, "-r range_str cannot be NULL, -b table_id cannot be 0, -a app_name \n");
    return -1;
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();

  switch (type) 
  {
    case 1:
      dump_tablet(file_name);
      break;
    case 2:
      dump_sstable_file(file_name, 0);
      break;
    case 3:
      dump_new_index(file_name, disk_no);
      break;
    case 4:
      search_index(file_name, app_name, table_id, range_str, hex_format);
      break;
    default:
      fprintf(stderr, "error type:%d\n", type);
      break;
  }


}
