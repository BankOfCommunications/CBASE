/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * dumpsst.cpp is for what ...
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *   huating <huating.zmq@taobao.com>
 *   rongxuan <rongxuan.lc@taobao.com>
 *
 */


#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_rowkey.h"
#include "common/ob_object.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_record_header.h"
#include "common/compress/lzo_compressor.h"
#include "sstable/ob_sstable_reader.h"
#include "chunkserver/ob_tablet_image.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_row.h"
#include "chunkserver/ob_disk_manager.h"
#include "common/ob_range.h"
#include "common/ob_array_helper.h"
#include "common/ob_schema_manager.h"
//#include "ob_tablet_meta.h"
#include "sstable/ob_sstable_block_reader.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_scanner.h"
#include "dumpsst.h"
#include "common_func.h"
#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <tbsys.h>
#include <dirent.h>
#include <string.h>

//using oceanbase::obsolete::chunkserver::ObTabletMeta;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;

const char *g_sstable_directory = NULL;
const int64_t LARGE_BUFSIZ = 1024 * 512L; // 512KB
ObSchemaManagerV2 g_schema;
ObMergerSchemaManager g_sm;

void hex_dump_rowkey(const void* data, const int32_t size,
    const bool char_type)
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

void print_obj(int o,const ObObj& obj)
{
  int64_t int_val = 0;
  float float_val = 0.0;
  double double_val = 0.0;
  bool is_add = false;
  ObString str_val;
  fprintf(stderr,"COLUMN %d:",o);
  switch(obj.get_type())
  {
    case ObNullType:
      fprintf(stderr, "type:ObNull\n");
      break;
    case ObIntType:
      obj.get_int(int_val,is_add);
      fprintf(stderr,"type:ObInt, val:%ld,is_add:%s\n",int_val,is_add ? "true" : "false");
      break;
    case ObVarcharType:
      obj.get_varchar(str_val);
      fprintf(stderr,"type:ObVarChar,len :%d,val:\n",str_val.length());
      hex_dump_rowkey(str_val.ptr(),str_val.length(),true);
      break;
    case ObFloatType:
      obj.get_float(float_val,is_add);
      fprintf(stderr,"type:ObFloat, val:%f,is_add:%s\n",float_val,is_add ? "true" : "false");
      break;
    case ObDoubleType:
      obj.get_double(double_val,is_add);
      fprintf(stderr,"type:ObDouble, val:%f,is_add:%s\n",double_val,is_add ? "true" : "false");
      break;
    case ObDateTimeType:
      obj.get_datetime(int_val,is_add);
      fprintf(stderr,"type:ObDateTime(seconds), val:%ld,is_add:%s\n",int_val,is_add ? "true" : "false");
      break;
    case ObPreciseDateTimeType:
      obj.get_precise_datetime(int_val,is_add);
      fprintf(stderr,"type:ObPreciseDateTime(microseconds), val:%ld,is_add:%s\n",int_val,is_add ? "true" : "false");
      break;
    case ObSeqType:
      //TODO
      break;
    case ObCreateTimeType:
      obj.get_createtime(int_val);
      fprintf(stderr,"type:ObCreateTime, val:%ld\n",int_val);
      break;
    case ObModifyTimeType:
      obj.get_modifytime(int_val);
      fprintf(stderr,"type:ObModifyTime, val:%ld\n",int_val);
      break;
    case ObExtendType:
      obj.get_ext(int_val);
      fprintf(stderr,"type:ObExt, val:%ld\n",int_val);
      break;
    default:
      fprintf(stderr,"unexpected type (%d)\n",obj.get_type());
      break;
  }
}

void print_row(int r,const ObSSTableRow& row)
{
  fprintf(stderr,"ROW %d ROW_KEY:\n",r);
  //hex_dump_rowkey(row.get_row_key().ptr(),row.get_row_key().length(),false);
  for(int i=0; i < row.get_obj_count(); ++i)
  {
    print_obj(i,*row.get_obj(i));
  }
  fprintf(stderr,"ROW %d END\n",r);
}

void display_trailer_table_info(const ObSSTableTrailer& trailer)
{
  UNUSED(trailer);
  /*
     int64_t table_count = trailer.get_table_count();
     uint64_t table_id   = OB_INVALID_ID;
     ObString start_key;
     ObString end_key;

     for (int64_t i = 0; i < table_count; ++i)
     {
     table_id = trailer.get_table_id(i);
     start_key = trailer.get_start_key(table_id);
     end_key = trailer.get_end_key(table_id);

     fprintf(stderr, "table_id_: %lu \n"
     "start_key: ", table_id);
     hex_dump_rowkey(start_key.ptr(), start_key.length(), false);
     fprintf(stderr, "end_key: ");
     hex_dump_rowkey(end_key.ptr(), end_key.length(), false);
     }
     */
}

int change_new_index(
    const char* idx_file,
    const int64_t version,
    const int32_t disk_no,
    const char* action,
    const int64_t table_id,
    const char* range_str,
    const char* new_range_str,
    const int32_t hex_format,
    const bool dump_sstable
    )
{
  FileInfoCache cache;
  cache.init(128);
  ObTabletImage image;
  image.set_fileinfo_cache(cache);

  int64_t parse_version = 0;
  int32_t parse_disk_no = 0;

  ObNewRange range;
  range.table_id_ = table_id;
  ObTablet* tablet = NULL;

  char idx_path[OB_MAX_FILE_NAME_LENGTH];
  snprintf(idx_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", g_sstable_directory, idx_file);

  int num = sscanf(idx_file, "idx_%ld_%d", &parse_version, &parse_disk_no);
  if (num < 2)
  {
    parse_version = version;
    parse_disk_no = disk_no;
  }

  fprintf(stderr, "change_meta, action=%s,  idx_path=%s, version=%ld, disk=%d, table=%ld, range_str=%s, new_range_str=%s\n",
      action, idx_path, parse_version, parse_disk_no, table_id, range_str, new_range_str);

  image.set_data_version(parse_version);
  int err = image.read(idx_path, parse_disk_no, dump_sstable);
  if (OB_SUCCESS != err)
  {
    fprintf(stderr, "read image file failed\n");
  }
  else if (NULL == action)
  {
    fprintf(stderr, "action not set\n");
  }
  else if (OB_SUCCESS != (err = parse_range_str(range_str, hex_format, range)))
  {
    fprintf(stderr, "parse_range_str %s error.\n", range_str);
  }
  else if (strcmp(action, "remove_range") == 0)
  {
    err = image.remove_tablet(range, parse_disk_no);
  }
  else if (strcmp(action, "change_range") == 0)
  {
    ObNewRange new_range;
    //ObTablet * new_tablet = NULL;
    if (NULL == new_range_str)
    {
      err = OB_ERROR;
    }
    else if (OB_SUCCESS != (err = parse_range_str(new_range_str, hex_format, new_range)))
    {
      fprintf(stderr, "parse_range_str %s error.\n",new_range_str);
    }
    else if (OB_SUCCESS != (err = image.acquire_tablet(range,
            ObMultiVersionTabletImage::SCAN_FORWARD, tablet)))
    {
      fprintf(stderr, "find table %s error.\n", range_str);
    }
    else
    {
      char range_buf[OB_MAX_FILE_NAME_LENGTH];
      tablet->get_range().to_string(range_buf, OB_MAX_FILE_NAME_LENGTH);
      fprintf(stderr, "before change_range: %s\n", range_buf);
      new_range.table_id_ = table_id;
      tablet->set_range(new_range);
      tablet->get_range().to_string(range_buf, OB_MAX_FILE_NAME_LENGTH);
      fprintf(stderr, "after change_range: %s\n", range_buf);
    }
  }
  else if (strcmp(action, "set_merged_flag") == 0)
  {
    err = image.acquire_tablet(range,
        ObMultiVersionTabletImage::SCAN_FORWARD, tablet);
    if (OB_SUCCESS == err && NULL != tablet)
    {
      tablet->set_merged(1);
    }
  }
  else if (strcmp(action, "clear_merged_flag") == 0)
  {
    err = image.acquire_tablet(range,
        ObMultiVersionTabletImage::SCAN_FORWARD, tablet);
    if (OB_SUCCESS == err && NULL != tablet)
    {
      tablet->set_merged(0);
      tablet->set_removed(0);
    }
  }
  else if (strcmp(action, "find_tablet") == 0)
  {
    err = image.acquire_tablet(range,
        ObMultiVersionTabletImage::SCAN_FORWARD, tablet);
    if (OB_SUCCESS == err && NULL != tablet)
    {
      dump_tablet(*tablet, dump_sstable);
    }
  }
  else if (strcmp(action, "clear_all_merged_flag") == 0)
  {
    err = image.begin_scan_tablets();
    while (OB_SUCCESS == err)
    {
      err = image.get_next_tablet(tablet);
      if (OB_SUCCESS == err)
      {
        tablet->set_merged(0);
        tablet->set_removed(0);
      }
      if (NULL != tablet)
      {
        image.release_tablet(tablet);
      }
    }
    err = image.end_scan_tablets();
  }

  snprintf(idx_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s.new", g_sstable_directory, idx_file);
  if (OB_SUCCESS == err && strcmp(action, "find_tablet") != 0)
  {
    image.write(idx_path, parse_disk_no);
  }
  else if (OB_SUCCESS != err)
  {
    fprintf(stderr, "action err:%d\n", err);
  }

  if (NULL != tablet) image.release_tablet(tablet);

  return err;
}

//from file dump_file.cpp
int dump_new_index(const char* idx_file, const int64_t version, const int32_t disk_no, const bool load_sstable)
{
  FileInfoCache cache;
  cache.init(128);
  ObTabletImage image;
  image.set_fileinfo_cache(cache);
  char idx_path[OB_MAX_FILE_NAME_LENGTH];
  snprintf(idx_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", g_sstable_directory, idx_file);
  int64_t parse_version = 0;
  int32_t parse_disk_no = 0;
  int num = sscanf(idx_file, "%*[^_]_%ld_%d", &parse_version, &parse_disk_no);
  if (num < 2)
  {
    parse_version = version;
    parse_disk_no = disk_no;
  }
  image.set_data_version(parse_version);
  int err = image.read(idx_path, parse_disk_no, load_sstable);
  if (OB_SUCCESS == err)
  {
    dump_tablet_image(image, load_sstable);
  }
  else
  {
    fprintf(stderr, "read image file failed\n");
  }
  return err;
}

//from file dump_file.cpp

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
    int32_t disk_no, const ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if(NULL == data_dir || NULL == app_name)
  {
    fprintf(stderr,"the input param error\n");
    ret = OB_INVALID_ARGUMENT;
  }
  char sstable_dir[OB_MAX_FILE_NAME_LENGTH];
  struct dirent **result = NULL;
  int meta_files = 0;
  if(OB_SUCCESS == ret)
  {
    snprintf(sstable_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%d/%s/sstable", data_dir, disk_no, app_name);
    meta_files = scandir(sstable_dir, &result, meta_filter,::versionsort);
    if (meta_files < 0)
      ret = OB_ERROR;
  }
  char sstable_idx_file[OB_MAX_FILE_NAME_LENGTH];
  ObTablet* dest_tablet = NULL;
  int64_t version = 0;
  if(OB_SUCCESS == ret)
  {
    for (int i = 0; i < meta_files; ++i)
    {
      ObTabletImage image;
      sscanf(result[i]->d_name, "idx_%ld_%d", &version, &disk_no);
      snprintf(sstable_idx_file, OB_MAX_FILE_NAME_LENGTH,
          "%s/%s", sstable_dir, result[i]->d_name);
      image.set_data_version(version);
      int ret = image.read(sstable_idx_file, disk_no, false);
      if (ret) break;
      if(OB_SUCCESS == ret)
        ret = image.acquire_tablet(range, 1, dest_tablet);

      if (OB_SUCCESS == ret)
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        dest_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        fprintf(stderr, "----------------------------------\n");
        fprintf(stderr, "found in tablet idx=%s, disk_no=%d,"
            "version=%ld, range=<%s>\n",
            sstable_idx_file, disk_no, dest_tablet->get_data_version(), range_buf);
        dump_tablet(*dest_tablet, true);
        fprintf(stderr, "----------------------------------\n");
        image.release_tablet(dest_tablet);
      }
    }
  }
  if(NULL != result)
    free(result);
  return ret;
}

int search_index(const char* data_dir, const char* app_name,
    const int64_t table_id, const char* range_str, int hex_format)
{
  int ret = 0;
  if(NULL == data_dir && NULL == app_name)
  {
    fprintf(stderr,"the input param error!\n");
    ret = -2;
  }
  ObNewRange range;
  if(0 == ret)
  {
    ret = parse_range_str(range_str, hex_format, range);
  }
  char buf[OB_RANGE_STR_BUFSIZ*2];
  if(0 == ret)
  {
    range.table_id_ = table_id;
    range.to_string(buf, OB_RANGE_STR_BUFSIZ*2);
    fprintf(stderr, "search range:%s\n", buf);
  }

  ObDiskManager disk_manager;
  int64_t max_sstable_size = 256 * 1024 * 1024;
  if(0 == ret)
  {
    ret = disk_manager.scan(data_dir, max_sstable_size);
  }
  int32_t disk_num = 0;
  if(0 == ret)
  {
    const int32_t * disk_array = disk_manager.get_disk_no_array(disk_num);

    for (int i = 0; i < disk_num; ++i)
    {
      find_tablet(data_dir, app_name, disk_array[i], range);
    }
  }
  return ret;
}

//from cmp_sstable.cpp
int get_content(int fd, int64_t offset, int64_t size, char * buf)
{
  int ret = 0;

  if(0 == ret)
  {
    ret = static_cast<int32_t>(pread(fd, buf, size, offset));
    if (ret != size)
    {
      printf("read fd =%d failed, errno=%d, %s\n", fd, errno, strerror(errno));
      ret = -1;
    }
    else ret = 0;
  }
  return ret;
}

int dump_trailer(const char* header, const ObSSTableTrailer &trailer)
{
  fprintf(stderr, "-----------------------------------------------------\n");
  fprintf(stderr, "%s, size=%d, row_count=%ld, block_count=%ld\n",
      header, trailer.get_size(), trailer.get_row_count(), trailer.get_block_count());
  /*
     fprintf(stderr, "dump start_key, end_key:\n");
     ObString start_key = trailer.get_start_key(trailer.get_table_id(0));
     ObString end_key = trailer.get_end_key(trailer.get_table_id(0));
     hex_dump(start_key.ptr(), start_key.length());
     hex_dump(end_key.ptr(), end_key.length());
     */
  fprintf(stderr, "-----------------------------------------------------\n");
  return 0;
}

int DumpSSTable::get_block_index(const char* file_name, const ObSSTableTrailer &trailer, ObSSTableBlockIndexV2* &index_object)
{
  //reader block index;
  int ret = 0;
  if(NULL == file_name)
  {
    fprintf(stderr,"the input file_name is null\n");
    ret = -2;
  }
  int fd = 0;
  if(0 == ret)
  {
    fd = ::open(file_name, O_RDONLY);
    if (fd < 0)
    {
      fprintf(stderr, "open file %s failure %d,%s\n", file_name, errno, strerror(errno));
      ret = OB_ERROR;
    }
  }
  int64_t block_index_size = trailer.get_block_index_record_size();
  int64_t block_index_offset = trailer.get_block_index_record_offset();
  char* block_index_buffer = NULL;
  int64_t object_size = 0;
  char* base_ptr = NULL;
  int64_t bi_size = sizeof(ObSSTableBlockIndexV2);
  ObSSTableBlockIndexV2* tmp_index = NULL;

  if (0 == ret)
  {
    block_index_buffer = (char*)ob_malloc(block_index_size + bi_size, ObModIds::TEST);
    if (NULL == block_index_buffer)
    {
      fprintf(stderr, "malloc block index buffer failure\n");
      ret = OB_ERROR;
    }
  }
  if(0 == ret)
  {
    get_content(fd, block_index_offset, block_index_size, block_index_buffer + bi_size);
    tmp_index = new (block_index_buffer) ObSSTableBlockIndexV2(block_index_size, false);
    object_size = tmp_index->get_deserialize_size();
    base_ptr = (char*)ob_malloc(object_size, ObModIds::TEST);
    if (NULL == base_ptr)
    {
      fprintf(stderr, "malloc deserialize object buffer failure\n");
      ret = OB_ERROR;
      if (fd > 0) close(fd);
      if (NULL != block_index_buffer) ob_free(block_index_buffer);
    }
  }
  if(0 == ret)
  {
    //fprintf(stderr, "object_size=%ld, pos=%ld\n", object_size, pos);

    index_object = tmp_index->deserialize_copy(base_ptr);
    if (NULL == index_object)
    {
      ret = OB_ERROR;
      fprintf(stderr, "deserialize block_index_object failure ret = %d\n", ret);
      if (fd > 0) close(fd);
      if (NULL != block_index_buffer) ob_free(block_index_buffer);
      if (NULL != base_ptr) ob_free(base_ptr);
    }
  }
  return ret;
}

int get_block_buffer(const char* file_name,
    ObSSTableReader &reader,
    ObSSTableBlockIndexV2::const_iterator block_pos,
    const char* &sstable_block_data_ptr,
    int64_t &sstable_block_data_size,
    uint64_t &data_checksum)
{
  int ret = 0;
  if(NULL == file_name)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  int fd = 0;
  if(OB_SUCCESS == ret)
  {
    fd = open(file_name, O_RDONLY);
    if (fd < 0)
    {
      fprintf(stderr, "open file %s failure %d,%s\n", file_name, errno, strerror(errno));
      ret = OB_ERROR;
    }
  }

  int64_t real_size = 0;
  int64_t max_block_size = 128 * 1024;
  char * compressed_data_ptr = NULL;
  int compressed_data_size = 0;
  if(OB_SUCCESS == ret)
  {
    compressed_data_ptr = (char*)ob_malloc(block_pos->block_record_size_, ObModIds::TEST);
    compressed_data_size = static_cast<int32_t>(block_pos->block_record_size_);
    if (NULL == compressed_data_ptr)
    {
      fprintf(stderr, "malloc block buffer failure\n");
      ret = OB_ERROR;
    }
  }
  if(OB_SUCCESS == ret)
  {
    ret = get_content(fd, block_pos->block_offset_, compressed_data_size, compressed_data_ptr);
    if (OB_SUCCESS != ret)
    {
      ret = OB_ERROR;
    }
  }
  ObRecordHeader header;
  memset(&header, 0, sizeof(header));
  sstable_block_data_size = compressed_data_size;

  if(OB_SUCCESS == ret)
  {
    ret = ObRecordHeader::check_record(
        compressed_data_ptr, compressed_data_size,
        ObSSTableWriter::DATA_BLOCK_MAGIC, header,
        sstable_block_data_ptr, sstable_block_data_size);
    if (OB_SUCCESS != ret)
    {
      ret = OB_ERROR;
    }
  }

  data_checksum = header.data_checksum_;
  real_size = sstable_block_data_size;
  char* uncompressed_data_ptr = (char*)ob_malloc(max_block_size, ObModIds::TEST);
  if(OB_SUCCESS == ret)
  {
    if (header.is_compress())
    {
      ObCompressor* dec = reader.get_decompressor();
      if (NULL != dec)
      {
        ret = dec->decompress(sstable_block_data_ptr, sstable_block_data_size,
            uncompressed_data_ptr, max_block_size, real_size);

        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "decompress failed...\n");
          ob_free(uncompressed_data_ptr);
          ret = OB_ERROR;
        }
        else
        {
          sstable_block_data_ptr = uncompressed_data_ptr;
          sstable_block_data_size = real_size;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get_decompressor failed, maybe decompress library install incorrectly.");
        ret = OB_CS_COMPRESS_LIB_ERROR;
      }
    }
    else
    {
      memcpy(uncompressed_data_ptr, sstable_block_data_ptr, sstable_block_data_size);
      sstable_block_data_ptr = uncompressed_data_ptr;
      sstable_block_data_size = real_size;
    }
  }

  /*
     pos = 0;
     ret = block_reader.deserialize(sstable_block_data_ptr, sstable_block_data_size, pos);
     */

  if (NULL != compressed_data_ptr) ob_free(compressed_data_ptr);
  if (fd > 0) close(fd);
  return ret;
}

int compare_block_content(
    const ObSSTableBlockReader &src_block_reader,
    const ObSSTableBlockReader &dst_block_reader,
    bool ignore_size
    )
{
  int ret  = 0;
  if (src_block_reader.get_row_count() != dst_block_reader.get_row_count())
  {
    fprintf(stderr, "src block row count = %d != dst row count = %d\n",
        src_block_reader.get_row_count() ,dst_block_reader.get_row_count());
    if (!ignore_size)
    {
      ret = -1;
    }
  }

  ObSSTableBlockReader::const_iterator src_it = src_block_reader.begin();
  ObSSTableBlockReader::const_iterator dst_it = dst_block_reader.begin();

  int64_t i = 0;
  ObObj id_array[OB_MAX_COLUMN_NUMBER];
  ObObj src_obj_array[OB_MAX_COLUMN_NUMBER];
  int64_t src_col_cnt = OB_MAX_COLUMN_NUMBER;
  ObObj dst_obj_array[OB_MAX_COLUMN_NUMBER];
  int64_t dst_col_cnt = OB_MAX_COLUMN_NUMBER;
  ObRowkey src_key, dst_key;
  if(0 == ret)
  {
    while (0 == ret && src_it != src_block_reader.end() && dst_it != dst_block_reader.end())
    {
      ret = src_block_reader.get_row(OB_SSTABLE_STORE_DENSE, src_it, src_key, id_array, src_obj_array, src_col_cnt);
      if (ret) { fprintf (stderr, "get src row failure i = %ld\n", i); }
      if(0 == ret)
      {
        ret = dst_block_reader.get_row(OB_SSTABLE_STORE_DENSE, dst_it, dst_key, id_array, dst_obj_array, dst_col_cnt);
        if (ret) { fprintf (stderr, "get dst row failure i = %ld\n", i);  }
      }
      if((0 == ret) && (src_key.compare(dst_key) != 0))
      {
        fprintf(stderr, "rowkey %ld not equal, src[%s], dst[%s] \n", i, to_cstring(src_key), to_cstring(dst_key));
        ret = -1;
      }

      if ((0 == ret) && (src_col_cnt != dst_col_cnt))
      {
        fprintf(stderr, "row index = %ld src_col_cnt = %ld != dst_col_cnt = %ld\n",
            i, src_col_cnt, dst_col_cnt);
        ret = -1;
      }
      if(0 == ret)
      {
        for (int64_t j = 0; j < src_col_cnt; ++j)
        {
          int ltype = src_obj_array[j].get_type();
          int rtype = dst_obj_array[j].get_type();
          if (ltype == ObCreateTimeType || ltype == ObModifyTimeType) { continue; }

          if (0 == ret && ltype != rtype)
          {
            fprintf(stderr, "row index %ld column %ld src type[%d] != dst type[%d], dump rowkey :\n",
                i, j, ltype, rtype);
            fprintf(stderr, "%s", to_cstring(src_key));
            ret = -1;
          }

          if (0 == ret && src_obj_array[j] != dst_obj_array[j])
          {
            fprintf(stderr, "row index %ld column %ld not equal, dump rowkey :\n",
                i, j);
            TBSYS_LOG(WARN, "%s", to_cstring(src_key));
            src_obj_array[j].dump(TBSYS_LOG_LEVEL_WARN);
            dst_obj_array[j].dump(TBSYS_LOG_LEVEL_WARN);
            ret = -1;
          }
          if(-1 == ret)
          {
            break;
          }
        }
      }
      if(0 == ret)
      {
        ++src_it; ++dst_it; ++i;
      }
    }
  }

  bool src_has_more = true;
  bool dst_has_more = true;
  if(ret == 0)
  {
    src_has_more= (src_it != src_block_reader.end());
    dst_has_more= (dst_it != dst_block_reader.end());
    while (ret == 0 && src_it != src_block_reader.end())
    {
      fprintf(stderr, "src has more index %ld \n", i) ;
      ret = src_block_reader.get_row(OB_SSTABLE_STORE_DENSE, src_it, src_key, id_array, src_obj_array, src_col_cnt);
      if (ret) { fprintf (stderr, "get src row failure i = %ld\n", i); }
      if(0 == ret)
      {
        TBSYS_LOG(WARN, "%s", to_cstring(src_key));
        ++src_it;
        ++i;
      }
    }
  }
  if(0 == ret)
  {
    while (0 == ret && dst_it != dst_block_reader.end())
    {
      fprintf(stderr, "dst has more index %ld \n", i) ;
      ret = dst_block_reader.get_row(OB_SSTABLE_STORE_DENSE, dst_it, dst_key, id_array, dst_obj_array, dst_col_cnt);
      if (ret) { fprintf (stderr, "get src row failure i = %ld\n", i);  }
      if(0 == ret)
      {
        TBSYS_LOG(WARN, "%s", to_cstring(dst_key));
        ++dst_it;
        ++i;
      }
    }
  }
  if(0 == ret)
  {
    if (src_has_more || dst_has_more)
      ret = -1;
  }
  return ret;
}

int64_t DumpSSTable::get_block_index_entry_size(const ObSSTableBlockIndexV2 & index_object)
{
  return index_object.end() - index_object.begin();
}

int DumpSSTable::compare_block(
    const char* src_file_name,
    const char* dst_file_name,
    ObSSTableReader &src_reader,
    ObSSTableReader &dst_reader,
    ObSSTableBlockIndexV2::const_iterator src_block_pos,
    ObSSTableBlockIndexV2::const_iterator dst_block_pos,
    bool ignore_size)
{
  int ret = OB_SUCCESS;
  if(NULL == src_file_name  || NULL == dst_file_name)
  {
    fprintf(stderr,"the input param error\n");
    ret = OB_INVALID_ARGUMENT;
  }
  const char* src_base_ptr = NULL;
  int64_t src_base_size = 0;
  uint64_t src_data_checksum = 0;
  const char* dst_base_ptr = NULL;
  int64_t dst_base_size = 0;
  uint64_t dst_data_checksum = 0;
  ObSSTableBlockReader src_block_reader, dst_block_reader;
  int64_t pos = 0;

  if(OB_SUCCESS == ret)
  {
    ret = get_block_buffer(src_file_name, src_reader, src_block_pos, src_base_ptr, src_base_size, src_data_checksum);
  }

  if(OB_SUCCESS == ret)
  {
    ret = get_block_buffer(dst_file_name, dst_reader, dst_block_pos, dst_base_ptr, dst_base_size, dst_data_checksum);
  }

  if(OB_SUCCESS == ret)
  {
    if (src_data_checksum != dst_data_checksum)
    {
      fprintf(stderr, "checksum is inconsistent, src_data_checksum=%lu, dst_data_checksum=%lu\n",
          src_data_checksum, dst_data_checksum);
      //ret = OB_ERROR;
      //goto error;
    }
  }

  const int32_t internal_bufsiz = 1024 * 128;
  int64_t column_count = 0;
  if(OB_SUCCESS == ret)
  {
    char src_internal_buf[internal_bufsiz];
    src_reader.get_schema()->get_rowkey_column_count(src_block_pos->table_id_,column_count);
    ObSSTableBlockReader::BlockDataDesc data_desc( NULL, column_count,
        src_reader.get_trailer().get_row_value_store_style());
    ObSSTableBlockReader::BlockData block_data(src_internal_buf, internal_bufsiz,
        src_base_ptr, src_base_size);
    ret = src_block_reader.deserialize(data_desc, block_data);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "deserialize src block failed\n");
    }
  }

  pos = 0;
  if(OB_SUCCESS == ret)
  {
    char dst_internal_buf[internal_bufsiz];
    dst_reader.get_schema()->get_rowkey_column_count(dst_block_pos->table_id_,column_count);
    ObSSTableBlockReader::BlockDataDesc data_desc(NULL, column_count,
        dst_reader.get_trailer().get_row_value_store_style());
    ObSSTableBlockReader::BlockData block_data(dst_internal_buf, internal_bufsiz,
        dst_base_ptr, dst_base_size);
    ret = dst_block_reader.deserialize(data_desc, block_data);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "deserialize dst block failed\n");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = compare_block_content(src_block_reader, dst_block_reader, ignore_size);
  }

  if (NULL != src_base_ptr) ob_free(const_cast<char*>(src_base_ptr));
  if (NULL != dst_base_ptr) ob_free(const_cast<char*>(dst_base_ptr));
  return ret;
}

int DumpSSTable::compare_block_index(
    const char* src_file_name,
    const char* dst_file_name,
    ObSSTableReader &src_reader,
    ObSSTableReader &dst_reader,
    const ObSSTableBlockIndexV2 & src_index,
    const ObSSTableBlockIndexV2 & dst_index,
    bool ignore_size)
{
  int ret = 0;
  if(NULL == src_file_name || NULL == dst_file_name)
  {
    fprintf(stderr,"the input param error\n");
    ret = -2;
  }
  int64_t src_size = get_block_index_entry_size(src_index);
  int64_t dst_size = get_block_index_entry_size(dst_index);

  if ( 0 == ret && src_size != dst_size)
  {
    fprintf(stderr, "src index size = %ld, dst index size = %ld\n", src_size, dst_size);
    if (!ignore_size) ret = -1;
  }

  ObSSTableBlockIndexV2::const_iterator src_it = src_index.begin();
  ObSSTableBlockIndexV2::const_iterator dst_it = dst_index.begin();

  int64_t i = 0;
  if(0 == ret)
  {
    while (0 == ret && src_it != src_index.end() && dst_it != dst_index.end())
    {
      ObRowkey src_end_key = src_it->rowkey_;
      ObRowkey dst_end_key = dst_it->rowkey_;
      if (src_end_key.compare(dst_end_key) != 0)
      {
        fprintf(stderr, "index %ld not equal, src offset = %ld, "
            "size = %ld, dst offset = %ld, size = %ld\n", i,
            src_it->block_offset_, src_it->block_record_size_,
            dst_it->block_offset_, dst_it->block_record_size_);
        fprintf(stderr, "src key [%s]\n", to_cstring(src_end_key));
        fprintf(stderr, "dst key [%s]\n", to_cstring(dst_end_key));
        ret = -1;
      }
      if(0 == ret)
      {
        ret = compare_block(src_file_name, dst_file_name, src_reader, dst_reader, src_it, dst_it, ignore_size);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr, "block index %ld compare_block failed\n", i);
        }
      }
      if(0 == ret)
      {
        ++src_it; ++dst_it; ++i;
      }
    }
  }


  bool src_has_more = true;
  bool dst_has_more = true;

  if(0 == ret)
  {
    src_has_more = (src_it != src_index.end());
    dst_has_more = (dst_it != dst_index.end());
    while (src_it != src_index.end())
    {
      fprintf(stderr, "src has more index %ld src offset = %ld, size = %ld\n",
          i, src_it->block_offset_, src_it->block_record_size_);
      src_it->rowkey_.dump(TBSYS_LOG_LEVEL_WARN);
      ++src_it;
      ++i;
    }

    while (dst_it != dst_index.end())
    {
      fprintf(stderr, "dst has more index %ld src offset = %ld, size = %ld\n",
          i, dst_it->block_offset_, dst_it->block_record_size_);
      dst_it->rowkey_.dump(TBSYS_LOG_LEVEL_WARN);
      ++dst_it;
      ++i;
    }

    if (src_has_more || dst_has_more) ret = -1;
  }

  return ret;
}

int DumpSSTable::compare_sstable(
    const int64_t src_file_id,
    const int64_t dst_file_id,
    ObSSTableReader &src_reader,
    ObSSTableReader &dst_reader,
    bool ignore_size)
{
  int ret = 0;
  const ObSSTableTrailer & src_trailer = src_reader.get_trailer();
  const ObSSTableTrailer & dst_trailer = dst_reader.get_trailer();
  dump_trailer("src", src_trailer);
  dump_trailer("dst", dst_trailer);
  UNUSED(ignore_size);

  char src_file_name[OB_MAX_FILE_NAME_LENGTH];
  char dst_file_name[OB_MAX_FILE_NAME_LENGTH];
  ObSSTableId src_id(src_file_id);
  ObSSTableId dst_id(dst_file_id);
  get_sstable_path(src_id, src_file_name, OB_MAX_FILE_NAME_LENGTH);
  get_sstable_path(dst_id, dst_file_name, OB_MAX_FILE_NAME_LENGTH);

  ObSSTableBlockIndexV2 *src_index = NULL;
  ObSSTableBlockIndexV2 *dst_index = NULL;
  if(ret == 0)
  {
    ret = get_block_index(src_file_name, src_trailer, src_index);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "get src index object failure\n");
      if (NULL != src_index) { src_index->~ObSSTableBlockIndexV2(); ob_free(src_index);}
      if (NULL != dst_index) { dst_index->~ObSSTableBlockIndexV2(); ob_free(dst_index);}
    }
  }
  if(0 == ret)
  {
    ret = get_block_index(dst_file_name, dst_trailer, dst_index);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "get dst index object failure\n");
      if (NULL != src_index) { src_index->~ObSSTableBlockIndexV2(); ob_free(src_index);}
      if (NULL != dst_index) { dst_index->~ObSSTableBlockIndexV2(); ob_free(dst_index);}
    }
  }
  if(0 == ret)
  {
    ret = compare_block_index(
        src_file_name, dst_file_name,
        src_reader, dst_reader,
        *src_index, *dst_index, true);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "compare src,dst index object failure\n");
      if (NULL != src_index) { src_index->~ObSSTableBlockIndexV2(); ob_free(src_index); src_index = NULL;}
      if (NULL != dst_index) { dst_index->~ObSSTableBlockIndexV2(); ob_free(dst_index); dst_index = NULL;}
    }
  }

  if (NULL != src_index) { src_index->~ObSSTableBlockIndexV2(); ob_free(src_index);}
  if (NULL != dst_index) { dst_index->~ObSSTableBlockIndexV2(); ob_free(dst_index);}
  return ret;
}

DumpSSTable::~DumpSSTable()
{
  if (block_index_ != NULL)
  {
    free((char*)block_index_);
    block_index_ = NULL;
  }
}

int DumpSSTable::open(const int64_t sstable_file_id)
{
  int ret = OB_SUCCESS;
  if (sstable_file_id <= 0)
  {
    fprintf(stderr,"file id %ld <= 0\n", sstable_file_id);
    ret = OB_ERROR;
  }


  ObSSTableId id;
  id.sstable_file_id_ = sstable_file_id;
  id.sstable_file_offset_ = 0;

  char filename[OB_MAX_FILE_NAME_LENGTH];
  get_sstable_path(id, filename, OB_MAX_FILE_NAME_LENGTH);

  if (OB_SUCCESS == ret && ((ret = reader_.open(id, 0)) != OB_SUCCESS))
  {
    fprintf(stderr,"open sstable (%ld,%s) failed (%d)\n", sstable_file_id , filename, ret);
  }
  else
  {
    opened_ = true;
    if ((util_.open(filename,O_RDONLY|O_EXCL)) < 0)
    {
      fprintf(stderr,"open (%s) failed (%d)\n",filename,ret);
      ret = OB_ERROR;
    }
  }

  if ((ret = load_schema()) != OB_SUCCESS)
  {
    fprintf(stderr,"load schema failed\n");
  }
  if ((ret = load_block_index()) != OB_SUCCESS)
  {
    fprintf(stderr,"load block index failed\n");
  }
  return ret;
}

ObSSTableReader& DumpSSTable::get_reader()
{
  return reader_;
}
int DumpSSTable::load_block_index()
{
  int ret = OB_SUCCESS;
  char *block_index_buffer = NULL;
  char* block_index_object_buffer = NULL;
  int64_t block_index_object_bufsiz = 0;
  int64_t block_index_size = 0;
  int64_t pos = 0;
  int64_t nsize = 0;
  const ObSSTableTrailer& trailer = reader_.get_trailer();
  if (!opened_)
  {
    fprintf(stderr,"file not open\n");
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    block_index_size = trailer.get_block_index_record_size();
    util_.lseek(trailer.get_block_index_record_offset(),SEEK_SET);

    block_index_buffer = (char *)malloc(block_index_size);
    if(NULL == block_index_buffer)
    {
      fprintf(stderr,"failed to malloc memory\n");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if ((nsize = util_.read(block_index_buffer, block_index_size)) != block_index_size)
    {
      fprintf(stderr,"read block index header failed: index size=%ld, nsize=%ld\n", block_index_size, nsize);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObSSTableBlockIndexV2 v2;
    block_index_object_bufsiz = v2.get_deserialize_size(block_index_buffer, block_index_size, pos);
    block_index_object_buffer = (char*) malloc(sizeof(ObSSTableBlockIndexV2) + block_index_object_bufsiz);

    const char* base = block_index_object_buffer + sizeof(ObSSTableBlockIndexV2);
    int64_t base_length = block_index_object_bufsiz;
    block_index_ = reinterpret_cast<ObSSTableBlockIndexV2*>(block_index_object_buffer);
    pos = 0;
    ret = block_index_->deserialize(block_index_buffer, block_index_size, pos, base, base_length);
  }



  if (block_index_buffer != NULL)
  {
    free(block_index_buffer);
    block_index_buffer = NULL;
  }

  return ret;
}

int DumpSSTable::read_and_decompress_record(const int16_t magic,const int64_t offset,
    const int64_t zsize,ObRecordHeader& record_header,
    char *& data_buf,int64_t& dsize)
{
  int ret = OB_SUCCESS;
  char *compress_buf = NULL;
  char *r_buf = NULL;
  const char *payload_ptr = NULL;
  int64_t payload_size = 0;
  int64_t data_size = 0;

  compress_buf = (char *) malloc(zsize);

  if(NULL == compress_buf)
  {
    fprintf(stderr,"failed to malloc memory\n");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  util_.lseek(offset,SEEK_SET);

  if(OB_SUCCESS == ret)
  {
    if (util_.read(compress_buf,zsize) != zsize)
    {
      fprintf(stderr,"read block failed.");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = ObRecordHeader::check_record(compress_buf,zsize,
        magic,
        record_header,
        payload_ptr, payload_size);
    if (ret != OB_SUCCESS)
    {
      fprintf(stderr,"check record failed");
    }
  }

  if (OB_SUCCESS == ret)
  {
    r_buf = (char *)malloc(record_header.data_length_);

    if(NULL == r_buf)
    {
      fprintf(stderr,"failed to malloc memory\n");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }

    else
    {
      if (record_header.data_length_ == record_header.data_zlength_)
      {
        memcpy(r_buf,payload_ptr,record_header.data_length_);
        data_size = record_header.data_length_;
      }
      else
      {
        if ( (ret = reader_.get_decompressor()->decompress(payload_ptr,payload_size,
                r_buf,record_header.data_length_,
                data_size)) != OB_SUCCESS )
        {
          fprintf(stderr,"decompress failed\n");
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    dsize = data_size;
    data_buf = r_buf;
  }

  if(compress_buf != NULL)
  {
    free(compress_buf);
    compress_buf = NULL;
  }
  return ret;
}

int DumpSSTable::load_schema()
{
  int ret = OB_SUCCESS;

  char *data = NULL;
  int64_t size = 0;
  ObRecordHeader record_header;

  const ObSSTableTrailer& trailer = reader_.get_trailer();

  if (!opened_)
  {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    ret = read_and_decompress_record(ObSSTableWriter::SCHEMA_MAGIC,
        trailer.get_schema_record_offset(),
        trailer.get_schema_record_size(),
        record_header,data,size);
    if (ret != OB_SUCCESS)
    {
      fprintf(stderr,"read schema failed");
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t pos = 0;
    if ((ret = schema_.deserialize(data,size,pos)) != OB_SUCCESS)
    {
      fprintf(stderr,"deserialize failed");
    }
  }
  return ret;
}

//11-07-28
void DumpSSTable::dump_another_block(int32_t block_id)
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t size = 0;
  int64_t pos = 0;
  ObRecordHeader record_header;
  ObSSTableBlockHeader block_header;
  ObSSTableBlockReader block_reader;
  ObRowkey row_key;
  ObObj current_ids[OB_MAX_COLUMN_NUMBER];
  ObObj current_columns[OB_MAX_COLUMN_NUMBER];
  int64_t column_count = 0;

  int64_t approx_space_usage = 0;
  ObSSTableRow new_row;//add column to new row
  //TODO
  //check block id

  const ObSSTableBlockIndexV2::IndexEntryType *entry = block_index_->begin() + block_id;

  ret = read_and_decompress_record(ObSSTableWriter::DATA_BLOCK_MAGIC,
      entry->block_offset_,
      entry->block_record_size_,
      record_header,data,size);
  if (OB_SUCCESS == ret)
  {
    if ((ret = block_header.deserialize(data,size,pos)) != OB_SUCCESS)
    {
      fprintf(stderr,"deserialize block header failed");
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t entry_size = sizeof(ObSSTableBlockReader::IndexEntryType)
      * (block_header.row_count_ + 1);
    char index_array[entry_size];
    pos = 0;

    ObSSTableBlockReader::BlockDataDesc data_desc(NULL, 1,
        reader_.get_trailer().get_row_value_store_style());
    ObSSTableBlockReader::BlockData block_data(index_array, entry_size, data, size);

    ret = block_reader.deserialize(data_desc, block_data);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr,"deserialize block index failed");
    }
    else
    {
      ObSSTableBlockReader::IndexEntryType* index =
        (ObSSTableBlockReader::IndexEntryType*)index_array;

      uint64_t table_id;
      uint64_t column_group_id_array[OB_MAX_COLUMN_NUMBER];
      int64_t tmp_size = OB_MAX_COLUMN_NUMBER ;
      static int64_t row_count = 0;//count the row
      static int64_t column_group_idx = 0;//count the column group id

      //get the table id and column_group_id_array
      table_id = reader_.get_trailer().get_first_table_id();
      ret = reader_.get_schema()->get_table_column_groups_id(table_id,
          column_group_id_array,tmp_size);
      if (OB_SUCCESS != ret)
      {
        fprintf(stderr, "get column group id failed, table_id=%lu,"
            "tmp_size=%ld, column_group_id=%lu\n",
            table_id, tmp_size, column_group_id_array[0]);
        exit(1);
      }
      //traverse the rows in block (id)
      for(int32_t i = 0; i < block_header.row_count_ && OB_SUCCESS == ret; ++i)
      {
        column_count = OB_MAX_COLUMN_NUMBER;
        //get each row of block for row_key ,current_ids ,current_columns
        ret = block_reader.get_row(reader_.get_trailer().get_row_value_store_style(),
            &index[i], row_key, current_ids,
            current_columns, column_count);
        if (OB_SUCCESS == ret)
        {
          if ( (row_count >0)
              && (row_count % reader_.get_trailer().get_row_count() == 0) )
          {//print if the row count == get_row_count
            fprintf(stderr, "prev_idex=%ld, group_id=%lu, row_count=%ld\n",
                column_group_idx, column_group_id_array[column_group_idx],
                reader_.get_trailer().get_row_count());
            ++column_group_idx;
            //count the column_group id
            //when the column is the last one of this row
            //same column_group_idx in same row
          }

          //set the new row
          new_row.clear();
          new_row.set_table_id(table_id);
          new_row.set_rowkey(row_key);
          new_row.set_column_group_id(column_group_id_array[column_group_idx]);

          //add columns to new row
          for(int j=0; j < column_count; ++j)
          {
            if( OB_SUCCESS != new_row.add_obj(current_columns[j]) )
            {//add each column to new row
              fprintf(stderr,"add column %d to row %d failed!\n",j,i);
            }
          }

          if( OB_SUCCESS != writer_.append_row(new_row, approx_space_usage))
          {//append row to new sstable
            fprintf(stderr,"add  row %d to block %d failed!, column_group_idx=%ld, "
                "group_id=%lu, row_count=%ld\n",i,block_id, column_group_idx,
                column_group_id_array[column_group_idx], row_count);
            exit(1);
          }
          ++row_count;//
        }
        else
        {
          fprintf(stderr,"deserialize row failed \n");
        }
      }
    }
  }

  if (data != NULL)
  {
    free(data);
    data = NULL;
  }
  return;
}

void DumpSSTable::dump_another_sstable(const char * file_dir_,const char *compressor_)
{
  int ret = OB_SUCCESS;
  const ObSSTableTrailer& trailer = reader_.get_trailer();
  int64_t block_count = trailer.get_block_count();
  common::ObString sstable_file_name_;
  char dest_file_[1000];
  char src_compressor_[1000];
  ObString compressor_name_;
  snprintf(dest_file_,sizeof(dest_file_), file_dir_);
  sstable_file_name_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));
  snprintf(src_compressor_,sizeof(src_compressor_),compressor_);
  compressor_name_.assign(src_compressor_,static_cast<int32_t>(strlen(src_compressor_)));
  //create sstable writer
  ret = writer_.create_sstable(*reader_.get_schema(), sstable_file_name_,compressor_name_ ,2);

  if(OB_SUCCESS != ret)
  {
    fprintf(stderr,"create sstable writer failed !\n");
    exit(1);
  }
  //dump all blocks
  for(int32_t i2 = 0 ; i2 < block_count; ++i2)
  {
    dump_another_block(i2);// dump block i2 to new sstable
  }
  int64_t offset = 0 ;
  writer_.close_sstable(offset);// close sstable writer
}


void DumpSSTable::load_block(int32_t block_id)
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t size = 0;
  int64_t internal_bufsiz = 128 * 1024;
  char* internal_buffer = NULL;
  int64_t column_count = 0;
  ObRecordHeader record_header;
  ObSSTableBlockReader block_reader;
  ObRowkey row_key;
  char rowkey_buffer[OB_RANGE_STR_BUFSIZ];
  ObObj current_ids[OB_MAX_COLUMN_NUMBER];
  ObObj current_columns[OB_MAX_COLUMN_NUMBER];

  //TODO
  //check block id

  const ObSSTableBlockIndexV2::IndexEntryType *entry = block_index_->begin() + block_id;

  ret = read_and_decompress_record(ObSSTableWriter::DATA_BLOCK_MAGIC,
      entry->block_offset_,
      entry->block_record_size_,
      record_header,data,size);

  if (OB_SUCCESS == ret)
  {
    internal_buffer = (char*) malloc(internal_bufsiz);
    const ObRowkeyInfo& rowkey_info = g_schema.get_table_schema(entry->table_id_)->get_rowkey_info();
    reader_.get_schema()->get_rowkey_column_count(entry->table_id_, column_count);
    ObSSTableBlockReader::BlockDataDesc data_desc(&rowkey_info, column_count,
        reader_.get_trailer().get_row_value_store_style());
    ObSSTableBlockReader::BlockData block_data(internal_buffer, internal_bufsiz, data, size);

    ret = block_reader.deserialize(data_desc, block_data);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr,"deserialize block index failed");
    }
    else
    {
      const ObSSTableBlockReader::IndexEntryType* index = block_reader.begin();
      fprintf(stderr,"this block has %d rows\n", block_reader.get_row_count());
      int32_t i = 0;
      int64_t column_id = 0;
      while (OB_SUCCESS == ret && index != block_reader.end())
      {
        column_count = OB_MAX_COLUMN_NUMBER;
        ret = block_reader.get_row(reader_.get_trailer().get_row_value_store_style(),
            index, row_key, current_ids, current_columns, column_count);
        if (OB_SUCCESS == ret)
        {
          row_key.to_string(rowkey_buffer, OB_RANGE_STR_BUFSIZ);
          fprintf(stderr,"ROW %d ROW_KEY: %s\n",i, rowkey_buffer);
          const ObSSTableSchemaColumnDef* def = schema_.get_group_schema(entry->table_id_, entry->column_group_id_, size);
          for(int j=0; j < column_count; ++j)
          {
            if (reader_.get_trailer().get_row_value_store_style() == OB_SSTABLE_STORE_SPARSE)
            {
              current_ids[j].get_int(column_id);
              ::print_obj(static_cast<int>(column_id), current_columns[j]);
            }
            else
            {
              if (NULL != def && j < size)
              {
                column_id = def[j].column_name_id_;
              }
              else
              {
                column_id = j;
              }
              ::print_obj(static_cast<int>(column_id), current_columns[j]);
            }
          }
          fprintf(stderr,"ROW %d END\n",i);
        }
        else
        {
          fprintf(stderr,"deserialize row failed");
        }
        ++index;
        ++i;
      }
    }
  }

  if (data != NULL)
  {
    free(data);
    data = NULL;
  }

  if (NULL != internal_buffer)
  {
    free(internal_buffer);
    internal_buffer = NULL;
  }

  return;
}

void DumpSSTable::load_blocks(int32_t block_id,int32_t block_n)
{
  const ObSSTableTrailer& trailer = reader_.get_trailer();
  int64_t block_count = trailer.get_block_count();
  if(block_count < block_id){
    fprintf(stderr,"wrong block_id and block_n , the block_count is %ld!\n",block_count);
    exit(1);
  }
  if((0 <= block_id) && (block_n <= block_count) ){
    for(int32_t i1 = block_id; i1 <= block_n ; i1++){
      fprintf(stderr,"------------block %d--------------------\n",i1);
      load_block(i1);
      fprintf(stderr,"------------block %d--------------------\n",i1);
    }
  }
  if( (-1 == block_id) || (block_n > block_count) ) {
    for(int32_t i2 = 0 ; i2 < block_count; ++i2){
      fprintf(stderr,"------------block %d--------------------\n",i2);
      load_block(i2);
      fprintf(stderr,"------------block %d--------------------\n",i2);
    }
  }

  return;
}

void DumpSSTable::display_trailer_info()
{
  const ObSSTableTrailer& trailer = reader_.get_trailer();
  fprintf(stderr, "size_: %d \n"
      "trailer_version_: %d \n"
      "table_version_: %ld \n"
      "first_block_data_offset_: %ld \n"
      "block_count_: %ld \n"
      "block_index_record_offset_: %ld \n"
      "block_index_record_size_: %ld \n"
      "bloom_filter_hash_count_: %ld \n"
      "bloom_filter_record_offset_: %ld \n"
      "bloom_filter_record_size_: %ld \n"
      "schema_record_offset_: %ld \n"
      "schema_record_size_: %ld \n"
      "block_size_: %ld \n"
      "row_count_: %ld \n"
      "sstable_checksum_: %lu \n"
      "compressor_name_: %s \n"
      "row_value_store_style_: %s \n"
      "frozen_time_: %ld \n"
      "block_index_memory_size: %ld \n" ,
      trailer.get_size(),
      trailer.get_trailer_version(),
      trailer.get_table_version(),
      trailer.get_first_block_data_offset(),
      trailer.get_block_count(),
      trailer.get_block_index_record_offset(),
      trailer.get_block_index_record_size(),
      trailer.get_bloom_filter_hash_count(),
      trailer.get_bloom_filter_record_offset(),
      trailer.get_bloom_filter_record_size(),
      trailer.get_schema_record_offset(),
      trailer.get_schema_record_size(),
      trailer.get_block_size(),
      trailer.get_row_count(),
      trailer.get_sstable_checksum(),
      trailer.get_compressor_name(),
      trailer.get_row_value_store_style() == 1 ? "dense" : "sparse",
      trailer.get_frozen_time(),
      block_index_->get_deserialize_size());

  display_trailer_table_info(trailer);
}

void DumpSSTable::display_bloom_filter()
{
  const ObBloomFilterV1* bfv1 = dynamic_cast<const ObBloomFilterV1*>(reader_.get_bloom_filter());
  if (NULL == bfv1)
  {
    fprintf(stderr, "sstable has no bloom filter\n");
  }
  else
  {
    fprintf(stderr, "ptr=%p, size=%ld\n", bfv1->get_buffer(), bfv1->get_nbyte());
    hex_dump(bfv1->get_buffer(), (int32_t)bfv1->get_nbyte());
  }
}

void DumpSSTable::display_block_index()
{
  const ObSSTableBlockIndexV2::IndexEntryType *entry = block_index_->begin();
  fprintf(stderr,"%8s %8s %8s %8s %8s %8s\n","block#","table","group", "offset","size","key");
  int64_t i = 0;
  while (entry != block_index_->end())
  {
    fprintf(stderr,"%8ld %8lu %8lu %8ld %8ld %s\n",
        i,entry->table_id_,entry->column_group_id_,
        entry->block_offset_,entry->block_record_size_, to_cstring(entry->rowkey_));
    ++entry;
    ++i;
  }
}

void DumpSSTable::display_schema_info()
{
  const ObSSTableSchemaColumnDef *col = NULL;
  fprintf(stderr,"SCHEMA INFO:\n");
  fprintf(stderr,"COLUMN COUNT:%ld\n", reader_.get_schema()->get_column_count());
  for(int32_t i=0;i < reader_.get_schema()->get_column_count(); ++i)
  {
    if ( (col = reader_.get_schema()->get_column_def(i)) != NULL)
    {
      fprintf(stderr,"%8d %8d %8d %8d %8u %8d\n", i,
          col->table_id_, col->column_group_id_, col->rowkey_seq_, col->column_name_id_,col->column_value_type_);
    }
  }
  return;
}

struct CmdLineParam
{
  const char *cmd_type;
  const char *idx_file_name;
  const char *dump_content;
  int32_t block_id;
  int32_t block_n;//if not 0 , from block_id to block_n
  int32_t disk_no;
  int64_t table_version;
  const char *search_range;
  const char *new_range;
  int64_t table_id;
  const char *app_name;
  int32_t hex_format;
  int32_t quiet;
  int64_t file_id;
  int64_t dst_file_id;
  const char *compressor_name;
  const char *output_sst_dir;
  const char *schema_file;
};


void usage()
{
  printf("\n");
  printf("Usage: sstable_tools [OPTION]\n");
  printf("   -c| --cmd_type command type [dump_sstable|cmp_sstable|dump_meta|search_meta] \n");
  printf("   -D| --sstable_directory sstable directory \n");
  printf("   -I| --idx_file_name must be set while cmd_type is dump_meta\n");
  printf("   -f| --file_id sstable file id\n");
  printf("   -p| --dst_file_id must be set while cmp_type is cmp_sstable\n");
  printf("   -d| --dump_content [dump_index|dump_schema|dump_trailer] could be set while cmd_type is dump_sstable\n");
  printf("   -b| --block_id show rows number in block,could be set  while cmd_type is dump_sstable and dump_content is dump_trailer\n");
  printf("   -i| --tablet_version must be set while cmp_type is dump_meta \n");
  printf("   -r| --search_range must be set while cmp_type is search _meta\n");
  printf("   -t| --table_id must be set while cmp_type is search _meta\n");
  printf("   -a| --app_name must be set while cmp_type is search _meta\n");
  printf("   -x| --hex_format (0 plain string/1 hex string/2 number)\n");
  printf("   -h| --help print this help info\n");
  printf("   samples:\n");
  printf("   -c dump_meta -P idx_file_path [-i disk_no -v data_version -x]\n");
  printf("   -c dump_sstable -d dump_index -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_schema -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_trailer -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_block -P sstable_file_path -b block_id\n");
  printf("   -c search_meta -t table_id -r search_range -a app_name -x hex_format -D sstable_directory \n");
  printf("   -c change_meta -t table_id -r search_range -d action"
      "(remove_range/set_merged_flag/clear_merged_flag/find_tablet/change_range) "
      "-x hex_format -D sstable_directory [-i disk_no -v data_version -f dump_sstable -n new_range]\n");
  printf("\n");
  exit(0);
}

bool is_all_num(char  *str)
{
  int str_len = 0;
  int dot_flag = 0;
  while(*str != '\0')
  {
    str_len++;
    if(*str > '9')
      return false;
    else if(*str < '0')
    {
      if(*str == '+' || *str == '-')
      {
        str++;
        dot_flag++;
        if( 1 < dot_flag )
        {
          fprintf(stderr,"wrong input the  + or - \n");
          exit(1);
        }else
          continue;
      }else
        return false;
    }
    if( dot_flag == 0 )
    {
      dot_flag++;
    }
    str++;
  }
  return true;
}

bool safe_char_int32(char *src_str , int32_t &dst_num)
{
  bool ret = true;
  char *tmp_str = src_str;
  char *str = NULL;
  int str_len = 0;
  int dot_num = 0;
  int64_t src_num_64;
  int64_t src_num_32;
  while(*tmp_str != '\0')
  {
    if(*tmp_str > '9')
    {
      fprintf(stderr,"error int : invalid char\n");
      ret = false;
      break;
    }
    else if(*tmp_str < '0')
    {
      if(*tmp_str == ' ')
      {
        if(str_len == 0)
        {
          tmp_str++;
          continue;
        }else
        {
          fprintf(stderr,"error int : invalid space \n");
          ret = false;
          break;
        }
      }
      else if(*tmp_str == '+' || *tmp_str == '-')
      {
        dot_num++;
        if( 1 < dot_num )
        {
          fprintf(stderr,"error int : invalid + or -\n");
          ret = false;
          break;
        }
        else
        {
          str = tmp_str;
        }
      }else
      {
        fprintf(stderr,"error int : invalid char\n");
        ret = false;
        break;
      }
    }
    else
    {
      if (*tmp_str == '0' && str_len == 0)
      {
        if(*(tmp_str+1) != '\0')
        {
          tmp_str++;
          continue;
        }else
        {
          str = tmp_str;
        }
      }else if(dot_num == 0)
      {
        dot_num++;
        str = tmp_str;
      }
    }

    str_len++;
    tmp_str++;
  }//end of while

  if(str_len > 10 )
  {
    ret = false;
    fprintf(stderr,"error int : invalid length \n");
  }
  if( ret )
  {
    src_num_64 = atoll(str);
    src_num_32 = atoi(str);
    if(src_num_32 != src_num_64)
    {
      fprintf(stderr,"error int : overflow or underflow\n");
      ret = false;
    }
    /*
       if(src_num > 0xffffffff)
       {
       fprintf(stderr,"error int : overflow \n");
       return false;
       }else if (src_num < 0xffffff+1)
       {
       fprintf(stderr,"error int : underflow \n");
       return false;
       }
       dst_num = src_num & 0xffffffff;
       */
    dst_num = static_cast<int32_t>(src_num_32);
  }
  return ret;
}

void deal_cmd_line(char * optarg_tmp , CmdLineParam &clp)
{
  const char *split_ = ",[]()\0";
  char * tmp;
  int32_t tmp_num;
  int split_count = 0;
  bool left_paren = false;
  bool right_paren = false;
  char *b_opt_arg;
  int paren_match = 0;
  int  paren_exist = 0;//if exist paren
  char *ptr;
  char *ptr2;

  b_opt_arg = optarg_tmp;

  //find the dot
  ptr = strchr(b_opt_arg,',');
  ptr2 = strrchr(b_opt_arg,',');
  if ( NULL != ptr && NULL != ptr2 )
  {
    if ( ptr == b_opt_arg || *(ptr2+1) == '\0' )
    {
      fprintf(stderr,"invalid input of , \n");
      exit(1);
    }
    if ( ptr != ptr2 )
    {
      fprintf(stderr,"multi input of , \n");
      exit(1);
    }
  }

  //match the paren
  ptr = strchr(b_opt_arg,'(');
  if(ptr)
  {
    left_paren = true;
    paren_exist++;
    paren_match++;
  }

  ptr2 = strrchr(b_opt_arg,'(');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi left parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,')');
  if(ptr)
  {
    right_paren = true;
    paren_exist++;
    paren_match--;
  }
  ptr2 = strrchr(b_opt_arg,')');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi right parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,'[');
  if(ptr)
  {
    paren_match++;
    paren_exist++;
  }
  ptr2 = strrchr(b_opt_arg,'[');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi left parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,']');
  if(ptr)
  {
    paren_match--;
    paren_exist++;
  }
  ptr2 = strrchr(b_opt_arg,']');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi right parens \n");
    exit(1);
  }

  //

  if ( paren_exist % 2 != 0 )
  {
    fprintf(stderr,"wrong num of parens ! \n");
    exit(1);
  }
  if ( 0 != paren_match )
  {
    fprintf(stderr,"wrong param , paren mismatch !\n");
    exit(1);
  }else if((0 == paren_match)
      && (2 != paren_exist && 0 != paren_exist))
  {
    fprintf(stderr,"wrong paren , only allow two parens !\n");
    exit(1);
  }

  tmp = strtok(b_opt_arg,split_);
  while( tmp!=NULL )
  {
    //check if all is num
    if( !safe_char_int32(tmp, tmp_num) )// !is_all_num(tmp)
    {
      fprintf(stderr,"wrong param , not all is number !\n");
      exit(1);
    }

    if( 0 == split_count )
    {
      clp.block_id = tmp_num ;//atoi(tmp);
      clp.block_n = clp.block_id;
      split_count++;
    }
    else if( 1 == split_count )
    {
      if( clp.block_id  <=  tmp_num )//( clp.block_id <= atoi(tmp) )
        clp.block_n = tmp_num; //clp.block_n = atoi(tmp);
      else
      {//swap that block_n should >= block_id
        clp.block_n = clp.block_id;
        clp.block_id = tmp_num; //clp.block_id = atoi(tmp);
      }
      split_count++;
    }

    if( 2 < split_count )
    {
      //more than 2 param is wrong
      fprintf(stderr,"wrong param , more than 2 params !\n");
      exit(1);
    }

    tmp = strtok(NULL,split_);
    tmp_num = 0 ;
  }//end of while (tmp!=NULL)

  if(1 == split_count && paren_exist)
  {
    fprintf(stderr,"wrong param , miss one param in 2 parens!\n");
    // (id , ]
    exit(1);
  }

  if( 2 == split_count && 2 > paren_exist)
  {
    fprintf(stderr,"wrong param , miss parens !\n");
    exit(1);
  }
  //deal the params

  if( (-1 == clp.block_id) && (-1 == clp.block_n) )//get all blocks
    return ;

  if( (-1 >= clp.block_id) || (-1 >= clp.block_n) ){
    fprintf(stderr,"wrong param , input error !\n");
    exit(1);
  }

  if( (0 <= clp.block_id) && (clp.block_n == clp.block_id)
      && (!left_paren && !right_paren))
    return ;
  else
  {
    //deal the parens
    if(left_paren)
      clp.block_id = clp.block_id + 1;
    if(right_paren)
      clp.block_n = clp.block_n -1;
  }

  if( clp.block_id > clp.block_n )
  {
    fprintf(stderr,"wrong param , input error2 !\n");
    exit(1);
  }
}

void parse_cmd_line(int argc,char **argv,CmdLineParam &clp)
{
  int opt = 0;
  const char* opt_string = "c:f:p:d:b:i:r:t:a:I:D:hx:v:qo:s:n:P:S:";
  const char* file_path = NULL;

  struct option longopts[] =
  {
    {"help",0,NULL,'h'},
    {"cmd_type",1,NULL,'c'},
    {"idx_file_name",1,NULL,'I'},
    {"file_id",1,NULL,'f'},
    {"dst_file_id",1,NULL,'p'},
    {"sstable_directory",1,NULL,'D'},
    {"dump_content",1,NULL,'d'},
    {"block_id",1,NULL,'b'},
    {"tablet_version",1,NULL,'v'},
    {"disk_no",1,NULL,'i'},
    {"search_range",1,NULL,'r'},
    {"new_range",1,NULL,'n'},
    {"table_id",1,NULL,'t'},
    {"app_name",1,NULL,'a'},
    {"hex_format",0,NULL,'x'},
    {"quiet",0,NULL,'q'},
    {"output_sstable_directory",1,NULL,'o'},
    {"compressor_name",1,NULL,'s'},
    {"file_path",1,NULL,'P'},
    {"schema",1,NULL,'S'},
    {0,0,0,0}
  };

  memset(&clp,0,sizeof(clp));
  //init clp
  clp.block_id = -1;
  while((opt = getopt_long(argc, argv, opt_string, longopts,NULL)) != -1)
  {
    switch (opt)
    {
      case 'h':
        usage();
        break;
      case 'c':
        clp.cmd_type = optarg;
        break;
      case 'I':
        clp.idx_file_name = optarg;
        break;
      case 'f':
        clp.file_id = strtoll(optarg, NULL, 10);
        break;
      case 'p':
        clp.dst_file_id = strtoll(optarg, NULL, 10);
        break;
      case 'P':
        file_path = optarg;
        break;
      case 'D':
        g_sstable_directory = optarg;
        break;
      case 'd':
        clp.dump_content = optarg;
        break;
      case 'b':
        {
          deal_cmd_line(optarg , clp);
        }
        break;
      case 'v':
        clp.table_version = atoi(optarg);
        break;
      case 'i':
        clp.disk_no = atoi(optarg);
        break;
      case 'r':
        clp.search_range = optarg;
        break;
      case 'n':
        clp.new_range = optarg;
        break;
      case 't':
        clp.table_id = strtoll(optarg, NULL, 10);
        break;
      case 'a':
        clp.app_name = optarg;
        break;
      case 'x':
        clp.hex_format = static_cast<int32_t>(strtoll(optarg, NULL, 10));
        break;
      case 'q':
        clp.quiet = 1;
        break;
      case 'o':
        {
          clp.output_sst_dir = optarg;
          //snprintf(clp.output_sst_dir,sizeof(clp.output_sst_dir),"%s",optarg);
          fprintf(stderr,"out_put_sst_dir = %s\n",clp.output_sst_dir);
        }
        break;
      case 's':
        {
          clp.compressor_name = optarg;
          //snprintf(clp.compressor_name,sizeof(clp.compressor_name),"%s",optarg);
          fprintf(stderr,"compressor_name = %s\n",clp.compressor_name);
        }
        break;
      case 'S':
        clp.schema_file = optarg;
        break;
      default:
        usage();
        exit(1);
    }
  }

  if (NULL == clp.cmd_type)
  {
    fprintf(stderr, "cmd_type not set.\n");
    usage();
    exit(-1);
  }

  if(0 <= clp.block_id
      && (0 != strcmp("dump_block",clp.dump_content)
        || NULL == clp.dump_content))
  {
    fprintf(stderr,"-i should appear after -d and the -d must be dump trailer\n");
    usage();
    exit(-1);
  }

  if(0 == strcmp("dump_sstable",clp.cmd_type)
      && NULL == clp.dump_content )
  {
    fprintf(stderr,"-d must be appear when the cmd_type is dump_sstable\n");
    usage();
    exit(-1);
  }



  char * fp = new char[OB_MAX_FILE_NAME_LENGTH];
  memset(fp, 0, OB_MAX_FILE_NAME_LENGTH);
  const char * directory = NULL;
  const char * name = NULL;
  if (NULL != file_path)
  {
    strcpy(fp, file_path);
    char * p = strrchr(fp, '/');
    if (NULL != p)
    {
      *p = 0;
      directory = fp;
      name = p + 1;
    }
  }

  if(0 == strcmp("dump_sstable",clp.cmd_type))
  {
    if (NULL != name) clp.file_id = strtoll(name, NULL, 10);
    if (NULL != directory) g_sstable_directory = directory;
  }

  if(0 == strcmp("dump_meta", clp.cmd_type))
  {
    if (NULL != name) clp.idx_file_name = name;
    if (NULL != directory) g_sstable_directory = directory;
  }

  if (NULL == g_sstable_directory && NULL == file_path)
  {
    fprintf(stderr, "sstable directory not set");
    usage();
    exit(-1);
  }

  if(NULL == clp.cmd_type
      || (NULL != clp.dump_content && (0 != strcmp("dump_sstable", clp.cmd_type) && 0 != strcmp("change_meta", clp.cmd_type)))
      || (0<=clp.block_id && NULL == clp.dump_content)
      || (0 <= clp.block_id && 0 != strcmp("dump_sstable",clp.cmd_type))
      || (0 == clp.idx_file_name && 0 == strcmp("dump_meta",clp.cmd_type))
      || (NULL == clp.search_range && 0 == strcmp("search_meta",clp.cmd_type))
      || (0 >= clp.table_id && 0 == strcmp("search_meta",clp.cmd_type))
      || (NULL == clp.app_name && 0 == strcmp("search_meta",clp.cmd_type))
      || (0 == clp.dst_file_id && 0 == strcmp("cmp_sstable",clp.cmd_type)))
  {
    usage();
    exit(-1);
  }
}

int main(const int argc, char **argv)
{
  int ret = 0;
  // char* sst_file = NULL;
  // ObSSTableReader reader;
  CmdLineParam clp;
  parse_cmd_line(argc,argv,clp);
  if (clp.quiet) TBSYS_LOGGER.setLogLevel("ERROR");

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  tbsys::CConfig config;

  if (NULL != clp.schema_file)
  {
    g_schema.parse_from_file(clp.schema_file, config);
    g_sm.init(false, g_schema);
    set_global_sstable_schema_manager(&g_sm);
  }

  //if dump_sstable
  if(0 == strcmp("dump_sstable",clp.cmd_type))
  {
    DumpSSTable dump;
    ret = dump.open(clp.file_id);
    if (OB_SUCCESS!= ret)
    {
      fprintf(stderr, "failed to open sstable file='%ld'\n",clp.file_id);
      exit(1);
    }
    //dump sstable index
    if (0 == strcmp("dump_index",clp.dump_content))
    {
      dump.display_block_index();
    }
    if(0 == strcmp("dump_schema",clp.dump_content))
    {
      dump.display_schema_info();
    }
    if(0 == strcmp("dump_trailer",clp.dump_content))
    {
      dump.display_trailer_info();
    }
    if(0 == strcmp("dump_bloom_filter",clp.dump_content))
    {
      dump.display_bloom_filter();
    }
    if(0 == strcmp("dump_block",clp.dump_content))
    {
      if(0 <= clp.block_id)
      {
        if( clp.block_id == clp.block_n )
          dump.load_block(clp.block_id);
        else
          dump.load_blocks(clp.block_id,clp.block_n);//
      }//end of if(0<= clp.block_id )
      else if( (-1 == clp.block_id) && (clp.block_id == clp.block_n) )
      {
        dump.load_blocks(clp.block_id,clp.block_n);//print all
      }
    }
  }

  if(0 == strcmp("dump_sstable_new_compressor",clp.cmd_type))
  {
    DumpSSTable dump;
    ret = dump.open(clp.file_id);
    fprintf(stderr,"dump sstable with new compressor \n");
    dump.dump_another_sstable(clp.output_sst_dir,clp.compressor_name);
  }
  //if dump_meta
  if(0 == strcmp("dump_meta",clp.cmd_type))
  {
    dump_new_index(clp.idx_file_name, clp.table_version, clp.disk_no, clp.hex_format > 0 ? true : false);
  }
  if(0 == strcmp("change_meta",clp.cmd_type))
  {
    change_new_index(clp.idx_file_name, clp.table_version, clp.disk_no, clp.dump_content,
        clp.table_id, clp.search_range, clp.new_range, clp.hex_format, clp.file_id);
  }

  //if search_meta
  if(0 == strcmp("search_meta",clp.cmd_type))
  {
    search_index(g_sstable_directory,clp.app_name,clp.table_id,clp.search_range,clp.hex_format);
  }
  //if cmp_sstable
  if(0 == strcmp("cmp_sstable",clp.cmd_type))
  {
    DumpSSTable src_dump,dst_dump;
    // ObSSTableReader *src_reader = &dump.get_reader();
    // ObSSTableReader *dst_reader = &dst_dump.get_reader();
    ret = src_dump.open(clp.file_id);
    if(OB_SUCCESS != ret)
    {
      fprintf(stderr,"failed to open src sstable file='%ld'\n",clp.file_id);
      exit(1);
    }
    ret = dst_dump.open(clp.dst_file_id);
    if (OB_SUCCESS!= ret)
    {
      fprintf(stderr, "failed to open dest sstable file='%ld'\n",clp.dst_file_id);
      exit(1);
    }
    DumpSSTable::compare_sstable(clp.file_id,clp.dst_file_id,src_dump.get_reader(),dst_dump.get_reader(),true);
  }
  return 0;
}
