/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         convert_idx_file.cpp is for what ...
 *
 *  Version: $Id: convert_idx_file.cpp 11/21/2012 10:25:18 AM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <tbsys.h>
#include <dirent.h>
#include <string.h>
#include "common/page_arena.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "common/ob_record_header.h"
#include "common/ob_rowkey_helper.h"
#include "common/file_directory_utils.h"
#include "chunkserver/ob_tablet_image.h"

using namespace oceanbase;
using namespace common;
using namespace chunkserver;

class ObTabletImageMod : public ObTabletImage
{
  public:
    int deserialize_with_schema(const ObSchemaManagerV2& schema,
        const int32_t disk_no, const char* buf, const int64_t data_len, int64_t& pos);
    int read_with_schema(const ObSchemaManagerV2& schema, const char* idx_path, const int32_t disk_no);
  private:
    static int binary2rowkey( const ObSchemaManagerV2 &schema,
        const ObTabletRangeInfo& info, ObTabletRangeInfo& rkinfo,
        const char* binary_stream, const int64_t binary_length,
        char* rowkey_stream, int64_t rowkey_length);
    CharArena  arena_;
    ObTablet* alloc_empty_tablet();
};

ObTablet* ObTabletImageMod::alloc_empty_tablet()
{
  ObTablet* tablet = NULL;
  ObNewRange *range = NULL;
  char* ptr = arena_.alloc(sizeof(ObTablet));
  char* rp =  arena_.alloc(sizeof(ObNewRange));
  int64_t max_rowkey_obj_arr_len = sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER ;
  if (NULL != ptr)
  {
    tablet = new (ptr) ObTablet(this);
    range = new (rp) ObNewRange;
    ptr = arena_.alloc(max_rowkey_obj_arr_len * 2);
    if (NULL != ptr)
    {
      range->start_key_.assign(reinterpret_cast<ObObj*>(ptr), OB_MAX_ROWKEY_COLUMN_NUMBER);
      range->end_key_.assign(reinterpret_cast<ObObj*>(ptr + max_rowkey_obj_arr_len), OB_MAX_ROWKEY_COLUMN_NUMBER);
      tablet->set_range(*range);
    }
  }
  return tablet;
}

int ObTabletImageMod::binary2rowkey( const ObSchemaManagerV2 &schema,
    const ObTabletRangeInfo& info, ObTabletRangeInfo& rkinfo,
    const char* binary_stream, const int64_t binary_length,
    char* rowkey_stream, int64_t rowkey_length)
{
  ObString binary_start_key(0, info.start_key_size_, binary_stream);
  ObString binary_end_key(0, info.end_key_size_, binary_stream + info.start_key_size_);
  ObObj    rowkey_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey rowkey;
  int64_t  pos = 0;
  int      ret = OB_SUCCESS;

  ObBorderFlag border_flag;
  border_flag.set_data(info.border_flag_);
  rkinfo = info;
  const ObRowkeyInfo &rowkey_info = schema.get_table_schema(info.table_id_)->get_rowkey_info();


  if (info.start_key_size_ + info.end_key_size_ > binary_length)
  {
    fprintf(stderr, "sk size =%d + end size=%d > binary_length=%ld\n",
        info.start_key_size_, info.end_key_size_, binary_length);
    ret = OB_SIZE_OVERFLOW;
  }

  if (OB_SUCCESS == ret)
  {
    rowkey.assign(rowkey_array, rowkey_info.get_size());
    if (border_flag.is_min_value())
    {
      if (info.start_key_size_ != 0)
      {
        fprintf(stderr, "min value, but start key:%d not equal 0\n", info.start_key_size_);
      }
      rowkey.set_min_row();
      border_flag.unset_min_value();
      rkinfo.border_flag_ = border_flag.get_data();
    }
    else if  ( OB_SUCCESS != (ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
            rowkey_info, binary_start_key, rowkey_array, OB_MAX_ROWKEY_COLUMN_NUMBER)))
    {
      fprintf(stderr, "binary_start_key convert failed, info=%s\n", to_cstring(rowkey_info));
    }

    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = rowkey.serialize_objs(rowkey_stream, rowkey_length, pos)))
    {
      fprintf(stderr, "serialize_objs start key convert failed, info=%s\n", to_cstring(rowkey_info));
    }
    else
    {
      rkinfo.start_key_size_ = static_cast<int16_t>(pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    rowkey.assign(rowkey_array, rowkey_info.get_size());
    if (border_flag.is_max_value())
    {
      if (info.end_key_size_ != 0)
      {
        fprintf(stderr, "min value, but end key:%d not equal 0\n", info.end_key_size_);
      }
      rowkey.set_max_row();
      border_flag.unset_max_value();
      rkinfo.border_flag_ = border_flag.get_data();
    }
    else if (OB_SUCCESS != (ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
            rowkey_info, binary_end_key, rowkey_array, OB_MAX_ROWKEY_COLUMN_NUMBER)))
    {
      fprintf(stderr, "binary_end_key convert failed, info=%s\n", to_cstring(rowkey_info));
    }

    if (OB_SUCCESS == ret && OB_SUCCESS != (rowkey.serialize_objs(rowkey_stream, rowkey_length, pos)))
    {
      fprintf(stderr, "serialize_objs end key convert failed, info=%s\n", to_cstring(rowkey_info));
    }
    else
    {
      rkinfo.end_key_size_ = static_cast<int16_t>(pos - rkinfo.start_key_size_);
    }
  }

  fprintf(stderr, "pos=%ld, rowkey=%s\n", pos, to_cstring(rowkey));
  return ret;
}

int ObTabletImageMod::deserialize_with_schema(const ObSchemaManagerV2& schema,
    const int32_t disk_no, const char* buf, const int64_t data_len, int64_t& pos)
{
  UNUSED(disk_no);

  int ret = OB_SUCCESS;
  int64_t origin_payload_pos = 0;
  const int64_t rowkey_max_size = OB_MAX_FILE_NAME_LENGTH;
  char rowkey_buffer[rowkey_max_size];

  ObTabletMetaHeader meta_header;
  if (OB_SUCCESS != ( ret = ObRecordHeader::nonstd_check_record(
          buf + pos, data_len, ObTabletMetaHeader::TABLET_META_MAGIC)))
  {
    fprintf(stderr, "check common record header failed, disk_no=%d", disk_no);
  }
  else
  {
    pos += OB_RECORD_HEADER_LENGTH;
    origin_payload_pos = pos;
    if (OB_SUCCESS != (ret = meta_header.deserialize(buf, data_len, pos)))
    {
      fprintf(stderr, "deserialize meta header failed, disk_no=%d", disk_no);
    }
    else if (get_data_version() != meta_header.data_version_)
    {
      fprintf(stderr, "data_version_=%ld != header.version=%ld",
          get_data_version(), meta_header.data_version_);
      ret = OB_ERROR;
    }
  }


  // check the rowkey char stream
  char* row_key_buf = NULL;
  if (OB_SUCCESS == ret)
  {
    if (origin_payload_pos
        + meta_header.row_key_stream_offset_
        + meta_header.row_key_stream_size_
        > data_len)
    {
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      const char* row_key_stream_ptr = buf + origin_payload_pos
        + meta_header.row_key_stream_offset_;
      row_key_buf = arena_.alloc(
          meta_header.row_key_stream_size_);
      if (NULL == row_key_buf)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memcpy(row_key_buf, row_key_stream_ptr,
            meta_header.row_key_stream_size_);
      }
    }
  }

  const char* tablet_extend_buf = NULL;
  if (OB_SUCCESS == ret
      && meta_header.tablet_extend_info_offset_ > 0
      && meta_header.tablet_extend_info_size_ > 0)
  {
    if (origin_payload_pos
        + meta_header.tablet_extend_info_offset_
        + meta_header.tablet_extend_info_size_
        > data_len)
    {
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      tablet_extend_buf =  buf + origin_payload_pos
        + meta_header.tablet_extend_info_offset_;
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t row_key_cur_offset = 0;
    int64_t tablet_extend_cur_pos = 0;
    ObTabletExtendInfo extend_info;
    ObTablet * tablet = NULL;
    for (int64_t i = 0; i < meta_header.tablet_count_ && OB_SUCCESS == ret; ++i)
    {
      ObTabletRangeInfo info, rkinfo;
      if (OB_SUCCESS != (ret = info.deserialize(buf, data_len, pos)))
      {
        fprintf(stderr, "deserialize info error len=%ld, pos=%ld\n", data_len, pos);
      }
      else if (NULL == (tablet = alloc_empty_tablet()))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = tablet->deserialize(buf, data_len, pos)))
      {
        fprintf(stderr, "deserialize tablet err len=%ld, pos=%ld\n", data_len, pos);
      }
      else if (OB_SUCCESS != (ret = binary2rowkey(
              schema, info, rkinfo,
              row_key_buf + row_key_cur_offset,
              meta_header.row_key_stream_size_ - row_key_cur_offset,
              rowkey_buffer, rowkey_max_size)))
      {
        fprintf(stderr, "binary2rowkey error, cur offset=%ld, size=%ld\n",
            row_key_cur_offset, meta_header.row_key_stream_size_ - row_key_cur_offset);
      }
      else if (OB_SUCCESS != (ret = tablet->set_range_by_info(rkinfo, rowkey_buffer, rowkey_max_size)))
      {
        fprintf(stderr, "set_range_by_info error, cur offset=%ld, size=%ld\n",
            row_key_cur_offset, meta_header.row_key_stream_size_ - row_key_cur_offset);
      }
      else
      {
        row_key_cur_offset += info.start_key_size_ + info.end_key_size_;
      }


      // set extend info if extend info exist
      if (OB_SUCCESS == ret && NULL != tablet_extend_buf
          && OB_SUCCESS == (ret = extend_info.deserialize(tablet_extend_buf,
              meta_header.tablet_extend_info_size_, tablet_extend_cur_pos)))
      {
        tablet->set_extend_info(extend_info);
      }

      if (OB_SUCCESS == ret)
      {
        tablet->set_disk_no(disk_no);
        ret = add_tablet(tablet);
      }

      if (OB_SUCCESS != ret) break;
    }
  }

  return ret;
}

int ObTabletImageMod::read_with_schema(const ObSchemaManagerV2& schema, const char* idx_path, const int32_t disk_no)
{
  int ret = OB_SUCCESS;
  if (NULL == idx_path || strlen(idx_path) == 0)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!FileDirectoryUtils::exists(idx_path))
  {
    TBSYS_LOG(INFO, "meta index file path=%s, disk_no=%d not exist", idx_path, disk_no);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    TBSYS_LOG(INFO, "read meta index file path=%s, disk_no=%d", idx_path, disk_no);
  }

  if (OB_SUCCESS == ret)
  {
    ObString fname(0, static_cast<int32_t>(strlen(idx_path)), const_cast<char*>(idx_path));

    char* file_buf = NULL;
    int64_t file_size = get_file_size(idx_path);
    int64_t read_size = 0;

    if (file_size < static_cast<int64_t>(sizeof(ObTabletMetaHeader)))
    {
      TBSYS_LOG(INFO, "invalid idx file =%s file_size=%ld", idx_path, file_size);
      ret = OB_ERROR;
    }

    // not use direct io
    FileComponent::BufferFileReader reader;
    if (OB_SUCCESS != (ret = reader.open(fname)))
    {
      TBSYS_LOG(ERROR, "open %s for read error, %s.", idx_path, strerror(errno));
    }

    if (OB_SUCCESS == ret)
    {
      file_buf = static_cast<char*>(ob_malloc(file_size, ObModIds::TEST));
      if (NULL == file_buf)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = reader.pread(file_buf, file_size, 0, read_size);
      if (ret != OB_SUCCESS || read_size < file_size)
      {
        TBSYS_LOG(ERROR, "read idx file = %s , ret = %d, read_size = %ld, file_size = %ld, %s.",
            idx_path, ret, read_size, file_size, strerror(errno));
        ret = OB_IO_ERROR;
      }
      else
      {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCCESS == ret)
    {
      int64_t pos = 0;
      ret = deserialize_with_schema(schema, disk_no, file_buf, file_size, pos);
    }

    if (NULL != file_buf)
    {
      ob_free(file_buf);
      file_buf = NULL;
    }

  }
  return ret;
}

void usage()
{
  fprintf(stderr, "convert_idx_path -s schema_file -i idx_path -o dst_file \n");
}

void parse_path(const char* file_path, const char* &directory, const char* &name)
{
  char * fp = new char[OB_MAX_FILE_NAME_LENGTH];
  memset(fp, 0, OB_MAX_FILE_NAME_LENGTH);
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
    else
    {
      directory = NULL;
      name = fp;
    }
  }
}

const char *g_sstable_directory = NULL;
int main(int argc, char** argv)
{
  const char * schema_file = 0;
  const char * idx_path = 0;
  const char * dst_file = 0;
  int32_t quite = 0;
  int32_t opt = 0;
  int32_t dump = 0;
  int64_t version = 0;
  int32_t disk_no = 0;
  int64_t parse_version = 0;
  int32_t parse_disk_no = 0;

  const char* opt_string = "s:i:v:d:qpw:o:";

  struct option longopts[] =
  {
    {"help",0,NULL,'h'},
    {"quite",0,NULL,'q'},
    {"dump",0,NULL,'p'},
    {"version",1,NULL,'v'},
    {"disk",1,NULL,'d'},
    {"schema",1,NULL,'s'},
    {"idx_path_name",1,NULL,'i'},
    {"output_file_name",1,NULL,'o'},
    {0,0,0,0}
  };

  while((opt = getopt_long(argc, argv, opt_string, longopts,NULL)) != -1)
  {
    switch (opt)
    {
      case 'h':
        usage();
        break;
      case 'i':
        idx_path = optarg;
        break;
      case 's':
        schema_file = optarg;
        break;
      case 'q':
        quite = 1;
        break;
      case 'p':
        dump = 1;
        break;
      case 'v':
        version = atoi(optarg);
        break;
      case 'd':
        disk_no = atoi(optarg);
        break;
      case 'o':
        dst_file = optarg;
        break;
      default:
        usage();
        exit(1);
    }
  }

  if (schema_file == NULL || idx_path == NULL )
  {
    fprintf(stderr, "schema_file = %s, idx_path = %s\n", schema_file, idx_path);
    usage();
    exit(1);
  }

  if (quite) TBSYS_LOGGER.setLogLevel("ERROR");

  ob_init_memory_pool();
  ObSchemaManagerV2 schema;
  tbsys::CConfig config;
  bool ok = schema.parse_from_file(schema_file, config);
  if (!ok)
  {
    fprintf(stderr, "parse_from_file %s error.\n", schema_file);
    exit(1);
  }

  const char* directory = NULL;
  const char* name = NULL;
  parse_path(idx_path, directory, name);
  int num = 0;

  if (NULL != name)
  {
    num = sscanf(name, "idx_%ld_%d", &parse_version, &parse_disk_no);
  }
  else
  {
    num = sscanf(idx_path, "idx_%ld_%d", &parse_version, &parse_disk_no);
  }


  if (num == 2)
  {
    version = parse_version ;
    disk_no = parse_disk_no ;
  }
  else
  {
    fprintf(stderr, "name %s not regular idx file name, use command line:v:%ld,d:%d\n",
        name, version, disk_no);
  }

  ObTabletImageMod image;
  image.set_data_version(version);
  int ret = image.read_with_schema(schema, idx_path, disk_no);
  if (!ret && dump)
  {
    image.dump();
  }
  if (!ret && dst_file)
  {
    directory = name = NULL;
    parse_path(dst_file, directory, name);
    g_sstable_directory = (NULL != directory) ? directory : "./";
    ret = image.write(dst_file, disk_no);
  }
  return ret;
}
