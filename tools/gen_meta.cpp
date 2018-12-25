/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         gen_meta.cpp is for what ...
 *
 *  Version: $Id: gen_meta.cpp 2010年10月30日 14时03分31秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <getopt.h>
#include "common/ob_define.h"
#include "common_func.h"
#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_tablet_image.h"


using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;

const char *g_sstable_directory = NULL;

struct CmdLineParam
{
  const char* range_file;
  int64_t version;
  int hex_format;
};


int sstable_file_filter(const struct dirent *d)
{
  int ret = 0;
  if (NULL != d)
  {
    int64_t id = strtoll(d->d_name, NULL, 10);
    if (id > 0) 
    {
      fprintf(stderr, "got sstable file %s\n", d->d_name);
      ret = 1;
    }
  }
  return ret;
}

int add_tablet(ObTabletImage& image, const ObNewRange &range, 
    const int64_t version, const int32_t disk_no)
{
  ObTablet* tablet;
  int ret = image.alloc_tablet_object(range, tablet);
  if (OB_SUCCESS != ret) return ret;

  tablet->set_data_version(version);
  tablet->set_disk_no(disk_no);

  ret = image.add_tablet(tablet);
  return ret;
}


int scan_file(const CmdLineParam& cmd_line_param)
{
  char* line = NULL;
  size_t len = 0;
  ssize_t read = 0;

  ObTabletImage image;
  char range_str[256];
  ObNewRange range;

  int64_t table_id = 0;
  int32_t disk_no = 0;

  const int32_t MAX_DISK_NUM = 24;
  int32_t disk_no_array[MAX_DISK_NUM] = {};
  int32_t disk_cnt = 0;

  FILE * fp = fopen(cmd_line_param.range_file, "r");
  if (NULL == fp) return OB_IO_ERROR;
  while ( -1 != (read = getline(&line, &len, fp)) )
  {
    memset(&range, 0, sizeof(range));
    int ret = sscanf(line, "%d,%ld,%s", &disk_no, &table_id, range_str);
    if (ret < 3)
    {
      fprintf(stderr, "error line = %s, read = %d\n", line , (int)read);
    }
    else if (OB_SUCCESS != (ret = parse_range_str(range_str, cmd_line_param.hex_format, range)))
    {
      fprintf(stderr, "range_str = %s not good.\n", range_str);
    }
    else
    {
      range.table_id_ = table_id;
      char range_buf[256];
      range.to_string(range_buf, 256);
      fprintf(stderr, "disk_no=%d, table_id=%ld, range_str=%s,range=%s\n", 
          disk_no, table_id, range_str, range_buf);
      add_tablet(image, range, cmd_line_param.version, disk_no);

      if (std::find(disk_no_array, 
            disk_no_array + disk_cnt, disk_no) 
          == disk_no_array + disk_cnt)
      {
        disk_no_array[disk_cnt++] = disk_no;
      }
    }
  }
  if (line) free(line);

  image.set_data_version(cmd_line_param.version);
  //image.dump();
  const int32_t MAX_IDX_FILE_LEN = 256;
  char idx_file[MAX_IDX_FILE_LEN];
  g_sstable_directory = "./"; //current directory.

  for (int i = 0; i < disk_cnt; ++i)
  {
    get_meta_path(cmd_line_param.version, disk_no_array[i], true, idx_file, MAX_IDX_FILE_LEN);
    image.write(idx_file, disk_no_array[i]);
  }

  return OB_SUCCESS;
}

void usage()
{
  fprintf(stderr, "./gen_meta -r, --range_file [-v, --version] [-x, --hex_format=0/1/2]\n");
  fprintf(stderr, "./gen_meta --\n");
}


void parse_cmd_line(int argc,char **argv,CmdLineParam &clp)
{
  int opt = 0;
  const char* opt_string = "r:v:x:";
  struct option longopts[] =
  {
    {"help",0,NULL,'h'},
    {"range_file",1,NULL,'r'},
    {"version",1,NULL,'v'},
    {"hex_format",1,NULL,'x'},
    {0,0,0,0}
  };

  memset(&clp,0,sizeof(clp));
  clp.hex_format = 0;
  clp.version = 1;
  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'h':
        usage();
        break;
      case 'r':
        clp.range_file = optarg;
        break;
      case 'v':
        clp.version = strtoll(optarg, NULL, 10);
        break;
      case 'x':
        clp.hex_format = static_cast<int32_t>(strtoll(optarg, NULL, 10));
        break;
      default:
        usage();
        exit(1);        
    }
  }

  if (NULL == clp.range_file)
  {
    fprintf(stderr, "range file not set\n");
    usage();
    exit(-1);
  }


}

int main ( int argc, char *argv[] )
{

  ob_init_memory_pool();

  ObTabletImage image;
  CmdLineParam clp;

  parse_cmd_line(argc, argv, clp);
  scan_file(clp);

  return EXIT_SUCCESS;
}       /* ----------  end of function main  ---------- */
