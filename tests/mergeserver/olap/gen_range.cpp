#include <iostream>
#include <cstdlib>
#include <getopt.h>
#include <cstdio>
#include <arpa/inet.h>
#include "olap.h"
#include "common/ob_malloc.h"
using namespace std;
struct gen_param
{
  uint32_t start_include_;
  uint32_t end_include_;
  uint32_t tablet_size_;
};

void usage(char **argv)
{
  fprintf(stderr, "%s -s row_key_start[uint32_t] -e row_key_end[uint32_t] -i tablet_size[uint32_t]\n", argv[0]);
  exit(0);
}

void parse_cmd_line(int argc, char **argv, gen_param &param)
{
  int opt = 0;
  const char *opt_string = "s:e:i:h";
  struct option longopts[] =
  {
    {"help", 0, NULL, 'h'},
    {"row_key_start", 1, NULL, 's'},
    {"row_key_end", 1, NULL, 'e'},
    {"tablet_size", 1, NULL, 'i'}
  };
  memset(&param, 0x00, sizeof(param));
  if(argc <= 1)
  {
    usage(argv);
  }
  while ((opt = getopt_long(argc,argv, opt_string,longopts,NULL)) != -1)
  {
    switch (opt)
    {
    case 'h':
      usage(argv);
      break; 
    case 's':
      param.start_include_ = static_cast<uint32_t>(strtoul(optarg,NULL,10));
      break;
    case 'e':
      param.end_include_ = static_cast<uint32_t>(strtoul(optarg,NULL,10));
      break;
    case 'i':
      param.tablet_size_ = static_cast<uint32_t>(strtoul(optarg,NULL,10));
      break;
    default:
      usage(argv);
      break;
    }
  }
}

void produce_ranges(const gen_param & param)
{
  int64_t prev_tablet_end_key = -1;
  int64_t cur_tablet_end_key = param.start_include_;
  int64_t end_key = param.end_include_;
  int64_t interval = param.tablet_size_;
  int64_t disk_idx = 0;
  prev_tablet_end_key = cur_tablet_end_key;
  cur_tablet_end_key += interval;
  const char *MIN = "MIN";
  const char *MAX = "MAX";
  for (;cur_tablet_end_key <= end_key + interval; 
    prev_tablet_end_key = cur_tablet_end_key,cur_tablet_end_key += interval, disk_idx ++)
  {
    uint32_t big_endian_end_key = 0;
    char big_endian_end_key_buf[9];
    uint32_t big_endian_prev_end_key  = 0;
    char big_endian_prev_end_key_buf[9];
    big_endian_end_key = static_cast<uint32_t>(cur_tablet_end_key);
    big_endian_prev_end_key = static_cast<uint32_t>(prev_tablet_end_key);
    snprintf(big_endian_end_key_buf, sizeof(big_endian_end_key_buf),"%.8X", big_endian_end_key);
    snprintf(big_endian_prev_end_key_buf,sizeof(big_endian_prev_end_key_buf), "%.8X", big_endian_prev_end_key);
    const char * range_start = big_endian_prev_end_key_buf;
    const char * range_end = big_endian_end_key_buf;
    if(prev_tablet_end_key == param.start_include_)
    {
      range_start = MIN;
    }
    if(cur_tablet_end_key >  param.end_include_)
    {
      range_end = MAX;
    }
    fprintf(stderr, "%ld,%lu,(%s,%s]\n", disk_idx%10 + 1,msolap::target_table_id, range_start, range_end);
  }
}

int main(int argc, char **argv)
{
  oceanbase::common::ob_init_memory_pool();
  gen_param param;
  parse_cmd_line(argc,argv,param);
  produce_ranges(param);
  return 0;
}
