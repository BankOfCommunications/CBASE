#include <getopt.h>
#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "parse_file.h"

using namespace oceanbase::common;

void print_usage()
{
  fprintf(stderr, "parse_file [OPTION]\n");
  fprintf(stderr, "   -f|--file intput file name\n");
  fprintf(stderr, "   -h|--help usage help\n");
}

int main(int argc, char ** argv)
{
  ob_init_memory_pool();
  int ret = OB_SUCCESS;
  const char * opt_string = "f:h";
  struct option longopts[] =
  {
    {"file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char * file_path = NULL; //"./data/collect_info_1298978077230768_3";
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'f':
      file_path = optarg;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if (NULL == file_path)
  {
    print_usage();
    exit(1);
  }

  char * buffer = NULL;
  FILE * file = NULL;
  if (OB_SUCCESS == ret)
  {
    file = fopen(file_path, "r");
    if (NULL == file)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "fopen file failed:file[%s]", file_path);
    }
    else
    {
      buffer = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      if (NULL == buffer)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check alloc memory buffer failed:buffer[%p]", buffer);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = process_file(buffer, OB_MAX_PACKET_LENGTH, file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check process file failed:file[%s], ret[%d]", file_path, ret);
    }
  }

  if (file)
  {
    fclose(file);
  }
  if (buffer)
  {
    delete []buffer;
    buffer = NULL;
  }
  return ret;
}

int process_file(char * buffer, const int64_t len, FILE * file)
{
  int ret = OB_SUCCESS;
  if ((NULL == buffer) || (len < 0) || (NULL == file))
  {
    TBSYS_LOG(ERROR, "check buffer or file failed:buff[%p], len[%ld], file[%p]", buffer, len, file);
    ret = OB_ERROR;
  }
  else
  {
    int64_t pos = 0;
    int64_t file_pos = pos;
    int64_t read_pos = pos;
    while ((read_pos = fread(buffer, 1, len, file)) > 0)
    {
      ret = print_scanner(buffer, read_pos, pos = 0, stdout);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "process scanner result failed:ret[%d]", ret);
        break;
      }
      else
      {
        file_pos += pos;
        ret = fseek(file, file_pos, SEEK_SET);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check fseek failed:file_pos[%ld], ret[%d]", file_pos, ret);
          break;
        }
      }
    }
  }
  return ret;
}

int print_scanner(const char * buffer, const int64_t len, int64_t & pos, FILE * file)
{
  int ret = OB_SUCCESS;
  if ((NULL == buffer) || (NULL == file))
  {
    TBSYS_LOG(ERROR, "check buffer failed:buff[%p], file[%p]", buffer, file);
    ret = OB_ERROR;
  }
  else
  {
    ObScanner result;
    ret = result.deserialize(buffer, len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check deserialize scanner failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      bool first_cell = true;
      ObCellInfo * cur_cell = NULL;
      ObCellInfo * pre_cell = NULL;
      while (result.next_cell() == OB_SUCCESS)
      {
        ret = result.get_cell(&cur_cell);
        if (OB_SUCCESS == ret)
        {
          pre_cell = cur_cell;
          if (first_cell)
          {
            first_cell = false;
            fprintf(file, "table_name:%.*s, rowkey:%s\n", cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
                to_cstring(cur_cell->row_key_));
          }
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "get cell failed:ret[%d]", ret);
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        fprintf(file, "table_name:%.*s, rowkey:%s\n", pre_cell->table_name_.length(), pre_cell->table_name_.ptr(),
            to_cstring(pre_cell->row_key_));
      }
    }
  }
  return ret;
}


