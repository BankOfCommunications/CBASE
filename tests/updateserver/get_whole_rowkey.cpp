#include <stdio.h>
#include "mock_client.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

static const int64_t TIMEOUT = 10000000L;

void init_mock_client(const char *addr, int32_t port, MockClient &client)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

int main(int argc, char **argv)
{
  if (7 > argc)
  {
    fprintf(stdout, "./get_whole_rowkey addr port table_id start_version end_version rfile\n");
    exit(1);
  }

  UNUSED(argc);
  char *addr = argv[1];
  int port = atoi(argv[2]);
  uint64_t table_id = atoll(argv[3]);
  int64_t start_version = atoll(argv[4]);
  int64_t end_version = INT64_MAX;
  if (0 != strcmp(argv[5], "MAX"))
  {
    end_version = atoll(argv[5]);
  }
  char *rfile = argv[6];

  ObScanParam scan_param;
  ObRange range;
  range.table_id_ = table_id;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  ObVersionRange version_range;
  version_range.start_version_ = start_version;
  version_range.end_version_ = end_version;
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  if (INT64_MAX == end_version)
  {
    version_range.border_flag_.set_max_value();
  }
  scan_param.set_version_range(version_range);
  scan_param.set(table_id, ObString(), range);
  scan_param.add_column(OB_FULL_ROW_COLUMN_ID);

  MockClient client;
  init_mock_client(addr, port, client);

  FILE *fd = fopen(rfile, "w");
  ObScanner scanner;
  while (true)
  {
    int ret = client.ups_scan(scan_param, scanner, TIMEOUT);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "ups_scan ret=%d\n", ret);
      break;
    }
    ObCellInfo *ci = NULL;
    while (OB_SUCCESS == scanner.next_cell())
    {
      bool irc = false;
      ret = scanner.get_cell(&ci, &irc);
      if (OB_ITER_END == ret)
      {
        break;
      }
      if (irc)
      {
        fwrite(ci->row_key_.ptr(), ci->row_key_.length(), 1, fd);
        //fwrite("\n", 1, 1, fd);
      }
    }
    if (NULL == ci)
    {
      break;
    }
    range.start_key_ = ci->row_key_;
    range.border_flag_.unset_min_value();
    scan_param.set_range(range);
  }
  fclose(fd);
}

