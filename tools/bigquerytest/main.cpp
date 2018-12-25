/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: main.cpp,v 0.1 2012/02/27 16:53:05 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <getopt.h>
#include "util.h"
#include "key_generator.h"
#include "bigquerytest.h"
#include "mysql.h"

char g_config[1024];

void print_usage()
{
  fprintf(stderr, "./bigquery -f bigquery.conf\n");
}

void parse_cmd_line(const int argc,  char *const argv[])
{
  int opt = 0;
  const char* opt_string = "f:h";
  struct option longopts[] =
  {
    {"config", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'f':
        snprintf(g_config, sizeof (g_config), "%s", optarg);
        break;
      case 'h':
        print_usage();
        exit(1);
      default:
        break;
    }
  }
}

int main(int argc, char** argv)
{
  int ret = EXIT_SUCCESS;
  mysql_init(NULL);
  parse_cmd_line(argc, argv);
  TBSYS_CONFIG.load(g_config);
  TBSYS_LOGGER.setFileName("bigquery.log");
  TBSYS_LOGGER.setLogLevel("info");
  TBSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L); /* 256M */

  BigqueryTest bigquery;
  bigquery.start();
  bigquery.wait();
  bigquery.stop();

  return ret;
}

