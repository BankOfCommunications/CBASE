/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.cpp is for what ...
 *
 *  Version: $Id: cs_admin.cpp 2010\xC4\xEA12\xD4\xC203\xC8\xD5 13\x{02b1}48\xB7\xD614\xC3\xEB qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "obsql.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"
#include "common_func.h"
#include "chunkserver/ob_tablet_image.h"
#include "ob_server_stats.h"
#include "schema_printer.h"
//#include "tablet_shower.h"
#include "selectstmt.h"
#include "insertstmt.h"
#include "updatestmt.h"
#include "deletestmt.h"

#include <getopt.h>
#include <ctype.h>
#include <config.h>
#include <string.h> 

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::obsql;

//--------------------------------------------------------------------
// class GFactory
//--------------------------------------------------------------------
GFactory GFactory::instance_;

GFactory::GFactory()
{
}

GFactory::~GFactory()
{
}

void GFactory::init_cmd_map()
{
  cmd_map_["show"] = CMD_SHOW;
  cmd_map_["help"] = CMD_HELP;
  cmd_map_["exit"] = CMD_EXIT;
  cmd_map_["set"] = CMD_SET;
  cmd_map_["select"] = CMD_SELECT;
  cmd_map_["insert"] = CMD_INSERT;
  cmd_map_["update"] = CMD_UPDATE;
  cmd_map_["delete"] = CMD_DELETE;
}

void GFactory::init_rowkey_map()
{
  tbsys::CConfig config;

  if (config.load(STR_ROWKWY_FILE) != EXIT_SUCCESS) 
    return;

  vector<string> tablekeylist;
  config.getSectionKey(STR_ROWKEY_COMPOSITION, tablekeylist);
  for (uint32_t i = 0; i < tablekeylist.size(); i++)
  {
    const char *table_key = tablekeylist[i].c_str();
    const char *value_str = config.getString(STR_ROWKEY_COMPOSITION, table_key, NULL);
    vector<RowKeySubType> elements;

    elements.clear();
    if (value_str == NULL)
    {
      /* empty input, it will be seen as default VARCHAR */
      rowkey_map_[table_key] = elements;
      continue;
    }
    else
    {
      vector<char*> type_list;
      char *up_value_str = duplicate_str(value_str);
      up_value_str = strupr(up_value_str);

      tbsys::CStringUtil::split(up_value_str, ",", type_list);
      for (uint32_t j = 0; j < type_list.size(); j++)
      {
        RowKeySubType sub_type = ROWKEY_MIN;
        if (strncmp(type_list[j], "INT8_T", 6) == 0)
          sub_type = ROWKEY_INT8_T;
        else if (strncmp(type_list[j], "INT16_T", 7) == 0)
          sub_type = ROWKEY_INT16_T;
        else if (strncmp(type_list[j], "INT32_T", 7) == 0)
          sub_type = ROWKEY_INT32_T;
        else if (strncmp(type_list[j], "INT64_T", 7) == 0)
          sub_type = ROWKEY_INT64_T;
        else if (strncmp(type_list[j], "FLOAT", 5) == 0)
          sub_type = ROWKEY_FLOAT;
        else if (strncmp(type_list[j], "DOUBLE", 6) == 0)
          sub_type = ROWKEY_DOUBLE;
        else if (strncmp(type_list[j], "VARCHAR", 7) == 0)
          sub_type = ROWKEY_VARCHAR;
        else if (strncmp(type_list[j], "DATETIME", 8) == 0)
          sub_type = ROWKEY_DATETIME;
        else if (strncmp(type_list[j], "PRECISEDATETIME", 15) == 0)
          sub_type = ROWKEY_PRECISEDATETIME;
        else if (strncmp(type_list[j], "CEATETIME", 9) == 0)
          sub_type = ROWKEY_CEATETIME;
        else if (strncmp(type_list[j], "MODIFYTIME", 10) == 0)
          sub_type = ROWKEY_MODIFYTIME;
     
        elements.push_back(sub_type);
      }
      rowkey_map_[table_key] = elements;
      ob_free(up_value_str);
    }
  }

}

int GFactory::initialize(const ObServer& root_server)
{
  common::ob_init_memory_pool(64*1024, false);
  common::ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  init_cmd_map();
  init_rowkey_map();

  int ret = client_.init();
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize client object failed, ret:%d", ret);
    return ret;
  }

  ret = get_instance().start();
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "start GFactory error, ret=%d\n", ret);
    return ret;
  }

  ret = rpc_stub_.initialize(root_server, &client_.get_client_manager());
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize rpc stub failed, ret:%d", ret);
  }
  
  return ret;
}

int GFactory::start()
{
  client_.start();
  return OB_SUCCESS;
}

int GFactory::stop()
{
  client_.stop();
  return OB_SUCCESS;
}

int GFactory::wait()
{
  client_.wait();
  return OB_SUCCESS;
}



int get_command_type(char *token)
{
  int cmd_type = CMD_MIN;
  
  const map<string, int> &cmd_map = GFactory::get_instance().get_cmd_map();
  map<string, int>::const_iterator it = cmd_map.find(token);
  if (it != cmd_map.end()) 
  {
    cmd_type = it->second;
  }
  
  return cmd_type;
}

int get_cs_or_ms_server(char *ipstr, bool is_cs, ObServer& server)
{
  int ret = OB_SUCCESS;

  std::vector<ObServer>* server_list = NULL;
  std::vector<ObServer>::iterator it;
  if (is_cs)
    server_list = &(GFactory::get_instance().get_rpc_stub().get_chunk_server_list());
  else
    server_list = &(GFactory::get_instance().get_rpc_stub().get_merge_server_list());
  for (it = server_list->begin(); it != server_list->end(); ++it)
  {
    if (it->get_ipv4() == static_cast<int32_t>(ObServer::convert_ipv4_addr(ipstr)))
      server = *it;
  }

  return ret;
}

/* this function will be replace by command parser */
int parse_show_command(char *&cmdstr, ObServer& server)
{
    
  char *token;
  int cmd_type = CMD_MIN;
  
  token = get_token(cmdstr);

  if (token == NULL)
  {
    return cmd_type;
  }
  else if (strcmp(token, "schema") == 0)
  {
    cmd_type = CMD_SHOW_SCHEMA;
  }
  else if (strcmp(token, "tablets") == 0)
  {
    cmd_type = CMD_SHOW_TABLET;
    token = get_token(cmdstr);

    if (token)
    {
      /* not follow by an IP string */
      if (NULL == strstr(token, "."))
      {
        cmdstr = putback_token(token, cmdstr);
        return cmd_type; 
      }
      
      get_cs_or_ms_server(token, true, server);
      if (server.get_ipv4() == 0)
        cmd_type = CMD_MIN;/* wrong IP */
    }
  }  
  else if (strcmp(token, "stats") == 0)
  {
    token = get_token(cmdstr);
    if (token == NULL)
    {
      return cmd_type;
    }
    else if (strcmp(token, "root_server") == 0)
    {
      cmd_type = CMD_SHOW_STATS_ROOT;
      server = GFactory::get_instance().get_rpc_stub().get_root_server();
    }
    else if (strcmp(token, "update_server") == 0)
    {
      cmd_type = CMD_SHOW_STATS_UPDATE;
      server = GFactory::get_instance().get_rpc_stub().get_update_server();
    }
    else if (strcmp(token, "chunk_server") == 0)
    {
      cmd_type = CMD_SHOW_STATS_CHUNK;
      token = get_token(cmdstr);

      if (token)
      {
        get_cs_or_ms_server(token, true, server);
        if (server.get_ipv4() == 0)
          cmd_type = CMD_MIN;/* wrong IP */
      }
    }
    else if (strcmp(token, "merge_server") == 0)
    {
      cmd_type = CMD_SHOW_STATS_MERGE;
      token = get_token(cmdstr);
        
      if (token)
      {
        get_cs_or_ms_server(token, false, server);
        if (server.get_ipv4() == 0)
          cmd_type = CMD_MIN;/* wrong IP */
      }
    }
  }

  return cmd_type;
}

int show_server_stats_info(int server_type, ObServer *server, int interval)
{
  int ret = OB_SUCCESS;

/*
  ObClientServerStub rpc_stub;
  ret = rpc_stub.initialize( 
      GFactory::get_instance().get_rpc_stub().get_root_server(),    
      &GFactory::get_instance().get_base_client().get_client_manager(),
      GFactory::get_instance().get_rpc_stub().get_update_server(),
      GFactory::get_instance().get_rpc_stub().get_chunk_server_manager()
      );
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "initialize rpc stub failed, ret = %d\n", ret);
    return ret;
  }
*/

  ObServerStats* server_stats = new ObServerStats(GFactory::get_instance().get_rpc_stub(), server_type);


  ret = server_stats->summary(server, interval);
  if (OB_SUCCESS != ret) 
    fprintf(stderr, "summary error, ret=%d\n", ret);

  delete server_stats;

  return ret;
}

int show_schema()
{
  int ret = OB_SUCCESS;

  ObSchemaManagerV2 schema_mgr;

  ret = GFactory::get_instance().get_rpc_stub().fetch_schema(schema_mgr);
  if (OB_SUCCESS != ret) 
    return ret;

  oceanbase::obsql::ObSchemaPrinter schema_prt(schema_mgr);
  ret = schema_prt.output();

  return ret;
}

/*
int do_work(const int cmd, const vector<string> &param)
{
  int ret = OB_SUCCESS;
  switch (cmd)
  {
    case CMD_DUMP_TABLET:
      ret = dump_tablet_image(param);
      break;
    case CMD_DUMP_SERVER_STATS:
      ret = dump_stats_info(ServerStats, param);
      break;
    case CMD_DUMP_CLUSTER_STATS:
      ret = dump_stats_info(ClusterStats, param);
      break;
    case CMD_MANUAL_MERGE:
      ret = manual_merge(param);
      break;
    case CMD_MANUAL_DROP_TABLET:
      ret = manual_drop_tablet(param);
      break;
    case CMD_MANUAL_GC:
      ret = manual_gc(param);
      break;
    default:
      fprintf(stderr, "unsupported command : %d\n", cmd);
      ret = OB_ERROR;
      break;
  }
  return ret;
}
*/

void usage()
{
  fprintf(stderr, "This is obsql, the OceanBase console & interactive terminal.\n\n");
  fprintf(stderr, "Usage: ./obsql -h root_server_ip -p root_server_port [-q] [-?/--help]\n");
  fprintf(stderr, "Options:\n");
  fprintf(stderr, "    -h HOSTIP    root server IP\n");
  fprintf(stderr, "    -p PORT      root server PORT\n");
  fprintf(stderr, "    -q           set log to ERROR level\n");
  fprintf(stderr, "    -?/--help    usage\n");
}

void cmd_usage()
{
  fprintf(stderr, "\nUsage: \n");
  fprintf(stderr, "show schema -- show table schema\n");
  fprintf(stderr, "show stats root_server/update_server/chunk_server/merge_server [XXX.XXX.XXX.XXX]\n");
  fprintf(stderr, "show tablets [XXX.XXX.XXX.XXX] [disk_no]/[disk_no_1, disk_no_2]/[disk_no_1~disk_no_2]\n");
  fprintf(stderr, "help -- get usage help\n");
  fprintf(stderr, "exit -- exit obsql\n");
  fprintf(stderr, "--\n");
  fprintf(stderr, "-- Three comments for using the SQL query:\n");
  fprintf(stderr, "-- 1. Define your rowkey composition in file rowkey.ini, now we only support INT type, other types are in debugging...\n");
  fprintf(stderr, "-- 2. use rowkey with parentheses\n");
  fprintf(stderr, "-- 3. we do not use sigle quote for string in stage 1, so do not use reserved keyword in string\n");
  fprintf(stderr, "--\n");
  fprintf(stderr, "select * from tablename where rowkey=(1001,5,84)\n");
  fprintf(stderr, "insert into tablename values(XXX, ..., XXX) where rowkey=(1001,5,84)\n");
  fprintf(stderr, "update tablename set column1=XXX where rowkey=(1001,5,84)\n");
  fprintf(stderr, "delete from tablename where rowkey=(1001,5,84)\n");
  fprintf(stderr, "more usages...\n");
}


int parse_obsql_options(int argc, char *argv[], ObServer &root_server)
{
  int ret = 0;
  const char *addr = NULL;
  int32_t port = 0;

  static struct option longopts[] = 
  {
    {"hostip", required_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"quiet", required_argument, NULL, 'q'},
    {"help", no_argument, NULL, '?'},
    {NULL, 0, NULL, 0}
  };
      
  while((ret = getopt_long(argc, argv,"h:p:q?", longopts, NULL)) != -1)
  {
    switch(ret)
    {
      case 'h':
        addr = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'q':
        TBSYS_LOGGER.setLogLevel("ERROR");
        break;
      case '?':
        if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0)
          usage();
        break;
      default:
        fprintf(stderr, "%s is not identified\n",optarg);
        usage();
        exit(1);
        break;
    };
  }

  ret = root_server.set_ipv4_addr(addr, port);
  if (!ret)
  {
    fprintf(stderr, "root_server addr invalid, addr=%s, port=%d\n", addr, port);
    return OB_ERROR;
  }

  return OB_SUCCESS;
}



int main(const int argc, char *argv[])
{
  int ret = 0;
  int exit_flag = 0;
  int cmd_type;
  char *token = NULL;

  ObServer root_server;

  ret = parse_obsql_options(argc, argv, root_server);
  if ( OB_SUCCESS != ret)
  {
    usage();
    return ret;
  }

  ret = GFactory::get_instance().initialize(root_server);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "initialize GFactory error, ret=%d\n", ret);
    return ret;
  }

  while ( !exit_flag)
  {
    char cmdbuf[256];
    char *p;

    fprintf(stderr, "obsql>");

    do
    {
      fgets(cmdbuf, sizeof(cmdbuf), stdin);
    }while (strlen(p = trim(cmdbuf)) == 0);

    token = get_token(p);
    cmd_type = get_command_type(token);

    switch(cmd_type)
    {
      case CMD_SHOW:
      {
/*
        const int MAX_SERVER_COUNT = 1000;
        ObServer data_holder[MAX_SERVER_COUNT];
        common::ObArrayHelper<ObServer> servers(MAX_SERVER_COUNT, data_holder);
*/
        ObServer server;
        cmd_type = parse_show_command(p, server);
        switch(cmd_type)
        {
          case CMD_SHOW_SCHEMA:
            ret = show_schema(); 
            break;
          case CMD_SHOW_TABLET:
          {
/*
            ObTabletShower tablet_shower(GFactory::get_instance().get_rpc_stub(), server);
            ret = parse_number_range(p, tablet_shower.get_disks_array());
            if (ret != OB_SUCCESS)
            {
              fprintf(stderr, "parse array numbers error, ret=%d\n", ret);
              return ret;
            }
            
            tablet_shower.output();
*/
            fprintf(stderr, "Cannot support to show tablets now! \n");
            break;
          }
          case CMD_SHOW_STATS_ROOT:
            ret = show_server_stats_info(common::ObStatManager::SERVER_TYPE_ROOT, 
                                         (server.get_ipv4() != 0) ? &server : NULL,
                                         0);
            break;
          case CMD_SHOW_STATS_UPDATE:
            ret = show_server_stats_info(common::ObStatManager::SERVER_TYPE_UPDATE,
                                         (server.get_ipv4() != 0) ? &server : NULL,
                                         0);
            break;
          case CMD_SHOW_STATS_CHUNK:
            ret = show_server_stats_info(common::ObStatManager::SERVER_TYPE_CHUNK, 
                                         (server.get_ipv4() != 0) ? &server : NULL,
                                         0);
            break;
          case CMD_SHOW_STATS_MERGE:
            ret = show_server_stats_info(common::ObStatManager::SERVER_TYPE_MERGE,
                                         (server.get_ipv4() != 0) ? &server : NULL,
                                         0);
            break;
          case CMD_MIN:
          default:
            fprintf(stderr, "wrong command. Try \"help\" for more help\n");
            break;
        }
        break;
      }
      case CMD_HELP:
        cmd_usage();
        break;
      case CMD_EXIT:
        fprintf(stderr, "exit the obsql\n");
        exit_flag = 1;
        break;
      case CMD_SET:
        /* To be done */
        break;
      case CMD_SELECT:
      {
        p = putback_token(token, p);
        oceanbase::obsql::SelectStmt select_stmt(p, strlen(p) + 1, 
                                                 GFactory::get_instance().get_rpc_stub(),
                                                 GFactory::get_instance().get_rowkey_map());
        select_stmt.query();

        break;
      }
      case CMD_INSERT:
      {
        p = putback_token(token, p);
        oceanbase::obsql::InsertStmt insert_stmt(p, strlen(p) + 1, 
                                                 GFactory::get_instance().get_rpc_stub(),
                                                 GFactory::get_instance().get_rowkey_map());

        insert_stmt.query();
        
        break;
      }
      case CMD_UPDATE:
      {
        p = putback_token(token, p);
        oceanbase::obsql::UpdateStmt update_stmt(p, strlen(p) + 1,
                                                 GFactory::get_instance().get_rpc_stub(),
                                                 GFactory::get_instance().get_rowkey_map());
        update_stmt.query();

        break;
      }
      case CMD_DELETE:
      {
        p = putback_token(token, p);
        oceanbase::obsql::DeleteStmt delete_stmt(p, strlen(p) + 1,
                                                 GFactory::get_instance().get_rpc_stub(),
                                                 GFactory::get_instance().get_rowkey_map());
        delete_stmt.query();

        break;
      }
      case CMD_MIN:
      case CMD_MAX:
      default:
        fprintf(stderr, "wrong command. Try \"help\" for more help\n");
        break;
    }
    
  }

  GFactory::get_instance().stop();
  GFactory::get_instance().wait();

  return ret;
}

