/*
 * Copyright (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * rootserver admin tool
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "rootserver/ob_root_admin2.h"

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include "common/ob_define.h"
#include "common/ob_version.h"
#include "common/ob_result.h"
#include "common/serialization.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_ups_info.h"
#include "common/utility.h"
#include "common/ob_get_param.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/file_directory_utils.h"
#include "rootserver/ob_root_admin_cmd.h"
#include "rootserver/ob_root_stat_key.h"
#include "ob_daily_merge_checker.h"
#include "ob_root_table2.h"
#include "ob_tablet_info_manager.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace oceanbase
{
  namespace rootserver
  {
    void usage()
    {
      printf("Usage: rs_admin -r <rootserver_ip> -p <rootserver_port> <command> -o <suboptions>\n");
      printf("\n\t-r <rootserver_ip>\tthe default is `127.0.0.1'\n");
      printf("\t-p <rootserver_port>\tthe default is 2500\n");
      printf("\t-t <request_timeout_us>\tthe default is 10000000(10S)\n");
      printf("\n\t<command>:\n");
      printf("\tstat -o ");
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        printf("%s|", *str);
      }
      printf("\n");
      printf("\tboot_strap\n");
      printf("\tboot_recover\n");
      printf("\tdo_check_point\n");
      printf("\treload_config\n");
      printf("\tlog_move_to_debug\n");
      printf("\tlog_move_to_error\n");
      printf("\tdump_root_table\n");
      printf("\tclean_root_table\n");
      printf("\tcheck_root_table -o cs_ip=<cs_ip>,cs_port=<cs_port>\n");
      printf("\tget_row_checksum -o tablet_version=<tablet_version>,table_id=<table_id>\n");
      printf("\tcheck_tablet_version -o tablet_version=<tablet_version>\n");
      printf("\tdump_unusual_tablets\n");
      printf("\tdump_server_info\n");
      printf("\tdump_migrate_info\n");
      printf("\tdump_cs_tablet_info -o cs_ip=<cs_ip>,cs_port=<cs_port>\n");      
      printf("\tchange_log_level -o ERROR|WARN|INFO|DEBUG\n");
      printf("\tclean_error_msg\n");
      printf("\tcs_create_table -o table_id=<table_id> -o table_version=<frozen_verson>\n");
      printf("\tget_obi_role\n");
      printf("\tset_obi_role -o OBI_SLAVE|OBI_MASTER\n");
      printf("\tget_config\n");
      printf("\tset_config -o config_name=config_value[,config_name2=config_value2[,...]]\n");
      printf("\tget_obi_config\n");
      printf("\tset_obi_config -o read_percentage=<read_percentage>\n");
      printf("\tset_obi_config -o rs_ip=<rs_ip>,rs_port=<rs_port>,read_percentage=<read_percentage>\n");
      printf("\tset_ups_config -o ups_ip=<ups_ip>,ups_port=<ups_port>,ms_read_percentage=<percentage>,cs_read_percentage=<percentage>\n");
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //printf("\tset_master_ups_config -o master_master_ups_read_percentage=<percentage>,slave_master_ups_read_percentage=<percentage>\n");
      printf("\tset_master_ups_config -o master_master_ups_read_percentage=<percentage>,is_strong_consistent=<1 or 0>\n");
      //mod:e
      //add:e
      //mod pangtianze [Paxos] not give choice to force change master for user 20170703:b
      //printf("\tchange_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>[,force]\n");
      printf("\tchange_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>\n");
      //mod:e
      printf("\timport_tablets -o table_id=<table_id>\n");
      //printf("\tprint_root_table -o table_id=<table_id>\n");
      printf("\tprint_schema -o location=<location>\n");
      printf("\tread_root_table_point -o location=<location>\n");
      printf("\trefresh_schema\n");
      printf("\tcheck_schema\n");
      printf("\tswitch_schema\n");
      printf("\tchange_table_id -o table_id=<table_id>\n");
      printf("\tforce_create_table -o table_id=<table_id>\n");
      printf("\tforce_drop_table -o table_id=<table_id>\n");
      printf("\tenable_balance|disable_balance\n");
      printf("\tenable_rereplication|disable_rereplication\n");
      printf("\tshutdown -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\trestart_cs -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\tsplit_tablet -o table_id=<table_id> -o table_version=<table_version>\n");
      printf("\timport table_name table_id uri\n");
      printf("\tkill_import table_name table_id\n");
      printf("\tforce_cs_report\n");
      printf("\n\t-- For Paxos --\n");
      //add pangtianze [Paxos rs_election] 20150731:b      
      printf("\tset_rs_election_priority -o rs_ip=<rs_ip>,rs_port=<rs_port>,priority=<priority>\n");
      printf("\tset_rs_leader -o rs_ip=<rs_ip>,rs_port=<rs_port>\n");
      //add:e
       //add chujiajia [Paxos ups_election] 20151222:b
      printf("\tchange_rs_paxos_num -o paxos_num=<rs_paxos_num>\n");
      printf("\tchange_ups_quorum_scale -o quorum_scale=<ups_quorum_scale>\n");
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607020:b
      printf("\tdump_balancer_info -o table_id=<table_id>\n");
      printf("\tget_replica_num\n");
      //add bingo [Paxos table replica] 20170620:b
      printf("\tget_table_replica_num\n");
      //add:e
      //add bingo [Paxos rs_election] 20161010
      printf("\tget_all_rs_priority\n");
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      printf("\tset_master_cluster_id -o new_master_cluster_id=<cluser_id>\n");
      printf("\tget_rs_leader\n");
      //add:e
      //add pangtianze [Paxos rs_election] 20170228:b
      printf("\trefresh_rs_list\n");
      //add:e
      //add bingo [Paxos set_boot_ok] 20170315:b
      printf("\tset_boot_ok\n");
      printf("\tdump_sstable_info\n");
      //add:e
      printf("\t-- End Paxos --\n");
      printf("\n\t-h\tprint this help message\n");
      printf("\t-V\tprint the version\n");
      printf("\nSee `rs_admin.log' for the detailed execution log.\n");
    }

    void version()
    {
      printf("rs_admin (%s %s)\n", PACKAGE_STRING, RELEASEID);
      printf("SVN_VERSION: %s\n", svn_version());
      printf("BUILD_TIME: %s %s\n\n", build_date(), build_time());
      printf("Copyright (c) 2007-2011 Taobao Inc.\n");
    }

    const char* Arguments::DEFAULT_RS_HOST = "127.0.0.1";

    // define all commands string and its pcode here
    Command COMMANDS[] = {
      {
        "get_obi_role", OB_GET_OBI_ROLE, do_get_obi_role
      }
      ,
        {
          "set_obi_role", OB_SET_OBI_ROLE, do_set_obi_role
        }
      ,
        {
          "force_create_table", OB_FORCE_CREATE_TABLE_FOR_EMERGENCY, do_create_table_for_emergency
        }
      ,
        {
          "force_drop_table", OB_FORCE_DROP_TABLE_FOR_EMERGENCY, do_drop_table_for_emergency
        }
      ,
        {
          "do_check_point", OB_RS_ADMIN_CHECKPOINT, do_rs_admin
        }
      ,
        {
          "reload_config", OB_RS_ADMIN_RELOAD_CONFIG, do_rs_admin
        }
      ,
        {
          "log_move_to_debug", OB_RS_ADMIN_INC_LOG_LEVEL, do_rs_admin
        }
      ,
        {
          "log_move_to_error", OB_RS_ADMIN_DEC_LOG_LEVEL, do_rs_admin
        }
      ,
        {
          "dump_root_table", OB_RS_ADMIN_DUMP_ROOT_TABLE, do_rs_admin
        },
        {
          "clean_root_table", OB_RS_ADMIN_CLEAN_ROOT_TABLE, do_rs_admin
        }
      ,
        {
          "dump_server_info", OB_RS_ADMIN_DUMP_SERVER_INFO, do_rs_admin
        }
      ,
        {
          "dump_migrate_info", OB_RS_ADMIN_DUMP_MIGRATE_INFO, do_rs_admin
        }
      ,
        {
          "dump_unusual_tablets", OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS, do_rs_admin
        }
      ,
        {
          "check_schema", OB_RS_ADMIN_CHECK_SCHEMA, do_rs_admin
        }

      ,
        {
          "refresh_schema", OB_RS_ADMIN_REFRESH_SCHEMA, do_rs_admin
        }
      ,
        {
          "switch_schema", OB_RS_ADMIN_SWITCH_SCHEMA, do_rs_admin
        }
      ,
        {
          "clean_error_msg", OB_RS_ADMIN_CLEAN_ERROR_MSG, do_rs_admin
        }
      ,
        {
          "change_log_level", OB_CHANGE_LOG_LEVEL, do_change_log_level
        }
      ,
        {
          "stat", OB_RS_STAT, do_rs_stat
        }
      ,
        {
          "set_ups_config", OB_SET_UPS_CONFIG, do_set_ups_config
        }
      ,
        {
          "cs_create_table", OB_CS_CREATE_TABLE, do_cs_create_table
        }
      ,
        {
          "set_master_ups_config", OB_SET_MASTER_UPS_CONFIG, do_set_master_ups_config
        }
      ,
        {
          "change_ups_master", OB_CHANGE_UPS_MASTER, do_change_ups_master
        }
      ,
        {
          "change_table_id", OB_CHANGE_TABLE_ID, do_change_table_id
        }
      ,

        {
          "import_tablets", OB_CS_IMPORT_TABLETS, do_import_tablets
        }
      ,
        {
          "get_row_checksum", OB_GET_ROW_CHECKSUM, do_get_row_checksum
        }
      ,
        {
          "print_schema", OB_FETCH_SCHEMA, do_print_schema
        }
      ,
        {
          "print_root_table", OB_GET_REQUEST, do_print_root_table
        }
      ,
        {
          "enable_balance", OB_RS_ADMIN_ENABLE_BALANCE, do_rs_admin
        }
      ,
        {
          "disable_balance", OB_RS_ADMIN_DISABLE_BALANCE, do_rs_admin
        }
      ,
        {
          "enable_rereplication", OB_RS_ADMIN_ENABLE_REREPLICATION, do_rs_admin
        }
      ,
        {
          "disable_rereplication", OB_RS_ADMIN_DISABLE_REREPLICATION, do_rs_admin
        }
      ,
        {
          "shutdown", OB_RS_SHUTDOWN_SERVERS, do_shutdown_servers
        }
      ,
        {
          "boot_strap", OB_RS_ADMIN_BOOT_STRAP, do_rs_admin
        }
      //add liuxiao [secondary index] 20150320
      //bootstrapç”¨äºŽå»ºåˆ—æ ¡éªŒå’Œå†…éƒ¨è¡¨
      ,
        {
          "boot_strap_for_create_all_cchecksum_info", OB_RS_ADMIN_CREATE_ALL_CCHECKSUM_INFO, do_rs_admin
        }
      ,
        //add e
        {
          "boot_recover", OB_RS_ADMIN_BOOT_RECOVER, do_rs_admin
        }
      ,
        {
          "init_cluster", OB_RS_ADMIN_INIT_CLUSTER, do_rs_admin
        }
      ,
        {
          "restart_cs", OB_RS_RESTART_SERVERS, do_restart_servers
        }
      ,
        {
          "dump_cs_tablet_info" , OB_RS_DUMP_CS_TABLET_INFO, do_dump_cs_tablet_info
        }
      ,
      //add pangtianze [Paxos rs_election] 20170228:b
        {
           "refresh_rs_list", OB_RS_ADMIN_REFRESH_RS_LIST, do_rs_admin
        }
      ,
      //add:e
      //add pangtianze [Paxos rs_election] 20150731:b
        {
          "set_rs_election_priority", OB_SET_ELECTION_PRIORIYT, do_set_election_priority
        }
      ,
      //add:e
      //add pangtianze [Paxos rs_election] 20150813:b
        {
          "set_rs_leader" , OB_RS_ADMIN_SET_LEADER, do_set_rs_leader
        }
      ,
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
        {
          "change_rs_paxos_num" , OB_CHANGE_RS_PAXOS_NUMBER, do_change_rs_paxos_number
        }
      ,
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
        {
          "change_ups_quorum_scale" , OB_CHANGE_UPS_QUORUM_SCALE, do_change_ups_quorum_scale
        }
      ,
      //add:e
        {
          "check_tablet_version", OB_RS_CHECK_TABLET_MERGED, do_check_tablet_version
        }
      ,
        {
          "split_tablet", OB_RS_SPLIT_TABLET, do_split_tablet
        }
      ,
        {
          "check_root_table", OB_RS_CHECK_ROOTTABLE, do_check_roottable
        }
      ,
        {
          "set_config", OB_SET_CONFIG, do_set_config
        }
      ,
        {
          "get_config", OB_GET_CONFIG, do_get_config
        }
      ,
        {
          "import", OB_RS_ADMIN_START_IMPORT, do_import
        }
      ,
        {
          "kill_import", OB_RS_ADMIN_START_KILL_IMPORT, do_kill_import
        }
      ,
        {
          "read_root_table_point", OB_RS_ADMIN_READ_ROOT_TABLE, read_root_table_point
        }
      ,
        {
          "force_cs_report", OB_RS_FORCE_CS_REPORT, do_force_cs_report
        }
      ,
      //add lbzhong [Paxos Cluster.Balance] 201607020:b
        {
          "dump_balancer_info", OB_RS_DUMP_BALANCER_INFO, do_dump_balancer_info
        }
      ,
        {
          "get_replica_num", OB_GET_REPLICA_NUM, do_get_replica_num
        }
      ,
      //add:e
      //add bingo [Paxos rs_election] 20161010
        {
          "get_all_rs_priority", OB_MASTER_GET_ALL_PRIORITY, do_get_all_priority
        }
      ,
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
        {
          "set_master_cluster_id", OB_SET_MASTER_CLUSTER_ID, do_set_master_cluster_id
        }
      ,
      //add:e
      //add bingo [Paxos rs management] 20170301:b
        {
          "get_rs_leader", OB_GET_RS_LEADER, do_get_rs_leader
        }
      ,
      //add:e
      //add bingo [Paxos set_boot_ok] 20170315:b
        {
          "set_boot_ok", OB_RS_ADMIN_SET_BOOT_OK, do_rs_admin
        }
      ,
      //add:e
      //add bingo [Paxos sstable info to rs log] 20170614:b
        {
          "dump_sstable_info", OB_DUMP_SSTABLE_INFO, do_dump_sstable_info
        }
      ,
      //add:e
      //add bingo [Paxos table replica] 20170620:b
        {
          "get_table_replica_num", OB_GET_TABLE_REPLICA_NUM, do_get_table_replica_num
        }
      ,
      //add:e
    };

    enum
    {
      OPT_OBI_ROLE_MASTER = 0,
      OPT_OBI_ROLE_SLAVE = 1,
      OPT_LOG_LEVEL_ERROR = 2,
      OPT_LOG_LEVEL_WARN = 3,
      OPT_LOG_LEVEL_INFO = 4,
      OPT_LOG_LEVEL_DEBUG = 5,
      OPT_READ_PERCENTAGE = 6,
      OPT_UPS_IP = 7,
      OPT_UPS_PORT = 8,
      OPT_MS_READ_PERCENTAGE = 9,
      OPT_CS_READ_PERCENTAGE = 10,
      OPT_RS_IP = 11,
      OPT_RS_PORT = 12,
      OPT_TABLE_ID = 13,
      OPT_FORCE = 14,
      OPT_SERVER_LIST = 15,
      OPT_CANCEL = 16,
      OPT_CS_IP = 17,
      OPT_CS_PORT = 18,
      OPT_TABLET_VERSION = 19,
      OPT_TABLE_VERSION = 20,
      OPT_READ_MASTER_MASTER_UPS_PERCENTAGE = 21,
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE = 22,
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      OPT_IS_STRONG_CONSISTENT = 22,
      //add:e
      OPT_SCHEMA_LOCATION = 23,
      //add pangtianze [Paxos rs_election] 20150811:b
      OPT_RS_ELECTION_PRIORITY = 24,
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      OPT_CHANGE_RS_PAXOS_NUMBER = 25,
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
      OPT_CHANGE_UPS_QUORUM_SCALE = 26,
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      OPT_CLUSTER_ID = 27,
      OPT_REPLICA_NUM = 28,
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      OPT_MASTER_CLUSTER_ID = 29,
      //add:e
      THE_END
    };

    // 需要与上面的enum一一对应
    const char* SUB_OPTIONS[] =
    {
      "OBI_MASTER",
      "OBI_SLAVE",
      "ERROR",
      "WARN",
      "INFO",
      "DEBUG",
      "read_percentage",            // 6
      "ups_ip",                     // 7
      "ups_port",                   // 8
      "ms_read_percentage",         // 9
      "cs_read_percentage",         // 10
      "rs_ip",                      // 11
      "rs_port",                    // 12
      "table_id",                   // 13
      "force",                      // 14
      "server_list",                // 15
      "cancel",                     // 16
      "cs_ip",                      //17
      "cs_port",                    //18
      "tablet_version",             //19
      "table_version",
      "master_master_ups_read_percentage", //21
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //"slave_master_ups_read_percentage",
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      "is_strong_consistent",       //22
      //add:e
      "location",                   //23
      //add pangtianze [Paxos rs_election] 20150811:b
      "priority",                   //24
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      "paxos_num",      //25
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      "quorum_scale",           //26
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      "cluster_id",                 //27
      "replica_num",                 //28
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      "new_master_cluster_id",        //29
      //add:e
      NULL
    };

    int parse_cmd_line(int argc, char* argv[], Arguments &args)
    {
      int ret = OB_SUCCESS;
      // merge SUB_OPTIONS and OB_RS_STAT_KEYSTR
      int key_num = 0;
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        key_num++;
      }
      int local_num = ARRAYSIZEOF(SUB_OPTIONS);
      const char** all_sub_options = new(std::nothrow) const char*[key_num+local_num];
      if (NULL == all_sub_options)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(all_sub_options, 0, sizeof(all_sub_options));
        for (int i = 0; i < local_num; ++i)
        {
          all_sub_options[i] = SUB_OPTIONS[i];
        }
        for (int i = 0; i < key_num; ++i)
        {
          all_sub_options[i+local_num-1] = OB_RS_STAT_KEYSTR[i];
        }
        all_sub_options[local_num+key_num-1] = NULL;

        char *subopts = NULL;
        char *value = NULL;
        int ch = -1;
        int suboptidx = 0;
        while(-1 != (ch = getopt(argc, argv, "hVr:p:o:t:")))
        {
          switch(ch)
          {
            case '?':
              usage();
              exit(-1);
              break;
            case 'h':
              usage();
              exit(0);
              break;
            case 'V':
              version();
              exit(0);
              break;
            case 'r':
              args.rs_host = optarg;
              break;
            case 'p':
              args.rs_port = atoi(optarg);
              break;
            case 't':
              args.request_timeout_us = atoi(optarg);
              break;
            case 'o':
              subopts = optarg;
              snprintf(args.config_str, MAX_CONFIG_STR_LENGTH, "%s", subopts);
              while('\0' != *subopts)
              {
                switch(suboptidx = getsubopt(&subopts, (char* const*)all_sub_options, &value))
                {
                  case -1:
                    break;
                  case OPT_OBI_ROLE_MASTER:
                    args.obi_role.set_role(ObiRole::MASTER);
                    break;
                  case OPT_OBI_ROLE_SLAVE:
                    args.obi_role.set_role(ObiRole::SLAVE);
                    break;
                  case OPT_LOG_LEVEL_ERROR:
                    args.log_level = TBSYS_LOG_LEVEL_ERROR;
                    break;
                  case OPT_LOG_LEVEL_WARN:
                    args.log_level = TBSYS_LOG_LEVEL_WARN;
                    break;
                  case OPT_LOG_LEVEL_INFO:
                    args.log_level = TBSYS_LOG_LEVEL_INFO;
                    break;
                  case OPT_LOG_LEVEL_DEBUG:
                    args.log_level = TBSYS_LOG_LEVEL_DEBUG;
                    break;
                  case OPT_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("read_percentage needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.obi_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_UPS_IP:
                    if (NULL == value)
                    {
                      printf("option `ups' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_SCHEMA_LOCATION:
                    if (NULL == value)
                    {
                      printf("option location needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.location = value;
                      TBSYS_LOG(INFO, "location name is %s", args.location);
                    }
                    break;
                  case OPT_UPS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_CS_IP:
                    if (NULL == value)
                    {
                      printf("option 'cs_ip' needs an argment value \n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_ip = value;
                    }
                    break;
                  case OPT_CS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_port = atoi(value);
                    }
                    break;
                  case OPT_TABLET_VERSION:
                    if (NULL == value)
                    {
                      printf("option 'tablet_version' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.tablet_version = atoi(value);
                    }
                    break;
                  //add pangtianze [Paxos rs_election] 20150811:b
                  case OPT_RS_ELECTION_PRIORITY:
                    if (NULL == value)
                    {
                      printf("option 'priority' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.election_priority = atoi(value);
                    }
                    break;
                    //add:e

                  //add chujiajia [Paxos rs_election] 20151102:b
                  case OPT_CHANGE_RS_PAXOS_NUMBER:
                    if (NULL == value)
                    {
                      printf("option 'paxos_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.rs_paxos_number = atoi(value);
                    }
                    break;
                    //add:e

                  //add chujiajia [Paxos rs_election] 20151225:b
                  case OPT_CHANGE_UPS_QUORUM_SCALE:
                    if (NULL == value)
                    {
                      printf("option 'quorum_scale' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_quorum_scale = atoi(value);
                    }
                    break;
                    //add:e
                  case OPT_CLUSTER_ID:
                    if (NULL == value)
                    {
                      printf("option 'cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cluster_id = atoi(value);
                    }
                    break;
                  case OPT_REPLICA_NUM:
                    if (NULL == value)
                    {
                      printf("option 'replica_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.replica_num = atoi(value);
                    }
                    break;
                  //add:e
                  //add bingo [Paxos Cluster.Balance] 20161020:b
                  case OPT_MASTER_CLUSTER_ID:
                    if(NULL == value)
                    {
                      printf("option 'new_master_cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.new_master_cluster_id = atoi(value);
                    }
                    break;
                  //add:e

                  case OPT_MS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `ms_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ms_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_CS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `cs_read_percentage' needs an argument value\n");
                      ret = OB_ERROR;
                    }
                    else
                    {
                      args.cs_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_READ_MASTER_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `master_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = static_cast<int32_t>(strtol(value, &end, 10));
                      if (end != value + strlen(value))
                      {
                        printf("option `master_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.master_master_ups_read_percentage = data;
                      }
                    }
                    break;
                    //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                    /*
                  case OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `slave_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = static_cast<int32_t>(strtol(value, &end, 10));
                      if (end !=  value + strlen(value))
                      {
                        printf("option `slave_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.slave_master_ups_read_percentage = data;
                      }
                    }
                    break;
                    */
                    //del:e
                    //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                    case OPT_IS_STRONG_CONSISTENT:
                      if(NULL == value)
                      {
                          printf("option 'is_strong_consistent' needs an argument value\n");
                          ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                         args.is_strong_consistent = atoi(value);
                      }
                      break;
                    //add:e
                  case OPT_FORCE:
                    //mod pangtianze [Paxos] not give choice to force change master for user 20170703:b
                    //args.force_change_ups_master = 1;
                    args.force_change_ups_master = 0;
                    //mod:e
                    break;
                  case OPT_RS_IP:
                    if (NULL == value)
                    {
                      printf("option `rs_ip' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_RS_PORT:
                    if (NULL == value)
                    {
                      printf("option `rs_port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_TABLE_VERSION:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.table_version = atoi(value);
                    }
                    break;
                  case OPT_TABLE_ID:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.table_id = atoi(value);
                    }
                    break;
                  case OPT_SERVER_LIST:
                    if (NULL == value)
                    {
                      printf("option `server_list' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.server_list = value;
                    }
                    break;
                  case OPT_CANCEL:
                    args.flag_cancel = 1;
                    break;
                  default:
                    // stat keys
                    args.stat_key = suboptidx - local_num + 1;
                    break;
                }
              }
            default:
              break;
          }
        }
        if (OB_SUCCESS != ret)
        {
          usage();
        }
        else if (optind >= argc)
        {
          printf("no command specified\n");
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          args.argc = argc - optind;
          args.argv = &argv[optind];
          char* cmd = argv[optind];
          for (uint32_t i = 0; i < ARRAYSIZEOF(COMMANDS); ++i)
          {
            if (0 == strcmp(cmd, COMMANDS[i].cmdstr))
            {
              args.command.cmdstr = cmd;
              args.command.pcode = COMMANDS[i].pcode;
              args.command.handler = COMMANDS[i].handler;
              break;
            }
          }
          if (-1 == args.command.pcode)
          {
            printf("unknown command=%s\n", cmd);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            args.print();
          }
        }
        if (NULL != all_sub_options)
        {
          delete [] all_sub_options;
          all_sub_options = NULL;
        }
      }
      return ret;
    }

    void Arguments::print()
    {
      TBSYS_LOG(INFO, "server_ip=%s port=%d", rs_host, rs_port);
      TBSYS_LOG(INFO, "cmd=%s pcode=%d", command.cmdstr, command.pcode);
      TBSYS_LOG(INFO, "obi_role=%d", obi_role.get_role());
      TBSYS_LOG(INFO, "log_level=%d", log_level);
      TBSYS_LOG(INFO, "stat_key=%d", stat_key);
      TBSYS_LOG(INFO, "obi_read_percentage=%d", obi_read_percentage);
      TBSYS_LOG(INFO, "ups=%s port=%d", ups_ip, ups_port);
      TBSYS_LOG(INFO, "cs=%s port=%d",cs_ip, cs_port);
      //add pangtianze [Paxos rs_election] 20150811:b
      TBSYS_LOG(INFO, "election_priority=%d",election_priority);
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      TBSYS_LOG(INFO, "paxos_num=%d", rs_paxos_number);
      TBSYS_LOG(INFO, "quorum_scale=%d", ups_quorum_scale);
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      TBSYS_LOG(INFO, "cluster_id=%d,replica_num=%d", cluster_id, replica_num);
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      TBSYS_LOG(INFO, "new_master_cluster_id=%d", new_master_cluster_id);
      //add:e
      TBSYS_LOG(INFO, "ms_read_percentage=%d", ms_read_percentage);
      TBSYS_LOG(INFO, "cs_read_percentage=%d", cs_read_percentage);
      TBSYS_LOG(INFO, "force_change_ups_master=%d", force_change_ups_master);
      TBSYS_LOG(INFO, "table_id=%lu", table_id);
      TBSYS_LOG(INFO, "server_list=%s", server_list);
      TBSYS_LOG(INFO, "tablet_version=%ld", tablet_version);
    }

    int do_get_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_obi_role...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiRole role;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = role.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialized role, err=%d\n", ret);
        }
        else
        {
          printf("obi_role=%s\n", role.get_role_str());
        }
      }
      return ret;
    }

    int do_set_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("set_obi_role...role=%d\n", args.obi_role.get_role());
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = args.obi_role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to set obi role, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_admin(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_admin, cmd=%d...\n", args.command.pcode);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      // admin消息的内容为一个int32_t类型，定义在ob_root_admin_cmd.h中
      if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.command.pcode)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d, timeout=%ld\n", ret, args.request_timeout_us);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_change_log_level(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_change_log_level, level=%d...\n", args.log_level);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (Arguments::INVALID_LOG_LEVEL == args.log_level)
      {
        printf("invalid log level, level=%d\n", args.log_level);
      }
      // change_log_level消息的内容为一个int32_t类型log_level
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.log_level)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_LOG_LEVEL, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to change_log_level, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_stat(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_stat, key=%d...\n", args.stat_key);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      // rs_stat消息的内容为一个int32_t类型，定义在ob_root_stat_key.h中
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (0 > args.stat_key)
      {
        printf("invalid stat_key=%d\n", args.stat_key);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.stat_key)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("%s\n", msgbuf.get_data()+msgbuf.get_position());
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }
    int do_get_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_master_ups_config...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int32_t master_master_ups_read_percent = 0;
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        //int32_t slave_master_ups_read_percent = 0;
        //del:e
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &master_master_ups_read_percent)))
        {
          printf("failed to deserialize master_master_ups_read_percent");
        }
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &slave_master_ups_read_percent)))
        {
          printf("failed to deserialize slave_master_ups_read_percent");
        }
        */
        //del:e
        else
        {
          //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
          /*
          printf("master_master_ups_read_percentage=%d, slave_master_ups_read_percent=%d\n",
              master_master_ups_read_percent, slave_master_ups_read_percent);
          */
          printf("master_master_ups_read_percentage=%d\n", master_master_ups_read_percent);
          //mode:e
        }
      }
      return ret;
    }

    int do_set_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      if (((-1 != args.master_master_ups_read_percentage)
            && (0 > args.master_master_ups_read_percentage
              || 100 < args.master_master_ups_read_percentage))
          //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
          /*
          || ((-1 != args.slave_master_ups_read_percentage)
            && (0 > args.slave_master_ups_read_percentage
              || 100 < args.slave_master_ups_read_percentage))
          */
          //del:e
          //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
          || (0 != args.is_strong_consistent
                  && 1 != args.is_strong_consistent)
          //add:e
          )
      {
        //add bingo[Paxos Cluster.Flow.UPS] 20170116:b
        //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        printf("invalid param, master_master_ups_read_percentage=%d, slave_master_ups_read_percentage=%d\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);
            */
        printf("invalid param, master_master_ups_read_percentage=%d, is_strong_consistent=%d\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        //mod:e
        //add:e
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add bingo[Paxos Cluster.Flow.UPS] 20170221:b
      else if(0 == args.master_master_ups_read_percentage && 1 == args.is_strong_consistent)
      {
        printf("invalid param, master_master_ups_read_percentage=%d, is_strong_consistent=%d\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add:e
      else
      {
        //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
        //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        printf("set_master_ups_config, master_master_ups_read_percentage=%d,read_salve_master_ups_percentage=%d...\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);
        */
        printf("set_master_ups_config, master_master_ups_read_percentage=%d, is_strong_consistent=%d...\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        //mod:e
        //add:e

        static const int32_t MY_VERSION = 1;
        const int buff_size = OB_MAX_PACKET_LENGTH;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.master_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.slave_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        */
        //del:e
        //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.is_strong_consistent)))
        {
          printf("failed to serialize read consistency, err=%d\n", ret);
        }
        //add:e
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set master ups config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_cs_create_table(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      common::ObServer cs_addr;
      if ((1 > args.table_id) || (OB_INVALID_ID == args.table_id))
      {
        printf("invalid param, table_id=%lu\n", args.table_id);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!cs_addr.set_ipv4_addr(args.rs_host, args.rs_port))
      {
        printf("invalid param, cs_host=%s cs_port=%d\n", args.rs_host, args.rs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("force cs create empty table cs_ip=%s cs_port=%d version=%ld table_id=%lu...\n",
            args.rs_host, args.rs_port, args.table_version, args.table_id);
        static const int32_t MY_VERSION = 1;
        const int buff_size = sizeof(ObPacket) + 128;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        ObNewRange range;
        range.table_id_ = args.table_id;
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.set_inclusive_end();
        range.set_whole_range();
        int64_t timeout_us = args.request_timeout_us;
        if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          TBSYS_LOG(ERROR, "failed to serialize range, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                msgbuf.get_position(), args.table_version)))
        {
          TBSYS_LOG(ERROR, "failed to serialize key_src, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CS_CREATE_TABLE, MY_VERSION, timeout_us, msgbuf)))
        {
          TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
        }
        else
        {
          ObResultCode result;
          int64_t pos = 0;
          if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
          {
            TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
          }
          else if (OB_SUCCESS != result.result_code_)
          {
            ret = result.result_code_;
            TBSYS_LOG(ERROR, "failed to create tablet:rang[%s], ret[%d]", to_cstring(range), ret);
          }
        }
        printf("[%s] create empty table:cs[%s], table[%lu], version[%ld], ret[%d]\n", (OB_SUCCESS == ret) ? "SUCC" : "FAIL",
            cs_addr.to_cstring(), range.table_id_, args.table_version, ret);
      }
      return ret;
    }

    int do_set_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      common::ObServer ups_addr;
      if (0 > args.ms_read_percentage || 100 < args.ms_read_percentage)
      {
        printf("invalid param, ms_read_percentage=%d\n", args.ms_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 > args.cs_read_percentage || 100 < args.cs_read_percentage)
      {
        printf("invalid param, cs_read_percentage=%d\n", args.cs_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("set_ups_config, ups_ip=%s ups_port=%d ms_read_percentage=%d cs_read_percentage=%d...\n",
            args.ups_ip, args.ups_port, args.ms_read_percentage, args.cs_read_percentage);

        static const int32_t MY_VERSION = 1;
        const int buff_size = OB_MAX_PACKET_LENGTH;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);

        if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ms_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.cs_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set obi config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_check_roottable(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer cs_addr;
      printf("check root_table integrity...");
      if ((NULL == args.cs_ip) && (0 >= args.cs_port))
      {
      }
      else
      {
        if ((NULL == args.cs_ip) || (0 >= args.cs_port))
        {
          printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
        else if (!cs_addr.set_ipv4_addr(args.cs_ip, args.cs_port))
        {
          printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
      }
      printf("expect chunkserver addr:cs_addr=%s...\n", to_cstring(cs_addr));
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = cs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_CHECK_ROOTTABLE, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to check root_table integrity. err=%d", ret);
          }
          else
          {
            printf("Okay\n");
            bool is_integrity = false;
            if (OB_SUCCESS != (ret = serialization::decode_bool(
                    msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &is_integrity)))
            {
              printf("failed to decode check result, err=%d", ret);
            }
            else
            {
              printf("check result : roottable is %s integrity, expect cs=%s", is_integrity ? "" : "not", to_cstring(cs_addr));
            }
          }
        }
      }
      return ret;
    }

    int do_dump_cs_tablet_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer cs_addr;
      if (NULL == args.cs_ip)
      {
        printf("invalid param, cs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.cs_port)
      {
        printf("invalid param, cs_port is %d\n", args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!cs_addr.set_ipv4_addr(args.cs_ip, args.cs_port))
      {
        printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("dump chunkserver tablet info, cs_ip=%s, cs_port=%d...\n", args.cs_ip, args.cs_port);
        if (OB_SUCCESS != (ret = cs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_CS_TABLET_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to dump cs tablet info. err=%d", ret);
          }
          else
          {
            printf("Okay\n");
            int64_t tablet_num = 0;
            if (OB_SUCCESS != (ret = serialization::decode_vi64(
                    msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &tablet_num)))
            {
              printf("failed to decode tablet_num, err=%d", ret);
            }
            else
            {
              printf("tablet_num=%ld\n", tablet_num);
            }
          }
        }
      }
      return ret;
    }
    //add pangtianze [Paxos rs_election] 20150731:b
    int do_set_rs_leader(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      common::ObServer rs_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, rs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, rs_port is %d\n", args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!rs_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, rs_host=%s rs_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("set rs leader, rs_ip=%s, rs_port=%d...\n", args.ups_ip, args.ups_port);
        if (OB_SUCCESS != (ret = rs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_SET_LEADER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          //mod chujiajia [Paxos rs_election] 20151105:b
          //else if (OB_SUCCESS != (ret = result_code.result_code_))
          //{
          //  printf("Error occur, please check target rootserver and try again. err=%d", ret);
          //}
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            switch (result_code.result_code_)
            {
              case OB_IS_ALREADY_THE_MASTER:
                printf("the new leader is same to the old leader, leader don't change! err=%d.\n", ret);
                break;
              case OB_NOT_MASTER:
                printf("the command is not send to the leader, please check command and try again. err=%d.\n", ret);
                break;
              case OB_RS_NOT_EXIST:
                printf("rootserver [%s] is not exist. err=%d.\n", rs_addr.to_cstring(), ret);
                break;
              default:
                printf("the command didn't execute succ, please check command and try again. err=%d.\n", ret);
                break;
            }
          }
          //add:e
          else
          {
            printf("Okay, begin to set new leader, please check the result after a few seconds.\n");
          }
        }
      }
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150731:b
    int do_set_election_priority(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer rs_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, rs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, rs_port is %d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!rs_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, rs_host=%s rs_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add huangjianwei [Paxos election] 20160425:b
      else if (args.election_priority < 0 || args.election_priority > 9)
      {
        printf("invalid election_priority\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add:e
      else
      {
        printf("set election priority, rs_ip=%s, rs_port=%d...\n", args.ups_ip, args.ups_port);
        if (OB_SUCCESS != (ret = rs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = common::serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.election_priority)))
        {
           printf("failed to serialize election_priority, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_ELECTION_PRIORIYT, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", ret);
          }
          else if (OB_NOT_MASTER == (ret = result_code.result_code_))
          {
            printf("the command sent to the slave rootserver, please check command and try again. err=%d.\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set rootserver election priority, err=%d\n", ret);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }
    //add:e
    int do_change_ups_master(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer ups_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != args.force_change_ups_master && 1 != args.force_change_ups_master)
      {
        printf("invalid param, force=%d\n", args.force_change_ups_master);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_ups_master, ups_ip=%s ups_port=%d force=%d...\n", args.ups_ip, args.ups_port, args.force_change_ups_master);
        if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.force_change_ups_master)))
        {
          printf("failed to serialize int32, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_UPS_MASTER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to change ups master, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_get_row_checksum(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObRowChecksum row_checksum;
      if (args.tablet_version <= 0)
      {
        printf("invalid tablet_version\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.tablet_version)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_GET_ROW_CHECKSUM, MY_VERSION, args.request_timeout_us*20, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get table row_checksum, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = row_checksum.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else
        {
          printf("Okay\n%s\n", to_cstring(row_checksum));
        }
      }
      return ret;
    }

    int do_import_tablets(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("import_tablets...table_id=%lu\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      int64_t import_version = 0;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), import_version)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CS_IMPORT_TABLETS, MY_VERSION, args.request_timeout_us*20, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to import tablets, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_print_schema(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_schema...\n");
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      int64_t schema_version = 0;
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
      {
        TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        //common::ObSchemaManagerV2 schema_manager;
        int64_t pos = 0;
        //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
        common::ObSchemaManagerV2 *schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
        if (NULL == schema_manager)
        {
          TBSYS_LOG(WARN, "fail to new schema_manager.");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        //add:e
        else if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to fetch schema, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = schema_manager->deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize schema manager, err=%d\n", ret);
        }

        //add zhaoqiong [Schema Manager] 20150327:b
        while (OB_SUCCESS == ret && !schema_manager->is_completion())
        {
          int64_t new_table_begin = -1;
          int64_t new_column_begin = -1;
          schema_manager->get_next_start_pos(new_table_begin,new_column_begin);
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_manager->get_version())))
          {
            TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), new_table_begin)))
          {
            TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), new_column_begin)))
          {
            TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = client.send_recv(OB_FETCH_SCHEMA_NEXT, MY_VERSION, args.request_timeout_us, msgbuf)))
          {
            printf("failed to send request, err=%d\n", ret);
          }
          else
          {
            pos = 0;
            if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
            {
              printf("failed to deserialize response, err=%d\n", ret);
            }
            else if (OB_SUCCESS != (ret = result_code.result_code_))
            {
              printf("failed to fetch schema, err=%d\n", result_code.result_code_);
            }
            else if (OB_SUCCESS != (ret = schema_manager->deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
            {
              printf("failed to deserialize schema manager, err=%d\n", ret);
            }
          }
        }
        //add:e
        //mod zhaoqiong [Schema Manager] 20150327:b
        //else
        if (OB_SUCCESS == ret)
        //mod:e
        {
          //schema_manager.print(stdout);
          if (common::FileDirectoryUtils::exists(args.location))
          {
            printf("file already exist. fail to write. file_name=%s\n", args.location);
          }
          else
          {
            printf("write schema to file. file_name=%s\n", args.location);
            ret = schema_manager->write_to_file(args.location);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to write schema to file. file_name=%s\n", args.location);
            }
          }
        }
        //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
        if (schema_manager != NULL)
        {
          OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
        }
        //add:e
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }      
      return ret;
    }

    int do_print_root_table(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_root_table, table_id=%lu...\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      ObObj objs[1];
      ObCellInfo cell;
      cell.table_id_ = args.table_id;
      cell.column_id_ = 0;
      objs[0].set_int(0);
      cell.row_key_.assign(objs, 1);
      ObGetParam get_param;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_param.add_cell(cell)))
      {
        printf("failed to add cell to get_param, err=%d\n", ret);
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = get_param.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        common::ObScanner scanner;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get root table, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = scanner.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize scanner, err=%d\n", ret);
        }
        else
        {
          print_root_table(stdout, scanner);
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    static int serialize_servers(const char* server_list, ObDataBuffer &msgbuf)
    {
      int ret = OB_SUCCESS;
      char* servers = NULL;
      char* servers2 = NULL;
      if (NULL == server_list)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (servers = strdup(server_list))
          || NULL == (servers2 = strdup(server_list)))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        char *saveptr = NULL;
        const char* delim = "+";
        // count
        int32_t count = 0;
        char *server = strtok_r(servers, delim, &saveptr);
        while(NULL != server)
        {
          count++;
          server = strtok_r(NULL, delim, &saveptr);
        }
        if (0 >= count)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(ERROR, "no valid server, servers=%s", server_list);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), count)))
        {
          TBSYS_LOG(ERROR, "serialize error");
        }
        else
        {
          // serialize
          ObServer addr;
          const int ignored_port = 1;
          server = strtok_r(servers2, delim, &saveptr);
          while(NULL != server)
          {
            if (!addr.set_ipv4_addr(server, ignored_port))
            {
              ret = OB_INVALID_ARGUMENT;
              TBSYS_LOG(ERROR, "invalid addr, server=%s", server);
              break;
            }
            else if (OB_SUCCESS != (ret = addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
            {
              TBSYS_LOG(ERROR, "serialize error");
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "server=%s", server);
            }
            server = strtok_r(NULL, delim, &saveptr);
          }
        }
      }
      if (NULL != servers)
      {
        free(servers);
        servers = NULL;
      }
      if (NULL != servers2)
      {
        free(servers2);
        servers2 = NULL;
      }
      return ret;
    }

    int do_restart_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_restart_servers, server_list=%s cancel=%d...\n",
          args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if(OB_SUCCESS == ret)
      {
        if(0 == strcmp(args.server_list, "all"))
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), true);
        }
        else
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), false);
          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
            {
              printf("failed to serialize get_param, err=%d\n", ret);
            }
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.flag_cancel)))
        {
          TBSYS_LOG(ERROR, "serialize error");
        }
        else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us,
                msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          int64_t pos = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to get restart servers, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_shutdown_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_shutdown_servers, server_list=%s cancel=%d...\n",
          args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
              msgbuf.get_position(), args.flag_cancel)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us,
              msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get shut down servers, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_check_tablet_version(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (0 >= args.tablet_version)
      {
        printf("invalid param. tablet_version is %ld", args.tablet_version);
        usage();
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                msgbuf.get_position(), args.tablet_version)))
        {
          printf("fail to encode tablet_version to buf. err=%d", err);
        }
        else if (OB_SUCCESS != (err = client.send_recv(OB_RS_CHECK_TABLET_MERGED, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to check tablet version. err=%d", err);
          }
          else
          {
            int64_t is_merged = 0;
            if (OB_SUCCESS != (err = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                    msgbuf.get_position(), &is_merged)))
            {
              printf("failed to deserialize is_merged, err=%d", err);
            }
            else
            {
              if (0 == is_merged)
              {
                printf("OKay, check_version=%ld, already have some tablet not reach this version\n",args.tablet_version);
              }
              else
              {
                printf("OKay, check_version=%ld, all tablet reach this version\n", args.tablet_version);
              }
            }
          }
        }
      }
      return err;
    }

    int do_split_tablet(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      printf("for split tablet...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (err = client.send_recv(OB_RS_SPLIT_TABLET, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d", err);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to split tablet. err=%d", err);
        }
        else
        {
          printf("OKay");
        }
      }
      return err;
    }

    int do_set_config(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObString str = ObString::make_string(args.config_str);
      int64_t pos = 0;
      if (strlen(args.config_str) <= 0)
      {
        printf("ERROR: config string not specified.\n");
        printf("\tset_config -o config_name=config_value[,config_name2=config_value2[,...]]\n");
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != (err = str.serialize(buf, sizeof (buf), pos)))
      {
        printf("Serialize config string failed! ret: [%d]", err);
      }
      else
      {
        printf("config_str: %s\n", args.config_str);

        ObDataBuffer msgbuf(buf, sizeof (buf));
        msgbuf.get_position() = pos;
        if (OB_SUCCESS != (err = client.send_recv(OB_SET_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to set config. err=%d\n", err);
          }
          else
          {
            printf("OKay\n");
          }
        }
      }
      return err;
    }

    int do_get_config(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to set config. err=%d\n", err);
        }
        else
        {
          static const int HEADER_LENGTH = sizeof (uint32_t) + sizeof (uint64_t);
          uint64_t length = *reinterpret_cast<uint64_t *>(msgbuf.get_data() + msgbuf.get_position() + sizeof (uint32_t));
          msgbuf.get_position() += HEADER_LENGTH;
          printf("%.*s", static_cast<int>(length), msgbuf.get_data() + msgbuf.get_position());
        }
      }
      return err;
    }
    int do_change_table_id(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (0 >= args.table_id)
      {
        printf("invalid param, table_id=%ld\n", args.table_id);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_table_id, table_id=%ld...\n", args.table_id);
        if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_TABLE_ID, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to change table id, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_import(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObString table_name;
      uint64_t table_id;
      ObString uri;
      int64_t timeout = 120*1000L*1000L;//120s

      if (args.request_timeout_us > timeout)
      {
        timeout = args.request_timeout_us;
      }

      if (args.argc != 4)
      {
        printf("wrong cmd format, please use import <tablename> <table_id> <uri>\n");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 == (table_id = strtoull(args.argv[2], 0, 10)))
      {
        printf("table id must not 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        table_name.assign_ptr(args.argv[1], static_cast<ObString::obstr_size_t>(strlen(args.argv[1])));
        uri.assign_ptr(args.argv[3], static_cast<ObString::obstr_size_t>(strlen(args.argv[3])));
        printf("import table_name=%.*s table_id=%lu uri=%.*s\n", table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
        if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = uri.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_START_IMPORT, MY_VERSION, timeout, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to import table, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_kill_import(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObString table_name;
      uint64_t table_id;
      int64_t timeout = 120*1000L*1000L;//120s

      if (args.request_timeout_us > timeout)
      {
        timeout = args.request_timeout_us;
      }


      if (args.argc != 3)
      {
        printf("wrong cmd format, please use import <tablename> <table_id>\n");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 == (table_id = strtoull(args.argv[2], 0, 10)))
      {
        printf("table id must not 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        table_name.assign_ptr(args.argv[1], static_cast<ObString::obstr_size_t>(strlen(args.argv[1])));
        printf("kill import table_name=%.*s table_id=%lu\n", table_name.length(), table_name.ptr(), table_id);
        if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_START_KILL_IMPORT, MY_VERSION, timeout, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to kill import table, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }
    int do_create_table_for_emergency(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force to create table for emergency, table_id=%ld\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_FORCE_CREATE_TABLE_FOR_EMERGENCY, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to create table for emergency, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
    int do_drop_table_for_emergency(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force to drop table for emergency, table_id=%ld\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_FORCE_DROP_TABLE_FOR_EMERGENCY, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to drop table for emergency, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int read_root_table_point(ObBaseClient &client, Arguments &args)
    {
      UNUSED(client);
      int ret = OB_SUCCESS;
      ObTabletInfoManager *tablet_manager = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER);
      ObRootTable2 *root_table  = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager);
      if (tablet_manager != NULL && root_table != NULL)
      {
        printf("read_from_file.");
        if (OB_SUCCESS == root_table->read_from_file(args.location))
        {
          root_table->dump();
        }
      }
      else
      {
        TBSYS_LOG(WARN, "obmalloc root table or tablet manager failed");
      }
      if (root_table != NULL)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table);
      }
      if (tablet_manager != NULL)
      {
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager);
      }
      return ret;
    }
    int do_force_cs_report(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force cs report...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_RS_FORCE_CS_REPORT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiRole role;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to request cs report, err=%d\n", result_code.result_code_);
        }
      }
      return ret;
    }

    //add chujiajia [Paxos rs_election] 20151102:b
    int do_change_rs_paxos_number(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.rs_paxos_number)
      {
        printf("invalid param, rs_paxos_num=%d\n", args.rs_paxos_number);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_rs_paxos_num, rs_paxos_num=%d...\n", args.rs_paxos_number);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.rs_paxos_number)))
        {
          printf("failed to serialize rs_paxos_num, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_RS_PAXOS_NUMBER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          //add pangtianze [Paxos rs_election] 20161010:b
          int32_t current_max_deploy_rs_count = 0;
          int32_t old_max_deploy_rs_count = 0;
          int32_t alive_rs_count = 0;
          //add:e
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          //add pangtianze [Paxos rs_election] 20161010:b
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &current_max_deploy_rs_count)))
          {
            TBSYS_LOG(WARN,"failed to deserialize current_rs_paxos_num, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &old_max_deploy_rs_count)))
          {
            TBSYS_LOG(WARN,"failed to deserialize old_rs_paxos_num, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &alive_rs_count)))
          {
            TBSYS_LOG(WARN,"failed to deserialize alive_rs_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            switch (result_code.result_code_)
            {
              case OB_INVALID_PAXOS_NUM:
                //add bingo [Paxos rs management] 201170217:b
                if(args.rs_paxos_number == 2 && alive_rs_count == 1){
                  printf("receive new paxos number:%d, but the second rs should be online first, alive_rs_count still is %d, err=%d.\n",
                         args.rs_paxos_number,alive_rs_count,ret);
                }
                //add:e
                else if(args.rs_paxos_number <= (alive_rs_count / 2) ||
                   args.rs_paxos_number >= (alive_rs_count * 2) ||
                   args.rs_paxos_number > OB_MAX_RS_COUNT)
                {
                  printf("receive invalid new paxos number:%d, it should be in (%d,%d), err=%d.\n",
                         args.rs_paxos_number, alive_rs_count/2, alive_rs_count*2, ret);
                }
                else if (args.rs_paxos_number != alive_rs_count && 0 != alive_rs_count && 1 != args.rs_paxos_number)
                {
                  printf("new paxos number must be equal to alive rootserver count, but new_paxos_num=%d, rs_count=%d, err=%d.\n",
                         args.rs_paxos_number, alive_rs_count, ret);
                }
                break;
              case OB_RS_NOT_EXIST:
                printf("some slave rootserver maybe not get new paxos number, please check and do again!, err=%d\n", ret);
                break;
              default:
                printf("error occurred. err=%d\n", ret);
                break;
            }
          }
          else if (1 == args.rs_paxos_number)
          {
            //mod bingo [Paxos rs management] 20170217:b
            printf("change paxos number [%d->1], slave rootserver should be offine soon, please check and kill them if still online\n", old_max_deploy_rs_count);
            //mod:e
          }
          else if (1 == old_max_deploy_rs_count)
          {
             //mod bingo [Paxos rs management] 20170217:b
             printf("change paxos number [1->%d], please check alive servers and the service\n", args.rs_paxos_number);
             //mod:e
          }
          //add:e
          else
          {
              printf("change rootserver paxos number [%d->%d] over, Please do 'rs_paxos_num' to see if the new paxos number is OK.\n"
                     , old_max_deploy_rs_count, current_max_deploy_rs_count);
          }
        }
      }
      return ret;
    }
    //add:e
    //add chujiajia [Paxos rs_election] 20151225:b
    int do_change_ups_quorum_scale(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.ups_quorum_scale)
      {
        printf("invalid param, ups_quorum_scale=%d\n", args.ups_quorum_scale);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_ups_quorum_scale, new_quorum_num=%d...\n", args.ups_quorum_scale);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ups_quorum_scale)))
        {
          printf("failed to serialize quorum scale, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_UPS_QUORUM_SCALE, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          //add pangtianze [Paxos rs_election] 20161010:b
          int32_t current_ups_quorum_scale = 0;
          int32_t old_ups_quorum_scale = 0;
          int32_t alive_ups_count = 0;
          //add:e
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          //add pangtianze [Paxos rs_election] 20161010:b
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &current_ups_quorum_scale)))
          {
            TBSYS_LOG(WARN,"failed to deserialize current_max_deploy_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &old_ups_quorum_scale)))
          {
            TBSYS_LOG(WARN,"failed to deserialize old_max_deploy_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                              msgbuf.get_position(), &alive_ups_count)))
          {
            TBSYS_LOG(WARN,"failed to deserialize alive_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            switch (result_code.result_code_)
            {
              //mod add bingo [Paxos ups management] 20170217:b
              //case OB_INVALID_PAXOS_NUM:
              case OB_INVALID_QUORUM_SCALE:
                if(args.ups_quorum_scale == 2 && alive_ups_count == 1){
                  printf("receive new quorum scale:%d, but the second ups should be online first, alive_ups_count still is %d, err=%d.\n",
                       args.ups_quorum_scale,alive_ups_count,ret);
                }
                //mod add:e
                else if(args.ups_quorum_scale <= (alive_ups_count / 2) ||
                   args.ups_quorum_scale >= (alive_ups_count * 2) ||
                   args.ups_quorum_scale > OB_MAX_UPS_COUNT)
                {
                  printf("receive invalid new quorum scale:%d, it should be in (%d,%d), err=%d.\n",
                         args.ups_quorum_scale, alive_ups_count/2, alive_ups_count*2, ret);
                }
                else if (args.ups_quorum_scale != alive_ups_count && 0 != alive_ups_count)
                {
                  printf("new quorum scale must be equal to alive updateserver count, but new_quorum_scale=%d, ups_count=%d, err=%d.\n",
                         args.ups_quorum_scale, alive_ups_count, ret);
                }
                break;
              case OB_RS_NOT_EXIST:
                printf("some slave rootserver maybe not get new quorum scale, please check and do again!, err=%d\n", ret);
                break;
              default:
                printf("error occurred. err=%d\n", ret);
                break;
            }
          }
          //add:e
          //add bingo [Paxos ups management] 20170217:b
          else if (1 == args.ups_quorum_scale)
          {
            printf("change quorum scale [%d->1], slave updateserver should be offine soon, please check and kill them\n", old_ups_quorum_scale);
          }
          else if (1 == old_ups_quorum_scale)
          {
             printf("change quorum scale [1->%d], please check alive servers and the service\n", args.ups_quorum_scale);
          }
          //add:e
          else
          {
              printf("change updateserver quorum scale [%d->%d] over, Please do 'ups_quorum_scale' to see if the new quorum scale is OK.\n"
                     ,old_ups_quorum_scale, current_ups_quorum_scale);
          }
        }
      }
      return ret;
    }
    //add:e

    //add lbzhong [Paxos Cluster.Balance] 201607020:b
    int do_dump_balancer_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      printf("dump balancer info, table_id=%ld...\n", args.table_id);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize table_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_BALANCER_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err = %d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to dump balancer info. err=%d\n", ret);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_get_replica_num(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_REPLICA_NUM, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to get replica num. err=%d\n", err);
        }
        else
        {
          for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &replicas_nums[cluster_id])))
            {
              printf("failed to deserialize replica num\n");
              break;
            }
          }
        }
        if(OB_SUCCESS == err)
        {
          for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if(replicas_nums[cluster_id] >= 0)
            {
              printf("cluster[%d]=%d\n", cluster_id, replicas_nums[cluster_id]);
            }
          }
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos rs_election] 20161010
    int do_get_all_priority(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_MASTER_GET_ALL_PRIORITY, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        int32_t server_count = 0;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to get replica num. err=%d\n", err);
        }
        else if(OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &server_count)))
        {
          printf("failed to deserialize server count\n");
        }
        else
        {
          ObServer servers[server_count];
          for (int32_t i = 0; i < server_count; i++)
          {
            if (OB_SUCCESS != (err = servers[i].deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
            {
               TBSYS_LOG(WARN, "failed to deserialize priority. err=%d", err);
               break;
            }
          }
          if(OB_SUCCESS == err)
          {
            int32_t priorities[server_count];
            for (int32_t j = 0; j < server_count; j++)
            {
              if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &priorities[j])))
              {
                 TBSYS_LOG(WARN, "failed to deserialize priority. err=%d", err);
              }
            }
            if(OB_SUCCESS == err)
            {
              for (int32_t k = 0; k < server_count; k++)
              {
                printf("rs: [%s, %d]\n", servers[k].to_cstring(), priorities[k]);
              }
            }
          }
        }
      }
      return err;
    }
    //add:e
	 //add bingo [Paxos Cluster.Balance] 20161020:b
    int do_set_master_cluster_id(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      if (args.new_master_cluster_id < 1 || args.new_master_cluster_id > OB_MAX_CLUSTER_ID)
      {
        printf("ERROR: new_master_cluster_id=%d, (1 <= cluster_id <= OB_MAX_CLUSTER_ID).\n", args.new_master_cluster_id);
        err = OB_ERROR;
      }
      else
      {
        printf("set new master_cluster_id: new_master_cluster_id=%d\n", args.new_master_cluster_id);

        ObDataBuffer msgbuf(buf, sizeof (buf));
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.new_master_cluster_id)))
        {
          printf("failed to serialize new master_cluster_id, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = client.send_recv(OB_SET_MASTER_CLUSTER_ID, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", err);
          }
          else if(OB_NOT_MASTER == (err = result_code.result_code_))
          {
            printf("the command is sent to the slave rootserver, please check command and try again. err=%d.\n", err);
          }
          else if (OB_CLUSTER_ID_ERROR == (err = result_code.result_code_))
          {
            printf("failed to set master_cluser_id, invalid cluster_id. err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to set master_cluser_id. err=%d\n", err);
          }
          else
          {
            printf("OKay\n");
          }
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos rs_election] 20170301:b
    int do_get_rs_leader(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_RS_LEADER, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        common::ObServer leader;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if(OB_SUCCESS != (err = leader.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize leader, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          if(OB_RS_DOING_ELECTION == err)
          {
            printf("rs is doing election, please wait and do again later\n");
          }
          else
          {
            printf("failed to get rs leader. err=%d\n", err);
          }
        }
        else
        {
          printf("rs election done, leader rootserver[%s]\n", leader.to_cstring());
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos sstable info to rs log] 20170614:b
    int do_dump_sstable_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      printf("dump sstable info...\n");
      if (OB_SUCCESS != (ret = client.send_recv(OB_DUMP_SSTABLE_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err = %d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to dump sstable info. err=%d\n", ret);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
    //add:e

    //add bingo [Paxos table replica] 20170620:b
    int do_get_table_replica_num(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));

      printf("get table replica num, table_id=%ld...\n", args.table_id);

      if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize table_id, err=%d\n", err);
      }
      else if (OB_SUCCESS != (err = client.send_recv(OB_GET_TABLE_REPLICA_NUM, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to get replica num. err=%d\n", err);
        }
        else
        {
          for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &replicas_nums[cluster_id])))
            {
              printf("failed to deserialize replica num\n");
              break;
            }
          }
        }
        if(OB_SUCCESS == err)
        {
          for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if(replicas_nums[cluster_id] >= 0)
            {
              printf("cluster[%d]=%d\n", cluster_id, replicas_nums[cluster_id]);
            }
          }
        }
      }
      return err;
    }
    //add:e
  } // end namespace rootserver
}
