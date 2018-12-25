/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.cpp is for what ...
 *
 *  Version: $Id: cs_admin.cpp 2010年12月03日 13时48分14秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include "backup_client.h"
#include <vector>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"
#include "common/utility.h"
#include "common/ob_general_rpc_stub.h"
#include "common/file_directory_utils.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_server_stats.h"
#include "ob_cluster_stats.h"
#include "common_func.h"


using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;
using namespace oceanbase::tools;

static char g_config_file_name[OB_MAX_FILE_NAME_LENGTH];

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
  cmd_map_["dump_tablet_image"] = CMD_DUMP_TABLET;
  cmd_map_["check_backup_process"] = CMD_CHECK_BACKUP_PROCESS;
  cmd_map_["show_param"] = CMD_SHOW_PARAM;
  cmd_map_["sync_images"] = CMD_SYNC_ALL_IMAGES;
  cmd_map_["install_disk"] = CMD_INSTALL_DISK;
  cmd_map_["uninstall_disk"] = CMD_UNINSTALL_DISK;
  cmd_map_["show_disk"] = CMD_SHOW_DISK;
  cmd_map_["stop_server"] = CMD_STOP_SERVER;
  cmd_map_["backup_database"] = CMD_BACKUP_DATABASE;
  cmd_map_["backup_table"] = CMD_BACKUP_TABLE;
  cmd_map_["backup_abort"] = CMD_BACKUP_ABORT;
}

int GFactory::initialize(const ObServer& remote_server, const int64_t timeout)
{
  common::ob_init_memory_pool();
  common::ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  init_cmd_map();
  init_obj_type_map_();

  int ret = client_.initialize(remote_server);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize client object failed, ret:%d", ret);
    return ret;
  }

  ret = rpc_stub_.initialize(remote_server, &client_.get_client_mgr(), timeout);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize rpc stub failed, ret:%d", ret);
  }
  timeout_ = timeout;
  return ret;
}

int GFactory::stop()
{
  client_.destroy();
  return OB_SUCCESS;
}

int parse_command(const char *cmdlist, vector<string> &param)
{
    int cmd = CMD_MIN;
    const int CMD_MAX_LEN = 1024;
    char key_buf[CMD_MAX_LEN];
    snprintf(key_buf, CMD_MAX_LEN, "%s", cmdlist);
    char *key = key_buf;
    char *token = NULL;
    while(*key == ' ') key++; // 去掉前空格
    token = key + strlen(key);
    while(*(token-1) == ' ' || *(token-1) == '\n' || *(token-1) == '\r') token --;
    *token = '\0';
    if (key[0] == '\0') {
        return cmd;
    }
    token = strchr(key, ' ');
    if (token != NULL) {
        *token = '\0';
    }
    const map<string, int> & cmd_map = GFactory::get_instance().get_cmd_map();
    map<string, int>::const_iterator it = cmd_map.find(key);
    if (it == cmd_map.end())
    {
        return CMD_MIN;
    }
    else
    {
        cmd = it->second;
    }

    if (token != NULL)
    {
        token ++;
        key = token;
    }
    else
    {
        key = NULL;
    }
    //分解
    param.clear();
    while((token = strsep(&key, " ")) != NULL) {
        if (token[0] == '\0') continue;
        param.push_back(token);
    }
    return cmd;
}

int check_backup_privilege(const char * username_str, const char * passwd_str, bool & result)
{
  int ret = OB_SUCCESS;

  ObString username(0, static_cast<common::ObString::obstr_size_t>(strlen(username_str)), username_str);

  ObString passwd(0, static_cast<common::ObString::obstr_size_t>(strlen(passwd_str)), passwd_str);

  fprintf(stderr, "check privilege username[%.*s]\n",username.length(),username.ptr());

  ret = GFactory::get_instance().get_rpc_stub().check_backup_privilege(username, passwd, result);
  return ret;
}

int dump_tablet_image(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1)
  {
    fprintf(stderr, "dump_tablet_image: input param error\n");
    fprintf(stderr, "dump_tablet_image disk_no \n");
    fprintf(stderr, "dump_tablet_image disk_no_1, disk_no_2, ... \n");
    fprintf(stderr, "dump_tablet_image disk_no_min~disk_no_max \n");
    return OB_ERROR;
  }

  const string &disk_list = param[0];
  int32_t disk_no_size = 64;
  int32_t disk_no_array[disk_no_size];
  ret = parse_number_range(disk_list.c_str(), disk_no_array, disk_no_size);
  if (ret)
  {
    fprintf(stderr, "parse disk_list(%s) parameter failure\n", disk_list.c_str());
    return ret;
  }

  fprintf(stderr, "dump disk count=%d\n", disk_no_size);

  int64_t pos = 0;
  FileInfoCache cache;
  cache.init(10);
  ObMultiVersionTabletImage image_obj(cache);
  for (int i = 0; i < disk_no_size; ++i)
  {
    for (int idx = 0; idx < 2; ++idx)
    {
      ObString image_buf;
      ret = GFactory::get_instance().get_rpc_stub().cs_dump_tablet_image(idx, disk_no_array[i], image_buf);
      if (OB_SUCCESS != ret)
      {
        //fprintf(stderr, "disk = %d has no tablets \n ", disk_no_array[i]);
      }
      else
      {
        pos = 0;
        ret = image_obj.deserialize(disk_no_array[i], image_buf.ptr(), image_buf.length(), pos);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr, "deserialize tablet image failed\n");
        }
        else
        {
          image_obj.upgrade_service();
        }
      }
    }
  }

  dump_multi_version_tablet_image(image_obj, false);
  return ret;
}

int show_disk(const vector<string> &param)
{
  int ret = 0;

  UNUSED(param);
  int32_t disk_no[common::OB_MAX_DISK_NUMBER];
  bool avail[common::OB_MAX_ROW_KEY_LENGTH];
  int32_t disk_num = 0;

  ret = GFactory::get_instance().get_rpc_stub().cs_show_disk(disk_no, avail, disk_num);
  
  if (OB_SUCCESS == ret)
  {
    fprintf(stderr, "show disk succ\n");
    for(int32_t i = 0; i < disk_num; i++)   
    {
      fprintf(stderr, "disk:%02d status:%s\n", disk_no[i], avail[i] ? "OK" : "error");
    }
  }
  else
  {
    fprintf(stderr, "show disk failed\n");
  }

  return ret;
}

int uninstall_disk(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1)
  {
    fprintf(stderr, "uninstall_disk disk_no \n");
    return OB_ERROR;
  }

  const string &disk_list = param[0];
  int32_t disk_no_size = 1;
  int32_t disk_no = -1;
  ret = parse_number_range(disk_list.c_str(), &disk_no, disk_no_size);
  if (ret)
  {
    fprintf(stderr, "parse disk_list(%s) parameter failure\n", disk_list.c_str());
    return ret;
  }

  if(disk_no < 0)
  {
    fprintf(stderr, "disk_no:%d must > 0\n", disk_no);
    return OB_ERROR;
  }

  ret = GFactory::get_instance().get_rpc_stub().cs_uninstall_disk(disk_no);
  
  if(OB_SUCCESS == ret)
  {
    fprintf(stderr, "uninstall_disk:%d Okay!\n", disk_no);
  }
  else
  {
    fprintf(stderr, "uninstall_disk:%d failed, ret:%d\n", disk_no, ret);
  }

  return ret;
}

int install_disk(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1)
  {
    fprintf(stderr, "install_disk mount_path \n");
    return OB_ERROR;
  }

  const char* mount_path_str = param[0].c_str();
  int32_t disk_no = -1;
  ObString mount_path(static_cast<common::ObString::obstr_size_t>(strlen(mount_path_str)),
      static_cast<common::ObString::obstr_size_t>(strlen(mount_path_str)), mount_path_str);

  

  fprintf(stderr, "path:%s", to_cstring(mount_path));
  ret = GFactory::get_instance().get_rpc_stub().cs_install_disk(mount_path, disk_no);
  
  if(OB_SUCCESS == ret)
  {
    fprintf(stderr, "install_disk:%s mount to disk_no:%d Okay!\n", mount_path_str, disk_no);
  }
  else
  {
    fprintf(stderr, "install_disk:%s failed, ret:%d\n", mount_path_str, ret);
  }

  return ret;
}

int check_backup_process(const vector<string> &param)
{

  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().check_backup_process();
  return ret;
}



int show_param(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().show_param();
  return ret;
}

int sync_all_tablet_images(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().sync_all_tablet_images();
  return ret;
}


int backup_database(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  int32_t backup_mode_id = -1;
  const char* backup_mode = param[0].c_str();
  if (strlen(backup_mode) == 0)
  {
    backup_mode_id = 0;
  }
  else if (0 == strcmp(backup_mode ,"FULL"))
  {
    backup_mode_id = 0;
  }
  else if (0 == strcmp (backup_mode,"STATIC"))
  {
    backup_mode_id = 1;
  }
  else if (0 == strcmp(backup_mode,"INCREMENTAL"))
  {
    backup_mode_id = 2;
  }
  TBSYS_LOG(INFO,"backup database [%s], mode [%d]", backup_mode, backup_mode_id);
  ret = GFactory::get_instance().get_rpc_stub().backup_database(backup_mode_id);
  return ret;
}

int backup_table(const vector<string> &param)
{

  int ret = 0;
  const char* db_name_str = param[0].c_str();
  ObString db_name;
  int32_t backup_mode_id = 1; //only support static backup for table;
  if (0 == strcmp(db_name_str,"DEFAULT"))
  {
    db_name= ObString::make_string("");
  }
  else
  {
    db_name.assign(const_cast<char*>(db_name_str),static_cast<common::ObString::obstr_size_t>(strlen(db_name_str)));
  }
  TBSYS_LOG(INFO,"db_name [%.*s]", db_name.length(),db_name.ptr());

  const char* table_name_str = param[1].c_str();
  ObString table_name(static_cast<common::ObString::obstr_size_t>(strlen(table_name_str)),
                   static_cast<common::ObString::obstr_size_t>(strlen(table_name_str)), table_name_str);
  TBSYS_LOG(INFO,"table_name [%.*s]", table_name.length(),table_name.ptr());
  ret = GFactory::get_instance().get_rpc_stub().backup_table(backup_mode_id, db_name, table_name);
  return ret;
}

int backup_abort(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().backup_abort();
  return ret;
}

int do_work(const int cmd, const vector<string> &param)
{
  int ret = OB_SUCCESS;
  switch (cmd)
  {
    case CMD_DUMP_TABLET:
      ret = dump_tablet_image(param);
      break;
    case CMD_CHECK_BACKUP_PROCESS:
      ret = check_backup_process(param);
      break;
    case CMD_SHOW_PARAM:
      ret = show_param(param);
      break;
    case CMD_SYNC_ALL_IMAGES:
      ret = sync_all_tablet_images(param);
      break;
      break;
    case CMD_INSTALL_DISK:
      ret = install_disk(param);
      break;
    case CMD_UNINSTALL_DISK:
      ret = uninstall_disk(param);
      break;
    case CMD_SHOW_DISK:
      ret = show_disk(param);
      break;
    case CMD_STOP_SERVER:
      ret = GFactory::get_instance().get_rpc_stub().stop_server();
      break;
    case CMD_BACKUP_DATABASE:
      ret = backup_database(param);
      break;
    case CMD_BACKUP_TABLE:
      ret = backup_table(param);
      break;
    case CMD_BACKUP_ABORT:
      ret = backup_abort(param);
      break;
    default:
      fprintf(stderr, "unsupported command : %d\n", cmd);
      ret = OB_ERROR;
      break;
  }
  return ret;
}

void usage()
{
  fprintf(stderr, "usage: ./backup_client -t timeout_second -s backupserver_ip -P backupserver_port -u username -p password -i \"command args\"\n"
          "command: \n\tdump_tablet_image \n\t"
          "check_backup_process\n\t"
          "show_param\n\t"
          "sync_images\n\t"
          "install_disk\n\t"
          "uninstall_disk\n\t"
          "show_disk\n\t"
          "stop_server\n\t"
          "backup_database\n\t"
          "backup_table\n\t"
          "backup_abort\n\n"
          "run command for detail.\n");
  fprintf(stderr, "    command args: \n");
  fprintf(stderr, "\tdump_tablet_image disk_no \n");
  fprintf(stderr, "\tinstall_disk mount_path\n");
  fprintf(stderr, "\tuninstall_disk disk_no \n");
  fprintf(stderr, "\tbackup_database db_name \n");
  fprintf(stderr, "\tbackup_database db_name table_name\n");
}

int main(const int argc, char *argv[])
{
  const char *addr = "127.0.0.1";
  const char *cmdstring = NULL;
  int32_t port = 2600;

  int ret = 0;
  int silent = 0;
  int64_t timeout = 0;

  char *username = NULL;
  char *passwd = NULL;

  ObServer backup_server;

  memset(g_config_file_name, OB_MAX_FILE_NAME_LENGTH, 0);

  while((ret = getopt(argc,argv,"s:u:P:p:i:f:qt:")) != -1)
  {
    switch(ret)
    {
      case 's':
        addr = optarg;
        break;
      case 'i':
        cmdstring = optarg;
        break;
      case 'u':
        username = optarg;
        break;
      case 'p':
        passwd = optarg;
        break;
      case 'P':
        port = atoi(optarg);
        break;
      case 'q':
        silent = 1;
        break;
      case 'f':
        strcpy(g_config_file_name, optarg);
        break;
      case 't':
        timeout = 1000000L * atoi(optarg);
        break;
      default:
        fprintf(stderr, "%s is not identified\n",optarg);
        usage();
        return ret;
        break;
    };
  }

  if (silent)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("DEBUG");
  }

  ret = backup_server.set_ipv4_addr(addr, port);
  if (!ret && strlen(g_config_file_name) <= 0)
  {
    fprintf(stderr, "backupserver addr invalid, addr=%s, port=%d\n", addr, port);
    usage();
    return ret;
  }
  ret = GFactory::get_instance().initialize(backup_server, timeout);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "initialize GFactory error, ret=%d\n", ret);
    return ret;
  }

  if (username != NULL && passwd != NULL)
  {
    vector<string> param;
    int cmd = parse_command(cmdstring, param);
    if (CMD_MIN != cmd)
    {
      //check privilege
      bool result = false;
      ret = check_backup_privilege(username,passwd,result);
      if (ret == OB_SUCCESS && result)
      {
        ret = do_work(cmd, param);
      }
    }
  }
  else
  {
    fprintf(stderr, "username or password is invalid \n");
    usage();
    return -1;
  }
  GFactory::get_instance().stop();

  return ret == OB_SUCCESS ? 0 : -1;
}
