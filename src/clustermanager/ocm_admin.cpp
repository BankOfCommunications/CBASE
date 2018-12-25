/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ocm_admin.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */
 
#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include "common/ob_base_client.h"
#include "common/ob_result.h"
#include "ob_server_ext.h"
#include "ob_ocm_instance.h"

using namespace oceanbase;
using namespace oceanbase::clustermanager;
using namespace oceanbase::common;

static const int64_t TIMEOUT_US = 2000000L;
const char* DEFAULT_SERVER_ADDR = "127.0.0.1";
const int32_t DEFAULT_SERVER_PORT = 3001;
void print_usage()
{
  fprintf(stdout, "\n");
  fprintf(stdout, "ocm_admin [OPTION]\n");
  fprintf(stdout, "  -a|--server_addr server address, the default is `127.0.0.1'\n"); 
  fprintf(stdout, "  -p|--port server port, the default is 3001\n");
  fprintf(stdout, "  -c|--cmd_type command type[set_instance_role][get_instance_role][set_ocm_if_exist]\n");
  fprintf(stdout, "  -n|--app_name application name, must be set when cmd_type are [set_instance_role] or [get_instance_role]\n");
  fprintf(stdout, "  -i|--instance_name must be set when cmd_typd are [set_instance_role] or [get_instance_role]\n");
  fprintf(stdout, "  -e|--ocm if exist, yes or no, must be set when cmd_typd is [set_ocm_if_exist]\n");
  fprintf(stdout, "  -o|--ocm_addr, must be set when cmd_type is [set_ocm_if_exist]\n");
  fprintf(stdout, "  -m|--master or slave, must be set when cmd_type is [set_instance_role]\n\n");
  fprintf(stdout, "  Example: ocm_admin -a 127.0.0.1 -p 3001 -c set_instance_role -n app_name -i instance_name -m master\n");
  fprintf(stdout, "\n");  
} 

struct CmdLineParam
{
  const char *serv_addr;
  int32_t serv_port;
  char *cmd_type;
  char *app_name;
  char *instance_name;
  bool ocm_if_exist;
  char *ocm_addr;
  int32_t status;
  CmdLineParam()
  {
    serv_addr = DEFAULT_SERVER_ADDR;
    serv_port = DEFAULT_SERVER_PORT;
    cmd_type = NULL;
    app_name = NULL;
    instance_name = NULL;
    ocm_if_exist = true;
    ocm_addr = NULL;
    status = -1;
  }
};

void parse_cmd_line(int argc, char **argv, CmdLineParam &clp)
{
  int opt = 0;
  const char* opt_string = "a:p:c:n:i:e:o:m:h";
  struct option longopts[] = 
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"cmd_type", 1, NULL, 'c'},
    {"app_name", 1, NULL, 'n'},
    {"instance_name", 1, NULL, 'i'},
    {"ocm_if_exist", 1, NULL, 'e'},
    {"ocm_addr", 1, NULL, 'o'},
    {"status", 1, NULL, 'm'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
	{
	  case 'a':
	    clp.serv_addr = optarg;
		break;
	  case 'p':
        clp.serv_port = atoi(optarg);
        break;
      case 'c':		
	    clp.cmd_type = optarg;
		break;
	  case 'n':
        clp.app_name = optarg;
        break;
      case 'i':
      	clp.instance_name = optarg;
        break;
       case 'e':
          if(0 == strcmp(optarg, "yes"))
		  {
		    clp.ocm_if_exist = true;
		  }
		  else if(0 == strcmp(optarg, "no"))
		  { 
		    clp.ocm_if_exist = false;
      }
          else
          {
            TBSYS_LOG(WARN, "invalid argument. eg: -e [yes][no]");
            print_usage();
            exit(1);
          }
break;
case 'o':
clp.ocm_addr = optarg;
TBSYS_LOG(INFO,"ocm addr is %s", clp.ocm_addr);
break;
case 'm':
if(0 == strcmp(optarg, "master"))
{
  clp.status = 0;
}
else if(0 == strcmp(optarg, "slave"))
{
  clp.status = 1;
}
else
{
  TBSYS_LOG(WARN, "invalid argument. eg: -m [master][slave]");
  print_usage();
  exit(1);
}
break;		  
case 'h':
default:
print_usage();
exit(1);		
	}
  }
  
if(NULL == clp.cmd_type
   || (NULL == clp.app_name && (0 == strcmp(clp.cmd_type, "get_instance_role") || 0 == strcmp(clp.cmd_type, "set_instance_role")))
   || (NULL == clp.instance_name && (0 == strcmp(clp.cmd_type, "get_instance_role") || 0 == strcmp(clp.cmd_type, "set_instance_role"))) 
   || (-1 == clp.status && 0 == strcmp(clp.cmd_type, "set_instance_role"))
   || (-1 == clp.ocm_if_exist && 0 == strcmp(clp.cmd_type, "set_ocm_if_exist"))
   || (NULL  == clp.ocm_addr && 0 == strcmp(clp.cmd_type, "set_ocm_if_exist")))
{
  print_usage();
  exit(-1);
}   
}

// set the instance role
int set_instance_role(ObBaseClient& client, CmdLineParam clp)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO,"set instance role:\n");
  static const int32_t MY_VERSION = 1;
  const int64_t buf_size = sizeof(ObPacket) + 64 + OB_MAX_APP_NAME_LENGTH + OB_MAX_INSTANCE_NAME_LENGTH;
  char buf[buf_size];
  ObDataBuffer msgbuf(buf, buf_size);
  ObString app_name;
  app_name.assign_ptr(clp.app_name, static_cast<int32_t>(strlen(clp.app_name)));
  ObString instance_name;
  instance_name.assign_ptr(clp.instance_name, static_cast<int32_t>(strlen(clp.instance_name)));
  
  if(OB_SUCCESS != (err = app_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(INFO,"warn, serialize app_name fail. err = %d\n", err);
  }  
  if(OB_SUCCESS == err)
  {
    if(OB_SUCCESS != (err = instance_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
	{
	  TBSYS_LOG(INFO,"warn, serialize instance_name fail. err = %d\n", err);
	}
  }
  if(OB_SUCCESS == err)
  {
    if(OB_SUCCESS != 
	    (err = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), clp.status)))
	{
	  TBSYS_LOG(INFO,"warn,  serialize status fail. err = %d\n", err);
	}
  }
  
  if(OB_SUCCESS == err)
  {
    err = client.send_recv(OB_SET_INSTANCE_ROLE, MY_VERSION, TIMEOUT_US, msgbuf);
  }
  
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(INFO,"fail to send request\n");
  }
  
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
	msgbuf.get_position() = 0;
	err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position());
	if(err != OB_SUCCESS)
	{
	  TBSYS_LOG(INFO,"deserialze result code fail. err = %d\n", err);
	}
	else
	{
	  if(result_code.result_code_ != OB_SUCCESS)
	  {
	    TBSYS_LOG(INFO,"fail to set instance role, result_code =%d\n", result_code.result_code_);
	  }
	  else
	  {
	    TBSYS_LOG(INFO,"set instance role succ! \n");
	  }
	}
  }
  return err;
}

int get_instance_role(ObBaseClient& client, CmdLineParam clp)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO,"set instance role:\n");
  static const int32_t MY_VERSION = 1;
  const int64_t buf_size = sizeof(ObPacket) + OB_MAX_APP_NAME_LENGTH + OB_MAX_INSTANCE_NAME_LENGTH;
  char buf[buf_size];
  ObDataBuffer msgbuf(buf, buf_size);
  ObString app_name;
  app_name.assign_ptr(clp.app_name, static_cast<int32_t>(strlen(clp.app_name)));
  TBSYS_LOG(INFO,"app_name=%s\n", app_name.ptr());
  ObString instance_name;
  instance_name.assign_ptr(clp.instance_name, static_cast<int32_t>(strlen(clp.instance_name)));
  TBSYS_LOG(INFO,"instance name = %s\n", instance_name.ptr());
  if(OB_SUCCESS != (err = app_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(INFO,"warn, serialize app_name fail. err = %d\n", err);
  }  
  if(OB_SUCCESS == err)
  {
    if(OB_SUCCESS != (err = instance_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
	{
	  TBSYS_LOG(INFO,"warn, serialize instance_name fail. err = %d\n", err);
	}
  }
  if(OB_SUCCESS == err)
  {
    err = client.send_recv(OB_GET_INSTANCE_ROLE, MY_VERSION, TIMEOUT_US, msgbuf);
  }
  
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(INFO,"warn, fail to send request\n");
  }
  
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
	Status status;
	msgbuf.get_position() = 0;
	err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position());
	if(err != OB_SUCCESS)
	{
	  TBSYS_LOG(INFO,"warn, fail to deserialize result code. err=%d\n", err);
	}
	else
	{
	  if(OB_SUCCESS != result_code.result_code_)
	  {
	    TBSYS_LOG(INFO,"warn,fail to get instance role. err = %d\n", result_code.result_code_);
	  }
    else
    {
      err = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(),reinterpret_cast<int64_t*>(&status));
      if(OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO,"get instance role succ! role =%d\n", status);
      }
      else
      {
        TBSYS_LOG(INFO,"warn, deserialize status fail.err=%d\n", err);
      }
    }
  }
  }
  return err;
}

int set_ocm_if_exist(ObBaseClient &client, CmdLineParam clp)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO,"set ocm exist:\n");
  static const int32_t MY_VERSION = 1;
  const int64_t buf_size = sizeof(ObPacket) + OB_IP_STR_BUFF + 64;
  char buf[buf_size];
  ObDataBuffer msgbuf(buf, buf_size);
  ObString ip_addr;
  ip_addr.assign_ptr(clp.ocm_addr, static_cast<int32_t>(strlen(clp.ocm_addr)));
  TBSYS_LOG(INFO,"ip_addr=%s\n", ip_addr.ptr());
  TBSYS_LOG(INFO,"ocm if exist =%d\n", clp.ocm_if_exist);
  err = ip_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position());
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(INFO,"warn, fail to serialize ip addr, err = %d\n", err);
  }
  if(OB_SUCCESS == err)
  {
    err = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), clp.ocm_if_exist);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(INFO,"warn, fail to encode ocm_if_exist. err=%d\n", err);
    }
  }
  if(OB_SUCCESS == err)
  {
    err = client.send_recv(OB_OCM_CHANGE_STATUS, MY_VERSION, TIMEOUT_US, msgbuf);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(INFO,"warn, fail to send request. err=%d\n", err);
    }
  }
  msgbuf.get_position() = 0;
  ObResultCode result_code;
  if(OB_SUCCESS == err)
  {
    err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(INFO,"warn ,fail to deserialize result_code,err=%d\n", err);
    }
    else
    {
      if(OB_SUCCESS != result_code.result_code_)
      {
        TBSYS_LOG(INFO,"warn, set ocm exist fail. err = %d\n", result_code.result_code_);
      }
      else
      {
        TBSYS_LOG(INFO,"set ocm exist succ!\n");
      }
    }
  }
  return err;
}
int main(int argc, char ** argv)
{
  int ret = OB_SUCCESS;
  //TBSYS_LOGGER.setFileName("ocm_admin.log", true);
  //TBSYS_LOG(INFO, "ocm_admin start!\n");
  TBSYS_LOG(INFO, "ocm_admin start");
  ob_init_memory_pool();

  CmdLineParam clp;
  parse_cmd_line(argc, argv, clp);

  ObServer server(ObServer::IPV4, clp.serv_addr, clp.serv_port);
  ObBaseClient *p_client = new ObBaseClient();
  ObBaseClient &client = *p_client;
  if(OB_SUCCESS != (ret = client.initialize(server)))
  {
    TBSYS_LOG(INFO,"fail to init client. ret = %d\n", ret);
  }
  else
  {
    if(0 == strcmp("set_instance_role", clp.cmd_type))
    {
      ret = set_instance_role(client, clp);
    }
    else if(0 == strcmp("get_instance_role", clp.cmd_type))
    {
      ret = get_instance_role(client, clp);
    }
    else if(0 == strcmp("set_ocm_if_exist", clp.cmd_type))
    {
      ret = set_ocm_if_exist(client, clp);
      TBSYS_LOG(INFO,"set ocm if exist ,err =%d\n", ret);
    }
  }
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(INFO,"do process fail. ret=%d\n", ret);
  }
  else
  {
    TBSYS_LOG(INFO,"do process succ!\n");
  }
  //client.destroy();
	return 0;
}













