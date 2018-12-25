/*
 *  (C) 2008-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         obsql.h is for what ...
 *
 *  Version: $Id: obsql.h 2010\xC4\xEA12\xD4\xC203\xC8\xD5 13\x{02b1}48\xB7\xD629\xC3\xEB tianguan Exp $
 *
 *  Authors:
 *     tianguan < tianguan@taobao.com >
 *        - some work details if you want
 */

#include "client_rpc.h"
#include "base_client.h"
#include "common_func.h"
#include <map>
#include <string>

enum
{
  CMD_MIN,

  /* manage commands */
  CMD_SHOW = 1,
  CMD_HELP,
  CMD_EXIT,
  CMD_SET,
  
  /* SQL statements */
  CMD_SELECT,
  CMD_INSERT,
  CMD_UPDATE,
  CMD_DELETE,

  /* command details */
  CMD_SHOW_STATS_ROOT,
  CMD_SHOW_STATS_CHUNK,
  CMD_SHOW_STATS_MERGE,
  CMD_SHOW_STATS_UPDATE,
  CMD_SHOW_SCHEMA,
  CMD_SHOW_TABLET,  

  CMD_MAX,
};	

#define STR_ROWKWY_FILE "./rowkey.ini"
#define STR_ROWKEY_COMPOSITION "rowkey_composition"
//#define SUB_TYPE(A) ROWKEY_##A

class GFactory
{
  public:
    GFactory();
    ~GFactory(); 
    int initialize(const oceanbase::common::ObServer& root_server);
    int start();
    int stop();
    int wait();
    static GFactory& get_instance() { return instance_; }
    inline ObClientServerStub& get_rpc_stub() { return rpc_stub_; }
    inline oceanbase::obsql::BaseClient& get_base_client() { return client_; }
    inline const std::map<std::string, int> & get_cmd_map() const { return cmd_map_; }
    inline std::map<std::string, std::vector<RowKeySubType> > & get_rowkey_map() { return rowkey_map_; }

  private:
    void init_cmd_map();
    void init_rowkey_map();
    static GFactory instance_;
    ObClientServerStub rpc_stub_;
    oceanbase::obsql::BaseClient client_;
    std::map<std::string, int> cmd_map_;
    std::map<std::string, std::vector<RowKeySubType> > rowkey_map_;
};


