/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.h is for what ...
 *
 *  Version: $Id: cs_admin.h 2010年12月03日 13时48分29秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include <set>
#include <map>
#include <string>
#include "client_rpc.h"
//#include "base_client.h"
#include "common/ob_base_client.h"
#include "common/ob_server.h"
#include "common/ob_rowkey.h"

using namespace oceanbase;

enum
{
  CMD_MIN,
  CMD_DUMP_TABLET = 1,
  CMD_DUMP_SERVER_STATS = 2,
  CMD_DUMP_CLUSTER_STATS = 3,
  CMD_MANUAL_MERGE = 4,
  CMD_MANUAL_DROP_TABLET = 5,
  CMD_MANUAL_GC = 6,
  CMD_RS_GET_TABLET_INFO = 7,
  CMD_SCAN_ROOT_TABLE = 8,
  CMD_MIGRATE_TABLET = 9,
  CMD_DUMP_APP_STATS = 10,
  CMD_CHECK_MERGE_PROCESS = 11,
  CMD_CHANGE_LOG_LEVEL = 12,
  CMD_STOP_SERVER = 13,
  CMD_RESTART_SERVER = 14,
  CMD_SHOW_PARAM = 15,
  CMD_CREATE_TABLET = 16,
  CMD_DELETE_TABLET = 17,
  CMD_EXECUTE_SQL = 18,
  CMD_SYNC_ALL_IMAGES = 19,
  CMD_DELETE_TABLE = 20,
  CMD_LOAD_SSTABLES = 21,
  CMD_DUMP_SSTABLE_DIST = 22,
  CMD_GET_BLOOM_FILTER = 23,
  CMD_INSTALL_DISK = 29,
  CMD_UNINSTALL_DISK = 30,
  CMD_SHOW_DISK = 31,
  CMD_FETCH_BYPASS_TABLE_ID = 34,
  CMD_REQUEST_REPORT_TABLET = 35,
  CMD_MAX,
};

enum
{
  INT8_T = 0,
  INT16_T,
  INT32_T,
  INT64_T,
  VARCHAR,
  DATETIME_1 = 5,
  DATETIME_2,
  DATETIME_3,
  DATETIME_4,
  DATETIME_5,
};

struct RowkeyItem
{
  int32_t item_type_;
  int32_t item_size_;
};

struct RowkeySplit
{
  static const int64_t MAX_ROWKEY_SPLIT = 32;
  RowkeyItem item_[MAX_ROWKEY_SPLIT];
  int64_t item_size_;
  int64_t rowkey_size_;
};

struct ScanRootTableArg
{
  const char* app_name_;
  const char* table_name_;
  uint64_t table_id_;
  RowkeySplit rowkey_split_;
  const char* output_path_;
  char key_delim_;
  char column_delim_;
};

class GFactory
{
  public:
    GFactory();
    ~GFactory();
    int initialize(const oceanbase::common::ObServer& chunk_server, const int64_t timeout);
    int stop();
    static GFactory& get_instance() { return instance_; }
    inline ObClientRpcStub& get_rpc_stub() { return rpc_stub_; }
    inline oceanbase::common::ObBaseClient& get_base_client() { return client_; }
    inline const std::map<std::string, int> & get_cmd_map() const { return cmd_map_; }
  private:
    void init_cmd_map();
    static GFactory instance_;
    ObClientRpcStub rpc_stub_;
    oceanbase::common::ObBaseClient client_;
    std::map<std::string, int> cmd_map_;
    int64_t timeout_;
};


struct Param
{
  oceanbase::common::ObServer chunk_server;
  int type;
};

struct SstableInfo{
  SstableInfo():version_(0),occupy_size_(0),
    record_count_(0){}
  ~SstableInfo()
  {
  }

  common::ObServer server_;
  int64_t version_;
  int64_t occupy_size_;
  int64_t record_count_;
  common::ObString path_;
};

class SstableDistDumper
{

  public:
    static const float REPLACE_RATE = 0.5; // random percent to update repeated sstable info of sstable_dist
    struct RangeComp
    {
      inline bool operator() (const common::ObNewRange& left, const common::ObNewRange& right) const
      {
        bool ret = false;
        if (left.table_id_ == right.table_id_)
        {
          ret = left.compare_with_endkey2(right) < 0 ? true: false;
        }
        else
        {
          ret = left.table_id_ < right.table_id_;
        }
        return ret;
      }
    };
    typedef std::map<common::ObNewRange, SstableInfo, RangeComp> SstableDist;

  public:
    SstableDistDumper(const common::ObClientManager *client, const common::ObServer& rs_server, const char* filename,
        const bool write_range_info = true):
      table_version_(0), mod_(), arena_(common::ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      client_(client), rs_server_(rs_server), rpc_buffer_(common::OB_MAX_PACKET_LENGTH),
      filename_(filename), fp_(NULL), write_range_info_(write_range_info)
  { srandom(static_cast<unsigned int>(time(NULL)));}
    ~SstableDistDumper(){}
    int add_table(const char* table_name, const uint64_t table_id);
    int dump();

  private:
    int insert_range_to_sstable_dist(const uint64_t table_id, const int64_t row_count,
        common::ObRowkey& prev_row_key, common::ObRowkey& cur_row_key);
    int load_root_table(common::ObString& table_name, const uint64_t table_id);
    int check_sstable_dist();
    inline int check_sstable_info(const SstableInfo& info);
    int write_sstable_dist();
    int update_sstable_dist(const common::ObServer& server, std::list<std::pair<common::ObNewRange, common::ObString> >& sstable_list);
    int fetch_schema(common::ObSchemaManagerV2& schema);

  private:
    SstableDist sstable_dist_;
    int64_t table_version_;
    std::set<common::ObServer> cs_list_;
    common::ModulePageAllocator mod_;
    common::ModuleArena arena_;
    const common::ObClientManager *client_;
    const common::ObServer rs_server_;
    common::ThreadSpecificBuffer rpc_buffer_;
    const char* filename_;
    FILE* fp_;
    bool write_range_info_;
};

inline int SstableDistDumper::check_sstable_info(const SstableInfo& info)
{
  int ret = common::OB_SUCCESS;
  if (info.path_.ptr() == NULL)
  {
    TBSYS_LOG(ERROR, "miss path of range");
    ret = common::OB_ERROR;
  }
  else if (info.server_.get_ipv4() == 0 || info.server_.get_port() == 0)
  {
    TBSYS_LOG(ERROR, "unknown server of range");
    ret = common::OB_ERROR;
  }
  return ret;
}
