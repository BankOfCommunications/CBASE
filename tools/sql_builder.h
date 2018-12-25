#ifndef SQL_BUILDER_H
#define SQL_BUILDER_H
#include "common/ob_repeated_log_reader.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "common/ob_define.h"
#include <string>
#include <vector>
#include <sstream>
#include "updateserver/ob_ups_table_mgr.h"
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;
using namespace std;


#define ARR_LEN(a) (sizeof(a)/sizeof(a[0]))

#define ID_TO_STR_FUNC(FUNC_NAME, ARRAY, TYPE) \
    static const char* FUNC_NAME(TYPE id) \
    { \
      static std::ostringstream osstream; \
      if (id < static_cast<TYPE>(ARR_LEN(ARRAY)) && 0 != ARRAY[id]) \
      { \
        return ARRAY[id]; \
      } \
      else \
      { \
        osstream.seekp(0); \
        osstream << "UNKNOWN" << id; \
        return osstream.str().c_str(); \
      } \
    }

class IDS
{
public:
  static void initialize()
  {
    init_log_command_str();
    init_action_flag_str();
    init_obj_type_str();
  }

  static void init_log_command_str()
  {
    memset(LogCommandStr, 0x00, sizeof(LogCommandStr));
    LogCommandStr[OB_LOG_SWITCH_LOG]            = "SWITCH_LOG";
    LogCommandStr[OB_LOG_CHECKPOINT]            = "CHECKPOINT";
    LogCommandStr[OB_LOG_NOP]                   = "NOP";
    LogCommandStr[OB_LOG_UPS_MUTATOR]           = "UPS_MUTATOR";
    LogCommandStr[OB_UPS_SWITCH_SCHEMA]         = "UPS_SWITCH_SCHEMA";
    LogCommandStr[OB_RT_SCHEMA_SYNC]            = "OB_RT_SCHEMA_SYNC";
    LogCommandStr[OB_RT_CS_REGIST]              = "OB_RT_CS_REGIST";
    LogCommandStr[OB_RT_MS_REGIST]              = "OB_RT_MS_REGIST";
    LogCommandStr[OB_RT_SERVER_DOWN]            = "OB_RT_SERVER_DOWN";
    LogCommandStr[OB_RT_CS_LOAD_REPORT]         = "OB_RT_CS_LOAD_REPORT";
    LogCommandStr[OB_RT_CS_MIGRATE_DONE]        = "OB_RT_CS_MIGRATE_DONE";
    LogCommandStr[OB_RT_CS_START_SWITCH_ROOT_TABLE]
        = "OB_RT_CS_START_SWITCH_ROOT_TABLE";
    LogCommandStr[OB_RT_START_REPORT]           = "OB_RT_START_REPORT";
    LogCommandStr[OB_RT_REPORT_TABLETS]         = "OB_RT_REPORT_TABLETS";
    LogCommandStr[OB_RT_ADD_NEW_TABLET]         = "OB_RT_ADD_NEW_TABLET";
    LogCommandStr[OB_RT_CREATE_TABLE_DONE]      = "OB_RT_CREATE_TABLE_DONE";
    LogCommandStr[OB_RT_BEGIN_BALANCE]          = "OB_RT_BEGIN_BALANCE";
    LogCommandStr[OB_RT_BALANCE_DONE]           = "OB_RT_BALANCE_DONE";
    LogCommandStr[OB_RT_US_MEM_FRZEEING]        = "OB_RT_US_MEM_FRZEEING";
    LogCommandStr[OB_RT_US_MEM_FROZEN]          = "OB_RT_US_MEM_FROZEN";
    LogCommandStr[OB_RT_CS_START_MERGEING]      = "OB_RT_CS_START_MERGEING";
    LogCommandStr[OB_RT_CS_MERGE_OVER]          = "OB_RT_CS_MERGE_OVER";
    LogCommandStr[OB_RT_CS_UNLOAD_DONE]         = "OB_RT_CS_UNLOAD_DONE";
    LogCommandStr[OB_RT_US_UNLOAD_DONE]         = "OB_RT_US_UNLOAD_DONE";
    LogCommandStr[OB_RT_DROP_CURRENT_BUILD]     = "OB_RT_DROP_CURRENT_BUILD";
    LogCommandStr[OB_RT_DROP_LAST_CS_DURING_MERGE]
        = "OB_RT_DROP_LAST_CS_DURING_MERGE";
    LogCommandStr[OB_RT_SYNC_FROZEN_VERSION]    = "OB_RT_SYNC_FROZEN_VERSION";
    LogCommandStr[OB_RT_SET_UPS_LIST]           = "OB_RT_SET_UPS_LIST";
    LogCommandStr[OB_RT_SET_CLIENT_CONFIG]      = "OB_RT_SET_CLIENT_CONFIG";
    LogCommandStr[OB_UPS_SWITCH_SCHEMA_MUTATOR]           = "OB_UPS_SWITCH_SCHEMA_MUTATOR";
    LogCommandStr[OB_UPS_SWITCH_SCHEMA_NEXT]           = "OB_UPS_SWITCH_SCHEMA_NEXT";
    LogCommandStr[OB_UPS_WRITE_SCHEMA_NEXT]           = "OB_UPS_WRITE_SCHEMA_NEXT";
  }

  static void init_action_flag_str()
  {
    memset(ActionFlagStr, 0x00, sizeof(ActionFlagStr));
    ActionFlagStr[ObActionFlag::OP_UPDATE]      = "UPDATE";
    ActionFlagStr[ObActionFlag::OP_INSERT]      = "INSERT";
    //ActionFlagStr[ObActionFlag::OP_TRUN_TAB]    = "TRUN_TABLE";
    ActionFlagStr[ObActionFlag::OP_DEL_ROW]     = "DEL_ROW";
  }

  static void init_obj_type_str()
  {
    memset(ObjTypeStr, 0x00, sizeof(ObjTypeStr));
    ObjTypeStr[ObNullType]                      = "NULL";
    ObjTypeStr[ObIntType]                       = "Int";
    ObjTypeStr[ObFloatType]                     = "Float";
    ObjTypeStr[ObDoubleType]                    = "Double";
    ObjTypeStr[ObDateTimeType]                  = "DateTime";
    ObjTypeStr[ObPreciseDateTimeType]           = "PreciseDateTime";
    ObjTypeStr[ObVarcharType]                   = "Varchar";
    ObjTypeStr[ObSeqType]                       = "Seq";
    ObjTypeStr[ObCreateTimeType]                = "CreateTime";
    ObjTypeStr[ObModifyTimeType]                = "ModifyTime";
    ObjTypeStr[ObExtendType]                    = "Extend";
  }

  ID_TO_STR_FUNC(str_log_cmd, LogCommandStr, int64_t)
  ID_TO_STR_FUNC(str_action_flag, ActionFlagStr, int64_t)
  ID_TO_STR_FUNC(str_obj_type, ObjTypeStr, int)

private:
  DISALLOW_COPY_AND_ASSIGN(IDS);

  static const char* LogCommandStr[1024];
  static const char* ActionFlagStr[1024];
  static const char* ObjTypeStr[ObMaxType + 1];
};



/**
 * @brief 通过日志对象恢复成为等价sql
 */
class SqlBuilder
{
private:
    const string obj_tostring(const ObObj &obj) const;
    const string rowkey_tostring(const ObRowkey &obj) const;
public:
    SqlBuilder();

    int ups_mutator_to_sql(uint64_t seq, ObUpsMutator &mutator, vector<string> &vector, CommonSchemaManagerWrapper *schema_manager);
};

#endif // SQL_BUILDER_H
