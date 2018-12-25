////===================================================================
 //
 // ob_log_partitioner.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_PARTITIONER_H_
#define  OCEANBASE_LIBOBLOG_PARTITIONER_H_

#ifdef __cplusplus
# define OB_LOG_CPP_START extern "C" {
# define OB_LOG_CPP_END }
#else
# define OB_LOG_CPP_START
# define OB_LOG_CPP_END
#endif

OB_LOG_CPP_START
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
OB_LOG_CPP_END

#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_config.h"
#include "ob_log_filter.h"
#include "ob_log_schema_getter.h"   // IObLogSchemaGetter
#include "common/ob_tsi_factory.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogPartitioner
    {
      public:
        virtual ~IObLogPartitioner() {};
      public:
        virtual int init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter) = 0;

        virtual void destroy() = 0;

        /// @brief Partition mutator cell to get DB partition number and table partition number
        /// @param cell target mutator cell
        /// @param db_partition DB partition which will be returned
        /// @param tb_partition table partition which will be returned
        /// @return partition state
        /// @retval OB_SUCCESS on success
        /// @retval OB_ENTRY_NOT_EXIST while table is not selected by users
        /// @retval ! OB_SUCCESS and ! OB_ENTRY_NOT_EXIST on error
        /// @note db_partition and tb_partition can only be filled will valid value when retval is OB_SUCCESS
        virtual int partition(const common::ObMutatorCellInfo *cell, uint64_t *db_partition, uint64_t *tb_partition) = 0;

        virtual bool is_inited() const = 0;
    };

    class ObLogPartitioner : public IObLogPartitioner
    {
      static const int64_t FUNCTION_NAME_LENGTH = 1024;
      static const int64_t TABLE_ID_MAP_SIZE = 128;
      static const int64_t VARCHAE_BUFFER_SIZE = 65536;
      enum PartitionFunctionType
      {
        DEFAULT_PAR_FUNC    = 0,
        ONE_LINE_LUA        = 1,
        SCRIPT_LUA          = 2
      };
      struct PartitionFunction
      {
        PartitionFunctionType type;
        char partition_function_name[FUNCTION_NAME_LENGTH];
      };
      struct TableInfo
      {
        PartitionFunction db;
        PartitionFunction tb;
      };
      struct ParseArg
      {
        const ObLogSchema * total_schema;
        const ObLogConfig * config;
      };
      typedef common::hash::ObHashMap<uint64_t, TableInfo*> TableIDMap;
      public:
        ObLogPartitioner();
        ~ObLogPartitioner();

      public:
        static int get_log_paritioner(IObLogPartitioner *&log_partitioner,
            const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter);

      public:
        int init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter);
        void destroy();
        int partition(const common::ObMutatorCellInfo *cell, uint64_t *db_partition, uint64_t *tb_partition);
        int operator() (const char *tb_name, const ParseArg * arg);
        bool is_inited() const {return inited_;}
      private:
        int prepare_partition_functions_(const char *tb_select,
            const ObLogConfig &config,
            const ObLogSchema &total_schema);
        static int push_rowkey_values_(lua_State *lua, const common::ObRowkey &rowkey,
            PartitionFunctionType type);
        int calc_partition_(const TableInfo &table_info,
            const common::ObRowkey &rowkey,
            uint64_t *db_partition,
            uint64_t *tb_partition);
        int calc_db_partition_default_(const common::ObRowkey &rowkey,
            uint64_t *db_partition);
        int calc_tb_partition_default_(const common::ObRowkey &rowkey,
            uint64_t *tb_partition);
        int lua_call_func_(PartitionFunctionType type, int arg_count);
        int set_partition_functions_(const ObLogConfig * config, const char * tb_name,
            TableInfo * table_info);
      private:
        bool inited_;
        lua_State *lua_;
        common::CharArena allocator_;
        TableIDMap table_id_map_;
        int router_thread_num_;     ///< LogFormator thread number
    };

  }
}

#endif //OCEANBASE_LIBOBLOG_PARTITIONER_H_
