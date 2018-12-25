////===================================================================
 //
 // ob_log_partitioner.cpp liboblog / Oceanbase
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

#include "common/ob_define.h"
#include "ob_log_common.h"
#include "ob_log_partitioner.h"

#define LUA_VAR_TABLE_NAME      "table_name"
#define LUA_VAR_RK_COLUMN_NAME  "rk_column_name"
#define LUA_FUN_DB_PARTITION    "db_partition"
#define LUA_FUN_TB_PARTITION    "tb_partition"
#define LUA_ONE_LINE_ARG_NAME   "arg"

#define LUA_FUN_DB_PARTITION_FORMAT    "%s_db_partition"
#define LUA_FUN_TB_PARTITION_FORMAT    "%s_tb_partition"
#define LUA_ONE_LINE_DB_PARTITION_FORMAT    "__inner_gen_random_one_line_lua_%s_db_partition"
#define LUA_ONE_LINE_TB_PARTITION_FORMAT    "__inner_gen_random_one_line_lua_%s_tb_partition"

#define GET_LUA_VALUE(key, check_type, value, get_type, pop) \
  if (OB_SUCCESS == ret) \
  { \
    lua_getglobal(lua_, key); \
    if (!lua_is##check_type(lua_, -1)) \
    { \
      TBSYS_LOG(WARN, "key=%s is not check_type=%s", key, #check_type); \
      ret = OB_ERR_UNEXPECTED; \
    } \
    else \
    { \
      TBSYS_LOG(INFO, "get_lua_value succ key=%s check_type=%s get_type=%s", key, #check_type, #get_type); \
      value = lua_to##get_type(lua_, -1); \
    } \
    if (pop) \
    { \
      lua_pop(lua_, 1); \
    } \
  }

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogPartitioner::ObLogPartitioner() : inited_(false),
                                           lua_(NULL),
                                           allocator_(),
                                           table_id_map_(),
                                           router_thread_num_(0)
    {
    }

    ObLogPartitioner::~ObLogPartitioner()
    {
      destroy();
    }

    int ObLogPartitioner::get_log_paritioner(IObLogPartitioner *&log_partitioner,
        const ObLogConfig &config,
        IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;

      ObLogPartitioner * tmp_partitioner = common::GET_TSI_MULT(ObLogPartitioner, TSI_LIBOBLOG_PARTITION);

      if (NULL == tmp_partitioner)
      {
        TBSYS_LOG(ERROR, "GET_TSI_MULT get IObLogPartitioner fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (! tmp_partitioner->is_inited())
      {
        ret = tmp_partitioner->init(config, schema_getter);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to initialize IObLogPartitioner, ret=%d", ret);
          tmp_partitioner = NULL;
        }
      }

      log_partitioner = tmp_partitioner;
      return ret;
    }

    int ObLogPartitioner::init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      const char *tb_select = NULL;
      const ObLogSchema *total_schema = NULL;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= (router_thread_num_ = config.get_router_thread_num()))
      {
        TBSYS_LOG(WARN, "invalid router thread number %d", router_thread_num_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (tb_select = config.get_tb_select()))
      {
        TBSYS_LOG(WARN, "tb_select null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == schema_getter)
      {
        TBSYS_LOG(WARN, "schema_getter null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (total_schema = schema_getter->get_schema()))
      {
        TBSYS_LOG(WARN, "get total schema fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (lua_ = luaL_newstate()))
      {
        TBSYS_LOG(WARN, "luaL_newstate fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        luaL_openlibs(lua_);
        if (OB_SUCCESS != (ret = prepare_partition_functions_(tb_select, config, *total_schema)))
        {
          TBSYS_LOG(WARN, "prepare_partition_functions fail, ret=%d tb_select=[%s]", ret, tb_select);
        }
        else
        {
          inited_ = true;
        }
      }
      if (NULL != total_schema)
      {
        schema_getter->revert_schema(total_schema);
        total_schema = NULL;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogPartitioner::destroy()
    {
      table_id_map_.destroy();
      allocator_.reuse();
      if (NULL != lua_)
      {
        lua_close(lua_);
        lua_ = NULL;
      }
    }

    int ObLogPartitioner::partition(const ObMutatorCellInfo *cell, uint64_t *db_partition, uint64_t *tb_partition)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      if (NULL == cell || NULL == db_partition || NULL == tb_partition)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        TableInfo *table_info = NULL;
        int hash_ret = table_id_map_.get(cell->cell_info.table_id_, table_info);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(ERROR, "table_id_map_ get table info fails, table_id=%lu, hash_ret=%d",
              cell->cell_info.table_id_, hash_ret);

          ret = OB_ERR_UNEXPECTED;
        }
        else if (hash::HASH_NOT_EXIST == hash_ret)
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (NULL == table_info)
        {
          TBSYS_LOG(WARN, "get from table_id_map fail, hash_ret=%d table_id=%lu table_info=%p",
              hash_ret, cell->cell_info.table_id_, table_info);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = calc_partition_(*table_info,
                  cell->cell_info.row_key_,
                  db_partition,
                  tb_partition)))
        {
          TBSYS_LOG(WARN, "calc_partition fail, ret=%d table_id=%lu rowkey=%s",
              ret, cell->cell_info.table_id_, to_cstring(cell->cell_info.row_key_));
        }
      }

      return ret;
    }

    /**
     * @note 如果lua脚本配置在ini文件中（即ONE_LINE_LUA）,
     * 那么arg变量是作为全局变量给lua脚本使用的, 这里需要显式的把arg构造成table.
     * 如果lua脚本配置在单独的lua脚本文件中（即SCRIPT_LUA）,
     * 那么arg变量是作为lua函数的参数传入给lua脚本使用, 由pcall函数自动从lua stack上构建.
     */
    int ObLogPartitioner::push_rowkey_values_(lua_State *lua, const ObRowkey &rowkey,
        PartitionFunctionType type)
    {
      int ret = OB_SUCCESS;
      int64_t pushed_cnt = 0;
      if (ONE_LINE_LUA == type)
      {
        lua_newtable(lua);
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey.get_obj_cnt(); i++)
      {
        const ObObj &value = rowkey.get_obj_ptr()[i];
        int64_t int_value = 0;
        if (ONE_LINE_LUA == type)
        { //push lua table的下标
          lua_pushinteger(lua, i + 1);
        }
        switch (value.get_type())
        {
        case ObNullType:
          int_value = 0;
        case ObIntType:
          value.get_int(int_value);
        case ObDateTimeType:
          value.get_datetime(int_value);
        case ObPreciseDateTimeType:
          value.get_precise_datetime(int_value);
        case ObCreateTimeType:
          value.get_createtime(int_value);
        case ObModifyTimeType:
          value.get_modifytime(int_value);
          lua_pushinteger(lua, int_value);
          pushed_cnt += 1;
          break;
        case ObVarcharType:
          {
            ObString varchar_value;
            value.get_varchar(varchar_value);
            char buffer[VARCHAE_BUFFER_SIZE];
            if (VARCHAE_BUFFER_SIZE <= snprintf(buffer,
                  VARCHAE_BUFFER_SIZE,
                  "%.*s",
                  varchar_value.length(),
                  varchar_value.ptr()))
            {
              TBSYS_LOG(WARN, "varchar length too long, value=%.*s",
                        varchar_value.length(), varchar_value.ptr());
              ret = OB_BUF_NOT_ENOUGH;
            }
            else
            {
              lua_pushstring(lua, buffer);
              pushed_cnt += 1;
            }
            break;
          }
        default:
          TBSYS_LOG(WARN, "rowkey column value type=%d not support", value.get_type());
          ret = OB_SCHEMA_ERROR;
          break;
        }
        if (ONE_LINE_LUA == type)
        { //设置到table中
          lua_settable(lua, -3);
        }
      }
      if (OB_SUCCESS != ret)
      {
        if (ONE_LINE_LUA == type)
        {
          lua_pop(lua, 2); // pop the table and the index
        }
        else
        {
          lua_pop(lua, (int)pushed_cnt);
        }
      }
      else
      {
        if (ONE_LINE_LUA == type)
        {
          lua_setglobal(lua, LUA_ONE_LINE_ARG_NAME);
        }
      }
      return ret;
    }

    int ObLogPartitioner::calc_partition_(const TableInfo &table_info,
        const ObRowkey &rowkey,
        uint64_t *db_partition,
        uint64_t *tb_partition)
    {
      int ret = OB_SUCCESS;
      if (NULL != db_partition)
      {
        if (DEFAULT_PAR_FUNC == table_info.db.type)
        {
          // Use default partition function
          if (OB_SUCCESS != (ret = calc_db_partition_default_(rowkey, db_partition)))
          {
            TBSYS_LOG(WARN, "calc_db_partition_default_() fail, ret=%d", ret);
          }
        }
        else
        {
          lua_getglobal(lua_, table_info.db.partition_function_name);
          if (OB_SUCCESS != (ret = push_rowkey_values_(lua_, rowkey, table_info.db.type)))
          {
            TBSYS_LOG(WARN, "push_rowkey_values fail, ret=%d rowkey=%s", ret, to_cstring(rowkey));
          }
          else if (0 != (ret = lua_call_func_(table_info.db.type, (int)rowkey.get_obj_cnt())))
          {
            TBSYS_LOG(WARN, "lua_call_func_ fail, ret=%d", ret);
          }
          else
          {
            *db_partition = lua_tointeger(lua_, -1);
          }
          lua_pop(lua_, 1) ;
        }
      }

      if (OB_SUCCESS == ret
          && NULL != tb_partition)
      {
        if (DEFAULT_PAR_FUNC == table_info.tb.type)
        {
          // Use default partition function
          if (OB_SUCCESS != (ret = calc_tb_partition_default_(rowkey, tb_partition)))
          {
            TBSYS_LOG(WARN, "calc_tb_partition_default_() fail, ret=%d", ret);
          }
        }
        else
        {
          lua_getglobal(lua_, table_info.tb.partition_function_name);
          if (OB_SUCCESS != (ret = push_rowkey_values_(lua_, rowkey, table_info.tb.type)))
          {
            TBSYS_LOG(WARN, "push_rowkey_values fail, ret=%d rowkey=%s", ret, to_cstring(rowkey));
          }
          else if (0 != (ret = lua_call_func_(table_info.tb.type, (int)rowkey.get_obj_cnt())))
          {
            TBSYS_LOG(WARN, "lua_call_func_ fail, ret=%d", ret);
          }
          else
          {
            *tb_partition = lua_tointeger(lua_, -1);
          }
          lua_pop(lua_, 1) ;
        }
      }
      return ret;
    }

    int ObLogPartitioner::calc_db_partition_default_(const common::ObRowkey &rowkey,
        uint64_t *db_partition)
    {
      OB_ASSERT(NULL != db_partition);

      int ret = OB_SUCCESS;
      UNUSED(rowkey);

      // NOTE: DataBase partition is always 0. Extend it
      *db_partition = 0;

      return ret;
    } // calc_db_partition_default_

    int ObLogPartitioner::calc_tb_partition_default_(const common::ObRowkey &rowkey,
        uint64_t *tb_partition)
    {
      OB_ASSERT(NULL != tb_partition);

      int ret = OB_SUCCESS;

      *tb_partition = (rowkey.murmurhash64A(0)) % router_thread_num_;

      return ret;
    } // calc_tb_partition_default_

    int ObLogPartitioner::lua_call_func_(PartitionFunctionType type, int arg_count)
    {
      int ret = OB_SUCCESS;
      int err = 0;
      if (SCRIPT_LUA == type)
      {
        if(0 != (err = lua_pcall(lua_, arg_count, 1, 0)))
        {
          LOG_AND_ERR(ERROR, "invoke lua failed, lua script may have syntax error, "
              "err=%d [%s]", err, lua_tostring(lua_, -1));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      else if (ONE_LINE_LUA == type)
      {
        if(0 != (err = lua_pcall(lua_, 0, 1, 0)))
        {
          LOG_AND_ERR(ERROR, "invoke lua failed, lua script may have syntax error, "
              "err=%d [%s]", err, lua_tostring(lua_, -1));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      else
      {
        LOG_AND_ERR(ERROR, "Internal error, unknown PartitionFunctionType(%d), "
            "please contact developer", type);
      }
      return ret;
    }

    int ObLogPartitioner::prepare_partition_functions_(const char *tb_select,
        const ObLogConfig &config,
        const ObLogSchema &total_schema)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      ParseArg parse_arg = {&total_schema, &config};
      if (NULL == tb_select)
      {
        TBSYS_LOG(ERROR, "Internal error, "
            "ObLogPartitioner::prepare_partition_functions_ tb_select == NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCCESS == ret)
      {
        if (NULL != config.get_lua_conf())
        {
          if (0 != (tmp_ret = luaL_loadfile(lua_, config.get_lua_conf())))
          {
            TBSYS_LOG(WARN, "luaL_loadfile fail, ret=%d conf=[%s] [%s]",
                      tmp_ret, config.get_lua_conf(), lua_tostring(lua_, -1));
            ret = OB_ERR_UNEXPECTED;
          }
          else if (0 != (tmp_ret = lua_pcall(lua_, 0, 0, 0)))
          {
            TBSYS_LOG(WARN, "lua_pcall fail, ret=%d [%s]", tmp_ret, lua_tostring(lua_, -1));
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (0 != table_id_map_.create(TABLE_ID_MAP_SIZE))
        {
          TBSYS_LOG(WARN, "create table_id_map fail");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = ObLogConfig::parse_tb_select(tb_select, *this, &parse_arg)))
        {
          TBSYS_LOG(WARN, "parse tb_select to build table_id_map fail, ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "build table_id_map succ, tb_select=[%s]", tb_select);
          TableIDMap::iterator iter;
          for (iter = table_id_map_.begin(); iter != table_id_map_.end(); iter++)
          {
            TBSYS_LOG(INFO, "table_id=%lu db_partition_function=%s tb_partition_function=%s",
                      iter->first, iter->second->db.partition_function_name,
                      iter->second->tb.partition_function_name);
          }
        }
      }
      return ret;
    }

#define CHECK_LUA_VALUE(key, type) \
  if (OB_SUCCESS == ret) \
  { \
    lua_getglobal(lua_, key); \
    if (lua_isnil(lua_, -1)) \
    { \
      ret = OB_ENTRY_NOT_EXIST; \
    } \
    else if (!lua_is##type(lua_, -1)) \
    { \
      TBSYS_LOG(INFO, "`%s' is not [%s]", key, #type); \
      ret = OB_ERR_UNEXPECTED; \
    } \
    else \
    { \
      TBSYS_LOG(INFO, "check_lua_value succ key=%s type=%s", key, #type); \
    } \
    lua_pop(lua_, 1); \
  }

#define FETCH_SCRIPT_PAR_FUNC(type, partition_format) \
  { \
    if (FUNCTION_NAME_LENGTH <= snprintf(func_name, FUNCTION_NAME_LENGTH, \
          partition_format, tb_name)) \
    { \
      LOG_AND_ERR(ERROR, "`%s' table name is too long to handle", tb_name); \
      ret = OB_BUF_NOT_ENOUGH; \
    } \
    else \
    { \
      CHECK_LUA_VALUE(func_name, function); \
      if (OB_SUCCESS == ret) \
      { \
        TBSYS_LOG(INFO, "`%s' " #type " partition function found in lua script %s", \
            tb_name, config->get_lua_conf()); \
      } \
    } \
  }

#define FETCH_ONE_LINE_PAR_FUNC(type, partition_format) \
  { \
    if (FUNCTION_NAME_LENGTH <= snprintf(func_name, FUNCTION_NAME_LENGTH, \
          partition_format, tb_name)) \
    { \
      LOG_AND_ERR(ERROR, "`%s' table name is too long to handle", tb_name); \
      ret = OB_BUF_NOT_ENOUGH; \
    } \
    else \
    { \
      lua_getglobal(lua_, func_name); \
      if (!lua_isnil(lua_, -1)) \
      { \
        lua_pop(lua_, 1); \
        LOG_AND_ERR(ERROR, "please do not use variable `%s' in lua script", \
            func_name); \
        ret = OB_ERR_UNEXPECTED; \
      } \
      else \
      { \
        lua_pop(lua_, 1); \
        err = luaL_loadbuffer(lua_, one_line_lua, strlen(one_line_lua), tb_name); \
        if (err) \
        { \
          LOG_AND_ERR(ERROR, "%s has syntax error for `%s'", one_line_lua, tb_name); \
          ret = OB_ERR_UNEXPECTED; \
        } \
        else \
        { \
          lua_setglobal(lua_, func_name); \
          TBSYS_LOG(INFO, "`%s' " #type " partition function found in configuration file (%s)", \
              tb_name, config->get_config_fpath()); \
        } \
      } \
    } \
  }

    int ObLogPartitioner::set_partition_functions_(const ObLogConfig * config,
        const char * tb_name, TableInfo * table_info)
    {
      int ret = OB_SUCCESS;
      int err = 0;
      char func_name[FUNCTION_NAME_LENGTH];
      if (NULL == tb_name || NULL == table_info || NULL == config)
      {
        LOG_AND_ERR(ERROR, "Internal error in ObLogPartitioner::set_partition_function, "
            "invalid function arguments, please contact developer.");
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCCESS == ret)
      {
        const char * one_line_lua = config->get_db_partition_lua(tb_name);
        if (NULL != one_line_lua)
        {
          FETCH_ONE_LINE_PAR_FUNC(db, LUA_ONE_LINE_DB_PARTITION_FORMAT);
          if (OB_SUCCESS == ret)
          {
            // Use partition function defined in configuration file
            memcpy(table_info->db.partition_function_name, func_name, strlen(func_name) + 1);
            table_info->db.type = ONE_LINE_LUA;
          }
        }
        else
        {
          FETCH_SCRIPT_PAR_FUNC(db, LUA_FUN_DB_PARTITION_FORMAT);
          if (OB_SUCCESS == ret)
          {
            // Use partition function defined in LUA scripts
            memcpy(table_info->db.partition_function_name, func_name, strlen(func_name) + 1);
            table_info->db.type = SCRIPT_LUA;
          }
          else if (OB_ENTRY_NOT_EXIST == ret)
          {
            // Use default partition function
            table_info->db.type = DEFAULT_PAR_FUNC;
            TBSYS_LOG(INFO, "'%s' use default db partition function", tb_name);
            ret = OB_SUCCESS;
          }
          else
          {
            LOG_AND_ERR(ERROR, "unexpected error: set db partition fuction for table %s fail",
              tb_name);
          }
        }

      }
      if (OB_SUCCESS == ret)
      {
        const char * one_line_lua = config->get_tb_partition_lua(tb_name);
        if (NULL != one_line_lua)
        {
          FETCH_ONE_LINE_PAR_FUNC(tb, LUA_ONE_LINE_TB_PARTITION_FORMAT);
          if (OB_SUCCESS == ret)
          {
            // Use partition function defined in configuration file
            memcpy(table_info->tb.partition_function_name, func_name, strlen(func_name) + 1);
            table_info->tb.type = ONE_LINE_LUA;
          }
        }
        else
        {
          FETCH_SCRIPT_PAR_FUNC(tb, LUA_FUN_TB_PARTITION_FORMAT);
          if (OB_SUCCESS == ret)
          {
            // Use partition function defined in LUA scripts
            memcpy(table_info->tb.partition_function_name, func_name, strlen(func_name) + 1);
            table_info->tb.type = SCRIPT_LUA;
          }
          else if (OB_ENTRY_NOT_EXIST == ret)
          {
            // Use default partition function
            table_info->tb.type = DEFAULT_PAR_FUNC;
            TBSYS_LOG(INFO, "'%s' use default tb partition function", tb_name);
            ret = OB_SUCCESS;
          }
          else
          {
            LOG_AND_ERR(ERROR, "unexpected error: set tb partition fuction for table %s fail",
              tb_name);
          }
        }
      }

      return ret;
    }

    int ObLogPartitioner::operator() (const char *tb_name, const ParseArg * arg)
    {
      int ret = OB_SUCCESS;
      const ObLogSchema * total_schema = NULL;
      const ObLogConfig * config = NULL;
      TableInfo * table_info = NULL;
      const ObTableSchema * table_schema = NULL;

      if (NULL == tb_name || NULL == arg || NULL == arg->total_schema
          || NULL == arg->config)
      {
        LOG_AND_ERR(ERROR, "Internal error in ObLogPartitioner::operator(), "
            "invalid function arguments, please contact developer.");
        ret = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == ret)
      {
        total_schema = arg->total_schema;
        config = arg->config;
        if (NULL == (table_info = (TableInfo*)allocator_.alloc(sizeof(TableInfo))))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (table_schema = total_schema->get_table_schema(tb_name)))
        {
          LOG_AND_ERR(ERROR, "`%s' table does not exist, please contact dba", tb_name);
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          for (int64_t i = 0; i < table_schema->get_rowkey_info().get_size(); i++)
          {
            const ObRowkeyColumn *rowkey_column = table_schema->get_rowkey_info().get_column(i);
            if (NULL == rowkey_column)
            {
              ret = OB_ERR_UNEXPECTED;
              break;
            }
            ObObjType type = rowkey_column->type_;
            if (ObIntType != type
                && ObDateTimeType != type
                && ObPreciseDateTimeType != type
                && ObVarcharType != type
                && ObCreateTimeType != type
                && ObModifyTimeType != type)
            {
              TBSYS_LOG(WARN, "rowkey column type=(%s,%d) not support", ob_obj_type_str(type), type);
              ret = OB_SCHEMA_ERROR;
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_partition_functions_(config, tb_name, table_info);
        if (OB_SUCCESS == ret)
        {
          int hash_ret = table_id_map_.set(table_schema->get_table_id(), table_info);
          if (hash::HASH_INSERT_SUCC != hash_ret)
          {
            TBSYS_LOG(WARN, "set to table_id_map fail, hash_ret=%d table_id=%lu table_name=%s",
                      hash_ret, table_schema->get_table_id(), tb_name);
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }

      return ret;
    }

  }
}

