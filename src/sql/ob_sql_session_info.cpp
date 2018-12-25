#include "ob_sql_session_info.h"
#include "common/ob_define.h"
#include "common/ob_mod_define.h"
#include "common/ob_obj_cast.h"
#include "common/ob_trace_log.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include "easy_connection.h"
#include "ob_sql_character_set.h"
#include <utility>
using namespace oceanbase::sql;
using namespace oceanbase::common;


const char *state_str[] ={
      "SLEEP",
      "ACTIVE",
      "QUERY_KILLED",
      "SESSION_KILLED",
};

ObSQLSessionInfo::ObSQLSessionInfo()
  :session_id_(OB_INVALID_ID),
   user_id_(OB_INVALID_ID),
   next_stmt_id_(0),
   cur_result_set_(NULL),
   state_(SESSION_SLEEP),
   conn_(NULL),
   cur_query_start_time_(0),
   cur_query_len_(0),
   is_autocommit_(true),
   is_interactive_(false),
   last_active_time_(0),
   curr_trans_start_time_(0),
   curr_trans_last_stmt_time_(0),
   version_provider_(NULL),
   config_provider_(NULL),
   block_allocator_(SMALL_BLOCK_SIZE, common::OB_COMMON_MEM_BLOCK_SIZE, ObMalloc(ObModIds::OB_SQL_SESSION_SBLOCK)),
   name_pool_(ObModIds::OB_SQL_SESSION, OB_COMMON_MEM_BLOCK_SIZE),
   parser_mem_pool_(ObModIds::OB_SQL_PARSER, OB_COMMON_MEM_BLOCK_SIZE),
  id_plan_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  stmt_name_id_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  var_name_val_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  sys_var_val_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  id_psinfo_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  ps_store_(NULL),
  arena_pointers_(sizeof(ObArenaAllocator), SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  result_set_pool_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  ps_session_info_pool_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  ps_session_info_param_pool_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_))
{
  pthread_rwlock_init(&rwlock_, NULL);
  cur_query_[0] = '\0';
  cur_query_[MAX_CUR_QUERY_LEN-1] = '\0';
}

ObSQLSessionInfo::~ObSQLSessionInfo()
{
  destroy();
}

int64_t ObSQLSessionInfo::to_string(char *buffer, const int64_t length) const
{
  int64_t size = 0;
  size += snprintf(buffer, length, "session_id=%ld", session_id_);
  return size;
}

int ObSQLSessionInfo::init(common::DefaultBlockAllocator &block_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = id_plan_map_.create(hash::cal_next_prime(128),
                                               &id_plan_map_allocer_,
                                               &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init id-plan map failed, ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = stmt_name_id_map_.create(hash::cal_next_prime(16),
                                                        &stmt_name_id_map_allocer_,
                                                        &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init name-id map failed, ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = var_name_val_map_.create(hash::cal_next_prime(16),
                                                        &var_name_val_map_allocer_,
                                                        &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init var_value map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = sys_var_val_map_.create(hash::cal_next_prime(64),
                                                        &sys_var_val_map_allocer_,
                                                        &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init sys_var_value map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = id_psinfo_map_.create(hash::cal_next_prime(64),
                                                      &id_psinfo_map_allocer_,
                                                      &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init id_psinfo_map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = transformer_mem_pool_.init(&block_allocator, OB_COMMON_MEM_BLOCK_SIZE)))
  {
    TBSYS_LOG(WARN, "failed to init transformer mem pool, err=%d", ret);
  }
  else
  {
    block_allocator.set_mod_id(ObModIds::OB_SQL_TRANSFORMER);
  }
  return ret;
}

int ObSQLSessionInfo::set_ps_store(ObPsStore *store)
{
  int ret = OB_SUCCESS;
  if (NULL == store)
  {
    TBSYS_LOG(ERROR, "invalid argument store is %p", store);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ps_store_ = store;
  }
  return ret;
}

void ObSQLSessionInfo::destroy()
{
  IdPlanMap::iterator iter;
  for (iter = id_plan_map_.begin(); iter != id_plan_map_.end(); iter++)
  {
    ObResultSet* result_set = NULL;
    if (hash::HASH_EXIST != id_plan_map_.get(iter->first, result_set))
    {
      TBSYS_LOG(WARN, "result_set whose key=[%lu] not found", iter->first);
    }
    else
    {
      result_set_pool_.free(result_set);
      id_plan_map_.erase(iter->first);
    }
  }
  pthread_rwlock_destroy(&rwlock_);
}

const char* ObSQLSessionInfo::get_session_state_str()const
{
  return state_str[state_];
}

int ObSQLSessionInfo::set_peer_addr(const char* addr)
{
  int ret = OB_SUCCESS;
  if (NULL == addr
      ||static_cast<int64_t>(strlen(addr)) >= MAX_IPADDR_LENGTH)
  {
    TBSYS_LOG(WARN, "invalid argument addr is null or strlen(addr) greater than %ld", MAX_IPADDR_LENGTH);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(addr_, addr, strlen(addr) + 1);
  }
  return ret;
}

int ObSQLSessionInfo::store_plan(const ObString& stmt_name, ObResultSet& result_set)
{
  int ret = OB_SUCCESS;
  uint64_t stmt_id = OB_INVALID_ID;
  ObString name;
  ObResultSet *new_res_set = NULL;
  if (id_plan_map_.size() >= MAX_STORED_PLANS_COUNT)
  {
    TBSYS_LOG(USER_ERROR, "too many prepared statements, the max allowed is %ld", MAX_STORED_PLANS_COUNT);
    ret = OB_ERR_TOO_MANY_PS;
  }
  else if ((ret = name_pool_.write_string(stmt_name, &name)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to save statement name in session name pool");
  }
  else if (name.length() > 0 && plan_exists(name, &stmt_id))
  {
    if (id_plan_map_.get(stmt_id, new_res_set) == hash::HASH_EXIST)
    {
      new_res_set->reset();
    }
    else
    {
      ret = OB_ERR_PREPARE_STMT_UNKNOWN;
      TBSYS_LOG(ERROR, "Can not find stored plan, id = %lu, name = %.*s",
          stmt_id, name.length(), name.ptr());
      stmt_name_id_map_.erase(stmt_name);
    }
  }
  else
  {
    stmt_id = get_new_stmt_id();
    if (name.length() > 0 && stmt_name_id_map_.set(name, stmt_id) != hash::HASH_INSERT_SUCC)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to save statement <name, id> pair");
    }
    else if ((new_res_set = result_set_pool_.alloc()) == NULL)
    {
      TBSYS_LOG(ERROR, "ob malloc for ObResultSet failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (name.length() > 0)
        stmt_name_id_map_.erase(name);
    }
    // from stmt_prepare, there is no statement name
    else if (id_plan_map_.set(stmt_id, new_res_set) != hash::HASH_INSERT_SUCC)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to save prepared plan");
      if (name.length() > 0)
        stmt_name_id_map_.erase(name);
      result_set_pool_.free(new_res_set);
    }
  }
  if (ret == OB_SUCCESS && new_res_set != NULL)
  {
    // params are just place holder, they will be filled when execute
    result_set.set_statement_id(stmt_id);
    result_set.set_statement_name(name);
    FILL_TRACE_LOG("stored_query_result=(%s)", to_cstring(result_set));
    if ((ret = result_set.to_prepare(*new_res_set)) == OB_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "new_res_set=%p output_result=(%s) stored_result=(%s)",
                new_res_set, to_cstring(result_set), to_cstring(*new_res_set));
    }
    else
    {
      new_res_set->reset();
    }
  }
  return ret;
}

int ObSQLSessionInfo::remove_plan(const uint64_t& stmt_id)
{
  int ret = OB_SUCCESS;
  ObResultSet *result_set = NULL;
  if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
  {
    ret = OB_ERR_PREPARE_STMT_UNKNOWN;
    TBSYS_LOG(WARN, "prepare statement id not found, statement id = %ld", stmt_id);
  }
  else
  {
    const ObString& stmt_name = result_set->get_statement_name();
    if (stmt_name.length() > 0 && stmt_name_id_map_.erase(stmt_name) != hash::HASH_EXIST)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "prepare statement name not fount, statement name = %.*s",
                stmt_name.length(), stmt_name.ptr());
    }
    if (ret == OB_SUCCESS && id_plan_map_.erase(stmt_id) != hash::HASH_EXIST)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "drop prepare statement error, statement id = %ld", stmt_id);
    }
    if (ret == OB_SUCCESS)
    {
      result_set_pool_.free(result_set); // will free the ps_transformer_allocator to the session
    }
  }
  return ret;
}

int ObSQLSessionInfo::store_params_type(int64_t stmt_id, const common::ObIArray<obmysql::EMySQLFieldType> &types)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  ret = id_psinfo_map_.get(static_cast<uint32_t>(stmt_id), info);
  if (hash::HASH_EXIST == ret)
  {
    if (0 < types.count())
    {
      info->params_type_.reserve(types.count());
      info->params_type_ = types;
    }
    ret = OB_SUCCESS;
  }
  else
  {
    TBSYS_LOG(WARN, "Get %ld ObPsSessionInfo failed ret=%d", stmt_id, ret);
  }
  return ret;
}

int ObSQLSessionInfo::store_ps_session_info(ObPsStoreItem *item, uint64_t &stmt_id)
{
  int ret = OB_SUCCESS;
  int64_t id = item->get_sql_id();
  int64_t pcount = item->get_item_value()->param_columns_.count();
  ret = insert_ps_session_info(id, pcount, stmt_id, item->get_item_value()->has_cur_time_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "insert ps session info failed ret=%d", ret);
  }
  return ret;
}

int ObSQLSessionInfo::store_ps_session_info(ObResultSet &result)
{
  int ret = OB_SUCCESS;
  uint64_t id = result.get_statement_id();
  int64_t pcount = result.get_param_columns().count();
  uint64_t out_stmt_id = 0;
  if (NULL == result.get_cur_time_place())
  {
    ret = insert_ps_session_info(id, pcount, out_stmt_id);
  }
  else
  {
    ret = insert_ps_session_info(id, pcount, out_stmt_id, true);
  }
  result.set_statement_id(out_stmt_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "insert ps session info failed ret=%d", ret);
  }
  return ret;
}

int ObSQLSessionInfo::insert_ps_session_info(uint64_t sql_id, int64_t pcount, uint64_t &new_stmt_id, bool has_cur_time)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  new_stmt_id = sql_id;
  ret = id_psinfo_map_.get(static_cast<uint32_t>(sql_id), info);
  if (hash::HASH_EXIST == ret)
  {
    new_stmt_id = ps_store_->allocate_new_id();
    ret = id_psinfo_map_.get(static_cast<uint32_t>(new_stmt_id), info);
    if (ret == hash::HASH_EXIST)
    {
      TBSYS_LOG(ERROR, "never reach here new_stmt_id is %lu", new_stmt_id);
    }
  }

  if (hash::HASH_NOT_EXIST == ret)
  {
    info  = ps_session_info_pool_.alloc();
    if (NULL == info)
    {
      TBSYS_LOG(ERROR, "can not alloc mem for ObPsSessionInfo");
      ret = OB_ERROR;
    }
    else
    {
      info->init(&ps_session_info_param_pool_);
      info->sql_id_ = sql_id;
      ObObj *place_holder = NULL;
      for (int64_t i = 0; i < pcount; i++)
      {
        if (NULL == (place_holder = info->alloc()))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        else
        {
          place_holder = new(place_holder) ObObj();
        }
        if (OB_SUCCESS != (ret = info->params_.push_back(place_holder)))
        {
          break;
        }
      }
      if (has_cur_time)
      {
        if (NULL == (place_holder = info->alloc()))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          //mod liuzy 20150910:b
//          place_holder = new(place_holder) ObObj();
//          info->cur_time_ = place_holder;
          for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
          {
            place_holder = new(place_holder) ObObj();
            info->cur_time_[i] = place_holder;
          }
          //mod 20150910:e
        }
      }
      ret = id_psinfo_map_.set(static_cast<uint32_t>(new_stmt_id), info);
      if (hash::HASH_INSERT_SUCC == ret)
      {
        TBSYS_LOG(DEBUG, "insert item into id_psinfo_map success id=%lu info=%p", new_stmt_id, info);
        ret = OB_SUCCESS;
      }
      else if (hash::HASH_EXIST == ret)
      {
        TBSYS_LOG(WARN, "id=%lu exist in id_psinfo_map", new_stmt_id);
        ps_session_info_pool_.free(info);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(WARN, "insert item into id_psinfo_map failed id=%lu ret=%d", new_stmt_id, ret);
        ps_session_info_pool_.free(info);
        ret = OB_ERROR;
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "get ps session info failed ret=%d", ret);
  }
  return ret;
}

int ObSQLSessionInfo::close_all_stmt()
{
  int ret = OB_SUCCESS;
  IdPsInfoMap::iterator iter;
  ObPsSessionInfo *info = NULL;
  uint64_t sql_id = 0;
  for (iter = id_psinfo_map_.begin(); iter != id_psinfo_map_.end(); iter++)
  {
    if (hash::HASH_EXIST != (ret = id_psinfo_map_.get(iter->first, info)))
    {
      TBSYS_LOG(WARN, "ObPsSessionInfo whose key=[%u] not found", iter->first);
    }
    else
    {
      sql_id = info->sql_id_;
      if (OB_SUCCESS != (ret = ps_store_->remove_plan(sql_id)))
      {
        TBSYS_LOG(WARN, "close prepared statement failed, session_id=%ld sql_id=%lu",
                  session_id_, sql_id);
      }
      else
      {
        TBSYS_LOG(INFO, "close prepared statement when session quit, session_id=%ld stmt_id=%u sql_id=%ld",
                  session_id_, iter->first, sql_id);
        OB_STAT_INC(SQL, SQL_PS_COUNT, -1);
      }
      ps_session_info_pool_.free(info);//free ps session info when session quit
    }
  }
  id_psinfo_map_.clear(); //clear id_psinfo_map
  return ret;
}

int ObSQLSessionInfo::remove_ps_session_info(const uint64_t stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *info = NULL;
  if (id_psinfo_map_.get(static_cast<uint32_t>(stmt_id), info) != hash::HASH_EXIST)
  {
    ret = OB_ERR_PREPARE_STMT_UNKNOWN;
    TBSYS_LOG(WARN, "prepare statement id not found in id_psinfo_map, statement id = %ld", stmt_id);
  }
  else
  {
    ps_session_info_pool_.free(info);
    if (id_psinfo_map_.erase(static_cast<uint32_t>(stmt_id)) != hash::HASH_EXIST)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "drop prepare statement error, statement id = %ld", stmt_id);
    }
    else
    {
      TBSYS_LOG(DEBUG, "remove ps statement, stmt_id=%lu", stmt_id);
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_ps_session_info(const int64_t stmt_id, ObPsSessionInfo*& info)
{
  int ret = OB_SUCCESS;
  ret = id_psinfo_map_.get(static_cast<uint32_t>(stmt_id), info);
  if (hash::HASH_EXIST != ret)
  {
    TBSYS_LOG(WARN, "get ps session info failed stmt_id=%ld ret=%d", stmt_id, ret);
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get ps session info success stmt_id=%ld, info=%p", stmt_id, info);
    ret = OB_SUCCESS;
  }
  return ret;
}

bool ObSQLSessionInfo::plan_exists(const ObString& stmt_name, uint64_t *stmt_id)
{
  uint64_t id = OB_INVALID_ID;
  return (stmt_name_id_map_.get(stmt_name, stmt_id ? *stmt_id : id) == hash::HASH_EXIST);
}

ObResultSet* ObSQLSessionInfo::get_plan(const uint64_t& stmt_id) const
{
  ObResultSet * result_set = NULL;
  if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
    result_set = NULL;
  return result_set;
}

ObResultSet* ObSQLSessionInfo::get_plan(const ObString& stmt_name) const
{
  ObResultSet * result_set = NULL;
  uint64_t stmt_id = OB_INVALID_ID;
  if (stmt_name_id_map_.get(stmt_name, stmt_id) != hash::HASH_EXIST)
  {
    result_set = NULL;
  }
  else if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
  {
    result_set = NULL;
  }
  return result_set;
}

int ObSQLSessionInfo::replace_variable(const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObString tmp_var;
  ObObj tmp_val;
  if (var.length() <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if ((ret = name_pool_.write_string(var, &tmp_var)) != OB_SUCCESS
    || (ret = name_pool_.write_obj(val, &tmp_val)) != OB_SUCCESS
    || ((ret = var_name_val_map_.set(tmp_var, tmp_val, 1)) != hash::HASH_INSERT_SUCC
        && ret != hash::HASH_OVERWRITE_SUCC))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Add variable %.*s error", var.length(), var.ptr());
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSQLSessionInfo::remove_variable(const ObString& var)
{
  int ret = OB_SUCCESS;
  if (var.length() <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if (var_name_val_map_.erase(var) != hash::HASH_EXIST)
  {
    ret = OB_ERR_VARIABLE_UNKNOWN;
    TBSYS_LOG(ERROR, "remove variable %.*s error", var.length(), var.ptr());
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSQLSessionInfo::update_system_variable(const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObObj *obj = NULL;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (var.length() <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid system variable, name=%.*s", var.length(), var.ptr());
  }
  else
  {
    obj = values.first;
    if ((ret = name_pool_.write_obj(val, obj)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "Update system variable %.*s error", var.length(), var.ptr());
    }
  }
  return ret;
}
int ObSQLSessionInfo::load_system_variable(const ObString& name, const ObObj& type, const ObObj& value)
{
  int ret = OB_SUCCESS;
  char var_buf[OB_MAX_VARCHAR_LENGTH];
  ObString var_str;
  var_str.assign_ptr(var_buf, OB_MAX_VARCHAR_LENGTH);
  ObObj casted_cell;
  casted_cell.set_varchar(var_str);
  const ObObj *res_cell = NULL;
  ObString tmp_name;
  ObObj *val_ptr = NULL;
  if (NULL == (val_ptr = (ObObj*)name_pool_.get_arena().alloc(sizeof(ObObj))))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "no memory");
  }
  else
  {
    val_ptr = new(val_ptr) ObObj();
  }
  if (OB_SUCCESS != ret)
  {
  }
  else if ((ret = name_pool_.write_string(name, &tmp_name)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Fail to store variable name, err=%d", ret);
  }
  else if ((ret = obj_cast(value, type, casted_cell, res_cell)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Cast variable value failed, err=%d", ret);
  }
  else if ((ret = name_pool_.write_obj(*res_cell, val_ptr)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Fail to store variable value, err=%d", ret);
  }
  else if (sys_var_val_map_.set(tmp_name, std::make_pair(val_ptr, type.get_type()), 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Load system variable error, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(TRACE, "system variable %.*s=%s", name.length(), name.ptr(), to_cstring(*val_ptr));
  }
  return ret;
}

bool ObSQLSessionInfo::variable_exists(const ObString& var)
{
  ObObj val;
  return (var_name_val_map_.get(var, val) == hash::HASH_EXIST);
}

//add peiouya  [NotNULL_check] [JHOBv0.1] 20131222:b
/*expr:Null constraint checking */
int ObSQLSessionInfo::variable_constrain_check(const bool val_params_constraint, const  ObString & var_name)
{
  int ret = OB_SUCCESS;
  {
    const ObObj *var = get_variable_value(var_name);
    if( !val_params_constraint && (oceanbase::common::ObNullType == var->get_type()))
    {
      ret = OB_ERR_VARIABLE_NULL;
    }
  }
  return ret;
}
//add 20131222:e
ObObjType ObSQLSessionInfo::get_sys_variable_type(const ObString &var_name)
{
  std::pair<common::ObObj*, common::ObObjType> val;
  int ret = sys_var_val_map_.get(var_name, val);
  OB_ASSERT(ret == hash::HASH_EXIST);
  return val.second;
}
bool ObSQLSessionInfo::sys_variable_exists(const ObString& var)
{
  std::pair<common::ObObj*, common::ObObjType> val;
  return (sys_var_val_map_.get(var, val) == hash::HASH_EXIST);
}

int ObSQLSessionInfo::get_variable_value(const ObString& var, ObObj& val) const
{
  int ret = OB_SUCCESS;
  if (var_name_val_map_.get(var, val) != hash::HASH_EXIST)
    ret = OB_ERR_VARIABLE_UNKNOWN;
  return ret;
}

const ObObj* ObSQLSessionInfo::get_variable_value(const ObString& var) const
{
  return var_name_val_map_.get(var);
}

int ObSQLSessionInfo::get_sys_variable_value(const ObString& var, ObObj& val) const
{
  int ret = OB_SUCCESS;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    ret = OB_ERR_VARIABLE_UNKNOWN;
  }
  else
  {
    val = *(values.first);
  }
  return ret;
}

const ObObj* ObSQLSessionInfo::get_sys_variable_value(const ObString& var) const
{
  ObObj *obj = NULL;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    // do nothing
  }
  else
  {
    obj = values.first;
    TBSYS_LOG(DEBUG, "sys_variable=%s name=%.*s addr=%p", to_cstring(*obj), var.length(), var.ptr(), obj);
  }
  return obj;
}

bool ObSQLSessionInfo::is_create_sys_table_disabled() const
{
  bool ret = true;
  ObObj val;
  if (OB_SUCCESS != get_sys_variable_value(ObString::make_string("ob_disable_create_sys_table"), val)
      || OB_SUCCESS != val.get_bool(ret))
  {
    ret = true;
  }
  return ret;
}

int ObSQLSessionInfo::set_username(const ObString & user_name)
{
  int ret = OB_SUCCESS;
  ret = name_pool_.write_string(user_name, &user_name_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "write username to string_buf_ failed,ret=%d", ret);
  }
  return ret;
}

int ObSQLSessionInfo::store_query_string(const common::ObString &stmt)
{
  int ret = OB_SUCCESS;
  int64_t truncated_len = std::min(MAX_CUR_QUERY_LEN-1, static_cast<int64_t>(stmt.length()));
  memcpy(cur_query_, stmt.ptr(), truncated_len);
  cur_query_[truncated_len] = '\0';
  cur_query_len_ = truncated_len;
  return ret;
}

void ObSQLSessionInfo::set_warnings_buf()
{
  tbsys::WarningBuffer *warnings_buf = NULL;
  if ((warnings_buf = tbsys::get_tsi_warning_buffer()) == NULL)
  {
    TBSYS_LOG(WARN, "can not get thread warnings buffer");
    warnings_buf_.reset();
  }
  else if (cur_result_set_ && cur_result_set_->is_show_warnings()
    && warnings_buf->get_readable_warning_count() == 0)
  {
    /* Successful "show warning" statement, skip */
  }
  else
  {
    warnings_buf_ = *warnings_buf;
    warnings_buf->reset();
  }
}

ObArenaAllocator* ObSQLSessionInfo::get_transformer_mem_pool_for_ps()
{
  ObArenaAllocator* ret = NULL;
  void *ptr = arena_pointers_.alloc();
  if (NULL == ptr)
  {
    TBSYS_LOG(WARN, "no memory");
  }
  else
  {
    ret = new(ptr) ObArenaAllocator(ObModIds::OB_SQL_PS_TRANS);
    TBSYS_LOG(DEBUG, "new allocator, addr=%p session_id=%ld", ret, session_id_);
    OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT);
  }
  return  ret;
}

void ObSQLSessionInfo::free_transformer_mem_pool_for_ps(ObArenaAllocator* arena)
{
  if (OB_LIKELY(NULL != arena))
  {
    arena->~ObArenaAllocator();
    arena_pointers_.free(arena);
    TBSYS_LOG(DEBUG, "destroy allocator, addr=%p", arena);
    OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT, -1);
  }
}

int ObSQLSessionInfo::update_session_timeout()
{
  int ret = OB_SUCCESS;
  ObObj val;
  if (is_interactive_)
  {
    if (OB_SUCCESS != (ret = get_sys_variable_value(ObString::make_string("interactive_timeout"), val)))
    {
      TBSYS_LOG(WARN, "failed to get sys variable value: interactive_timeout, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = update_system_variable(ObString::make_string("wait_timeout"), val)))
    {
      TBSYS_LOG(WARN, "failed to update sys variable value: wait_timeout %s, ret=%d", to_cstring(val), ret);
    }
  }

  return ret;
}

bool ObSQLSessionInfo::is_timeout()
{
  bool ret = false;
  int err = OB_SUCCESS;
  ObObj val;
  int64_t timeout = 0;
  if (!is_interactive_)
  {
    if (OB_SUCCESS != (err = get_sys_variable_value(ObString::make_string("wait_timeout"), val)))
    {
      TBSYS_LOG(ERROR, "failed to get sys variable value: wait_timeout, err=%d", err);
    }
  }
  else
  {
    if (OB_SUCCESS != (err = get_sys_variable_value(ObString::make_string("interactive_timeout"), val)))
    {
      TBSYS_LOG(ERROR, "failed to get sys variable value: interactive_timeout, err=%d", err);
    }
  }

  if (OB_SUCCESS == err)
  {
    int64_t cur_time = tbsys::CTimeUtil::getTime();
    if (OB_SUCCESS != (err = val.get_int(timeout)))
    {
      TBSYS_LOG(ERROR, "failed to get session timeout, val=%s err=%d", to_cstring(val), err);
    }
    else if (0 == timeout)
    {
      // no timeout check for timeout == 0
    }
    else if (timeout > INT64_MAX/1000/1000)
    {
      TBSYS_LOG(WARN, "session timeout setting %ld is too larger, skip check timeout", timeout);
    }
    else if (last_active_time_ + timeout*1000*1000 < cur_time)
    {
      TBSYS_LOG(INFO, "session %lu: %.*s from %s timeout: last active time=%s, cur=%s, timeout=%lds",
          session_id_, user_name_.length(), user_name_.ptr(), inet_ntoa_r(conn_->addr),
          time2str(last_active_time_), time2str(cur_time), timeout);
      ret = true;
    }
  }
  return ret;
}

uint16_t ObSQLSessionInfo::get_charset()
{
  ObString charset_string;
  ObObj val;
  int ret = OB_SUCCESS;
  int32_t charset = 0;
  if (OB_SUCCESS != (ret = get_sys_variable_value(ObString::make_string("ob_charset"), val)))
  {
    TBSYS_LOG(ERROR, "failed to get sys variable value: ob_charset, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = val.get_varchar(charset_string)))
  {
    TBSYS_LOG(ERROR, "failed to get ob_charset string, ret=%d", ret);
  }
  else if (0 >= (charset = get_char_number_from_name(charset_string)))
  {
    TBSYS_LOG(ERROR, "failed to get char number from name, charset_string=%.*s",
        charset_string.length(), charset_string.ptr());
  }

  if (0 >= charset)
  {
    charset = get_char_number_from_name("gbk");
    TBSYS_LOG(WARN, "use default charset gbk");
  }
  return static_cast<uint16_t>(charset);
}

//add wenghaixing [database manage]20150615
int ObSQLSessionInfo::set_db_name(const ObString& db_name)
{
  int ret = OB_SUCCESS;
  ret = name_pool_.write_string(db_name, &db_name_);
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "write dbname to string_buf_ failed, ret = %d", ret);
  }
  return ret;
}
//add e
