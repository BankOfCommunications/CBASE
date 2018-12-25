#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "ob_schema_manager.h"

using namespace oceanbase::common;

ObMergerSchemaManager::ObMergerSchemaManager()
{
  init_ = false;
  update_timestamp_ = 0;
  latest_version_ = -1;
}

ObMergerSchemaManager::~ObMergerSchemaManager()
{
}

// init in main thread
int ObMergerSchemaManager::init(const bool system, const ObSchemaManagerV2 & schema)
{
  int ret = OB_SUCCESS;
  if (true == init_)
  {
    TBSYS_LOG(WARN, "%s", "init schema manager twice");
    ret = OB_INIT_TWICE;
  }
  else if (false == system)
  {
    user_schemas_[0].ref_count = 1;
    user_schemas_[0].schema = schema;
    update_timestamp_ = tbsys::CTimeUtil::getTime();
    latest_version_ = schema.get_version();
    TBSYS_LOG(INFO, "init user schema manager succ:timestamp[%ld]", latest_version_);
    init_ = true;
  }
  else
  {
    sys_schema_.ref_count = 1;
    sys_schema_.schema = schema;
    TBSYS_LOG(INFO, "init sys schema manager succ:timestamp[%ld]", schema.get_version());
    init_ = true;
  }
  return ret;
}

// schema version must incremental
int ObMergerSchemaManager::add_schema(const ObSchemaManagerV2 & schema,
    const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat or schema version failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    tbsys::CThreadGuard lock(&lock_);
    if (latest_version_ >= schema.get_version())
    {
      TBSYS_LOG(WARN, "check schema version failed:latest[%ld], schema[%ld]",
          latest_version_, schema.get_version());
      ret = OB_OLD_SCHEMA_VERSION;
    }
    else
    {
      // find replace position for the new schema
      // at first not inited, then not used and the oldest version pos
      int64_t pos = find_replace_pos();
      if (pos >= 0)
      {
        user_schemas_[pos].schema = schema;
        user_schemas_[pos].ref_count = 1;
        update_timestamp_ = tbsys::CTimeUtil::getTime();
        latest_version_ = schema.get_version();
        if (NULL != manager)
        {
          user_schemas_[pos].ref_count = 2;
          *manager = &user_schemas_[pos].schema;
        }
      }
      else
      {
        ret = OB_NO_EMPTY_ENTRY;
      }
    }
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(TRACE, "==========print schema version[%ld] start==========",schema.get_version());
    schema.print_debug_info();
    TBSYS_LOG(TRACE, "==========print schema version[%ld] end==========",schema.get_version());
  }
  //add:e
  return ret;
}

int64_t ObMergerSchemaManager::find_replace_pos(void) const
{
  int64_t pos = -1;
  int64_t old_version = MAX_INT64_VALUE;
  for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
  {
    // empty pos
    if (0 == user_schemas_[i].ref_count)
    {
      pos = i;
      break;
    }
    // not empty but not using right now
    else if (1 == user_schemas_[i].ref_count)
    {
      if (old_version > user_schemas_[i].schema.get_version())
      {
        old_version = user_schemas_[i].schema.get_version();
        pos = i;
      }
      continue;
    }
  }
  return pos;
}

const ObSchemaManagerV2 * ObMergerSchemaManager::get_schema(const common::ObString & table_name)
{
  const ObSchemaManagerV2 * schema = NULL;
  const ObTableSchema * table = sys_schema_.schema.get_table_schema(table_name);
  if (table != NULL)
  {
    // get system table schema
    schema = &sys_schema_.schema;
  }
  else
  {
    // get local newest user table schema
    schema = get_user_schema(0);
  }
  return schema;
}

const ObSchemaManagerV2 * ObMergerSchemaManager::get_schema(const uint64_t table_id)
{
  const ObSchemaManagerV2 * schema = NULL;
  const ObTableSchema * table = sys_schema_.schema.get_table_schema(table_id);
  if (table != NULL)
  {
    // get system table schema
    schema = &sys_schema_.schema;
  }
  else
  {
    // get local newest user table schema
    schema = get_user_schema(0);
  }
  return schema;
}

const ObSchemaManagerV2 * ObMergerSchemaManager::get_user_schema(const int64_t version)
{
  const ObSchemaManagerV2 * schema = NULL;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
  }
  else
  {
    int64_t new_version = version;
    // get the latest version
    tbsys::CThreadGuard lock(&lock_);
    if (0 == version)
    {
      new_version = latest_version_;
    }
    //
    for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
    {
      if (0 == user_schemas_[i].ref_count)
      {
        continue;
      }
      else
      {
        if (user_schemas_[i].schema.get_version() == new_version)
        {
          ++user_schemas_[i].ref_count;
          schema = &user_schemas_[i].schema;
        }
      }
    }
  }
  return schema;
}

int ObMergerSchemaManager::release_schema(const ObSchemaManagerV2 * schema)
{
  int ret = OB_RELEASE_SCHEMA_ERROR;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat or release version failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (NULL == schema)
  {
    TBSYS_LOG(WARN, "check input param schema failed");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (schema != &(sys_schema_.schema))
  {
    bool find = false;
    tbsys::CThreadGuard lock(&lock_);
    for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
    {
      // 0 is empty, 1 is not used
      if (user_schemas_[i].ref_count <= 1)
      {
        continue;
      }
      else
      {
        if (&user_schemas_[i].schema == schema)
        {
          --user_schemas_[i].ref_count;
          assert(user_schemas_[i].ref_count > 0);
          find = true;
          ret = OB_SUCCESS;
          break;
        }
      }
    }
    assert(find == true);
  }
  else
  {
    ret = OB_SUCCESS;
  }
  
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "fail to release schema. schema=%p", schema);
  }
  return ret;
}

int64_t ObMergerSchemaManager::get_oldest_version(void) const
{
  int64_t version = MAX_INT64_VALUE;
  tbsys::CThreadGuard lock(&lock_);
  for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
  {
    if (0 == user_schemas_[i].ref_count)
    {
      continue;
    }
    else
    {
      if (user_schemas_[i].schema.get_version() < version)
      {
        version = user_schemas_[i].schema.get_version();
      }
    }
  }
  return version;
}

int ObMergerSchemaManager::print_info(void) const
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    tbsys::CThreadGuard lock(&lock_);
    TBSYS_LOG(INFO, "update[%ld], latest[%ld]", update_timestamp_, latest_version_);
    for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
    {
      if (0 == user_schemas_[i].ref_count)
      {
        continue;
      }
      else
      {
        TBSYS_LOG(INFO, "schema ref_count[%ld]", user_schemas_[i].ref_count);
        user_schemas_[i].schema.print_info();
      }
    }
  }
  return ret;
}
