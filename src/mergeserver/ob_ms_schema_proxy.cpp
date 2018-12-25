#include "common/ob_schema_manager.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_schema_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerSchemaProxy::ObMergerSchemaProxy()
{
  root_rpc_ = NULL;
  schema_manager_ = NULL;
  schema_service_ = NULL;//add zhaoqiong [Schema Manager] 20150327
}

//mod zhaoqiong [Schema Manager] 20150327:b
//ObMergerSchemaProxy::ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, ObMergerSchemaManager * schema)
ObMergerSchemaProxy::ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, ObMergerSchemaManager * schema, common::ObSchemaServiceImpl *schema_service)
//mod:e
{
  root_rpc_ = rpc;
  fetch_schema_timestamp_ = 0;
  schema_manager_ = schema;
  schema_service_ = schema_service;//add zhaoqiong [Schema Manager] 20150327
}

ObMergerSchemaProxy::~ObMergerSchemaProxy()
{
}

int ObMergerSchemaProxy::get_schema(const ObString & table_name, const int64_t timestamp, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    switch (timestamp)
    {
    case LOCAL_NEWEST:
      {
        // get local newest version sys or user table schema
        *manager = schema_manager_->get_schema(table_name);
        if (NULL == *manager)
        {
          ret = get_user_schema(timestamp, manager);
          TBSYS_LOG(INFO, "force get user schema, ts=%ld err=%d manager=%p", timestamp, ret, *manager);
        }
        break;
      }
    default:
      {
        // fetch only new user table schema
        ret = get_user_schema(timestamp, manager);
      }
    }
    // check shema data
    if ((ret != OB_SUCCESS) || (NULL == *manager))
    {
      TBSYS_LOG(WARN, "check get schema failed:schema[%p], version[%ld], ret[%d]",
          *manager, timestamp, ret);
    }
  }
  return ret;
}

int ObMergerSchemaProxy::get_user_schema(const int64_t timestamp, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  // check update timestamp LEAST_FETCH_SCHMEA_INTERVAL
  if (tbsys::CTimeUtil::getTime() - fetch_schema_timestamp_ < LEAST_FETCH_SCHEMA_INTERVAL)
  {
    TBSYS_LOG(WARN, "check last fetch schema timestamp is too nearby:version[%ld]", timestamp);
    ret = OB_OP_NOT_ALLOW;
  }
  else
  {
    int64_t new_version = 0;
    ret = root_rpc_->fetch_schema_version(new_version);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
    }
    else if (new_version <= timestamp)
    {
      TBSYS_LOG(DEBUG, "check local version not older than root version:local[%ld], root[%ld]",
        timestamp, new_version);
      ret = OB_NO_NEW_SCHEMA;
    }
    else
    {
      ret = fetch_user_schema(new_version, manager);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch new schema failed:local[%ld], root[%ld], ret[%d]",
          timestamp, new_version, ret);
      }
    }
  }
  return ret;
}

int ObMergerSchemaProxy::get_schema_version(int64_t & timestamp)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = root_rpc_->fetch_schema_version(timestamp);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
    }
  }
  return ret;
}

int ObMergerSchemaProxy::fetch_user_schema(const int64_t version, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(WARN, "check inner stat or shema manager param failed:manager[%p]", manager);
    ret = OB_ERROR;
  }
  else if (tbsys::CTimeUtil::getTime() - fetch_schema_timestamp_ < LEAST_FETCH_SCHEMA_INTERVAL)
  {
    TBSYS_LOG(WARN, "check last fetch schema timestamp is too nearby:version[%ld]", version);
    ret = OB_OP_NOT_ALLOW;
  }
  else
  {
    // maybe get local version is ok
    tbsys::CThreadGuard lock(&schema_lock_);
    if (schema_manager_->get_latest_version() >= version)
    {
      *manager = schema_manager_->get_user_schema(0);
      if (NULL == *manager)
      {
        TBSYS_LOG(WARN, "get latest but local schema failed:schema[%p], latest[%ld]",
          *manager, schema_manager_->get_latest_version());
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "get new schema is fetched by other thread:schema[%p], latest[%ld]",
          *manager, (*manager)->get_version());
      }
    }
    //add zhaoqiong [Schema Manager] 20150519:b
    else if (schema_manager_->get_latest_version() == -1)
    {
      //ms start, fetch full schema from rs
      ret = root_rpc_->fetch_newest_schema(schema_manager_, manager);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch newest schema from root server failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "fetch newest schema from root server success");
        fetch_schema_timestamp_ = tbsys::CTimeUtil::getTime();
      }
    }
    //add:e
    else
    {
      //mod zhaoqiong [Schema Manager] 20150421:b
//      ret = root_rpc_->fetch_newest_schema(schema_manager_, manager);
//      if (ret != OB_SUCCESS)
//      {
//        TBSYS_LOG(WARN, "fetch newest schema from root server failed:ret[%d]", ret);
//      }
//      else
//      {
//        fetch_schema_timestamp_ = tbsys::CTimeUtil::getTime();
//      }
      const ObSchemaManagerV2 * schema_tmp = NULL;
      bool need_fetch_schema = true;
      ObSchemaManagerV2 * schema = NULL;
      char * temp = NULL;
      temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_MS_RPC);
      if (NULL == temp)
      {
        TBSYS_LOG(ERROR, "check ob malloc failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (schema = new(temp) ObSchemaManagerV2))
      {
        TBSYS_LOG(ERROR, "check replacement new schema failed:schema[%p]", schema);
        ret = OB_INNER_STAT_ERROR;
      }
      else if (NULL != (schema_tmp = schema_manager_->get_user_schema(0)))
      {
        //fetch schema mutator
        ObSchemaMutator schema_mutator;
        ret = schema_service_->fetch_schema_mutator(schema_manager_->get_latest_version(), version, schema_mutator);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fetch schema mutator from sys tables failed:ret[%d]", ret);
        }
        else if (!schema_mutator.get_refresh_schema())
        {
          need_fetch_schema = false;
          schema->copy_without_sort(*schema_tmp);
          if (OB_SUCCESS != (ret = schema->apply_schema_mutator(schema_mutator)))
          {
            TBSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                      schema_mutator.get_start_version(), schema_mutator.get_end_version());
          }
        }

        if (OB_SUCCESS != (ret = schema_manager_->release_schema(schema_tmp)))
        {
          TBSYS_LOG(WARN, "can not release schema version [%ld], ret[%d]",schema_manager_->get_latest_version(), ret);
        }
      }

      if (OB_SUCCESS == ret && need_fetch_schema)
      {
        //fetch full schema from system tables
        if (OB_SUCCESS !=  (ret = schema_service_->get_schema(false, *schema)))
        {
          TBSYS_LOG(WARN, "get schema from system table failed:ret[%d]", ret);
        }
        else
        {
          schema->set_version(version);
        }
      }
      //mod:e
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = schema_manager_->add_schema(*schema, manager)))
        {
          TBSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
          if (OB_NO_EMPTY_ENTRY != ret)
          {
            ret = OB_SUCCESS;
            *manager = schema_manager_->get_user_schema(0);
            if (NULL == *manager)
            {
              TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
                  *manager, schema_manager_->get_latest_version());
              ret = OB_INNER_STAT_ERROR;
            }
          }
        }
        else
        {
          fetch_schema_timestamp_ = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "fetch mutator and add new schema succ:version[%ld]", schema->get_version());
        }
      }

      if (schema != NULL)
      {
        schema->~ObSchemaManagerV2();
      }
      if (temp != NULL)
      {
        ob_free(temp);
        temp = NULL;
      }
    }
  }
  return ret;
}

int ObMergerSchemaProxy::release_schema(const ObSchemaManagerV2 * manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ret = schema_manager_->release_schema(manager);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "release scheam failed:schema[%p], timestamp[%ld]",
          manager, manager->get_version());
    }
  }
  return ret;
}

