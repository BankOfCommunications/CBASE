////===================================================================
 //
 // ob_log_meta_manager.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2014-01-17 by Xiuming (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   Schema Getter Implementation File
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "common/ob_define.h"
#include "common/ob_mod_define.h"     // ObModIds
#include "ob_log_server_selector.h"   // IObLogServerSelector IObLogRpcStub
#include "ob_log_config.h"            // ObLogConfig
#include "ob_log_schema_getter.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogSchemaGetter::ObLogSchemaGetter() : inited_(false),
                                             server_selector_(NULL),
                                             rpc_stub_(NULL),
                                             schema_get_lock_(),
                                             schema_refresh_lock_(),
                                             multi_refresh_mutex_(),
                                             cur_schema_(NULL),
                                             schema_allocator_()
    {
      schema_allocator_.set_mod_id(ObModIds::OB_LOG_SCHEMA_GETTER);
    }

    ObLogSchemaGetter::~ObLogSchemaGetter()
    {
      destroy();
    }

    int ObLogSchemaGetter::init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub, ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      ObLogSchema *tmp_schema = NULL;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (server_selector_ = server_selector) || NULL == (rpc_stub_ = rpc_stub))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = schema_allocator_.init(OB_LOG_SCHEMA_SIZE_TOTAL_LIMIT,
              OB_LOG_SCHEMA_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(ERROR, "initialize schema allocator fail, ret=%d, total_limit=%ld, "
            "hold_limit=%ld, page_size=%ld",
            ret,
            OB_LOG_SCHEMA_SIZE_TOTAL_LIMIT,
            OB_LOG_SCHEMA_SIZE_HOLD_LIMIT,
            ALLOCATOR_PAGE_SIZE);
      }
      else if (OB_SUCCESS != (ret = fetch_schema_(tmp_schema)))
      {
        TBSYS_LOG(ERROR, "fetch_schema_ fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = config.refresh_tb_select(tmp_schema)))
      {
        TBSYS_LOG(ERROR, "refresh_tb_select fail, tmp_schema=%p, schema version=%ld, ret=%d",
            tmp_schema, tmp_schema->get_version(), ret);
      }
      else
      {
        cur_schema_ = tmp_schema;
        cur_schema_->ref();

        inited_ = true;
      }

      if (OB_SUCCESS != ret)
      {
        if (NULL != tmp_schema)
        {
          free_schema_(tmp_schema);
          tmp_schema = NULL;
        }

        destroy();
      }
      return ret;
    }

    void ObLogSchemaGetter::destroy()
    {
      inited_ = false;

      if (NULL != cur_schema_)
      {
        free_schema_(cur_schema_);
        cur_schema_ = NULL;
      }

      rpc_stub_ = NULL;
      server_selector_ = NULL;

      schema_allocator_.destroy();
    }

    const ObLogSchema *ObLogSchemaGetter::get_schema()
    {
      ObLogSchema *ret = NULL;
      if (inited_)
      {
        SpinRLockGuard guard(schema_get_lock_);
        ret = cur_schema_;
        ret->ref();
      }
      return ret;
    }

    void ObLogSchemaGetter::revert_schema(const ObLogSchema *schema)
    {
      ObLogSchema *ret = const_cast<ObLogSchema*>(schema);
      if (NULL != ret)
      {
        SpinRLockGuard guard(schema_get_lock_);
        if (0 == ret->deref())
        {
          free_schema_(ret);
          ret = NULL;
        }
      }
    }

    int ObLogSchemaGetter::get_schema_newer_than(const ObLogSchema *old_schema, const ObLogSchema *&new_schema)
    {
      int ret = OB_SUCCESS;
      bool need_refresh_schema = false;

      if (NULL == old_schema)
      {
        TBSYS_LOG(ERROR, "invalid argument, schema is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (new_schema = get_schema()))
      {
        TBSYS_LOG(ERROR, "get_schema fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (! new_schema->is_newer_than(*old_schema))
      {
        revert_schema(new_schema);
        new_schema = NULL;
        need_refresh_schema = true;
      }

      if (OB_SUCCESS == ret && need_refresh_schema)
      {
        // NOTE: Lock here to prevent refreshing schema by more than one caller
        // at the same time
        tbsys::CThreadGuard guard(&multi_refresh_mutex_);

        // Get schema again
        if (NULL == (new_schema = get_schema()))
        {
          TBSYS_LOG(ERROR, "get_schema fail, return NULL");
          ret = OB_ERR_UNEXPECTED;
        }
        // Refresh schema if schema is still the old one.
        else if (! new_schema->is_newer_than(*old_schema))
        {
          TBSYS_LOG(INFO, "force to refresh schema: current schema=(%ld,%ld), asked schema=(%ld,%ld)",
              new_schema->get_version(), new_schema->get_timestamp(),
              old_schema->get_version(), old_schema->get_timestamp());

          revert_schema(new_schema);
          new_schema = NULL;

          if (OB_SUCCESS != (ret = force_refresh()))
          {
            TBSYS_LOG(ERROR, "force_refresh fail, ret=%d", ret);
          }
          // Get newer schema after refresh
          else if (NULL == (new_schema = get_schema()))
          {
            TBSYS_LOG(ERROR, "get_schema fail, return NULL");
            ret = OB_ERR_UNEXPECTED;
          }
          else if (! new_schema->is_newer_than(*old_schema))
          {
            TBSYS_LOG(ERROR, "refreshed schema (%ld,%ld) is not newer than old schema (%ld,%ld)",
                new_schema->get_version(), new_schema->get_timestamp(),
                old_schema->get_version(), old_schema->get_timestamp());

            // 如果更新的Schema不满足要求，则要求外部重试
            ret = OB_NEED_RETRY;
          }
          else
          {
            // do nothing on success
          }
        }
      }

      if (OB_SUCCESS == ret && NULL == new_schema)
      {
        TBSYS_LOG(ERROR, "unexcepted error: fail to get newer schema");
        ret = OB_ERR_UNEXPECTED;
      }

      if (NULL != new_schema && OB_SUCCESS != ret)
      {
        revert_schema(new_schema);
        new_schema = NULL;
      }

      return ret;
    }

    int ObLogSchemaGetter::force_refresh()
    {
      int ret = OB_SUCCESS;

      if (! inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ObSpinLockGuard guard(schema_refresh_lock_);

        ret = refresh_();
      }

      return ret;
    }

    int ObLogSchemaGetter::refresh_schema_greater_than(int64_t base_version)
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_VERSION == base_version)
      {
        TBSYS_LOG(ERROR, "invalid argument: base_version=%ld", base_version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (! inited_)
      {
        TBSYS_LOG(ERROR, "schema getter has not been initialized");
        ret = OB_NOT_INIT;
      }
      else if (get_current_schema_version_() <= base_version)
      {
        // NOTE: Lock here to prevent refreshing schema by more than one caller
        // at the same time
        tbsys::CThreadGuard guard(&multi_refresh_mutex_);

        if (get_current_schema_version_() <= base_version)
        {
          ObSpinLockGuard guard(schema_refresh_lock_);

          ret = refresh_(base_version);

          int64_t current_version = get_current_schema_version_();
          if (OB_SUCCESS != ret || current_version <= base_version)
          {
            TBSYS_LOG(ERROR, "refresh schema greater than version=%ld fail, current version=%ld, ret=%d",
                base_version, current_version, ret);

            // 如果目标Schema不满足要求，则要求外部重试
            ret = OB_NEED_RETRY;
          }
        }
      }

      return ret;
    }

    const int64_t ObLogSchemaGetter::get_current_schema_version_()
    {
      OB_ASSERT(inited_ && NULL != cur_schema_);

      SpinRLockGuard guard(schema_get_lock_);
      return cur_schema_->get_version();
    }

    int ObLogSchemaGetter::refresh_(int64_t base_version)
    {
      OB_ASSERT(inited_);

      int ret = OB_SUCCESS;
      ObLogSchema *next_schema = NULL;
      int64_t refresh_interval = 0;
      int64_t start_time = tbsys::CTimeUtil::getTime();

      while (OB_SUCCESS == ret)
      {
        if (REACH_TIME_INTERVAL(RS_ADDR_REFRESH_INTERVAL))
        {
          server_selector_->refresh();
        }

        if (OB_SUCCESS != (ret = fetch_schema_(next_schema)))
        {
          TBSYS_LOG(ERROR, "fetch_schema_ fail, ret=%d", ret);
          break;
        }

        // NOTE: 要求目标版本大于base_version
        if (OB_INVALID_VERSION == base_version
            || base_version < next_schema->get_version())
        {
          break;
        }

        free_schema_(next_schema);
        next_schema = NULL;

        refresh_interval = tbsys::CTimeUtil::getTime() - start_time;

        if (SCHEMA_REFRESH_FAIL_RETRY_TIMEOUT <= refresh_interval)
        {
          TBSYS_LOG(ERROR, "refresh schema to (version > %ld) fail, TIMEOUT, refresh_interval=%ld/%ld",
              base_version, refresh_interval, SCHEMA_REFRESH_FAIL_RETRY_TIMEOUT);

          // 更新Schema失败时，要求外部重试
          ret = OB_NEED_RETRY;
          break;
        }

        usleep(SCHEMA_FETCH_FAIL_TIMEWAIT);
      }

      if (OB_SUCCESS == ret)
      {
        OB_ASSERT(NULL != next_schema
            && (OB_INVALID_VERSION == base_version || base_version < next_schema->get_version()));

        if (! next_schema->is_newer_than(*cur_schema_))
        {
          TBSYS_LOG(ERROR, "refresh schema fail, refreshed schema (%ld,%ld) is not newer than "
              "current schema (%ld,%ld), base_version=%ld",
              cur_schema_->get_version(), cur_schema_->get_timestamp(),
              next_schema->get_version(), next_schema->get_timestamp(),
              base_version);

          free_schema_(next_schema);
          next_schema = NULL;
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          TBSYS_LOG(INFO, "update schema from (%ld,%ld) to (%ld,%ld), base_version=%ld, refresh_interval=%ld",
              cur_schema_->get_version(), cur_schema_->get_timestamp(),
              next_schema->get_version(), next_schema->get_timestamp(),
              base_version, refresh_interval);

          next_schema->ref();

          // Update schema
          SpinWLockGuard guard(schema_get_lock_);
          ObLogSchema *prev_schema = cur_schema_;
          cur_schema_ = next_schema;

          // Destroy old schema if reference becomes 0
          if (0 == prev_schema->deref())
          {
            free_schema_(prev_schema);
            prev_schema = NULL;
          }
        }
      }

      return ret;
    }

    int ObLogSchemaGetter::fetch_schema_(ObLogSchema *&schema)
    {
      OB_ASSERT(NULL == schema);

      ObServer rs_addr;
      int ret = OB_SUCCESS;
      int64_t retry_times = 0;
      bool change_rs_addr = false;
      ObLogSchema *tmp_schema = NULL;

      if (OB_SUCCESS != (ret = alloc_schema_(tmp_schema)))
      {
        TBSYS_LOG(ERROR, "alloc_schema_ fail, ret=%d", ret);
      }
      else
      {
        while (MAX_SCHEMA_FETCH_FAIL_RETRY_TIMES >= ++retry_times)
        {
          if (OB_SUCCESS != (ret = server_selector_->get_rs(rs_addr, change_rs_addr)))
          {
            TBSYS_LOG(WARN, "get rs addr failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = rpc_stub_->fetch_schema(rs_addr, *tmp_schema, FETCH_SCHEMA_TIMEOUT)))
          {
            TBSYS_LOG(WARN, "fetch schema fail ret=%d", ret);
          }

          if (OB_SUCCESS == ret)
          {
            break;
          }

          // 获取Schema失败后，切换RS
          change_rs_addr = true;

          if (MAX_SCHEMA_FETCH_FAIL_RETRY_TIMES <= retry_times)
          {
            TBSYS_LOG(ERROR, "fetch schema fail, retry_times=%ld/%ld, ret=%d",
                retry_times, MAX_SCHEMA_FETCH_FAIL_RETRY_TIMES, ret);

            // 获取Schema失败后，要求外部重试
            ret = OB_NEED_RETRY;
            break;
          }

          usleep(SCHEMA_FETCH_FAIL_TIMEWAIT);
        }
      }

      if (OB_SUCCESS == ret)
      {
        schema = tmp_schema;

        if (1 < retry_times)
        {
          TBSYS_LOG(WARN, "fetch schema success after retry for times=%ld/%ld",
              retry_times, MAX_SCHEMA_FETCH_FAIL_RETRY_TIMES);
        }
      }
      else if (NULL != tmp_schema)
      {
        free_schema_(tmp_schema);
        tmp_schema = NULL;
      }

      return ret;
    }

    int ObLogSchemaGetter::alloc_schema_(ObLogSchema *&schema)
    {
      int ret = OB_SUCCESS;
      void *buffer = NULL;
      ObLogSchema *tmp_schema = NULL;

      if (NULL == (buffer = schema_allocator_.alloc(sizeof(ObLogSchema))))
      {
        TBSYS_LOG(ERROR, "allocate memory form ObLogSchema fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (tmp_schema = new(buffer) ObLogSchema()))
      {
        TBSYS_LOG(WARN, "construct schema fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        schema = tmp_schema;
      }

      return ret;
    }


    void ObLogSchemaGetter::free_schema_(ObLogSchema *schema)
    {
      if (NULL != schema)
      {
        schema->~ObLogSchema();
        schema_allocator_.free(reinterpret_cast<char *>(schema));
        schema = NULL;
      }
    }
  }
}
