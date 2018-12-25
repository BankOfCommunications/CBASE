////===================================================================
 //
 // ob_log_spec.cpp liboblog / Oceanbase
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

#include "tbsys.h"
#include "ob_log_spec.h"
#include "ob_log_utils.h"
#include "libobsql.h"         // ObSQLConfig

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogSpec::ObLogSpec() : inited_(false),
                             need_query_back_(false),
                             config_(),
                             rpc_stub_(NULL),
                             server_selector_(NULL),
                             schema_getter_(NULL),
                             dbname_builder_(NULL),
                             tbname_builder_(NULL),
                             log_fetcher_(NULL),
                             log_filter_(NULL),
                             query_engine_(NULL),
                             log_router_(NULL),
                             start_checkpoint_(0)
    {
      OBLOG_COMMON.reset();
    }

    ObLogSpec::~ObLogSpec()
    {
      destroy();

      OBLOG_COMMON.reset();
    }

    int ObLogSpec::init(const char *config, const uint64_t position, ERROR_CALLBACK err_cb, ObLogPositionType pos_type)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == config)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = config_.init(config)))
      {
        TBSYS_LOG(WARN, "ObLogConfig init fail, config=%s, ret=%d", config, ret);
      }
      else if (OB_SUCCESS != (ret = init_internal_(position, pos_type, err_cb)))
      {
        TBSYS_LOG(WARN, "init_internal_ fail, position=%lu, pos_type=%s, err_cb=%p, ret=%d",
            position, print_pos_type(pos_type), err_cb, ret);
      }

      TBSYS_LOG(INFO, "init log spec: config=%s, position=%lu, pos_type=%s, err_cb=%p, ret=%d",
          config, position, print_pos_type(pos_type), err_cb, ret);
      return ret;
    }

    int ObLogSpec::init(const std::map<std::string, std::string>& configs,
        const uint64_t position,
        ERROR_CALLBACK err_cb,
        ObLogPositionType pos_type)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = config_.init(configs)))
      {
        TBSYS_LOG(WARN, "ObLogConfig init fail, config=%p, ret=%d", &configs, ret);
      }
      else if (OB_SUCCESS != (ret = init_internal_(position, pos_type, err_cb)))
      {
        TBSYS_LOG(WARN, "init_internal_ fail, position=%lu, pos_type=%s, err_cb=%p, ret=%d",
            position, print_pos_type(pos_type), err_cb, ret);
      }

      TBSYS_LOG(INFO, "init log spec: configs=%p, position=%lu, pos_type=%s, err_cb=%p, ret=%d",
          &configs, position, print_pos_type(pos_type), err_cb, ret);

      return ret;
    }

    void ObLogSpec::destroy()
    {
      inited_ = false;

      destroy_components_();
      deconstruct_components_();
      destroy_obsql_();
      config_.destroy();

      TBSYS_LOG(INFO, "ObLogSpec destroyed");
    }

    int ObLogSpec::launch()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL != query_engine_ && OB_SUCCESS != (ret = query_engine_->start()))
      {
        TBSYS_LOG(ERROR, "start mysql query engine fail, ret=%d", ret);
      }
      else
      {
        ret = log_router_->launch();
      }
      return ret;
    }

    void ObLogSpec::stop()
    {
      if (inited_)
      {
        log_fetcher_->stop();
        log_router_->stop();

        if (NULL != query_engine_)
        {
          query_engine_->stop();
        }
      }
    }

    IObLogRouter *ObLogSpec::get_log_router()
    {
      IObLogRouter *ret = NULL;
      if (inited_)
      {
        ret = log_router_;
      }
      return ret;
    }

    IObLogSchemaGetter *ObLogSpec::get_schema_getter()
    {
      IObLogSchemaGetter *ret = NULL;
      if (inited_)
      {
        ret = schema_getter_;
      }
      return ret;
    }

    IObLogDBNameBuilder *ObLogSpec::get_db_name_builder()
    {
      IObLogDBNameBuilder *ret = NULL;
      if (inited_)
      {
        ret = dbname_builder_;
      }
      return ret;
    }

    IObLogTBNameBuilder *ObLogSpec::get_tb_name_builder()
    {
      IObLogTBNameBuilder *ret = NULL;
      if (inited_)
      {
        ret = tbname_builder_;
      }
      return ret;
    }

    IObLogFilter *ObLogSpec::get_log_filter()
    {
      IObLogFilter *ret = NULL;
      if (inited_)
      {
        ret = log_filter_;
      }
      return ret;
    }

    IObLogMysqlQueryEngine *ObLogSpec::get_mysql_query_engine()
    {
      IObLogMysqlQueryEngine *ret = NULL;

      if (inited_)
      {
        ret = query_engine_;
      }

      return ret;
    }

#define CONSTRUCT(v, type) \
    if (OB_SUCCESS == ret \
        && NULL == (v = new(std::nothrow) type())) \
    { \
      TBSYS_LOG(WARN, "construct %s fail", #type); \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
    }

#define RCALL(f, v, args...) \
    if (OB_SUCCESS == ret \
        && OB_SUCCESS != (ret = v->f(args))) \
    { \
      TBSYS_LOG(WARN, "call %s this=%p fail, ret=%d", #f, v, ret); \
    }

#define CALL(f, v) \
    if (NULL != v) \
    { \
      v->f(); \
    }

#define DECONSTRUCT(v) \
    if (NULL != v) \
    { \
      delete v; \
      v = NULL; \
    }

    int ObLogSpec::init_internal_(const uint64_t position, ObLogPositionType pos_type, ERROR_CALLBACK err_cb)
    {
      OB_ASSERT(! inited_);

      int ret = OB_SUCCESS;
      need_query_back_ = (0 != config_.get_query_back());

      // 设置错误处理函数
      OBLOG_COMMON.set_err_callback(err_cb);

      if (POS_TYPE_UNKNOWN >= pos_type || POS_TYPE_NUMBER <= pos_type)
      {
        TBSYS_LOG(WARN, "invalid position type: %d, which should be in (%d, %d)", pos_type, POS_TYPE_UNKNOWN, POS_TYPE_NUMBER);
        ret = OB_INVALID_ARGUMENT;
      }
      // NOTE: 目前仅支持位点启动方式
      else if (POS_TYPE_CHECKPOINT != pos_type)
      {
        TBSYS_LOG(ERROR, "position type %s(%d) not supported", print_pos_type(pos_type), pos_type);
        ret = OB_NOT_SUPPORTED;
      }
      else if (OB_SQL_SUCCESS != (ret = init_obsql_()))
      {
        TBSYS_LOG(ERROR, "init_obsql fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = construct_components_()))
      {
        TBSYS_LOG(ERROR, "construct components fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = init_components_(position)))
      {
        TBSYS_LOG(ERROR, "init components fail, ret=%d", ret);
      }
      else
      {
        start_checkpoint_ = position;
        inited_ = true;
      }

      if (OB_SUCCESS != ret)
      {
        destroy_components_();
        deconstruct_components_();
        destroy_obsql_();
      }

      return ret;
    }

    int ObLogSpec::init_obsql_()
    {
      int ret = OB_SUCCESS;

      ObSQLConfig obsql_config;
      memset(&obsql_config, 0, sizeof(ObSQLConfig));

      // NOTE: Read slave cluster only
      obsql_config.read_slave_only_ = true;

      int32_t min_conn = config_.get_obsql_min_conn();
      int32_t max_conn = config_.get_obsql_max_conn();

      min_conn = 0 < min_conn ? min_conn : OB_SQL_DEFAULT_MIN_CONN;

      if (0 >= max_conn)
      {
        max_conn = (2 * config_.get_query_back_thread_num());
        max_conn = (OB_SQL_DEFAULT_MAX_CONN > max_conn) ? OB_SQL_DEFAULT_MAX_CONN : max_conn;
      }

      if (min_conn > max_conn)
      {
        TBSYS_LOG(WARN, "libobsql min_conn(%d) > max_conn(%d), set max_conn=%d",
            min_conn, max_conn, min_conn);

        max_conn = min_conn;
      }

      obsql_config.min_conn_ = min_conn;
      obsql_config.max_conn_ = max_conn;
      TBSYS_LOG(INFO, "load libobsql min_conn=%d, max_conn=%d", obsql_config.min_conn_, obsql_config.max_conn_);

      const char *user_name = config_.get_mysql_user();
      const char *passwd = config_.get_mysql_password();
      const char *cluster_address = config_.get_cluster_address();

      if (0 >= snprintf(obsql_config.username_, sizeof(obsql_config.username_), "%s", user_name))
      {
        TBSYS_LOG(ERROR, "invalid configuration: libobsql user name='%s' is invalid", user_name);
        ret = OB_INVALID_ARGUMENT;
      }
      // NOTE: Password can be empty
      else if (0 > snprintf(obsql_config.passwd_, sizeof(obsql_config.passwd_), "%s", passwd))
      {
        TBSYS_LOG(ERROR, "invalid configuration: libobsql passwd='%s' is invalid", passwd);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= snprintf(obsql_config.cluster_address_, sizeof(obsql_config.cluster_address_), "%s", cluster_address))
      {
        TBSYS_LOG(ERROR, "invalid configuration: libobsql cluster_address='%s' is invalid", cluster_address);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SQL_SUCCESS != ob_sql_init(obsql_config))
      {
        TBSYS_LOG(ERROR, "ob_sql_init fail");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        TBSYS_LOG(INFO, "libobsql inited success");
        ret = OB_SUCCESS;
      }

      return ret;
    }

    void ObLogSpec::destroy_obsql_()
    {
      ob_sql_destroy();
      TBSYS_LOG(INFO, "obsql destroyed");
    }

    int ObLogSpec::construct_components_()
    {
      int ret = OB_SUCCESS;
      CONSTRUCT(rpc_stub_,        ObLogRpcStub);
      CONSTRUCT(server_selector_, ObLogServerSelector);
      CONSTRUCT(schema_getter_,   ObLogSchemaGetter);
      CONSTRUCT(dbname_builder_,  ObLogDBNameBuilder);
      CONSTRUCT(tbname_builder_,  ObLogTBNameBuilder);
      CONSTRUCT(log_fetcher_,     ObLogFetcher);
      CONSTRUCT(log_filter_,      ObLogTableFilter);
      CONSTRUCT(log_router_,      ObLogRouter);

      if (need_query_back_)
      {
        CONSTRUCT(query_engine_, ObLogMysqlQueryEngine);
      }

      return ret;
    }

    int ObLogSpec::init_components_(const uint64_t checkpoint)
    {
      int ret = OB_SUCCESS;
      RCALL(init,       rpc_stub_);
      RCALL(init,       server_selector_,   config_);
      RCALL(init,       schema_getter_,     server_selector_, rpc_stub_, config_);
      RCALL(init,       dbname_builder_,    config_);
      RCALL(init,       tbname_builder_,    config_);
      RCALL(init,       log_fetcher_,       config_, server_selector_, rpc_stub_, checkpoint);
      RCALL(init,       log_filter_,        log_fetcher_, config_, schema_getter_);
      RCALL(init,       log_router_,        config_, log_filter_, schema_getter_);

      if (need_query_back_)
      {
        RCALL(init,       query_engine_,      config_, log_filter_);
      }

      return ret;
    }

    void ObLogSpec::destroy_components_()
    {
      TBSYS_LOG(INFO, "destroy components");

      if (need_query_back_)
      {
        CALL(destroy,     query_engine_);
      }

      CALL(destroy,     log_router_);
      CALL(destroy,     log_filter_);
      CALL(destroy,     log_fetcher_);
      CALL(destroy,     tbname_builder_);
      CALL(destroy,     dbname_builder_);
      CALL(destroy,     schema_getter_);
      CALL(destroy,     server_selector_);
      CALL(destroy,     rpc_stub_);
    }

    void ObLogSpec::deconstruct_components_()
    {
      TBSYS_LOG(INFO, "deconstruct components");

      if (need_query_back_)
      {
        DECONSTRUCT(query_engine_);
      }

      DECONSTRUCT(log_router_);
      DECONSTRUCT(log_filter_);
      DECONSTRUCT(log_fetcher_);
      DECONSTRUCT(tbname_builder_);
      DECONSTRUCT(dbname_builder_);
      DECONSTRUCT(schema_getter_);
      DECONSTRUCT(server_selector_);
      DECONSTRUCT(rpc_stub_);
    }
  }
}

