////===================================================================
 //
 // ob_log_spec.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_SPEC_H_
#define  OCEANBASE_LIBOBLOG_SPEC_H_

#include "common/ob_define.h"
#include "liboblog.h"
#include "ob_log_config.h"
#include "ob_log_fetcher.h"
#include "ob_log_filter.h"
#include "ob_log_formator.h"
#include "ob_log_meta_manager.h"    // IObLogDBNameBuilder, IObLogTBNameBuilder
#include "ob_log_schema_getter.h"   // IObLogSchemaGetter
#include "ob_log_partitioner.h"
#include "ob_log_router.h"
#include "ob_log_rpc_stub.h"
#include "ob_log_server_selector.h"
#include "ob_log_mysql_query_engine.h"          // IObLogMysqlQueryEngine

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogSpec : public IObLogSpec
    {
      public:
        static const int32_t OB_SQL_DEFAULT_MIN_CONN = 1;
        static const int32_t OB_SQL_DEFAULT_MAX_CONN = 8;

      public:
        ObLogSpec();
        ~ObLogSpec();

      public:
        int init(const char *config,
            const uint64_t log_position,
            ERROR_CALLBACK err_cb = NULL,
            ObLogPositionType pos_type = POS_TYPE_CHECKPOINT);
        int init(const std::map<std::string, std::string>& configs,
            const uint64_t log_position,
            ERROR_CALLBACK err_cb = NULL,
            ObLogPositionType pos_type = POS_TYPE_CHECKPOINT);
        void destroy();
        int launch();
        void stop();
      public:
        IObLogRouter *get_log_router();
        IObLogSchemaGetter *get_schema_getter();
        IObLogDBNameBuilder *get_db_name_builder();
        IObLogTBNameBuilder *get_tb_name_builder();
        IObLogFilter *get_log_filter();
        IObLogMysqlQueryEngine *get_mysql_query_engine();
        const ObLogConfig &get_config() const {return config_;};
        uint64_t get_start_checkpoint() const {return start_checkpoint_;};
        void set_perf_print_to_stderr(bool print_to_stderr)
        {
          if (inited_)
          {
            log_router_->set_perf_print_to_stderr(print_to_stderr);
          }
        }
      private:
        int init_obsql_();
        void destroy_obsql_();
        int init_internal_(const uint64_t position, ObLogPositionType pos_type, ERROR_CALLBACK err_cb);
        int construct_components_();
        int init_components_(const uint64_t checkpoint);
        void destroy_components_();
        void deconstruct_components_();
      private:
        bool inited_;
        bool need_query_back_;
        ObLogConfig             config_;
        IObLogRpcStub           *rpc_stub_;
        IObLogServerSelector    *server_selector_;
        IObLogSchemaGetter      *schema_getter_;
        IObLogDBNameBuilder     *dbname_builder_;
        IObLogTBNameBuilder     *tbname_builder_;
        IObLogFetcher           *log_fetcher_;
        IObLogFilter            *log_filter_;
        IObLogMysqlQueryEngine  *query_engine_;
        IObLogRouter            *log_router_;
        uint64_t start_checkpoint_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_SPEC_H_

