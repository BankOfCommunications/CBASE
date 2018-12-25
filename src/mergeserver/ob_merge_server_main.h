#ifndef OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_
#define OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_

#include "common/base_main.h"
#include "common/ob_config_manager.h"
#include "common/ob_version.h"
#include "ob_merge_server.h"
#include "obmysql/ob_mysql_server.h"
#include "ob_merge_reload_config.h"
#include "ob_merge_server_config.h"

namespace oceanbase
{
  namespace mergeserver
  {
    enum ObMsType
    {
      INVALID = 0,
      NORMAL,
      LMS,
    };
    class ObMergeServerMain : public common::BaseMain
    {
      public:
        static ObMergeServerMain * get_instance();
        int do_work();
        void do_signal(const int sig);

      public:
        ObMergeServer& get_merge_server() { return server_; }
        const ObMergeServer& get_merge_server() const { return server_; }
        const obmysql::ObMySQLServer& get_mysql_server() const { return sql_server_; }
      protected:
        virtual void print_version();
      private:
        ObMergeServerMain();
        int init_sql_server();
        ObMsType get_ms_type(const char *ms_type);
      private:
        /* ObMergeServerParams ms_params_; */
        ObMergeReloadConfig ms_reload_config_;
        ObMergeServerConfig ms_config_;
        ObConfigManager config_mgr_;
        ObMergeServer server_;
        obmysql::ObMySQLServer sql_server_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_ */
