#ifndef _OCEANBASE_TOOLS_RS_CLIENT_RPC_H_
#define _OCEANBASE_TOOLS_RS_CLIENT_RPC_H_

#include "ob_server_client.h"
#include "common/ob_obi_role.h"
#include "common/ob_obi_config.h"
#include "common/ob_schema.h"

namespace oceanbase {
  namespace tools {
    using namespace oceanbase::common;
    
    class ObRootServerClient : public ObServerClient
    {
// declare all the handlers here
      public:
        ObRootServerClient();
        ~ObRootServerClient();        
        int get_obi_role(ObiRole &role) {
          return send_request(OB_GET_OBI_ROLE, role, timeout_);
        }
        int set_obi_role(const ObiRole &role) {
          return send_command(OB_SET_OBI_ROLE, role, timeout_);
        }
        int rs_admin(int32_t pcode) {
          return send_command(OB_RS_ADMIN, pcode, timeout_);
        }
        int change_log_level(int32_t log_level) {
          return send_command(OB_CHANGE_LOG_LEVEL, log_level, timeout_);
        }
        int rs_stat(int32_t start_key) {
          return send_command(OB_RS_STAT, start_key, timeout_);
        }
        int get_obi_config(ObiConfig &config) {
          return send_request(OB_GET_OBI_ROLE, config, timeout_);
        }
        int set_obi_config(ObiConfig &config) {
          return send_command(OB_SET_OBI_CONFIG, config, timeout_);
        }
        // int set_ups_config(ObBaseClient &client, Arguments &args);
        // int change_ups_master(ObBaseClient &client, Arguments &args);
        // int import_tablets(ObBaseClient &client, Arguments &args);
        int print_schema(ObSchemaManagerV2 &schema_manager) {
          return send_request(OB_FETCH_SCHEMA, schema_manager, timeout_);
        }
        // int print_root_table(ObBaseClient &client, Arguments &args);
        // int shutdown_servers(ObBaseClient &client, Arguments &args);

      private:
        int64_t timeout_;        
    };
  } // end of namespace tools
}   // end of namespace oceanbase

#endif /* _OCEANBASE_TOOLS_RS_CLIENT_RPC_H_ */
