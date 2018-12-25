#ifndef _OB_MERGER_SCHEMA_PROXY_H_
#define _OB_MERGER_SCHEMA_PROXY_H_

#include "common/ob_string.h"
#include "common/ob_schema_service_impl.h"//add zhaoqiong [Schema Manager] 20150327

namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
    class ObMergerSchemaManager;
  }

  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
    class ObMergerSchemaProxy
    {
    public:
      ObMergerSchemaProxy();
      //mod zhaoqiong [Schema Manager] 20150327:b
      //ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, common::ObMergerSchemaManager * schema);
      ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, common::ObMergerSchemaManager * schema, common::ObSchemaServiceImpl *schema_service);
      //mod:e
      virtual ~ObMergerSchemaProxy();

    public:
      // check inner stat
      bool check_inner_stat(void) const;

      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 500 * 1000; // 500ms

      // local location newest version
      // get the scheam data according to the timetamp, in some cases depend on the timestamp value
      // if timestamp is LOCAL_NEWEST, it meanse only get the local latest version
      // otherwise, it means that at first find in the local versions, if not exist then need rpc
      // waring: after using manager, u must release this schema version for washout
      // param  @timeout every rpc call timeout
      //        @manager the real schema pointer returned
      static const int64_t LOCAL_NEWEST = 0;
      int get_schema(const common::ObString & table_name, const int64_t timestamp,
          const common::ObSchemaManagerV2 ** manager);

      // waring: release schema after using for dec the manager reference count
      //        @manager the real schema pointer returned
      int release_schema(const common::ObSchemaManagerV2 * manager);

      int get_schema_version(int64_t & timestamp);
    public:
      int fetch_user_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);
    private:
      // fetch new schema according to the version, if the last fetch timestamp is to nearly
      // return fail to avoid fetch too often from root server
      // param  @version schema version
      //        @manager the real schema pointer returned
      int get_user_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

    private:
      int64_t fetch_schema_timestamp_;              // last fetch schema timestamp from root server
      tbsys::CThreadMutex schema_lock_;             // mutex lock for update schema manager
      ObMergerRootRpcProxy * root_rpc_;             // root server rpc proxy
      common::ObMergerSchemaManager * schema_manager_;      // merge server schema cache
      //add zhaoqiong [Schema Manager] 20150327:b
      oceanbase::common::ObSchemaServiceImpl* schema_service_;
      //add:e
    };

    inline bool ObMergerSchemaProxy::check_inner_stat(void) const
    {
      //mod zhaoqiong [Schema Manager] 20150327:b
      //return ((root_rpc_ != NULL) && (schema_manager_ != NULL));
      return ((root_rpc_ != NULL) && (schema_manager_ != NULL) && (schema_service_ != NULL));
      //mod:e
    }
  }
}


#endif // _OB_MERGER_SCHEMA_PROXY_H_
