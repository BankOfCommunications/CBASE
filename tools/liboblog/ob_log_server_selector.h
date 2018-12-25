////===================================================================
 //
 // ob_log_server_selector.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_SERVER_SELECTOR_H_
#define  OCEANBASE_LIBOBLOG_SERVER_SELECTOR_H_

#include "libobsql.h"      // ob_sql*
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "common/ob_list.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_config.h"
#include "ob_log_rpc_stub.h"
#include "common/hash/ob_placement_hashset.h" // ObPlacementHashSet

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogServerSelector
    {
      public:
        virtual ~IObLogServerSelector() {};

      public:
        virtual int init(const ObLogConfig &config) = 0;

        virtual void destroy() = 0;

        virtual int get_ups(common::ObServer &server, const bool change) = 0;

        virtual int64_t get_ups_num() = 0;

        virtual int get_ms(common::ObServer &server, const bool change) = 0;

        virtual int get_rs(common::ObServer &server, const bool change) = 0;

        virtual void refresh() = 0;
    };

    class ObLogServerSelector : public IObLogServerSelector
    {
      static const uint64_t INSTANCE_MASTER = 0x1;
      static const uint64_t CLUSTER_MASTER = 0x2;
      struct ObLogServer
      {
        common::ObServer addr;
        uint64_t role;
      };

      static const uint64_t MAX_SERVER_NUMBER = 100;
      typedef common::hash::ObPlacementHashSet<common::ObServer, MAX_SERVER_NUMBER> ServerHistory;
      typedef common::ObArray<ObLogServer> ServerVector;
      typedef common::ObList<ObLogServer> ServerList;
      typedef common::hash::ObHashMap<uint64_t, uint64_t> IdRoleMap;
      static const int64_t ID_ROLE_MAP_SIZE = 4;
      public:
        ObLogServerSelector();
        ~ObLogServerSelector();
      public:
        int init(const ObLogConfig &config);
        void destroy();
        int get_ups(common::ObServer &server, const bool change);
        int64_t get_ups_num();
        int get_rs(common::ObServer &server, const bool change);
        void refresh();
        int get_ms(common::ObServer &server, const bool change)
        {
          UNUSED(server);
          UNUSED(change);
          return common::OB_NOT_IMPLEMENT;
        };
      private:
        /// @brief Get a different UPS which does not exist in specific history
        /// @param[out] ups returned updateserver
        /// @param[in] ups_history specific history
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        /// @note If the history contains all available UPS, it will clear the history
        ///       and get the next UPS, then return OB_SUCCESS
        int get_different_ups_(common::ObServer &ups, ServerHistory &ups_history);
        int query_server_addr_();
        int query_cluster_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role);
        int query_ups_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role);
        int query_rs_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role);
      private:
        bool inited_;
        const char *ob_mysql_addr_;
        int ob_mysql_port_;
        const char *ob_mysql_user_;
        const char *ob_mysql_password_;
        common::SpinRWLock schema_refresh_lock_;
        ServerList master_ups_list_;
        ServerList slave_ups_list_;
        ServerVector ups_vector_;
        int64_t cur_ups_idx_;
        ServerVector rs_vector_;
        int64_t cur_rs_idx_;
        ServerHistory ups_history_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_SERVER_SELECTOR_H_

