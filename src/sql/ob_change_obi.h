/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_change_obi.h
 *
 * Authors:
 *   Wu Di <lide.wd@alipay.com>
 *
 */

#ifndef OB_CHANGE_OBI_H_
#define OB_CHANGE_OBI_H_

#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/ob_stack_allocator.h"
#include "common/ob_obi_role.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_result_set.h"
namespace oceanbase
{
  namespace sql
  {
    struct ObRsUps
    {
      int64_t cluster_id_;
      ObServer master_rs_;
      ObServer master_ups_;
      bool got_master_ups_;
      ObRsUps():cluster_id_(-1), got_master_ups_(false)
      {
      }
    };
    class ObChangeObi : public ObNoChildrenPhyOperator
    {
      public:
        ObChangeObi();
        virtual ~ObChangeObi();
        virtual int open();
        virtual int close();
        virtual void reuse();
        virtual void reset();
        virtual ObPhyOperatorType get_type() const
        {
          return PHY_OB_CHANGE_OBI;
        }
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int get_next_row(const common::ObRow *&row);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void set_context(ObSqlContext *context);
        void set_old_ob_query_timeout(const ObObj &old_ob_query_timeout);
        int set_change_obi_timeout(const ObObj &change_obi_timeout_value);
        void set_check_ups_log_interval(const int interval);
        void set_force(const bool force);
        bool is_force();
        int set_target_server_addr(const ObString &target_server);
        void set_target_role(const ObiRole role);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObChangeObi);
      private:
        int get_rs();
        void get_master_ups();
        int check_new_master_addr();
        int check_ups_log();
        int set_slave_in_inner_table(int64_t cluster_index);
        int change_cluster_to_slave(int64_t cluster_index);
        int check_if_cluster_is_slave(int64_t cluster_index);
        int waiting_old_slave_ups_sync();
        int change_cluster_to_master(const ObServer &target_server);
        int check_if_cluster_is_master(const ObServer &target_server);
        int set_master_rs_vip_port_to_all_cluster();
        int broadcast_master_cluster();
        int ms_renew_master_master_ups();
        int update_all_sys_config();
        int waiting_for_all_sys_config_stat_updated();
        int check_master_rs_vip_port();
        int execute_sql(const ObString &sql);
        int post_check();
        int check_addr();
        // get master rs index by rs rpc: get_obi_role
        int get_master_rs_by_rpc(int64_t &master_index);
      private:
        static const int32_t MAX_CLUSTER_COUNT = 32;
        static const int64_t TIMEOUT = 2 * 1000 * 1000;
        static const int64_t RETRY_INTERVAL_US = 200 * 1000;//200ms
        static const int64_t RETRY_TIME = 6;
        static const int64_t WAIT_SLAVE_UPS_SYNC_TIME_US = 200 * 1000;//200ms
        ObSqlContext *context_;
        ObRsUps  all_rs_ups_[MAX_CLUSTER_COUNT];
        ObResultSet *result_set_out_;
        int64_t master_cluster_index_;
        int64_t cluster_count_;
        int64_t target_cluster_index_;
        ObObj old_ob_query_timeout_;
        ObObj change_obi_timeout_value_;
        int check_ups_log_interval_;
        bool force_;
        int64_t change_obi_timeout_us_;
        
        common::ObiRole role_;
        ObServer target_server_;
        int target_server_port_;
        char target_server_ip_[MAX_IP_ADDR_LENGTH];
        bool got_rs_;
        bool multi_master_cluster_;
        bool master_cluster_exists_;

    };
  }
}
#endif
