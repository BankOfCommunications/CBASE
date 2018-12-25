////===================================================================
 //
 // ob_log_server_selector.cpp liboblog / Oceanbase
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

#include "ob_log_utils.h"
#include "ob_log_server_selector.h"

#define SQL_QUERY_CLUSTER_INFO  \
  "select cluster_id,cluster_role from __all_cluster;"
#define SQL_QUERY_UPS_ADDR \
  "select cluster_id,svr_ip,svr_port,svr_role from __all_server where svr_type='updateserver' order by cluster_id,svr_role DESC;"
#define SQL_QUERY_RS_ADDR \
  "select cluster_id,svr_ip,svr_port,svr_role from __all_server where svr_type='rootserver' order by cluster_id,svr_role DESC;"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogServerSelector::ObLogServerSelector() : inited_(false),
                                                   ob_mysql_addr_(NULL),
                                                   ob_mysql_port_(-1),
                                                   ob_mysql_user_(NULL),
                                                   ob_mysql_password_(NULL),
                                                   schema_refresh_lock_(),
                                                   master_ups_list_(),
                                                   slave_ups_list_(),
                                                   ups_vector_(),
                                                   cur_ups_idx_(-1),
                                                   rs_vector_(),
                                                   cur_rs_idx_(-1),
                                                   ups_history_()
    {
    }

    ObLogServerSelector::~ObLogServerSelector()
    {
      destroy();
    }

    int ObLogServerSelector::init(const ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (ob_mysql_addr_ = config.get_mysql_addr())
              || -1 == (ob_mysql_port_ = config.get_mysql_port())
              || NULL == (ob_mysql_user_ = config.get_mysql_user())
              || NULL == (ob_mysql_password_ = config.get_mysql_password()))
      {
        TBSYS_LOG(WARN, "invalid param from config, ob_mysql_addr=%s ob_mysql_port=%d",
                  ob_mysql_addr_, ob_mysql_port_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = query_server_addr_()))
      {
        TBSYS_LOG(WARN, "query server addr from ob_mysql fail, ret=%d", ret);
      }
      else
      {
        cur_ups_idx_ = 0;
        cur_rs_idx_ = 0;
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogServerSelector::destroy()
    {
      inited_ = false;
      cur_rs_idx_ = -1;
      cur_ups_idx_ = -1;
      rs_vector_.clear();
      ups_vector_.clear();
      ups_history_.clear();
      ob_mysql_port_ = -1;
      ob_mysql_addr_ = NULL;
    }

    int ObLogServerSelector::get_ups(common::ObServer &server, const bool change)
    {
      int ret = OB_SUCCESS;
      static ObServer last_get_ups;
      static bool last_change_status = false;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SpinRLockGuard guard(schema_refresh_lock_);

        if (0 >= ups_vector_.count())
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          if (0 > cur_ups_idx_ || cur_ups_idx_ >= ups_vector_.count())
          {
            cur_ups_idx_ = 0;
          }

          if (change)
          {
            // NOTE: Ensure only one UPS is in UPS history
            if (true != last_change_status && last_get_ups.is_valid())
            {
              ups_history_.clear();
              ups_history_.set(last_get_ups);
            }

            // Get a new ups which is not in ups_history
            if (OB_SUCCESS != (ret = get_different_ups_(server, ups_history_)))
            {
              TBSYS_LOG(ERROR, "get_different_ups_ fail, ret=%d", ret);
            }
          }
          else
          {
            // Get current UPS
            server = ups_vector_.at(cur_ups_idx_).addr;
          }

          last_change_status = change;
        }

        if (OB_SUCCESS == ret && server != last_get_ups)
        {
          TBSYS_LOG(INFO, "get_ups: ups=%s, index=%ld", to_cstring(server), cur_ups_idx_);
          last_get_ups = server;
        }
      }

      return ret;
    }

    int ObLogServerSelector::get_different_ups_(ObServer &ups, ServerHistory &ups_history)
    {
      OB_ASSERT(inited_ && 0 < ups_vector_.count() && 0 <= cur_ups_idx_ && cur_ups_idx_ < ups_vector_.count());

      int ret = OB_SUCCESS;
      int hash_ret = 0;
      int ups_count = 0;
      ObServer server;

      // Search all UPS and get a different one which is not in current history.
      while (ups_vector_.count() >= ++ups_count)
      {
        cur_ups_idx_ = (cur_ups_idx_ + 1) % ups_vector_.count();
        server = ups_vector_.at(cur_ups_idx_).addr;

        if (hash::HASH_NOT_EXIST == (hash_ret = ups_history.exist(server)))
        {
          break;
        }
      }

      if (OB_SUCCESS == ret && ups_vector_.count() < ups_count && ups_vector_.count() > 1)
      {
        TBSYS_LOG(DEBUG, "SERVER SELECTOR: clear ups list history");

        // Clear UPS history when ALL UPS are filled in history as now ups_history has been useless.
        ups_history.clear();

        // Get next server
        cur_ups_idx_ = (cur_ups_idx_ + 1) % ups_vector_.count();
        server = ups_vector_.at(cur_ups_idx_).addr;
      }

      // Update history
      // FIXME: Clear ups_history when it is full
      if (OB_SUCCESS == ret && OB_ERROR == (hash_ret = ups_history.set(server)))
      {
        TBSYS_LOG(WARN, "UPS history has been full, clear it.");
        ups_history.clear();
        ups_history.set(server);
      }

      if (OB_SUCCESS == ret)
      {
        ups = server;
      }

      return ret;
    }

    int64_t ObLogServerSelector::get_ups_num()
    {
      int64_t ret = 0;

      if (inited_)
      {
        SpinRLockGuard guard(schema_refresh_lock_);
        ret = ups_vector_.count();
      }

      return ret;
    }

    int ObLogServerSelector::get_rs(common::ObServer &server, const bool change)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SpinRLockGuard guard(schema_refresh_lock_);
        if (change)
        {
          int64_t old_v = cur_rs_idx_;
          int64_t new_v = ((old_v + 1) >= rs_vector_.count()) ? 0 : (old_v + 1);
          ATOMIC_CAS(&cur_rs_idx_, old_v, new_v);
        }
        if (0 <= cur_ups_idx_
            && cur_ups_idx_ < ups_vector_.count())
        {
          server = rs_vector_.at(cur_rs_idx_).addr;
          if (REACH_TIME_INTERVAL(1000000))
          {
            TBSYS_LOG(INFO, "get_rs addr=%s cur_rs_idx=%ld", to_cstring(server), cur_rs_idx_);
          }
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    void ObLogServerSelector::refresh()
    {
      if (inited_)
      {
        if (OB_SUCCESS == query_server_addr_())
        {
          cur_ups_idx_ = 0;
          cur_rs_idx_ = 0;
        }
      }
    }

    int ObLogServerSelector::query_server_addr_()
    {
      int ret = OB_SUCCESS;
      ObSQLMySQL *mysql = NULL;
      IdRoleMap cluster_id2role;
      if (NULL == (mysql = ob_mysql_init(NULL)))
      {
        TBSYS_LOG(WARN, "mysql init fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (mysql != ob_mysql_real_connect(mysql,
            ob_mysql_addr_,
            ob_mysql_user_,
            ob_mysql_password_,
            OB_MYSQL_DATABASE,
            ob_mysql_port_,
            NULL, 0))
      {
        TBSYS_LOG(WARN, "connect to ob_mysql %s:%d fail, %s", ob_mysql_addr_, ob_mysql_port_, ob_mysql_error(mysql));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != cluster_id2role.create(ID_ROLE_MAP_SIZE))
      {
        TBSYS_LOG(WARN, "create cluster_id2role fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = query_cluster_info_(mysql, cluster_id2role)))
      {
        TBSYS_LOG(WARN, "query_cluster_info fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = query_ups_info_(mysql, cluster_id2role)))
      {
        TBSYS_LOG(WARN, "query_ups_info fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = query_rs_info_(mysql, cluster_id2role)))
      {
        TBSYS_LOG(WARN, "query_rs_info fail, ret=%d", ret);
      }
      else
      {
        // do nothing
      }
      if (NULL != mysql)
      {
       ob_mysql_close(mysql);
        mysql = NULL;
      }
      ob_mysql_thread_end();
      return ret;
    }

    int ObLogServerSelector::query_cluster_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role)
    {
      int ret = OB_SUCCESS;
      int mysql_ret = 0;
      MYSQL_RES *mysql_results = NULL;
      if (0 != (mysql_ret = ob_mysql_query(mysql, SQL_QUERY_CLUSTER_INFO)))
      {
        TBSYS_LOG(WARN, "query sql [%s] fail, mysql_ret=%d", SQL_QUERY_CLUSTER_INFO, mysql_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (mysql_results = ob_mysql_store_result(mysql)))
      {
        TBSYS_LOG(WARN, "mysql_store_result fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        MYSQL_ROW mysql_record;
        while(NULL != (mysql_record = ob_mysql_fetch_row(mysql_results)))
        {
          uint64_t cluster_id = atoll(mysql_record[0]);
          uint64_t cluster_role = atoll(mysql_record[1]);
          int hash_ret = cluster_id2role.set(cluster_id, cluster_role);
          if (hash::HASH_INSERT_SUCC != hash_ret)
          {
            TBSYS_LOG(WARN, "set cluster_id=%lu cluster_role=%lu to id2role map fail, hash_ret=%d",
                      cluster_id, cluster_role, hash_ret);
          }
          else
          {
            TBSYS_LOG(INFO, "set cluster_id=%lu cluster_role=%lu to id2role map succ",
                      cluster_id, cluster_role);
          }
        }
        if (0 >= cluster_id2role.size())
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      if (NULL != mysql_results)
      {
        ob_mysql_free_result(mysql_results);
        mysql_results = NULL;
      }
      return ret;
    }

    int ObLogServerSelector::query_ups_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role)
    {
      int ret = OB_SUCCESS;
      int mysql_ret = 0;
      MYSQL_RES *mysql_results = NULL;
      if (0 != (mysql_ret = ob_mysql_query(mysql, SQL_QUERY_UPS_ADDR)))
      {
        TBSYS_LOG(WARN, "query sql [%s] fail, mysql_ret=%d", SQL_QUERY_UPS_ADDR, mysql_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (mysql_results = ob_mysql_store_result(mysql)))
      {
        TBSYS_LOG(WARN, "mysql_store_result fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        SpinWLockGuard guard(schema_refresh_lock_);
        int64_t prev_ups_count = ups_vector_.count();
        master_ups_list_.clear();
        slave_ups_list_.clear();
        ups_vector_.clear();
        MYSQL_ROW mysql_record;
        while(NULL != (mysql_record = ob_mysql_fetch_row(mysql_results)))
        {
          uint64_t cluster_id = atoll(mysql_record[0]);
          const char *svr_ip = mysql_record[1];
          int svr_port = atoi(mysql_record[2]);
          uint64_t svr_role = atoll(mysql_record[3]);
          uint64_t cluster_role = 0;
          int hash_ret = cluster_id2role.get(cluster_id, cluster_role);
          if (hash::HASH_EXIST != hash_ret)
          {
            TBSYS_LOG(WARN, "cluster_id=%lu not found in cluster_id2role map, hash_ret=%d",
                      cluster_id, hash_ret);
          }
          else
          {
            //TBSYS_LOG(INFO, "cluster_id=%lu found in cluster_id2role map, cluster_role=%lu",
            //          cluster_id, cluster_role);
            ObLogServer serv;
            serv.addr.set_ipv4_addr(svr_ip, svr_port);
            serv.role = 0;
            if (1 == cluster_role)
            {
              serv.role |= INSTANCE_MASTER;
              if (1 == svr_role)
              {
                serv.role |= CLUSTER_MASTER;
                master_ups_list_.push_back(serv);
              }
              else
              {
                master_ups_list_.push_back(serv);
              }
            }
            else
            {
              if (1 == svr_role)
              {
                serv.role |= CLUSTER_MASTER;
                slave_ups_list_.push_back(serv);
              }
              else
              {
                slave_ups_list_.push_back(serv);
              }
            }
          }
        }
        ServerList::iterator iter;
        for (iter = slave_ups_list_.begin(); iter != slave_ups_list_.end(); iter++)
        {
          ups_vector_.push_back(*iter);
          TBSYS_LOG(INFO, "prepare ups list addr=%s role=%lx", to_cstring(iter->addr), iter->role);
        }
        for (iter = master_ups_list_.begin(); iter != master_ups_list_.end(); iter++)
        {
          ups_vector_.push_back(*iter);
          TBSYS_LOG(INFO, "prepare ups list addr=%s role=%lx", to_cstring(iter->addr), iter->role);
        }
        if (0 >= ups_vector_.count())
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          if (prev_ups_count != ups_vector_.count())
          {
            cur_ups_idx_ = 0;
          }
        }
      }
      if (NULL != mysql_results)
      {
        ob_mysql_free_result(mysql_results);
        mysql_results = NULL;
      }
      return ret;
    }

    int ObLogServerSelector::query_rs_info_(ObSQLMySQL *mysql, IdRoleMap &cluster_id2role)
    {
      int ret = OB_SUCCESS;
      int mysql_ret = 0;
      MYSQL_RES *mysql_results = NULL;
      if (0 != (mysql_ret = ob_mysql_query(mysql, SQL_QUERY_RS_ADDR)))
      {
        TBSYS_LOG(WARN, "query sql [%s] fail, mysql_ret=%d", SQL_QUERY_RS_ADDR, mysql_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (mysql_results = ob_mysql_store_result(mysql)))
      {
        TBSYS_LOG(WARN, "mysql_store_result fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        SpinWLockGuard guard(schema_refresh_lock_);
        int64_t prev_rs_count = rs_vector_.count();
        rs_vector_.clear();
        MYSQL_ROW mysql_record;
        while(NULL != (mysql_record = ob_mysql_fetch_row(mysql_results)))
        {
          uint64_t cluster_id = atoll(mysql_record[0]);
          const char *svr_ip = mysql_record[1];
          int svr_port = atoi(mysql_record[2]);
          uint64_t svr_role = atoll(mysql_record[3]);
          uint64_t cluster_role = 0;
          int hash_ret = cluster_id2role.get(cluster_id, cluster_role);
          if (hash::HASH_EXIST != hash_ret)
          {
            TBSYS_LOG(WARN, "cluster_id=%lu not found in cluster_id2role map, hash_ret=%d",
                      cluster_id, hash_ret);
          }
          else
          {
            //TBSYS_LOG(INFO, "cluster_id=%lu found in cluster_id2role map, cluster_role=%lu",
            //          cluster_id, cluster_role);
            ObLogServer serv;
            serv.addr.set_ipv4_addr(svr_ip, svr_port);
            serv.role = 0;
            if (1 == cluster_role)
            {
              serv.role |= INSTANCE_MASTER;
            }
            if (1 == svr_role)
            {
              serv.role |= CLUSTER_MASTER;
            }
            rs_vector_.push_back(serv);
            TBSYS_LOG(INFO, "prepare rs list addr=%s role=%lx", to_cstring(serv.addr), serv.role);
          }
        }
        if (0 >= rs_vector_.count())
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          if (prev_rs_count != rs_vector_.count())
          {
            cur_rs_idx_ = 0;
          }
        }
      }
      if (NULL != mysql_results)
      {
        ob_mysql_free_result(mysql_results);
        mysql_results = NULL;
      }
      return ret;
    }

  }
}

