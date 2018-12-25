/*
* Version: $ObPaxos rs_switch$
*
* Authors:
*   huangjianwei
*
* Date:
*  20160726
*
*  --rootserver get server status info from inner table--
*
*/

#ifndef __OB_ROOTSERVER_OB_RS_GET_SERVER_STATUS_TIMER_TASK_H__
#define __OB_ROOTSERVER_OB_RS_GET_SERVER_STATUS_TIMER_TASK_H__

#include "common/ob_timer.h"
#include "common/ob_schema_service.h"
#include "common/ob_array.h"
#include "common/ob_buffer.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootWorker;
    //add huangjianwei [Paxos rs_switch] 20160726:b
    struct ServerStatus
    {
      common::ObServer addr_;
      //mod pangtianze [Paxos rs_switch] 20170209:b
      /*common::ObString svr_type_;
      common::ObString svr_info_;*/
      common::ObRole svr_type_;
      //mod:e
      int32_t svr_role_;
      //add pangtianze [Paxos rs_switch] 20170209:b      
      int32_t ms_sql_port_;   /* for ms sql port */
      ServerStatus()
        : svr_type_(OB_INVALID),
          svr_role_(0),
          ms_sql_port_(0)
      {
      }
      bool operator ==(const ServerStatus& ss) const
      {
        bool ret = true;
        if (addr_ != ss.addr_
                || svr_type_ != ss.svr_type_
                || svr_role_ != ss.svr_role_
                || ms_sql_port_ != ss.ms_sql_port_ )
        {
           ret = false;
        }
        return ret;
      }
      //add：e
    };
    /**
     * @brief The ObRsGetServerStatusTask class
     *  This timertask do two things:
     *  1. fetch ms/cs server info from inner table
     *  2. fetch rs list from memory and send it to all ms/cs/ups
     */
    class ObRsGetServerStatusTask: public common::ObTimerTask
    {
      public:
        ObRsGetServerStatusTask();
        virtual ~ObRsGetServerStatusTask();
        int init(ObRootWorker *worker, common::ObSchemaService *schema_service);
        virtual void runTimerTask(void);

        //add huangjianwei [Paxos rs_switch] 20160729:b
        int get_server_status();//get server status from inner table;
        const common::ObArray<ServerStatus>&  get_server_status_array()
        {
          return server_status_array_;
        }
        //add:e
        int64_t get_server_status_count() const;
        //add pangtianze [Paxos rs_switch] 20170209:b
        inline const ServerStatus get_status_by_index(const int32_t index)
        {
            ServerStatus status;
            if (index < 0)
            {
                TBSYS_LOG(ERROR, "invalid index, idx=%d", index);
            }
            else
            {
                tbsys::CThreadGuard guard(&svr_status_mutex_);
                status = server_status_array_.at(index);
            }
            return status;
        }
        inline void set_last_check_server_time(const int64_t time)
        {
            last_check_server_time_ = time;
        }
        /**
         * @brief check server status array, if lease timeout, remove the server and modify all_server
         *        only master rs can use the function.
         * @param lease_duration_time
         * @return
         */
        int check_server_alive(const int64_t &lease_duration_time);
      private:
        inline ObRole type_str_to_int(common::ObString &svr_type)
        {
            return  (OB_SUCCESS == svr_type.compare("chunkserver"))?OB_CHUNKSERVER:OB_MERGESERVER;
        }
        /**
         * @brief get_status_array_idx, no lock
         * @param server
         * @param svr_type
         * @return
         */
        int32_t get_status_array_idx(common::ObServer &server, common::ObRole svr_type);
        //add:e
        //add pangtianze [Paxos rs_switch] 20170628:b
        bool array_compare();
        int fill_server_manager();
        //add:e
        //add pangtianze [Paxos rs_switch] 20170705:b
        int get_ms_list();  //fetch ms list from master rs
        int get_cs_list();  //fetch ms list from master rs
        //add:e
      private:
        //add pangtianze [Paxos rs_switch] 20170705:b
        buffer buff_;
        //add:e
        static const int32_t DEFAULT_TIMEOUT = 3000000; //3s
        ObRootWorker *worker_;
        mutable tbsys::CThreadMutex svr_status_mutex_;
        common::ObSchemaService *schema_service_;
        //add huangjianwei [Paxos rs_switch] 20160726:b
        common::ObArray<ServerStatus> server_status_array_;
        //add:e
        //add pangtianze [Paxos rs_switch] 20170424:b
        common::ObArray<ServerStatus> tmp_server_status_array_;
        //add:e
        //add pangtianze [Paxos rs_switch] 20170210:b
        int64_t last_check_server_time_;
        //add:e
    };
    //add:e
  }
}
#endif /* __OB_ROOTSERVER_OB_RS_GET_SERVER_STATUS_TIMER_TASK_H__ */
