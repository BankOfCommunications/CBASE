/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 *
 ================================================================*/
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/utility.h"
#include "common/ob_atomic.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_trigger_msg.h"
#include "common/ob_general_rpc_stub.h"
#include "common/ob_data_source_desc.h"
#include "rootserver/ob_root_callback.h"
#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_admin_cmd.h"
#include "rootserver/ob_root_util.h"
#include "rootserver/ob_root_stat_key.h"
#include "rootserver/ob_root_bootstrap.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_strings.h"
#include "common/ob_log_cursor.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_common_stat.h"
#include "sql/ob_sql_scan_param.h"
#include "sql/ob_sql_get_param.h"
#include <sys/types.h>
#include <unistd.h>
//#define PRESS_TEST
#define __rs_debug__
#include "common/debug.h"
#include "common/ob_libeasy_mem_pool.h"
//add peiouya [Get_masterups_and_timestamp] 20141017:b
#include "ob_ups_manager.h"
//add 20141017:e
//add pangtianze [Paxos rs_election] 20150630:b
#include "common/ob_version.h"
//add:e
//add bingo [Paxos Cluster.Balance] 20161020:b
#include "common/ob_inner_table_operator.h"
#include "rootserver/ob_root_sql_proxy.h"
//add:e
namespace
{
  const int WRITE_THREAD_FLAG = 1;
  const int LOG_THREAD_FLAG = 2;
  //add pangtianze [Paxos rs_election] 20150619:b
  const int ELECTION_THREAD_FLAG = 3;
  //add:e
  //add pangtianze [Paxos] 20170523:b
  const int32_t PING_RPC_TIMEOUT = 1 * 1000 * 1000; //1s
  //add:e
  const int32_t ADDR_BUF_LEN = 64;
}
namespace oceanbase
{
  namespace rootserver
  {
    using namespace oceanbase::common;

    ObRootWorker::ObRootWorker(ObConfigManager &config_mgr, ObRootServerConfig &rs_config)
      : config_mgr_(config_mgr), config_(rs_config), is_registered_(false),
      root_server_(config_), sql_proxy_(const_cast<ObChunkServerManager&>(root_server_.get_server_manager()), const_cast<ObRootServerConfig&>(rs_config), const_cast<ObRootRpcStub&>(rt_rpc_stub_)
                                        //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                                        , root_server_
                                        //add:e
                                        )
    {
      schema_version_ = 0;
      //add wenghaixing [secondary index static_index_build]20150317
      index_monitor_.init(this);
      //add e


    }

    ObRootWorker::~ObRootWorker()
    {
    }
    int ObRootWorker::create_eio()
    {
      int ret = OB_SUCCESS;
      easy_pool_set_allocator(ob_easy_realloc);
      eio_ = easy_eio_create(eio_, io_thread_count_);
      eio_->do_signal = 0;
      eio_->force_destroy_second = OB_CONNECTION_FREE_TIME_S;
      eio_->checkdrc = 1;
      eio_->no_redispatch = 1;
      if (NULL == eio_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "easy_io_create error");
      }
      return ret;
    }
    int ObRootWorker::initialize()
    {
      int ret = OB_SUCCESS;
      __debug_init__();
      //set call back function
      if (OB_SUCCESS == ret)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObRootCallback::process;//root server process
        //server_handler_.batch_process = ObTbnetCallback::batch_process;
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }
      if (OB_SUCCESS == ret)
      {
        ret = client_manager.initialize(eio_, &server_handler_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init client manager, err=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rt_rpc_stub_.init(&client_manager, &my_thread_buffer);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init rpc stub failed, err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = general_rpc_stub_.init(&my_thread_buffer, &client_manager);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init general rpc stub failed, err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = this->set_listen_port((int32_t)config_.port);
      }
      if (OB_SUCCESS == ret)
      {
        ret = this->set_dev_name(config_.devname);
      }

      ObServer vip_addr;
      //mod chujiajia[Paxos rs_election]20151012:b
      //uint32_t vip = tbsys::CNetUtil::getAddr(config_.root_server_ip);
      uint32_t vip = tbsys::CNetUtil::getAddr(config_.master_root_server_ip);
      //mod:e
      int32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name_);
      if (ret == OB_SUCCESS)
      {
        //modify huangjianwei [Paxos rs_election] 20160427:b
        //if (!vip_addr.set_ipv4_addr(vip, port_))
        if (!vip_addr.set_ipv4_addr(vip, static_cast<int32_t>(config_.master_root_server_port)))
        {
          //TBSYS_LOG(ERROR, "rootserver vip address invalid, ip:%d, port:%d", local_ip, port_);
          TBSYS_LOG(ERROR, "rootserver vip address invalid, ip:%d, port:%d", vip, static_cast<int32_t>(config_.master_root_server_port));
          ret = OB_ERROR;
        }
        //modify:e
        else if (!self_addr_.set_ipv4_addr(local_ip, port_))
        {
          TBSYS_LOG(ERROR, "rootserver address invalid, ip:%d, port:%d", local_ip, port_);
          ret = OB_ERROR;
        }
        //add lbzhong [Paxos Cluster.Balance] 201607011:b
        self_addr_.cluster_id_ = static_cast<int32_t>(config_.cluster_id);
        //add:e
      }

      if (ret == OB_SUCCESS)
      {
        stat_manager_.init(vip_addr);
        ObStatSingleton::init(&stat_manager_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = slave_mgr_.init(&role_mgr_, vip, &rt_rpc_stub_,
            config_.log_sync_timeout,
            config_.lease_interval_time,
            config_.lease_reserved_time);

        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init slave manager, err=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {

        read_thread_queue_.setThreadParameter((int32_t)config_.read_thread_count, this, NULL);
        void* args = reinterpret_cast<void*>(WRITE_THREAD_FLAG);
        write_thread_queue_.setThreadParameter(1, this, args);

        args = reinterpret_cast<void*>(LOG_THREAD_FLAG);
        log_thread_queue_.setThreadParameter(1, this, args);
        //add pangtianze [Paxos rs_election] 20150619:b
        args = reinterpret_cast<void*>(ELECTION_THREAD_FLAG);
        election_thread_queue_.setThreadParameter(1, this, args);
        //add:e
      }

      if (OB_SUCCESS == ret)
      {
        set_io_thread_count((int32_t)config_.io_thread_count);
      }

      if (ret == OB_SUCCESS)
      {
        if (tbsys::CNetUtil::isLocalAddr(vip))
        {
          TBSYS_LOG(INFO, "I am holding the VIP, set role to MASTER");
          role_mgr_.set_role(ObRoleMgr::MASTER);
        }
        else
        {
          TBSYS_LOG(INFO, "I am not holding the VIP, set role to SLAVE");
          role_mgr_.set_role(ObRoleMgr::SLAVE);
        }
        //modify huangjianwei [Paxos rs_election] 20160427:b
        //rt_master_.set_ipv4_addr(vip, port_);
        rt_master_.set_ipv4_addr(vip, static_cast<int32_t>(config_.master_root_server_port));
        //modify:e
        //add lbzhong [Paxos Cluster.Balance] 20160707:b
        rt_master_.cluster_id_ = (int32_t) config_.cluster_id;
        //add:e
        ret = check_thread_.init(&role_mgr_, vip, config_.vip_check_period,
            &rt_rpc_stub_, &rt_master_, &self_addr_);
      }
      if (ret == OB_SUCCESS)
      {
        if (OB_SUCCESS != (ret = inner_table_task_.init((int32_t)config_.cluster_id,
                sql_proxy_, timer_, *root_server_.get_task_queue()
                //add lbzhong [Paxos Cluster.Balance] 201607014:b
                , &root_server_
                //add:e
                )))
        {
          TBSYS_LOG(WARN, "init inner table task failed. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = after_restart_task_.init(this)))
        {
          TBSYS_LOG(WARN, "init after_restart_task fail. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = timer_.init()))
        {
          TBSYS_LOG(ERROR, "init timer fail, ret: [%d]", ret);
        }
        else if (OB_SUCCESS != (ret = timer_.schedule(inner_table_task_, ASYNC_TASK_TIME_INTERVAL, true)))
        {
          TBSYS_LOG(WARN, "fail to schedule inner table task. ret=%d", ret);
        }
        //add huangjianwei, mod pangtianze [Paxos rs_switch] 20170209:b
        else if (OB_SUCCESS != (ret = timer_.schedule(get_server_status_task_, get_config().server_status_refresh_time, true)))
        {
          TBSYS_LOG(WARN, "failed to schedule get server status task, ret=%d", ret);
        }
        //add:e
        else
        {
          TBSYS_LOG(INFO, "init timer success.");
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        if (!root_server_.init(now, this))
        {
          ret = OB_ERROR;
        }
      }
      //add huangjianwei [Paxos rs_switch] 20160727:b
      if  (ret == OB_SUCCESS)
      {
        if (OB_SUCCESS != (ret = get_server_status_task_.init(this,root_server_.get_schema_service())))
        {
          TBSYS_LOG(WARN, "init get_server_status_task failed. ret=%d", ret);
        }
      }
      //add:e
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ms_list_task_.init(
                rt_master_,
                &client_manager,
                false)))
        {
          TBSYS_LOG(ERROR, "init ms list failt, ret: [%d]", ret);
        }
        else if (OB_SUCCESS !=
                 (ret = config_mgr_.init(root_server_.get_ms_provider(), client_manager, timer_)))
        {
          //mod jinty [Paxos Cluster.Balance] 20160708:b
          //TBSYS_LOG(ERROR, "init error, ret: [%d]", ret);
            TBSYS_LOG(ERROR, "init ms_provider() error, ret: [%d]", ret);
          //mod e
        }
        //add jinty [Paxos Cluster.Balance] 20160708:b
        else if (OB_SUCCESS !=
                 (ret = config_mgr_.init(ms_list_task_, client_manager, timer_)))
        {
          TBSYS_LOG(ERROR, "init ms_list_task error, ret: [%d]", ret);
        }
        //add e
      }

      TBSYS_LOG(INFO, "root worker init, ret=%d", ret);
      return ret;
    }

    int ObRootWorker::set_io_thread_count(int io_thread_count)
    {
      int ret = OB_SUCCESS;
      if (io_thread_count < 1)
      {
        TBSYS_LOG(WARN, "invalid argument io thread count is %d", io_thread_count);
        ret = OB_ERROR;
      }
      else
      {
        io_thread_count_ = io_thread_count;
      }
      return ret;
    }

    //add wenghaixing [secondary index static_index_build]20150317
    int ObRootWorker::submit_index_task()
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO,"all tablet is common merged,now begin to create index!");
      ObDataBuffer in_buffer;
      int64_t data_len = 2*1024*1024;
      char* data = (char *)ob_malloc(data_len, ObModIds::OB_STATIC_INDEX);
      in_buffer.set_data(data, data_len);
      ret = submit_async_task_(OB_CREATE_INDEX_COMMAND, read_thread_queue_, (int32_t)config_.read_queue_size, &in_buffer);
      ob_free(data);
      return ret;
    }
    //add e

    int ObRootWorker::start_service()
    {
      int ret = OB_ERROR;

      if (OB_SUCCESS != (ret = timer_.schedule(ms_list_task_, 1000000, true)))
      {
        TBSYS_LOG(ERROR, "schedule ms list task fail, ret: [%d]", ret);
      }
      ObRoleMgr::Role role = role_mgr_.get_role();
      if (role == ObRoleMgr::MASTER)
      {
        //add bingo [Paxos Cluster.Balance] 20170209:b
        //to load all_cluster_config in balance thread
        //role_mgr_.set_new_master_flag(true); //del pangtianze [Paxos Cluster.Balance] 20170629
        //add:e
        //mod pangtiaze [Paxos rs_election] 20150723:b
        //ret = start_as_master();
        if (get_config().use_paxos)
        {
          ret = start_as_master_leader();
        }
        else
        {
          //never into this branch
          TBSYS_LOG(INFO, "start as master standalone");
          ret = start_as_master();
        }
        //mod:e
      }
      else if (role == ObRoleMgr::SLAVE)
      {
        //mod pangtiaze [Paxos rs_election] 20150723:b
        //ret = start_as_slave();
        if (get_config().use_paxos)
        {
          ret = start_as_slave_follower();
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "slave rootserver can't start as standalone");
        }
        //mod:e
      }
      else
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "unknow role: %d, rootserver start failed", role);
      }
      TBSYS_LOG(INFO, "exit");
      return ret;
    }

    int ObRootWorker::start_as_master()
    {
      int ret = OB_ERROR;
      TBSYS_LOG(INFO, "[NOTICE] master start step1");
      root_server_.init_boot_state();
      ret = log_manager_.init(&root_server_, &slave_mgr_, &get_root_server().get_self());
      if (ret == OB_SUCCESS)
      {
        // try to replay log
        TBSYS_LOG(INFO, "[NOTICE] master start step2");
        ret = log_manager_.replay_log();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "[NOTICE] master replay log failed, err=%d", ret);
        }
      }
      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "[NOTICE] master start step3");
        root_server_.start_merge_check();
      }

      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "[NOTICE] master start step4");
        root_server_.reset_hb_time();
      }

      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "[NOTICE] master start step5");
        role_mgr_.set_state(ObRoleMgr::ACTIVE);

        read_thread_queue_.start();
        write_thread_queue_.start();
        root_server_.start_master_rootserver();
        check_thread_.start();
        TBSYS_LOG(INFO, "[NOTICE] master start-up finished");
        root_server_.dump_root_table();

        // wait finish
        for (;;)
        {
          if (ObRoleMgr::STOP == role_mgr_.get_state()
              || ObRoleMgr::ERROR == role_mgr_.get_state())
          {
            TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
            break;
          }
          usleep(10 * 1000); // 10 ms
        }
      }

      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();
      return ret;
    }

    int ObRootWorker::start_as_slave()
    {
      int err = OB_SUCCESS;

      // get obi role from the master
      if (err == OB_SUCCESS)
      {
        err = get_obi_role_from_master();
      }
      if (OB_SUCCESS == err)
      {
        log_thread_queue_.start();
        //read_thread_queue_.start();
        err = log_manager_.init(&root_server_, &slave_mgr_, &get_root_server().get_self());
      }
      ObFetchParam fetch_param;
      if (err == OB_SUCCESS)
      {
        err = slave_register_(fetch_param);
      }
      if (OB_SUCCESS == err)
      {
        err = get_boot_state_from_master();
      }
      if (err == OB_SUCCESS)
      {
        root_server_.init_boot_state();
        err = log_replay_thread_.init(log_manager_.get_log_dir_path(),
            fetch_param.min_log_id_, 0, &role_mgr_, NULL,
            config_.log_replay_wait_time);
        log_replay_thread_.set_log_manager(&log_manager_);
      }

      if (err == OB_SUCCESS)
      {
        err = fetch_thread_.init(rt_master_, log_manager_.get_log_dir_path(), fetch_param,
            &role_mgr_, &log_replay_thread_);
        if (err == OB_SUCCESS)
        {
          fetch_thread_.set_limit_rate(config_.log_sync_limit);
          fetch_thread_.add_ckpt_ext(ObRootServer2::ROOT_TABLE_EXT); // add root table file
          fetch_thread_.add_ckpt_ext(ObRootServer2::CHUNKSERVER_LIST_EXT); // add chunkserver list file
          fetch_thread_.add_ckpt_ext(ObRootServer2::LOAD_DATA_EXT); // add load data check point
          fetch_thread_.set_log_manager(&log_manager_);
          fetch_thread_.start();
          TBSYS_LOG(INFO, "slave fetch_thread started");
          if (fetch_param.fetch_ckpt_)
          {
            err = fetch_thread_.wait_recover_done();
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to init fetch log thread");
        }
      }

      if (err == OB_SUCCESS)
      {
        role_mgr_.set_state(ObRoleMgr::ACTIVE);
        //root_server_.init_boot_state();
        // we SHOULD start root_modifier after recover checkpoint
        root_server_.start_threads();
        TBSYS_LOG(INFO, "slave root_table_modifier, balance_worker, heartbeat_checker threads started");
      }

      // we SHOULD start replay thread after wait_init_finished
      if (err == OB_SUCCESS)
      {
        log_replay_thread_.start();
        TBSYS_LOG(INFO, "slave log_replay_thread started");
      }
      else
      {
        TBSYS_LOG(ERROR, "failed to start log replay thread");
      }

      if (err == OB_SUCCESS)
      {
        check_thread_.start();
        TBSYS_LOG(INFO, "slave check_thread started");

        while (ObRoleMgr::SWITCHING != role_mgr_.get_state() // lease is valid and vip is mine
            && ObRoleMgr::INIT != role_mgr_.get_state() //  lease is invalid, should reregister to master
            // but now just let it exit.
            && ObRoleMgr::STOP != role_mgr_.get_state() // stop normally
            && ObRoleMgr::ERROR != role_mgr_.get_state())
        {
          usleep(10 * 1000); // 10 ms
        }

        if (ObRoleMgr::SWITCHING == role_mgr_.get_state())
        {
          TBSYS_LOG(WARN, "rootserver slave begin switch to master");

          TBSYS_LOG(INFO, "[NOTICE] set role to master");
          root_server_.reset_hb_time();

          TBSYS_LOG(INFO, "wait log_thread");
          log_thread_queue_.stop();
          log_thread_queue_.wait();
          TBSYS_LOG(INFO, "wait fetch_thread");
          fetch_thread_.wait();
          role_mgr_.set_role(ObRoleMgr::MASTER);

          ObLogCursor replayed_cursor;
          ObLogCursor flushed_cursor;
          // log replay thread will stop itself when
          // role switched to MASTER and nothing
          // more to replay
          if (OB_SUCCESS != (err = log_manager_.get_flushed_cursor(flushed_cursor)))
          {
            TBSYS_LOG(ERROR, "log_manager.get_flushed_cursor()=>%d", err);
          }
          else
          {
            TBSYS_LOG(INFO, "wait replay_thread");
            log_replay_thread_.wait_replay(flushed_cursor);
          }

          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = log_replay_thread_.get_replayed_cursor(replayed_cursor)))
          {
            TBSYS_LOG(ERROR, "replayed_thread.get_replayed_cursor()=>%d", err);
          }
          else if (OB_SUCCESS != (err = log_manager_.start_log_maybe(replayed_cursor)))
          {
            TBSYS_LOG(ERROR, "log_manager.start_log(replayed[%s], flushed[%s])=>%d",
                to_cstring(replayed_cursor), to_cstring(flushed_cursor), err);
          }
          else
          {
            TBSYS_LOG(INFO, "start_log_after_switch_to_master(replayed[%s], flushed[%s])",
                to_cstring(replayed_cursor), to_cstring(flushed_cursor));
          }
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "slave->master ERROR, but switch to MASTER anyway, replayed_cursor[%s], flushed_cursor[%s]",
                to_cstring(replayed_cursor), to_cstring(flushed_cursor));
            err = OB_SUCCESS;
          }
          read_thread_queue_.start();
          write_thread_queue_.start();
          if (OB_SUCCESS == err)
          {
            err = root_server_.init_boot_state();
          }

          if (OB_SUCCESS == err)
          {
            int64_t count = 0;
            err = OB_ERROR;
            // refresh new schema for
            while (OB_SUCCESS != err && ObRoleMgr::STOP != role_mgr_.get_state()
                && ObRoleMgr::ERROR != role_mgr_.get_state())
            {
              if (OB_SUCCESS != (err = root_server_.refresh_new_schema(count)))
              {
                TBSYS_LOG(WARN, "root_server.refresh_new_schema(count)=>%d", err);
              }
              else if (OB_SUCCESS != (err = root_server_.get_last_frozen_version_from_ups(false)))
              {
                TBSYS_LOG(WARN,"get frozen version failed");
              }
            }
          }

          if (OB_SUCCESS != err)
          {
            role_mgr_.set_state(ObRoleMgr::ERROR);
            TBSYS_LOG(INFO, "set stat to ERROR");
          }
          else
          {
            role_mgr_.set_state(ObRoleMgr::ACTIVE);
            TBSYS_LOG(INFO, "set stat to ACTIVE");
            is_registered_ = false;
            root_server_.after_switch_to_master();
            TBSYS_LOG(WARN, "rootserver slave switched to master");
          }

          if (err == OB_SUCCESS)
          {
            TBSYS_LOG(INFO, "start merge check");
            root_server_.start_merge_check();
          }

          while (true)
          {
            if (ObRoleMgr::STOP == role_mgr_.get_state()
                || ObRoleMgr::ERROR == role_mgr_.get_state())
            {
              TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
              break;
            }
            usleep(10 * 1000);
          }
        }
      }
      if (ObRoleMgr::ERROR == role_mgr_.get_state())
      {
        TBSYS_LOG(ERROR, "check role manager stat error");
      }
      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();
      TBSYS_LOG(INFO, "[NOTICE] server terminated");
      return err;
    }

    //add pangtianze [Paxos rs_election] 20150701:b
    int ObRootWorker::start_as_master_leader()
    {
      int err = OB_SUCCESS;
      //add pangtianze [Paxos rs_election] 20170221:b
      if (0 == get_config().rs_paxos_number || 0 == get_config().ups_quorum_scale)
      {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "invalid command params when leader starting, paxos number and quorum scale can not be equal 0");
      }
      //add:e

      if (err == OB_SUCCESS)
      {
        // start as master
          int ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "[NOTICE] master start step1");
          root_server_.init_boot_state();
          if (ret == OB_SUCCESS)
          {
               TBSYS_LOG(INFO, "[NOTICE] background threads started");
               root_server_.start_threads();

          }
          if (ret == OB_SUCCESS)
          {
            TBSYS_LOG(INFO, "[NOTICE] master start step3");
            root_server_.start_merge_check();
          }

          if (ret == OB_SUCCESS)
          {
            TBSYS_LOG(INFO, "[NOTICE] master start step4");
            root_server_.reset_hb_time();
          }

          if (ret == OB_SUCCESS)
          {
            TBSYS_LOG(INFO, "[NOTICE] master start step5");
            role_mgr_.set_state(ObRoleMgr::ACTIVE);

            TBSYS_LOG(INFO, "[NOTICE] master start step2");
            if (err == OB_SUCCESS)
            {
              read_thread_queue_.start();
              write_thread_queue_.start();
              election_thread_queue_.start(); //try start election mod
              err = root_server_.set_leader_when_starting();
              TBSYS_LOG(INFO, "wait for rootserver followers registered");
              while(err == OB_SUCCESS)
              {
                if (!root_server_.check_all_rs_registered())
                {
                  usleep(10 * 1000);
                }
                else
                {
                  TBSYS_LOG(INFO, "all rootserver registered successful");
                  break;
                }
              }
            }
            if (err == OB_SUCCESS)
            {
              root_server_.start_rs_heartbeat();
              root_server_.start_election_checker();
            }
            root_server_.start_master_rootserver();
            TBSYS_LOG(INFO, "[NOTICE] master start-up finished");
            root_server_.dump_root_table();
          }
      }
      // wait finish or switch
      if (err == OB_SUCCESS)
      {
        while (ObRoleMgr::INIT != role_mgr_.get_state() //  lease is invalid, should reregister to master
                                                        // but now just let it exit.
            && ObRoleMgr::STOP != role_mgr_.get_state() // stop normally
            && ObRoleMgr::ERROR != role_mgr_.get_state())
        {
          if (ObRoleMgr::SWITCHING == role_mgr_.get_state()
              && ObRoleMgr::SLAVE == role_mgr_.get_role()) // lease is not valid and vip is not mine
          {
            if (OB_SUCCESS != (err = switch_to_master()))
            {
              TBSYS_LOG(ERROR, "rootserver slave failed switch to master");
            }
            else
            {
                //add wangdonghui [ups state exception in __all_server] 20170522 :b
                //refresh stale info in __all_server when slave rs switch to master
                root_server_.commit_task(RS_ROLE_CHANGE, OB_UPDATESERVER, self_addr_, 0, "hb server version null");
                //add :e
            }
          }

          if (ObRoleMgr::STOP == role_mgr_.get_state()
              || ObRoleMgr::ERROR == role_mgr_.get_state())
          {
            TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
            break;
          }
          else
          {
            //do nothing
          }
          usleep(10 * 1000); // 10 ms
        }
      }
      if (ObRoleMgr::ERROR == role_mgr_.get_state())
      {
        TBSYS_LOG(ERROR, "check role manager stat error");
      }
      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();
      TBSYS_LOG(INFO, "[NOTICE] server terminated");
      return err;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150619:b
    int ObRootWorker::switch_to_slave()
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "rootserver slave begin master to slave");
      TBSYS_LOG(INFO, "[NOTICE] set role to slave");
      role_mgr_.set_role(ObRoleMgr::SLAVE);
      //add pangtianze [Paxos rs_election] 20170628:b
      //role_mgr_.set_state(ObRoleMgr::SWITCHING);
      //add:e
      //add chujiajia [Paxos rs_election] 20151109:b
      TBSYS_LOG(INFO, "[NOTICE] reset ups info");
      root_server_.get_ups_manager()->ups_array_reset();
      //add:e
      //add pangtianze [Paxos rs_election] 20170717:b
      TBSYS_LOG(INFO, "[NOTICE] reset ms/cs server_info");
      root_server_.get_server_manager().reset_server_info();

      TBSYS_LOG(INFO, "[NOTICE] clean root table");
      root_server_.slave_clean_root_table();
      //add:e

//      if (ret == OB_SUCCESS)  //del pangtianze [Paxos rs_election] 20170629:b
//      {
//        root_server_.stop_merge_check();
//        TBSYS_LOG(INFO, "stop merge check");
//      }
      //del chujiajia [Paxos rs_election] 20151027:b
      if (ret == OB_SUCCESS)
      {
        //read_thread_queue_.stop();
        //read_thread_queue_.wait();
        //write_thread_queue_.stop();
        //write_thread_queue_.wait();
      }
      //del:e
      if (ret == OB_SUCCESS)
      {
        role_mgr_.set_state(ObRoleMgr::ACTIVE);
        TBSYS_LOG(INFO, "set stat to ACTIVE");
        TBSYS_LOG(INFO, "rootserver master switched to slave");
      }
      else
      {
        role_mgr_.set_state(ObRoleMgr::ERROR);
        TBSYS_LOG(INFO, "set stat to ERROR");
      }
      return ret;
    }
    int ObRootWorker::switch_to_master()
    {
      int err = OB_SUCCESS;
        TBSYS_LOG(INFO, "rootserver slave begin switch to master");
        //add chujiajia [Paxos rs_election] 20151109:b
        root_server_.get_ups_manager()->ups_array_reset();
        root_server_.get_server_manager().reset_server_info();
        //add:e
        //read_thread_queue_.start();
        //write_thread_queue_.start();

        TBSYS_LOG(INFO, "[NOTICE] start to boot recover");
        root_server_.boot_recover();
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "[NOTICE] set role to master");
          //root_server_.reset_hb_time();
          role_mgr_.set_role(ObRoleMgr::MASTER);
          //add lbzhong [Paxos Cluster.Balance] 201607018:b
          ///after set new master flag, rs can reload replica num when checking balance
          //role_mgr_.set_new_master_flag(true); //del pangtianze [Paxos Cluster.Balance] 20170629
          //add:e
          char ip_tmp[OB_MAX_SERVER_ADDR_SIZE]="";
          self_addr_.ip_to_string(ip_tmp,sizeof(ip_tmp));
          get_config().master_root_server_ip.set_value(ip_tmp);
          get_config().master_root_server_port=self_addr_.get_port();
          if (OB_SUCCESS == err)
          {
            err = root_server_.init_boot_state();
          }
        }
        //add huangjiawei, pangtianze [Paxos rs_switch] 20170302:b
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "[NOTICE] start to notify cs/ms re-regist");
          rt_force_server_regist();
        }
        //add:e
        /*else
        {
          TBSYS_LOG(INFO, "boot recover failed");
        }*/

        //add bingo [Paxos __all_cluster] 20170714:b
        bool need_refresh_all_cluster = false;
        //add:e
        if (OB_SUCCESS == err)
        {
          int64_t count = 0;
          err = OB_ERROR;
          // refresh new schema for
            while(root_server_.get_alive_cs_number() == 0
                   //add pangtianze [Paxos rs_election] [avoid rs in while forever when it's role swith to slave] 20170720:b
                  && ObRoleMgr::MASTER == role_mgr_.get_role())
            {
              TBSYS_LOG(WARN, "alive_cs < 1, need wait!");
              usleep(50 * 1000);
            }

           while (ObRoleMgr::MASTER == role_mgr_.get_role() && OB_SUCCESS != err && ObRoleMgr::STOP != role_mgr_.get_state()
              && ObRoleMgr::ERROR != role_mgr_.get_state())
          //mod:e
          {

            ObServer ups_addr;
            bool use_inner_port = false;
            err = root_server_.get_master_ups(ups_addr, use_inner_port);
            if (OB_ENTRY_NOT_EXIST != err)
            {
              /** add pangtianze [Paxos rs_switch] 20170419:b
               * [Paxos bugfix: sometimes, in the situation of partition,
               * new master rs refresh_new_schema is very slow
               */
              TBSYS_LOG(INFO, "reset ms list in schema service provider when switching to master");

              if (OB_SUCCESS != (err = root_server_.get_schema_service_ms_provider()->reset_when_master_swtich()))
              {
                 TBSYS_LOG(WARN, "reset ms list in schema service provider failed, err=%d", err);
              }
               /** add:e  */
              else if (OB_SUCCESS != (err = root_server_.refresh_new_schema(count)))
              {
                TBSYS_LOG(WARN, "root_server.refresh_new_schema(count)=>%d", err);
              }
              else if (OB_SUCCESS != (err = root_server_.get_last_frozen_version_from_ups(false)))
              {
                TBSYS_LOG(WARN,"get frozen version failed");
              }
              //mod:e

              //add bingo [Paxos Cluster.Balance] 20161024:b
              if(OB_SUCCESS == err)
              {
                ObSEArray<int64_t, OB_MAX_CLUSTER_COUNT> cluster_ids;
                ObServer ms;
                //bool query_master = true;
                ObScanParam scan_param;
                int64_t retry_num = 0;
                if (OB_SUCCESS != (err = root_server_.get_schema_service_ms_provider()->get_ms(scan_param, retry_num, ms)))
                {
                  TBSYS_LOG(WARN, "get mergeserver address failed, err %d", err);
                }
                else if (OB_SUCCESS != (err = get_sql_proxy().get_rpc_stub().fetch_master_cluster_ids(ms, cluster_ids,config_.network_timeout)))
                {
                  TBSYS_LOG(WARN, "fetch master cluster id list failed, err %d, ms %s", err, to_cstring(ms));
                }
                else if(cluster_ids.count() == 1 && cluster_ids.at(0) > 0 && cluster_ids.at(0) <= OB_MAX_CLUSTER_ID)
                {
                  if(OB_SUCCESS != (err = get_root_server().set_master_cluster_id((int32_t)cluster_ids.at(0))))
                  {
                    TBSYS_LOG(WARN,"set master cluster id failed, err=%d", err);
                  }
                }
                else
                {
                  if(OB_SUCCESS != (err = get_root_server().set_master_cluster_id(self_addr_.cluster_id_)))
                  {
                    TBSYS_LOG(WARN,"set self cluster as master cluster failed, err=%d", err);
                  }
                  else
                  {
                    need_refresh_all_cluster = true;  //add bingo [Paxos __all_cluster] 20170714:b:e
                  }
                }
              }
              //add:e
            }
            usleep(100 * 1000);//500ms
            //mod:e
          }
        }

        if (OB_SUCCESS != err)
        {
          role_mgr_.set_state(ObRoleMgr::ERROR);
          TBSYS_LOG(INFO, "set stat to ERROR");
        }
        else
        {
          role_mgr_.set_state(ObRoleMgr::ACTIVE);
          TBSYS_LOG(INFO, "set stat to ACTIVE");
          is_registered_ = false;
          root_server_.after_switch_to_master();
          TBSYS_LOG(INFO, "rootserver slave switched to master");
          //add chujiajia[Paxos rs_election]20151020:b
          if (OB_SUCCESS != (err = config_mgr_.dump2file()))
          {
            TBSYS_LOG(WARN, "Dump to file failed! ret = %d", err);
          }
          char server_version[OB_SERVER_VERSION_LENGTH] = "";
          get_package_and_svn(server_version, sizeof(server_version));
          if (OB_SUCCESS != (err = root_server_.get_rs_node_mgr()->refresh_inner_table(SERVER_ONLINE, self_addr_, common::ObRoleMgr::MASTER, server_version)))
          {
            TBSYS_LOG(WARN, "refresh inner table failed! err=%d", err);
          }
          //add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
          ObServer old_rs_master;
          if (OB_INVALID_INDEX == root_server_.get_rs_node_mgr()->get_old_rs_master(old_rs_master))
          {
            //if (OB_SUCCESS != (err = root_server_.commit_task(SERVER_OFFLINE, OB_ROOTSERVER, old_rs_master, 0, "hb server version null")))
            if (OB_SUCCESS != (err = root_server_.get_rs_node_mgr()->refresh_inner_table(SERVER_OFFLINE, old_rs_master, common::ObRoleMgr::MASTER, server_version)))
            {
              TBSYS_LOG(WARN, "commit task to refresh inner table failed! err=%d", err);
            }
          }
          root_server_.get_rs_node_mgr()->set_old_rs_master(self_addr_);
          //add:e
        }
        //add bingo [Paxos __all_cluster] 20170714:b:e
        if(OB_SUCCESS == err && need_refresh_all_cluster && (OB_SUCCESS != (err = set_new_master_cluster(0, get_root_server().get_master_cluster_id()))))
        {
          TBSYS_LOG(WARN,"fail to refresh table after set self cluster as master cluster, err=%d", err);
        }
        //add:e
        if (OB_SUCCESS == err)
        {
          int64_t end_time = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "RS slave->master, switch duration_time=[%ld]ms" , (end_time - root_server_.get_rs_node_mgr()->get_begin_time()) / 1000);
        }
//        if (err == OB_SUCCESS)  //del pangtianze [Paxos rs_election] 20170629:b
//        {
//          TBSYS_LOG(INFO, "start merge check");
//          root_server_.start_merge_check();
//        }
        //add pangtianze [Paxos rs_switch] 20170620:b
        //restart index monitor
        get_monitor()->start();
        //add:e

      return err;
    }
    int ObRootWorker::start_as_slave_follower()
    {
      int err = OB_SUCCESS;
      //check start params
      if (0 != get_config().rs_paxos_number || 0 != get_config().ups_quorum_scale)
      {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "invalid command params when follower starting, needn't input -U and -u");
      }
      //start election thread queue
      if (err == OB_SUCCESS)
      {        
        election_thread_queue_.start();
      }
      //start read and write queue
      if (err == OB_SUCCESS)
      {
          read_thread_queue_.start();
          write_thread_queue_.start();
      }
      ObFetchParam fetch_param;
      if (err == OB_SUCCESS)
      {
          //register to master rs
        err = slave_register_(fetch_param);
      }
      if (err == OB_SUCCESS)
      {
        // wait for rootserver leader takeover
        int ret = OB_SUCCESS;
        ObServer rs_leader;
        ret = root_server_.get_rs_leader(rs_leader);
        while (true)
        {
          if (rs_leader == rt_master_)
          {
            TBSYS_LOG(INFO, "find rootserver leader, leader=%s", rs_leader.to_cstring());
            break;
          }
          else if (rs_leader.is_valid() && rs_leader != rt_master_)
          {
            TBSYS_LOG(ERROR, "find unexpected rootserver leader when starting, so will exit program, rs=%s", rs_leader.to_cstring());
            ret = OB_ERROR;
            break;
          }
          ret = root_server_.get_rs_leader(rs_leader);
          usleep(2 * 1000 * 1000); //2s
        }
      }
      if (err == OB_SUCCESS)
      {
        //start election mod
        get_root_server().start_election_checker();
      }
      if (OB_SUCCESS == err)
      {
        err = get_boot_state_from_master();
      }
      if (err == OB_SUCCESS)
      {
        root_server_.init_boot_state();
      }

      if (err == OB_SUCCESS)
      {
        // start as slave
            role_mgr_.set_state(ObRoleMgr::ACTIVE);
            //root_server_.init_boot_state();
            // we SHOULD start root_modifier after recover checkpoint
            root_server_.start_threads();
            TBSYS_LOG(INFO, "slave root_table_modifier, balance_worker, heartbeat_checker, rs_checker threads started");

            root_server_.start_rs_heartbeat();
            TBSYS_LOG(INFO, "slave rs heartbeat checker start");
            //add pangtianze [Paxos rs_election] 20170629:b
            if (err == OB_SUCCESS)
            {
              TBSYS_LOG(INFO, "slave merge checker start");
              root_server_.start_merge_check();
            }
            //add:e
            //add chujiajia[Paxos rs_election]20151020:b
            char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
            ObServer rs_leader;
            root_server_.get_rs_leader(rs_leader);
            rs_leader.ip_to_string(ip_tmp,sizeof(ip_tmp));
            root_server_.get_config().master_root_server_ip.set_value(ip_tmp);
            root_server_.get_config().master_root_server_port = rs_leader.get_port();
            if (OB_SUCCESS != (err = config_mgr_.dump2file()))
            {
              TBSYS_LOG(WARN, "Dump to file failed! err=%d", err);
            }

            //add jinty [Paxos Cluster.Balance] 20160809:b
            //wait to get ms_list
            std::vector<ObServer> ms_list;
            while(ms_list.empty())
            {
              usleep(100* 1000);
              config_mgr_.get_ms_list(ms_list);
            }
            //init ms provider ,then schema service can scan inner table
            root_server_.get_schema_service_ms_provider()->reset(ms_list);
            //local_ms_for_slave_rs_.init(config_mgr_.get_ms_list_ptr());
            //root_server_.init_schema_service_scan_helper(&local_ms_for_slave_rs_);

            //add jinty [Paxos Cluster.Balance] 20160708:b
//            std::vector<ObServer> list;
//            //TBSYS_LOG(INFO,"rs slave::print ms list start");
//            if(OB_SUCCESS == ( err = config_mgr_.get_ms_list(list)) )
//            {
//              std::vector<ObServer>::const_iterator iter;
//              for (iter = list.begin(); iter != list.end(); iter++)
//              {
//                TBSYS_LOG(INFO, "rs slave has Mergeserver %s", to_cstring(*iter));
//              }
//            }
            //TBSYS_LOG(INFO,"rs slave::print ms list end");
            //add e         
      }


      if (err == OB_SUCCESS)
      {
        while (ObRoleMgr::INIT != role_mgr_.get_state() //  lease is invalid, should reregister to master
                                                        // but now just let it exit.
            && ObRoleMgr::STOP != role_mgr_.get_state() // stop normally
            && ObRoleMgr::ERROR != role_mgr_.get_state())
        {
          if (ObRoleMgr::SWITCHING == role_mgr_.get_state()
              && ObRoleMgr::SLAVE == role_mgr_.get_role()) // lease is not valid and vip is not mine
          {
            if (OB_SUCCESS != (err = switch_to_master()))
            {
              TBSYS_LOG(ERROR, "rootserver slave failed switch to master");
            }
            else
            {
                //add wangdonghui [ups state exception in __all_server] 20170522 :b
                //refresh stale info in __all_server when slave rs switch to master
                root_server_.commit_task(RS_ROLE_CHANGE, OB_UPDATESERVER, self_addr_, 0, "hb server version null");
                //add :e
            }
          }
          else if (ObRoleMgr::STOP == role_mgr_.get_state()
              || ObRoleMgr::ERROR == role_mgr_.get_state())
          {
            TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
            break;
          }
          else
          {
            //do nothing
          }
          usleep(10 * 1000); // 10 ms
        }
      }
      if (ObRoleMgr::ERROR == role_mgr_.get_state())
      {
        TBSYS_LOG(ERROR, "check role manager stat error");
      }
      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();
      TBSYS_LOG(INFO, "[NOTICE] server terminated");
      return err;
    }
    //add:e

    //send obi role to slave_rs
    int ObRootWorker::send_obi_role(common::ObiRole obi_role)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "send obi_role to slave rootserver");
      ret = slave_mgr_.set_obi_role(obi_role);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to set obi role. err=%d", ret);
      }
      return ret;
    }

    int ObRootWorker::get_obi_role_from_master()
    {
      int ret = OB_SUCCESS;
      ObiRole role;
      const static int SLEEP_US_WHEN_INIT = 2000*1000; // 2s
      while(true)
      {
        ret = rt_rpc_stub_.get_obi_role(rt_master_, config_.network_timeout, role);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to get obi_role from the master, err=%d", ret);
          usleep(SLEEP_US_WHEN_INIT);
        }
        else if (ObiRole::INIT == role.get_role())
        {
          TBSYS_LOG(INFO, "we should wait when obi_role=INIT");
          usleep(SLEEP_US_WHEN_INIT);
        }
        else
        {
          ret = root_server_.set_obi_role(role);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to set_obi_role, err=%d", ret);
          }
          break;
        }
        if (ObRoleMgr::STOP == role_mgr_.get_state())
        {
          TBSYS_LOG(INFO, "server stopped, break");
          ret = OB_ERROR;
          break;
        }
      } // end while
      return ret;
    }
    int ObRootWorker::get_boot_state_from_master()
    {
      int ret = OB_SUCCESS;
      bool boot_ok = false;
      const static int SLEEP_US_WHEN_INIT = 2000*1000; // 2s
      while(true)
      {
        ret = rt_rpc_stub_.get_boot_state(rt_master_, config_.network_timeout, boot_ok);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to get boot state from the master, err=%d", ret);
          usleep(SLEEP_US_WHEN_INIT);
        }
        else if (boot_ok)
        {
          ret = root_server_.init_first_meta();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to init first meta file, err=%d", ret);
          }
          break;
        }
        else
        {
          break;
        }
        if (ObRoleMgr::STOP == role_mgr_.get_state()
            || ObRoleMgr::ERROR == role_mgr_.get_state())
        {
          TBSYS_LOG(INFO, "server stopped, break");
          ret = OB_ERROR;
          break;
        }
      } // end while
      return ret;
    }

    void ObRootWorker::destroy()
    {
      root_server_.grant_eternal_ups_lease();
      role_mgr_.set_state(ObRoleMgr::STOP);

      timer_.destroy();

      if (ObRoleMgr::SLAVE == role_mgr_.get_role())
      {
        if (is_registered_)
        {
          rt_rpc_stub_.slave_quit(rt_master_, self_addr_, config_.network_timeout);
          is_registered_ = false;
        }
        log_thread_queue_.stop();
        fetch_thread_.stop();
        log_replay_thread_.stop();
        //add pangtianze [Paxos rs_election] 20170720:b
        read_thread_queue_.stop();
        write_thread_queue_.stop();
        //add:e
        check_thread_.stop();
      }
      else
      {
        read_thread_queue_.stop();
        write_thread_queue_.stop();
        check_thread_.stop();
      }
      //add pangtianze [Paxos rs_election] 20150619:b
      election_thread_queue_.stop();
      //add:e
      TBSYS_LOG(INFO, "stop flag set");
      root_server_.stop_threads();
      wait_for_queue();
      ObBaseServer::destroy();
    }

    void ObRootWorker::wait_for_queue()
    {
      if (ObRoleMgr::SLAVE == role_mgr_.get_role())
      {
        log_thread_queue_.wait();
        TBSYS_LOG(INFO, "log thread stopped");
        fetch_thread_.wait();
        TBSYS_LOG(INFO, "fetch thread stopped");
        log_replay_thread_.wait();
        TBSYS_LOG(INFO, "replay thread stopped");
        //add pangtianze [Paxos rs_election] 20170720:b
        read_thread_queue_.wait();
        TBSYS_LOG(INFO, "read threads stopped");
        write_thread_queue_.wait();
        TBSYS_LOG(INFO, "write threads stopped");
        //add:e
        check_thread_.wait();
        TBSYS_LOG(INFO, "check thread stopped");
      }
      else
      {
        read_thread_queue_.wait();
        TBSYS_LOG(INFO, "read threads stopped");
        write_thread_queue_.wait();
        TBSYS_LOG(INFO, "write threads stopped");
        check_thread_.wait();
        TBSYS_LOG(INFO, "check thread stopped");
      }
      //add pangtianze [Paxos rs_election] 20150619:b
      election_thread_queue_.wait();
      //add:e
    }

    int ObRootWorker::submit_restart_task()
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_INNER_MSG_AFTER_RESTART, read_thread_queue_,
                (int32_t)config_.read_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task to delete tablet. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "submit after_restart task.");
        }
      }
      return ret;
    }

    int ObRootWorker::submit_delete_tablets_task(const common::ObTabletReportInfoList& delete_list)
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = delete_list.serialize(buff.get_data(), buff.get_capacity(), buff.get_position())))
        {
          TBSYS_LOG(WARN, "fail to serialize delete_list. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_INNER_MSG_DELETE_TABLET, write_thread_queue_,
                (int32_t)config_.write_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task to delete tablet. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "submit async task to delete tablet");
        }
      }
      return ret;
    }

    int ObRootWorker::handlePacket(ObPacket *packet)
    {
      int ret = OB_SUCCESS;
      bool ps = true;
      int packet_code = packet->get_packet_code();

      switch(packet_code)
      {
        case OB_SEND_LOG:
        case OB_SET_OBI_ROLE_TO_SLAVE:
        case OB_GRANT_LEASE_REQUEST:
          if (ObRoleMgr::SLAVE == role_mgr_.get_role())
          {
            if (packet_code == OB_SEND_LOG)
            {
              TBSYS_LOG(INFO, "receive new log");
            }
            ps = log_thread_queue_.push(packet, (int32_t)config_.log_queue_size, false, false);
          }
          else
          {

            ps = false;
          }
          break;
        //add pangtianze [Paxos rs_election] 20150612
        //election queue
        case OB_RS_RS_HEARTBEAT:
        case OB_RS_VOTE_REQUEST:
        case OB_RS_LEADER_BROADCAST:
        case OB_RS_SLAVE_INIT_FIRST_META:
        case OB_SET_SPECIFIED_RS_PRIORIYT:
        case OB_RS_SET_LEADER:
        case OB_RS_VOTE_REQUEST_TASK:
        case OB_RS_LEADER_BROADCAST_TASK:
        //add bingo [Paxos rs_election] 20161009
        case OB_GET_RS_PRIORITY:
        //add:e          
        //add chujiajia [Paxos rs_election] 20151102:b
        case OB_RS_CHANGE_PAXOS_NUM_REQUEST:
        case OB_RS_CHANGE_UPS_QUORUM_SCALE_REQUEST:
        //add:e
        //add bingo [Paxos Cluster.Flow.UPS] 20170405:b
        case OB_SYNC_IS_STRONG_CONSISTENT:
        //add:e
          ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          break;
        case OB_SLAVE_REG:
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          }
          else
          {
            ps = false;
          }
          break;
        case OB_RS_RS_HEARTBEAT_RESPONSE:
          if (ObRootElectionNode::OB_LEADER == root_server_.get_rs_node_role())
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          }
          else
          {
            ps = false;
          }
          break;
        //add chujiajia [Paxos rs_election] 20151102:b
        case OB_CHANGE_RS_PAXOS_NUMBER:
        case OB_CHANGE_UPS_QUORUM_SCALE:
        //add:e
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            //mod pangtianze [Paxos] 20170614:b
            //ps = read_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
             ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false);
            //mod:e
          }
          else
          {
            ps = false;
          }
          break;
        case OB_RS_VOTE_REQUEST_RESPONSE:
          if (ObRootElectionNode::OB_CANDIDATE == root_server_.get_rs_node_role()
            && root_server_.is_in_vote_phase())
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false, false);
          }
          else
          {
            ps = false;
          }
        break;
        case OB_RS_LEADER_BROADCAST_RESPONSE:
          if (ObRootElectionNode::OB_CANDIDATE == root_server_.get_rs_node_role()
            && root_server_.is_in_broadcast_phase())
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false, false);
          }
          else
          {
             ps = false;
          }
        break;
        //add:e
        // read queue
        case OB_RS_STAT:
        case OB_GET_CONFIG:
        case OB_GET_MASTER_OBI_RS:
        case OB_GET_OBI_ROLE:
        case OB_GET_BOOT_STATE:
        case OB_GET_UPS:
        case OB_GET_CS_LIST:
        case OB_GET_MS_LIST:
        case OB_GET_MASTER_UPS_CONFIG:
        case OB_DUMP_CS_INFO:
        case OB_FETCH_STATS:
        case OB_GET_REQUEST:
        case OB_SCAN_REQUEST:
        case OB_SQL_SCAN_REQUEST:
          //add pangtianze [Paxos rs_election] 20170309:b
            if (ObRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false, false);
            }
            else
            {
              ps = false;
            }
            break;
            //add:e
        // master or slave can set
        case OB_SET_CONFIG:
        case OB_CHANGE_LOG_LEVEL:
        case OB_GET_ROW_CHECKSUM:
        case OB_RS_IMPORT:
        case OB_RS_GET_IMPORT_STATUS:
        case OB_RS_SET_IMPORT_STATUS:
        case OB_RS_NOTIFY_SWITCH_SCHEMA:
        //add pangtianze [Paxos rs_election] 20170309:b
          ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false);
          break;
        //add:e
        case OB_RS_FORCE_CS_REPORT:
        //add liuxiao [secondary index] 20150401
        case OB_GET_OLD_TABLET_COLUMN_CHECKSUM:
        //add e
        //add wenghaixing [secondary index cluster.p2]20150630
        case OB_FETCH_INDEX_STAT:
        //add e
        //mod pangtianze [Paxos rs_election] 20170309:b
//          ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false);
//          break;
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false, false);
          }
          else
          {
            ps = false;
          }
          break;
          //mod:e
        //add pangtianze [Paxos rs_election] 20170311:b
        case OB_RS_UPS_REGISTER:
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false, false);
          }
          else
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          }
          break;
        case OB_SERVER_REGISTER:
        case OB_MERGE_SERVER_REGISTER:
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = write_thread_queue_.push(packet, (int32_t)config_.write_queue_size, false, false);
          }
          else
          {
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          }
          break;
        //add:e
        case OB_REPORT_TABLETS:
       //add liumz, [secondary index static_index_build] 20150324:b
        case OB_REPORT_HISTOGRAMS:
        //add:e
        /*del liumz, [move to read queue]20160811
        //add liuxiao [secondary index] 20150401
        case OB_GET_OLD_TABLET_COLUMN_CHECKSUM:
        //add e
        //add wenghaixing [secondary index cluster.p2]20150630
        case OB_FETCH_INDEX_STAT:
        //add e
        */
        //del pangtianze [Paxos rs_election] 20170311:b
//        case OB_SERVER_REGISTER:
//        case OB_MERGE_SERVER_REGISTER:
        //del:e
        case OB_MIGRATE_OVER:
        case OB_CREATE_TABLE:
        case OB_ALTER_TABLE:
        case OB_FORCE_CREATE_TABLE_FOR_EMERGENCY:
        case OB_FORCE_DROP_TABLE_FOR_EMERGENCY:
        case OB_DROP_TABLE:
        case OB_TRUNCATE_TABLE: //add zhaoqiong [Truncate Table]:20160318
        //add wenghaixing [secondary index drop index]20141223
        case OB_DROP_INDEX:
        //add e
        case OB_REPORT_CAPACITY_INFO:
        //del pangtianze [Paxos rs_election] 20150701:b
        //case OB_SLAVE_REG:
        //del:e
        case OB_WAITING_JOB_DONE:
        case OB_RS_INNER_MSG_CHECK_TASK_PROCESS:
        case OB_CS_DELETE_TABLETS:
        case OB_UPDATE_SERVER_REPORT_FREEZE:
        case OB_RS_PREPARE_BYPASS_PROCESS:
        case OB_RS_START_BYPASS_PROCESS:
        case OB_CS_DELETE_TABLE_DONE:
        case OB_CS_LOAD_BYPASS_SSTABLE_DONE:
        //add pangtianze [Paxos rs_election] 20160928:b
        case OB_RS_ADMIN_SET_LEADER:
        case OB_SET_ELECTION_PRIORIYT:
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161020:b
        case OB_SET_MASTER_CLUSTER_ID:
        //add:e
        //the packet will cause write to b+ tree
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = write_thread_queue_.push(packet, (int32_t)config_.write_queue_size, false, false);
          }
          else
          {
            ps = false;
          }
          break;
        case OB_RENEW_LEASE_REQUEST:
        case OB_SLAVE_QUIT:
        case OB_SET_OBI_ROLE:
        case OB_FETCH_SCHEMA:
        case OB_WRITE_SCHEMA_TO_FILE:
        case OB_FETCH_SCHEMA_VERSION:
        case OB_RS_GET_LAST_FROZEN_VERSION:
        case OB_HEARTBEAT:
        case OB_MERGE_SERVER_HEARTBEAT:
        case OB_GET_PROXY_LIST:
        case OB_RS_CHECK_ROOTTABLE:
        case OB_GET_UPDATE_SERVER_INFO:
        case OB_RS_DUMP_CS_TABLET_INFO:
        case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
        case OB_RS_ADMIN:
        case OB_RS_UPS_HEARTBEAT_RESPONSE:
        //del pangtianze [Paxos rs_election] 20170311:b
        //case OB_RS_UPS_REGISTER:
        //del:e
        case OB_RS_UPS_SLAVE_FAILURE:
        case OB_SET_UPS_CONFIG:
        case OB_SET_MASTER_UPS_CONFIG:
        case OB_CHANGE_UPS_MASTER:
        case OB_CHANGE_TABLE_ID:
        case OB_CS_IMPORT_TABLETS:
        case OB_RS_SHUTDOWN_SERVERS:
        case OB_RS_RESTART_SERVERS:
        case OB_RS_CHECK_TABLET_MERGED:
        case OB_RS_SPLIT_TABLET:
        case OB_HANDLE_TRIGGER_EVENT:
        case OB_RS_ADMIN_START_IMPORT:
        case OB_RS_ADMIN_START_KILL_IMPORT:
		//add peiouya [Get_masterups_and_timestamp] 20141017:b
        case OB_GET_UPDATE_SERVER_INFO_AND_TIMESTAMP:
		//add 20141017:e
        //add zhaoqiong [Schema Manager] 20150327:b
        case OB_HANDLE_DDL_TRIGGER_EVENT:
        case OB_FETCH_SCHEMA_NEXT:
        //add:e
        //add bingo [Paxos rs_election] 20161009
        case OB_MASTER_GET_ALL_PRIORITY:
        //add:e
        //add bingo [Paxos rs_election] 20161116
        case OB_GET_REPLICA_NUM:
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 201607020:b
        case OB_RS_DUMP_BALANCER_INFO:
        //add:e
        //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        case OB_GET_CLUSTER_UPS:
        //add:e
        //add bingo [Paxos sstable info to rs log] 20170614:b
        case OB_DUMP_SSTABLE_INFO:
        //add:e
        //add bingo [Paxos table replica] 20170620:b
        case OB_GET_TABLE_REPLICA_NUM:
        //add:e
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false, false);
          }
          else
          {
            ps = false;
          }
          break;
        //add bingo [Paxos rs management] 20170301:b
        case OB_GET_RS_LEADER:
        //add:e
          if (ObRoleMgr::MASTER == role_mgr_.get_role())
          {
            ps = read_thread_queue_.push(packet, (int32_t)config_.read_queue_size, false, false);
          }
          else if (ObRootElectionNode::OB_FOLLOWER == root_server_.get_rs_node_role())
          {
            if(packet_code == OB_GET_RS_LEADER){
            }
            ps = election_thread_queue_.push(packet, (int32_t)config_.election_queue_size, false);
          }
          else
          {
            ps = false;
          }
          break;
        //add:e
        case OB_PING_REQUEST: // response PING immediately
          ps = true;
          {
            ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
            ObDataBuffer thread_buffer(my_buffer->current(), my_buffer->remain());
            int return_code = rt_ping(packet->get_api_version(), *packet->get_buffer(),
                packet->get_request(), packet->get_channel_id(), thread_buffer);
            if (OB_SUCCESS != return_code)
            {
              TBSYS_LOG(WARN, "response ping error. return code is %d", return_code);
            }
          }
          break;
        default:
          ps = false; // so this unknown packet will be freed
          if (NULL != packet->get_request()
              && NULL != packet->get_request()->ms
              && NULL != packet->get_request()->ms->c)
          {
            TBSYS_LOG(WARN, "UNKNOWN packet %d src=%s, ignore this", packet_code,
                get_peer_ip(packet->get_request()));
          }
          else
          {
            TBSYS_LOG(ERROR, "UNKNOWN packet %d from UNKNOWN src, ignore this", packet_code);
          }
          break;
      }
      if (!ps)
      {
        if (OB_FETCH_STATS != packet_code)
        {
          //mod pangtianze [Paxos rs_election] 20150814:b
          //TBSYS_LOG(ERROR, "packet %d can not be distribute to queue, role=%d rqueue_size=%ld wqueue_size=%ld",
          //    packet_code, role_mgr_.get_role(), read_thread_queue_.size(), write_thread_queue_.size());
          TBSYS_LOG(ERROR, "packet %d can not be distribute to queue, role=%d rqueue_size=%ld wqueue_size=%ld, equeue_size=%ld",
              packet_code, role_mgr_.get_role(), read_thread_queue_.size(), write_thread_queue_.size(), election_thread_queue_.size());
          //mod:e
        }
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObRootWorker::handleBatchPacket(ObPacketQueue &packetQueue)
    {
      UNUSED(packetQueue);
      TBSYS_LOG(ERROR, "you should not reach this, not supporrted");
      return OB_SUCCESS;
    }

    bool ObRootWorker::handlePacketQueue(ObPacket *packet, void *args)
    {
      bool ret = true;
      int return_code = OB_SUCCESS;
      static __thread int64_t worker_counter = 0;
      static volatile uint64_t total_counter = 0;

      ObPacket* ob_packet = packet;
      int packet_code = ob_packet->get_packet_code();
      int version = ob_packet->get_api_version();
      uint32_t channel_id = ob_packet->get_channel_id();//tbnet need this

      int64_t source_timeout = ob_packet->get_source_timeout();
      if (source_timeout > 0)
      {
        int64_t block_us = tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts();
        int64_t expected_process_us = config_.expected_request_process_time;
        PROFILE_LOG(DEBUG, PACKET_RECEIVED_TIME, ob_packet->get_receive_ts());
        PROFILE_LOG(DEBUG, WAIT_TIME_US_IN_QUEUE, block_us);
        if (source_timeout <= expected_process_us)
        {
          expected_process_us = 0;
        }
        if (block_us + expected_process_us > source_timeout)
        {
          TBSYS_LOG(WARN, "packet timeout, pcode=%d timeout=%ld block_us=%ld expected_us=%ld receive_ts=%ld",
              packet_code, source_timeout, block_us, expected_process_us, ob_packet->get_receive_ts());
          return_code = OB_RESPONSE_TIME_OUT;
        }
      }

      if (OB_SUCCESS == return_code)
      {
        return_code = ob_packet->deserialize();
        if (OB_SUCCESS == return_code)
        {
          ObDataBuffer* in_buf = ob_packet->get_buffer();
          if (in_buf == NULL)
          {
            TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
          }
          else
          {
            easy_request_t* req = ob_packet->get_request();
            if (OB_SELF_FLAG != ob_packet->get_target_id()
                && (NULL == req || NULL == req->ms || NULL == req->ms->c))
            {
              TBSYS_LOG(ERROR, "req or req->ms or req->ms->c is NULL should not reach here!");
            }
            else
            {
              ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
              if (my_buffer != NULL)
              {
                ob_reset_err_msg();
                my_buffer->reset();
                ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
                // wirte queue
                if ((void*)WRITE_THREAD_FLAG == args)
                {
                  TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
                  switch(packet_code)
                  {
                    //add by pangtianze [Paxos rs_election] 20160928:b
                    case OB_RS_ADMIN_SET_LEADER:
                      return_code = rt_rs_admin_set_rs_leader(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_ELECTION_PRIORIYT:
                      return_code = rt_set_election_priority(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_REPORT_TABLETS:
                      return_code = rt_report_tablets(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add liumz, [secondary index static_index_build] 20150320:b
                    case OB_REPORT_HISTOGRAMS:
                      return_code = rt_report_histograms(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e

                    case OB_SERVER_REGISTER:
                      return_code = rt_register(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_MERGE_SERVER_REGISTER:
                      return_code = rt_register_ms(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_MIGRATE_OVER:
                      return_code = rt_migrate_over(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CREATE_TABLE:
                      return_code = rt_create_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_FORCE_DROP_TABLE_FOR_EMERGENCY:
                      return_code = rt_force_drop_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_FORCE_CREATE_TABLE_FOR_EMERGENCY:
                      return_code = rt_force_create_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_ALTER_TABLE:
                      return_code = rt_alter_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_DROP_TABLE:
                      return_code = rt_drop_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add zhaoqiong [Truncate Table]:20160318:b
                    case OB_TRUNCATE_TABLE:
                      return_code = rt_truncate_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add wenghaixing [secondary index drop index]20141223
                    case OB_DROP_INDEX:
                      return_code =rt_drop_index(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add e
                    /*del liumz, [move to read queue]20160811
                    //add liuxiao [secondary index] 20150401
                    case OB_GET_OLD_TABLET_COLUMN_CHECKSUM:
                      return_code = rt_get_old_tablet_column_checksum(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add e
                    //add wenghaixing [secondary index cluster.p2]20150630
                    case OB_FETCH_INDEX_STAT:
                      return_code = rt_get_index_process_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add e
                    */
                    case OB_REPORT_CAPACITY_INFO:
                      return_code = rt_report_capacity_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //del pangtianze [Paxos rs_election] 20150701
                    /*
                    case OB_SLAVE_REG:
                      return_code = rt_slave_register(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    */
                    //del:e
                    case OB_WAITING_JOB_DONE:
                      return_code = rt_waiting_job_done(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CS_DELETE_TABLETS:
                      return_code = rt_cs_delete_tablets(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_UPDATE_SERVER_REPORT_FREEZE:
                      return_code = rt_update_server_report_freeze(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_INNER_MSG_DELETE_TABLET:
                      return_code = rt_delete_tablets(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_INNER_MSG_CHECK_TASK_PROCESS:
                      TBSYS_LOG(INFO, "get inner msg to check bypass task process");
                      return_code = rt_check_task_process(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_PREPARE_BYPASS_PROCESS:
                      return_code = rt_prepare_bypass_process(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_START_BYPASS_PROCESS:
                      return_code = rt_start_bypass_process(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CS_DELETE_TABLE_DONE:
                      return_code = rt_cs_delete_table_done(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CS_LOAD_BYPASS_SSTABLE_DONE:
                      return_code = rs_cs_load_bypass_sstable_done(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_OBI_ROLE_TO_SLAVE:
                      return_code = rt_set_obi_role_to_slave(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add bingo [Paxos Cluser.Balance] 20161020:b
                    case OB_SET_MASTER_CLUSTER_ID:
                      return_code = rt_set_master_cluster_id(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    default:
                      return_code = OB_ERROR;
                      break;
                  }
                }
                else if ((void*)LOG_THREAD_FLAG == args)
                {
                  switch(packet_code)
                  {
                    case OB_GRANT_LEASE_REQUEST:
                      return_code = rt_grant_lease(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SEND_LOG:
                      in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
                      return_code = rt_slave_write_log(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_OBI_ROLE_TO_SLAVE:
                      return_code = rt_set_obi_role_to_slave(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    default:
                      return_code = OB_ERROR;
                      break;
                  }
                }
                //add pangtianze [Paxos rs_election] 20150619:b
                else if ((void*)ELECTION_THREAD_FLAG == args)
                {
                  switch(packet_code)
                  {
                    //add pangtianze [Paxos rs_election] 20150731:b
                    case OB_SET_SPECIFIED_RS_PRIORIYT:
                      return_code = rt_set_my_election_priority(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add pangtianze [Paxos rs_election] 20150813:b
                    case OB_RS_SET_LEADER:
                      return_code = rt_set_rs_leader(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_RS_RS_HEARTBEAT:
                      return_code = rt_handle_rs_heartbeat(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_RS_HEARTBEAT_RESPONSE:
                      return_code = rt_handle_rs_heartbeat_resp(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_VOTE_REQUEST_TASK:
                      return_code = rt_send_vote_request(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_LEADER_BROADCAST_TASK:
                      return_code = rt_send_leader_broadcast(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_VOTE_REQUEST:
                      return_code = rt_handle_vote_request(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_VOTE_REQUEST_RESPONSE:
                      return_code = rt_handle_vote_request_resp(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_LEADER_BROADCAST:
                      return_code = rt_handle_leader_broadcast(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_LEADER_BROADCAST_RESPONSE:
                      return_code = rt_handle_leader_broadcast_resp(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_SLAVE_INIT_FIRST_META:
                      return_code = rt_slave_init_first_meta_row(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SLAVE_REG:
                      return_code = rt_slave_register(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add bingo [Paxos rs_election] 20161009
                    case OB_GET_RS_PRIORITY:
                      return_code = rt_get_priority(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_RS_CHANGE_PAXOS_NUM_REQUEST:
                      return_code = rt_handle_change_paxos_num_request(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_CHANGE_UPS_QUORUM_SCALE_REQUEST:
                      return_code = rt_handle_new_ups_quorum_scale_request(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add bingo [Paxos rs management] 20170301:b
                    case OB_GET_RS_LEADER:
                      return_code = rt_get_rs_leader(version, *in_buf, req, channel_id, thread_buff);
                    break;
                    //add:e
                    //add bingo [Paxos Cluster.Flow.UPS] 20170405:
                    case OB_SYNC_IS_STRONG_CONSISTENT:
                       return_code = rt_sync_is_strong_consistent(version, *in_buf, req, channel_id, thread_buff);
                    break;
                    //add:e
                    case OB_RS_UPS_REGISTER:
                    case OB_SERVER_REGISTER:
                    case OB_MERGE_SERVER_REGISTER:
                      return_code = rt_slave_handle_register(version, *in_buf, req, channel_id, thread_buff);
                    break;
                    default:
                      return_code = OB_ERROR;
                      break;
                  }
                }
                //add:e
                // read queue
                else
                {
                  TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
                  switch(packet_code)
                  {
                    case OB_RS_INNER_MSG_AFTER_RESTART:
                      return_code = rt_after_restart(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add huangjianwei [Paxos rs_switch] 20160726:b
                    case OB_RS_GET_SERVER_STATUS:                      
                      return_code = rt_get_server_status(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_WRITE_SCHEMA_TO_FILE:
                      return_code = rt_write_schema_to_file(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_FETCH_SCHEMA:
                      return_code = rt_fetch_schema(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add zhaoqiong [Schema Manager] 20150327:b
                    case OB_FETCH_SCHEMA_NEXT:
                      return_code = rt_fetch_schema_next(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_FETCH_SCHEMA_VERSION:
                      return_code = rt_fetch_schema_version(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_REQUEST:
                      return_code = rt_get(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SCAN_REQUEST:
                      return_code = rt_scan(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SQL_SCAN_REQUEST:
                      return_code = rt_sql_scan(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_HEARTBEAT:
                      return_code = rt_heartbeat(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_MERGE_SERVER_HEARTBEAT:
                      return_code = rt_heartbeat_ms(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_DUMP_CS_INFO:
                      return_code = rt_dump_cs_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_CHECK_TABLET_MERGED:
                      return_code = rt_check_tablet_merged(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add wenghaixing [secondary index] 20141110
                    case OB_CREATE_INDEX_COMMAND:
                      return_code = rt_create_index_command(version, *in_buf, req, channel_id, thread_buff);
                      break;
                     //add e
                    //add liuxiao [secondary index] 20150401
                    case OB_GET_OLD_TABLET_COLUMN_CHECKSUM:
                      return_code = rt_get_old_tablet_column_checksum(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add e
                    //add wenghaixing [secondary index cluster.p2]20150630
                    case OB_FETCH_INDEX_STAT:
                      return_code = rt_get_index_process_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add e
                    case OB_FETCH_STATS:
                      return_code = rt_fetch_stats(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_UPDATE_SERVER_INFO:
                      {
                        TBSYS_LOG(DEBUG, "server addr=%s fetch master update server info.", get_peer_ip(packet->get_request()));
                        return_code = rt_get_update_server_info(version, *in_buf, req, channel_id, thread_buff);
                        break;
                      }
					//add peiouya [Get_masterups_and_timestamp] 20141017:b
                    case OB_GET_UPDATE_SERVER_INFO_AND_TIMESTAMP:
                      {
                        TBSYS_LOG(DEBUG, "server addr=%s fetch master update server info.", get_peer_ip(packet->get_request()));
                        return_code = rt_get_update_server_info(version, *in_buf, req, channel_id, thread_buff,false,true);
                        break;
                      }
					//add 20141017:e
                    case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
                      return_code = rt_get_update_server_info(version, *in_buf, req, channel_id, thread_buff,true);
                      break;
                    case OB_RENEW_LEASE_REQUEST:
                      return_code = rt_renew_lease(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SLAVE_QUIT:
                      return_code = rt_slave_quit(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_BOOT_STATE:
                      return_code = rt_get_boot_state(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_FORCE_CS_REPORT:
                      return_code = rt_force_cs_to_report(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_OBI_ROLE:
                      return_code = rt_get_obi_role(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_OBI_ROLE:
                      return_code = rt_set_obi_role(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_GET_LAST_FROZEN_VERSION:
                      return_code = rt_get_last_frozen_version(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_ADMIN:
                      return_code = rt_admin(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_CHECK_ROOTTABLE:
                      return_code = rs_check_root_table(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_DUMP_CS_TABLET_INFO:
                      return_code = rs_dump_cs_tablet_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add lbzhong [Paxos Cluster.Balance] 201607020:b
                    case OB_RS_DUMP_BALANCER_INFO:
                      return_code = rs_dump_balancer_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add bingo [Paxos sstable info to rs log] 20170614:b
                    case OB_DUMP_SSTABLE_INFO:
                      return_code = rs_dump_sstable_info(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_RS_STAT:
                      return_code = rt_stat(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CHANGE_LOG_LEVEL:
                      return_code = rt_change_log_level(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_MASTER_UPS_CONFIG:
                      return_code = rt_get_master_ups_config(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_UPS_HEARTBEAT_RESPONSE:
                      return_code = rt_ups_heartbeat_resp(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_UPS_REGISTER:
                      return_code = rt_ups_register(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_UPS_SLAVE_FAILURE:
                      return_code = rt_ups_slave_failure(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_UPS:
                      return_code = rt_get_ups(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_MASTER_UPS_CONFIG:
                      return_code = rt_set_master_ups_config(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_UPS_CONFIG:
                      return_code = rt_set_ups_config(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CHANGE_UPS_MASTER:
                      return_code = rt_change_ups_master(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CHANGE_TABLE_ID:
                      return_code = rt_change_table_id(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_CS_LIST:
                      return_code = rt_get_cs_list(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_ROW_CHECKSUM:
                      return_code = rt_get_row_checksum(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_MS_LIST:
                      return_code = rt_get_ms_list(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_PROXY_LIST:
                      return_code = rt_get_proxy_list(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CS_IMPORT_TABLETS:
                      return_code = rt_cs_import_tablets(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_SHUTDOWN_SERVERS:
                      return_code = rt_shutdown_cs(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_RESTART_SERVERS:
                      return_code = rt_restart_cs(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SQL_EXECUTE: /* server manager async task */
                      return_code = rt_execute_sql(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_HANDLE_TRIGGER_EVENT:
                      return_code = rt_handle_trigger_event(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add zhaoqiong [Schema Manager] 20150327:b
                    case OB_HANDLE_DDL_TRIGGER_EVENT:
                      return_code = rt_handle_ddl_trigger_event(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    case OB_GET_MASTER_OBI_RS:
                      return_code = rt_get_master_obi_rs(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_SET_CONFIG:
                      return_code = rt_set_config(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_CONFIG:
                      return_code = rt_get_config(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_ADMIN_START_IMPORT:
                      return_code = rt_start_import(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_IMPORT:
                      return_code = rt_import(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_ADMIN_START_KILL_IMPORT:
                      return_code = rt_start_kill_import(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_GET_IMPORT_STATUS:
                      return_code = rt_get_import_status(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_SET_IMPORT_STATUS:
                      return_code = rt_set_import_status(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_RS_NOTIFY_SWITCH_SCHEMA:
                      return_code = rt_notify_switch_schema(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_REPLICA_NUM:
                      return_code = rt_get_replica_num(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add bingo [Paxos table replica] 20170620:b
                    case OB_GET_TABLE_REPLICA_NUM:
                      return_code = rt_get_table_replica_num(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                    case OB_GET_CLUSTER_UPS:
                      return_code = rt_get_cluster_ups(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add bingo [Paxos rs_election] 20161009
                    case OB_MASTER_GET_ALL_PRIORITY:
                      return_code = rt_master_get_all_priority(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_GET_RS_PRIORITY:
                      return_code = rt_get_priority(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                     //add pangtianze [Paxos rs_election] 20170309
                    case OB_CHANGE_RS_PAXOS_NUMBER:
                      return_code = rt_rs_admin_change_rs_paxos_number(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_CHANGE_UPS_QUORUM_SCALE:
                      return_code = rt_rs_admin_change_ups_quorum_scale(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    //add bingo [Paxos rs_admin bugfix] 20170313:b
                    case OB_GET_RS_LEADER:
                      return_code = rt_get_rs_leader(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    //add:e
                    default:
                      TBSYS_LOG(ERROR, "unknow packet code %d in read queue", packet_code);
                      return_code = OB_ERROR;
                      break;
                  }
                }
                if (OB_SUCCESS != return_code)
                {
                  TBSYS_LOG(DEBUG, "call func error packet_code is %d return code is %d client %s",
                      packet_code, return_code, get_peer_ip(ob_packet->get_request()));
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
              }
            }
          }
        }
        else
        {
          //TODO get peer id
          TBSYS_LOG(ERROR, "packet deserialize error packet code is %d from server %s, ret=%d",
              packet_code, get_peer_ip(ob_packet->get_request()), return_code);
        }
      }

      worker_counter++;
      atomic_inc(&total_counter);
      if (0 == worker_counter % 500)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        int64_t receive_ts = ob_packet->get_receive_ts();
        TBSYS_LOG(INFO, "worker report, tid=%ld my_counter=%ld total_counter=%ld current_elapsed_us=%ld",
            syscall(__NR_gettid), worker_counter, total_counter, now - receive_ts);
      }
      return ret;//if return true packet will be deleted.
    }

    int ObRootWorker::schedule_after_restart_task(const int64_t delay,bool repeat /*=false*/)
    {
      TBSYS_LOG(INFO, "start to schedule restart task. delay=%ld, repeat=%s", delay, repeat ? "true" : "false");
      int ret = timer_.schedule(after_restart_task_, delay, repeat);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to schedule after_restart_task. ret=%d", ret);
      }
      return ret;
    }
	//modify peiouya [Get_masterups_and_timestamp] 20141017:b
    int ObRootWorker::rt_get_update_server_info(const int32_t version, ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, ObDataBuffer& out_buff,
        bool use_inner_port /* = false*/, bool is_get_ups_settimestamp /*= false*/)
	//modify 20141017:e
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      //next two lines only for exmaples, actually this func did not need this
      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      UNUSED(in_buff); // rt_get_update_server_info() no input params
      common::ObServer found_server;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        found_server = root_server_.get_update_server_info(use_inner_port);
      }
	  //add pangtianze [Paxos] 20170523:b
      if (OB_SUCCESS == ret && found_server.is_valid() && !is_get_ups_settimestamp && !use_inner_port
              && OB_SUCCESS != (ret = get_rpc_stub().ping_server(found_server, PING_RPC_TIMEOUT)))
      {
        ret = OB_SUCCESS;
        result_msg.result_code_ = OB_UPS_TIMEOUT;
        TBSYS_LOG(WARN, "maybe master ups has been down, ups=%s", found_server.to_cstring());
      }
      //add:e
      if (OB_SUCCESS == ret)
      {
        // if (!found_server.is_valid())
        // {
        //   result_msg.result_code_ = OB_ENTRY_NOT_EXIST;
        // }
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = found_server.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "found_server.serialize error");
        }
        else
        {
          TBSYS_LOG(DEBUG, "find master update server:server[%s]", found_server.to_cstring());
        }
        //add peiouya master_ups 20141017:b
        if (is_get_ups_settimestamp)
        {
          int64_t ups_master_set_time = root_server_.get_ups_set_time(found_server);
          serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(),ups_master_set_time);
        }
        //add 20141017:e
      }
      if (OB_SUCCESS == ret)
      {
	    //modify peiouya [Get_masterups_and_timestamp] 20141017:b
        if (!is_get_ups_settimestamp)
        {
          send_response(OB_GET_UPDATE_SERVER_INFO_RES, MY_VERSION, out_buff, req, channel_id);
        }
        else
        {
          send_response(OB_GET_UPDATE_SERVER_INFO_AND_TIMESTAMP_RES, MY_VERSION, out_buff, req, channel_id);
        }
		//modify 20141017:e
      }
      return ret;
    }

    int ObRootWorker::rt_scan(const int32_t version, ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObScanParam scan_param_in;
      ObScanner * scanner = GET_TSI_MULT(ObScanner, TSI_RS_SCANNER_1);
      if (scanner != NULL)
      {
        scanner->reset();
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get tsi ob_scanner failed");
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = scan_param_in.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "scan_param_in.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.find_root_table_range(scan_param_in, *scanner);
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = scanner->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scanner_out.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_SCAN_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(ROOTSERVER, INDEX_SUCCESS_SCAN_COUNT);
      }
      else
      {
        OB_STAT_INC(ROOTSERVER, INDEX_FAIL_SCAN_COUNT);
      }
      return ret;
    }


    int ObRootWorker::rt_sql_scan(const int32_t version,
        ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      UNUSED(channel_id);
      UNUSED(req);
      const int32_t RS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObNewScanner *new_scanner = GET_TSI_MULT(ObNewScanner, TSI_RS_NEW_SCANNER_1);
      sql::ObSqlScanParam *sql_scan_param_ptr = GET_TSI_MULT(sql::ObSqlScanParam,
          TSI_RS_SQL_SCAN_PARAM_1);
      if (version != RS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == sql_scan_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or new scanner, "
            "new_scanner=%p, sql_scan_param_ptr=%p", new_scanner, sql_scan_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        sql_scan_param_ptr->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = sql_scan_param_ptr->deserialize(
            in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_sql_scan input scan param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        new_scanner->set_range(*sql_scan_param_ptr->get_range());
        int64_t table_count = root_server_.get_table_count();
        OB_STAT_SET(ROOTSERVER, INDEX_ALL_TABLE_COUNT, table_count);
        int64_t tablet_count = 0;
        int64_t row_count = 0;
        int64_t data_size = 0;
        root_server_.get_tablet_info(tablet_count, row_count, data_size);
        OB_STAT_SET(ROOTSERVER, INDEX_ALL_TABLET_COUNT, tablet_count);
        OB_STAT_SET(ROOTSERVER, INDEX_ALL_ROW_COUNT, row_count);
        OB_STAT_SET(ROOTSERVER, INDEX_ALL_DATA_SIZE, data_size);
        rc.result_code_ = stat_manager_.get_scanner(*new_scanner);
        if(OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "open query service fail:err[%d]", rc.result_code_);
        }
      }

      out_buff.get_position() = 0;
      int serialize_ret = rc.serialize(out_buff.get_data(),
          out_buff.get_capacity(),
          out_buff.get_position());
      if (OB_SUCCESS != serialize_ret)
      {
        TBSYS_LOG(ERROR, "serialize result code object failed.");
      }

      // if scan return success , we can return scanner.
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        serialize_ret = new_scanner->serialize(out_buff.get_data(),
            out_buff.get_capacity(),
            out_buff.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize ObScanner failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        send_response(OB_SQL_SCAN_RESPONSE, RS_SCAN_VERSION, out_buff, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObRootWorker::rt_get(const int32_t version, ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      easy_addr_t addr = get_easy_addr(req);
      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObGetParam * get_param = GET_TSI_MULT(ObGetParam, TSI_RS_GET_PARAM_1);
      ObScanner * scanner = GET_TSI_MULT(ObScanner, TSI_RS_SCANNER_1);
      //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
      int32_t cluster_id = 0;
      //add:e
      if ((NULL == get_param) || (NULL == scanner))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get tsi ob_scanner or ob_get_param failed");
      }
      else
      {
        get_param->reset();
        scanner->reset();
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = get_param->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get_param_in.deserialize error");
        }
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position(), &cluster_id)))
        {
          TBSYS_LOG(WARN, "failed to decode cluster_id, err=%d", ret);
        }
        //add:e
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        if ((*get_param)[0]->table_id_ == OB_ALL_SERVER_STAT_TID)
        {
          result_msg.result_code_ = root_server_.find_monitor_table_key(*get_param, *scanner);
        }
        else if ((*get_param)[0]->table_id_ == OB_ALL_SERVER_SESSION_TID)
        {
          result_msg.result_code_ = root_server_.find_session_table_key(*get_param, *scanner);
        }
        else if ((*get_param)[0]->table_id_ == OB_ALL_STATEMENT_TID)
        {
          result_msg.result_code_ = root_server_.find_statement_table_key(*get_param, *scanner);
        }
        else
        {
          result_msg.result_code_ = root_server_.find_root_table_key(*get_param, *scanner
                                                                     //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                                                                     , cluster_id
                                                                     //add:e
                                                                     );
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = scanner->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scanner_out.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_GET_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(ROOTSERVER, INDEX_SUCCESS_GET_COUNT);

        ObStat* stat = NULL;
        OB_STAT_GET(ROOTSERVER, stat);
        if (NULL != stat)
        {
          int64_t get_count = stat->get_value(INDEX_SUCCESS_GET_COUNT);
          if (0 == get_count % 500)
          {
            TBSYS_LOG(INFO, "get request stat, count=%ld from=%s", get_count, inet_ntoa_r(addr));
          }
        }
      }
      else
      {
        OB_STAT_INC(ROOTSERVER, INDEX_FAIL_GET_COUNT);
      }
      return ret;
    }

    int ObRootWorker::rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      int64_t required_version = 0;
      bool get_only_core_tables = false; // @todo
      if (1 != version && 2 != version)
      {
        TBSYS_LOG(WARN, "invalid request version, version=%d", version);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      if (1 == version || 2 == version)
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position(), &required_version)))
        {
          TBSYS_LOG(WARN, "failed to decode version, err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret && 2 == version)
      {
        if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position(), &get_only_core_tables)))
        {
          TBSYS_LOG(WARN, "failed to decode version, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch core schema ? %s", get_only_core_tables ? "yes" : "no");
        }
      }

      ObSchemaManagerV2* schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
      if (schema == NULL)
      {
        TBSYS_LOG(ERROR, "error can not get mem for schema");
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        if (OB_SUCCESS != (ret = root_server_.get_schema(false, get_only_core_tables, *schema)))
        {
          TBSYS_LOG(WARN, "get schema failed, only_core_tables=%c, err=%d", get_only_core_tables ? 'Y': 'N', ret);
          result_msg.result_code_ = ret;
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(INFO, "get schema, only_core_tables=%c version=%ld from=%s",
              get_only_core_tables ? 'Y':'N', schema->get_version(),
              get_peer_ip(req));
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == result_msg.result_code_)
      {
        //mod zhaoqiong [Schema Manager] 20150327:b
//        ret = schema->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
//        if (ret != OB_SUCCESS)
//        {
//          TBSYS_LOG(ERROR, "schema.serialize error");
//        }
        if (OB_SUCCESS != (ret = schema->determine_serialize_pos()))
        {
          TBSYS_LOG(WARN, "fail to determine_serialize_pos, ret = %d", ret);
        }
        else if(OB_SUCCESS !=(ret = schema->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(ERROR, "fail to serialize schema, ret = %d", ret);
        }
        //mod:e
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_SCHEMA_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      if (NULL != schema)
      {
        OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema);
      }
      OB_STAT_INC(ROOTSERVER, INDEX_GET_SCHMEA_COUNT);
      return ret;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    int ObRootWorker::rt_fetch_schema_next(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      int64_t required_version = 0;
      bool get_only_core_tables = false;
      int64_t start_table_pos = -1;
      int64_t start_column_pos = -1;

      if (1 != version && 2 != version)
      {
        TBSYS_LOG(WARN, "invalid request version, version=%d", version);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      if (1 == version || 2 == version)
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position(), &required_version)))
        {
          TBSYS_LOG(WARN, "failed to decode version, err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                                                                         in_buff.get_position(), &start_table_pos)))
        {
          TBSYS_LOG(WARN, "failed to decode table id, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                                                                 in_buff.get_position(), &start_column_pos)))
        {
          TBSYS_LOG(WARN, "failed to decode table id, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "next table start:%ld, column start:%ld", start_table_pos, start_column_pos);
        }
      }

      ObSchemaManagerV2* schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
      if (schema == NULL)
      {
        TBSYS_LOG(ERROR, "error can not get mem for schema");
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        if (OB_SUCCESS != (ret = root_server_.get_schema(false, get_only_core_tables, *schema)))
        {
          TBSYS_LOG(WARN, "get schema failed, only_core_tables=%c, err=%d", get_only_core_tables ? 'Y': 'N', ret);
          result_msg.result_code_ = ret;
          ret = OB_SUCCESS;
        }
        else if (schema->get_version() != required_version)
        {
          result_msg.result_code_ = OB_VERSION_NOT_MATCH;
          TBSYS_LOG(ERROR, "required version[%ld] not match current schema version[%ld]", required_version, schema->get_version());
          ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != (ret=schema->determine_serialize_pos(start_table_pos,start_column_pos)))
        {
          result_msg.result_code_ = ret;
          TBSYS_LOG(ERROR, "failed to determine_serialize_pos, err=%d", ret);
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCCESS == ret)
      {
          ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
          if (ret != OB_SUCCESS)
          {
              TBSYS_LOG(ERROR, "result_msg.serialize error");
          }
      }

      if (OB_SUCCESS == result_msg.result_code_)
      {
        if(OB_SUCCESS !=(ret = schema->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_SCHEMA_NEXT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      if (NULL != schema)
      {
        OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema);
      }
      OB_STAT_INC(ROOTSERVER, INDEX_GET_SCHMEA_COUNT);
      return ret;
    }
    //add:e
	
    //add wenghaixing [secondary index cluster.p2]20150630
    int ObRootWorker::rt_get_index_process_info(const int32_t version, ObDataBuffer &in_buff, easy_request_t *req, const uint32_t channel_id, ObDataBuffer &out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 2;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int64_t status = -1;
      int64_t index_tid = -1;
      int64_t cluster_id = -1;
      //1. first check version
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid request version, version=%d", version);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
        ret = OB_ERROR_FUNC_VERSION;
      }

      //2. decode index_tid & cluster_id
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &index_tid)))
        {
          TBSYS_LOG(WARN, "failed to decode index_tid,ret[%d]", ret);
          result_msg.result_code_ = ret;
        }
        else if(OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &cluster_id)))
        {
          TBSYS_LOG(WARN, "failed to decode cluster_id,ret[%d]", ret);
          result_msg.result_code_ = ret;
        }
      }

      //3 fetch index process info
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = root_server_.fetch_index_stat(index_tid, cluster_id, status)))
        {
          result_msg.result_code_ = ret;
          TBSYS_LOG(WARN, "failed to fetch status, ret[%d]", ret);
        }
      }

      //4. encode result_msg & status
      if(OB_SUCCESS == result_msg.result_code_)
      {
        if(OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize porcess index info msg");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), status)))
        {
           TBSYS_LOG(WARN, "failed to serialize porcess index status msg");
        }
      }

      //5.return result
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = send_response(OB_FETCH_INDEX_STAT_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response status");
        }
      }
      //TBSYS_LOG(ERROR, "test::whx index[%ld], cluster[%ld], status[%ld]", index_tid, cluster_id, status);
      return ret;
    }
    //add e

	//add liuxiao [secondary index] 20150401
    int ObRootWorker::rt_get_old_tablet_column_checksum(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
        int ret = OB_SUCCESS;
        static const int MY_VERSION = 2;
        common::ObResultCode result_msg;
        result_msg.result_code_ = OB_SUCCESS;
        int64_t chekcsum_version = 0;
        int32_t cluster_id = -1;//add liumz, [paxos static index]20170626
        ObNewRange new_range;
        //ObString设置默认指针防止null
        ObString cchecksum;
        char tmp[OB_MAX_COL_CHECKSUM_STR_LEN];
        cchecksum.assign_buffer(tmp,OB_MAX_COL_CHECKSUM_STR_LEN);
        //设置查询主键范围
        ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        new_range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        new_range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

        //判断版本号
        if (MY_VERSION != version)
        {
          TBSYS_LOG(WARN, "invalid request version, version=%d", version);
          result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
        }

        //解析查询的tablet range
        if(2 == version && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = new_range.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
          {
            TBSYS_LOG(WARN, "failed to get new range");
          }
        }

        //解析列校验和版本号
        if(2 == version && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),in_buff.get_position(), &chekcsum_version)))
          {
            TBSYS_LOG(WARN, "failed to get version");
          }
        }

        //add liumz, [paxos static index]20170626:b
        //解析cluster_id
        if(2 == version && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),in_buff.get_position(), &cluster_id)))
          {
            TBSYS_LOG(WARN, "failed to get cluster id");
          }
        }
        //add:e

        //查询列校验和

        if(OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
        {
          //mod liumz, [paxos static index]20170626:b
          //if(OB_SUCCESS != ( ret = root_server_.get_cchecksum_info(new_range,chekcsum_version,cchecksum)))
          if(OB_SUCCESS != ( ret = root_server_.get_cchecksum_info(new_range,chekcsum_version,cluster_id,cchecksum)))
          //mod:e
          {
            TBSYS_LOG(WARN, "failed to get schema_service_ cchecksum");
            result_msg.result_code_ = ret;
            ret = OB_SUCCESS;
          }
          else
          {
            //保留填写日志信息
          }
        }

        //序列化结果
        if(OB_SUCCESS == result_msg.result_code_)
        {
          if(OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
          {
            TBSYS_LOG(WARN, "failed to serialize cchecksum msg");
          }
        }
        //序列化旧的列校验和
        if(2 == version && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = cchecksum.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
          {
            TBSYS_LOG(WARN, "failed to serialize cchecksum");
          }
        }
        //返回结果
        if(OB_SUCCESS == ret)
        {
          //TBSYS_LOG(ERROR, "TEST::liuxiao %s", cchecksum.ptr());
          if(OB_SUCCESS != (ret = send_response(OB_GET_OLD_TABLET_COLUMN_CHECKSUM_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
          {
            TBSYS_LOG(WARN, "failed to send response cchecksum");
          }
        }
        return ret;
    }
    //add e

    int ObRootWorker::rt_after_restart(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS == result_msg.result_code_ && OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.after_restart();
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to restart rootserver. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "restart rootserver ok~!");
        }
      }
      return ret;
    }
    int ObRootWorker::rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t schema_version = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        schema_version = root_server_.get_schema_version();
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), schema_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema version serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_SCHEMA_VERSION_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 2;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObServer server;
      ObTabletReportInfoList tablet_list;
      int64_t time_stamp = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = tablet_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "tablet_list.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &time_stamp);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "time_stamp.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.report_tablets(server, tablet_list, time_stamp);
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_REPORT_TABLETS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      return ret;
    }
    //add liumz, [secondary index static_index_build] 20150320:b
    int ObRootWorker::rt_report_histograms(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 2;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObServer server;
      ObTabletHistogramReportInfoList *tablet_list = NULL;
      int64_t time_stamp = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        tablet_list = OB_NEW(ObTabletHistogramReportInfoList,  ObModIds::OB_STATIC_INDEX_HISTOGRAM);
        ret = tablet_list->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "tablet_list deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &time_stamp);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "time_stamp.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        //add liumz, [secondary_index: check monitor phase]20160328:b
        if (MONITOR_LOCAL != index_monitor_.get_monitor_phase())
        {
          TBSYS_LOG(WARN, "report_histograms() refused! now monitor_phase:[%d]", index_monitor_.get_monitor_phase());
        }
        //add:e
        else if (OB_SUCCESS != (result_msg.result_code_ = root_server_.report_histograms(server, *tablet_list, time_stamp)))
        {
          TBSYS_LOG(WARN, "report_histograms() error.");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_REPORT_HISTOGRAMS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      OB_DELETE(ObTabletHistogramReportInfoList, ObModIds::OB_STATIC_INDEX_HISTOGRAM, tablet_list);

      return ret;
    }
    //add:e

    int ObRootWorker::rt_waiting_job_done(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      int64_t frozen_mem_version = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &frozen_mem_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "frozen_mem_version.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.waiting_job_done(server, frozen_mem_version);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_WAITING_JOB_DONE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;

    }

    int ObRootWorker::rt_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      common::ObTabletReportInfoList delete_list;
      ret = delete_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to serialize delete_list");
      }
      else if (OB_SUCCESS != (ret = root_server_.delete_tablets(delete_list)))
      {
        TBSYS_LOG(WARN, "failed to delete tablets, ret=%d", ret);
      }
      return ret;
    }

    int ObRootWorker::rt_cs_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      UNUSED(version);
      int ret = OB_SUCCESS;
      ObServer server;
      ObTabletReportInfoList tablet_list;
      common::ObResultCode result_msg;
      if (OB_SUCCESS == ret)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = tablet_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "tablet_list.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = root_server_.delete_replicas(false, server, tablet_list);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "delete all the replicas failed:server[%s], ret[%d]", server.to_cstring(), ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "delete all the replicas succ:server[%s]", server.to_cstring());
        }
      }
      result_msg.result_code_ = ret;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "result_msg.serialize error");
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_CS_DELETE_TABLETS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_register(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        TBSYS_LOG(WARN, "version:%d,MY_VERSION:%d",version,MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      bool is_merge_server = false;
      int32_t status = 0;
      int64_t server_version_length = 0;
      const char* server_version = NULL;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &is_merge_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "time_stamp.deserialize error");
        }
        else if (is_merge_server)
        {
          TBSYS_LOG(WARN,"receive merge server register using old interface, ms: [%s]", to_cstring(server));
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        server_version = serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &server_version_length);
        if (server_version == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "server_version.deserialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG,"receive server register,is_merge_server %d",is_merge_server ? 1 : 0);
        result_msg.result_code_ = root_server_.regist_chunk_server(server, server_version, status);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), status);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
                                         out_buff.get_position(), config_.cluster_id);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "serialize cluster_id error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_SERVER_REGISTER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_register_ms(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        TBSYS_LOG(WARN, "version:%d, MY_VERSION:%d",version,MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      int32_t sql_port = 0;
      int32_t status = 0;
      int64_t server_version_length = 0;
      const char* server_version = NULL;
      bool lms = false;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &sql_port);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "sql_port.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        server_version = serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &server_version_length);
        if (server_version == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "server_version.deserialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        //means new version mergeserver
        if (in_buff.get_remain() > 0)
        {
          ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &lms);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "lms.deserialize error");
          }
        }
        else  //old version mergeserver
        {
          if (sql_port == OB_FAKE_MS_PORT)
          {
            lms = true;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "receive merge server register");
        result_msg.result_code_ = root_server_.regist_merge_server(server, sql_port, lms, server_version);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), status);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
                                         out_buff.get_position(), config_.cluster_id);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "serialize cluster_id error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_SERVER_REGISTER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int CS_MIGRATE_OVER_VERSION = 3;
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      if (version != CS_MIGRATE_OVER_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObDataSourceDesc desc;
      int64_t occupy_size = 0;
      uint64_t crc_sum = 0;
      int64_t row_count = 0;
      int64_t row_checksum = 0;

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize reuslt msg, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = desc.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "desc.deserialize error, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &occupy_size);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode occupy size, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int64_t*>(&crc_sum));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode crc sum, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int64_t*>(&row_checksum));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode crc sum, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &row_count);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode row count, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.migrate_over(result_msg.result_code_,
            desc, occupy_size, crc_sum, row_checksum, row_count);
      }
      else
      {
        result_msg.result_code_ = ret;
      }

      int tmp_ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != tmp_ret)
      {
        TBSYS_LOG(ERROR, "result_msg.serialize error, ret=%d", tmp_ret);
        if (OB_SUCCESS == ret)
        {
          tmp_ret = ret;
        }
      }
      else
      {
        send_response(OB_MIGRATE_OVER_RESPONSE, CS_MIGRATE_OVER_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      int64_t capacity = 0;
      int64_t used = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &capacity);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "capacity.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &used);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "used.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.update_capacity_info(server, capacity, used);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_REPORT_CAPACITY_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;

    }

    // for chunk server
    int ObRootWorker::rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 2;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ObServer server;
      ObRole role = OB_CHUNKSERVER;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if ((OB_SUCCESS == ret) && (version == MY_VERSION))
      {
        ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int32_t *>(&role));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "decoe role error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.receive_hb(server, server.get_port(), false, role);
      }
      easy_request_wakeup(req);
      return ret;
    }
    // for merge server
    int ObRootWorker::rt_heartbeat_ms(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 3;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ObServer server;
      ObRole role = OB_MERGESERVER;
      int32_t sql_port = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "server.deserialize failed");
        }
      }
      // role
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int32_t *>(&role));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "decoe role failed:ret[%d]", ret);
        }
      }
      // sql port
      if ((OB_SUCCESS == ret) && (MY_VERSION == version))
      {
        ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &sql_port);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "decode sql port failed:ret[%d]", ret);
        }
      }
      bool is_listen_ms = false;
      if ((OB_SUCCESS == ret) && (MY_VERSION == version))
      {
        // means new version mergeserver
        if (in_buff.get_remain() > 0)
        {
          ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_listen_ms);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "is_listen_ms.deserialize error");
          }
        }
        else  //old version mergeserver
        {
          if (sql_port == OB_FAKE_MS_PORT)
          {
            is_listen_ms = false;
          }
        }
      }
      if ((OB_SUCCESS == ret) && (OB_SUCCESS == result_msg.result_code_))
      {
        result_msg.result_code_ = root_server_.receive_hb(server, sql_port, is_listen_ms, role);
      }
      easy_request_wakeup(req);
      return ret;
    }
//add wenghaixing [secondary index] 20141110

    int ObRootWorker::rt_create_index_command(const int32_t version, ObDataBuffer &in_buff, easy_request_t *req, const uint32_t channel_id, ObDataBuffer &out_buff)
    {
        int err=OB_SUCCESS;
        UNUSED(in_buff);
        UNUSED(req);
        UNUSED(version);
        UNUSED(channel_id);
        UNUSED(out_buff);
        err = index_monitor_.schedule();
        return err;

    }

//add e

    int ObRootWorker::rt_check_tablet_merged(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "function version not equeal. version=%d, my_version=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t tablet_version = 0;
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        err = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &tablet_version);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to decode tablet_version, err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "start to check tablet_version[%ld] whether merged.", tablet_version);
        }
      }
      bool is_merged = false;
      int64_t merged_result = 0;
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.check_tablet_version(tablet_version, 0, is_merged);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to check tablet version[%ld] whether safe merged!", tablet_version);
        }
        else if (true == is_merged)
        {
          merged_result = 1;
          TBSYS_LOG(INFO, "check_tablet[%ld] is already been merged.", tablet_version);
        }
        else
        {
          merged_result = 0;
          TBSYS_LOG(INFO, "check_tablet[%ld] has not been merged.", tablet_version);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), merged_result);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to encode merged_result. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_CHECK_TABLET_MERGED_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send response. err=%d", err);
        }
      }
      return err;
    }

    int ObRootWorker::rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      UNUSED(version);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_NOT_SUPPORTED;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      ret = send_response(OB_DUMP_CS_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      return ret;
    }

    int ObRootWorker::rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = stat_manager_.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "stat_manager_.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_STATS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;

    }

    ObRootLogManager* ObRootWorker::get_log_manager()
    {
      return &log_manager_;
    }

    ObRoleMgr* ObRootWorker::get_role_manager()
    {
      return &role_mgr_;
    }

    common::ThreadSpecificBuffer::Buffer* ObRootWorker::get_rpc_buffer() const
    {
      return my_thread_buffer.get_buffer();
    }

    int ObRootWorker::rt_ping(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result message serialize failed, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_PING_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      return ret;
    }

    int ObRootWorker::rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer rt_slave;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "slave deserialize failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = slave_mgr_.delete_server(rt_slave);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "ObSlaveMgr delete slave error, ret: %d", ret);
        }

        TBSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", to_cstring(rt_slave), ret);
      }

      if (ret == OB_SUCCESS)
      {
        common::ObResultCode result_msg;
        result_msg.result_code_ = ret;

        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_SLAVE_QUIT_RES, MY_VERSION, out_buff, req, channel_id);
      }

      return ret;
    }

    int ObRootWorker::rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int64_t frozen_version = 1;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer update_server;
      if (ret == OB_SUCCESS)
      {
        ret = update_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "deserialize failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &frozen_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "decode frozen version error, ret: %d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "update report a new froze version:ups[%s], version[%ld]",
              update_server.to_cstring(), frozen_version);
        }
      }

      if (ret == OB_SUCCESS)
      {
        root_server_.report_frozen_memtable(frozen_version, 0, false);
      }

      if (ret == OB_SUCCESS)
      {
        common::ObResultCode result_msg;
        result_msg.result_code_ = ret;

        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_RESULT, MY_VERSION, out_buff, req, channel_id);
      }
      if (ret == OB_SUCCESS)
      {
        int64_t rt_version = 0;
        root_server_.get_max_tablet_version(rt_version);
        root_server_.receive_new_frozen_version(rt_version, frozen_version, 0, false);
      }
      OB_STAT_INC(ROOTSERVER, INDEX_REPORT_VERSION_COUNT);
      return ret;
    }


    int ObRootWorker::rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      //del pangtianze [Paxos rs_election] 20150701:b
      //uint64_t new_log_file_id = 0;
      //del:e
      ObServer rt_slave;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize rt_slave failed, err=%d", ret);
        }
      }
      //del pangtianze [Paxos rs_election] 20150701:b
      /*
      if (ret == OB_SUCCESS)
      {
        ret = log_manager_.add_slave(rt_slave, new_log_file_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add_slave error, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "add slave, slave_addr=%s, new_log_file_id=%ld, ckpt_id=%lu, err=%d",
              to_cstring(rt_slave), new_log_file_id, log_manager_.get_check_point(), ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        if (config_.lease_on)
        {
          ObLease lease;
          ret = slave_mgr_.extend_lease(rt_slave, lease);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "failed to extends lease, ret=%d", ret);
          }
        }
      }
      ObFetchParam fetch_param;
      if (ret == OB_SUCCESS)
      {
        fetch_param.fetch_log_ = true;
        fetch_param.min_log_id_ = log_manager_.get_replay_point();
        fetch_param.max_log_id_ = new_log_file_id - 1;

        if (log_manager_.get_check_point() > 0)
        {
          TBSYS_LOG(INFO, "master has check point, tell slave fetch check point files, id: %ld",
              log_manager_.get_check_point());
          fetch_param.fetch_ckpt_ = true;
          fetch_param.ckpt_id_ = log_manager_.get_check_point();
          fetch_param.min_log_id_ = fetch_param.ckpt_id_ + 1;
        }
        else
        {
          fetch_param.fetch_ckpt_ = false;
          fetch_param.ckpt_id_ = 0;
        }
      }
      */
      //del:e
      //add by pangtianze [Paxos rs_election] 20160926:b
      //del pangtianze [Paxos rs_election] 20170221:b
      /*int64_t paxos_num = 0;
      int64_t quorum_scale = 0;
      if (OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &paxos_num)))
        {
          TBSYS_LOG(WARN, "decode paxos_num failed!ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &quorum_scale)))
        {
          TBSYS_LOG(WARN, "decode quorum_scale failed!ret[%d]", ret);
        }
      }
      if (OB_SUCCESS == ret && (paxos_num != config_.rs_paxos_number || quorum_scale != config_.ups_quorum_scale))
      {
        ret = OB_REGISTER_TO_LEADER_WITH_WRONG_PARAM;
        TBSYS_LOG(WARN, "receive slave register, but parameters are wrong(-u or -U), ret=%d", ret);
      }*/
      //del:e
      //add chujiajia[Paxos rs_election] 20151207:b
      ObServer servers[OB_MAX_RS_COUNT];
      int32_t server_count=0;
      root_server_.get_rs_node_mgr()->get_all_servers(servers, server_count); //mod pangtianze [Paxos rs_election] 20170713
      if(OB_SUCCESS == ret && (server_count < (int32_t)config_.rs_paxos_number || 1 == config_.rs_paxos_number
                               //add bingo [Paxos rs restart] 20170221:b
                               || is_exist(servers, rt_slave)//add:e
                               ))
      {
      //add:e
        ret = root_server_.add_rootserver_register_info(rt_slave);
        //add pangtianze [rs_election] 20170221:b
        ///if paxos_number is 1, auto set it 2 when new rs register [1->2]
        if (1 == config_.rs_paxos_number)
        {
           root_server_.set_paxos_num(2);
        }
        //add:e
        if (OB_SUCCESS == ret)
        {
          //update __all_server
          TBSYS_LOG(INFO, "after slave register, update inner table, rs_slave=%s", rt_slave.to_cstring());
          char server_version[OB_SERVER_VERSION_LENGTH] = "";
          get_package_and_svn(server_version, sizeof(server_version));
          root_server_.commit_task(SERVER_ONLINE, OB_ROOTSERVER, rt_slave, 0, server_version);
        }
      //add chujiajia [Paxos rs_election] 20151207:b
      }
      else
      {
        ret = OB_SERVER_COUNT_ENOUGH;
        TBSYS_LOG(WARN, "slave register failed, rs_slave=%s, alive_rootserver_count=%d, rs_paxos_number=%d, ret=%d",
        rt_slave.to_cstring(), server_count, (int32_t)(config_.rs_paxos_number), ret);
      }
      //add:e
      ObFetchParam fetch_param;
      //add:e
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "slave register failed:server[%s], ret[%d]", rt_slave.to_cstring(), ret);
      }
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }
      //add pangtianze [Paxos rs_election] 20170221:b
      if (OB_SUCCESS == ret)
      {
          int64_t paxos_num = (int64_t)config_.rs_paxos_number;
          int64_t quorum_scale = (int64_t)config_.ups_quorum_scale;
          if(OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), paxos_num)))
          {
            TBSYS_LOG(WARN, "encode paxos_num failed,err[%d]", ret);
          }
          else if(OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), quorum_scale)))
          {
            TBSYS_LOG(WARN, "encode quorum_scale failed,err[%d]", ret);
          }
      }
      //add:e
      if (OB_SUCCESS == ret)
      {
        send_response(OB_SLAVE_REG_RES, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::slave_register_(common::ObFetchParam& fetch_param)
    {
      int err = OB_SUCCESS;
      const ObServer& self_addr = self_addr_;

      err = OB_RESPONSE_TIME_OUT;
      for (int64_t i = 0; ObRoleMgr::STOP != role_mgr_.get_state()
          && OB_RESPONSE_TIME_OUT == err; i++)
      {
        // slave register

        //add pangtianze [Paxos rs_election] 20170221:b
        int64_t new_paxos_num = 0;
        int64_t new_quorum_scale = 0;
        //add:e
        //mod by pangtianze [Paxos rs_election] 20150926:b
        //err = rt_rpc_stub_.slave_register(rt_master_, self_addr_, fetch_param, config_.slave_register_timeout);
        err = rt_rpc_stub_.slave_register(rt_master_, self_addr_, fetch_param, new_paxos_num,
                                          new_quorum_scale ,config_.slave_register_timeout);
        //mod:e
        if (OB_RESPONSE_TIME_OUT == err)
        {
          TBSYS_LOG(INFO, "slave register timeout, i=%ld, err=%d", i, err);
        }
        //add pangtianze [Paxos rs_election] 20170221:b
        else if (OB_SERVER_COUNT_ENOUGH == err)
        {
          TBSYS_LOG(ERROR, "there are already enough rootservers online, can't register, err=%d", err);
        }
        else
        {
            ///set new param
            root_server_.set_paxos_num(new_paxos_num);
            root_server_.set_quorum_scale(new_quorum_scale);
        }
        //add:e
      }

      if (ObRoleMgr::STOP == role_mgr_.get_state())
      {
        TBSYS_LOG(INFO, "the slave is stopped manually.");
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
      }

      if (err == OB_SUCCESS)
      {
        int64_t renew_lease_timeout = 1000 * 1000L;
        check_thread_.set_renew_lease_timeout(renew_lease_timeout);
      }

      //mod pangtianze [Paxos rs_election] 20150701:b
      //TBSYS_LOG(INFO, "slave register, self=[%s], min_log_id=%ld, max_log_id=%ld, ckpt_id=%lu, err=%d",
      //    to_cstring(self_addr), fetch_param.min_log_id_, fetch_param.max_log_id_, fetch_param.ckpt_id_, err);
      TBSYS_LOG(INFO, "slave register, self=[%s],  err=%d", to_cstring(self_addr), err);
      //mod:e
      if (err == OB_SUCCESS)
      {
        is_registered_ = true;
      }

      return err;
    }

    int ObRootWorker::rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer rt_slave;
      ObLease lease;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize root slave, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = slave_mgr_.extend_lease(rt_slave, lease);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
        }
      }

      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;

      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_RENEW_LEASE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      return ret;
    }
    int ObRootWorker::rt_set_obi_role_to_slave(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      ObiRole role;
      if (OB_SUCCESS != (ret = role.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        ret = root_server_.set_obi_role(role);
      }
      result_msg.result_code_ = ret;
      // send response
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_SET_OBI_ROLE_TO_SLAVE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObLease lease;
      if (ret == OB_SUCCESS)
      {
        ret = lease.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize lease, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = check_thread_.renew_lease(lease);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
        }
      }

      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;

      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_GRANT_LEASE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buffer)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      uint64_t log_id;
      static bool is_first_log = true;
      // send response to master ASAP
      ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = send_response(OB_SEND_LOG_RES, MY_VERSION, out_buffer, req, channel_id);
      }

      if (ret == OB_SUCCESS)
      {
        if (is_first_log)
        {
          is_first_log = false;

          ObLogEntry log_ent;
          ret = log_ent.deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
          if (ret != OB_SUCCESS)
          {
            common::hex_dump(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_limit()), TBSYS_LOG_LEVEL_INFO);
            TBSYS_LOG(WARN, "ObLogEntry deserialize error, error code: %d, position: %ld, limit: %ld",
                ret, in_buffer.get_position(), in_buffer.get_limit());
            ret = OB_ERROR;
          }
          else
          {
            if (OB_LOG_SWITCH_LOG != log_ent.cmd_)
            {
              TBSYS_LOG(WARN, "the first log of slave should be switch_log, cmd_=%d", log_ent.cmd_);
              ret = OB_ERROR;
            }
            else
            {
              ObLogCursor start_cursor;
              ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(),
                  in_buffer.get_position(), (int64_t*)&log_id);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", ret);
              }
              else
              {
                start_cursor.file_id_ = log_id;
                start_cursor.log_id_ = log_ent.seq_ + 1;
                start_cursor.offset_ = 0;
                ret = log_manager_.start_log(start_cursor);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "start_log error, start_cursor=%s err=%d", to_cstring(start_cursor), ret);
                }
                else
                {
                  in_buffer.get_position() = in_buffer.get_limit();
                }
              }
            }
          }
        }
      } // end of first log

      if (OB_SUCCESS == ret && in_buffer.get_limit() - in_buffer.get_position() > 0)
      {
        ret = log_manager_.store_log(in_buffer.get_data() + in_buffer.get_position(),
            in_buffer.get_limit() - in_buffer.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObUpsLogMgr store_log error, err=%d", ret);
        }
      }

      return ret;
    }

int ObRootWorker::rt_get_boot_state(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      bool boot_ok = (root_server_.get_boot()->is_boot_ok());

      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_bool(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position(), boot_ok)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_BOOT_STATE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_get_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      ObiRole role = root_server_.get_obi_role();

      OB_STAT_INC(ROOTSERVER, INDEX_GET_OBI_ROLE_COUNT);
      ObStat* stat = NULL;
      OB_STAT_GET(ROOTSERVER, stat);
      if (NULL != stat)
      {
        int64_t count = stat->get_value(INDEX_GET_OBI_ROLE_COUNT);
        if (0 == count % 500)
        {
          TBSYS_LOG(INFO, "get obi role, count=%ld role=%s from=%s", count, role.get_role_str(),
              get_peer_ip(req));
        }
      }
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = role.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_OBI_ROLE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rt_force_cs_to_report(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      UNUSED(in_buff);
      TBSYS_LOG(INFO, "receive order to force cs_report.");
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      ret = root_server_.request_cs_report_tablet();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to request cs to report tablet. err=%d", ret);
      }
      result_msg.result_code_ = ret;
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "fail to serialize result_msg. ret=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_FORCE_CS_REPORT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      ObiRole role;
      if (OB_SUCCESS != (ret = role.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        ret = root_server_.set_obi_role(role);
        root_server_.commit_task(OBI_ROLE_CHANGE, OB_ROOTSERVER, rt_master_, rt_master_.get_port(), "rootserver",
                                 ObiRole::MASTER == role.get_role()? 1 : 2);
      }
      result_msg.result_code_ = ret;
      // send response
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_get_last_frozen_version(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      UNUSED(in_buff);
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position(), root_server_.get_last_frozen_version())))
      {
        TBSYS_LOG(ERROR, "serialize(last_frozen_version):fail.");
      }
      else
      {
        ret = send_response(OB_RS_GET_LAST_FROZEN_VERSION_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rs_check_root_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, my_version=%d", version, MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }

      common::ObServer cs;
      if (OB_SUCCESS == err)
      {
        err = cs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to deserialize cs addr. err = %d", err);
        }
      }
      bool is_integrity = false;
      if (OB_SUCCESS == err)
      {
        is_integrity = root_server_.check_root_table(cs);
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", err);
        }
      }
      if (OB_SUCCESS == result_msg.result_code_ && OB_SUCCESS == err)
      {
        err = serialization::encode_bool(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), is_integrity);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to encode tablet num to buff. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_CHECK_ROOTTABLE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", err);
        }
      }
      return err;
    }

    int ObRootWorker::rs_dump_cs_tablet_info(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, my_version=%d", version, MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }

      common::ObServer cs;
      if (OB_SUCCESS == err)
      {
        err = cs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to deserialize cs addr. err = %d", err);
        }
      }
      int64_t tablet_num = 0;
      if (OB_SUCCESS == err)
      {
        root_server_.dump_cs_tablet_info(cs, tablet_num);
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", err);
        }
      }
      if (OB_SUCCESS == result_msg.result_code_ && OB_SUCCESS == err)
      {
        err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(),
            out_buff.get_position(), tablet_num);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to encode tablet num to buff. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_DUMP_CS_TABLET_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", err);
        }
      }
      return err;
    }

    int ObRootWorker::rt_admin(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t admin_cmd = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &admin_cmd)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        result.result_code_ = do_admin_with_return(admin_cmd);
        TBSYS_LOG(INFO, "admin cmd=%d, err=%d", admin_cmd, result.result_code_);
        if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
        do_admin_without_return(admin_cmd);
      }
      return ret;
    }

    int ObRootWorker::do_admin_with_return(int admin_cmd)
    {
      int ret = OB_SUCCESS;
      switch(admin_cmd)
      {
        case OB_RS_ADMIN_ENABLE_BALANCE:
          config_.enable_balance = true;
          config_.print();
          break;
        case OB_RS_ADMIN_DISABLE_BALANCE:
          config_.enable_balance = false;
          config_.print();
          break;
        case OB_RS_ADMIN_ENABLE_REREPLICATION:
          config_.enable_rereplication = true;
          config_.print();
          break;
        case OB_RS_ADMIN_DISABLE_REREPLICATION:
          config_.enable_rereplication = false;
          config_.print();
          break;
        case OB_RS_ADMIN_INC_LOG_LEVEL:
          TBSYS_LOGGER._level++;
          break;
        case OB_RS_ADMIN_DEC_LOG_LEVEL:
          TBSYS_LOGGER._level--;
          break;
        case OB_RS_ADMIN_INIT_CLUSTER:
          {
            TBSYS_LOG(INFO, "start init cluster table");
            ObBootstrap bootstrap(root_server_);
            ret = bootstrap.init_all_cluster();
            break;
          }
        case OB_RS_ADMIN_BOOT_STRAP:
          TBSYS_LOG(INFO, "start to bootstrap");
          ret = root_server_.boot_strap();
          TBSYS_LOG(INFO, "bootstrap over");
          break;
        case OB_RS_ADMIN_CHECKPOINT:
          if (root_server_.is_master())
          {
            tbsys::CThreadGuard log_guard(get_log_manager()->get_log_sync_mutex());
            ret = log_manager_.do_check_point();
            TBSYS_LOG(INFO, "do checkpoint, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(WARN, "I'm not the master");
          }
          break;
        case OB_RS_ADMIN_CHECK_SCHEMA:
          ret = root_server_.check_schema();
          break;
        case OB_RS_ADMIN_CLEAN_ROOT_TABLE:
          ret = root_server_.clean_root_table();
          break;
        case OB_RS_ADMIN_CLEAN_ERROR_MSG:
          root_server_.clean_daily_merge_tablet_error();
          break;
        case OB_RS_ADMIN_RELOAD_CONFIG:
          ret = config_mgr_.reload_config();
          break;
        case OB_RS_ADMIN_SWITCH_SCHEMA:
          ret = root_server_.switch_ini_schema();
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "switch ini schema failed:ret[%d]", ret);
          }
          break;
        case OB_RS_ADMIN_BOOT_RECOVER:
        case OB_RS_ADMIN_DUMP_ROOT_TABLE:
        case OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS:
        case OB_RS_ADMIN_DUMP_SERVER_INFO:
        case OB_RS_ADMIN_DUMP_MIGRATE_INFO:
        case OB_RS_ADMIN_REFRESH_SCHEMA:
        //add bingo [Paxos set_boot_ok] 20170315:b
        case OB_RS_ADMIN_SET_BOOT_OK:
        //add:e
          break;
        //add liuxiao [secondary index] 20150320
        case OB_RS_ADMIN_CREATE_ALL_CCHECKSUM_INFO:
          root_server_.boot_strap_for_create_all_cchecksum_info();
          break;
        //add e
        //add pangtianze [Paxos rs_election] 20170228:b
        case OB_RS_ADMIN_REFRESH_RS_LIST:
          ret = root_server_.sync_refresh_rs_list();
          break;
        //add:e

        default:
          TBSYS_LOG(WARN, "unknown admin command, cmd=%d\n", admin_cmd);
          ret = OB_ENTRY_NOT_EXIST;
          break;
      }
      return ret;
    }


    int ObRootWorker::do_admin_without_return(int admin_cmd)
    {
      int64_t count = 0;
      int ret = OB_SUCCESS;
      switch(admin_cmd)
      {
        case OB_RS_ADMIN_BOOT_RECOVER:
          TBSYS_LOG(INFO, "start to boot recover");
          ret = root_server_.boot_recover();
          break;
        case OB_RS_ADMIN_DUMP_ROOT_TABLE:
          root_server_.dump_root_table();
          break;
        case OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS:
          root_server_.dump_unusual_tablets();
          break;
        case OB_RS_ADMIN_DUMP_SERVER_INFO:
          root_server_.print_alive_server();
          break;
        case OB_RS_ADMIN_DUMP_MIGRATE_INFO:
          root_server_.dump_migrate_info();
          break;
        case OB_RS_ADMIN_REFRESH_SCHEMA:
          root_server_.renew_user_schema(count);
          break;
        //add bingo [Paxos set_boot_ok] 20170315:b
        case OB_RS_ADMIN_SET_BOOT_OK:
          root_server_.set_boot_ok();
          break;
        //add:e
        default:
          TBSYS_LOG(DEBUG, "not supported admin cmd:cmd[%d]", admin_cmd);
          break;
      }
      return ret;
    }

    int ObRootWorker::rt_change_log_level(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (TBSYS_LOG_LEVEL_ERROR <= log_level
            && TBSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          TBSYS_LOGGER._level = log_level;
          TBSYS_LOG(INFO, "change log level, level=%d", log_level);
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
      }
      return ret;
    }

    int ObRootWorker::rt_stat(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t stat_key = -1;

      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &stat_key)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        result.message_.assign_ptr(const_cast<char*>("hello world"), sizeof("hello world"));
        if (OB_SUCCESS != (ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(),
                out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "get stat, stat_key=%d", stat_key);
          do_stat(stat_key, out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
          ret = send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
      }
      return ret;
    }

    using oceanbase::common::databuff_printf;

    int ObRootWorker::do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "do_stat start, stat_key=%d buf=%p buf_len=%ld pos=%ld",
          stat_key, buf, buf_len, pos);
      switch(stat_key)
      {
        case OB_RS_STAT_RS_SLAVE_NUM:
          databuff_printf(buf, buf_len, pos, "slave_num: %d", slave_mgr_.get_num());
          ret = OB_SUCCESS;
          break;
        case OB_RS_STAT_RS_SLAVE:
          slave_mgr_.print(buf, buf_len, pos);
          break;
        default:
          ret = root_server_.do_stat(stat_key, buf, buf_len, pos);
          break;
      }
      // skip the trailing '\0'
      pos++;
      TBSYS_LOG(DEBUG, "do_stat finish, stat_key=%d buf=%p buf_len=%ld pos=%ld",
          stat_key, buf, buf_len, pos);
      return ret;
    }
    int ObRootWorker::rt_get_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int my_version = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      int32_t master_master_ups_read_percent = 0;
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //int32_t slave_master_ups_read_percent = 0;
      //del:
      result_msg.result_code_ = root_server_.get_master_ups_config(master_master_ups_read_percent
                                                                   //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                                                   //, slave_master_ups_read_percent
                                                                   //del:e
                                                                   );
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == result_msg.result_code_
          && OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position(), master_master_ups_read_percent)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      /*
      else if (OB_SUCCESS == result_msg.result_code_
          && OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position(), slave_master_ups_read_percent)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      */
      //del:e
      else
      {
        ret = send_response(OB_GET_MASTER_UPS_CONFIG_RESPONSE, my_version, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_ups_heartbeat_resp(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      ObMsgUpsHeartbeatResp msg;
      if (msg.MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = msg.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else
      {
        ObUpsStatus ups_status = UPS_STAT_OFFLINE;
        if (msg.status_ == msg.SYNC)
        {
          ups_status = UPS_STAT_SYNC;
        }
        else if (msg.status_ == msg.NOTSYNC)
        {
          ups_status = UPS_STAT_NOTSYNC;
        }
        else
        {
          TBSYS_LOG(ERROR, "fatal error, stat=%d", msg.status_);
        }
        //mod chujiajia [Paxos rs_election] 20151229:b
        //ret = root_server_.receive_ups_heartbeat_resp(msg.addr_, ups_status, msg.obi_role_);
        ret = root_server_.receive_ups_heartbeat_resp(msg.addr_, ups_status, msg.obi_role_, msg.quorum_scale_);
        //mod:e
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }

    int ObRootWorker::rt_get_ups(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      common::ObUpsList ups_list;
      static const int MY_VERSION = 1;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = root_server_.get_ups_list(ups_list);
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == res.result_code_
          && OB_SUCCESS != (ret = ups_list.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_UPS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_ups_register(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ObMsgUpsRegister msg;
      if (msg.MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = msg.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else
      {
        //mod wangjiahao [Paxos]
        ret = root_server_.register_ups(msg.addr_, msg.inner_port_, msg.log_seq_num_, msg.lease_, msg.server_version_);
      }
      // send response
      ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        if (OB_SUCCESS == res.result_code_)
        {
          if (OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(),
                  out_buff.get_capacity(), out_buff.get_position(), config_.ups_renew_reserved_time)))
          {
            TBSYS_LOG(WARN, "failed to serialize");
          }
          else if (OB_SUCCESS != (ret = serialization::encode_vi64(out_buff.get_data(),
                                                                   out_buff.get_capacity(),
                                                                   out_buff.get_position(),
                                                                   config_.cluster_id)))
          {
            TBSYS_LOG(WARN, "failed to serialize cluster id");
          }
        }
        if (OB_SUCCESS == ret)
        {
          ret = send_response(OB_RS_UPS_REGISTER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
      }
      return ret;
    }
    int ObRootWorker::rt_set_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      int32_t read_master_master_ups_percentage = -1;
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //int32_t read_slave_master_ups_percentage = -1;
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      int32_t is_strong_consistent = 0;
      //add:e
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &read_master_master_ups_percentage)))
      {
        TBSYS_LOG(ERROR, "failed to serialize read_percentage, err=%d", ret);
      }
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      /*
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &read_slave_master_ups_percentage)))
      {
        TBSYS_LOG(ERROR, "failed to serialize read_percentage, err=%d", ret);
      }
      */
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &is_strong_consistent)))
      {
        TBSYS_LOG(ERROR, "failed to serialize read consistency, err=%d", ret);
      }
      //add:e
      else
      {
        common::ObResultCode res;
        res.result_code_ = root_server_.set_ups_config(read_master_master_ups_percentage
                                                       //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                                       //, read_slave_master_ups_percentage
                                                       //del:e
                                                       //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                                                       ,is_strong_consistent
                                                       //add:e
                                                       );
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = send_response(OB_SET_MASTER_UPS_CONFIG_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
      }
      return ret;
    }
    int ObRootWorker::rt_set_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObServer ups_addr;
      int32_t ms_read_percentage = -1;
      int32_t cs_read_percentage = -1;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = ups_addr.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to serialize ups_addr, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &ms_read_percentage)))
      {
        TBSYS_LOG(ERROR, "failed to serialize read_percentage, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &cs_read_percentage)))
      {
        TBSYS_LOG(ERROR, "failed to serialize read_percentage, err=%d", ret);
      }
      else
      {
        common::ObResultCode res;
        res.result_code_ = root_server_.set_ups_config(ups_addr, ms_read_percentage, cs_read_percentage);
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = send_response(OB_SET_UPS_CONFIG_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        }
      }
      return ret;
    }

    int ObRootWorker::rt_ups_slave_failure(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      ObMsgUpsSlaveFailure msg;
      if (msg.MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = msg.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else
      {
        ret = root_server_.ups_slave_failure(msg.my_addr_, msg.slave_addr_);
      }
      // send response
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_UPS_SLAVE_FAILURE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_change_ups_master(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObServer ups_addr;
      int32_t force = 0;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = ups_addr.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &force)))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else
      {
        bool did_force = (0 != force);
        ret = root_server_.change_ups_master(ups_addr, did_force);
      }
      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_CHANGE_UPS_MASTER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_get_cs_list(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = root_server_.serialize_cs_list(
              out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_CS_LIST_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_get_row_checksum(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int64_t tablet_version = 0;
      uint64_t table_id = 0;
      ObRowChecksum row_checksum;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &tablet_version)))
      {
        TBSYS_LOG(WARN, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = root_server_.get_row_checksum(tablet_version, table_id, row_checksum)) && OB_ENTRY_NOT_EXIST != ret)
      {
        TBSYS_LOG(WARN, "failed to get rowchecksum, tablet_version:%ld table_id:%lu ret:%d", tablet_version, table_id, ret);
      }

      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row_checksum.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_ROW_CHECKSUM, MY_VERSION, out_buff, req, channel_id);
      }

      return ret;
    }

    int ObRootWorker::rt_get_ms_list(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
      /**
       * if cluster = 0, then get all ms
       * if cluster > 0, then get ms from one cluster
       */
      int32_t cluster_id = 0;
      //add:e
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
              in_buff.get_position(), &cluster_id)))
      {
        TBSYS_LOG(WARN, "failed to decode cluster_id, err=%d", ret);
      }
      //add:e
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = root_server_.serialize_ms_list(
              out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()
              //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
              , cluster_id
              //add:e
              )))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
      else if(OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
                                                              out_buff.get_position(), root_server_.get_master_cluster_id())))
      {
        TBSYS_LOG(WARN, "failed to encode cluster_id, ret=%d", ret);
      }
      //add:e
      else
      {
        ret = send_response(OB_GET_MS_LIST_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_get_proxy_list(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == res.result_code_ && OB_SUCCESS != (ret = root_server_.serialize_proxy_list(
              out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_PROXY_LIST_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_cs_import_tablets(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;
      int64_t tablet_version = 0;
      static const int MY_VERSION = 1;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), (int64_t*)&table_id))
      {
        TBSYS_LOG(ERROR, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &tablet_version))
      {
        TBSYS_LOG(ERROR, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = root_server_.cs_import_tablets(table_id, tablet_version);
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_CS_IMPORT_TABLETS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_restart_cs(const int32_t version, ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int32_t count = 0;
      int32_t cancel = 0;
      ObArray<ObServer> servers;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }

      bool is_restart_all = false;
      if(OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &is_restart_all);
      }

      if(OB_SUCCESS == ret && !is_restart_all)
      {
        if (OB_SUCCESS != serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
              in_buff.get_position(), &count))
        {
          TBSYS_LOG(ERROR, "failed to deserialize");
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          ObServer server;
          for (int i = 0; i < count; ++i)
          {
            if (OB_SUCCESS != (ret = server.deserialize(in_buff.get_data(),
                    in_buff.get_capacity(), in_buff.get_position())))
            {
              TBSYS_LOG(WARN, "deserialize error, i=%d", i);
              break;
            }
            else if (OB_SUCCESS != (ret = servers.push_back(server)))
            {
              TBSYS_LOG(WARN, "push error, err=%d", ret);
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position(), &cancel)))
        {
          TBSYS_LOG(WARN, "deserialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        if(!is_restart_all)
        {
          if(cancel)
          {
            ret = root_server_.cancel_shutdown_cs(servers, RESTART);
          }
          else
          {
            ret = root_server_.shutdown_cs(servers, RESTART);
          }
        }
        else
        {
          if(cancel)
          {
            root_server_.cancel_restart_all_cs();
          }
          else
          {
            root_server_.restart_all_cs();
          }
        }
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_SHUTDOWN_SERVERS, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_shutdown_cs(const int32_t version, ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int32_t count = 0;
      ObArray<ObServer> servers;
      int32_t cancel = 0;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &count))
      {
        TBSYS_LOG(ERROR, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObServer server;
        for (int i = 0; i < count; ++i)
        {
          if (OB_SUCCESS != (ret = server.deserialize(in_buff.get_data(),
                  in_buff.get_capacity(), in_buff.get_position())))
          {
            TBSYS_LOG(WARN, "deserialize error, i=%d", i);
            break;
          }
          else if (OB_SUCCESS != (ret = servers.push_back(server)))
          {
            TBSYS_LOG(WARN, "push error, err=%d", ret);
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
                  in_buff.get_capacity(), in_buff.get_position(), &cancel)))
          {
            TBSYS_LOG(WARN, "deserialize error");
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (cancel)
        {
          ret = root_server_.cancel_shutdown_cs(servers, SHUTDOWN);
        }
        else
        {
          ret = root_server_.shutdown_cs(servers, SHUTDOWN);
        }
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_SHUTDOWN_SERVERS, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    common::ObClientManager* ObRootWorker::get_client_manager()
    {
      return &client_manager;
    }

    int64_t ObRootWorker::get_network_timeout()
    {
      return config_.network_timeout;
    }

    common::ObServer ObRootWorker::get_rs_master()
    {
      //add pangtianze [Paxos rs_election] 20150629:b
      common::ObSpinLockGuard guard(rt_master_lock_);
      //add:e
      return rt_master_;
    }

    common::ThreadSpecificBuffer* ObRootWorker::get_thread_buffer()
    {
      return &my_thread_buffer;
    }
    template <class Queue>
      int ObRootWorker::submit_async_task_(const PacketCode pcode, Queue& qthread, int32_t task_queue_size,
          const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req,
          const uint32_t channel_id, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        ObPacket *ob_packet = NULL;
        if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
        {
          TBSYS_LOG(WARN, "create packet fail");
          ret = OB_ERROR;
        }
        else
        {
          ob_packet->set_packet_code(pcode);
          ob_packet->set_api_version(version);
          ob_packet->set_request(req);
          ob_packet->set_channel_id(channel_id);
          ob_packet->set_target_id(OB_SELF_FLAG);
          ob_packet->set_receive_ts(tbsys::CTimeUtil::getTime());
          ob_packet->set_source_timeout(timeout);
          ob_packet->set_data(in_buff);
          if (OB_SUCCESS != (ret = ob_packet->serialize()))
          {
            TBSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
          }
          else if (!qthread.push(ob_packet, task_queue_size, false))
          {
            TBSYS_LOG(WARN, "add create index task to thread queue fail task_queue_size=%d, pcode=%d",
                task_queue_size, pcode);
            ret = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(DEBUG, "submit async task succ pcode=%d", pcode);
          }
          if (OB_SUCCESS != ret)
          {
            packet_factory_.destroyPacket(ob_packet);
            ob_packet = NULL;
          }
        }
        return ret;
      }

    template <class Queue>
      int ObRootWorker::submit_async_task_(const PacketCode pcode, Queue &qthread,
          int32_t task_queue_size, const ObDataBuffer *data_buffer, const common::ObPacket *packet)
      {
        int ret = OB_SUCCESS;
        ObPacket *ob_packet = NULL;
        if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
        {
          TBSYS_LOG(WARN, "create packet fail");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != packet)
          {
            ob_packet->set_api_version(packet->get_api_version());
            ob_packet->set_request(packet->get_request());
            ob_packet->set_channel_id(packet->get_channel_id());
            ob_packet->set_source_timeout(packet->get_source_timeout());
          }

          ob_packet->set_packet_code(pcode);
          ob_packet->set_target_id(OB_SELF_FLAG);
          ob_packet->set_receive_ts(tbsys::CTimeUtil::getTime());
          ob_packet->set_source_timeout(INT32_MAX);
          if (NULL != data_buffer)
          {
            ob_packet->set_data(*data_buffer);
          }
          if (OB_SUCCESS != (ret = ob_packet->serialize()))
          {
            TBSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
          }
          else if (!qthread.push(ob_packet, task_queue_size, false))
          {
            TBSYS_LOG(WARN, "add create index task to thread queue fail task_queue_size=%d, pcode=%d",
                task_queue_size, pcode);
            ret = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "submit async task succ pcode=%d", pcode);
          }
          if (OB_SUCCESS != ret)
          {
            packet_factory_.destroyPacket(ob_packet);
            ob_packet = NULL;
          }
        }
        return ret;
      }
    int ObRootWorker::rt_split_tablet(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      UNUSED(in_buff);
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "function version not equeal. version=%d, my_version=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t frozen_version = 1;
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        err = rt_rpc_stub_.get_last_frozen_version(root_server_.get_update_server_info(false),
            config_.network_timeout, frozen_version);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to get frozen_version. err=%d", err);
        }
      }
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        int64_t rt_version = 0;
        root_server_.get_max_tablet_version(rt_version);
        result_msg.result_code_ = root_server_.receive_new_frozen_version(
            rt_version, frozen_version, 0, false);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to split tablet. err=%d", result_msg.result_code_);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_SPLIT_TABLET_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send response. err=%d", err);
        }
      }
      return err;
    }

    int ObRootWorker::rt_create_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      bool if_not_exists = false;
      TableSchema tschema;
      ObString user_name;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (role != common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "ddl operation not allowed in slave cluster");
      }
      else
      {
        if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), &if_not_exists)))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = tschema.deserialize(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = root_server_.create_table(if_not_exists, tschema)))
        {
          TBSYS_LOG(WARN, "failed to create table, err=%d", ret);
        }
        if (OB_SUCCESS != ret)
        {
          res.message_ = ob_get_err_msg();
          TBSYS_LOG(WARN, "create table err=%.*s", res.message_.length(), res.message_.ptr());
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_CREATE_TABLE_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "send response for creating table, if_not_exists=%c table_name=%s ret=%d",
              if_not_exists?'Y':'N', tschema.table_name_, res.result_code_);
        }
      }
      return ret;
    }

    int ObRootWorker::rt_alter_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      AlterTableSchema tschema;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (role != common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "ddl operation not allowed in slave cluster");
      }
      else
      {
        if (OB_SUCCESS != (ret = tschema.deserialize(in_buff.get_data(), in_buff.get_capacity(),
                in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = root_server_.alter_table(tschema)))
        {
          TBSYS_LOG(WARN, "failed to alter table, err=%d", ret);
        }
        if (OB_SUCCESS != ret)
        {
          res.message_ = ob_get_err_msg();
          TBSYS_LOG(WARN, "create table err=%.*s", res.message_.length(), res.message_.ptr());
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(),
                out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_ALTER_TABLE_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "send response for alter table, table_name=%s ret=%d", tschema.table_name_, res.result_code_);
        }
      }
      return ret;
    }

    //add wenghaixing [secondary index drop index]20141223
    int ObRootWorker::rt_drop_index(const int32_t version, ObDataBuffer &in_buff, easy_request_t *req, const uint32_t channel_id, ObDataBuffer &out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (role != common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "ddl operation not allowed in slave cluster");
      }
      else
      {
        bool if_exists = false;
        ObStrings index;
        if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(),
                  in_buff.get_capacity(), in_buff.get_position(), &if_exists)))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = index.deserialize(in_buff.get_data(),
                  in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = root_server_.drop_index(index)))
        {
          TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                  out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_DROP_TABLE_RESPONSE, MY_VERSION,
                  out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
        }
      }
      return ret;
    }
    //add e

    int ObRootWorker::rt_drop_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      //add wenghaixing[secondary index drop table_with_index] 20150122
      ObStrings table_and_index;
      //add e
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (role != common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "ddl operation not allowed in slave cluster");
      }
      else
      {
        bool if_exists = false;
        ObStrings tables;
        if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), &if_exists)))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = tables.deserialize(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        //add wenghaixing [secondary index drop table_with_index]20150122
        else
        {
          const ObSchemaManagerV2* schema_mgr = root_server_.get_local_schema();
          ObString table_name;
          //ObString index_name;
          //char index_str[OB_MAX_TABLE_NAME_LENGTH];
          if(NULL == schema_mgr)
          {
            TBSYS_LOG(WARN,"get schema failed, NULL pointer");
            ret = OB_ERR_INVALID_SCHEMA;
          }
          else
          {
            //mod liumz, [bugfix, check ret in outer loop]
            //for(int64_t i = 0;i < tables.count();i++)
            for(int64_t i = 0;i < tables.count()&&OB_LIKELY(OB_SUCCESS == ret);i++)
            //mod:e
            {
              table_name.reset();
              if(OB_SUCCESS != (ret = tables.get_string(i,table_name)))
              {
                TBSYS_LOG(WARN,"get table name failed!");
                ret = OB_ERR_ILLEGAL_NAME;
                break;
              }
              else if(OB_SUCCESS != (ret = table_and_index.add_string(table_name)))
              {
                TBSYS_LOG(WARN,"add table name into strings failed!");
                ret = OB_ERROR;
                break;
              }
              else
              {
                const ObTableSchema* schema = schema_mgr->get_table_schema(table_name);
                if(NULL == schema && !if_exists)
                {
                  TBSYS_LOG(WARN,"get schema failed!");
                  ret = OB_ERR_INVALID_SCHEMA;
                  break;
                }
                else if(NULL != schema)
                {
                  uint64_t data_tid = OB_INVALID_ID;
                  IndexList il;
                  data_tid = schema->get_table_id();
                  if(OB_SUCCESS != (ret = schema_mgr->get_index_list_for_drop(data_tid,il)))
                  {
                    TBSYS_LOG(ERROR,"generate drop index list error");
                    break;
                  }
                  else
                  {
                    //TBSYS_LOG(ERROR,"test::whx index list count[%ld]",il.get_count());
                    for(int64_t m = 0;m<il.get_count();m++)
                    {
                    //memset(index_str,0,OB_MAX_TABLE_NAME_LENGTH);
                      uint64_t index_tid = OB_INVALID_ID;
                      il.get_idx_id(m,index_tid);
                      const ObTableSchema* index_schema = schema_mgr->get_table_schema(index_tid);
                      if(NULL == index_schema)
                      {
                        TBSYS_LOG(WARN,"get schema failed for index %ld",index_tid);
                        ret = OB_ERR_INVALID_SCHEMA;
                        break;
                      }
                      else
                      {
                        int32_t len = static_cast<int32_t>(strlen(index_schema->get_table_name()));
                        ObString val(len,len,index_schema->get_table_name());
                        if(OB_SUCCESS != (ret = (table_and_index.add_string(val))))
                        {
                          TBSYS_LOG(WARN,"add index failed [%.*s]",val.length(),val.ptr());
                          ret = OB_ERROR;
                          break;
                        }
                      }
                    }
                  }

                }
              }
            }
          }
        }
        //add e
        //modify wenghaixing [secondary index drop table_with_index]20150122
        /*else if (OB_SUCCESS != (ret = root_server_.drop_tables(if_exists, tables)))

        {
          TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
        */
        if(OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = root_server_.drop_tables(if_exists, table_and_index)))
          {
            TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
          }
          res.result_code_ = ret;
          ret = OB_SUCCESS;
        }

        //modify e
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_DROP_TABLE_RESPONSE, MY_VERSION,
                out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
        }
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int ObRootWorker::rt_truncate_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (role != common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "ddl operation not allowed in slave cluster");
      }
      else
      {
        bool if_exists = false;
        ObStrings tables;
        ObString user;
        ObString comment;
        if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), &if_exists)))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = tables.deserialize(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = user.deserialize(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = comment.deserialize(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = root_server_.truncate_tables(if_exists, tables, user, comment)))
        {
          TBSYS_LOG(WARN, "failed to truncate table, err=%d", ret);
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_TRUNCATE_TABLE_RESPONSE, MY_VERSION,
                out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
        }
      }
      return ret;
    }
    //add:e

    //add liumz, [bugfix: cchecksum too large]20161109:b
    int ObRootWorker::clean_checksum_info(const int64_t max_draution_of_version,const int64_t current_version)
    {
      int ret = OB_SUCCESS;
      static const int64_t timeout = 30L * 1000L * 1000L;
      const ObChunkServerManager *server_mgr = NULL;
      ObString sqlstr;
      char sql_buf[OB_MAX_SQL_LENGTH];
      int n = 0;
      n = snprintf(sql_buf, sizeof(sql_buf),
        "delete /*+UD_MULTI_BATCH*/ from %s where version < %ld", OB_ALL_CCHECKSUM_INFO_TABLE_NAME, current_version - max_draution_of_version);
      if (n<0 || n >= OB_MAX_SQL_LENGTH)
      {
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (NULL == (server_mgr = &root_server_.get_server_manager()))
      {
        TBSYS_LOG(WARN, "server manager is not inited right now");
        ret = OB_NOT_INIT;
      }
      else
      {
        sqlstr.assign_ptr(sql_buf, n);
        ObChunkServerManager::const_iterator it = server_mgr->get_serving_ms();
        ObServer ms(it->server_);
        ms.set_port(it->port_ms_);
        if (it == server_mgr->end())
        {
          TBSYS_LOG(WARN, "no serving mergeserver right now");
          ret = OB_NOT_INIT;
        }
        else if (OB_SUCCESS != (ret = rt_rpc_stub_.execute_sql(ms, sqlstr, timeout)))
        {
          TBSYS_LOG(WARN, "execute sql fail, ret:[%d], sql:[%.*s]", ret, sqlstr.length(), sqlstr.ptr());
        }
        else
        {
          TBSYS_LOG(INFO, "execulte sql successfully! sql:[%.*s]", sqlstr.length(), sqlstr.ptr());
        }
      }
      return ret;
    }
    //add:e

    int ObRootWorker::rt_execute_sql(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(req);              /* NULL */
      UNUSED(channel_id);
      UNUSED(out_buff);
      static const int64_t timeout = 1000L * 1000L;
      ObString sqlstr;
      ObString table_name;
      int64_t try_times = 1;
      const ObChunkServerManager *server_mgr = NULL;
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = sqlstr.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize sql string fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &try_times)))
      {
        TBSYS_LOG(WARN, "deserializ try times error, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "deserialize table name fail, ret: [%d]", ret);
      }
      else if (NULL == (server_mgr = &root_server_.get_server_manager()))
      {
        TBSYS_LOG(WARN, "server manager is not inited right now");
        ret = OB_NOT_INIT;
      }
      else
      {
        bool table_exist = false;
        if (OB_SUCCESS != (ret = root_server_.check_table_exist(table_name, table_exist)))
        {
          TBSYS_LOG(WARN, "Table existence check fail, ret: [%d]", ret);
        }
        else if (false == table_exist)
        {
          TBSYS_LOG(DEBUG, "query table '%.*s' not exist", table_name.length(), table_name.ptr());
          ret = OB_NOT_INIT;
        }
        if (OB_SUCCESS == ret)
        {
          ObChunkServerManager::const_iterator it = server_mgr->get_serving_ms();
          ObServer ms(it->server_);
          ms.set_port(it->port_ms_);
          if (it == server_mgr->end())
          {
            TBSYS_LOG(WARN, "no serving mergeserver right now");
            ret = OB_NOT_INIT;
          }
          else
          {
            ret = rt_rpc_stub_.execute_sql(ms, sqlstr, timeout);
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "execute sql fail, ret:[%d], sql:[%.*s]", ret, sqlstr.length(), sqlstr.ptr());
      }
      else
      {
        TBSYS_LOG(INFO, "execulte sql successfully! sql:[%.*s]", sqlstr.length(), sqlstr.ptr());
      }
      return ret;
    }

    int ObRootWorker::rt_handle_trigger_event(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);

      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      res.result_code_ = ret;
      common::ObTriggerMsg msg;
      if (OB_SUCCESS != (ret = msg.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize msg");
      }
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      TBSYS_LOG(INFO, "I got a trigger message. value=%.*s,%ld,%ld. my role=%d",
          msg.src.length(), msg.src.ptr(), msg.type, msg.param,
          static_cast<int>(root_server_.get_obi_role().get_role()));
      switch(msg.type)
      {
        case REFRESH_NEW_SCHEMA_TRIGGER:
          {
            int64_t count = 0;
            if (role != common::ObiRole::MASTER)
            {
              ret = root_server_.renew_user_schema(count);
            }
            TBSYS_LOG(INFO, "[TRIGGER][renew_user_schema(%ld)] done. ret=%d", count, ret);
            break;
          }
        case REFRESH_NEW_CONFIG_TRIGGER:
          {
            int64_t version = tbsys::CTimeUtil::getTime();
            config_mgr_.got_version(version);
            get_log_manager()->get_log_worker()->got_config_version(version);
            TBSYS_LOG(INFO, "[TRIGGER][set_config_version(%ld)] done. ret=%d", version, ret);
            break;
          }
        case UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER:
          {
            int64_t version = tbsys::CTimeUtil::getTime();
            root_server_.set_privilege_version(version);
            TBSYS_LOG(INFO, "[TRIGGER][set_privilege_version(%ld) done. ret=%d]", version, ret);
            break;
          }
        case SLAVE_BOOT_STRAP_TRIGGER:
          {
            if (role != common::ObiRole::MASTER)
            {
              if (OB_SUCCESS != (ret = root_server_.slave_boot_strap()))
              {
                TBSYS_LOG(WARN, "fail to do slave boot strap. ret=%d", ret);
              }
            }
            TBSYS_LOG(INFO, "[TRIGGER][slave_boot_strap] done. ret=%d", ret);
            break;
          }
        //del zhaoqiong [Schema Manager] 20150327:b
        //CREATE_TABLE_TRIGGER and DROP_TABLE_TRIGGER move to ddl_trigger process
//        case CREATE_TABLE_TRIGGER:
//          {
//            if (role != common::ObiRole::MASTER)
//            {
//              if (OB_SUCCESS != (ret = root_server_.trigger_create_table(msg.param)))
//              {
//                TBSYS_LOG(WARN, "fail to create table for slave obi master, ret=%d", ret);
//              }
//            }
//            TBSYS_LOG(INFO, "[TRIGGER][slave_create_table] done. ret=%d", ret);
//            break;
//          }
//        case DROP_TABLE_TRIGGER:
//          {
//            if (role != common::ObiRole::MASTER)
//            {
//              if (OB_SUCCESS != (ret = root_server_.trigger_drop_table(msg.param)))
//              {
//                TBSYS_LOG(WARN, "fail to drop table for slave obi master, ret=%d", ret);
//              }
//            }
//            TBSYS_LOG(INFO, "[TRIGGER][slave_drop_table] done. ret=%d", ret);
//            break;
//          }
        //del:e
        //add liuxiao [secondary index] 20150321
        case SLAVE_CREATE_ALL_CCHECKSUM_INFO:
        {
          if (role != common::ObiRole::MASTER)
          {
            if (OB_SUCCESS != (ret = root_server_.boot_strap_for_create_all_cchecksum_info()))
            {
              TBSYS_LOG(WARN, "fail to do slave create all_ccheck_sum. ret=%d", ret);
            }
          }
          TBSYS_LOG(INFO, "[TRIGGER][slave_create_all_ccheck_sum] done. ret=%d", ret);
          break;
        }
        //add e
        default:
          {
            TBSYS_LOG(WARN, "get unknown trigger event msg:type[%ld]", msg.type);
          }
      }
      int err = OB_SUCCESS;
      // send response message, always success
      if (OB_SUCCESS != (err = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", err);
      }
      else if (OB_SUCCESS != (err = send_response(OB_HANDLE_TRIGGER_EVENT_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, err=%d", err);
      }
      return ret;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    int ObRootWorker::rt_handle_ddl_trigger_event(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);

      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      res.result_code_ = ret;
      common::ObDdlTriggerMsg msg;
      if (OB_SUCCESS != (ret = msg.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize msg");
      }
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      TBSYS_LOG(INFO, "I got a ddl trigger message. version=%ld,%ld,%ld. my role=%d",
          msg.version, msg.type, msg.param,
          static_cast<int>(root_server_.get_obi_role().get_role()));
      //msg type: create=1, drop=2, alter=3, refresh_schema=4
      switch(msg.type)
      {
        case CREATE_TABLE:
          {
            if (role != common::ObiRole::MASTER)
            {
              if (OB_SUCCESS != (ret = root_server_.trigger_create_table(msg.param,msg.version)))
              {
                TBSYS_LOG(ERROR, "fail to create table for slave obi master, ret=%d", ret);
              }
            }
            TBSYS_LOG(INFO, "[TRIGGER][slave_create_table] done. ret=%d", ret);
            break;
          }
        case DROP_TABLE:
          {
            if (role != common::ObiRole::MASTER)
            {
              if (OB_SUCCESS != (ret = root_server_.trigger_drop_table(msg.param,msg.version)))
              {
                TBSYS_LOG(ERROR, "fail to drop table for slave obi master, ret=%d", ret);
              }
            }
            TBSYS_LOG(INFO, "[DDL TRIGGER][slave_drop_table] done. ret=%d", ret);
            break;
          }
        case ALTER_TABLE:
          {
            if (role != common::ObiRole::MASTER)
            {
              if (OB_SUCCESS != (ret = root_server_.trigger_alter_table(msg.param,msg.version)))
              {
                TBSYS_LOG(ERROR, "fail to drop table for slave obi master, ret=%d", ret);
              }
            }
            TBSYS_LOG(INFO, "[DDL TRIGGER][slave_alter_table] done. ret=%d", ret);
            break;
          }
        case REFRESH_SCHEMA:
          {
            int64_t count = 0;
            if (role != common::ObiRole::MASTER)
            {
              if (OB_SUCCESS != (ret = root_server_.renew_user_schema(count,msg.version)))
              {
                TBSYS_LOG(ERROR, "fail to renew schema for slave obi master, ret=%d", ret);
              }
            }
            TBSYS_LOG(INFO, "[TRIGGER][renew_user_schema(%ld)] done. ret=%d", count, ret);
            break;
          }
        default:
          {
            TBSYS_LOG(WARN, "get unknown ddl trigger event msg:type[%ld]", msg.type);
          }
      }
      int err = OB_SUCCESS;
      // send response message, always success
      if (OB_SUCCESS != (err = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", err);
      }
      else if (OB_SUCCESS != (err = send_response(OB_HANDLE_DDL_TRIGGER_EVENT_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, err=%d", err);
      }
      return ret;
    }
    //add:e

    //add pangtianze [Paxos rs_election] 20150612:b
    void ObRootWorker::set_rs_heartbeat_res(ObMsgRsHeartbeatResp &hb_resp)
    {
      hb_resp.addr_ = self_addr_;
      root_server_.get_rs_term(hb_resp.current_term_);
      hb_resp.paxos_num_ = (int32_t)config_.rs_paxos_number;
      //add pangtianze [Paxos rs_election] 20161010:b
      hb_resp.quorum_scale_ = (int32_t)config_.ups_quorum_scale;
      //add:e
    }
    int ObRootWorker::rt_send_vote_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ret = root_server_.send_rs_vote_request();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send vote request async. ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "successed to send vote request async");
      }
      return ret;
    }

    int ObRootWorker::rt_send_leader_broadcast(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ret = root_server_.send_rs_leader_broadcast();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send leader broadcast async. ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "successed to send leader broadcast async");
      }
      return ret;
    }
    int ObRootWorker::rt_handle_rs_heartbeat(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      int32_t server_count = 0;
      common::ObServer server;
      common::ObMsgRsHeartbeat hb;
      if (hb.MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, hb.MY_VERSION);
        ret = OB_ERROR_FUNC_VERSION;
      }
      //deserilize hb info
      if (OB_SUCCESS != (ret = hb.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to deserialize hb_info, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &server_count)))
      {
        TBSYS_LOG(WARN, "failed to deserialize server_count. err=%d", ret);
      }
      else
      {
        //handle hb info from leader
        ret = root_server_.receive_rs_heartbeat(hb);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to handle rs heartbeat. err=%d", ret);
        }
      }
      if (server_count < 0)
      {
          TBSYS_LOG(ERROR, "invalid server_count[%d]!", server_count);
          ret = OB_ERROR;
      }
      if (ret == OB_SUCCESS && server_count > 0)
      {
        common::ObServer servers[server_count];
        for (int32_t i = 0; i < server_count; i++)
        {
          if (OB_SUCCESS != (ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
          {
             TBSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", ret);
             break;
          }
          else
          {
             servers[i] = server;
          }
        }
        if (OB_SUCCESS != (ret = root_server_.update_rs_array(servers, server_count)))
        {
            TBSYS_LOG(ERROR, "update rs array error, err=%d", ret);
        }
      }
      if (OB_ERROR_FUNC_VERSION != ret)
      {
        ObMsgRsHeartbeatResp hb_resp;
        set_rs_heartbeat_res(hb_resp);
        if (OB_SUCCESS != (ret = root_server_.get_rpc_stub().send_rs_heartbeat_resp(hb.addr_, hb_resp)))
        {
          TBSYS_LOG(WARN, "failed to send heartbeat response");
        }
        else
        {
          //success
        }
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    int ObRootWorker::rt_handle_rs_heartbeat_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ObMsgRsHeartbeatResp hb_resp;
      if (OB_SUCCESS != (ret = hb_resp.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize rs heartbeat resp msg error, err=%d", ret);
      }
      else
      {//get the timestamp when rs leader received hb response
        int64_t now = tbsys::CTimeUtil::getTime();
        //mod chujiajia [Paxos rs_election] 20151030:b
        //ret = root_server_.receive_rs_heartbeat_resp(hb_resp.addr_, now, (int32_t)config_.rs_paxos_number);
        ret = root_server_.receive_rs_heartbeat_resp(hb_resp, now);
        //mod:e
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }

    int ObRootWorker::rt_handle_vote_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(out_buff);
      //static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ObMsgRsRequestVote rv;
      ObMsgRsRequestVoteResp rv_resp;
      if (OB_SUCCESS != (ret = rv.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize vote request msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_rs_vote_request(rv, rv_resp)))
      {
         TBSYS_LOG(ERROR, "handle rs vote reuqest failed, err=%d", ret);
      }
      else
      {
        if (OB_SUCCESS != (ret = root_server_.get_rpc_stub().send_vote_request_resp(rv.addr_, rv_resp)))
        {
          TBSYS_LOG(WARN, "failed to send vote request response");
        }
        else
        {
          //success
        }
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    int ObRootWorker::rt_handle_leader_broadcast(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(out_buff);
      //static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      bool is_force;
      ObMsgRsLeaderBroadcast lb;
      ObMsgRsLeaderBroadcastResp lb_resp;
      if (OB_SUCCESS != (ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_force)))
      {
        TBSYS_LOG(WARN, "deserialize force broadcast flag error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = lb.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize leader broadcast msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_rs_leader_broadcast(lb, lb_resp, is_force)))
      {
         TBSYS_LOG(ERROR, "handle rs leader broadcast failed, err=%d", ret);
      }
      else
      {
        if (OB_SUCCESS != (ret = root_server_.get_rpc_stub().send_leader_broadcast_resp(lb.addr_, lb_resp)))
        {
          TBSYS_LOG(WARN, "failed to send leader broadcast response");
        }
        else
        {
          //success
        }
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    int ObRootWorker::rt_handle_vote_request_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ObMsgRsRequestVoteResp rv_resp;
      if (OB_SUCCESS != (ret = rv_resp.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize vote request response msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_rs_vote_request_resp(rv_resp)))
      {
         TBSYS_LOG(WARN, "failed receive vote request response, err=%d", ret);
      }
      else
      {
        //success
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    int ObRootWorker::rt_handle_leader_broadcast_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      ObMsgRsLeaderBroadcastResp lb_resp;
      if (OB_SUCCESS != (ret = lb_resp.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize leader broadcast response msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_rs_leader_broadcast_resp(lb_resp)))
      {
         TBSYS_LOG(WARN, "failed receive leader broadcast response, err=%d", ret);
      }
      else
      {
        //success
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150629:b
    int ObRootWorker::rt_slave_init_first_meta_row(const int32_t version, common::ObDataBuffer& in_buff,
                                             easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ObTabletMetaTableRow row;
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      row.end_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      row.start_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      char buff[OB_MAX_TABLE_NAME_LENGTH];
      row.table_name_.assign(buff, OB_MAX_TABLE_NAME_LENGTH);
      ret = row.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());

      //init first meta
      if (ret == OB_SUCCESS)
      {
        if (root_server_.get_boot_state() == ObBootState::OB_BOOT_OK)
        {
          ret = OB_INIT_TWICE;
          TBSYS_LOG(WARN, "root server is boot ok alreay, not need to replay init log.");
        }
        if (OB_SUCCESS != (ret = root_server_.get_first_meta()->init(row)))
        {
          TBSYS_LOG(WARN, "fail to init first meta file. err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "root server boot strap success. set boot ok");
          root_server_.get_boot()->set_boot_ok();
        }
      }

      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_SLAVE_INIT_FIRST_META_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150709:b
    int ObRootWorker::rt_slave_handle_register(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      common::ObResultCode res;
      res.result_code_ = OB_ERROR;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else
      {
        if (OB_SUCCESS != (ret = send_response(OB_SERVER_REGISTER_RESPONSE,
                MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
        }
      }
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150813:b
    int ObRootWorker::rt_rs_admin_set_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      static const int MY_VERSION = 1;
      common::ObServer rs;
      if (OB_SUCCESS != (ret = rs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize server. err=%d", ret);
      }
      else if (rs == get_rs_master())  ///target rs cannot be self
      {
        //mod chujiajia [Paxos rs_election] 20151105:b
        //ret = OB_SUCCESS;
        ret = OB_IS_ALREADY_THE_MASTER;
        //mod:e
        TBSYS_LOG(WARN, "rootserver has been the leader, rs=%s", rs.to_cstring());
      }
      //add chujiajia [Paxos rs_election] 20151105:b
      else if (ObRoleMgr::MASTER != role_mgr_.get_role()) ///self must be master
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "slave receive set_rs_leader command, error! slave_rs=%s", rs.to_cstring());
      }
      //add:e
      //add pangtianze [Paxos rs_election] 20161009:b
      else if (root_server_.get_ups_manager()->get_is_select_new_master())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "master rs is select new master ups , can't do set _rs_leader now");
      }
      //add:e
      //add pangtianze [Paxos rs_election] 20161009:b
      else
      {
          if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = get_rpc_stub().ping_server(rs, PING_RPC_TIMEOUT)))
          {
            ret = OB_RS_NOT_EXIST;
            TBSYS_LOG(WARN, "fail to ping slave rootserver, rs=%s", rs.to_cstring());
          }
          //add:e
          else
          {
            //add chujiajia [Paxos rs_election] 20151027:b
            TBSYS_LOG(INFO, "leader begin switch to follower");
            if (OB_SUCCESS != (ret = root_server_.switch_master_to_slave()))
            {
              role_mgr_.set_state(ObRoleMgr::STOP);
              TBSYS_LOG(ERROR, "rootserver switch to slave failed, will stop");
            }
            else
            {
              TBSYS_LOG(INFO, "rootserver switch to slave success");
            }
            int64_t timeout = tbsys::CTimeUtil::getTime();
            int64_t currnet_time = timeout;
            while(!role_mgr_.get_role() == ObRoleMgr::SLAVE)
            {
              //wait utill state become slave
              currnet_time = tbsys::CTimeUtil::getTime();
              if (currnet_time - timeout > 3000 * 1000) //3s
              {
                 TBSYS_LOG(WARN, "master switch to slave is timeout, system will elect a new leader soon");
                 break;
              }
              else
              {
                 usleep(100 * 1000);
              }
            }
            //add:e
            ret = get_rpc_stub().set_rs_leader(rs);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "fail to send set new leader rpc, rs=%s", rs.to_cstring());
            }
          }
      }
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_ADMIN_SET_LEADER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", ret);
        }
      }
      return ret;
    }
    int ObRootWorker::rt_set_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "begin force leader broadcast for setting new leader, new leader=%s", get_self_rs().to_cstring());
      ret = root_server_.force_leader_broadcast();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "send force leader broadcast failed");
      }
      // no response
      easy_request_wakeup(req);
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150731:b
    int ObRootWorker::rt_set_election_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      static const int MY_VERSION = 1;
      common::ObServer rs;
      int32_t prioriy = 1;
      if (OB_SUCCESS != (ret = rs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize server. err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &prioriy)))
      {
        TBSYS_LOG(WARN, "fail to deserialize prioriy. err=%d", ret);
      }
      else
      {
        //add pangtianze [Paxos rs_election] 20161014:b
        if (rs == self_addr_)
        {
        //add:e
          if (OB_SUCCESS != (ret = root_server_.set_election_priority(rs, prioriy)))
          {
            TBSYS_LOG(ERROR, "fail to set rootserver election priority, rs=%s, priority=%d",
                    rs.to_cstring(), prioriy);
          }
        }
        //mod:e
        else if (OB_SUCCESS != (ret = get_rpc_stub().send_election_priority(rs, prioriy, config_.network_timeout)))
        {
          TBSYS_LOG(ERROR, "fail to send rootserver election priority, rs=%s, priority=%d",
                    rs.to_cstring(), prioriy);
        }
      }
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", ret);
      }
      else
      {
        ret = send_response(OB_SET_ELECTION_PRIORIYT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", ret);
        }
      }
      return ret;
    }
    int ObRootWorker::rt_set_my_election_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      static const int MY_VERSION = 1;
      int64_t prioriy = 0;
      //mod pangtianze [Paxos rs_election] 20161014:
      //common::ObServer my_addr = get_rs_master();
      common::ObServer my_addr = self_addr_;
      //mod:e
      if (OB_SUCCESS != (ret = common::serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &prioriy)))
      {
        TBSYS_LOG(WARN, "fail to deserialize prioriy. err=%d", ret);
      }
      else
      {
        ret = root_server_.set_election_priority(my_addr, (int32_t)prioriy);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "fail to set my election priority, priority=%ld", prioriy);
        }
      }
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", ret);
      }
      else
      {
        ret = send_response(OB_SET_SPECIFIED_RS_PRIORIYT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", ret);
        }
      }
      return ret;
    }
    //add:e
    int ObRootWorker::rt_get_master_obi_rs(const int32_t version, common::ObDataBuffer &in_buff,
        easy_request_t *req, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      res.result_code_ = ret;
      common::ObiRole::Role role = root_server_.get_obi_role().get_role();
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else
      {
        //mod pangtianze [Paxos rs_election] 20150717:b
        //if (role == common::ObiRole::MASTER)
        if (get_role_manager()->get_role() == common::ObRoleMgr::MASTER)
        //mod:e
        {
          if (OB_SUCCESS != (ret = self_addr_.serialize(out_buff.get_data(),
                  out_buff.get_capacity(), out_buff.get_position())))
          {
            TBSYS_LOG(ERROR, "seriliaze self addr fail, ret: [%d]", ret);
          }
          TBSYS_LOG(TRACE, "master cluster, send master obi rs: [%s]", to_cstring(self_addr_));
        }
        //mod pangtianze [Paxos rs_election] 20150717:b
        //else /* if (role == common::ObiRole::SLAVE) */
        else if (get_role_manager()->get_role() == common::ObRoleMgr::SLAVE)
        //mod:e
        {
          ObServer master_rs;
          //del pangtianze [Paxos rs_election] 20150717:b
          /*
          if (OB_SUCCESS != config_.get_master_root_server(master_rs))
          {
            TBSYS_LOG(ERROR, "Get master root server error, ret: [%d]", ret);
          }
          else if (OB_SUCCESS != (ret = master_rs.serialize(out_buff.get_data(),
                  out_buff.get_capacity(),
                  out_buff.get_position())))
          {
            TBSYS_LOG(ERROR, "seriliaze master obi rs fail, ret: [%d]", ret);
          }
          */
          //del:e
          //add pangtianze [Paxos rs_election] 20150717:b
          master_rs = get_rs_master();
          if (OB_SUCCESS != (ret = master_rs.serialize(out_buff.get_data(),
                            out_buff.get_capacity(),
                            out_buff.get_position())))
          {
            TBSYS_LOG(ERROR, "seriliaze master obi rs fail, ret: [%d]", ret);
          }
          //add:e
          if (role == common::ObiRole::SLAVE)
          {
            TBSYS_LOG(TRACE, "slave cluster, send master obi rs: [%s]", to_cstring(master_rs));
          }
          else
          {
            TBSYS_LOG(INFO, "init cluster, send master obi rs: [%s]", to_cstring(master_rs));
          }
        }
      }

      if (OB_SUCCESS != (ret = send_response(OB_GET_MASTER_OBI_RS_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObRootWorker::rt_set_config(const int32_t version,
        common::ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObString config_str;
      if (OB_SUCCESS != (ret = config_str.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "Deserialize config string failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_.add_extra_config(config_str.ptr(), true)))
      {
        TBSYS_LOG(ERROR, "Set config failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_mgr_.reload_config()))
      {
        TBSYS_LOG(ERROR, "Reload config failed! ret: [%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "Set config successfully! str: [%s]", config_str.ptr());
        config_.print();
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_SET_CONFIG_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }

    int ObRootWorker::rt_get_config(const int32_t version,
        common::ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS !=
          (ret = config_.serialize(out_buff.get_data(),
                                   out_buff.get_capacity(),
                                   out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize configuration fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_GET_CONFIG_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }
    //for bypass
    int ObRootWorker::rt_check_task_process(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id,
        common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(in_buff);
      UNUSED(out_buff);
      root_server_.check_bypass_process();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "bypas process is already processing, wait.., ret=%d", ret);
      }
      easy_request_wakeup(req);
      return ret;
    }

    int ObRootWorker::rt_prepare_bypass_process(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObBypassTaskInfo table_name_id;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = table_name_id.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize table_name_id. err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        TBSYS_LOG(INFO, "start to prepare bypass process. update max table id");
        result_msg.result_code_ = root_server_.prepare_bypass_process(table_name_id);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to do bypass proces. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize result_msg. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = table_name_id.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize table_name_id. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_RS_PREPARE_BYPASS_PROCESS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rs_cs_load_bypass_sstable_done(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      common::ObTableImportInfoList table_list;
      bool is_load_succ = false;
      ObServer cs;
      easy_addr_t addr = get_easy_addr(req);
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = cs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize cs_addr. cs_addr=%s, err=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = table_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize table_list. cs_addr=%s, err=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_load_succ);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize bool flag. cs_addr=%s, err=%d", inet_ntoa_r(addr), ret);
        }
        else
        {
          TBSYS_LOG(INFO, "cs = %s have finished load sstable. result=%s",
              inet_ntoa_r(addr), is_load_succ ? "successed" : "failed");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.cs_load_sstable_done(cs, table_list, is_load_succ);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to do bypass proces. cs_addr=%s, ret=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize result_msg. cs_addr=%s, ret=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_CS_DELETE_TABLE_DONE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rt_cs_delete_table_done(const int32_t version, common::ObDataBuffer& in_buff,
         easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      uint64_t table_id =  UINT64_MAX;
      bool is_delete_succ = false;
      ObServer cs;
      easy_addr_t addr = get_easy_addr(req);
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = cs.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize cs_addr. cs_addr=%s, err=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize table_id.cs_addr=%s, err=%d",
              inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_delete_succ);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize bool flag.cs_addr=%s, err=%d",
              inet_ntoa_r(addr), ret);
        }
        else
        {
          TBSYS_LOG(INFO, "cs = %s have finished delete table, table_id = %lu. result=%s",
              inet_ntoa_r(addr), table_id, is_delete_succ ? "successed" : "failed");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.cs_delete_table_done(cs, table_id, is_delete_succ);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to do bypass proces. cs_addr=%s, ret=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize result_msg. cs_addr=%s, ret=%d", inet_ntoa_r(addr), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_CS_DELETE_TABLE_DONE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rt_start_bypass_process(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObBypassTaskInfo table_name_id;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = table_name_id.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize table_name_id. err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.start_bypass_process(table_name_id);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to do bypass proces. ret=%d", result_msg.result_code_);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize result_msg. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_RS_START_BYPASS_PROCESS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    //add pangtianze [Paxos rs_election] 20150814:b
    int ObRootWorker::submit_leader_broadcast_task(const bool force)
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = common::serialization::encode_bool(buff.get_data(), buff.get_capacity(), buff.get_position(), force)))
        {
          TBSYS_LOG(WARN, "fail to serialize force. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_LEADER_BROADCAST_TASK, election_thread_queue_, (int32_t)config_.election_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task for leader broadcast. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "submit async task to check bypass process");
        }
      }
      return ret;
    }
    int ObRootWorker::submit_vote_request_task()
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_VOTE_REQUEST_TASK, election_thread_queue_, (int32_t)config_.election_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task for leader broadcast. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "submit async task to check bypass process");
        }
      }
      return ret;
    }
    //add:e

    int ObRootWorker::submit_check_task_process()
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_INNER_MSG_CHECK_TASK_PROCESS, write_thread_queue_, (int32_t)config_.write_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task to check bypass done. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "submit async task to check bypass process");
        }
      }
      return ret;
    }
int ObRootWorker::rt_write_schema_to_file(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  UNUSED(in_buff);
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "invalid request version, version=%d", version);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }


  if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
  {
    if (OB_SUCCESS != (ret = root_server_.write_schema_to_file()))
    {
      TBSYS_LOG(WARN, "write schema to file faile, err=%d", ret);
      result_msg.result_code_ = ret;
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if (OB_SUCCESS == ret)
  {
    send_response(OB_WRITE_SCHEMA_TO_FILE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
  }
  OB_STAT_INC(ROOTSERVER, INDEX_GET_SCHMEA_COUNT);
  return ret;
}
int ObRootWorker::rt_change_table_id(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      int64_t table_id = 0;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &table_id)))
      {
        TBSYS_LOG(WARN, "deserialize register msg error, err=%d", ret);
      }
      else
      {
        ret = root_server_.change_table_id(table_id);
      }
      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_CHANGE_TABLE_ID_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }

    int ObRootWorker::rt_start_import(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObString table_name;
      uint64_t table_id = 0;
      ObString uri;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize table name error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table id error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = uri.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize uri error, err=%d", ret);
      }
      else if (config_.enable_load_data == false)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
            table_name.length(), table_name.ptr(), table_id);
      }
      else if (OB_SUCCESS != (ret = root_server_.start_import(table_name, table_id, uri)))
      {
        TBSYS_LOG(WARN, "failed to import table_name=%.*s table_id=%lu, uri=%.*s, ret=%d",
            table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
      }

      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_ADMIN_START_IMPORT_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send repsone, ret=%d", ret);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }

    int ObRootWorker::rt_import(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObString table_name;
      uint64_t table_id = 0;
      ObString uri;
      int64_t start_time = 0;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize table name error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table id error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = uri.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize uri error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &start_time)))
      {
        TBSYS_LOG(WARN, "deserialize start_time error, err=%d", ret);
      }
      else if (config_.enable_load_data == false)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
            table_name.length(), table_name.ptr(), table_id);
      }
      else if (OB_SUCCESS != (ret = root_server_.import(table_name, table_id, uri, start_time)))
      {
        TBSYS_LOG(WARN, "failed to import table_name=%.*s table_id=%lu, uri=%.*s, ret=%d",
            table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
      }

      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_IMPORT_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send repsone, ret=%d", ret);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }

    int ObRootWorker::rt_start_kill_import(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObString table_name;
      uint64_t table_id;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize table name error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table id error, err=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        ret = root_server_.start_kill_import(table_name, table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to kill import table_name=%.*s table_id=%lu", table_name.length(),
              table_name.ptr(), table_id);
        }
      }
      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_ADMIN_START_KILL_IMPORT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }

    int ObRootWorker::rt_get_import_status(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObString table_name;
      uint64_t table_id = 0;
      ObLoadDataInfo::ObLoadDataStatus status = ObLoadDataInfo::FAILED;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize table name error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table id error, err=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        ret = root_server_.get_import_status(table_name, table_id, status);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to check import status table_name=%.*s table_id=%lu, ret=%d", table_name.length(),
              table_name.ptr(), table_id, ret);
        }
      }
      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      int32_t status_i32 = static_cast<int32_t>(status);
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_i32(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position(), status_i32)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_GET_IMPORT_STATUS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }

    int ObRootWorker::rt_set_import_status(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObString table_name;
      uint64_t table_id = 0;
      int32_t status_i32 = 0;
      ObLoadDataInfo::ObLoadDataStatus status = ObLoadDataInfo::FAILED;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = table_name.deserialize(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize table name error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table id error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &status_i32)))
      {
        TBSYS_LOG(WARN, "deserialize status error, err=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        status = static_cast<ObLoadDataInfo::ObLoadDataStatus>(status_i32);
        ret = root_server_.set_import_status(table_name, table_id, status);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to check import status table_name=%.*s table_id=%lu, ret=%d", table_name.length(),
              table_name.ptr(), table_id, ret);
        }
      }
      // send response
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_RS_SET_IMPORT_STATUS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }

    int ObRootWorker::rt_force_create_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      uint64_t table_id = 0;
      int64_t schema_version = 0;//add zhaoqiong [Schema Manager] 20150327
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (root_server_.get_obi_role().get_role() == common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "master instance can't force to create table. role=%s", root_server_.get_obi_role().get_role_str());
      }
      else
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&table_id)))
        {
          TBSYS_LOG(WARN, "failed to deserialize table_id, err=%d", ret);
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&schema_version)))
        {
          TBSYS_LOG(WARN, "failed to deserialize table_id, err=%d", ret);
        }
        //add:e
        //mod zhaoqiong [Schema Manager] 20150327:b
        //rs should use value from __all_ddl_operation as local schema timestamp
        //else if (OB_SUCCESS != (ret = root_server_.force_create_table(table_id)))
        else if (OB_SUCCESS != (ret = root_server_.force_create_table(table_id,schema_version)))
        //mod:e
        {
          TBSYS_LOG(WARN, "failed to create table, table_id=%ld, err=%d", table_id, ret);
        }
        if (OB_SUCCESS != ret)
        {
          res.message_ = ob_get_err_msg();
          TBSYS_LOG(WARN, "create table err=%.*s", res.message_.length(), res.message_.ptr());
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_FORCE_CREATE_TABLE_FOR_EMERGENCY_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, table_id=%ld, err=%d", table_id, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "send response for creating table, table_id=%ld, ret=%d",
              table_id, res.result_code_);
        }
      }
      return ret;
    }

    int ObRootWorker::rt_force_drop_table(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      uint64_t table_id = 0;
      int64_t schema_version = 0;//add zhaoqiong [Schema Manager] 20150327
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (root_server_.get_obi_role().get_role() == common::ObiRole::MASTER)
      {
        res.result_code_ = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "master instance can't force to drop table. role=%s", root_server_.get_obi_role().get_role_str());
      }
      else
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&table_id)))
        {
          TBSYS_LOG(WARN, "failed to deserialize table_id, err=%d", ret);
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
                in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&schema_version)))
        {
          TBSYS_LOG(WARN, "failed to deserialize table_id, err=%d", ret);
        }
        //add:e
        //mod zhaoqiong [Schema Manager] 20150327:b
        //rs should use value from __all_ddl_operation as local schema timestamp
        //else if (OB_SUCCESS != (ret = root_server_.force_drop_table(table_id)))
        else if (OB_SUCCESS != (ret = root_server_.force_drop_table(table_id,schema_version)))
        //mod:e
        {
          TBSYS_LOG(WARN, "failed to drop table, table_id=%ld, err=%d", table_id, ret);
        }
        if (OB_SUCCESS != ret)
        {
          res.message_ = ob_get_err_msg();
          TBSYS_LOG(WARN, "drop table err=%.*s", res.message_.length(), res.message_.ptr());
        }
        res.result_code_ = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret)
      {
        // send response message
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = send_response(OB_FORCE_DROP_TABLE_FOR_EMERGENCY_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
        {
          TBSYS_LOG(WARN, "failed to send response, table_id=%ld, err=%d", table_id, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "send response for drop table, table_id=%ld, ret=%d",
              table_id, res.result_code_);
        }
      }
      return ret;
    }

    int ObRootWorker::rt_notify_switch_schema(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      uint64_t table_id = 0;
      bool only_core_tables = false;
      bool force_update = true;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "un-supported rpc version=%d", version);
        res.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(OB_SUCCESS != (ret = root_server_.notify_switch_schema(only_core_tables, force_update)))
      {
        TBSYS_LOG(WARN, "failed to notify_switch_schema");
      }

      res.result_code_ = ret;

      // send response message
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_NOTIFY_SWITCH_SCHEMA_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, table_id=%ld, err=%d", table_id, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "send response for notify_switch_schema, ret=%d", res.result_code_);
      }

      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }
      return ret;
    }
    //add pangtianze [Paxos rs_election] 20161010:b
    int ObRootWorker::rt_handle_change_paxos_num_request(const int32_t version, common::ObDataBuffer& in_buff,
                                                         easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      ObMsgRsChangePaxosNum change_paxos_num;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = change_paxos_num.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize change_paxos_num msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_change_paxos_num_request(change_paxos_num)))
      {
         TBSYS_LOG(WARN, "refuse change_paxos_num request, rs sender=%s, err=%d", to_cstring(change_paxos_num.addr_), ret);
      }
      else
      {
        config_mgr_.dump2file();
        config_mgr_.get_update_task().write2stat();
      }

      // send response message
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_CHANGE_PAXOS_NUM_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "succ send response for change_paxos_num_response");
      }
      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20161011:b
    int ObRootWorker::rt_handle_new_ups_quorum_scale_request(const int32_t version, common::ObDataBuffer& in_buff,
                                                             easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      ObMsgRsNewQuorumScale new_quorum_scale;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = new_quorum_scale.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize new_quorum_scale msg error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = root_server_.receive_new_quorum_scale_request(new_quorum_scale)))
      {
         TBSYS_LOG(WARN, "refuse new_quorum_scale request, rs sender=%s, err=%d", to_cstring(new_quorum_scale.addr_), ret);
      }
      else
      {
          //succ
          config_mgr_.dump2file();
          config_mgr_.get_update_task().write2stat();
      }

      // send response message
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_RS_CHANGE_UPS_QUORUM_SCALE_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "succ send response for new_quorum_scale request");
      }
      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }

      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20161010:b
    int ObRootWorker:: rt_rs_admin_change_rs_paxos_number(const int32_t version, common::ObDataBuffer& in_buff,
                                                          easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      int32_t old_rs_paxos_num = (int32_t)config_.rs_paxos_number;
      int32_t new_paxos_num = 0;
      int32_t server_count=0;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == root_server_.get_rs_node_mgr())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "rs_node_mgr_ is null");
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &new_paxos_num)))
      {
        TBSYS_LOG(WARN, "deserialize new_paxos_num error, err=%d", ret);
      }
      else
      {
        //mod bingo [Paxos RS management] 20170215:b
        ObServer servers[OB_MAX_RS_COUNT];
        root_server_.get_rs_node_mgr()->get_all_alive_servers(servers, server_count);
        for(int32_t i = 0; i < (server_count - 1); i++)
        {
          if (OB_SUCCESS != (ret = get_rpc_stub().ping_server(servers[i], PING_RPC_TIMEOUT)))
          {
            ret = OB_RS_NOT_EXIST;
            TBSYS_LOG(ERROR, "fail to ping slave rootserver, rs=%s", servers[i].to_cstring());
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (((new_paxos_num > server_count / 2 &&
            new_paxos_num < server_count * 2) ||
            (1 == new_paxos_num && 2 == old_rs_paxos_num ) || 1 == old_rs_paxos_num) &&
            new_paxos_num <= OB_MAX_RS_COUNT)
          {
            TBSYS_LOG(INFO, "change paxos number [%d->%d]", old_rs_paxos_num, new_paxos_num);
            if((new_paxos_num < server_count && 1 != new_paxos_num)
               || (1 == old_rs_paxos_num && (2 != new_paxos_num || 2 != server_count))
               || (1 == new_paxos_num && old_rs_paxos_num > 2))
            {
              ret = OB_INVALID_PAXOS_NUM;
              TBSYS_LOG(WARN, "new_paxos_num[%d] is invalid while the old is[%d], alive rs count is[%d], err=%d, please check first!",
                        new_paxos_num,old_rs_paxos_num,server_count, ret);
            }
            else
            {
              int32_t succ_count = 1;
              root_server_.get_rs_node_mgr()->raise_my_term();
              //update loacal
              root_server_.set_paxos_num(new_paxos_num);
              root_server_.get_rs_node_mgr()->set_my_paxos_num(new_paxos_num);
              config_mgr_.dump2file();
              config_mgr_.get_update_task().write2stat();
              //set message
              ObMsgRsChangePaxosNum change_paxos_num_msg;
              change_paxos_num_msg.addr_ = root_server_.get_self();
              change_paxos_num_msg.term_ = root_server_.get_rs_node_mgr()->get_my_term();
              change_paxos_num_msg.new_paxos_num_ = new_paxos_num;
              //send new paxos number to other rs
              for(int32_t i = 0; i < (server_count - 1); i++)
              {
                ret = root_server_.get_rpc_stub().send_change_config_request(servers[i], change_paxos_num_msg, config_.network_timeout);
                if(ret == OB_SUCCESS)
                {
                  succ_count++;
                }
                else
                {
                  TBSYS_LOG(WARN, "send change_paxos_num request to rs[%s] failed! ret=%d.", servers[i].to_cstring(), ret);
                }
              }
              if (succ_count != server_count)
              {
                 ret = OB_RS_NOT_EXIST;
              }
              else
              {
                 TBSYS_LOG(INFO, "change paxos num in all rootserver succ.");
              }
              //add pangtianze [Paxos rs_election] 20170707:b
              {
                 TBSYS_LOG(INFO, "remove invalid rootserver info");
                 root_server_.get_rs_node_mgr()->remove_invalid_info();
              }
              //add:e
            }
          }
          else
          {
             ret = OB_INVALID_PAXOS_NUM;
             TBSYS_LOG(WARN, "receive invalid new paxos number:%d, it should be in (%d,%d), err=%d",
                       new_paxos_num, server_count/2, server_count*2, ret);
          }
        }
       //mod:e plus server ping, replace old_rs_paxos_num with server_count somewhere
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                                             out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "result_code.serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), (int32_t)config_.rs_paxos_number)))
      {
        TBSYS_LOG(WARN, "serialize current_rs_paxos_number failed, ret:%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), old_rs_paxos_num)))
      {
        TBSYS_LOG(WARN, "serialize old_rs_paxos_number failed, ret:%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), server_count)))
      {
        TBSYS_LOG(WARN, "serialize server_count failed, ret:%d", ret);
      }
      else
      {
        ret = send_response(OB_CHANGE_RS_PAXOS_NUMBER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    //add:e

    //add pangtianze [Paxos rs_election] 20161010:b
    int ObRootWorker::rt_rs_admin_change_ups_quorum_scale(const int32_t version, common::ObDataBuffer& in_buff,
                                                          easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      int32_t old_ups_quorum_scale_num = (int32_t)config_.ups_quorum_scale;
      int32_t new_ups_quorum_scale_num = 0;
      int32_t server_count = 0;
      int32_t ups_server_count = 0;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == root_server_.get_ups_manager())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "ups_nanager_ is null");
      }
      else if (NULL == root_server_.get_rs_node_mgr())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "rs_node_mgr_ is null");
      }
      else if (root_server_.get_ups_manager()->get_is_select_new_master())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "master rs is select new master ups , can't do change quorum scale now");
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
              in_buff.get_capacity(), in_buff.get_position(), &new_ups_quorum_scale_num)))
      {
        TBSYS_LOG(WARN, "deserialize new_ups_quorum_scale error, err=%d", ret);
      }
      else
      {
        //mod bingo [Paxos UPS management] 20170215:b
        ObServer ups_array[OB_MAX_UPS_COUNT];
        root_server_.get_ups_manager()->get_online_ups_array(ups_array, ups_server_count);
        for(int32_t i = 0; i < OB_MAX_UPS_COUNT; i++)
        {
            if (ups_array[i].is_valid())
            {
                if (OB_SUCCESS != (ret = get_rpc_stub().ping_server(ups_array[i], PING_RPC_TIMEOUT)))
                {
                   ret = OB_UPS_NOT_EXIST;
                   TBSYS_LOG(ERROR, "fail to ping slave updateserver, ups=%s", ups_array[i].to_cstring());
                   break;
                }
            }
        }
        if (OB_SUCCESS == ret)
        {
            if(((new_ups_quorum_scale_num > ups_server_count / 2 &&
               new_ups_quorum_scale_num < ups_server_count * 2) ||
               (1 == new_ups_quorum_scale_num && 2 == old_ups_quorum_scale_num)|| 1 == old_ups_quorum_scale_num) &&
                new_ups_quorum_scale_num <= OB_MAX_UPS_COUNT)
            {
              TBSYS_LOG(INFO, "change quorum scale [%d->%d], ups_count[%d]",
                        old_ups_quorum_scale_num, new_ups_quorum_scale_num, ups_server_count);
              if((new_ups_quorum_scale_num < ups_server_count && 1 != new_ups_quorum_scale_num)
                  || (1 == old_ups_quorum_scale_num && (2 != new_ups_quorum_scale_num || 2 != ups_server_count))
                  || (1 == new_ups_quorum_scale_num && old_ups_quorum_scale_num > 2))
              {
                ret = OB_INVALID_QUORUM_SCALE;
                TBSYS_LOG(WARN, "new_ups_quorum_scale_num[%d] is invalid while the old is[%d], alive ups count is[%d], please check first!",
                          new_ups_quorum_scale_num,old_ups_quorum_scale_num,ups_server_count);
              }
              else
              {
                ObServer servers[OB_MAX_RS_COUNT];
                root_server_.get_rs_node_mgr()->get_all_alive_servers(servers, server_count);
                int32_t succ_count = 1;
                root_server_.get_rs_node_mgr()->raise_my_term();
                //update local
                root_server_.set_quorum_scale(new_ups_quorum_scale_num);
                root_server_.get_rs_node_mgr()->set_my_quorum_scale(new_ups_quorum_scale_num);
                config_mgr_.dump2file();
                config_mgr_.get_update_task().write2stat();
                //set message info
                ObMsgRsNewQuorumScale new_ups_quorum_scale_msg;
                new_ups_quorum_scale_msg.addr_ = root_server_.get_self();
                new_ups_quorum_scale_msg.term_ = root_server_.get_rs_node_mgr()->get_my_term();;
                new_ups_quorum_scale_msg.new_ups_quorum_scale_ = new_ups_quorum_scale_num;
                //send new quorum scale values to other rs
                for(int32_t i = 0; i < (server_count - 1); i++)
                {
                  ret = root_server_.get_rpc_stub().send_change_ups_quorum_scale_request(servers[i], new_ups_quorum_scale_msg, config_.network_timeout);
                  if(ret == OB_SUCCESS)
                  {
                    succ_count++;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "send change_ups_quorum_scale request to rs[%s] failed! ret=%d.", servers[i].to_cstring(), ret);
                  }
                }
                if (succ_count != server_count)
                {
                   ret = OB_RS_NOT_EXIST;
                }
                else
                {
                   TBSYS_LOG(INFO, "change ups quorum scale in all rootserver succ.");
                }
              }
            }
            else
            {
                ret = OB_INVALID_QUORUM_SCALE;
                TBSYS_LOG(WARN, "receive invalid new quorum scale:%d, it should be in (%d,%d), err=%d"
                          , new_ups_quorum_scale_num, ups_server_count/2, ups_server_count*2, ret);
            }
        }
      //mod:e  plus server ping, replace old_ups_quorum_scale_num with ups_server_count, somewhere
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                                             out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "result_code.serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), (int32_t)config_.ups_quorum_scale)))
      {
        TBSYS_LOG(WARN, "serialize current_ups_quorum_scale failed, ret:%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), old_ups_quorum_scale_num)))
      {
        TBSYS_LOG(WARN, "serialize old_ups_quorum_scale failed, ret:%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(),
                                                               out_buff.get_capacity(), out_buff.get_position(), ups_server_count)))
      {
        TBSYS_LOG(WARN, "serialize ups_server_count failed, ret:%d", ret);
      }
      else
      {
        ret = send_response(OB_CHANGE_UPS_QUORUM_SCALE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    //add:e
    //add lbzhong [Paxos Cluster.Balance] 201607020:b
    int ObRootWorker::rs_dump_balancer_info(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, my_version=%d", version, MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }

      int64_t table_id = 0;
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
              in_buff.get_position(), (int64_t*)&table_id))
        {
          TBSYS_LOG(ERROR, "failed to deserialize");
          err = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == err)
      {
        root_server_.dump_balancer_info(table_id);
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_DUMP_BALANCER_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", err);
        }
      }
      return err;
    }

    int ObRootWorker::rt_get_replica_num(const int32_t version,
        common::ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;

      int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
      memset(replicas_nums, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
      res.result_code_ = ret;
      if(OB_SUCCESS != (ret = root_server_.get_cluster_tablet_replicas_num(replicas_nums)))
      {
        TBSYS_LOG(INFO, "fail to get cluster replicas num, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else
      {
        bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
        root_server_.get_alive_cluster(is_cluster_alive);
        for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
        {
          int32_t replca_num = replicas_nums[cluster_id];
          if(!is_cluster_alive[cluster_id] && replicas_nums[cluster_id] == 0)
          {
            replca_num = -1; // no use cluster
          }
          if(OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position(), replca_num)))
          {
            TBSYS_LOG(ERROR, "serialize replica_num fail, ret: [%d]", ret);
            break;
          }
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = send_response(OB_GET_REPLICA_NUM_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }
    //add:e

    //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
    int ObRootWorker::rt_get_cluster_ups(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObUpsList ups_list;
      static const int MY_VERSION = 1;
      int32_t cluster_id = 0;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid reqeust version, version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &cluster_id)))
      {
        TBSYS_LOG(ERROR, "deserialize cluster_id error");
      }
      else
      {
        ret = root_server_.get_ups_list(ups_list, cluster_id);
      }
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS == res.result_code_
          && OB_SUCCESS != (ret = ups_list.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_UPS_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    //add:e

    //add huangjianwei [Paxos rs_switch] 20160726:b
    int ObRootWorker::submit_get_server_status_task()
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      if (OB_SUCCESS == ret)
      {
        ObDataBuffer buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS != (ret = submit_async_task_(OB_RS_GET_SERVER_STATUS, read_thread_queue_,
                (int32_t)config_.read_queue_size, &buff)))
        {
          TBSYS_LOG(WARN, "fail to submit async task to get server status. ret =%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "submit get server status task.");
        }
      }
      return ret;
    }

    int ObRootWorker::rt_get_server_status(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      UNUSED(version);
      UNUSED(req);
      UNUSED(channel_id);
      UNUSED(out_buff);

      int ret = OB_SUCCESS;
      //mod pangtianze [Paxos rs_switch] 20170628:b
      //if (!is_switching_to_master())
      if (get_role_manager()->is_slave())
      //mod:e
      {
        if (OB_SUCCESS != (ret = get_server_status_task_.get_server_status()))
        {
          TBSYS_LOG(WARN, "fail to get server status. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "get server status success");

        }
        //add pangtianze [Paxos rs_switch] 20170209:b  //del pangtianze 20170628
//        if (root_server_.is_master())
//        {
//           ret = get_server_status_task_.check_server_alive(root_server_.get_config().cs_lease_duration_time);
//           if (OB_SUCCESS != ret)
//           {
//             TBSYS_LOG(WARN, "fail to check server status. ret=%d", ret);
//           }
//        }
        //add:e
      }
      //add pangtianze [Paxos] 20170630:b
      else if (get_role_manager()->is_master())
      {
        if (OB_SUCCESS != (ret = root_server_.refresh_list_to_all()))
        {
          TBSYS_LOG(WARN, "fail to refresh rs list. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "refresh rs list success");
        }
      }
      //add:e
      return ret;
    }
    //add:e

    //add pangtianze [Paxos rs_switch] 20170208:b
    int ObRootWorker::rt_force_server_regist()
    {
      int ret = OB_SUCCESS;
      int64_t now = tbsys::CTimeUtil::getTime();
      ServerStatus server_status;
      int64_t server_status_num = get_server_status_task_.get_server_status_count();            
      root_server_.get_server_manager().reset_server_info(); //add pangtianze [Paxos] 20170706:b:e
      for (int32_t i = 0; i < server_status_num; i++)
      {
          server_status = get_server_status_task_.get_status_by_index(i);
          ObServer server = server_status.addr_;
          ObRole svr_type = server_status.svr_type_;

          //init last_check server alive time, then rs can do check after a period
          get_server_status_task_.set_last_check_server_time(now);
          //make server lease timeout, then it can re-regist
          if (OB_CHUNKSERVER == svr_type)
          {
              root_server_.receive_hb(server, 0, false, OB_CHUNKSERVER, false);
          }
          else if (OB_MERGESERVER == svr_type)
          {
              root_server_.receive_hb(server, server_status.ms_sql_port_, false, OB_MERGESERVER, false);
          }
          if(OB_SUCCESS != (ret = get_rpc_stub().force_server_regist(self_addr_, server)))
          {
              TBSYS_LOG(WARN, "send message to force server regist failed, svr_type=%d, addr=%s", svr_type, server.to_cstring());
          }
          else
          {
              TBSYS_LOG(INFO, "send message to force server regist succ, svr_type=%d, addr=%s", svr_type, server.to_cstring());
          }
      }
      return ret;
    }
    const char * ObRootWorker::server_to_cstr(const ObString &svr_str)
    {
        const char* ret = "UNKNOW";
        ObRole flag = (OB_SUCCESS == svr_str.compare("chunkserver"))?OB_CHUNKSERVER:OB_MERGESERVER;
        switch(flag)
        {
          case OB_CHUNKSERVER:
            ret = "chunkserver";
            break;
          case OB_MERGESERVER:
            ret = "mergeserver";
            break;
          default:
            break;
        }
        return ret;
    }
    //add:e
    //add bingo [Paxos rs_election] 20161009
    int ObRootWorker::rt_master_get_all_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      //get all rs, slaves first, master tail
      ObServer servers[OB_MAX_RS_COUNT];
      int32_t server_count = 0;
      root_server_.get_rs_node_mgr()->get_all_alive_servers(servers, server_count);

      //get all priority, slaves first, master tail
      int32_t all_priority[server_count];
      for(int32_t i = 0; i < server_count; i++)
      {
        if(OB_SUCCESS != (ret = rt_rpc_stub_.get_priority_from_rs(servers[i], config_.network_timeout, all_priority[i])))
        {
          TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
        }
      }

      //serialize all servers and priorities
      out_buff.get_position() = 0;
      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if(OB_SUCCESS != (ret = rt_rpc_stub_.serialize_servers(out_buff, servers, server_count)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        for(int32_t j = 0; j < server_count; j++)
        {
          if(OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
                                                             out_buff.get_position(), all_priority[j])))
          {
            TBSYS_LOG(WARN, "serialize priority of %d error, err=%d", j, ret);
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = send_response(OB_MASTER_GET_ALL_PRIORITY_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    int ObRootWorker::rt_get_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      int32_t priority = root_server_.get_rs_node_mgr()->get_my_priority();

      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
                                                               out_buff.get_position(), priority)))
      {
        TBSYS_LOG(WARN, "serialize error, err=%d", ret);
      }
      else
      {
        ret = send_response(OB_GET_RS_PRIORITY_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    //add:e
    //add bingo [Paxos Cluser.Balance] 20161020:b
    int ObRootWorker::rt_set_master_cluster_id(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req,
                                 const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int32_t new_master_cluster_id = 0;
      common::ObResultCode res;
      if (ObRoleMgr::MASTER == role_mgr_.get_role())
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &new_master_cluster_id)))
        {
          TBSYS_LOG(ERROR, "deserialize new_master_cluster_id error");
        }
        //validity check
        bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
        root_server_.get_alive_cluster(is_cluster_alive);
        if(!is_cluster_alive[new_master_cluster_id])
        {
          ret = OB_CLUSTER_ID_ERROR;
          TBSYS_LOG(WARN, "invalid new_master_cluster_id=%d, not alive", new_master_cluster_id);
        }
        else if(root_server_.get_obi_role().get_role() == ObiRole::MASTER)
        {
          int32_t old_master_cluster_id = root_server_.get_master_cluster_id();
          if(OB_SUCCESS != (ret = root_server_.set_master_cluster_id(new_master_cluster_id)))
          {
            TBSYS_LOG(WARN, "fail to set master cluster id to master RS, cluster_id=%d", new_master_cluster_id);
          }
          else if(OB_SUCCESS != (ret = set_new_master_cluster(old_master_cluster_id,new_master_cluster_id))) //update all cluster, replace bingo [Paxos __all_cluster] 20170714:b:e
          {
            TBSYS_LOG(WARN, "fail to refresh __all_cluster while set new master cluster, ret: [%d]", ret);
          }
        }
      }
      else
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "slave receive set_replica_num command, error!");
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_SET_MASTER_CLUSTER_ID_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }
    //add:e

    //add bingo [Paxos __all_cluster] 20170714:b
    int ObRootWorker::set_new_master_cluster(const int32_t old_master_cluster_id, const int32_t new_master_cluster_id)
    {
      int ret = OB_SUCCESS;
      if(old_master_cluster_id != 0){
        //change old master cluster role and flow percent
        char buf[OB_MAX_SQL_LENGTH] = "";
        const char * sql_temp = "REPLACE INTO %s"
          "(cluster_id, cluster_role, cluster_flow_percent)"
          "VALUES(%d, %d, %d);";
        snprintf(buf, sizeof (buf), sql_temp, OB_ALL_CLUSTER, old_master_cluster_id, 2, 0);
        ObString sql;
        sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
        ret = get_sql_proxy().query(true, config_.retry_times, config_.network_timeout, sql);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "update all_cluster sql success! ret: [%d], cluster_id=%d", ret, old_master_cluster_id);
        }
        else
        {
          TBSYS_LOG(WARN, "update all_cluster sql failed! ret: [%d], cluster_id=%d", ret, old_master_cluster_id);
        }
      }

      //change new master cluster role and flow percent
      char buf_new[OB_MAX_SQL_LENGTH] = "";
      const char * sql_temp_new = "REPLACE INTO %s"
        "(cluster_id, cluster_role, cluster_flow_percent)"
        "VALUES(%d, %d, %d);";
      snprintf(buf_new, sizeof (buf_new), sql_temp_new, OB_ALL_CLUSTER, new_master_cluster_id, 1, 100);
      ObString sql_new;
      sql_new.assign_ptr(buf_new, static_cast<ObString::obstr_size_t>(strlen(buf_new)));
      ret = get_sql_proxy().query(true, config_.retry_times, config_.network_timeout, sql_new);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "update all_cluster sql success! ret: [%d], cluster_id=%d", ret, new_master_cluster_id);
      }
      else
      {
        TBSYS_LOG(WARN, "update all_cluster sql failed! ret: [%d], cluster_id=%d", ret, new_master_cluster_id);
      }

      return ret;
    }
    //add:e

    //add bingo [Paxos rs restart] 20170221:b
    bool ObRootWorker::is_exist(common::ObServer *servers, common::ObServer server)
    {
      bool exist = false;
      for(int32_t i = 0; i < OB_MAX_RS_COUNT; i++)
      {
        if(servers[i] == server){
          exist = true;
          break;
        }
      }
      return exist;
    }
    //add:e

    //add bingo [Paxos rs management] 20170301:b
    int ObRootWorker::rt_get_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      common::ObServer leader;
      if (RS_ELECTION_DOING == root_server_.get_rs_node_mgr()->get_election_state())
      {
        ret = OB_RS_DOING_ELECTION;
        TBSYS_LOG(WARN,"rs is doing election, please wait and do again later");
      }
      else
      {
        ret = root_server_.get_rs_node_mgr()->get_leader(leader);
        TBSYS_LOG(INFO,"get leader, ret=%d", ret);
      }

      if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(),
              out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize result_msg error, err=%d", ret);
      }
      else if(OB_SUCCESS != (ret = leader.serialize(out_buff.get_data(), out_buff.get_capacity(),
                                                    out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "serialize leader error, err=%d", ret);
      }

      {
        ret = send_response(OB_GET_RS_LEADER_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      return ret;
    }
    //add:e
    //add bingo [Paxos Cluster.Flow.UPS] 20170405:
    int ObRootWorker::rt_sync_is_strong_consistent(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t master_is_strong_consistent = 0;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%d", version);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
              in_buff.get_position(), &master_is_strong_consistent)))
      {
        TBSYS_LOG(WARN, "failed to decode is_strong_consistent, err=%d", ret);
      }
      else
      {
        config_.is_strong_consistency_read = master_is_strong_consistent;
        config_mgr_.dump2file();
        config_mgr_.get_update_task().write2stat();
      }

      // send response message
      common::ObResultCode res;
      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
              out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = send_response(OB_SYNC_IS_STRONG_CONSISTENT_RESPONSE, MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "failed to send response, err=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "succ send response for change_paxos_num_response");
      }
      if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
      }
      return ret;
    }

    //add bingo [Paxos sstable info to rs log] 20170614:b
    int ObRootWorker::rs_dump_sstable_info(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, my_version=%d", version, MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS != root_server_.get_balancer()->print_sstable_info())
      {
        result_msg.result_code_ = err;
      }

      if (OB_SUCCESS != result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position()))
      {
         TBSYS_LOG(WARN, "failed to serialize result_msg to buf. err=%d", err);
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_DUMP_SSTABLE_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send response. err=%d", err);
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos table replica] 20170620:b
    int ObRootWorker::rt_get_table_replica_num(const int32_t version,
        common::ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      UNUSED(in_buff);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      int64_t table_id = 0;

      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, my_version=%d", version, MY_VERSION);
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
              in_buff.get_position(), (int64_t*)&table_id))
      {
        TBSYS_LOG(ERROR, "failed to deserialize");
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
        memset(replicas_nums, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
        ret = root_server_.get_balancer()->calculate_table_replica_num(table_id, replicas_nums);
        res.result_code_ = ret;
        if (OB_SUCCESS != (ret = res.serialize(out_buff.get_data(),
                out_buff.get_capacity(),
                out_buff.get_position())))
        {
          TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
        }
        else
        {
          bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
          root_server_.get_alive_cluster(is_cluster_alive);
          for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            int32_t replca_num = replicas_nums[cluster_id];
            if(replicas_nums[cluster_id] == 0 && !is_cluster_alive[cluster_id])
            {
              replca_num = -1; // no use cluster
            }
            if(OB_SUCCESS != (ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(),
                out_buff.get_position(), replca_num)))
            {
              TBSYS_LOG(ERROR, "serialize replica_num fail, ret: [%d]", ret);
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = send_response(OB_GET_TABLE_REPLICA_NUM_RESPONSE,
              MY_VERSION, out_buff, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }
      return ret;
    }
    //add:e
  }; // end namespace
}
