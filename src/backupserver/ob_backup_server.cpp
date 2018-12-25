/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <stdint.h>
#include <tblog.h>
#include "common/ob_trace_log.h"
#include "ob_backup_server.h"
#include "ob_backup_server_main.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_fill_log.h"
#include "ob_backup_callback.h"

#include "common/file_directory_utils.h"
#include "common/ob_log_dir_scanner.h"

#include "sql/ob_sql_result_set.h"

#include "common/ob_encrypted_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace backupserver
  {

    const char * ObBackupServer::select_backup_info_str = "select log_id_end from \"TANG\".backup_info where status = 'FINISH' and mode != 1 %s order by start_time desc limit 1";
    const char * ObBackupServer::update_backup_info_str = "REPLACE INTO \"TANG\".backup_info"
        "(start_time, finish_time, log_id_start, log_id_end, mode, status, frozen_version) "
        "VALUES(%ld,%ld,%ld,%ld,%d,'%s',%ld);";
    const char * ObBackupServer::check_backup_privilege_str = "SELECT pass_word, priv_all from %s where user_name = '%s'";
    const char * ObBackupServer::ups_commit_log_dir = "data/ups_commitlog";
    const char * ObBackupServer::rs_data_dir = "data/rs";
    const char * ObBackupServer::schema_ext = ".schema";

    ObBackupServer::ObBackupServer(ObChunkServerConfig &config,
                                   ObBackupServerConfig &backup_config)
      : config_(config),backup_config_(backup_config), file_service_(), file_client_(), file_client_rpc_buffer_(),
      response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
      rpc_buffer_(RPC_BUFFER_SIZE),
      min_sstable_id_(0),max_sstable_id_(0),
      min_log_id_(0),max_log_id_(0),
      backup_mgr_(NULL),backup_thread_(NULL),
      backup_mode_(MIN_BACKUP),backup_start_time_(0),backup_end_time_(0),
      rs_flag_(false),ups_flag_(false),cs_flag_(false),
      rs_ret_value_(OB_SUCCESS),ups_ret_value_(OB_SUCCESS),cs_ret_value_(OB_SUCCESS)
    {
    }

    ObBackupServer::~ObBackupServer()
    {
      if (backup_mgr_)
      {
        delete backup_mgr_;
        backup_mgr_ = NULL;
      }
     /* if (NULL != ups_fetch_thread_)
      {
        delete ups_fetch_thread_;
        ups_fetch_thread_ = NULL;
      }*/
      if (NULL != rs_fetch_thread_)
      {
        delete rs_fetch_thread_;
        rs_fetch_thread_ = NULL;
      }
      if (NULL != backup_thread_)
      {
        delete backup_thread_;
        backup_thread_ = NULL;
      }

    }

    common::ThreadSpecificBuffer::Buffer* ObBackupServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    common::ThreadSpecificBuffer::Buffer* ObBackupServer::get_response_buffer() const
    {
      return response_buffer_.get_buffer();
    }

    const common::ThreadSpecificBuffer* ObBackupServer::get_thread_specific_rpc_buffer() const
    {
      return &rpc_buffer_;
    }

    const common::ObServer& ObBackupServer::get_self() const
    {
      return self_;
    }

    const ObChunkServerConfig & ObBackupServer::get_config() const
    {
      return config_;
    }

    ObChunkServerConfig & ObBackupServer::get_config()
    {
      return config_;
    }

    const ObBackupServerConfig & ObBackupServer::get_backup_config() const
    {
      return backup_config_;
    }

    ObBackupServerConfig & ObBackupServer::get_backup_config()
    {
      return backup_config_;
    }

    ObSchemaManagerV2* ObBackupServer::get_schema_manager()
    {
      return & schema_mgr_;
    }


    const common::ObServer ObBackupServer::get_root_server() const
    {
      return config_.get_root_server();
    }

    const common::ObServer ObBackupServer::get_merge_server() const
    {
      return ObServer(ObServer::IPV4, config_.root_server_ip, static_cast<int32_t>(backup_config_.inner_port));
    }

    int ObBackupServer::set_self(const char* dev_name, const int32_t port)
    {
      int ret = OB_SUCCESS;
      int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip)
      {
        TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        bool res = self_.set_ipv4_addr(ip, port);
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
              dev_name, port);
          ret = OB_ERROR;
        }
      }
      return ret;
    }


    int ObBackupServer::initialize()
    {
      int ret = OB_SUCCESS;
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);

      // set listen port
      if (OB_SUCCESS == ret)
      {
        ret = set_listen_port((int)config_.port);
      }

      if (OB_SUCCESS == ret)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObBackupCallback::process;
        //server_handler_.batch_process = ObTbnetCallback::batch_process;
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_dev_name(config_.devname);
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS == ret)
        {
          ret = set_self(config_.devname, (int32_t)config_.port);
        }
      }
      if (OB_SUCCESS == ret)
      {
        set_self_to_thread_queue(self_);
      }

      // task queue and work thread count
      if (OB_SUCCESS == ret)
      {
        ret = set_default_queue_size((int)config_.task_queue_size);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_thread_count((int)config_.task_thread_count);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_io_thread_count((int)config_.io_thread_count);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_min_left_time(config_.task_left_time);
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_manager_.initialize(eio_, &server_handler_);
      }


      if (OB_SUCCESS == ret)
      {
        ret = tablet_manager_.init(&config_, &client_manager_);
      }


      // init file service
      if (OB_SUCCESS == ret)
      {
        int64_t max_migrate_task_count = get_config().max_migrate_task_count;
        ret = file_service_.initialize(this,
            &this->get_default_task_queue_thread(),
            config_.network_timeout,
            static_cast<uint32_t>(max_migrate_task_count));
      }

      if (OB_SUCCESS == ret)
      {
        ret = file_client_.initialize(&file_client_rpc_buffer_,
                                      &client_manager_, config_.migrate_band_limit_per_second);
      }

      if (NULL == (backup_mgr_ = new(std::nothrow) ObTabletBackupManager(schema_mgr_)))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_MEM_OVERFLOW;
      }
      else if (NULL == (backup_thread_ = new(std::nothrow) ObTabletBackupRunnable(
          config_, backup_mgr_)))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_MEM_OVERFLOW;
      }
      else if (NULL == (rs_fetch_thread_ = new(std::nothrow) ObBackupRootFetchRunnable()))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_MEM_OVERFLOW;
      }

      const char *store_root = backup_config_.store_root;
      const char *raid_regex = backup_config_.raid_regex;
      const char *dir_regex = backup_config_.dir_regex;
      ret = sstable_mgr_.init(store_root, raid_regex, dir_regex);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init sstable mgr, ret=%d", ret);
      }
      else if (!sstable_mgr_.load_new())
      {
        TBSYS_LOG(WARN, "sstable mgr load new fail");
        ret = OB_ERROR;
      }
      else if (NULL == (ups_fetch_thread_ = new(std::nothrow) ObBackupUpsFetchRunnable()))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_MEM_OVERFLOW;
      }

      // server initialize, including start transport,
      // listen port, accept socket data from client
      if (OB_SUCCESS == ret)
      {
        ret = backup_mgr_->initialize(this);
      }

      if (OB_SUCCESS == ret)
      {
        ret = ObSingleServer::initialize();
      }

      if (OB_SUCCESS == ret)
      {
        ret = service_.initialize(this);
      }

      return ret;
    }

    int ObBackupServer::start_service()
    {
      TBSYS_LOG(INFO, "start service...");
      int ret = OB_SUCCESS;

      if (!FileDirectoryUtils::is_directory(rs_data_dir))
      {
        TBSYS_LOG(ERROR, "the directory [%s] doesn't exist", rs_data_dir);
        ret = OB_ERROR;
      }
      else if (!FileDirectoryUtils::is_directory(ups_commit_log_dir))
      {
        TBSYS_LOG(ERROR, "the directory [%s] doesn't exist", ups_commit_log_dir);
        ret = OB_ERROR;
      }
      else
      {
        ObLogDirScanner scanner;
        ret = scanner.init(ups_commit_log_dir);
        if (OB_SUCCESS != ret)
        {
          if (OB_DISCONTINUOUS_LOG == ret)
          {
            TBSYS_LOG(WARN, "log_dir_scanner.init(%s): log file not continuous", ups_commit_log_dir);
          }
          else
          {
            TBSYS_LOG(ERROR, "log_dir_scanner.init(%s)=>%d", ups_commit_log_dir, ret);
          }
        }
        else
        {
          // finally, start service, handle the request from client.
          ret = service_.start();
        }
      }
      return ret;
    }

    void ObBackupServer::wait_for_queue()
    {
      ObSingleServer::wait_for_queue();
    }

    void ObBackupServer::destroy()
    {
      ObSingleServer::destroy();
      service_.destroy();
    }

    const ObTabletManager & ObBackupServer::get_tablet_manager() const
    {
      return tablet_manager_;
    }

    ObTabletManager & ObBackupServer::get_tablet_manager()
    {
      return tablet_manager_;
    }

    const ObTabletBackupManager * ObBackupServer::get_backup_manager() const
    {
      return backup_mgr_;
    }

    ObTabletBackupManager * ObBackupServer::get_backup_manager()
    {
      return backup_mgr_;
    }

    common::ObGeneralRpcStub & ObBackupServer::get_rpc_stub()
    {
      return rpc_stub_;
    }

    rootserver::ObRootRpcStub & ObBackupServer::get_root_rpc_stub()
    {
      return rt_rpc_stub_;
    }

    int64_t ObBackupServer::get_ups_frozen_version() const
    {
      return cur_frozen_version_;
    }
/*
    int64_t ObBackupServer::get_ups_max_log_id() const
    {
      return cur_max_log_id_;
    }
*/
    int64_t ObBackupServer::get_process_timeout_time(
        const int64_t receive_time, const int64_t network_timeout)
    {
      int64_t timeout_time  = 0;
      int64_t timeout       = network_timeout;

      if (network_timeout <= 0)
      {
        timeout = THE_BACKUP_SERVER.get_config().network_timeout;
      }

      timeout_time = receive_time + timeout;

      return timeout_time;
    }

    int ObBackupServer::do_request(ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->get_channel_id();
      int64_t receive_time = ob_packet->get_receive_ts();
      int64_t network_timeout = ob_packet->get_source_timeout();
      easy_request_t* req = ob_packet->get_request();
      ObDataBuffer* in_buffer = ob_packet->get_buffer();
      ThreadSpecificBuffer::Buffer* thread_buffer = response_buffer_.get_buffer();
      if (NULL == req || NULL == req->ms || NULL == req->ms->c)
      {
        TBSYS_LOG(ERROR, "req or req->ms or req->ms->c is NULL, should not reach here");
      }
      else if (NULL == in_buffer || NULL == thread_buffer)
      {
        TBSYS_LOG(ERROR, "in_buffer = %p or out_buffer=%p cannot be NULL.", 
            in_buffer, thread_buffer);
      }
      else
      {
        int64_t timeout_time = get_process_timeout_time(receive_time, network_timeout);
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
        TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p",
            packet_code, ob_packet);
        PFILL_ITEM_START(handle_request_time);
        ret = service_.do_request(receive_time, packet_code,
            version, channel_id, req,
            *in_buffer, out_buffer, timeout_time);
        PFILL_ITEM_END(handle_request_time);
        PFILL_CS_PRINT();
        PFILL_CLEAR_LOG();
      }
      return ret;
    }

    int ObBackupServer::init_backup_rpc()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        ret = rpc_stub_.init(&rpc_buffer_, &client_manager_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init rpc stub failed, err=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rt_rpc_stub_.init(&client_manager_, &rpc_buffer_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init rt rpc stub failed, err=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret =  ups_rpc_stub_.init(&client_manager_, &rpc_buffer_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init ups rpc stub failed, err=%d", ret);
        }
      }
      return ret;
    }


    int ObBackupServer::init_backup_env(int mode)
    {
      int ret = OB_SUCCESS;
      ObSchemaManagerV2 *newest_schema_mgr = NULL;
      int64_t timeout = 0;

      //fetch the latest schema
      if (OB_SUCCESS == ret)
      {
        newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
        if (NULL == newest_schema_mgr)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        timeout = config_.network_timeout;
        int64_t retry_times = 0;
        while ((ret == OB_SUCCESS) && !stoped_)
        {
          // fetch table schema for startup.
          ret = rpc_stub_.fetch_schema(timeout, get_root_server(),
                                          0, false, *newest_schema_mgr);
          if (OB_SUCCESS == ret || retry_times > config_.retry_times) break;
          usleep(RETRY_INTERVAL_TIME);
          TBSYS_LOG(INFO, "retry to fetch schema:retry_times[%ld]", retry_times ++);
        }
        if (ret != OB_SUCCESS && ret != OB_ALLOCATE_MEMORY_FAILED)
        {
          ret = OB_INNER_STAT_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        schema_mgr_ = *newest_schema_mgr;
      }

      if (NULL != newest_schema_mgr)
      {
        delete newest_schema_mgr;
        newest_schema_mgr = NULL;
      }


      //fetch the master ups, frozen_version
      if (ret == OB_SUCCESS)
      {
        int64_t retry_times = 0;
        while (!stoped_)
        {
          if (OB_SUCCESS != (ret = rpc_stub_.fetch_update_server(config_.network_timeout,get_root_server(), master_ups_)))
          {
            TBSYS_LOG(WARN, "failed to fetch master ups address :retry_times[%ld]", retry_times);
          }
          else if (OB_SUCCESS != (ret = rpc_stub_.get_last_frozen_memtable_version(config_.network_timeout,
                                                         master_ups_,cur_frozen_version_)))
          {
            TBSYS_LOG(WARN, "failed to fetch ups frozen_version :retry_times[%ld]", retry_times);
          }
          if (OB_SUCCESS == ret || retry_times > config_.retry_times) break;
          usleep(RETRY_INTERVAL_TIME);
          TBSYS_LOG(INFO, "retry to fetch ups frozen_version :retry_times[%ld]", retry_times ++);
        }
        if (ret != OB_SUCCESS)
        {
          ret = OB_INNER_STAT_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "succeed to fetch ups frozen_version :[%ld]", cur_frozen_version_);
        }
      }


      //check if all tablets have been merged if mode unequal to INCREMENTAL_BACKUP
      if (ret == OB_SUCCESS && mode != INCREMENTAL_BACKUP)
      {
        static const int32_t MY_VERSION = 1;
        const int buff_size = OB_MAX_PACKET_LENGTH;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);

        if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                  msgbuf.get_position(), cur_frozen_version_)))
        {
          TBSYS_LOG(WARN,"fail to encode tablet_version to buf. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client_manager_.send_request(
                                  get_root_server(),OB_RS_CHECK_TABLET_MERGED, MY_VERSION, config_.network_timeout, msgbuf)))
        {
          TBSYS_LOG(WARN,"failed to send request. ret=%d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            TBSYS_LOG(WARN,"failed to deserialize response code, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            TBSYS_LOG(WARN,"failed to check tablet version. ret=%d", ret);
          }
          else
          {
            int64_t is_merged = 0;
            if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                msgbuf.get_position(), &is_merged)))
            {
              TBSYS_LOG(WARN,"failed to deserialize is_merged, ret=%d", ret);
            }
            else if (0 == is_merged)
            {
              TBSYS_LOG(WARN,"OKay, check_version=%ld, already have some tablet not reach this version\n",cur_frozen_version_);
              ret = OB_INNER_STAT_ERROR;
            }
            else
            {
              TBSYS_LOG(WARN,"OKay, check_version=%ld, all tablet reach this version\n", cur_frozen_version_);
            }
          }
        }
      }

      if (ret == OB_SUCCESS)
      {
        updateserver::ObUpsFetchParam param;
        ret = rs_fetch_thread_->init(get_root_server(),rs_data_dir);
        ret = ups_fetch_thread_->init(master_ups_,ups_commit_log_dir,param,&sstable_mgr_,this);
      }
      return ret;
    }


    int ObBackupServer::start_backup_task(int mode)
    {
      int ret = OB_SUCCESS;
      if ( backup_mode_ != MIN_BACKUP)
      {
        ret = OB_EAGAIN;
      }
      else if (OB_SUCCESS != (ret = init_backup_env(mode))) //fetch schema, ups addr, frozen version
      {
        TBSYS_LOG(WARN, "failed to init backup env");
      }
      else if (OB_SUCCESS != (ret = init_ups_backup_info(mode))) // init log range according to inner table
      {
        TBSYS_LOG(WARN, "failed to init ups backup info");
      }
      else if (OB_SUCCESS != (ret = update_ups_backup_info(mode))) // update log range from ups
      {
        TBSYS_LOG(WARN, "failed to update ups backup info");
      }

      if ( ret == OB_SUCCESS)
      {
        backup_start_time_ = tbsys::CTimeUtil::getTime();
        backup_mode_ = mode;
        int64_t retry_times = 0;
        while (!stoped_)
        {
          // update inner backup table
          ret = update_inner_backup_table();
          if (OB_SUCCESS == ret || retry_times > config_.retry_times) break;
          usleep(RETRY_INTERVAL_TIME);
          TBSYS_LOG(INFO, "retry to update inner backup table:retry_times[%ld]", retry_times ++);
        }
        if( OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to update inner backup table");
        }
        else if (ret == OB_SUCCESS && mode == FULL_BACKUP)
        {
          if (ret == OB_SUCCESS)
          {
            rs_fetch_thread_->start();
            TBSYS_LOG(INFO, "backupserver rs_fetch_thread started");

            ret = rs_fetch_thread_->wait_fetch_done();
            if (ret == OB_SUCCESS)
            {
              rs_fetch_thread_->wait();
              TBSYS_LOG(INFO, "succeed to fetch rs log");
            }
            else
            {
              rs_fetch_thread_->wait();
              TBSYS_LOG(ERROR, "failed to fetch rs log");
            }
          }
        }
        report_sub_task_finish(OB_ROOTSERVER,ret);

        if (OB_SUCCESS == ret && mode != STATIC_BACKUP)
        {
          ups_fetch_thread_->start();
        }
        else
        {
          report_sub_task_finish(OB_UPDATESERVER,ret);
        }
      }
      return ret;
    }

    void ObBackupServer::abort_ups_backup_task()
    {
      rs_fetch_thread_->wait();
      ups_fetch_thread_->wait();
      TBSYS_LOG(INFO, "stop rs, ups backup thread");

      //report
      report_sub_task_finish(OB_UPDATESERVER,OB_CANCELED);

    }

    void ObBackupServer::start_threads()
    {
      backup_thread_->start();
    }

    void ObBackupServer::stop_threads()
    {
      if (NULL != backup_thread_)
      {
        backup_thread_->stop();
        backup_thread_->wakeup();
        backup_thread_->wait();
        TBSYS_LOG(INFO, "backup worker thread stopped");
      }
    }

    int ObBackupServer::init_ups_backup_info( int mode)
    {
      int ret = OB_SUCCESS;
      uint64_t max_log_id = 0;
      uint64_t max_sstable_id = 0;
      int64_t pos = 0;
      char sql_buf[1024] = {0};
      databuff_printf(sql_buf,1024,pos,select_backup_info_str,"");
      ObString sql_str;
      sql_str.assign_ptr(sql_buf,static_cast<common::ObString::obstr_size_t>(strlen(sql_buf)));
      ret = get_ups_fech_param(sql_str,mode,max_log_id, max_sstable_id);

      if ( ret == OB_SUCCESS && mode == INCREMENTAL_BACKUP)
      {
        uint64_t max_log_id_temp = 0;
        databuff_printf(sql_buf,1024,pos,select_backup_info_str," and mode = 0");
        ret = get_ups_fech_param(sql_str, mode, max_log_id_temp,max_sstable_id);

      }

      if (ret == OB_SUCCESS)
      {
        max_log_id_ = max_log_id;
        min_log_id_ = max_log_id;
        TBSYS_LOG(INFO, "succeed to renew ups fetch info, min_log_id= %ld, max_log_id =%ld ", min_log_id_, max_log_id_);
      }

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "failed to ini ups fetch info, ret=%d", ret);
      }
      return ret;
    }

    int ObBackupServer::get_ups_fech_param(common::ObString& sql_str, int mode, uint64_t& max_log_id, uint64_t& max_sstable_id)
    {
      int ret = OB_SUCCESS;
      max_log_id = 0;
      max_sstable_id = 0;
      char buf[OB_MAX_PACKET_LENGTH] = {0};
      ObDataBuffer msgbuf(buf,OB_MAX_PACKET_LENGTH);
      int32_t DEFAULT_VERSION = 1;
      int64_t session_id = 0;
      msgbuf.get_position() = 0;

      if (OB_SUCCESS != (ret = sql_str.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize sql, ret=%d", ret);
      }
      else if (OB_SUCCESS !=
          (ret = client_manager_.send_request(get_merge_server(), OB_SQL_EXECUTE, DEFAULT_VERSION, config_.network_timeout, msgbuf, session_id)))
      {
        TBSYS_LOG(WARN, "failed to send request sql %.*s to ms %s, ret=%d", sql_str.length(),sql_str.ptr(), to_cstring(get_merge_server()), ret);
      }
      else
      {
        bool fullfilled = true;
        //scanner should at almost contain one row
        ObRow row;
        const ObObj *cell = NULL;
        uint64_t tid = 0;
        uint64_t cid = 0;
        int64_t cell_idx = 0;

        int64_t log_id = 0;
        sql::ObSQLResultSet rs;
        do
        {
          msgbuf.get_position() = 0;
          if (OB_SUCCESS !=
              (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            TBSYS_LOG(WARN, "fail to deserialize result buffer, ret=%d", ret);
          }
          else if (OB_SUCCESS != rs.get_errno())
          {
            TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]", to_cstring(rs.get_sql_str()), rs.get_errno());
            ret = rs.get_errno();
            break;
          }
          else
          {
            ObNewScanner& scanner = rs.get_new_scanner();
            if (scanner.is_empty() && mode == INCREMENTAL_BACKUP)
            {
              TBSYS_LOG(ERROR, "failed to init ups backup info, at least backup successfully in full mode before, ret=%d", ret);
              ret = OB_ERROR;
            }
            else if (OB_SUCCESS != (ret = scanner.get_next_row(row)))
            {
              TBSYS_LOG(WARN, "failed to parse and update proxy list, ret=%d", ret);
              break;
            }
            else
            {
              cell_idx = 0;
              if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
              {
                TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = cell->get_int(log_id)))
              {
                TBSYS_LOG(WARN, "failed to get ip, ret=%d", ret);
              }
              else
              {
                max_log_id = static_cast<uint64_t>(log_id);
              }
            }
            rs.get_fullfilled(fullfilled);
            if (fullfilled || scanner.is_empty())
            {
              break;
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(ERROR, "backup info should not have more than one package");
            }
          }
        } while (OB_SUCCESS == ret);
      }
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObBackupServer::update_inner_backup_table(bool is_finish)
    {
      int ret = OB_SUCCESS;
      int DEFAULT_VERSION = 1;
      //(start_time, finish_time, log_id_start, log_id_end, mode, status, frozen_version)
      int64_t pos = 0;
      char sqlbuf[OB_MAX_PACKET_LENGTH] = {0};
      char buff[OB_MAX_PACKET_LENGTH] = {0};
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      char *saveptr = NULL;
      if (is_finish)
      {
        if (rs_ret_value_ == OB_SUCCESS && ups_ret_value_ == OB_SUCCESS && cs_ret_value_ == OB_SUCCESS)
        {
          databuff_printf(sqlbuf,OB_MAX_PACKET_LENGTH,pos, update_backup_info_str,backup_start_time_,backup_end_time_,
                      min_log_id_,max_log_id_,backup_mode_,"FINISH",cur_frozen_version_);
        }
        else
        {
          databuff_printf(sqlbuf,OB_MAX_PACKET_LENGTH,pos, update_backup_info_str,backup_start_time_,backup_end_time_,
                        min_log_id_,max_log_id_,backup_mode_,"ERROR",cur_frozen_version_);
        }
      }
      else
      {
        databuff_printf(sqlbuf,OB_MAX_PACKET_LENGTH,pos, update_backup_info_str,backup_start_time_,backup_end_time_,
                      min_log_id_,max_log_id_,backup_mode_,"",cur_frozen_version_);
      }

      const char *one = strtok_r(sqlbuf, "\n", &saveptr);


      while (OB_SUCCESS == ret && NULL != one)
      {
        msgbuf.get_position() = 0;

        ObString sqlstr = ObString::make_string(one);
        TBSYS_LOG(INFO, "update backup info SQL[%.*s]",sqlstr.length(),sqlstr.ptr());
        if (OB_SUCCESS != (ret = sqlstr.serialize(msgbuf.get_data(),
                                                  msgbuf.get_capacity(),
                                                  msgbuf.get_position())))
        {
          TBSYS_LOG(ERROR, "failed to serialize, err = [%d]", ret);
        }
        else if (OB_SUCCESS !=
                 (ret = client_manager_.send_request(get_merge_server(), OB_SQL_EXECUTE,
                                                               DEFAULT_VERSION, config_.network_timeout,
                                                               msgbuf)))
        {
          TBSYS_LOG(ERROR, "failed to send request, err = [%d]", ret);
        }
        else
        {
          sql::ObSQLResultSet rs;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS !=
              (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                    msgbuf.get_position())))
          {
            TBSYS_LOG(WARN,
                      "fail to deserialize result buffer, ret = [%d]\n", ret);
          }
          else if (OB_SUCCESS != rs.get_errno())
          {
            TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]",
                      one, rs.get_errno());
            ret = rs.get_errno();
          }
        }
        one = strtok_r(NULL, "\n", &saveptr);
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "update backup info stat successfully!");
      }
      return ret;
    }

    int ObBackupServer::report_sub_task_finish(ObRole role, int32_t retValue)
    {
      int ret = OB_SUCCESS;
      if (role == OB_ROOTSERVER && !rs_flag_)
      {
        rs_flag_ = true;
        rs_ret_value_ = retValue;
        backup_end_time_ = tbsys::CTimeUtil::getTime();
      }
      else if (role == OB_UPDATESERVER && !ups_flag_)
      {
        ups_flag_ = true;
        ups_ret_value_ = retValue;
        backup_end_time_ = tbsys::CTimeUtil::getTime();
      }
      else if (role == OB_CHUNKSERVER && !cs_flag_)
      {
        cs_flag_ = true;
        cs_ret_value_ = retValue;
        backup_end_time_ = tbsys::CTimeUtil::getTime();
      }
      if (rs_flag_ && ups_flag_ && cs_flag_)
      {
        int64_t retry_times = 0;
        while (!stoped_)
        {
          // update inner backup table
          ret = update_inner_backup_table(true);
          if (OB_SUCCESS == ret || retry_times > config_.retry_times) break;
          usleep(RETRY_INTERVAL_TIME);
          TBSYS_LOG(INFO, "retry to update inner backup table:retry_times[%ld]", retry_times ++);
        }
        if( OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to update inner backup table");
        }
        kill(getpid(),2); //finish the backup task, kill self
      }
      return ret;
    }

    int ObBackupServer::update_ups_backup_info(int mode)
    {
      int ret = OB_SUCCESS;
      updateserver::ObUpsFetchParam param;
      param.fetch_log_=true;
      param.min_log_id_= min_log_id_;
      param.max_log_id_= max_log_id_;
      param.fetch_sstable_ = false;

      ObServer update_server;
      updateserver::ObSlaveInfo slave_info;
      slave_info.min_sstable_id = min_sstable_id_;
      slave_info.max_sstable_id = max_sstable_id_;
      slave_info.self = get_self();
      if (OB_SUCCESS != (ret = rpc_stub_.fetch_update_server(config_.network_timeout,get_root_server(), update_server)))
      {
      TBSYS_LOG(WARN, "failed to fetch master update server addr");
      }
      else if (update_server != master_ups_)
      {

        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(WARN, " master update server has been switched");
      }
      else if (OB_SUCCESS != (ret = ups_rpc_stub_.backup_register(update_server,slave_info,param,config_.network_timeout)))
      {

        TBSYS_LOG(WARN, "failed to regist backup to master update server");
      }

      if (ret == OB_SUCCESS)
      {
        if (mode == INCREMENTAL_BACKUP )
        {
          param.min_log_id_ = __sync_add_and_fetch(&min_log_id_,1);
        }
        else if (mode == FULL_BACKUP)
        {
          min_log_id_ = param.min_log_id_;
        }
        else
        {
          min_log_id_ = param.min_log_id_ = 0;
          max_log_id_ = param.max_log_id_ = 0;
        }
        if (OB_SUCCESS != (ret = ups_fetch_thread_->set_fetch_param(param)))
        {
          TBSYS_LOG(WARN, "failed to init ups fetch thread");
        }
        else
        {
          max_log_id_ = param.max_log_id_;
          TBSYS_LOG(INFO, "ups_fetch_thread init success");
        }
      }
      return ret;
    }


    int ObBackupServer::check_backup_privilege(const char * username, const char * password, bool & result)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char sqlbuf[OB_MAX_PACKET_LENGTH] = {0};
      char buff[OB_MAX_PACKET_LENGTH] = {0};
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      databuff_printf(sqlbuf,OB_MAX_PACKET_LENGTH, pos, check_backup_privilege_str, OB_ALL_USER_TABLE_NAME, username);

      int32_t DEFAULT_VERSION = 1;
      int64_t session_id = 0;
      msgbuf.get_position() = 0;
      ObString sql_str;
      sql_str.assign_ptr(sqlbuf,static_cast<common::ObString::obstr_size_t>(strlen(sqlbuf)));

      char password_str[SCRAMBLE_LENGTH * 2] = {0};
      ObString sys_passwd;
      sys_passwd.assign_ptr(password_str,SCRAMBLE_LENGTH * 2);

      int64_t priv_all = 0;

      if (OB_SUCCESS != (ret = sql_str.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        TBSYS_LOG(WARN, "failed to serialize sql, ret=%d", ret);
      }
      else if (OB_SUCCESS !=
          (ret = client_manager_.send_request(get_merge_server(), OB_SQL_EXECUTE, DEFAULT_VERSION, config_.network_timeout, msgbuf, session_id)))
      {
        TBSYS_LOG(WARN, "failed to send request sql %.*s to ms %s, ret=%d", sql_str.length(),sql_str.ptr(), to_cstring(get_merge_server()), ret);
      }
      else
      {
        bool fullfilled = true;
        //scanner should at almost contain one row
        ObRow row;
        const ObObj *cell = NULL;
        uint64_t tid = 0;
        uint64_t cid = 0;
        int64_t cell_idx = 0;

        sql::ObSQLResultSet rs;
        do
        {
          msgbuf.get_position() = 0;
          if (OB_SUCCESS !=
              (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            TBSYS_LOG(WARN, "fail to deserialize result buffer, ret=%d", ret);
          }
          else if (OB_SUCCESS != rs.get_errno())
          {
            TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]", to_cstring(rs.get_sql_str()), rs.get_errno());
            ret = rs.get_errno();
            break;
          }
          else
          {
            ObNewScanner& scanner = rs.get_new_scanner();
            if (scanner.is_empty())
            {
              TBSYS_LOG(WARN, "username [%s] doesn't exist ",username);
              ret = OB_USER_NOT_EXIST;
            }
            else if (OB_SUCCESS != (ret = scanner.get_next_row(row)))
            {
              TBSYS_LOG(WARN, "failed to parse scanner, ret=%d", ret);
              break;
            }
            else
            {
              cell_idx = 0;
              if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
              {
                TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = cell->get_varchar(sys_passwd)))
              {
                TBSYS_LOG(WARN, "failed to get passwd, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
              {
                TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = cell->get_int(priv_all)))
              {
                TBSYS_LOG(WARN, "failed to get priv, ret=%d", ret);
              }
            }
            rs.get_fullfilled(fullfilled);
            if (fullfilled || scanner.is_empty())
            {
              break;
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(ERROR, "user info should not have more than one package");
            }
          }
        } while (OB_SUCCESS == ret);
      }
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }

      if (ret == OB_SUCCESS)
      {
        if (priv_all != 1)
        {
          TBSYS_LOG(WARN, "username [%s] don't have backup privilege",username);
          ret = OB_ERR_NO_PRIVILEGE;
        }
        else
        {
          //encrypt
          ObString passwd_str(static_cast<common::ObString::obstr_size_t>(strlen(password)),
                         static_cast<common::ObString::obstr_size_t>(strlen(password)), password);
          ObString encrypt_passwd;
          char converted_password[SCRAMBLE_LENGTH * 2 + 1];
          encrypt_passwd.assign_ptr(converted_password, SCRAMBLE_LENGTH * 2 + 1);
          ObEncryptedHelper::encrypt(encrypt_passwd, passwd_str);
          encrypt_passwd.assign_ptr(converted_password, SCRAMBLE_LENGTH * 2);

          if (sys_passwd == encrypt_passwd)
          {
            TBSYS_LOG(INFO, "username[%s],system password=[%.*s], provided password=[%.*s]", username, sys_passwd.length(), sys_passwd.ptr(), encrypt_passwd.length(), encrypt_passwd.ptr());
            result = true;
          }
          else
          {
            TBSYS_LOG(WARN, "username[%s],system password=[%.*s], provided password=[%.*s]", username, sys_passwd.length(), sys_passwd.ptr(), encrypt_passwd.length(), encrypt_passwd.ptr());
            ret = OB_ERR_WRONG_PASSWORD;
          }
        }
      }
      return ret;
    }
  } // end namespace backupserver
} // end namespace oceanbase

