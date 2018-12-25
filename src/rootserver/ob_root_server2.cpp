/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *
 *
 ================================================================*/
#include <new>
#include <string.h>
#include <cmath>
#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "common/ob_atomic.h"
#include "common/ob_version.h"
#include "common/utility.h"
#include "common/ob_rowkey_helper.h"
#include "common/ob_array.h"
#include "common/ob_table_id_name.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_inner_table_operator.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service_impl.h"
#include "common/ob_extra_tables_schema.h"
#include "common/utility.h"
#include "common/ob_schema_helper.h"
#include "common/ob_common_stat.h"
#include "common/ob_bypass_task_info.h"
#include "common/ob_rowkey.h"
#include "common/file_directory_utils.h"
#include "ob_root_server2.h"
#include "ob_root_worker.h"
#include "ob_root_stat_key.h"
#include "ob_root_util.h"
#include "ob_root_bootstrap.h"
#include "ob_root_ms_provider.h"
#include "ob_root_monitor_table.h"
#include "ob_rs_trigger_event_util.h"
#include "ob_root_ups_provider.h"
#include "ob_root_ddl_operator.h"
//add liumz, [secondary index static_index_build] 20150320:b
#include "common/ob_tablet_histogram.h"
#include "common/ob_tablet_histogram_meta.h"
//add:e
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

using oceanbase::common::databuff_printf;

namespace oceanbase
{
  namespace rootserver
  {
    const int WAIT_SECONDS = 1;
    const int RETURN_BACH_COUNT = 8;
    const int MAX_RETURN_BACH_ROW_COUNT = 1000;
    const int MIN_BALANCE_TOLERANCE = 1;

    const char* ROOT_1_PORT =   "1_port";
    const char* ROOT_1_MS_PORT = "1_ms_port";
    const char* ROOT_1_IPV6_1 = "1_ipv6_1";
    const char* ROOT_1_IPV6_2 = "1_ipv6_2";
    const char* ROOT_1_IPV6_3 = "1_ipv6_3";
    const char* ROOT_1_IPV6_4 = "1_ipv6_4";
    const char* ROOT_1_IPV4   = "1_ipv4";
    const char* ROOT_1_TABLET_VERSION= "1_tablet_version";

    const char* ROOT_2_PORT =   "2_port";
    const char* ROOT_2_MS_PORT = "2_ms_port";
    const char* ROOT_2_IPV6_1 = "2_ipv6_1";
    const char* ROOT_2_IPV6_2 = "2_ipv6_2";
    const char* ROOT_2_IPV6_3 = "2_ipv6_3";
    const char* ROOT_2_IPV6_4 = "2_ipv6_4";
    const char* ROOT_2_IPV4   = "2_ipv4";
    const char* ROOT_2_TABLET_VERSION= "2_tablet_version";

    const char* ROOT_3_PORT =   "3_port";
    const char* ROOT_3_MS_PORT = "3_ms_port";
    const char* ROOT_3_IPV6_1 = "3_ipv6_1";
    const char* ROOT_3_IPV6_2 = "3_ipv6_2";
    const char* ROOT_3_IPV6_3 = "3_ipv6_3";
    const char* ROOT_3_IPV6_4 = "3_ipv6_4";
    const char* ROOT_3_IPV4   = "3_ipv4";
    const char* ROOT_3_TABLET_VERSION= "3_tablet_version";

    //add zhaoqiong[roottable tablet management]20160104:b
    const char* ROOT_4_PORT =   "4_port";
    const char* ROOT_4_MS_PORT = "4_ms_port";
    const char* ROOT_4_IPV6_1 = "4_ipv6_1";
    const char* ROOT_4_IPV6_2 = "4_ipv6_2";
    const char* ROOT_4_IPV6_3 = "4_ipv6_3";
    const char* ROOT_4_IPV6_4 = "4_ipv6_4";
    const char* ROOT_4_IPV4   = "4_ipv4";
    const char* ROOT_4_TABLET_VERSION= "4_tablet_version";

    const char* ROOT_5_PORT =   "5_port";
    const char* ROOT_5_MS_PORT = "5_ms_port";
    const char* ROOT_5_IPV6_1 = "5_ipv6_1";
    const char* ROOT_5_IPV6_2 = "5_ipv6_2";
    const char* ROOT_5_IPV6_3 = "5_ipv6_3";
    const char* ROOT_5_IPV6_4 = "5_ipv6_4";
    const char* ROOT_5_IPV4   = "5_ipv4";
    const char* ROOT_5_TABLET_VERSION= "5_tablet_version";

    const char* ROOT_6_PORT =   "6_port";
    const char* ROOT_6_MS_PORT = "6_ms_port";
    const char* ROOT_6_IPV6_1 = "6_ipv6_1";
    const char* ROOT_6_IPV6_2 = "6_ipv6_2";
    const char* ROOT_6_IPV6_3 = "6_ipv6_3";
    const char* ROOT_6_IPV6_4 = "6_ipv6_4";
    const char* ROOT_6_IPV4   = "6_ipv4";
    const char* ROOT_6_TABLET_VERSION= "6_tablet_version";
    //add:e

    const char* ROOT_OCCUPY_SIZE = "occupy_size";
    const char* ROOT_RECORD_COUNT = "record_count";

    char max_row_key[oceanbase::common::OB_MAX_ROW_KEY_LENGTH];

    const int NO_REPORTING = 0;
    const int START_REPORTING = 1;
    const int WAIT_REPORT = 3;
  }
}

ObBootState::ObBootState():state_(OB_BOOT_NO_META)
{
}

bool ObBootState::is_boot_ok() const
{
  return (state_ == OB_BOOT_OK);
}

void ObBootState::set_boot_ok()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_OK;
}

ObBootState::State ObBootState::get_boot_state() const
{
  return state_;
}

void ObBootState::set_boot_strap()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_STRAP;
}

void ObBootState::set_boot_recover()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_RECOVER;
}

bool ObBootState::can_boot_strap() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

bool ObBootState::can_boot_recover() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

const char* ObBootState::to_cstring() const
{
  ObSpinLockGuard guard(lock_);
  const char* ret = "UNKNOWN";
  switch(state_)
  {
    case OB_BOOT_NO_META:
      ret = "BOOT_NO_META";
      break;
    case OB_BOOT_OK:
      ret = "BOOT_OK";
      break;
    case OB_BOOT_STRAP:
      ret = "BOOT_STRAP";
      break;
    case OB_BOOT_RECOVER:
      ret = "BOOT_RECOVER";
      break;
    default:
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
const char* ObRootServer2::ROOT_TABLE_EXT = "rtable";
const char* ObRootServer2::CHUNKSERVER_LIST_EXT = "clist";
const char* ObRootServer2::LOAD_DATA_EXT = "load_data";
const char* ObRootServer2::SCHEMA_FILE_NAME = "./schema_bypass.ini";
const char* ObRootServer2::TMP_SCHEMA_LOCATION = "./tmp_schema.ini";

ObRootServer2::ObRootServer2(ObRootServerConfig &config)
  : config_(config),
    worker_(NULL),
    log_worker_(NULL),
    root_table_(NULL), tablet_manager_(NULL),
    //hist_table_(NULL), hist_manager_(NULL),//del liumz, [paxos static index]20170626
    have_inited_(false),
    first_cs_had_registed_(false), receive_stop_(false),
    last_frozen_mem_version_(OB_INVALID_VERSION), last_frozen_time_(1),
    time_stamp_changing_(-1),
    ups_manager_(NULL), ups_heartbeat_thread_(NULL),
    ups_check_thread_(NULL),
    //add pangtianze [Paxos rs_election] 20150623:b
    rs_node_mgr_(NULL),
    rs_heartbeat_thread_(NULL),
    rs_election_checker_(NULL),
    //add:e
    //add chujiajia [Paxos rs_election] 20151225:b
    count_rs_receive_quorum_scale_(0),
    //add:e
    balancer_(NULL), balancer_thread_(NULL),
    restart_server_(NULL),
    schema_timestamp_(0),
    privilege_timestamp_(0),
    schema_service_(NULL),
    schema_service_scan_helper_(NULL),
    schema_service_ms_provider_(NULL),
    schema_service_ups_provider_(NULL),
    first_meta_(NULL),
    rt_service_(NULL),
    merge_checker_(this),
    heart_beat_checker_(this),
    ms_provider_(server_manager_),
    local_schema_manager_(NULL),
    schema_manager_for_cache_(NULL)
{
  //add liumz, [paxos static index]20170626:b
  for (int i = 0; i< OB_MAX_COPY_COUNT; i++)
  {
    hist_manager_[i] = NULL;
    hist_table_[i] = NULL;
  }
  //add:e
}

ObRootServer2::~ObRootServer2()
{
  if (local_schema_manager_)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, local_schema_manager_);
    local_schema_manager_ = NULL;
  }
  if (schema_manager_for_cache_)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager_for_cache_);
    schema_manager_for_cache_ = NULL;
  }
  if (root_table_)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_);
    root_table_ = NULL;
  }
  if (tablet_manager_)
  {
    OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
    tablet_manager_ = NULL;
  }
  //mod liumz, [paxos static index]20170606:b
  /*//add liumz, [secondary index static_index_build] 20150320:b
  if (hist_table_)
  {
    OB_DELETE(ObTabletHistogramTable, ObModIds::OB_STATIC_INDEX_HISTOGRAM, hist_table_);
  }
  if (hist_manager_)
  {
    OB_DELETE(ObTabletHistogramManager, ObModIds::OB_STATIC_INDEX_HISTOGRAM, hist_manager_);
  }
  //add:e*/
  if (hist_table_)
  {
    destroy_hist_tables(hist_table_, OB_MAX_COPY_COUNT);
  }
  if (hist_manager_)
  {
    destroy_hist_managers(hist_manager_, OB_MAX_COPY_COUNT);
  }
  //mod:e
  if (NULL != ups_manager_)
  {
    delete ups_manager_;
    ups_manager_ = NULL;
  }
  if (NULL != ups_heartbeat_thread_)
  {
    delete ups_heartbeat_thread_;
    ups_heartbeat_thread_ = NULL;
  }
  if (NULL != ups_check_thread_)
  {
    delete ups_check_thread_;
    ups_check_thread_ = NULL;
  }
  //add pangtianze [Paxos rs_election] 20150623:b
  if (NULL != rs_node_mgr_)
  {
    delete rs_node_mgr_;
    rs_node_mgr_ = NULL;
  }
  if (NULL != rs_heartbeat_thread_)
  {
    delete rs_heartbeat_thread_;
    rs_heartbeat_thread_ = NULL;
  }
  if (NULL != rs_election_checker_)
  {
    delete rs_election_checker_;
    rs_election_checker_ = NULL;
  }
  //add:e
  if (NULL != balancer_thread_)
  {
    delete balancer_thread_;
    balancer_thread_ = NULL;
  }
  if (NULL != balancer_)
  {
    delete balancer_;
    balancer_ = NULL;
  }
  if (NULL != first_meta_)
  {
    delete first_meta_;
    first_meta_ = NULL;
  }
  if (NULL != schema_service_)
  {
    delete schema_service_;
    schema_service_ = NULL;
  }
  if (NULL != rt_service_)
  {
    delete rt_service_;
    rt_service_ = NULL;
  }
  if (NULL != restart_server_)
  {
    delete restart_server_;
    restart_server_ = NULL;
  }
  have_inited_ = false;
}

//mod chujiajia[Paxos rs_election]20151016:b
//const ObChunkServerManager& ObRootServer2::get_server_manager() const
ObChunkServerManager& ObRootServer2::get_server_manager()
//mod:e
{
  return server_manager_;
}

ObRootAsyncTaskQueue * ObRootServer2::get_task_queue()
{
  return &seq_task_queue_;
}

ObFirstTabletEntryMeta* ObRootServer2::get_first_meta()
{
  return first_meta_;
}

//add liumz, [secondary index static_index_build] 20150601:b
//only used within check_report_hist_info_done(), lock only once!!!
int ObRootServer2::get_rt_tablet_info(const int32_t meta_index, const ObTabletInfo *&tablet_info) const
{
  int ret = OB_SUCCESS;
  tablet_info = NULL;
  //tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL == root_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  else if (NULL == (tablet_info = root_table_->get_tablet_info(meta_index)))
  {
    ret = OB_INVALID_DATA;
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150320:b
int ObRootServer2::get_root_meta(const common::ObTabletInfo &tablet_info, ObRootTable2::const_iterator &meta)
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    ret = root_table_->get_root_meta(tablet_info, meta);
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  return ret;
}

int ObRootServer2::get_root_meta(const int32_t meta_index, ObRootTable2::const_iterator &meta)
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    ret = root_table_->get_root_meta(meta_index, meta);
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  return ret;
}

//add:e

//add liumz, [secondary index static_index_build] 20150601:b
int ObRootServer2::get_root_meta_index(const common::ObTabletInfo &tablet_info, int32_t &meta_index)
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    ret = root_table_->get_root_meta_index(tablet_info, meta_index);
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  return ret;
}
//add:e

ObConfigManager* ObRootServer2::get_config_mgr()
{
  ObConfigManager *config_mgr_ = NULL;
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "worker is NULL!");
  }
  else
  {
    config_mgr_ = &worker_->get_config_mgr();
  }
  return config_mgr_;
}

bool ObRootServer2::init(const int64_t now, ObRootWorker* worker)
{
  bool res = false;
  UNUSED(now);
  if (NULL == worker)
  {
    TBSYS_LOG(ERROR, "worker=NULL");
  }
  else if (have_inited_)
  {
    TBSYS_LOG(ERROR, "already inited");
  }
  else
  {
    time(&start_time_);
    worker_ = worker;
    log_worker_ = worker_->get_log_manager()->get_log_worker();
    //mod pangtianze [Paxos rs_election] 20150717:b
    //obi_role_.set_role(ObiRole::INIT); // init as init instance
    obi_role_.set_role(ObiRole::MASTER); // init as init instance
    //mod:e
    worker_->get_config_mgr().got_version(0);
    timer_.init();
    operation_helper_.init(this, &config_, &worker_->get_rpc_stub(), &server_manager_);
    schema_timestamp_ = 0;
    TBSYS_LOG(INFO, "init schema_version=%ld", schema_timestamp_);
    res = false;
    tbsys::CConfig config;
    int err = OB_SUCCESS;
    if (NULL == (schema_manager_for_cache_ = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, 0)))
    {
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    if (NULL == (local_schema_manager_ = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, now)))
    {
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    else if (!local_schema_manager_->parse_from_file(config_.schema_filename, config))
    {
      TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
      res = false;
    }
    else if (NULL == (tablet_manager_ = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER)))
    {
      TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
    }
    else if (NULL == (root_table_ = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager_)))
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
    }
    //mod liumz, [paxos static index]20170606:b
    /*//add liumz, [secondary index static_index_build] 20150320:b
    else if (NULL == (hist_manager_ = OB_NEW(ObTabletHistogramManager,  ObModIds::OB_STATIC_INDEX_HISTOGRAM))) {
      TBSYS_LOG(ERROR, "new ObTabletHistogramManager error");
    }
    else if (NULL == (hist_table_ = OB_NEW(ObTabletHistogramTable, ObModIds::OB_STATIC_INDEX_HISTOGRAM, this, hist_manager_)))
    {
      TBSYS_LOG(ERROR, "new ObTabletHistogramTable error");
    }
    //add:e*/
    else if (OB_SUCCESS != create_hist_managers(hist_manager_, OB_MAX_COPY_COUNT)) {
      TBSYS_LOG(ERROR, "new ObTabletHistogramManager error");
    }
    else if (OB_SUCCESS != create_hist_tables(hist_table_, OB_MAX_COPY_COUNT))
    {
      TBSYS_LOG(ERROR, "new ObTabletHistogramTable error");
    }
    //mod:e
    else if (NULL == (ups_manager_
                      = new(std::nothrow) ObUpsManager(worker_->get_rpc_stub(),
                                                       worker_, config_.network_timeout,
                                                       config_.ups_lease_time,
                                                       config_.ups_lease_reserved_time,
                                                       config_.ups_waiting_register_time,
                                                       //mod zhaoqiong [Schema Manager] 20150327:b
                                                       //obi_role_, schema_timestamp_,
                                                       obi_role_, schema_manager_for_cache_->get_timestamp(),
                                                       //mod:e
                                                       worker_->get_config_mgr().get_version()
                                                       //add pangtianze [Paxos rs_election] 20150820:b
                                                       , config_.ups_quorum_scale)))
                                                       //add:e
{
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (ups_heartbeat_thread_ = new(std::nothrow) ObUpsHeartbeatRunnable(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (ups_check_thread_ = new(std::nothrow) ObUpsCheckRunnable(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }

    //add pangtianze [Paxos rs_election] 20150623:b
    else if (NULL == &worker_->get_root_server())
    {
      TBSYS_LOG(ERROR, "root_server_ is null");
    }
    else if (NULL == (rs_node_mgr_ = new(std::nothrow) ObRootElectionNodeMgr(&worker_->get_root_server(),
                                                                             worker_->get_role_manager(),
                                                                             worker_->get_rpc_stub(),
                                                                             config_.network_timeout,
                                                                             config_.no_hb_response_duration_time,
                                                                             config_.lease_interval_time,
                                                                             config_.leader_lease_interval_time,
                                                                             config_.rs_paxos_number,
                                                                             worker_->get_config_mgr().get_version())))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (rs_election_checker_ = new(std::nothrow) ObRsCheckRunnable(*rs_node_mgr_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (rs_heartbeat_thread_ = new(std::nothrow) ObRsHeartbeatRunnable(*rs_node_mgr_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    //add:e

    else if (NULL == (first_meta_
                      = new(std::nothrow) ObFirstTabletEntryMeta((int32_t)config_.read_queue_size,
                                                                 get_file_path(config_.rs_data_dir,
                                                                               config_.first_meta_filename))))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ms_provider_ = new(std::nothrow) ObSchemaServiceMsProvider(server_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ups_provider_ = new(std::nothrow) ObSchemaServiceUpsProvider(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_scan_helper_ = new(std::nothrow) ObScanHelperImpl()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ = new(std::nothrow) ObSchemaServiceImpl()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (OB_SUCCESS != (err = schema_service_->init(schema_service_scan_helper_, false)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", err);
    }
    else if (NULL == (rt_service_ = new(std::nothrow) ObRootTableService(*first_meta_, *schema_service_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (balancer_ = new(std::nothrow) ObRootBalancer()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (balancer_thread_ = new(std::nothrow) ObRootBalancerRunnable(
        config_, *balancer_, *worker_->get_role_manager())))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (restart_server_ = new(std::nothrow) ObRestartServer()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else
    {
      // task queue init max task count
      seq_task_queue_.init(10 * 1024);
      ups_manager_->set_async_queue(&seq_task_queue_);
      //add pangtianze [Paxos rs_election] 20150715:b
      rs_node_mgr_->set_async_queue(&seq_task_queue_);
      //add:e
      ddl_tool_.init(this, schema_service_);

      TBSYS_LOG(INFO, "new root table created, root_table_=%p", root_table_);
      TBSYS_LOG(INFO, "tablet_manager=%p", tablet_manager_);
      //add liumz, [secondary index static_index_build] 20150320:b
      TBSYS_LOG(INFO, "new hist table created, hist_table_=%p", hist_table_);
      TBSYS_LOG(INFO, "hist_manager_=%p", hist_manager_);
      //add:e
      TBSYS_LOG(INFO, "first_meta=%p", first_meta_);
      TBSYS_LOG(INFO, "schema_service=%p", schema_service_);
      TBSYS_LOG(INFO, "rt_service=%p", rt_service_);
      TBSYS_LOG(INFO, "balancer=%p", balancer_);
      TBSYS_LOG(INFO, "balancer_thread=%p", balancer_thread_);
      TBSYS_LOG(INFO, "ups_manager=%p", ups_manager_);
      TBSYS_LOG(INFO, "ups_heartbeat_thread=%p", ups_heartbeat_thread_);
      TBSYS_LOG(INFO, "ups_check_thread=%p", ups_check_thread_);
      //add pangtianze [Paxos rs_election] 20150623:b
      TBSYS_LOG(INFO, "rs_node_mgr_=%p", rs_node_mgr_);
      TBSYS_LOG(INFO, "rs_election_checker_=%p", rs_election_checker_);
      TBSYS_LOG(INFO, "rs_heartbeat_thread_=%p", rs_heartbeat_thread_);
      //add:e
      ups_manager_->set_ups_config((int32_t)config_.read_master_master_ups_percent
                                   //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                   //,(int32_t)config_.read_slave_master_ups_percent
                                   //del:e
                                   );
      config_.get_root_server(my_addr_);
      //add pangtianze [Paxos rs_election] 20150713:b
      rs_node_mgr_->set_local_node_addr(worker_->get_self_rs());
      //add:e
      // init client helper
      client_helper_.init(worker_->get_client_manager(),
          worker_->get_thread_buffer(), worker_->get_rs_master(),
          worker_->get_network_timeout());
      // init components for balancer
      balancer_->set_config(&config_);
      balancer_->set_root_server(this);
      balancer_->set_root_table(root_table_);
      balancer_->set_server_manager(&server_manager_);
      balancer_->set_root_table_lock(&root_table_rwlock_);
      balancer_->set_root_table_build_mutex(&root_table_build_mutex_);
      balancer_->set_server_manager_lock(&server_manager_rwlock_);
      balancer_->set_rpc_stub(&worker_->get_rpc_stub());
      balancer_->set_log_worker(log_worker_);
      balancer_->set_role_mgr(worker_->get_role_manager());
      balancer_->set_ups_manager(ups_manager_);
      balancer_->set_restart_server(restart_server_);
      balancer_->set_boot_state(&boot_state_);
      balancer_->set_ddl_operation_mutex(&mutex_lock_);
      //add lbzhong [Paxos Cluster.Balance] 201607014:b
      cluster_mgr_.set_schema_service(schema_service_);
      balancer_->set_cluster_manager(&cluster_mgr_);
      balancer_->set_cluster_manager_lock(&cluster_mgr_rwlock_);
      //add:e
      // init schema_service_scan_helper
      schema_service_scan_helper_->set_ms_provider(schema_service_ms_provider_);
      schema_service_scan_helper_->set_ups_provider(schema_service_ups_provider_);
      schema_service_scan_helper_->set_rpc_stub(&worker_->get_rpc_stub());
      schema_service_scan_helper_->set_scan_timeout(config_.inner_table_network_timeout);
      schema_service_scan_helper_->set_mutate_timeout(config_.inner_table_network_timeout);
      schema_service_scan_helper_->set_scan_retry_times(3); // @todo config
      // init restart_server_
      restart_server_->set_root_config(&config_);
      restart_server_->set_server_manager(&server_manager_);
      TBSYS_LOG(INFO, "root_table_ address 0x%p", root_table_);
      restart_server_->set_root_table2(root_table_);
      restart_server_->set_root_rpc_stub(&worker_->get_rpc_stub());
      restart_server_->set_root_log_worker(log_worker_);
      restart_server_->set_root_table_build_mutex(&root_table_build_mutex_);
      restart_server_->set_server_manager_rwlock(&server_manager_rwlock_);
      restart_server_->set_root_table_rwlock(&root_table_rwlock_);
      ms_provider_.init(config_, worker_->get_rpc_stub()
                        //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                        , *this
                        //add:e
                        );
      // init root trigger
      root_trigger_.set_ms_provider(&ms_provider_);
      root_trigger_.set_rpc_stub(&worker_->get_general_rpc_stub());
      res = true;
      have_inited_ = res;
    }
  }
  return res;
}

void ObRootServer2::after_switch_to_master()
{
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  worker_->get_config_mgr().got_version(timestamp);
  privilege_timestamp_ = timestamp;
  //report is needed, maybe slave was doing clean roottable before switching
  request_cs_report_tablet();
  TBSYS_LOG(INFO, "[NOTICE] start service now");
}

int ObRootServer2::init_boot_state()
{
  int ret = OB_SUCCESS;
  ret = first_meta_->init();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to init the first meta, err=%d", ret);
  }
  if (OB_INIT_TWICE == ret || OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "rootserver alreay bootstrap, set boot ok.");
    boot_state_.set_boot_ok();
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRootServer2::start_master_rootserver()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "start master rootserver");

  if (OB_SUCCESS == ret)
  {
    request_cs_report_tablet();
    // after restart master root server refresh the schema
    static const int64_t RETRY_TIMES = 3;
    for (int64_t i = 0; i < RETRY_TIMES; ++i)
    {
      ret = worker_->schedule_after_restart_task(1000000, false);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fail to schedule after_restart task, ret=%d", ret);
      }
      else
      {
        break;
      }
    }
  }
  privilege_timestamp_ = tbsys::CTimeUtil::getTime();
  TBSYS_LOG(INFO, "set privilege timestamp to %ld", privilege_timestamp_);
  return ret;
}

void ObRootServer2::start_threads()
{
  balancer_thread_->start();
  heart_beat_checker_.start();
  ups_heartbeat_thread_->start();
  ups_check_thread_->start();
}

//add pangtiaze [Paxos rs_election] 20150624
void ObRootServer2::start_rs_heartbeat()
{
    rs_heartbeat_thread_->start();
    TBSYS_LOG(INFO, "start rootserver heartbeat");
}
void ObRootServer2::start_election_checker()
{
    rs_election_checker_->start();
    TBSYS_LOG(INFO, "start rootserver election checker");
}
int ObRootServer2::set_leader_when_starting()
{
  int ret = OB_ERROR;
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "worker is NULL!");
  }
  else if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    //add chujiajia [Paxos rs_election] 20151027:b
    rs_node_mgr_->set_as_leader(my_addr_);
    //add bingo [Paxos Cluster.Balance] 20161020:b
    set_master_cluster_id(static_cast<int32_t>(config_.cluster_id));
    //add:e
    TBSYS_LOG(INFO, "rootserver is set leader by user, rs=%s", to_cstring(my_addr_));
    ret = OB_SUCCESS;
  }
  return ret;
}
void ObRootServer2::set_rt_master(const ObServer &rs_master)
{
  //del chujiajia [Paxos rs_election] 20160104:b
  //my_addr_ = rs_master;
  //del:e
  worker_->set_rt_master(rs_master);
  worker_->get_ms_list_task().set_rs(rs_master);
  client_helper_.set_root_server(rs_master);
}
//add:e

//add wangdonghui [paxos daily merge] 20170510 :b
void ObRootServer2::set_last_frozen_time(const int64_t last_frozen_time)
{
    last_frozen_time_ = last_frozen_time;
}

//add :e

//add jinty [Paxos Cluster.Balance] 20160708:b
void ObRootServer2::init_schema_service_scan_helper(common::ObLocalMsList *ms_list_for_rs_slave)
{
  schema_service_scan_helper_->set_ms_provider(ms_list_for_rs_slave);
}
//add e
//add pangtianze [Paxos rs_election] 20150629:b
int ObRootServer2::slave_init_first_meta(const ObTabletMetaTableRow &first_meta_row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rs_node_mgr_->slave_init_first_meta(first_meta_row)))
  {
    TBSYS_LOG(ERROR, "init first meta in slave rootserver error");
  }
  return ret;
}
int ObRootServer2::get_rs_master(common::ObServer &rs)
{
  int ret = OB_SUCCESS;
  if (NULL == worker_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "worker is NULL!");
  }
  else
  {
    rs = worker_->get_rs_master();
  }
  return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20150701:b
int ObRootServer2::force_leader_broadcast()
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->leader_broadcast(true);
    if (ret == OB_SUCCESS)
    {
      TBSYS_LOG(INFO, "successed to send first leader broadcast");
    }
  }
  return ret;
}
bool ObRootServer2::check_most_rs_registered()
{
  int ret = false;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->check_most_rs_alive();
  }
  return ret;
}
int ObRootServer2::get_rs_leader(common::ObServer &rs)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->get_leader(rs);
  }
  return ret;
}
//add:e

//add chujiajia [Paxos rs_election] 20151210:b
int ObRootServer2::add_rootserver_register_info(const common::ObServer &rs)

{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->add_rs_register_info(rs);
  }
  return ret;
}

//add:e

//add pangtianze [Paxos rs_election] 20150731:b
int ObRootServer2::update_rs_array(const common::ObServer *array, const int32_t count)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    //add chujiajia [Paxos rs_election] 20151111:b
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "rs_node_mgr is NULL");
    //add:e
  }
  else if (NULL != array)
  {
    if(OB_SUCCESS != (ret = rs_node_mgr_->update_rs_array(array, count)))
    {
      TBSYS_LOG(WARN, "update rs_array err, err = %d", ret);
    }
  }
  else
  {
    //mod chujiajia [Paxos rs_election] 20151111:b
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "rs_array is NULL");
    //mod:e
  }  
  return ret;
}
int ObRootServer2::set_election_priority(const common::ObServer target_rs, const int32_t priority)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->set_rs_priority(target_rs, priority);
  }
  return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20150813:b
ObRootWorker* ObRootServer2::get_worker()
{
  return worker_;
}
//add:e

void ObRootServer2::start_merge_check()
{
  merge_checker_.start();
  TBSYS_LOG(INFO, "start merge check");
}

//add liumz, [paxos static index]20170626:b
int ObRootServer2::create_hist_managers(ObTabletHistogramManager** managers, const int64_t size)
{
  int ret = OB_SUCCESS;
  char* ptr = NULL;

  if (NULL == managers || 0 >= size)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (ptr = reinterpret_cast<char*>(ob_malloc(sizeof(ObTabletHistogramManager) * size, ObModIds::OB_STATIC_INDEX_HISTOGRAM))))
  {
    TBSYS_LOG(WARN, "allocate memrory error.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    for (int64_t i = 0 ; i < size; ++i)
    {
      ObTabletHistogramManager* manager = new (ptr + i * sizeof(ObTabletHistogramManager)) ObTabletHistogramManager();
      if (NULL == manager )
      {
        TBSYS_LOG(WARN, "new hist manager error.");
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      }
      else
      {
        managers[i] = manager;
      }
    }
  }
  return ret;
}

int ObRootServer2::create_hist_tables(ObTabletHistogramTable** tables, const int64_t size)
{
  int ret = OB_SUCCESS;
  char* ptr = NULL;

  if (NULL == tables || 0 >= size)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (ptr = reinterpret_cast<char*>(ob_malloc(sizeof(ObTabletHistogramTable) * size, ObModIds::OB_STATIC_INDEX_HISTOGRAM))))
  {
    TBSYS_LOG(WARN, "allocate memrory error.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    for (int64_t i = 0 ; i < size; ++i)
    {
      if (NULL == hist_manager_ || NULL == hist_manager_[i])
      {
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      ObTabletHistogramTable* table = new (ptr + i * sizeof(ObTabletHistogramTable)) ObTabletHistogramTable(this, hist_manager_[i]);
      if (NULL == table )
      {
        TBSYS_LOG(WARN, "new hist table error.");
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      }
      else
      {
        tables[i] = table;
      }
    }
  }
  return ret;
}

int ObRootServer2::destroy_hist_managers(ObTabletHistogramManager** managers, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == managers || 0 >= size)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    for (int64_t i = 0 ; i < size; ++i)
    {
      ObTabletHistogramManager* manager = managers[i];
      if (NULL != manager)
      {
        manager->~ObTabletHistogramManager();
      }
    }
    void * ptr = managers[0];
    ob_free(ptr);
    memset(managers, 0, size * sizeof(ObTabletHistogramManager*));
  }
  return ret;
}

int ObRootServer2::destroy_hist_tables(ObTabletHistogramTable** tables, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == tables || 0 >= size)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    for (int64_t i = 0 ; i < size; ++i)
    {
      ObTabletHistogramTable* table = tables[i];
      if (NULL != table)
      {
        table->~ObTabletHistogramTable();
      }
    }
    void * ptr = tables[0];
    ob_free(ptr);
    memset(tables, 0, size * sizeof(ObTabletHistogramTable*));
  }
  return ret;
}
//add:e

//add pangtianze [Paxos rs_election] 20150619:b
void ObRootServer2::stop_merge_check()
{
  merge_checker_.stop();
  merge_checker_.wait();
  TBSYS_LOG(INFO, "merge checker thread stopped");
}
//add:e

//mod chujiajia[Paxos rs_election]20151016:b
//const ObRootServerConfig& ObRootServer2::get_config() const
ObRootServerConfig& ObRootServer2::get_config() const
//mod:e
{
  return config_;
}

void ObRootServer2::stop_threads()
{
  receive_stop_ = true;
  if (NULL != balancer_)
  {
    balancer_->set_stop();
    TBSYS_LOG(INFO, "set balancer stop flag");
  }
  heart_beat_checker_.stop();
  heart_beat_checker_.wait();
  TBSYS_LOG(INFO, "cs heartbeat thread stopped");
  if (NULL != balancer_thread_)
  {
    balancer_thread_->stop();
    balancer_thread_->wakeup();
    balancer_thread_->wait();
    TBSYS_LOG(INFO, "balance worker thread stopped");
  }
  merge_checker_.stop();
  merge_checker_.wait();
  TBSYS_LOG(INFO, "merge checker thread stopped");
  if (NULL != ups_heartbeat_thread_)
  {
    ups_heartbeat_thread_->stop();
    ups_heartbeat_thread_->wait();
    TBSYS_LOG(INFO, "ups heartbeat thread stopped");
  }
  if (NULL != ups_check_thread_)
  {
    ups_check_thread_->stop();
    ups_check_thread_->wait();
    TBSYS_LOG(INFO, "ups check thread stopped");
  }
  //add pangtianze [Paxos rs_election] 20150619:b
  if (NULL != rs_heartbeat_thread_)
  {
    rs_heartbeat_thread_->stop();
    rs_heartbeat_thread_->wait();
    TBSYS_LOG(INFO, "ups heartbeat thread stopped");
  }
  if (NULL != rs_election_checker_)
  {
    rs_election_checker_->stop();
    rs_election_checker_->wait();
    TBSYS_LOG(INFO, "ups heartbeat thread stopped");
  }
  //add:e
}

int ObRootServer2::boot_recover()
{
  int ret = OB_SUCCESS;
  if (!boot_state_.can_boot_recover())
  {
    TBSYS_LOG(WARN, "boot strap state cann't be recover. boot_state=%s", boot_state_.to_cstring());
    ret = OB_ERROR;
  }
  else
  {
    boot_state_.set_boot_recover();
    ObBootstrap bootstrap(*this);
    while (!receive_stop_)
    {
      if (OB_SUCCESS != (ret = request_cs_report_tablet()))
      {
        TBSYS_LOG(WARN, "fail to request cs to report tablet. err=%d", ret);
      }
      else
      {
        usleep(1 * 1000 * 1000);
        //get tablet info from root_table
        ObNewRange key_range;
        key_range.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
        key_range.set_whole_range();
        ObRootTable2::const_iterator first;
        ObRootTable2::const_iterator last;
        ret = root_table_->find_range(key_range, first, last);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "cann't find first_tablet_entry's tablet. ret=%d", ret);
        }
        else
        {
          common::ObTabletInfo *tablet_info = root_table_->get_tablet_info(first);
          if (first == last
              && tablet_info != NULL
              && key_range.equal(tablet_info->range_))
          {
            //write meta_file
            ObArray<ObServer> create_cs;
		    //add zhaoqiong[roottable tablet management]20150302:b
			//for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
            for (int i = 0; i < OB_MAX_COPY_COUNT; ++i)
			//add e
            {
              int64_t server_index = first->server_info_indexes_[i];
              if (OB_INVALID_INDEX != server_index)
              {
                ObServer cs = server_manager_.get_cs(static_cast<int32_t>(server_index));
                create_cs.push_back(cs);
              }
              else
              {
                break;
              }
            }
            if (OB_SUCCESS != (bootstrap.init_meta_file(create_cs)))
            {
              TBSYS_LOG(WARN, "fail to recover first_meta.bin. ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "recover first_meta.bin success.");
              break;
            }
          }
          else if (config_.is_ups_flow_control)
          {
            ups_manager_->set_ups_config((int32_t)config_.read_master_master_ups_percent
                                         //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                         //,(int32_t)config_.read_slave_master_ups_percent
                                         //del:e
                                         );
          }
          else
          {
            TBSYS_LOG(WARN, "cann't find first_tablet_entry's tablet. ret=%d", ret);
          }
        } //end else
      }//end
    }//end while
  }//end else
  return ret;
}

int ObRootServer2::renew_core_schema(void)
{
  int ret = OB_ERROR;
  const static int64_t retry_times = 3;
  for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
  {
    if (OB_SUCCESS != (ret = notify_switch_schema(true)))
    {
      TBSYS_LOG(WARN, "fail to notify_switch_schema, retry_time=%ld, ret=%d", i, ret);
      sleep(1);
    }
  }
  return ret;
}

//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::renew_user_schema(int64_t & count)
int ObRootServer2::renew_user_schema(int64_t & count, int64_t version)
//mod:e
{
  int ret = OB_ERROR;
  const static int64_t retry_times = 3;
  for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
  {
    //mod zhaoqiong [Schema Manager] 20150327:b
    //if (OB_SUCCESS != (ret = refresh_new_schema(count)))
    {// BLOCK LOCK
      tbsys::CThreadGuard guard(&mutex_lock_);
      ret = refresh_new_schema(count, version);
    }
    // mod:e
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to refresh schema, retry_time=%ld, ret=%d", i, ret);
      sleep(1);
    }
    else
    {
      TBSYS_LOG(INFO, "refresh schema success. count=%ld", count);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = OB_ERROR;
    for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify_switch_schema, retry_time=%ld, ret=%d", i, ret);
        sleep(1);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema success.");
      }
    }
  }
  return ret;
}

int ObRootServer2::boot_strap(void)
{
  int ret = OB_ERROR;
  //add pangtianze [Paxos rs_election] 20161101:b
  int ret2 = OB_ERROR;
  //add:e
  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() start");
  ObBootstrap bootstrap(*this);
  if (!boot_state_.can_boot_strap())
  {
    TBSYS_LOG(WARN, "cannot bootstrap twice, boot_state=%s", boot_state_.to_cstring());
    ret = OB_INIT_TWICE;
  }
  //add pangtianze [Paxos rs_election] 20161101:b
  else if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager_ is null");
    ret = OB_ERROR;
  }
  //add:e
  else
  {
    boot_state_.set_boot_strap();
    bootstrap.set_log_worker(log_worker_);
    int64_t mem_version = -1;
    // step 1. wait for master ups election ok
    while (!receive_stop_)
    {
      ret = fetch_mem_version(mem_version);
      //add pangtianze [Paxos rs_election] 20161101:b
      if (ups_manager_->get_active_ups_count_lock() == config_.ups_quorum_scale)
      {
          ret2 = OB_SUCCESS;
      }
      //add:e
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get mem frozen version failed");
        sleep(2);
      }
      //add pangtianze [Paxos rs_election] 20161101:b
      else if (ret2 != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "not enough ups online when bootstrap");
        sleep(1);
      }
      //add:e
      else
      {
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = do_bootstrap(bootstrap)))
      {
        TBSYS_LOG(ERROR, "bootstrap failed! ret: [%d]", ret);
      }
    }
    //add pangtianze [Paxos rs_election] 20170301:b
    if (OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = sync_refresh_rs_list()))
        {
           TBSYS_LOG(ERROR, "sync refresh rs list to all servers when bootstrap");
        }
    }
    //add:e
  }

  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() end");
  return ret;
}

int ObRootServer2::do_bootstrap(ObBootstrap & bootstrap)
{
  int ret = OB_SUCCESS;
  //add lbzhong [Paxos Cluster.Balance] 20160708:b
  //step 1. init cluster replica num
  if (OB_SUCCESS == ret)
  {
    // for create system tables
    bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
    //mod bingo [Paxos Cluster.Balance] 20161118:b
    get_alive_cluster_with_cs(is_cluster_alive);
    //mod:e
    tbsys::CRLockGuard guard(cluster_mgr_rwlock_);
    cluster_mgr_.init_cluster_tablet_replicas_num(is_cluster_alive);
    //add pangtianze [Paxos Cluster.Balance] 20170620:b
    int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
    memset(replicas_nums, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
    if(OB_SUCCESS != (ret = get_cluster_tablet_replicas_num(replicas_nums)))
    {
      TBSYS_LOG(INFO, "fail to get cluster replicas num, ret=%d", ret);
    }
    else
    {
        char value_buf[OB_MAX_CONFIG_VALUE_LEN];
        int pos = 0;
        memset(value_buf, 0, sizeof(value_buf));
        sprintf(value_buf, "%d", replicas_nums[1]);
        pos += 1;
        for(int cluster_id = 2; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
        {
            sprintf(value_buf + pos , ";%d", replicas_nums[cluster_id]);
            pos += 2;
        }
        char config_str[100];
        memset(config_str, 0 , sizeof(config_str));
        int cnt = snprintf(config_str, sizeof(config_str), "cluster_replica_num=%s", value_buf);
        ObString str;
        str.assign_ptr(config_str, cnt);
        if (OB_SUCCESS != (ret = config_.add_extra_config(config_str, true)))
        {
          TBSYS_LOG(ERROR, "Set cluster_replica_num config failed! ret: [%d]", ret);
        }
        else
        {
            get_config_mgr()->dump2file();
            get_config_mgr()->get_update_task().write2stat();
        }
    }
    //add:e
  }
  //add:e
  // step 2. create core tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_core_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap core tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap create core tables succ");
      ret = renew_core_schema();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "renew schema error after core sys tables, err = %d", ret);
      }
    }
  }
  // step 3. create sys tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_sys_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap create other sys tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap create other sys tables succ");
    }
  }
  // step 4. create schema.ini tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_ini_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap create schmea.ini tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "create schema.ini tables ok, all tables created");
    }
  }
  // step 5. renew schema manager and notify the servers
  if (OB_SUCCESS == ret)
  {
    int64_t table_count = 0;
    ret = renew_user_schema(table_count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "renew schema error after create sys tables, err = %d", ret);
    }
    else if (table_count != bootstrap.get_table_count())
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "check renew schema table count failed:right[%ld], cur[%ld]",
                bootstrap.get_table_count(), table_count);
    }
    else
    {
      boot_state_.set_boot_ok();
      TBSYS_LOG(INFO, "bootstrap set boot ok, all initial tables created");
    }
  }
  // step 6. for schema ini tables report
  if (OB_SUCCESS == ret)
  {
    static const int retry_times = 3;
    ret = OB_ERROR;
    int i = 0;
    while (OB_SUCCESS != ret && !receive_stop_)
    {
      if (OB_SUCCESS != (ret = request_cs_report_tablet()))
      {
        TBSYS_LOG(ERROR, "fail to request cs report tablet. retry_time=%d, ret=%d", i, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "let chunkservers report tablets");
      }
      i ++;
      if (i >= retry_times) break;
    }
  }
  // step 7. init sys table content
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = after_boot_strap(bootstrap)))
    {
      TBSYS_LOG(ERROR, "execute after boot strap fail, ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap successfully!");
    }
  }
  /* check if async task queue empty */
  if (OB_SUCCESS == ret)
  {
    ret = OB_NOT_INIT;
    for (int i = 0; i < 100; i++) /* 10s */
    {
      if (async_task_queue_empty())
      {
        ret = OB_SUCCESS;
        break;
      }
      usleep(100000);
      TBSYS_LOG(DEBUG, "waiting async task queue empty");
    }
  }
  return ret;
}

int ObRootServer2::after_boot_strap(ObBootstrap & bootstrap)
{
  int ret = OB_SUCCESS;

  //add zhaoqiong [Schema Manager] 20150507:b
  int64_t retry_times = 3;
  const static int64_t sleep_time = config_.network_timeout;

  while (OB_SUCCESS != (ret = bootstrap.init_system_table()) && ((retry_times--) > 0))
  {
    usleep(static_cast<useconds_t>(sleep_time));
  }

  if (OB_SUCCESS != ret)

  //if (OB_SUCCESS != (ret = bootstrap.init_system_table()))
  {
    TBSYS_LOG(ERROR, "fail to init all_sys_param and all_sys_stat table. err=%d", ret);
  }
  //add:e
  else
  {
    // wait fetch ups list
    usleep(1000 * 1000);
    // force push the mem version to mergeserves
    int err = force_heartbeat_all_servers();
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "force heartbeat all servers failed:err[%d]", err);
    }
  }
  const ObiRole &role = get_obi_role();
  if ((OB_SUCCESS == ret) && (role.get_role() == ObiRole::MASTER))
  {
    ret = ObRootTriggerUtil::slave_boot_strap(root_trigger_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "write trigger table for slave cluster bootstrap failed, ret:%d", ret);
    }
    else
    {
      //TBSYS_LOG(INFO, "[TRIGGER] notify slave to do boot strap succ");
      TBSYS_LOG(INFO, "write to all_sys/all_param succ");
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "[NOTICE] start service now:role[%s]", role.get_role_str());
  }
  return ret;
}

int ObRootServer2::slave_boot_strap()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() start");
  ObBootstrap bootstrap(*this);
  if (!boot_state_.can_boot_strap())
  {
    TBSYS_LOG(WARN, "cannot bootstrap twice, boot_state=%s", boot_state_.to_cstring());
    ret = OB_INIT_TWICE;
  }
  else
  {
    boot_state_.set_boot_strap();
    bootstrap.set_log_worker(log_worker_);
    if (OB_SUCCESS != (ret = do_bootstrap(bootstrap)))
    {
      TBSYS_LOG(ERROR, "Slave bootstrap failed! ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "slave cluster bootstrap succ");
    }
  }
  return ret;
}

int ObRootServer2::after_restart()
{
  TBSYS_LOG(INFO, "fresh schema after restart.");
  int64_t table_count = 0;
  int ret = renew_user_schema(table_count);
  int err = ret;
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to renew user schema try again. err=%d", ret);
    static const int64_t RETRY_TIMES = 3;
    for (int64_t i = 0; i < RETRY_TIMES; ++i)
    {
      ret = worker_->schedule_after_restart_task(1000000, false);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fail to schedule after restart task.ret=%d", ret);
        sleep(1);
      }
      else
      {
        ret = err;
        break;
      }
    }
  }
  else
  {
    // renew schema version and config version succ
    // start service right now
    worker_->get_config_mgr().got_version(tbsys::CTimeUtil::getTime());
    TBSYS_LOG(INFO, "after restart renew schema succ:count[%ld]", table_count);
    TBSYS_LOG(INFO, "[NOTICE] start service now");

    //fix bug: update rootserver info to __all_server
    ObServer rs;
    //mod pangtianze [Paxos rs_election] 20150731:b
    //config_.get_root_server(rs);
    get_rs_master(rs);
    //mod:e
    config_.get_root_server(rs);
    char server_version[OB_SERVER_VERSION_LENGTH] = "";
    get_package_and_svn(server_version, sizeof(server_version));
    commit_task(SERVER_ONLINE, OB_ROOTSERVER, rs, 0, server_version);
  }
  return ret;
}

int ObRootServer2::start_notify_switch_schema()
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  ObServer ms_server;
  //mod pangtianze [Paxos rs_election] 20150630:b
  //master_rs.set_ipv4_addr(config_.master_root_server_ip, (int32_t)config_.master_root_server_port);
   get_rs_master(master_rs);
  //mod:e
  TBSYS_LOG(INFO, "start_notify_switch_schema");

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "this rs is not master of marster cluster, cannot start_notify_switch_schema");
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    const int64_t timeout = 30 * 1000L * 1000L; // 30s
    int tmp_ret = rpc_stub.notify_switch_schema(master_rs, timeout);
    if (OB_SUCCESS == tmp_ret)
    {
      TBSYS_LOG(INFO, "succeed  to notify_switch_schema, master_rs=%s", to_cstring(master_rs));
    }
    else
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to notify_switch_schema, master_rs=%s,ret=%d",
          to_cstring(master_rs), tmp_ret);
    }

    for (int64_t i = 0; i < slave_count; ++i)
    {
      tmp_ret = rpc_stub.notify_switch_schema(slave_rs[i], timeout);
      if (OB_SUCCESS == tmp_ret)
      {
        TBSYS_LOG(INFO, "succeed to notify_switch_schema, slave_rs[%ld]=%s", i, to_cstring(slave_rs[i]));
      }
      else
      {
        ret = tmp_ret;
        TBSYS_LOG(WARN, "failed to notify_switch_schema, slave_rs[%ld]=%s, ret=%d", i, to_cstring(slave_rs[i]), ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "succeed to notify_switch_schema");
  }
  else
  {
    TBSYS_LOG(ERROR, "failed to notify_switch_schema, ret=%d",ret);
  }

  return ret;
}

int ObRootServer2::notify_switch_schema(bool only_core_tables, bool force_update)
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 *out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (OB_SUCCESS != (ret = get_schema(force_update, only_core_tables, *out_schema)))
  {
    TBSYS_LOG(WARN, "fail to get schema. err = %d", ret);
  }
  else
  {
    if (obi_role_.get_role() == ObiRole::MASTER)
    {
      ObUps ups_master;
      if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
      {
        TBSYS_LOG(WARN, "fail to get ups master. err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().switch_schema(ups_master.addr_,
              *out_schema, config_.network_timeout)))
      {
        TBSYS_LOG(ERROR, "fail to switch schema to ups. ups: [%s] err=%d",
                  to_cstring(ups_master.addr_), ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify ups to switch schema succ! version = %ld only_core_tables = %s",
            out_schema->get_version(), (only_core_tables == true) ? "YES" : "NO");
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = force_sync_schema_all_servers(*out_schema)))
      {
        TBSYS_LOG(WARN, "fail to sync schema to ms and cs:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify cs/ms to switch schema succ! version = %ld only_core_tables = %s",
            out_schema->get_version(), (only_core_tables == true) ? "YES" : "NO");
      }
    }
    //add zhaoqiong [Schema Manager] 20150514:b
    if (OB_SUCCESS != ret)
    {
      ret = OB_SCHEMA_SYNC_ERROR;
    }
    //add:e
  }
  if (NULL != out_schema)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}

ObBootState* ObRootServer2::get_boot()
{
  return &boot_state_;
}
ObBootState::State ObRootServer2::get_boot_state()const
{
  return boot_state_.get_boot_state();
}

ObRootRpcStub& ObRootServer2::get_rpc_stub()
{
  return worker_->get_rpc_stub();
}

ObSchemaManagerV2* ObRootServer2::get_ini_schema() const
{
  return local_schema_manager_;
}

//add pangtianze [Paxos rs_election] 20150708:b
ObRootElectionNodeMgr* ObRootServer2::get_rs_node_mgr() const
{
  return rs_node_mgr_;
}
//add:e

//add zhaoqiong [Schema Manager] 20150327:b
int ObRootServer2::notify_switch_schema(ObSchemaMutator &schema_mutator)
{
  int ret = OB_SUCCESS;
  if (obi_role_.get_role() == ObiRole::MASTER)
  {
    ObUps ups_master;
    if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
    {
      TBSYS_LOG(WARN, "fail to get ups master. err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().switch_schema_mutator(ups_master.addr_,
            schema_mutator, config_.network_timeout)))
    {
      TBSYS_LOG(ERROR, "fail to switch schema mutator to ups. ups: [%s] err=%d",
                to_cstring(ups_master.addr_), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify ups to switch schema mutator succ! version = %ld",
          schema_mutator.get_end_version());
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = force_sync_schema_mutator_all_servers(schema_mutator)))
    {
      TBSYS_LOG(WARN, "fail to sync schema mutator to ms and cs:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify cs/ms to switch schema mutator succ! start version = %ld, end version = %ld",
          schema_mutator.get_start_version(),schema_mutator.get_end_version());
    }
  }
  if (OB_SUCCESS != ret)
  {
    ret = OB_SCHEMA_SYNC_ERROR;
  }
  return ret;
}

int ObRootServer2::get_schema_mutator(ObSchemaMutator& schema_mutator, int64_t& table_count)
{
  int ret = OB_SUCCESS;
  //mod liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
  //ObSchemaManagerV2 new_schema;
  common::ObSchemaManagerV2* new_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  //mod:e
  int64_t start_version = -1;
  int64_t end_version = -1;
  //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
  if (NULL == new_schema)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }//add:e
  else if (NULL == schema_manager_for_cache_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "should not be here, ret = %d", ret);
  }
  else if (schema_manager_for_cache_->get_version() >= schema_timestamp_)
  {

    //do nothing, no mutator
    ret = OB_INNER_STAT_ERROR;
    TBSYS_LOG(ERROR, "should not be here, ret = %d, schema_version[%ld], schema_timestamp_[%ld]",
              ret, schema_manager_for_cache_->get_version(), schema_timestamp_);
  }

  if (OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    start_version = schema_manager_for_cache_->get_version();
    end_version = schema_timestamp_;
    if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = schema_service_->fetch_schema_mutator(start_version,end_version,schema_mutator)))
    {
      TBSYS_LOG(WARN, "failed to fetch_schema_mutator, err=%d", ret);
    }
    else if (!schema_mutator.get_refresh_schema())
    {
      *new_schema = *schema_manager_for_cache_;
      if (OB_SUCCESS != (ret = new_schema->apply_schema_mutator(schema_mutator)))
      {
        TBSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                  schema_mutator.get_start_version(), schema_mutator.get_end_version());
      }
      else
      {
        table_count = new_schema->get_table_count();
        if (table_count <= 0)
        {
          ret = OB_INNER_STAT_ERROR;
          TBSYS_LOG(WARN, "check schema table count less than 4:version[%ld], count[%ld]",
            new_schema->get_version(), table_count);
        }
        else
        {
          *schema_manager_for_cache_ = *new_schema;
          TBSYS_LOG(TRACE, "==========print schema version[%ld] start==========",new_schema->get_version());
          new_schema->print_debug_info();
          TBSYS_LOG(TRACE, "==========print schema version[%ld] end==========",new_schema->get_version());
        }
      }
    }
    else
    {
      ret = get_schema(true, false, *new_schema);
    }
  }
  //add liumz, [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
  if (new_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, new_schema);
  }
  //add:e
  return ret;
}
//add:e
int ObRootServer2::get_schema(const bool force_update, bool only_core_tables, ObSchemaManagerV2& out_schema)
{
  int ret = OB_SUCCESS;
  bool cache_found = false;
  if (config_.enable_cache_schema && !only_core_tables)
  {
    if (!force_update)
    {
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      out_schema = *schema_manager_for_cache_;
      cache_found = true;
    }
  }
  if (false == cache_found)
  {
    TBSYS_LOG(INFO, "get schema begin, force_update=%c only_core_tables=%c "
            "curr_version=%ld cache_found=%c",
            force_update?'Y':'N', only_core_tables?'Y':'N',
            schema_timestamp_, cache_found?'Y':'N');
    //avoid schema_timestamp_ changed while get schema from system table
    int64_t cur_schema_timestamp = schema_timestamp_;//add zhaoqiong [Schema Manager] 20150327
    ObTableIdNameIterator tables_id_name;
    ObTableIdName* tid_name = NULL;
    int32_t table_count = 0;
    ObArray<TableSchema> table_schema_array;
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else
    {
      schema_service_->init(schema_service_scan_helper_, only_core_tables);
      if (OB_SUCCESS != (ret = tables_id_name.init(schema_service_scan_helper_, only_core_tables)))
      {
        TBSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
      }
      else
      {
        while (OB_SUCCESS == ret
               && OB_SUCCESS == (ret = tables_id_name.next()))
        {
          TableSchema table_schema;
          if (OB_SUCCESS != (ret = tables_id_name.get(&tid_name)))
          {
            TBSYS_LOG(WARN, "failed to get next name, err=%d", ret);
          }
          else
          {
            //modify dolphin [database manager]@20150616:b
            //if (OB_SUCCESS != (ret = schema_service_->get_table_schema(/**modify dolphin [datbase manager]@20150613 tid_name->table_name_*/dt, table_schema)))
            if (OB_SUCCESS != (ret = schema_service_->get_table_schema(tid_name->table_name_, table_schema,tid_name->dbname_)))
            //modify:e
            {
              TBSYS_LOG(WARN, "failed to get table schema, err=%d, table_name=%.*s", ret,
                  tid_name->table_name_.length(), tid_name->table_name_.ptr());
              ret = OB_INNER_STAT_ERROR;
            }
            else
            {
              // @todo DEBUG
              TBSYS_LOG(INFO,"table_schema db_name is:%s,table name is:%s",table_schema.dbname_, table_schema.table_name_);
              table_schema_array.push_back(table_schema);
              TBSYS_LOG(DEBUG, "get table schema add into shemaManager, tname=%.*s",
                  tid_name->table_name_.length(), tid_name->table_name_.ptr());
              ++table_count;
            }
          }
        } // end while
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,  "failed to get all table schema, only_core_tables=%s, ret=%d",
              only_core_tables ? "true" : "false", ret);
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = out_schema.add_new_table_schema(table_schema_array)))
          {
            TBSYS_LOG(WARN, "failed to add table schema into the schema manager, err=%d", ret);
          }
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        //rs restart, should get latest timestamp from __all_ddl_operation
        if (OB_SUCCESS == ret && !only_core_tables)
        {
          if (0 == schema_timestamp_ && out_schema.get_table_count() > CORE_TABLE_COUNT)
          {
            int64_t schema_timestamp = 0;
            if (OB_SUCCESS != (ret = tables_id_name.get_latest_schema_version(schema_timestamp)))
            {
              TBSYS_LOG(WARN, "failed to get latest_schema_version, err=%d", ret);
            }
            else if (schema_timestamp > 0)
            {
              schema_timestamp_ = schema_timestamp;
              schema_service_->set_schema_version(schema_timestamp);
              if (OB_SUCCESS != (ret = renew_schema_version()))
              {
                TBSYS_LOG(WARN, "failed to renew schema version, err=%d", ret);
              }
              else
              {
                cur_schema_timestamp = schema_timestamp_;
              }
            }
          }
        }
        //add:e
        if (OB_SUCCESS == ret)
        {
          if (!only_core_tables)
          {
            tbsys::CWLockGuard guard(schema_manager_rwlock_);
            //mod zhaoqiong [Schema Manager] 20150327:b
            //schema_timestamp_ use __all_ddl_operation values
            //schema_timestamp_ = tbsys::CTimeUtil::getTime();
            //out_schema.set_version(schema_timestamp_);
            out_schema.set_version(cur_schema_timestamp);
            //mod:e
            if (OB_SUCCESS != (ret = out_schema.sort_column()))
            {
              TBSYS_LOG(WARN, "failed to sort columns in schema manager, err=%d", ret);
            }
             //add wenghaixing [secondary index drop table_with_index]20150121
            //init index hash when rootserver refresh new version schema
            else if(OB_SUCCESS != (ret = out_schema.init_index_hash()))
            {
              TBSYS_LOG(WARN, "failed to init_index_hash");
            }
            //add e
            else if (OB_SUCCESS != (ret = switch_schema_manager(out_schema)))
            {
              TBSYS_LOG(WARN, "fail to switch schema. ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "get schema succ. table count=%d, version[%ld]", table_count, schema_timestamp_);
            }
          }
          else
          {
            out_schema.set_version(CORE_SCHEMA_VERSION);
          }
        }
      }
    }
  }
  //
  if (ret == OB_SUCCESS)
  {
    if (out_schema.get_table_count() <= 0)
    {
      ret = OB_INNER_STAT_ERROR;
	  //mod zhaoqiong [Schema Manager] 20150327:b
//      TBSYS_LOG(WARN, "check schema table count less than 3:core[%d], version[%ld], count[%ld]",
//        only_core_tables, out_schema.get_version(), out_schema.get_table_count());
      TBSYS_LOG(WARN, "check schema table count less than 4:core[%d], version[%ld], count[%ld]",
        only_core_tables, out_schema.get_version(), out_schema.get_table_count());
		//mod:e
    }
  }
  return ret;
}

int64_t ObRootServer2::get_last_frozen_version() const
{
  return last_frozen_mem_version_;
}

int ObRootServer2::switch_schema_manager(const ObSchemaManagerV2 & schema_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == schema_manager_for_cache_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. schema_manager_for_cache_=%p", schema_manager_for_cache_);
  }
  //mod zhaoqiong [Schema Manager] 20150327:b
  //core tables num is 4, __all_ddl_operation
  //else if (boot_state_.is_boot_ok() && schema_manager.get_table_count() <= 3)
  else if (boot_state_.is_boot_ok() && schema_manager.get_table_count() <= CORE_TABLE_COUNT)
  //mod:e
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "schema verison:%ld", schema_manager.get_version());
    schema_manager.print_info();
    TBSYS_LOG(ERROR, "check new schema table count failed:count[%ld]", schema_manager.get_table_count());
  }
  else
  {
    *schema_manager_for_cache_ = schema_manager;
    //add zhaoqiong [Schema Manager] 20150327:b
    TBSYS_LOG(TRACE, "==========print schema version[%ld] start==========",schema_manager.get_version());
    schema_manager.print_debug_info();
    TBSYS_LOG(TRACE, "==========print schema version[%ld] end==========",schema_manager.get_version());
    //add:e
  }
  return ret;
}

int ObRootServer2::get_table_id_name(ObTableIdNameIterator *table_id_name, bool& only_core_tables)
{
  int ret = OB_SUCCESS;
  ObRootMsProvider ms_provider(server_manager_);
  ms_provider.init(config_, worker_->get_rpc_stub()
                   //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                   , *this
                   //add:e
                   );
  ObUps ups_master;
  ups_manager_->get_ups_master(ups_master);
  ObRootUpsProvider ups_provider(ups_master.addr_);
  ObScanHelperImpl scan_helper;
  scan_helper.set_ms_provider(&ms_provider);
  scan_helper.set_rpc_stub(&worker_->get_rpc_stub());
  scan_helper.set_ups_provider(&ups_provider);
  scan_helper.set_scan_timeout(config_.inner_table_network_timeout);
  scan_helper.set_mutate_timeout(config_.inner_table_network_timeout);
  only_core_tables = false;
  if (ObBootState::OB_BOOT_OK != boot_state_.get_boot_state())
  {
    TBSYS_LOG(INFO, "rs is not bootstarp. fetch core_schema only");
    only_core_tables = true;
  }
  if (OB_SUCCESS != (ret = table_id_name->init(&scan_helper, only_core_tables)))
  {
    TBSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
  }
  return ret;
}

int ObRootServer2::get_table_schema(const uint64_t table_id, TableSchema &table_schema)
{
  ObString table_name;
  int ret = schema_service_->get_table_name(table_id, table_name);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table_name. err=%d, table_id=%lu", ret, table_id);
  }
  else
  {
    //add dolphin [database manager]@20150616:b
    char dn[OB_MAX_DATBASE_NAME_LENGTH];
    char tn[OB_MAX_TABLE_NAME_LENGTH];
    ObString dname;
    ObString tname;
    dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
    tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
    table_name.split_two(dname,tname);
    //add:e
    //modify dolphin [database manager]@20150616
    //ret = schema_service_->get_table_schema(table_name, table_schema);
    ret = schema_service_->get_table_schema(tname, table_schema,dname);
    //modify:e
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", table_name.ptr(), ret);
    }
  }
  return ret;
}

int64_t ObRootServer2::get_schema_version() const
{
  //mod zhaoqiong [Schema Manager] 20150327:b
  //RS schema timestamp update early than schema
  //avoid ups update schema early than rs
  //return schema_timestamp_;
  return schema_manager_for_cache_->get_version();
  //mod:e
}

int64_t ObRootServer2::get_privilege_version() const
{
  return privilege_timestamp_;
}

void ObRootServer2::set_privilege_version(const int64_t privilege_version)
{
  privilege_timestamp_ = privilege_version;
}

void ObRootServer2::set_daily_merge_tablet_error(const char* err_msg, const int64_t length)
{
 state_.set_daily_merge_error(err_msg, length);
}

void ObRootServer2::clean_daily_merge_tablet_error()
{
  state_.clean_daily_merge_error();
}

bool ObRootServer2::is_daily_merge_tablet_error()const
{
  return state_.is_daily_merge_tablet_error();
}
char* ObRootServer2::get_daily_merge_error_msg()
{
  return state_.get_error_msg();
}
int64_t ObRootServer2::get_config_version() const
{
  return worker_->get_config_mgr().get_version();
}

int ObRootServer2::get_max_tablet_version(int64_t &version) const
{
  int err = OB_SUCCESS;
  version = -1;
  if (NULL != root_table_)
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    version = root_table_->get_max_tablet_version();
    TBSYS_LOG(INFO, "root table max tablet version =%ld", version);
  }
  return err;
}

void ObRootServer2::print_alive_server() const
{
  TBSYS_LOG(INFO, "start dump server info");
  //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
  ObChunkServerManager::const_iterator it = server_manager_.begin();
  int32_t index = 0;
  for (; it != server_manager_.end(); ++it, ++index)
  {
    if (!it->is_lms())
    {
      it->dump(index);
    }
  }
  TBSYS_LOG(INFO, "dump server info complete");
  return;
}

int ObRootServer2::get_cs_info(ObChunkServerManager* out_server_manager) const
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  if (out_server_manager != NULL)
  {
    *out_server_manager = server_manager_;
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

void ObRootServer2::reset_hb_time()
{
  TBSYS_LOG(INFO, "reset heartbeat time");
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  ObChunkServerManager::iterator it = server_manager_.begin();
  ObServer server;
  for(; it != server_manager_.end(); ++it)
  {
    server = it->server_;
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && it->port_cs_ > 0)
    {
      server.set_port(it->port_cs_);
      receive_hb(server, it->port_cs_, false, OB_CHUNKSERVER);
    }
    if (it->ms_status_ != ObServerStatus::STATUS_DEAD
        && it->port_ms_ > 0)
    {
      server.set_port(it->port_ms_);
      receive_hb(server, it->port_ms_sql_, it->lms_, OB_MERGESERVER);
    }
  }
  TBSYS_LOG(INFO, "reset heartbeat time end");
}

void ObRootServer2::dump_root_table() const
{
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump root table");
  if (root_table_ != NULL)
  {
    root_table_->dump();
  }
  //mod liumz, [paxos static index]20170626:b
  for (int i = 0; i < OB_MAX_COPY_COUNT; i++)
  {
    //add liumz, [secondary index static_index_build] 20150423:b
    //for debug
    tbsys::CThreadGuard mutex_gard(&hist_table_mutex_);
    if (NULL != hist_table_[i])
    {
      hist_table_[i]->dump();
    }
    //add:e
  }
  //mod:e
}

//add liumz, [secondary index static_index_build] 20150601:b
void ObRootServer2::dump_root_table(const int32_t index) const
{
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (root_table_ != NULL)
  {
    root_table_->dump(index);
  }
}
//add:e

bool ObRootServer2::check_root_table(const common::ObServer &expect_cs) const
{
  bool err = false;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  int server_index = get_server_index(expect_cs);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(INFO, "can not find server's info, just check roottable");
  }
  else
  {
    TBSYS_LOG(INFO, "check roottable without cs=%s in considered.", to_cstring(expect_cs));
  }
  if (root_table_ != NULL)
  {
    err = root_table_->check_lost_data(server_index);
  }
  return err;
}

int ObRootServer2::dump_cs_tablet_info(const common::ObServer & cs, int64_t &tablet_num) const
{
  int err = OB_SUCCESS;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump cs[%s]'s tablet info", cs.to_cstring());
  int server_index = get_server_index(cs);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", cs.to_cstring());
    err = OB_ENTRY_NOT_EXIST;
  }
  else if (root_table_ != NULL)
  {
    root_table_->dump_cs_tablet_info(server_index, tablet_num);
  }
  return err;
}

void ObRootServer2::dump_unusual_tablets() const
{
  int32_t num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump unusual tablets");
  if (root_table_ != NULL)
  {
    root_table_->dump_unusual_tablets(last_frozen_mem_version_,
                                      (int32_t)config_.tablet_replicas_num, num);
  }
}

int ObRootServer2::try_create_new_table(int64_t frozen_version, const uint64_t table_id)
{
  int err = OB_SUCCESS;
  bool table_not_exist_in_rt = false;
  TBSYS_LOG(INFO, "create new table start: table_id=%lu", table_id);
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  if (NULL == root_table_)
  {
    TBSYS_LOG(WARN, "root table is not init; wait another second.");
  }
  else
  {
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if(!root_table_->table_is_exist(table_id))
      {
        TBSYS_LOG(INFO, "table not exist, table_id=%lu", table_id);
        table_not_exist_in_rt = true;
      }
    }
    bool is_exist_in_cs = true;
    if (table_not_exist_in_rt)
    {
      //check cs number
      if (DEFAULT_SAFE_CS_NUMBER >= get_alive_cs_number())
      {
        //check table not exist in chunkserver
        if (OB_SUCCESS != (err = table_exist_in_cs(table_id, is_exist_in_cs)))
        {
          TBSYS_LOG(WARN, "fail to check table. table_id=%lu, err=%d", table_id, err);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "cs number is too litter. alive_cs_number=%ld", get_alive_cs_number());
      }
    }
    if (!is_exist_in_cs)
    {
      ObTabletInfoList tablets;
      if (OB_SUCCESS != (err = split_table_range(frozen_version, table_id, tablets)))
      {
        TBSYS_LOG(WARN, "fail to get tablet info for table[%lu], err=%d", table_id, err);
      }
      else
      {
        err = create_table_tablets(table_id, tablets);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to create table[%lu] tablets, err=%d", table_id, err);
        }
      }
    }
  }
  return err;
}

int ObRootServer2::create_table_tablets(const uint64_t table_id, const ObTabletInfoList & list)
{
  int err = OB_SUCCESS;
  int64_t version = -1;
  if (OB_SUCCESS != (err = check_tablets_legality(list)))
  {
    TBSYS_LOG(WARN, "get wrong tablets. err=%d", err);
  }
  else if (OB_SUCCESS != (err = get_max_tablet_version(version)))
  {
    TBSYS_LOG(WARN, "fail get max tablet version. err=%d", err);
  }
  else if (OB_SUCCESS != (err = create_tablet_with_range(version, list)))
  {
    TBSYS_LOG(ERROR, "fail to create tablet. rt_version=%ld, err=%d", version, err);
  }
  else
  {
    TBSYS_LOG(INFO, "create tablet for table[%lu] success.", table_id);
  }
  return err;
}

int64_t ObRootServer2::get_alive_cs_number()
{
  int64_t number = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      number++;
    }
  }
  return number;
}

int ObRootServer2::check_tablets_legality(const common::ObTabletInfoList &tablets)
{
  int err = OB_SUCCESS;
  common::ObTabletInfo *p_tablet_info = NULL;
  if (0 == tablets.get_tablet_size())
  {
    TBSYS_LOG(WARN, "tablets has zero tablet_info. tablets size = %ld", tablets.get_tablet_size());
    err = OB_INVALID_ARGUMENT;
  }
  static char buff[OB_MAX_ROW_KEY_LENGTH * 2];
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(0);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.start_key_.is_min_row())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not start correctly. first tablet start_key=%s",
                to_cstring(p_tablet_info->range_));
    }
  }
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(tablets.get_tablet_size() - 1);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.end_key_.is_max_row())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not end correctly. last tablet range=%s",
                to_cstring(p_tablet_info->range_));
    }
  }
  if (OB_SUCCESS == err)
  {
    for (int64_t i = 1; OB_SUCCESS == err && i < tablets.get_tablet_size(); i++)
    {
      p_tablet_info = tablets.tablet_list.at(i);
      if (NULL == p_tablet_info)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      }
      else if (!p_tablet_info->range_.is_left_open_right_closed())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, should be left open right closed. range=%s", buff);
      }
      else if (p_tablet_info->range_.start_key_ == p_tablet_info->range_.end_key_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, start_key = end_key; range=%s", buff);
      }
      else if (0 != p_tablet_info->range_.start_key_.compare(tablets.tablet_list.at(i - 1)->range_.end_key_))
      {
        TBSYS_LOG(WARN, "range not continuous. %ldth tablet range=%s, %ldth tablet range=%s",
            i-1, to_cstring(tablets.tablet_list.at(i - 1)->range_), i, to_cstring(p_tablet_info->range_));
        err = OB_INVALID_ARGUMENT;
      }
    }
  }
  return err;
}

int ObRootServer2::try_create_new_tables(int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  if (NULL == root_table_)
  {
    TBSYS_LOG(WARN, "root table is not init; wait another second.");
  }
  else
  {
    for (const ObTableSchema* it=schema_manager_for_cache_->table_begin();
         it != schema_manager_for_cache_->table_end(); ++it)
    {
      if (it->get_table_id() != OB_INVALID_ID)
      {
        {
          tbsys::CRLockGuard guard(root_table_rwlock_);
          if(!root_table_->table_is_exist(it->get_table_id()))
          {
            TBSYS_LOG(INFO, "table not exist, table_id=%lu", it->get_table_id());
            table_id = it->get_table_id();
          }
        }
        if (OB_INVALID_ID != table_id)
        {
          err = try_create_new_table(frozen_version, table_id);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "fail to create table.table_id=%ld", table_id);
            ret = err;
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::split_table_range(const int64_t frozen_version, const uint64_t table_id,
                                     common::ObTabletInfoList &tablets)
{
  int err = OB_SUCCESS;
  //step1: get master ups
  ObServer master_ups;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_master_ups(master_ups, false)))
    {
      TBSYS_LOG(WARN, "fail to get master ups addr. err=%d", err);
    }
  }
  //setp2: get tablet_info form ups ,retry
  int64_t rt_version = 0;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_max_tablet_version(rt_version)))
    {
      TBSYS_LOG(WARN, "fail to get max tablet version.err=%d", err);
    }
  }
  int64_t fetch_version = rt_version;
  if (1 == fetch_version)
  {
    fetch_version = 2;
  }
  if (OB_SUCCESS == err)
  {
    while (fetch_version <= frozen_version)
    {
      common::ObTabletInfoList tmp_tablets;
      if (OB_SUCCESS != (err = worker_->get_rpc_stub().get_split_range(master_ups,
                                                                       config_.network_timeout, table_id,
                                                                       fetch_version, tmp_tablets)))
      {
        TBSYS_LOG(WARN, "fail to get split range from ups. ups=%s, table_id=%ld, fetch_version=%ld, forzen_version=%ld",
                  master_ups.to_cstring(), table_id, fetch_version, frozen_version);
        if (OB_UPS_INVALID_MAJOR_VERSION == err)
        {
          err = OB_SUCCESS;
          TBSYS_LOG(INFO, "fetch tablets retruen invalid_major_version, version=%ld, try next version.", fetch_version);
        }
        else
        {
          TBSYS_LOG(WARN, "get split_range has some trouble, abort it. err=%d", err);
          break;
        }
      }
      else
      {
        if ((1 == tmp_tablets.get_tablet_size())
            && (0 == tmp_tablets.tablet_list.at(0)->row_count_))
        {
          tablets = tmp_tablets;
          if (fetch_version == frozen_version)
          {
            TBSYS_LOG(INFO, "reach the bigger fetch_version, equal to frozen_version[%ld]", frozen_version);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "fetch only one empty tablet. fetch_version=%ld, frozen_version=%ld, row_count=%ld, try the next version",
                      fetch_version, frozen_version, tmp_tablets.tablet_list.at(0)->row_count_);
          }
        }
        else
        {
          tablets = tmp_tablets;
          break;
        }
      }
      sleep(WAIT_SECONDS);
      fetch_version ++;
    }
    if (OB_SUCCESS != err && fetch_version > frozen_version)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "retry all version and failed. check it.");
    }
    else if (OB_SUCCESS == err)
    {
      TBSYS_LOG(INFO, "fetch tablet_list succ. fetch_version=%ld, tablet_list size=%ld",
                fetch_version, tablets.get_tablet_size());
      ObTabletInfo *tablet_info = NULL;
      for (int64_t i = 0; i < tablets.get_tablet_size(); i++)
      {
        tablet_info = tablets.tablet_list.at(i);
        TBSYS_LOG(INFO, "%ldth tablet:range=%s", i, to_cstring(tablet_info->range_));
      }
    }
  }
  return err;
}

int ObRootServer2::create_tablet_with_range(const int64_t frozen_version, const ObTabletInfoList& tablets)
{
  int ret = OB_SUCCESS;
  int64_t index = tablets.tablet_list.get_array_index();
  ObTabletInfo* p_table_info = NULL;
  ObRootTable2* root_table_for_split = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (NULL == root_table_for_split)
  {
    TBSYS_LOG(WARN, "new ObRootTable2 fail.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *root_table_for_split = *root_table_;
  }

  int32_t **server_index = NULL;
  int32_t *create_count = NULL;
  if (OB_SUCCESS == ret)
  {
    create_count = new (std::nothrow)int32_t[index];
    server_index = new (std::nothrow)int32_t*[index];
    if (NULL == server_index || NULL == create_count)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      for (int32_t i = 0; i < index; i++)
      {
	    //add zhaoqiong[roottable tablet management]20150302:b
		//server_index[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
        server_index[i] = new(std::nothrow) int32_t[OB_MAX_COPY_COUNT];
		//add e
        if (NULL == server_index[i])
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < index; i ++)
  {
    p_table_info = tablets.tablet_list.at(i);
    if (p_table_info != NULL)
    {
      TBSYS_LOG(INFO, "tablet_index=%ld, range=%s", i, to_cstring(p_table_info->range_));
      if (!p_table_info->range_.is_left_open_right_closed())
      {
        TBSYS_LOG(WARN, "illegal tablet, range=%s", to_cstring(p_table_info->range_));
      }
      else
      {
        //閫氱煡cs寤虹珛tablet
        if (OB_SUCCESS != (ret =
                           create_empty_tablet_with_range(frozen_version, root_table_for_split,
                                                          *p_table_info, create_count[i], server_index[i])))
        {
          TBSYS_LOG(WARN, "fail to create tablet, ret =%d, range=%s", ret, to_cstring(p_table_info->range_));
        }
        else
        {
          TBSYS_LOG(INFO, "create tablet succ. range=%s", to_cstring(p_table_info->range_));
        }
      }
    }//end p_table_info != NULL
  }//end for
  if (OB_SUCCESS == ret && root_table_for_split != NULL)
  {
    TBSYS_LOG(INFO, "create tablet success.");
    if (is_master())
    {
      int ret2 = log_worker_->batch_add_new_tablet(tablets, server_index, create_count, frozen_version);
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "failed to log add_new_tablet, err=%d", ret2);
      }
    }
    switch_root_table(root_table_for_split, NULL);
    root_table_for_split = NULL;
  }
  else
  {
    if (NULL != root_table_for_split)
    {
      OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_split);
    }
  }
  for (int64_t i = 0; i < index; i++)
  {
    if (NULL != server_index[i])
    {
      delete [] server_index[i];
    }
  }
  if (NULL != server_index)
  {
    delete [] server_index;
  }
  if (NULL != create_count)
  {
    delete [] create_count;
  }
  return ret;
}

int ObRootServer2::create_empty_tablet_with_range(const int64_t frozen_version,
    ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
    int32_t& created_count, int* t_server_index)
{
  int ret = OB_SUCCESS;
  //add zhaoqiong[roottable tablet management]20150302:b
  //int32_t server_index[OB_SAFE_COPY_COUNT];
  int32_t server_index[OB_MAX_COPY_COUNT];
  //add e
  if (NULL == root_table)
  {
    TBSYS_LOG(WARN, "invalid argument. root_table_for_split is NULL;");
    ret = OB_INVALID_ARGUMENT;
  }
  //add zhaoqiong[roottable tablet management]20150302:b
  //for (int i = 0; i < OB_SAFE_COPY_COUNT; i++)
  for (int i = 0; i < OB_MAX_COPY_COUNT; i++)
  //add e
  {
    server_index[i] = OB_INVALID_INDEX;
    t_server_index[i] = OB_INVALID_INDEX;
  }

  int32_t result_num = -1;
  if (OB_SUCCESS == ret)
  {
    get_available_servers_for_new_table(server_index, (int32_t)config_.tablet_replicas_num, result_num);
    if (0 >= result_num)
    {
      TBSYS_LOG(WARN, "no cs seleted");
      ret = OB_NO_CS_SELECTED;
    }
  }

  created_count = 0;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "cs selected for create new table, num=%d", result_num);
    for (int32_t i = 0; i < result_num; i++)
    {
      if (server_index[i] != OB_INVALID_INDEX)
      {
        ObServerStatus* server_status = server_manager_.get_server_status(server_index[i]);
        if (server_status != NULL)
        {
          common::ObServer server = server_status->server_;
          server.set_port(server_status->port_cs_);
          int err = worker_->get_rpc_stub().create_tablet(server, tablet.range_, frozen_version,
                                                          config_.network_timeout);
          if (OB_SUCCESS != err && OB_ENTRY_EXIST != err)
          {
            TBSYS_LOG(WARN, "fail to create tablet replica, server=%d", server_index[i]);
          }
          else
          {
            err = OB_SUCCESS;
            t_server_index[created_count] = server_index[i];
            created_count++;
            TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                      tablet.range_.table_id_, server_index[i], frozen_version);
          }
        }
        else
        {
          server_index[i] = OB_INVALID_INDEX;
        }
      }
    }
    if (created_count > 0)
    {
      ObArray<int> sia;
      for (int64_t i = 0 ; i < created_count; ++i)
      {
        sia.push_back(t_server_index[i]);
      }
      ret = root_table->create_table(tablet, sia, frozen_version);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to create table.err=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "no tablet created");
      ret = OB_NO_TABLETS_CREATED;
    }
  }
  return ret;
}

// WARN: if safe_count is zero, then use system min(safe replica count , alive chunk server count) as safe_count
int ObRootServer2::check_tablet_version(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const
{
  int err = OB_SUCCESS;
  int32_t chunk_server_count = server_manager_.get_alive_server_count(true);
  TBSYS_LOG(TRACE, "check tablet version[required_version=%ld]", tablet_version);
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    int64_t min_replica_count = safe_count;
    if (0 == safe_count)
    {
      min_replica_count = config_.tablet_replicas_num;
      if ((chunk_server_count > 0) && (chunk_server_count < min_replica_count))
      {
        TBSYS_LOG(TRACE, "check chunkserver count less than replica num:server[%d], replica[%ld]",
            chunk_server_count, safe_count);
        min_replica_count = chunk_server_count;
      }
    }
    if (root_table_->is_empty())
    {
      TBSYS_LOG(WARN, "root table is empty, try it later");
      is_merged = false;
    }
    else
    {
      //add liumz, [secondary index static_index_build] 20150422:b
      common::ObSchemaManagerV2* out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
      if (NULL == out_schema)
      {
        TBSYS_LOG(WARN, "fail to new schema_manager.");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != ( err = const_cast<ObRootServer2*>(this)->get_schema(false, false, *out_schema)))
      {
        TBSYS_LOG(WARN, "fail to get schema. ret=%d", err);
      }
      else if (OB_SUCCESS != (err = root_table_->check_tablet_version_merged_v2(tablet_version, min_replica_count, is_merged, *out_schema)))
      {
        TBSYS_LOG(WARN, "check_tablet_version_merged_v2() failed. ret=%d", err);
      }
      if (out_schema != NULL)
      {
        OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
      }
      //add:e
      //del liumz, [secondary index static_index_build] 20150422:b
      //err = root_table_->check_tablet_version_merged(tablet_version, min_replica_count, is_merged);
      //del:e
    }
    //add wenghaixing [secondary index static_index_build]20150317
    if(OB_SUCCESS == err)
    {
      if(NULL == worker_)
      {
        TBSYS_LOG(ERROR, "root_worker pointer is NULL");
      }
      else if(is_merged && tablet_version == get_last_frozen_version())
      /*
       * only guard build index version, if tablet_version > get_build_index_version(), is_merged must be false
       * because get_build_index_version() will block tablet_version's merge.
       */
      //else if(is_merged && tablet_version == get_build_index_version())
      {
        TBSYS_LOG(INFO, "common merged complete,tablet_version[%ld],last frozen version[%ld]",tablet_version,get_last_frozen_version());
        //worker_->get_monitor()->set_start_version(tablet_version); //del liumz, [secondary index version management]20160426
        //add liumz, [bugfix: do not start index mission here]20150917
        //if(worker_->get_monitor()->is_start()&&OB_SUCCESS == (err = worker_->get_monitor()->start_mission()))
        if(worker_->get_monitor()->is_start() && !(worker_->get_monitor()->check_create_index_over()))
        //add:e
        {
          is_merged = false;
        }
      }
    }
    //add e
  }
  else
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "fail to check tablet_version_merged. root_table_ = null");
  }
  return err;
}

//add liumz, [secondary index static_index_build] 20150529:b
int ObRootServer2::check_tablet_version_v3(const uint64_t table_id, const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const
{
  int err = OB_SUCCESS;
  int32_t chunk_server_count = server_manager_.get_alive_server_count(true);
  TBSYS_LOG(TRACE, "check tablet version[required_version=%ld]", tablet_version);
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    int64_t min_replica_count = safe_count;
    if (0 == safe_count)
    {
      min_replica_count = config_.tablet_replicas_num;
      if ((chunk_server_count > 0) && (chunk_server_count < min_replica_count))
      {
        TBSYS_LOG(TRACE, "check chunkserver count less than replica num:server[%d], replica[%ld]",
            chunk_server_count, safe_count);
        min_replica_count = chunk_server_count;
      }
    }
    if (root_table_->is_empty())
    {
      TBSYS_LOG(WARN, "root table is empty, try it later");
      is_merged = false;
    }
    else
    {
      err = root_table_->check_tablet_version_merged_v3(table_id, tablet_version, min_replica_count, is_merged);
    }
  }
  else
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "check_tablet_version_v3 failed. root_table_ = null");
  }
  return err;
}
//add:e

void ObRootServer2::dump_migrate_info() const
{
  balancer_->dump_migrate_info();
}

int ObRootServer2::get_deleted_tables(const common::ObSchemaManagerV2 &old_schema,
                                      const common::ObSchemaManagerV2 &new_schema,
                                      common::ObArray<uint64_t> &deleted_tables)
{
  int ret = OB_SUCCESS;
  deleted_tables.clear();
  const ObTableSchema* it = old_schema.table_begin();
  const ObTableSchema* it2 = NULL;
  for (; it != old_schema.table_end(); ++it)
  {
    if (NULL == (it2 = new_schema.get_table_schema(it->get_table_id())))
    {
      if (OB_SUCCESS != (ret = deleted_tables.push_back(it->get_table_id())))
      {
        TBSYS_LOG(WARN, "failed to push array, err=%d", ret);
        break;
      }
      else
      {
        //TBSYS_LOG(INFO, "table deleted, table_id=%lu", it->get_table_id());
      }
    }
  }
  return ret;
}

//add liumz, [secondary index static_index_build] 20150320:b
//reuse hist_table_ for building next index table
int ObRootServer2::reuse_hist_table(const uint64_t index_tid)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObTableSchema* table_schema = NULL;
  if (NULL == hist_table_ || NULL == hist_manager_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ or hist_manager_ is null.");
  }
  else if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed.");
  }
  else
  {
    table_schema = schema_mgr->get_table_schema(index_tid);
    if (NULL != table_schema)
    {
      table_id = table_schema->get_index_helper().tbl_tid;//get main table_id
      //mod liumz, [paxos static index]20170626:b
      for(int i = 0; i < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; i++)
      {
        if (NULL != hist_manager_[i] && NULL != hist_table_[i])
        {
          tbsys::CThreadGuard mutex_gard(&hist_table_mutex_);
          hist_manager_[i]->reuse();
          ret = hist_manager_[i]->set_table_id(table_id, index_tid);          
          hist_table_[i]->reset();
          ret = hist_table_[i]->set_table_id(table_id, index_tid);
        }
      }
      //mod:e
    }
    else
    {
      ret = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "get table schema failed. tid=%lu", index_tid);
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}

int ObRootServer2::clean_hist_table()
{
  int ret = OB_SUCCESS;
  if (NULL == hist_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ is null.");
  }
  else
  {
    //mod liumz, [paxos static index]20170626:b
    for(int i = 0; i < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; i++)
    {
      if (NULL != hist_manager_[i] && NULL != hist_table_[i])
      {
        tbsys::CThreadGuard mutex_gard(&hist_table_mutex_);
        hist_manager_[i]->reuse();
        hist_table_[i]->reset();
      }
    }
    //mod:e
  }
  return ret;
}
//add:e

void ObRootServer2::switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti)
{
  /*
  //add liumz, [secondary index static_index_build] 20150320:b
  TBSYS_LOG(INFO, "clean hist table before switch to new root table.");
  int err = clean_hist_table();
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "clean hist table failed.");
  }
  //add:e
  */
  OB_ASSERT(rt);
  OB_ASSERT(rt != root_table_);
  tbsys::CWLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "switch to new root table, old=%p, new=%p", root_table_, rt);
  if (NULL != root_table_)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_);
    root_table_ = NULL;
  }
  root_table_ = rt;
  balancer_->set_root_table(root_table_);
  restart_server_->set_root_table2(root_table_);

  if (NULL != ti && ti != tablet_manager_)
  {
    if (NULL != tablet_manager_)
    {
      OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
      tablet_manager_ = NULL;
    }
    tablet_manager_ = ti;
  }
}

/*
 * chunk server register
 * @param out status 0 do not start report 1 start report
 */
int ObRootServer2::regist_chunk_server(const ObServer& server, const char* server_version, int32_t& status, int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "regist chunk server:addr[%s], version[%s]", to_cstring(server), server_version);
  //add pangtianze [Paxos rs_election] 20160928:b
  if (NULL == get_rs_node_mgr())
  {
      TBSYS_LOG(ERROR, "rs_node_mgr_ is NULL");
      ret = OB_NOT_INIT;
  }
  else if (RS_ELECTION_DOING == get_rs_node_mgr()->get_election_state())
  {
      ret = OB_RS_DOING_ELECTION;
  }
  //add:e
  bool master = is_master();
  if (master && OB_SUCCESS == ret)
  {
    time_stamp = tbsys::CTimeUtil::getTime();
    ret = log_worker_->regist_cs(server, server_version, time_stamp);
  }
  if (ret == OB_SUCCESS)
  {
    bool need_force_report = false;
    // regist
    {
      time_stamp = tbsys::CTimeUtil::getTime();
      tbsys::CWLockGuard guard(server_manager_rwlock_);
      const ObChunkServerManager::iterator it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())  //found server
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          need_force_report = true;
          TBSYS_LOG(INFO, "recive cs regist request while server not dead, server [%s]",
              to_cstring(server));
        }
        //add bingo [Paxos CS restart] 20170307:b  mod 20170426:b:e
        else if(it->ms_status_ == ObServerStatus::STATUS_DEAD)
        {
          it->server_.set_cluster_id(server.cluster_id_);
        }
        if(it->server_.cluster_id_ != server.cluster_id_)
        {
          ret = OB_CLUSTER_ID_ERROR;
          TBSYS_LOG(INFO,"receive cs regist request while a cs/ms with the same ip already registered with cluster_id[%d], ret=%d", it->server_.cluster_id_, ret);
        }
        //add:e
      }
      //add bingo [Paxos ms/cs cluster_id] 20170401:b
      if(OB_SUCCESS == ret){
      //add:e
        server_manager_.receive_hb(server, time_stamp, false, false, 0, true);
        if (master)
        {
          commit_task(SERVER_ONLINE, OB_CHUNKSERVER, server, 0, server_version);
        }
      }
    }
    //add bingo [Paxos ms/cs cluster_id] 20170401:b
    if(OB_SUCCESS == ret){
    //add:e
      first_cs_had_registed_ = true;
      //now we always want cs report its tablet
      status = START_REPORTING;
      if (START_REPORTING == status)
      {
        tbsys::CThreadGuard mutex_guard(&root_table_build_mutex_); //this for only one thread modify root_table
        tbsys::CWLockGuard root_table_guard(root_table_rwlock_);
        tbsys::CWLockGuard server_info_guard(server_manager_rwlock_);
        ObChunkServerManager::iterator it;
        it = server_manager_.find_by_ip(server);
        if (it != server_manager_.end())
        {
          {
            //remove this server's tablet
            if (root_table_ != NULL)
            {
              root_table_->server_off_line(static_cast<int32_t>(it - server_manager_.begin()), 0);
            }
          }
          if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
          {
            // chunk server will start report
            it->status_ = ObServerStatus::STATUS_REPORTING;
          }

          if (need_force_report)
          {
            if (OB_SUCCESS != (ret = worker_->get_rpc_stub().request_report_tablet(it->server_)))
            {
              TBSYS_LOG(WARN, "request cs %s report tablet failed, ret %d",
                  to_cstring(it->server_), ret);
            }
          }
        }
      }
    }
  }
  TBSYS_LOG(INFO, "process regist server:%s ret:%d", server.to_cstring(), ret);

  if(OB_SUCCESS == ret)
  {
    if(NULL != balancer_thread_)
    {
      balancer_thread_->wakeup();
    }
    else
    {
      TBSYS_LOG(WARN, "balancer_thread_ is null");
    }
  }
  return ret;
}

void ObRootServer2::commit_task(const ObTaskType type, const ObRole role, const ObServer & server,
    int32_t inner_port, const char* server_version, const int32_t cluster_role)
{
  ObRootAsyncTaskQueue::ObSeqTask task;
  //add pangtianze [Paxos rs_election] 20150630:b
  if (type == SERVER_ONLINE && role == OB_ROOTSERVER)
  {
    task.server_status_ = ObRoleMgr::SLAVE;
    ObServer master;
    get_rs_master(master);
    if (master == server)
    {
      task.server_status_ = ObRoleMgr::MASTER;
    }
  }
  //add:e
  task.type_ = type;
  task.role_ = role;
  task.server_ = server;
  task.inner_port_ = inner_port;
  task.cluster_role_ = cluster_role;
  int64_t server_version_length = strlen(server_version);
  if (server_version_length < OB_SERVER_VERSION_LENGTH)
  {
    strncpy(task.server_version_, server_version, server_version_length + 1);
  }
  else
  {
    strncpy(task.server_version_, server_version, OB_SERVER_VERSION_LENGTH - 1);
    task.server_version_[OB_SERVER_VERSION_LENGTH - 1] = '\0';
  }

  // change server inner port
  if (OB_MERGESERVER == role)
  {
    task.server_.set_port(inner_port);
    //add lbzhong [Paxos Cluster.Balance] 20160707:b
    if(LMS_ONLINE != type)
    {
      task.inner_port_ = server.get_port();
    //add:e
    } //add lbzhong [Paxos Cluster.Balance] 20160707:b:e
  }
  int ret = seq_task_queue_.push(task);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "commit inner task failed:server[%s], task_type[%d], server_role[%s], inner_port[%d], ret[%d]",
        task.server_.to_cstring(), type, print_role(task.role_), task.inner_port_, ret);
  }
  else
  {
    TBSYS_LOG(INFO, "commit inner task succ:server[%s], task_type[%d], server_role[%s], inner_port[%d]",
        task.server_.to_cstring(), type, print_role(task.role_), task.inner_port_);
  }
}

int ObRootServer2::regist_merge_server(const common::ObServer& server, const int32_t sql_port,
                                       const bool is_listen_ms, const char * server_version, int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "regist merge server:addr[%s], sql_port[%d], server_version[%s], is_listen_ms=%s",
            to_cstring(server), sql_port, server_version, is_listen_ms ? "true" : "false");
  //mod chujiajia [Paxos rs_election] 20151207:b
  bool master = is_master();
  //bool master = (get_rs_role() == ObRoleMgr::MASTER)?true:false;
  if (master)
  {
    if (!is_listen_ms)
    {
      time_stamp = tbsys::CTimeUtil::getTime();
      ret = log_worker_->regist_ms(server, sql_port, server_version, time_stamp);
    }
    else
    {
      ret = log_worker_->regist_lms(server, sql_port, server_version, time_stamp);
    }
  }
  //add pangtianze [Paxos rs_election] 20160928:b
  if (NULL == get_rs_node_mgr())
  {
      TBSYS_LOG(ERROR, "rs_node_mgr_ is NULL");
      ret = OB_NOT_INIT;
  }
  else if (RS_ELECTION_DOING == get_rs_node_mgr()->get_election_state())
  {
      ret = OB_RS_DOING_ELECTION;
  }
  //add:e
  if(OB_SUCCESS == ret)
  {
    time_stamp = tbsys::CTimeUtil::getTime();
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    //add bingo [Paxos MS restart] 20170307:b
    const ObChunkServerManager::iterator it = server_manager_.find_by_ip(server);
    if (it != server_manager_.end())  //found server
    {
      if (it->ms_status_ != ObServerStatus::STATUS_DEAD)
      {
          ObServer ms_server(it->server_);
          ms_server.set_port(it->port_ms_);
          TBSYS_LOG(INFO, "recive ms[%s] regist request while server as[%s] not dead.",
          to_cstring(server), to_cstring(ms_server));
      }
      //add bingo [Paxos ms restart bugfix]  20170426:b:e
      else if(it->status_ == ObServerStatus::STATUS_DEAD)
      {
        it->server_.set_cluster_id(server.cluster_id_);
      }
      if(it->server_.cluster_id_ != server.cluster_id_)
      {
        ret = OB_CLUSTER_ID_ERROR;
        TBSYS_LOG(WARN,"receive ms regist request while a ms/cs with the same ip already registered with cluster_id[%d], ret=%d", it->server_.cluster_id_, ret);
      }
      //add:e
    }
    //add bingo [Paxos ms/cs cluster_id] 20170401:b
    if(OB_SUCCESS == ret){
    //add:e
      // regist merge server
      server_manager_.receive_hb(server, time_stamp, true, is_listen_ms, sql_port, true);
      if (master)
      {
          if (!is_listen_ms)
          {
            commit_task(SERVER_ONLINE, OB_MERGESERVER, server, sql_port, server_version);
          }
          else
          {
            ObServer cluster_vip;
            //mod pangtianze [Paxos Cluster.Balance] 20170103:b
            //config_.get_root_server(cluster_vip);
            cluster_vip = server;
            TBSYS_LOG(INFO,"lms register,start refresh inner table, lms[%s]", cluster_vip.to_cstring());
            //mod:e
            commit_task(LMS_ONLINE, OB_MERGESERVER, cluster_vip, sql_port, server_version);
          }
      }
    }
    //add:e
  }
  return ret;
}

/*
 * chunk server鏇存柊鑷繁鐨勭鐩樻儏鍐典俊鎭?
 */
int ObRootServer2::update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used)
{
  int ret = OB_SUCCESS;
  if (is_master())
  {
    ret = log_worker_->report_cs_load(server, capacity, used);
  }

  if (ret == OB_SUCCESS)
  {
    ObServerDiskInfo disk_info;
    disk_info.set_capacity(capacity);
    disk_info.set_used(used);
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    ret = server_manager_.update_disk_info(server, disk_info);
  }

  TBSYS_LOG(INFO, "server %s update capacity info capacity is %ld,"
            " used is %ld ret is %d", to_cstring(server), capacity, used, ret);

  return ret;
}

/*
 * 杩佺Щ瀹屾垚鎿嶄綔
 */
int ObRootServer2::migrate_over(const int32_t result, const ObDataSourceDesc& desc,
    const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  const ObNewRange& range = desc.range_;
  const ObServer& src_server = desc.src_server_;
  const common::ObServer& dest_server = desc.dst_server_;
  const int64_t tablet_version = desc.tablet_version_;
  const bool keep_src = desc.keep_source_;
  if (OB_SUCCESS == result)
  {
    TBSYS_LOG(INFO, "migrate_over received, result=%d dest_cs=%s desc=%s occupy_size=%ld crc_sum=%lu row_count=%ld",
       result, to_cstring(dest_server), to_cstring(desc), occupy_size, crc_sum, row_count);
  }
  else
  {
    TBSYS_LOG(WARN, "migrate_over received, result=%d dest_cs=%s desc=%s occupy_size=%ld crc_sum=%lu row_count=%ld",
       result, to_cstring(dest_server), to_cstring(desc), occupy_size, crc_sum, row_count);
  }

  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  /*
  if (!is_loading_data() && state_.get_bypass_flag())
  {
    TBSYS_LOG(WARN, "rootserver in clean root table process, refuse to migrate over report");
    ret = OB_RS_STATE_NOT_ALLOW;
  }
  else
  {
  */
  //del:e
    int src_server_index = get_server_index(src_server);
    int dest_server_index = get_server_index(dest_server);

    ObRootTable2::const_iterator start_it;
    ObRootTable2::const_iterator end_it;
    common::ObTabletInfo *tablet_info = NULL;
    if (OB_SUCCESS == result)
    {
      tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
      tbsys::CWLockGuard guard(root_table_rwlock_);
      int find_ret = root_table_->find_range(range, start_it, end_it);
      if (OB_SUCCESS == find_ret && start_it == end_it)
      {
        tablet_info = root_table_->get_tablet_info(start_it);
        if (NULL == tablet_info)
        {
          TBSYS_LOG(ERROR, "tablet_info must not null, start_it=%p, range=%s", start_it, to_cstring(desc.range_));
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (range.equal(tablet_info->range_))
        {
          if (keep_src)
          {
            if (OB_INVALID_INDEX == dest_server_index)
            {
              // dest cs is down
              TBSYS_LOG(WARN, "can not find cs, src=%d dest=%d", src_server_index, dest_server_index);
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              // add replica
              ret = root_table_->modify(start_it, dest_server_index, tablet_version);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to add new replica, range=%s dest_server_index=%d tablet_version=%ld, ret=%d",
                    to_cstring(tablet_info->range_), dest_server_index, tablet_version, ret);
              }
              else
              {
                ObTabletCrcHistoryHelper *crc_helper = root_table_->get_crc_helper(start_it);
                if (NULL == crc_helper)
                {
                  TBSYS_LOG(ERROR, "crc_helper must not null, range=%s start_it=%p",
                      to_cstring(tablet_info->range_), start_it);
                  ret = OB_ERR_SYS;
                }
                else
                {
                  int64_t min_version = 0;
                  int64_t max_version = 0;
                  crc_helper->get_min_max_version(min_version, max_version);
                  if (0 == min_version && 0 == max_version)
                  { // load tablet from outside, update tablet info
                    if (OB_SUCCESS != (ret = crc_helper->check_and_update(tablet_version, crc_sum, row_checksum)))
                    {
                      TBSYS_LOG(ERROR, "failed to check and update crc_sum, range=%s, ret=%d",
                          to_cstring(tablet_info->range_), ret);
                    }
                    else
                    {
                      tablet_info->row_count_ = row_count;
                      tablet_info->occupy_size_ = occupy_size;
                      tablet_info->crc_sum_ = crc_sum;
                      tablet_info->row_checksum_ = row_checksum;
                    }
                  }
                }
              }
            }
          }
          else
          {
            // dest_server_index and src_server_index may be INVALID
            ret = root_table_->replace(start_it, src_server_index, dest_server_index, tablet_version);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to mofidy replica, range=%s src_server_index=%d, "
                  "dest_server_index=%d tablet_version=%ld, ret=%d",
                  to_cstring(tablet_info->range_), src_server_index, dest_server_index, tablet_version, ret);
            }
          }
          ObServerStatus* src_status = server_manager_.get_server_status(src_server_index);
          ObServerStatus* dest_status = server_manager_.get_server_status(dest_server_index);
          if (src_status != NULL && dest_status != NULL && tablet_info != NULL)
          {
            if (!keep_src)
            {
              if (OB_INVALID_INDEX != src_server_index)
              {
                src_status->disk_info_.set_used(src_status->disk_info_.get_used() - tablet_info->occupy_size_);
              }
            }
            if (OB_INVALID_INDEX != dest_server_index)
            {
              dest_status->disk_info_.set_used(dest_status->disk_info_.get_used() + tablet_info->occupy_size_);
            }
          }

          if (OB_SUCCESS == ret && server_manager_.get_migrate_num() != 0
              //del lbzhong [Paxos Cluster.Balance] 20160705:b
              //&& balancer_->is_loading_data() == false
              //del:e
              )
          {
            const_cast<ObRootTable2::iterator>(start_it)->has_been_migrated();
          }

          if (is_master())
          {
            ret = log_worker_->cs_migrate_done(result, desc, occupy_size, crc_sum, row_checksum, row_count);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to write migrate over log, range=%s, src_server=%s, "
                  "dest_server=%s, keep_src=%s, tablet_version=%ld, ret=%d",
                  to_cstring(range), to_cstring(src_server), to_cstring(dest_server),
                  keep_src ? "true" : "false", tablet_version, ret);
            }
          }
        }
        else
        {
          ret = OB_ROOT_TABLE_RANGE_NOT_EXIST;
          TBSYS_LOG(INFO, "can not find the range %s in root table, ignore this", to_cstring(range));
        }
      }
      else
      {
        TBSYS_LOG(WARN, "fail to find range %s in roottable. find_ret=%d, start_it=%p, end_it=%p",
            to_cstring(range), find_ret, start_it, end_it);
        ret = OB_ROOT_TABLE_RANGE_NOT_EXIST;
      }
    }

    if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
    {
      if (ret == OB_SUCCESS)
      {
        balancer_->nb_trigger_next_migrate(desc, result);
      }
    }
  //} //del lbzhong [Paxos Cluster.Balance] 20160705:b:e
  return ret;
}

int ObRootServer2::make_out_cell(ObCellInfo& out_cell, ObRootTable2::const_iterator first,
                                 ObRootTable2::const_iterator last, ObScanner& scanner, const int32_t max_row_count, const int32_t max_key_len
                                 //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                                 , const int32_t cluster_id
                                 //add:e
                                 ) const
{
  static ObString s_root_1_port(static_cast<int32_t>(strlen(ROOT_1_PORT)), static_cast<int32_t>(strlen(ROOT_1_PORT)), (char*)ROOT_1_PORT);
  static ObString s_root_1_ms_port(static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), (char*)ROOT_1_MS_PORT);
  static ObString s_root_1_ipv6_1(static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), (char*)ROOT_1_IPV6_1);
  static ObString s_root_1_ipv6_2(static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), (char*)ROOT_1_IPV6_2);
  static ObString s_root_1_ipv6_3(static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), (char*)ROOT_1_IPV6_3);
  static ObString s_root_1_ipv6_4(static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), (char*)ROOT_1_IPV6_4);
  static ObString s_root_1_ipv4(  static_cast<int32_t>(strlen(ROOT_1_IPV4)), static_cast<int32_t>(strlen(ROOT_1_IPV4)), (char*)ROOT_1_IPV4);
  static ObString s_root_1_tablet_version(static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), (char*)ROOT_1_TABLET_VERSION);

  static ObString s_root_2_port(   static_cast<int32_t>(strlen(ROOT_2_PORT)),    static_cast<int32_t>(strlen(ROOT_2_PORT)),    (char*)ROOT_2_PORT);
  static ObString s_root_2_ms_port(static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), (char*)ROOT_2_MS_PORT);
  static ObString s_root_2_ipv6_1( static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  (char*)ROOT_2_IPV6_1);
  static ObString s_root_2_ipv6_2( static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  (char*)ROOT_2_IPV6_2);
  static ObString s_root_2_ipv6_3( static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  (char*)ROOT_2_IPV6_3);
  static ObString s_root_2_ipv6_4( static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  (char*)ROOT_2_IPV6_4);
  static ObString s_root_2_ipv4(   static_cast<int32_t>(strlen(ROOT_2_IPV4)),    static_cast<int32_t>(strlen(ROOT_2_IPV4)),    (char*)ROOT_2_IPV4);
  static ObString s_root_2_tablet_version(static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), (char*)ROOT_2_TABLET_VERSION);

  static ObString s_root_3_port(   static_cast<int32_t>(strlen(ROOT_3_PORT)),    static_cast<int32_t>(strlen(ROOT_3_PORT)),    (char*)ROOT_3_PORT);
  static ObString s_root_3_ms_port(static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), (char*)ROOT_3_MS_PORT);
  static ObString s_root_3_ipv6_1( static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  (char*)ROOT_3_IPV6_1);
  static ObString s_root_3_ipv6_2( static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  (char*)ROOT_3_IPV6_2);
  static ObString s_root_3_ipv6_3( static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  (char*)ROOT_3_IPV6_3);
  static ObString s_root_3_ipv6_4( static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  (char*)ROOT_3_IPV6_4);
  static ObString s_root_3_ipv4(   static_cast<int32_t>(strlen(ROOT_3_IPV4)),    static_cast<int32_t>(strlen(ROOT_3_IPV4)),    (char*)ROOT_3_IPV4);
  static ObString s_root_3_tablet_version(static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), (char*)ROOT_3_TABLET_VERSION);

  //add zhaoqiong[roottable tablet management]20160104:b
  static ObString s_root_4_port(static_cast<int32_t>(strlen(ROOT_4_PORT)), static_cast<int32_t>(strlen(ROOT_4_PORT)), (char*)ROOT_4_PORT);
  static ObString s_root_4_ms_port(static_cast<int32_t>(strlen(ROOT_4_MS_PORT)), static_cast<int32_t>(strlen(ROOT_4_MS_PORT)), (char*)ROOT_4_MS_PORT);
  static ObString s_root_4_ipv6_1(static_cast<int32_t>(strlen(ROOT_4_IPV6_1)), static_cast<int32_t>(strlen(ROOT_4_IPV6_1)), (char*)ROOT_4_IPV6_1);
  static ObString s_root_4_ipv6_2(static_cast<int32_t>(strlen(ROOT_4_IPV6_2)), static_cast<int32_t>(strlen(ROOT_4_IPV6_2)), (char*)ROOT_4_IPV6_2);
  static ObString s_root_4_ipv6_3(static_cast<int32_t>(strlen(ROOT_4_IPV6_3)), static_cast<int32_t>(strlen(ROOT_4_IPV6_3)), (char*)ROOT_4_IPV6_3);
  static ObString s_root_4_ipv6_4(static_cast<int32_t>(strlen(ROOT_4_IPV6_4)), static_cast<int32_t>(strlen(ROOT_4_IPV6_4)), (char*)ROOT_4_IPV6_4);
  static ObString s_root_4_ipv4(  static_cast<int32_t>(strlen(ROOT_4_IPV4)), static_cast<int32_t>(strlen(ROOT_4_IPV4)), (char*)ROOT_4_IPV4);
  static ObString s_root_4_tablet_version(static_cast<int32_t>(strlen(ROOT_4_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_4_TABLET_VERSION)), (char*)ROOT_4_TABLET_VERSION);

  static ObString s_root_5_port(   static_cast<int32_t>(strlen(ROOT_5_PORT)),    static_cast<int32_t>(strlen(ROOT_5_PORT)),    (char*)ROOT_5_PORT);
  static ObString s_root_5_ms_port(static_cast<int32_t>(strlen(ROOT_5_MS_PORT)), static_cast<int32_t>(strlen(ROOT_5_MS_PORT)), (char*)ROOT_5_MS_PORT);
  static ObString s_root_5_ipv6_1( static_cast<int32_t>(strlen(ROOT_5_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_5_IPV6_1)),  (char*)ROOT_5_IPV6_1);
  static ObString s_root_5_ipv6_2( static_cast<int32_t>(strlen(ROOT_5_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_5_IPV6_2)),  (char*)ROOT_5_IPV6_2);
  static ObString s_root_5_ipv6_3( static_cast<int32_t>(strlen(ROOT_5_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_5_IPV6_3)),  (char*)ROOT_5_IPV6_3);
  static ObString s_root_5_ipv6_4( static_cast<int32_t>(strlen(ROOT_5_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_5_IPV6_4)),  (char*)ROOT_5_IPV6_4);
  static ObString s_root_5_ipv4(   static_cast<int32_t>(strlen(ROOT_5_IPV4)),    static_cast<int32_t>(strlen(ROOT_5_IPV4)),    (char*)ROOT_5_IPV4);
  static ObString s_root_5_tablet_version(static_cast<int32_t>(strlen(ROOT_5_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_5_TABLET_VERSION)), (char*)ROOT_5_TABLET_VERSION);

  static ObString s_root_6_port(   static_cast<int32_t>(strlen(ROOT_6_PORT)),    static_cast<int32_t>(strlen(ROOT_6_PORT)),    (char*)ROOT_6_PORT);
  static ObString s_root_6_ms_port(static_cast<int32_t>(strlen(ROOT_6_MS_PORT)), static_cast<int32_t>(strlen(ROOT_6_MS_PORT)), (char*)ROOT_6_MS_PORT);
  static ObString s_root_6_ipv6_1( static_cast<int32_t>(strlen(ROOT_6_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_6_IPV6_1)),  (char*)ROOT_6_IPV6_1);
  static ObString s_root_6_ipv6_2( static_cast<int32_t>(strlen(ROOT_6_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_6_IPV6_2)),  (char*)ROOT_6_IPV6_2);
  static ObString s_root_6_ipv6_3( static_cast<int32_t>(strlen(ROOT_6_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_6_IPV6_3)),  (char*)ROOT_6_IPV6_3);
  static ObString s_root_6_ipv6_4( static_cast<int32_t>(strlen(ROOT_6_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_6_IPV6_4)),  (char*)ROOT_6_IPV6_4);
  static ObString s_root_6_ipv4(   static_cast<int32_t>(strlen(ROOT_6_IPV4)),    static_cast<int32_t>(strlen(ROOT_6_IPV4)),    (char*)ROOT_6_IPV4);
  static ObString s_root_6_tablet_version(static_cast<int32_t>(strlen(ROOT_6_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_6_TABLET_VERSION)), (char*)ROOT_6_TABLET_VERSION);
  //add:e


  static ObString s_root_occupy_size (static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), (char*)ROOT_OCCUPY_SIZE);
  static ObString s_root_record_count(static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), (char*)ROOT_RECORD_COUNT);

  int ret = OB_SUCCESS;
  UNUSED(max_row_key);
  UNUSED(max_key_len);
  const common::ObTabletInfo* tablet_info = NULL;
  int count = 0;
  for (ObRootTable2::const_iterator it = first; it <= last; it++)
  {
    if (count > max_row_count) break;
    tablet_info = ((const ObRootTable2*)root_table_)->get_tablet_info(it);
    if (tablet_info == NULL)
    {
      TBSYS_LOG(ERROR, "you should not reach this bugs");
      break;
    }
    out_cell.row_key_ = tablet_info->range_.end_key_;
    TBSYS_LOG(DEBUG,"add range %s",to_cstring(tablet_info->range_));
    TBSYS_LOG(DEBUG, "add a row key to out cell, rowkey = %s", to_cstring(out_cell.row_key_));
    count++;
    //start one row
    out_cell.column_name_ = s_root_occupy_size;
    out_cell.value_.set_int(tablet_info->occupy_size_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
    {
      break;
    }

    out_cell.column_name_ = s_root_record_count;
    out_cell.value_.set_int(tablet_info->row_count_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
    {
      break;
    }

    const ObServerStatus* server_status = NULL;
    //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
    int32_t c_id = cluster_id;
    if(cluster_id > 0)
    {
      int count = 0;
      for(int32_t i = 0; i < OB_MAX_COPY_COUNT; ++i)
      {
        if (it->server_info_indexes_[i] != OB_INVALID_INDEX &&
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[i])) != NULL
            && c_id == server_status->server_.cluster_id_)
        {
          count++;
        }
      }
      if(count == 0) //no replica in the cluster
      {
        c_id = get_master_cluster_id(); //master cluster
        count = 0;
        for(int32_t i = 0; i < OB_MAX_COPY_COUNT; ++i)
        {
          if (it->server_info_indexes_[i] != OB_INVALID_INDEX &&
              (server_status = server_manager_.get_server_status(it->server_info_indexes_[i])) != NULL
              && c_id == server_status->server_.cluster_id_)
          {
            count++;
          }
        }
        if(count == 0) //no replica in master cluster
        {
          c_id = 0; //all cluster
        }
      }
    }
    //add:e
    if (it->server_info_indexes_[0] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[0])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_1_port;
      out_cell.value_.set_int(server_status->port_cs_);

      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_1_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_1_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_1_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[0]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }
    if (it->server_info_indexes_[1] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[1])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_2_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_2_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_2_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_2_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[1]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }
    if (it->server_info_indexes_[2] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[2])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_3_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_3_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_3_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_3_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[2]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }

    //add zhaoqiong[roottable tablet management]20160104:b
    if (it->server_info_indexes_[3] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[3])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_4_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_4_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_4_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_4_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[3]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }

    if (it->server_info_indexes_[4] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[4])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_5_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_5_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_5_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_5_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[4]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }

    if (it->server_info_indexes_[5] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[5])) != NULL
        //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
        && (c_id == 0 || c_id == server_status->server_.cluster_id_)
        //add:e
        )
    {
      out_cell.column_name_ = s_root_6_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_6_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_6_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_6_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[5]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }
    //add:e

  } // end for each tablet
  return ret;
}

int ObRootServer2::find_monitor_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    //mod pangtianze [Paxos rs_election] 20150716:b
    //monitor_table.init(my_addr_, server_manager_, *ups_manager_);
    monitor_table.init(worker_->get_rs_master(), server_manager_, *ups_manager_);
    //mod:e
    ret = monitor_table.get(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_session_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    //mod pangtianze [Paxos rs_election] 20150716:b
    //monitor_table.init(my_addr_, server_manager_, *ups_manager_);
    monitor_table.init(worker_->get_rs_master(), server_manager_, *ups_manager_);
    //mod:e
    ret = monitor_table.get_ms_only(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_statement_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find statement table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    //mod pangtianze [Paxos rs_election] 20150716:b
    //monitor_table.init(my_addr_, server_manager_, *ups_manager_);
    monitor_table.init(worker_->get_rs_master(), server_manager_, *ups_manager_);
    //mod:e
    ret = monitor_table.get_ms_only(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner
                                       //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                                       , const int32_t cluster_id
                                       //add:e
                                       )
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else if (get_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
  {
    TBSYS_LOG(WARN, "we are not a master instance");
    ret = OB_NOT_MASTER;
  }
  else
  {
    int8_t rt_type = 0;
    UNUSED(rt_type); // for now we ignore this; OP_RT_TABLE_TYPE or OP_RT_TABLE_INDEX_TYPE

    if (cell->table_id_ != 0 && cell->table_id_ != OB_INVALID_ID)
    {
      TBSYS_LOG(DEBUG, "get table info, table_id=%ld", cell->table_id_);
      ObString table_name;
      int32_t max_key_len = 0;
      if (OB_SUCCESS != (ret = find_root_table_key(cell->table_id_, table_name, max_key_len,
              cell->row_key_, scanner
               //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
               , cluster_id
               //add:e
               )))
      {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu rowkey=%s",
            ret, cell->table_id_, to_cstring(cell->row_key_));
      }
    }
    else {
//      //add dolphin [database manager]@20150611
//      ret = OB_INVALID_ARGUMENT;
//      TBSYS_LOG(ERROR, "invalid table id for find_root_table_key");

      TBSYS_LOG(DEBUG, "get table info ");
      int32_t max_key_len = 0;
      uint64_t table_id = 0;
      ret = get_table_info(cell->table_name_, table_id, max_key_len);
      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "failed to get table id, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = find_root_table_key(table_id, cell->table_name_, max_key_len,
            cell->row_key_, scanner
            //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
            , cluster_id
            //add:e
            )))
      {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
      }
    }
  }
  return ret;
}

int ObRootServer2::get_rowkey_info(const uint64_t table_id, ObRowkeyInfo &info) const
{
  int ret = OB_SUCCESS;
  // @todo
  UNUSED(table_id);
  UNUSED(info);
  return ret;
}

int ObRootServer2::find_root_table_key(const uint64_t table_id, const ObString& table_name,
    const int32_t max_key_len, const common::ObRowkey& key, ObScanner& scanner
     //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
     , const int32_t cluster_id
     //add:e
     )
{
  int ret = OB_SUCCESS;
  if (table_id == OB_INVALID_ID || 0 == table_id)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_ == NULL)
    {
      ret = OB_NOT_INIT;
    }
    else
    {
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      ObRootTable2::const_iterator ptr;
      ret = root_table_->find_key(table_id, key, RETURN_BACH_COUNT, first, last, ptr);
      TBSYS_LOG(DEBUG, "first %p last %p ptr %p", first, last, ptr);
      if (ret == OB_SUCCESS)
      {
        //liumz, compare with end_key_, so make a fake MIN_ROWKEY
        if (first == ptr)
        {
          // make a fake startkey
          out_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          out_cell.row_key_ = ObRowkey::MIN_ROWKEY;
          ret = scanner.add_cell(out_cell);
          out_cell.value_.reset();
        }
        if (OB_SUCCESS == ret)
        {
          ret = make_out_cell(out_cell, first, last, scanner, RETURN_BACH_COUNT, max_key_len
                              //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                              , cluster_id
                              //add:e
                              );
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to make cell out. err=%d", ret);
          }
        }
      }
      else if (config_.is_import && OB_ERROR_OUT_OF_RANGE == ret)
      {
        TBSYS_LOG(WARN, "import application cann't privide service while importing");
        ret = OB_IMPORT_NOT_IN_SERVER;
      }
      else
      {
        TBSYS_LOG(WARN, "fail to find key. table_id=%lu, key=%s, ret=%d",
                  table_id, to_cstring(key), ret);
      }
    }
  }
  return ret;
}

int ObRootServer2::find_root_table_range(const common::ObScanParam& scan_param, ObScanner& scanner)
{
  const ObString &table_name = scan_param.get_table_name();
  const ObNewRange &key_range = *scan_param.get_range();
  int32_t max_key_len = 0;
  uint64_t table_id = 0;
  int ret = get_table_info(table_name, table_id, max_key_len);
  if (0 == table_id || OB_INVALID_ID == table_id)
  {
    TBSYS_LOG(WARN,"table name are invaild:%.*s", table_name.length(), table_name.ptr());
    ret = OB_INVALID_ARGUMENT;
  }
  else if (scan_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
  {
    TBSYS_LOG(INFO, "we are not a master instance");
    ret = OB_NOT_MASTER;
  }
  else
  {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_ == NULL)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN,"scan request in initialize phase");
    }
    else
    {
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      ObNewRange search_range = key_range;
      search_range.table_id_ = table_id;
      ret = root_table_->find_range(search_range, first, last);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"cann't find this range %s, ret[%d]", to_cstring(search_range), ret);
      }
      else
      {
        if ((ret = make_out_cell(out_cell, first, last, scanner,
                                 MAX_RETURN_BACH_ROW_COUNT, max_key_len)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"make out cell failed,ret[%d]",ret);
        }
      }
    }
  }
  return ret;
}

/*
 * CS姹囨姤tablet缁撴潫
 *
 */
int ObRootServer2::waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version)
{
  int ret = OB_ENTRY_NOT_EXIST;
  UNUSED(frozen_mem_version);
  ObChunkServerManager::iterator it;
  int64_t cs_index = get_server_index(server);
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end())
  {
    TBSYS_LOG(INFO, "cs waiting_job_done, status=%d", it->status_);
    if (is_master() && !is_bypass_process())
    {
      log_worker_->cs_merge_over(server, frozen_mem_version);
    }
    if (is_bypass_process())
    {
      operation_helper_.waiting_job_done(static_cast<int32_t>(cs_index));
    }
    else if (it->status_ == ObServerStatus::STATUS_REPORTING)
    {
      it->status_ = ObServerStatus::STATUS_SERVING;
    }
    ret = OB_SUCCESS;
  }
  return ret;
}
int ObRootServer2::get_server_index(const common::ObServer& server) const
{
  int ret = OB_INVALID_INDEX;
  ObChunkServerManager::const_iterator it;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end())
  {
    if (ObServerStatus::STATUS_DEAD != it->status_)
    {
      ret = static_cast<int32_t>(it - server_manager_.begin());
    }
  }
  return ret;
}

//add liumz, [secondary index static_index_build] 20150320:b
/*
 * receive reported histograms from CS
 */
int ObRootServer2::report_histograms(const ObServer& server, const ObTabletHistogramReportInfoList& tablets,
    const int64_t frozen_mem_version)
{
  int return_code = OB_SUCCESS;
  int server_index = get_server_index(server);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", to_cstring(server));
    return_code = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG_US(INFO, "[NOTICE] report histograms, server=%d ip=%s count=%ld version=%ld",
                 server_index, to_cstring(server),
                 tablets.tablet_list_.get_array_index(), frozen_mem_version);
//    if (is_master())
//    {
//      log_worker_->report_tablets(server, tablets, frozen_mem_version);
//    }
    //mod liumz, [paxos static index]20170626:b
    //return_code = got_reported_for_histograms(tablets, server_index, frozen_mem_version);
    return_code = got_reported_for_histograms(tablets, server.cluster_id_, server_index, frozen_mem_version);
    //mod:e
  }
  return return_code;
}
/*
 * deep copy reported histograms into hist_manager_, add an new entry in hist_table_
 */
//mod liumz, [paxos static index]20170626:b
//int ObRootServer2::got_reported_for_histograms(const common::ObTabletHistogramReportInfoList& add_tablets, const int server_index,
//    const int64_t frozen_mem_version, const bool for_bypass/*=false*/, const bool is_replay_log /*=false*/)
int ObRootServer2::got_reported_for_histograms(const common::ObTabletHistogramReportInfoList& add_tablets, const int32_t cluster_id, const int server_index,
    const int64_t frozen_mem_version, const bool for_bypass/*=false*/, const bool is_replay_log /*=false*/)
//mod:e
{
  UNUSED(frozen_mem_version);
  UNUSED(for_bypass);
  int ret = OB_SUCCESS;
  uint64_t idx_tid = OB_INVALID_ID;
  TBSYS_LOG(INFO, "will add histograms to hist_table_");
//  ObTabletHistogramReportInfoList add_tablets;
  if (ObBootState::OB_BOOT_OK != boot_state_.get_boot_state() && !is_replay_log)
  {
    TBSYS_LOG(INFO, "rs has not boot strap and is_replay_log=%c, refuse report.", is_replay_log?'Y':'N');
    //ret = OB_NOT_INIT; cs cant handle this... modify this in cs later
  }
  //mod liumz, [paxos static index]20170626:b
  //else if (NULL == hist_table_ || NULL == hist_manager_)
  else if (NULL == hist_table_ || NULL == hist_manager_ || NULL == hist_table_[cluster_id-1] || NULL == hist_manager_[cluster_id-1])
  //mod:e
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ or hist_manager_ is null.");
  }
  else
  {
    for (int64_t i = 0; i < add_tablets.tablet_list_.get_array_index() && OB_SUCCESS == ret; i ++)
    {
      tbsys::CThreadGuard hist_mutex_gard(&hist_table_mutex_);
      int32_t hist_index;//index indicates hist's location in hist_manager
      //mod liumz, no need to set server index, 20150512:b
      //ObTabletHistogramReportInfo &report_info = const_cast<ObTabletHistogramReportInfo &>(add_tablets.tablets_[i]);
      //report_info.static_index_histogram_.set_server_index(server_index);
      const ObTabletHistogramReportInfo &report_info = add_tablets.tablets_[i];
      //mod:e
      if (report_info.tablet_info_.range_.table_id_ != hist_manager_[cluster_id-1]->get_table_id())
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "tid is not consistent in hist_manager_ and report info, tid in hist_manager_ is %lu, tid in report info is %lu.", hist_manager_[cluster_id-1]->get_table_id(), report_info.tablet_info_.range_.table_id_);
      }
      else if (report_info.tablet_info_.range_.table_id_ != hist_table_[cluster_id-1]->get_table_id())
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "tid is not consistent in hist_table_ and report info, tid in hist_table_ is %lu, tid in report info is %lu.", hist_table_[cluster_id-1]->get_table_id(), report_info.tablet_info_.range_.table_id_);
      }
      else if (OB_SUCCESS != (ret = hist_manager_[cluster_id-1]->add_histogram(report_info.static_index_histogram_, hist_index, idx_tid)))
      {
        TBSYS_LOG(WARN, "add hist into hist_manager_ failed.");
      }
      else
      {
        //lock hist_table_ whenever write it
        //tbsys::CThreadGuard hist_mutex_gard(&hist_table_mutex_);
        if (OB_SUCCESS != (ret = hist_table_[cluster_id-1]->add_hist_meta(report_info.tablet_info_, hist_index, server_index, idx_tid)))
        {
          TBSYS_LOG(WARN, "add hist meta into hist_table_ failed.");
        }
        else
        {
          TBSYS_LOG(INFO, "add hist meta into hist_table_ succ.");
        }
        //hist_table_->dump();
      }
    }
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150630:b
int ObRootServer2::check_index_init_changed(const uint64_t index_tid, bool &is_changed)
{
  int ret = OB_SUCCESS;
  is_changed = false;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObTableSchema* table_schema = NULL;
  if (NULL == hist_table_ || NULL == root_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ or root_table_ is null.");
  }
  else if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed.");
  }
  else if (NULL == (table_schema = schema_mgr->get_table_schema(index_tid)))
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get table schema failed. tid=%ld", index_tid);
  }
  else if (INDEX_INIT != table_schema->get_index_helper().status)
  {
    is_changed = true;
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150323:b
//modify liumz, 20150324
/*
 * check if building local index done: all tablet has been reported
 */
int ObRootServer2::check_create_local_index_done(const uint64_t index_tid, bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;//add liumz, init is_finished first
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObTableSchema* table_schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  if (NULL == hist_table_ || NULL == root_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ or root_table_ is null.");
  }
  else if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed.");
  }
  else if (NULL == (table_schema = schema_mgr->get_table_schema(index_tid)))
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get table schema failed. tid=%ld", index_tid);
  }
  else
  {
    table_id = table_schema->get_index_helper().tbl_tid;
    //uint64_t table_id = hist_table_->get_table_id();
    common::ObRowkey min_rowkey;
    min_rowkey.set_min_row();
    common::ObRowkey max_rowkey;
    max_rowkey.set_max_row();
    common::ObNewRange start_range;
    start_range.table_id_ = table_id;
    start_range.end_key_ = min_rowkey;
    common::ObNewRange end_range;
    end_range.table_id_ = table_id;
    end_range.end_key_ = max_rowkey;

    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CRLockGuard guard(root_table_rwlock_);
    tbsys::CThreadGuard hist_mutex_gard(&hist_table_mutex_);

    ObRootTable2::const_iterator begin_pos = root_table_->find_pos_by_range(start_range);
    ObRootTable2::const_iterator end_pos = root_table_->find_pos_by_range(end_range);

    if (NULL == begin_pos || NULL == end_pos)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "find_pos_by_range failed, begin_pos or end_pos is NULL. ret=%d", ret);
    }
    else if (begin_pos > end_pos)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "find_pos_by_range failed, out of range. begin():[%p], end():[%p], begin:[%p], end:[%p], ret=%d",
                root_table_->begin(), root_table_->end(), begin_pos, end_pos, ret);
    }
    //add liumz, [paxos static index]20170606:b
    bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
    get_alive_cluster_with_cs(is_cluster_alive);
    //add:e
    for (ObRootTable2::const_iterator it = begin_pos; it <= end_pos && OB_LIKELY(OB_SUCCESS == ret); it++)
    {
      const ObTabletInfo* compare_tablet = NULL;
      compare_tablet = root_table_->get_tablet_info(it);
      TBSYS_LOG(DEBUG, "test::lmz, compared tablet info [%s]", to_cstring(compare_tablet->range_));
      if (NULL == compare_tablet)
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "compared tablet info is null. ret=%d", ret);
      }
      //mod liumz, [paxos static index]20170606:b
      for (int i = 0; i < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; i++)
      {
        if (is_cluster_alive[i+1] && NULL != hist_table_[i])
        {
          if (OB_SUCCESS != (ret = hist_table_[i]->check_reported_hist_info(compare_tablet, is_finished)))
          {
            TBSYS_LOG(INFO, "build local static index has not finished. ret=%d", ret);
          }
          //add liumz, [secondary_index_bugfix: break loop if is_finished==false]20160324:b
          else if (!is_finished)
          {
            break;
          }
          //add:e
          TBSYS_LOG(DEBUG, "test::lmz, cluster_id[%d], is_finished [%d]", i+1, is_finished);
        }
      }
      if (!is_finished)
      {
        break;
      }
      //mod:e
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//fill reported range's rowkey into sorted list, used to split global index range
int ObRootServer2::fill_all_samples()
{
  int ret = OB_SUCCESS;
  if (NULL == hist_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ is null.");
  }
  else
  {
    //mod liumz, [paxos static index]20170606:b
    bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
    get_alive_cluster_with_cs(is_cluster_alive);
    for (int i = 0; i < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; i++)
    {
      if (is_cluster_alive[i+1] && NULL != hist_table_[i])
      {
      //add:e
        tbsys::CThreadGuard hist_mutex_gard(&hist_table_mutex_);
        //hist_table_->dump();//add liumz, [dump hist_table_]20151207
        ret = hist_table_[i]->fill_all_ranges();
      }
    }
    //mod:e
  }
  return ret;
}
//get root server's tablet_manager_
int ObRootServer2::get_tablet_manager(ObTabletInfoManager *&tablet_manager) const
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "tablet_manager_ is null.");
  }
  else
  {
    tablet_manager = tablet_manager_;
  }
  return ret;
}

/*
 * check if create global index succ:
 * V1: check if all tablet's row_checksum not equal 0, [logical bug when 0 rows]
 * V2: check if all tablet's tablet_version_ != OB_INVALID_VERSION, tablet_version_ == OB_INVALID_VERSION when splitting range
 */
int ObRootServer2::check_create_global_index_done(const uint64_t index_tid, bool &is_finished) const
{
  int ret = OB_SUCCESS;
  is_finished = false;//add liumz, init is_finished first
  if (NULL == root_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  else
  {
    //uint64_t index_tid = hist_table_->get_index_tid();
    common::ObRowkey min_rowkey;
    min_rowkey.set_min_row();
    common::ObRowkey max_rowkey;
    max_rowkey.set_max_row();
    common::ObNewRange start_range;
    start_range.table_id_ = index_tid;
    start_range.end_key_ = min_rowkey;
    common::ObNewRange end_range;
    end_range.table_id_ = index_tid;
    end_range.end_key_ = max_rowkey;

    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CRLockGuard guard(root_table_rwlock_);

    ObRootTable2::const_iterator begin_pos = root_table_->find_pos_by_range(start_range);
    ObRootTable2::const_iterator end_pos = root_table_->find_pos_by_range(end_range);

    if (NULL == begin_pos || NULL == end_pos)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "find_pos_by_range failed, begin_pos or end_pos is NULL. ret=%d", ret);
    }
    else if (begin_pos > end_pos)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "find_pos_by_range failed, out of range. begin():[%p], end():[%p], begin:[%p], end:[%p], ret=%d",
                root_table_->begin(), root_table_->end(), begin_pos, end_pos, ret);
    }
    //add liumz, [paxos static index]20170606:b
    bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
    get_alive_cluster_with_cs(is_cluster_alive);
    //add:e
    for (ObRootTable2::const_iterator it = begin_pos; it <= end_pos && OB_SUCCESS == ret; it++)
    {
      //mod liumz, [paxos static index]20170626:b
      for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT; cluster_idx++)
      {
        if (is_cluster_alive[cluster_idx+1])
        {
          //modify liumz, [modify check logic] 20150409:b
          is_finished = false;
          for (int32_t idx = 0; idx < OB_MAX_COPY_COUNT; idx++)
          {
            int32_t cluster_id = server_manager_.get_cs(it->server_info_indexes_[idx]).cluster_id_;
            if (OB_INVALID_INDEX != it->server_info_indexes_[idx] && OB_INVALID_VERSION != it->tablet_version_[idx] && (cluster_idx+1) == cluster_id)
            {
              is_finished = true;
              break;
            }
          }
          if (!is_finished)
          {
            break;
          }
        }
      }
      //mod:e
      if (!is_finished)
      {
        TBSYS_LOG(INFO, "create global static index has not finished");
        break;
      }
      //modify:e
      /*
      const ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      //ObssTabletInfo *p_tablet_info = root_table_->get_tablet_info(it);
      ObTabletCrcHistoryHelper *crc_helper = root_table_->get_crc_helper(it);
      uint64_t row_checksum = 0;
      if (OB_INVALID_INDEX == server_index)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "global index tablet has no valid copy");
        it->dump();
      }
      else if (NULL == new_server_status)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "server status wrong, server index=%d", server_index);
      }
      //TODO, check which version's row_checksum, frozen_version?
      else if (OB_SUCCESS != (ret = crc_helper->get_row_checksum(get_last_frozen_version(), row_checksum)))
      {
        TBSYS_LOG(WARN, "crc helper get row checksum error. ret=%d", ret);
      }
      else if (0 == row_checksum)
      {
        ret = OB_EAGAIN;
        TBSYS_LOG(INFO, "create global static index has not finished. ret=%d", ret);
      }
      */
    }
  }
  return ret;
}

//add liumz, [secondary index static_index_build] 20150325:b
/*
 * split global index range and write info into rt.
 * iterate hist_table_, get one tablet_info everytime, and store tablet_info in ObTabletInfoList, classified by CS
 * then call write_tablet_info_list_to_rt(), batch report constructed ObTabletReportInfoList
 */
int ObRootServer2::write_global_index_range(const int64_t sample_num)
{
  int ret = OB_SUCCESS;
  uint64_t index_tid = OB_INVALID_ID;
  //mod liumz, [max list size = OB_MAX_COPY_COUNT]201512110:b
  int32_t server_count = static_cast<int32_t>(server_manager_.size());
  //int32_t server_count = OB_MAX_COPY_COUNT;
  //mod:e
  ObIndexTabletInfoList* p_info_list[OB_MAX_COPY_COUNT][server_count];

  if (NULL == hist_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "hist_table_ is null.");
  }
  else
  {
    //mod liumz, [paxos static index]20170606:b
    bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
    get_alive_cluster_with_cs(is_cluster_alive);
    int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
    memset(replicas_nums, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
    get_cluster_tablet_replicas_num(replicas_nums);

    for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; cluster_idx++)
    {
      if (is_cluster_alive[cluster_idx+1] && NULL != hist_table_[cluster_idx])
      {
        //reset for each cluster
        for (int32_t j = 0; j< OB_MAX_COPY_COUNT; j++)
        {
          for (int32_t i = 0; i< server_count; i++)
          {
            p_info_list[j][i] = NULL;
          }
        }
        tbsys::CThreadGuard hist_mutex_gard(&hist_table_mutex_);
        {//block
          index_tid = hist_table_[cluster_idx]->get_index_tid();
          while (hist_table_[cluster_idx]->next_srw() && OB_SUCCESS == ret)
          {
            ObTabletInfo tablet_info;
            //TODO, ObTabletInfo.version_
            //int32_t copy_count = OB_MAX_COPY_COUNT;
            int32_t copy_count = replicas_nums[cluster_idx+1];//replicas num for each cluster
            int32_t server_index[copy_count];
            for (int32_t i = 0; i < copy_count; i++)
            {
              server_index[i] = OB_INVALID_INDEX;
            }
            //split global index range, get one tablet_info and it's server_info_index array
            if (OB_SUCCESS == (ret = hist_table_[cluster_idx]->get_next_tablet_info(sample_num, tablet_info, server_index, copy_count, cluster_idx+1)))
            {
              //add liumz, [dump server_index]20151207:b
              TBSYS_LOG(INFO, "tablet[%s]", to_cstring(tablet_info.range_));
              for (int32_t i = 0; i < copy_count; i++)
              {
                TBSYS_LOG(INFO, "server_index[%d]", server_index[i]);
              }
              //add:e
              for (int32_t j = 0; j< OB_MAX_COPY_COUNT && OB_SUCCESS == ret; j++)
              {
                if (OB_SUCCESS != (ret = fill_tablet_info_list_by_server_index(p_info_list[j], server_count, tablet_info, server_index, copy_count, j)))
                {
                  TBSYS_LOG(WARN, "fill tablet info list error. ret=%d", ret);
                }
              }
            }
            else
            {
              TBSYS_LOG(WARN, "fail get tablet info. ret=%d", ret);
            }
          }//end while
        }//block
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "will write global index range into rt for query, index table id = [%lu]", index_tid);
          //comment liumz, will switch root table
          for (int32_t j = 0; j< OB_MAX_COPY_COUNT && OB_SUCCESS == ret; j++)
          {
            if (OB_SUCCESS != (ret = write_tablet_info_list_to_rt(p_info_list[j], server_count)))
            {
              TBSYS_LOG(WARN, "write tablet info list to rt error. ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "write global index range into rt succ, index table id = [%lu]", index_tid);
            }
          }
        }
      }
    }//end for
    //mod:e
  }

  for (int32_t j = 0; j< OB_MAX_COPY_COUNT; j++)
  {
    for (int32_t i = 0; i< server_count; i++)
    {
      OB_DELETE(ObIndexTabletInfoList, ObModIds::OB_STATIC_INDEX_HISTOGRAM, p_info_list[j][i]);
    }
  }
  if (OB_SUCCESS == ret)
  {
    //dump_root_table();
  }

  return ret;
}

//fill tablet info into tablet_info_list array, classified by CS[server_info_index]
/**
 * @param array of ObTabletInfoList
 * @param size of array
 * @param ObTabletInfo
 * @param server_info_index array, which indicates which cs hold the ObTabletInfo
 * @param size of server_info_index array
 *
 * @return int if success,return OB_SUCCESS, else return
 *         OB_ERROR
 */
int ObRootServer2::fill_tablet_info_list_by_server_index(ObIndexTabletInfoList *tablet_info_list[], const int32_t list_size, const ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count, const int32_t replica_order)
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_info_list || NULL == server_index)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "ObTabletInfoList* tablet_info_list[%p] or server_index[%p] is null.", tablet_info_list, server_index);
  }
  for (int32_t i = 0; i < copy_count && OB_SUCCESS == ret; i++)
  {
    if (i != replica_order)
    {
      continue;
    }
    int32_t idx = server_index[i];
    if (OB_INVALID_INDEX != idx)
    {
      if (idx < 0 || idx >= list_size)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "list_size:[%d], server_info_index:[%d]. ret=%d", list_size, idx, ret);
      }
      else if (NULL != tablet_info_list[idx])
      {
        if (OB_SUCCESS != (ret = tablet_info_list[idx]->add_tablet(tablet_info)))
        {
          TBSYS_LOG(WARN, "fail add tablet report info. ret=%d", ret);
        }
      }
      else if (NULL != (tablet_info_list[idx] = OB_NEW(ObIndexTabletInfoList,  ObModIds::OB_STATIC_INDEX_HISTOGRAM)))
      {
        if (OB_SUCCESS != (ret = tablet_info_list[idx]->add_tablet(tablet_info)))
        {
          TBSYS_LOG(WARN, "fail add tablet report info. ret=%d", ret);
        }
      }
      else
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "new ObTabletInfoList failed. ret=%d", ret);
      }
    }
    //del liumz, 20150513:b
    /*
    else
    {
      break;
    }*/
    //del:e
  }//end for
  return ret;
}

//batch write tablet_info into root table
int ObRootServer2::write_tablet_info_list_to_rt(ObIndexTabletInfoList **tablet_info_list, const int32_t list_size)
{
  int ret = OB_SUCCESS;
  ObIndexTabletReportInfoList add_tablet_list;
  ObTabletReportInfo report_info;
  if (NULL == tablet_info_list)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "ObTabletInfoList* tablet_info_list[] is null.");
  }
  for (int32_t server_idx = 0; server_idx < list_size && OB_SUCCESS == ret; server_idx++)
  {
    add_tablet_list.reset();//reset list
    if (NULL != tablet_info_list[server_idx])
    {
      ObServer cs = server_manager_.get_cs(server_idx);
      for (int64_t tablet_idx = 0; tablet_idx < tablet_info_list[server_idx]->get_tablet_size() && OB_SUCCESS == ret; tablet_idx++)
      {
        ObTabletInfo &tablet_info = tablet_info_list[server_idx]->tablets[tablet_idx];
        //set tablet_version_ = OB_INVALID_VERSION, used to check if it is a valid copy in rt.
        //see @check_create_global_index_done()
        ObTabletLocation tablet_location(OB_INVALID_VERSION, cs);
        report_info.tablet_location_ = tablet_location;
        report_info.tablet_info_ = tablet_info;
        if (OB_SUCCESS != (ret = add_tablet_list.add_tablet(report_info)))
        {
          TBSYS_LOG(WARN, "fail add tablet report info. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        //add liumz, [check cs status]20151217:b
        //if one range's all replica cs is offline, this range will not be builded!!!
        ObServerStatus* server_status = server_manager_.get_server_status(server_idx);
        if (NULL == server_status)
        {
          TBSYS_LOG(WARN, "can not find server");
        }
        else if (server_status->status_ != ObServerStatus::STATUS_DEAD && server_status->port_cs_ > 0)
        {
        //add:e
          ObTabletReportInfoList tablet_report_list;
          for (int64_t i = 0; i <= add_tablet_list.get_tablet_size() && OB_SUCCESS == ret;)
          {
            tablet_report_list.reset();
            for (int64_t j = 0; j < OB_MAX_TABLET_LIST_NUMBER && (i + j) < add_tablet_list.get_tablet_size() && OB_SUCCESS == ret; j++)
            {
              ret = tablet_report_list.add_tablet(add_tablet_list.tablets_[i+j]);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "failed to add tablet to report list, ret=%d", ret);
              }
            }
            if (OB_SUCCESS == ret)
            {
              // write add_tablet_list into rt.
              //mod liumz, [write global range into rs commitlog]20160616:b
              if (OB_SUCCESS != (ret = report_tablets(cs, tablet_report_list, get_last_frozen_version())))
              //if (OB_SUCCESS != (ret = got_reported(add_tablet_list, server_idx, get_last_frozen_version())))
              //mod:e
              {
                TBSYS_LOG(ERROR, "fail add global index range into root table. ret=%d", ret);
              }
            }
            i += OB_MAX_TABLET_LIST_NUMBER;
          }//end for
        }
      }
    }
  }//end for
  return ret;
}
//add:e

//add wenghaixing [secondary index.cluster]20150629
int ObRootServer2::fetch_init_index(const int64_t version, ObArray<uint64_t> *list)
{
  int ret = OB_SUCCESS;
  //ObGeneralRpcStub rpc_stub;
  //ObRootRpcStub root_rpc;
  ObServer ups;
  int64_t timeout = 3000000;
  if(NULL == worker_ || NULL == list)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "root worker cannot be NULL");
  }
  //TBSYS_LOG(ERROR, "test::whx start_version[%ld]",version);
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = worker_->get_rpc_stub().get_master_ups_info(my_addr_, ups, timeout)))
    {
      TBSYS_LOG(WARN, "get obi ups failed, ret = %d", ret);
    }
    else if(OB_SUCCESS != (ret = worker_->get_rpc_stub().get_init_index_from_ups(ups, config_.monitor_row_checksum_timeout, version, list)))
    {
      TBSYS_LOG(WARN, "fetch init index from ups[%s] failed, version[%ld], ret[%d]", to_cstring(ups),version, ret);
    }
  }
  return ret;
}

//mod liumz, [paxos static index]20170626:b
//int ObRootServer2::modify_index_process_info(const uint64_t index_tid, const IndexStatus stat)
int ObRootServer2::modify_index_process_info(const int32_t cluster_id, const uint64_t index_tid, const IndexStatus stat)
//mod:e
{
  int ret = OB_SUCCESS;
  ObMutator* mutator = NULL;
  ObServer master_master_ups;
  ObServer obi_rs;
  ObGeneralRpcStub rpc_stub;
  int64_t timeout = 3000000;
  ObRowkey rowkey;
  ObObj obj[2];
  ObObj status;
  if(NULL == worker_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "root worker cannot be NULL");
  }
  else
  {
    rpc_stub = worker_->get_general_rpc_stub();
  }
  //1. first, we get a master ups in master cluster.
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = rpc_stub.get_master_obi_rs(timeout, my_addr_, obi_rs)))
    {
      TBSYS_LOG(WARN, "get obi rs failed, ret = %d", ret);
    }
    //delete by liuxiao [secondary index.cluster bug fixed]
    //获取主集群主UPS放在后面发mutator的时候一起做，防止ups切换后，即使重试也不刷新master ups信息
    //else if(OB_SUCCESS != (ret = rpc_stub.get_master_ups_info(timeout, obi_rs, master_master_ups)))
    //{
    //  TBSYS_LOG(WARN, "get obi ups failed, ret = %d", ret);
    //}
    //delete e
  }
  //2. next, we construct mutator to apply
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator failed");
    }
    else if(OB_SUCCESS != (ret = mutator->reset()))
    {
      TBSYS_LOG(WARN, "reset mutator failed, ret[%d]", ret);
    }
    else
    {
      uint64_t stat_cid = 18;
      obj[0].set_int(index_tid);
      //mod pangtianze, liumz [second index for Paxos] 20170502:b
      //obj[1].set_int(config_.cluster_id);
      obj[1].set_int(cluster_id);
      //mode:e
      status.set_int(stat);
      rowkey.assign(obj, 2);

      if(OB_SUCCESS != (ret = mutator->insert(OB_INDEX_PROCESS_TID, rowkey, stat_cid, status)))
      {
        TBSYS_LOG(WARN, "construct mutator for modify index process info failed, ret[%d]", ret);
      }
    }
  }

  //3. finally, we send mutator to apply
  ObScanner* scanner=NULL;
  if(OB_SUCCESS == ret)
  {
    int64_t retry_times = config_.retry_times;
    for(int64_t i = 0; i < retry_times; i++)
    {
      //usleep(5000000);
      //modify liuxiao[secondary index.cluster]
      //if(OB_SUCCESS != (ret = rpc_stub.mutate(timeout, master_master_ups, *mutator, false, *scanner)))
      //将master ups获取的操作移动到此处
      //防止ups切换后，retry时不更新新UPS的地址导致无论重试多少次也只会失败的情况
      if(OB_SUCCESS != (ret = rpc_stub.get_master_ups_info(timeout, obi_rs, master_master_ups)))
      {
        TBSYS_LOG(WARN, "failed to get master master ups, ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = rpc_stub.mutate(timeout, master_master_ups, *mutator, false, *scanner)))
      //modify e
      {
        TBSYS_LOG(WARN, "replace mutator failed [%ld] times, ret[%d]", i, ret);
        //usleep(5000000);
      }
      else
      {
        break;
      }
      usleep(5000000);
    }
  }
  return ret;
}

//add e

//add liumz, [secondary index static_index_build] 20150331:b
//modify liuxiao [secondary index] 20150714
//本次修改，将锁的位置调整，防止获得索引表名后，为进行modify操作前schema中的索引表信息已经被drop掉的情况
int ObRootServer2::modify_index_stat(const uint64_t index_tid, const IndexStatus stat)
{
  int ret = OB_SUCCESS;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObTableSchema* table_schema = NULL;
  ObString index_name;
  bool need_update_schema = false;  
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    ObSchemaMutator schema_mutator;
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
      {
        TBSYS_LOG(WARN, "get schema manager failed.");
      }
      else
      {
        table_schema = schema_mgr->get_table_schema(index_tid);
        if (NULL == table_schema)
        {
          ret = OB_SCHEMA_ERROR;
          TBSYS_LOG(WARN, "get table schema failed. tid=%ld", index_tid);
        }
        else if (table_schema->get_index_helper().status == stat ||
                 ((AVALIBALE == table_schema->get_index_helper().status ||
                  WRITE_ONLY == table_schema->get_index_helper().status) &&
                  NOT_AVALIBALE == stat))
        {
          //available->not_available | write_only->not_available not allowed!
          ret = OB_STATE_NOT_MATCH;
          TBSYS_LOG(WARN, "try modify index stat, index name[%lu], stat: %d->%d",
                    index_tid, table_schema->get_index_helper().status, stat);
        }
        else
        {
          index_name = ObString::make_string(table_schema->get_table_name());
          //modify liuxiao [secondary index] 20150714
          //转移将锁的位置前移，避免获得索引表名至modify索引状态之间该表已经从内部表中删除的情况
          //tbsys::CThreadGuard guard(&mutex_lock_);
          //modify e
          //need_update_schema = true;//del liumz, [obsolete trigger event, use ddl_operation]20150701
          //add liuxiao [muti database] 20150702
          ObString db_name;
          ObString org_table_name;
          char ptr_db_name[OB_MAX_DATBASE_NAME_LENGTH];//mod liumz [name_length]
          db_name.assign_buffer(ptr_db_name, OB_MAX_DATBASE_NAME_LENGTH);//mod liumz [name_length]
          char ptr_table_name[OB_MAX_TABLE_NAME_LENGTH];//mod liumz [name_length]
          org_table_name.assign_buffer(ptr_table_name, OB_MAX_TABLE_NAME_LENGTH);//mod liumz [name_length]
          bool split_ret = index_name.split_two(db_name, org_table_name);
          if(!split_ret)
          {
            TBSYS_LOG(WARN, "split table_name failed, table_name:%.*s",index_name.length(), index_name.ptr());
            ret = OB_ERROR;
          }
          //add:e
          if(OB_SUCCESS == ret)
          {
            //tbsys::CThreadGuard guard(&mutex_lock_);
            ret = ddl_tool_.modify_index_stat(org_table_name,index_tid,db_name,stat);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "modify index stat failed, index name[%.*s], stat: %d->%d, ret[%d]",
                    index_name.length(), index_name.ptr(), table_schema->get_index_helper().status, stat, ret);
            }
            else
            {
              need_update_schema = true;//add liumz, [obsolete trigger event, use ddl_operation]20150701
              TBSYS_LOG(INFO, "modify index stat succ, index name[%.*s], stat: %d->%d",
                    index_name.length(), index_name.ptr(), table_schema->get_index_helper().status, stat);
            }
          }
          //add liumz, [obsolete trigger event, use ddl_operation]20150701:b
          if (need_update_schema)
          {
            int64_t count = 0;
            // if refresh failed output error log because maybe do succ if reboot
            int err = refresh_new_schema(count,schema_mutator);
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "refresh new schema manager after modify index stat failed:"
                  "err[%d], ret[%d]", err, ret);
              ret = err;
            }
          }
        }
      }
    }

    // notify schema update to all servers
    if (OB_SUCCESS == ret && need_update_schema)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    //add:e

    //del liumz, [obsolete trigger event, use ddl_operation]20150701:b
    /*//refresh new schema
    if(need_update_schema)
    {
      int64_t count = 0;
      // if refresh failed output error log because maybe do succ if reboot
      int err = refresh_new_schema(count);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "refresh new schema manager after modify_index_stat failed:"
                  "err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters, no matter notify_switch_schema() succ or fail.
    {
      int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }*/
    //del:e
    if (OB_STATE_NOT_MATCH == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//modify:e
//add:e

//add liumz, [secondary index static_index_build] 20150721:b
int ObRootServer2::modify_index_stat(const ObArray<uint64_t> &index_tid_list, const IndexStatus stat)
{
  int ret = OB_SUCCESS;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObTableSchema* table_schema = NULL;
  ObString index_name;
  bool need_update_schema = false;
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    ObSchemaMutator schema_mutator;
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
      {
        TBSYS_LOG(WARN, "get schema manager failed.");
      }
      else
      {
        for (int64_t i = 0; i < index_tid_list.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
        {
          uint64_t index_tid = index_tid_list.at(i);
          table_schema = schema_mgr->get_table_schema(index_tid);
          if (NULL == table_schema)
          {
            ret = OB_SCHEMA_ERROR;
            TBSYS_LOG(WARN, "get table schema failed. tid=%ld", index_tid);
          }
          else if (table_schema->get_index_helper().status == stat ||
                   ((AVALIBALE == table_schema->get_index_helper().status ||
                     WRITE_ONLY == table_schema->get_index_helper().status) &&
                    NOT_AVALIBALE == stat))
          {
            //available->not_available | write_only->not_available not allowed!
            TBSYS_LOG(WARN, "try modify index stat, index name[%lu], stat: %d->%d",
                      index_tid, table_schema->get_index_helper().status, stat);
            continue;
          }
          else
          {
            index_name = ObString::make_string(table_schema->get_table_name());
            ObString db_name;
            ObString org_table_name;
            char ptr_db_name[OB_MAX_DATBASE_NAME_LENGTH];//mod liumz [name_length]
            db_name.assign_buffer(ptr_db_name, OB_MAX_DATBASE_NAME_LENGTH);//mod liumz [name_length]
            char ptr_table_name[OB_MAX_TABLE_NAME_LENGTH];//mod liumz [name_length]
            org_table_name.assign_buffer(ptr_table_name, OB_MAX_TABLE_NAME_LENGTH);//mod liumz [name_length]
            bool split_ret = index_name.split_two(db_name, org_table_name);
            if(!split_ret)
            {
              TBSYS_LOG(WARN, "split table_name failed, table_name:%.*s",index_name.length(), index_name.ptr());
              ret = OB_ERROR;
            }
            if(OB_SUCCESS == ret)
            {
              ret = ddl_tool_.modify_index_stat(org_table_name,index_tid,db_name,stat);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "modify index stat failed, index name[%.*s], stat: %d->%d, ret[%d]",
                          index_name.length(), index_name.ptr(), table_schema->get_index_helper().status, stat, ret);
              }
              else
              {
                need_update_schema = true;
                TBSYS_LOG(INFO, "modify index stat succ, index name[%.*s], stat: %d->%d",
                          index_name.length(), index_name.ptr(), table_schema->get_index_helper().status, stat);
              }
            }
          }
        }//end for
        //add liumz, [obsolete trigger event, use ddl_operation]20150701:b
        if (need_update_schema)
        {
          int64_t count = 0;
          // if refresh failed output error log because maybe do succ if reboot
          int err = refresh_new_schema(count,schema_mutator);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "refresh new schema manager after modify index stat failed:"
                      "err[%d], ret[%d]", err, ret);
            ret = err;
          }
        }//end if
      }
    }

    // notify schema update to all servers
    if (OB_SUCCESS == ret && need_update_schema)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    //add:e

    //del liumz, [obsolete trigger event, use ddl_operation]20150701:b
    /*//refresh new schema
    if(need_update_schema)
    {
      int64_t count = 0;
      // if refresh failed output error log because maybe do succ if reboot
      int err = refresh_new_schema(count);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "refresh new schema manager after modify_index_stat failed:"
                  "err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters, no matter notify_switch_schema() succ or fail.
    {
      int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }*/
    //del:e
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150410:b
//delete illusive server_info_index of global index tablet in rt, trigger balance condition
int ObRootServer2::trigger_balance_index(const uint64_t index_tid)
{
  int ret = OB_SUCCESS;
  if (NULL == root_table_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "root_table_ is null.");
  }
  else
  {
//    uint64_t index_tid = hist_table_->get_index_tid();
    common::ObRowkey min_rowkey;
    min_rowkey.set_min_row();
    common::ObRowkey max_rowkey;
    max_rowkey.set_max_row();
    common::ObNewRange start_range;
    start_range.table_id_ = index_tid;
    start_range.end_key_ = min_rowkey;
    common::ObNewRange end_range;
    end_range.table_id_ = index_tid;
    end_range.end_key_ = max_rowkey;

    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CRLockGuard guard(root_table_rwlock_);

    ObRootTable2::const_iterator begin_pos = root_table_->find_pos_by_range(start_range);
    ObRootTable2::const_iterator end_pos = root_table_->find_pos_by_range(end_range);

    if (NULL == begin_pos || NULL == end_pos)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "find_pos_by_range failed, begin_pos or end_pos is NULL. ret=%d", ret);
    }
    else if (root_table_->end() == begin_pos || root_table_->end() == end_pos || begin_pos > end_pos)
    {
      ret = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "find_pos_by_range failed, out of range. begin():[%p], end():[%p], begin:[%p], end:[%p], ret=%d",
                root_table_->begin(), root_table_->end(), begin_pos, end_pos, ret);
    }
    for (ObRootTable2::const_iterator it = begin_pos; it <= end_pos && OB_LIKELY(OB_SUCCESS == ret); it++)
    {
      for (int32_t idx = 0; idx < OB_MAX_COPY_COUNT; idx++)
      {
        if (OB_INVALID_INDEX != it->server_info_indexes_[idx] && OB_INVALID_VERSION == it->tablet_version_[idx])
        {
          //modify to OB_INVALID_INDEX, trigger balance
          atomic_exchange((uint32_t*) &(it->server_info_indexes_[idx]), OB_INVALID_INDEX);
        }
      }
    }
  }
  return ret;
}
//add:e


int ObRootServer2::report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets,
    const int64_t frozen_mem_version)
{
  int return_code = OB_SUCCESS;
  int server_index = get_server_index(server);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", to_cstring(server));
    return_code = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG_US(INFO, "[NOTICE] report tablets, server=%d ip=%s count=%ld version=%ld",
                 server_index, to_cstring(server),
                 tablets.tablet_list_.get_array_index(), frozen_mem_version);
    if (is_master())
    {
      log_worker_->report_tablets(server, tablets, frozen_mem_version);
    }
    return_code = got_reported(tablets, server_index, frozen_mem_version);
    TBSYS_LOG_US(INFO, "got_reported over");
  }
  return return_code;
}

/*
 * 鏀跺埌姹囨姤娑堟伅鍚庤皟鐢?
 */
int ObRootServer2::got_reported(const ObTabletReportInfoList& tablets, const int server_index,
    const int64_t frozen_mem_version, const bool for_bypass/*=false*/, const bool is_replay_log /*=false*/)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "will add tablet info to root_table_for_query");
  delete_list_.reset();
  ObTabletReportInfoList add_tablet;
  if (ObBootState::OB_BOOT_OK != boot_state_.get_boot_state() && !is_replay_log
        //add pangtianze [Paxos] 20170329 allow to receive report tablet in boot recover stat
        && ObBootState::OB_BOOT_RECOVER != boot_state_.get_boot_state()
        //add:e
      )

  {
    TBSYS_LOG(INFO, "rs has not boot strap and is_replay_log=%c, refuse report.", is_replay_log?'Y':'N');
    //ret = OB_NOT_INIT; cs cant handle this... modify this in cs later
  }
  else
  {
    for (int64_t i = 0; i < tablets.get_tablet_size() && OB_SUCCESS == ret; i ++)
    {
      ret = add_tablet.add_tablet(tablets.tablets_[i]);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to add tablet to report list, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = got_reported_for_query_table(add_tablet, server_index, frozen_mem_version, for_bypass);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to do got_reported_for_query_table, ret=%d", ret);
      }
    }
    if (0 < delete_list_.get_tablet_size())
    {
      if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
      {
        //ObRootUtil::delete_tablets(worker_->get_rpc_stub(), server_manager_, delete_list_, config_.network_timeout);
        TBSYS_LOG(INFO, "need to delete tablet. tablet_num=%ld", delete_list_.get_tablet_size());
        worker_->submit_delete_tablets_task(delete_list_);
      }
    }
  }
  return ret;
}
/*
 * 澶勭悊姹囨姤娑堟伅, 鐩存帴鍐欏埌褰撳墠鐨剅oot table涓?
 * 濡傛灉鍙戠幇姹囨姤娑堟伅涓湁瀵瑰綋鍓峳oot table鐨則ablet鐨勫垎瑁傛垨鑰呭悎骞?
 * 瑕佽皟鐢ㄩ噰鐢ㄥ啓鎷疯礉鏈哄埗鐨勫鐞嗗嚱鏁?
 */
int ObRootServer2::got_reported_for_query_table(const ObTabletReportInfoList& tablets,
    const int32_t server_index, const int64_t frozen_mem_version, const bool for_bypass)
{
  UNUSED(frozen_mem_version);
  int ret = OB_SUCCESS;
  int64_t have_done_index = 0;
  bool need_split = false;
  bool need_add = false;

  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);

  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  /*
  if (state_.get_bypass_flag() && !is_loading_data())
  {
    ret = operation_helper_.report_tablets(tablets, server_index, frozen_mem_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to do report tablet. ret=%d", ret);
    }
  }
  else
  {
  */
  //del:e
    if (new_server_status == NULL && !for_bypass)
    {
      TBSYS_LOG(ERROR, "can not find server");
    }
    else
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      ObTabletReportInfo* p_table_info = NULL;
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      int64_t index = tablets.tablet_list_.get_array_index();
      int find_ret = OB_SUCCESS;
      common::ObTabletInfo* tablet_info = NULL;
      int range_pos_type = ObRootTable2::POS_TYPE_ERROR;

      for(have_done_index = 0; have_done_index < index; ++have_done_index)
      {
        p_table_info = tablets.tablet_list_.at(have_done_index);
        if (p_table_info != NULL)
        {
          //TODO(maoqi) check the table of this tablet is exist in schema
          //if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
          //  continue;
          if (!p_table_info->tablet_info_.range_.is_left_open_right_closed())
          {
            TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d range=%s", server_index, to_cstring(p_table_info->tablet_info_.range_));
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs reported  tablet, server=%d range=%s", server_index, to_cstring(p_table_info->tablet_info_.range_));
          }
          tablet_info = NULL;
          find_ret = root_table_->find_range(p_table_info->tablet_info_.range_, first, last);
          TBSYS_LOG(DEBUG, "root_table_for_query_->find_range ret = %d", find_ret);
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
          if (OB_SUCCESS == find_ret)
          {
            tablet_info = root_table_->get_tablet_info(first);
            if (NULL != tablet_info)
            {
              range_pos_type = root_table_->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
              TBSYS_LOG(DEBUG, " range_pos_type = %d", range_pos_type);
            }
            else
            {
              TBSYS_LOG(ERROR, "no tablet_info found");
            }
          }
          else if (OB_FIND_OUT_OF_RANGE == find_ret)
          {
            range_pos_type = ObRootTable2::POS_TYPE_ADD_RANGE;
            need_add = true;
            break;
          }

          if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
          {
            need_split = true;  //will create a new table to deal with the left
            break;
          }
          else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
          {
            need_add = true;
            break;
          }

          if (NULL != tablet_info &&
              (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
             )
          {
            if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                  p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_))
            {
              TBSYS_LOG(ERROR, "write new tablet error");
              char buff[OB_MAX_ERROR_MSG_LEN];
              snprintf(buff, OB_MAX_ERROR_MSG_LEN, "write new info to root_table fail. range=%s", to_cstring(p_table_info->tablet_info_.range_));
              set_daily_merge_tablet_error(buff, strlen(buff));
              TBSYS_LOG(INFO, "%s", to_cstring(p_table_info->tablet_info_.range_));
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "can not found range ignore this: tablet_info[%p], range_pos_type[%d] range:%s",
                tablet_info, range_pos_type, to_cstring(p_table_info->tablet_info_.range_));
            //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "null tablet report info in tablet report list, have_done_index[%ld]", have_done_index);
        }
      }
    } //end else, release lock
    if (need_split || need_add)
    {
      TBSYS_LOG(INFO, "update ranges: server=%d", server_index);
      ret = got_reported_with_copy(tablets, server_index, have_done_index, for_bypass);
    }
  //} //del lbzhong [Paxos Cluster.Balance] 20160705:b:e
  return ret;
}
/*
 * 鍐欐嫹璐濇満鍒剁殑,澶勭悊姹囨姤娑堟伅
 */
int ObRootServer2::got_reported_with_copy(const ObTabletReportInfoList& tablets,
                                          const int32_t server_index, const int64_t have_done_index,
                                          const bool for_bypass)
{
  int ret = OB_SUCCESS;
  ObTabletReportInfo* p_table_info = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;
  TBSYS_LOG(DEBUG, "root table write on copy");
  if (new_server_status == NULL && !for_bypass)
  {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  }
  else
  {
    ObRootTable2* root_table_for_split = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
    if (root_table_for_split == NULL)
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
      ret = OB_ERROR;
    }
    else
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      *root_table_for_split = *root_table_;
      int range_pos_type = ObRootTable2::POS_TYPE_UNINIT;
      for (int64_t index = have_done_index; OB_SUCCESS == ret && index < tablets.tablet_list_.get_array_index(); index++)
      {
        p_table_info = tablets.tablet_list_.at(index);
        if (p_table_info == NULL)
        {
          TBSYS_LOG(ERROR, "tablets.tablet_list_.at(%ld) should not be NULL", index);
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
        }
        else
        {
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
          int find_ret = root_table_for_split->find_range(p_table_info->tablet_info_.range_, first, last);
          if (OB_FIND_OUT_OF_RANGE == find_ret || OB_SUCCESS == find_ret)
          {
            range_pos_type = root_table_for_split->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
            TBSYS_LOG(DEBUG, "range_pos_type2 = %d", range_pos_type);
            if (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
            { // TODO: no merge is implemented yet! so range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE is not proper
              if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                                                             p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_split))
              {
                TBSYS_LOG(ERROR, "write new tablet error");
                char buff[OB_MAX_ERROR_MSG_LEN];
                snprintf(buff, OB_MAX_ERROR_MSG_LEN, "write new info to root_table fail. range=%s", to_cstring(p_table_info->tablet_info_.range_));
                set_daily_merge_tablet_error(buff, strlen(buff));
                TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
            }
            else if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
            {
              if (first->get_max_tablet_version() >= p_table_info->tablet_location_.tablet_version_)
              {
                TBSYS_LOG(ERROR, "same version different range error !! version %ld",
                          p_table_info->tablet_location_.tablet_version_);
                char buff[OB_MAX_ERROR_MSG_LEN];
                snprintf(buff, OB_MAX_ERROR_MSG_LEN, "report tablet has split range, but version not satisfy split process, roottable version=%ld, report_version=%ld",
                    first->get_max_tablet_version(), p_table_info->tablet_location_.tablet_version_);
                set_daily_merge_tablet_error(buff, strlen(buff));
                TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
              else
              {
                ret = root_table_for_split->split_range(p_table_info->tablet_info_, first,
                                                        p_table_info->tablet_location_.tablet_version_, server_index);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "split range error, ret = %d", ret);
                  char buff[OB_MAX_ERROR_MSG_LEN];
                  snprintf(buff, OB_MAX_ERROR_MSG_LEN, "split range error, ret=%d, range=%s", ret, to_cstring(p_table_info->tablet_info_.range_));
                  set_daily_merge_tablet_error(buff, strlen(buff));
                  TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                  //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                }
              }
            }
            else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
            {
              ret = root_table_for_split->add_range(p_table_info->tablet_info_, first,
                                                    p_table_info->tablet_location_.tablet_version_, server_index);
            }
            else
            {
              TBSYS_LOG(WARN, "error range be ignored range_pos_type =%d",range_pos_type );
              TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "find_ret[%d] != OB_FIND_OUT_OF_RANGE or OB_SUCCESS", find_ret);
            ret = OB_ERROR;
          }
        }
      }
    }

    if (OB_SUCCESS == ret && root_table_for_split != NULL)
    {
      switch_root_table(root_table_for_split, NULL);
      root_table_for_split = NULL;
    }
    else
    {
      TBSYS_LOG(ERROR, "update root table failed: ret[%d] root_table_for_split[%p]",
                ret, root_table_for_split);
      if (root_table_for_split != NULL)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_split);
      }
    }

  }
  return ret;
}

/*
 * 鍦ㄤ竴涓猼abelt鐨勫悇浠芥嫹璐濅腑, 瀵绘壘鍚堥?傜殑澶囦唤鏇挎崲鎺?
 */
int ObRootServer2::write_new_info_to_root_table(
  const ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
  ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table)
{
  int ret = OB_SUCCESS;
  int32_t found_it_index = OB_INVALID_INDEX;
  int64_t max_tablet_version = 0;
  ObServerStatus* server_status = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  if (new_server_status == NULL)
  {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  }
  else
  {
    for (ObRootTable2::const_iterator it = first; it <= last; it++)
    {
      ObTabletInfo *p_tablet_write = p_root_table->get_tablet_info(it);
      ObTabletCrcHistoryHelper *crc_helper = p_root_table->get_crc_helper(it);
      if (crc_helper == NULL)
      {
        TBSYS_LOG(ERROR, "%s", "get src helper error should not reach this bugs!!");
        ret = OB_ERROR;
        break;
      }
      max_tablet_version = it->get_max_tablet_version();
      //TODO: no merge is supported yet. this code should be refacted to support merge.
      if (tablet_version >= max_tablet_version)
      {
        if (first != last)
        {
          TBSYS_LOG(ERROR, "we should not have merge tablet max tabelt is %ld this one is %ld",
                    max_tablet_version, tablet_version);
          ret = OB_ERROR;
          break;
        }
      }
      if (first == last)
      {
        //check crc sum
        ret = crc_helper->check_and_update(tablet_version, tablet_info.crc_sum_, tablet_info.row_checksum_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check crc sum error crc is %lu tablet is", tablet_info.crc_sum_);
          TBSYS_LOG(INFO, "tablet_info.range_=%s", to_cstring(tablet_info.range_));
          it->dump();
          char buff[OB_MAX_ERROR_MSG_LEN];
          snprintf(buff, OB_MAX_ERROR_MSG_LEN, "crc error. range=%s, error crc=%ld", to_cstring(tablet_info.range_), tablet_info.crc_sum_);
          set_daily_merge_tablet_error(buff, strlen(buff));
          break;
        }
      }
      //try to over write dead server or old server;
      ObTabletReportInfo to_delete;
      to_delete.tablet_location_.chunkserver_.set_port(-1);
      found_it_index = ObRootTable2::find_suitable_pos(it, server_index, tablet_version, &to_delete);
      if (found_it_index == OB_INVALID_INDEX)
      {
        //find the server whose disk have max usage percent
        int32_t found_percent = -1;
        //add zhaoqiong[roottable tablet management]20150302:b
        //for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
        for (int32_t i = 0; i < OB_MAX_COPY_COUNT; i++)
        //add e
        {
          server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
          if (server_status != NULL &&
              server_status->disk_info_.get_percent() > found_percent)
          {
            found_it_index = i;
            found_percent = server_status->disk_info_.get_percent();
          }
        }
        if (OB_INVALID_INDEX != found_it_index)
        {
          to_delete.tablet_info_ = tablet_info;
          to_delete.tablet_location_.tablet_version_ = it->tablet_version_[found_it_index];
          to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[found_it_index]);
          if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
          {
            TBSYS_LOG(WARN, "failed to add into delete list");
          }
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "find it idx=%d", found_it_index);
        if (-1 != to_delete.tablet_location_.chunkserver_.get_port())
        {
          to_delete.tablet_info_ = tablet_info;
          if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
          {
            TBSYS_LOG(WARN, "failed to add into delete list");
          }
        }
      }
      if (found_it_index != OB_INVALID_INDEX)
      {
        TBSYS_LOG(DEBUG, "write a tablet to root_table found_it_index = %d server_index =%d tablet_version = %ld",
                  found_it_index, server_index, tablet_version);

        //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
        TBSYS_LOG(DEBUG, "tablet_info.range_=%s", to_cstring(tablet_info.range_));
        //over write
        atomic_exchange((uint32_t*) &(it->server_info_indexes_[found_it_index]), server_index);
        atomic_exchange((uint64_t*) &(it->tablet_version_[found_it_index]), tablet_version);
        if (p_tablet_write != NULL)
        {
          atomic_exchange((uint64_t*) &(p_tablet_write->row_count_), tablet_info.row_count_);
          atomic_exchange((uint64_t*) &(p_tablet_write->occupy_size_), tablet_info.occupy_size_);
        }
      }
    }
  }
  return ret;
}

int64_t ObRootServer2::get_table_count(void) const
{
  int64_t ret = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
  }
  else
  {
    ret = schema_manager_for_cache_->get_table_count();
  }
  return ret;
}

void ObRootServer2::get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & data_size) const
{
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    root_table_->get_tablet_info(tablet_count, row_count, data_size);
  }
}

int ObRootServer2::get_row_checksum(const int64_t tablet_version, const uint64_t table_id, ObRowChecksum &row_checksum)
{
  int ret = OB_SUCCESS;
  bool is_all_merged = false;
  common::ObSchemaManagerV2* schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_manager)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCCESS == ret)
  {
    ret = get_schema(false, false, *schema_manager);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get schema manager. ret=%d", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    //check tablet_version is merged
    if(OB_SUCCESS != (ret = check_tablet_version(tablet_version, 1, is_all_merged)))
    {
      TBSYS_LOG(WARN, "check tablet_version:%ld merged failed", tablet_version);
    }
    else if(!is_all_merged)
    {
      TBSYS_LOG(INFO, "tablet_version:%ld is not all merged", tablet_version);
      ret = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      const ObTableSchema* it = NULL;
      uint64_t tmp_table_id = 0;
      row_checksum.data_version_ = tablet_version;
      ObRowChecksumInfo rc_info;
      uint64_t tmp_row_checksum = 0;
      for (it = schema_manager->table_begin(); schema_manager->table_end() != it; ++it)
      {
        tmp_row_checksum = 0;
        tmp_table_id = it->get_table_id();

        if(OB_INVALID_ID != table_id && tmp_table_id != table_id)
          continue;

        tbsys::CRLockGuard guard(root_table_rwlock_);
        if(OB_SUCCESS != (ret = root_table_->get_table_row_checksum(tablet_version, tmp_table_id, tmp_row_checksum)))
        {
          TBSYS_LOG(WARN, "get_row_checksum failed, table_id:%lu ret:%d", tmp_table_id, ret);
        }
        else
        {
          rc_info.reset();
          rc_info.table_id_ = tmp_table_id;
          rc_info.row_checksum_ = tmp_row_checksum;
          if(OB_SUCCESS != (ret = row_checksum.rc_array_.push_back(rc_info)))
          {
            TBSYS_LOG(WARN, "put row_checksum to rc_array failed, ret:%d", ret);
            break;
          }
        }
      }
    }
  }

  if (NULL != schema_manager)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
  }
  return ret;
}

// the array size of server_index should be larger than expected_num
void ObRootServer2::get_available_servers_for_new_table(int* server_index,
    int32_t expected_num, int32_t &results_num)
{
  //tbsys::CThreadGuard guard(&server_manager_mutex_);
  results_num = 0;
  int64_t mnow = tbsys::CTimeUtil::getTime();
  if (next_select_cs_index_ >= server_manager_.get_array_length())
  {
    next_select_cs_index_ = 0;
  }
  ObChunkServerManager::iterator it = server_manager_.begin() + next_select_cs_index_;
  for (; it != server_manager_.end() && results_num < expected_num; ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && mnow > it->register_time_ + config_.cs_probation_period)
    {
      int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
      server_index[results_num] = cs_index;
      results_num++;
    }
  }
  if (results_num < expected_num)
  {
    it = server_manager_.begin();
    for(; it != server_manager_.begin() + next_select_cs_index_ && results_num < expected_num; ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && mnow > it->register_time_ + config_.cs_probation_period)
      {
        int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
        server_index[results_num] = cs_index;
        results_num++;
      }
    }
  }
  next_select_cs_index_ = it - server_manager_.begin() + 1;
}

int ObRootServer2::slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
    int32_t** t_server_index, int32_t* replicas_num, const int64_t mem_version)
{
  int ret = OB_SUCCESS;
  int64_t index = tablets.get_tablet_size();
  ObArray<int32_t> server_array;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  common::ObTabletInfo *p_table_info = NULL;
  {
    ObRootTable2* root_table_for_create = NULL;
    root_table_for_create = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
    if (root_table_for_create == NULL)
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
      ret = OB_ERROR;
    }
    if (NULL != root_table_for_create)
    {
      *root_table_for_create = *root_table_;
    }
    for (int64_t i = 0 ; i < index && OB_SUCCESS == ret; i++)
    {
      p_table_info = tablets.tablet_list.at(i);
      server_array.clear();
      for (int32_t j = 0; i < replicas_num[j]; ++j)
      {
        server_array.push_back(t_server_index[i][j]);
      }
      ret = root_table_for_create->create_table(*p_table_info, server_array, mem_version);
    }
    if (OB_SUCCESS == ret)
    {
      switch_root_table(root_table_for_create, NULL);
      root_table_for_create = NULL;
    }
    if (root_table_for_create != NULL)
    {
      OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_create);
    }
  }
  return ret;
}

// just for replay root server remove rplica commit log
int ObRootServer2::remove_replica(const bool did_replay, const common::ObTabletReportInfo &replica)
{
  int ret = OB_SUCCESS;
  UNUSED(did_replay);
  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  tbsys::CWLockGuard guard(root_table_rwlock_);
  int find_ret = root_table_->find_range(replica.tablet_info_.range_, start_it, end_it);
  if (OB_SUCCESS == find_ret && start_it == end_it)
  {
    //add zhaoqiong[roottable tablet management]20150302:b
    //for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
    for (int i = 0; i < OB_MAX_COPY_COUNT; ++i)
    //add e
    {
      if (OB_INVALID_INDEX != start_it->server_info_indexes_[i]
          && start_it->server_info_indexes_[i] == replica.tablet_location_.chunkserver_.get_port())
      {
        start_it->server_info_indexes_[i] = OB_INVALID_INDEX;
        break;
      }
    }
  }
  return ret;
}

// delete the replicas that chunk server remove automaticly when daily merge failed
int ObRootServer2::delete_replicas(const bool did_replay, const common::ObServer & cs, const common::ObTabletReportInfoList & replicas)
{
  int ret = OB_SUCCESS;
  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  // step 1. find server index for delete replicas
  int32_t server_index = get_server_index(cs);
  if (OB_INVALID_INDEX == server_index)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "get server index failed:server[%s], ret[%d]", cs.to_cstring(), server_index);
  }
  else
  {
    common::ObTabletReportInfo * replica = NULL;
    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CWLockGuard guard(root_table_rwlock_);
    // step 1. modify root table delete replicas
    for (int64_t i = 0; OB_SUCCESS == ret && i < replicas.tablet_list_.get_array_index(); ++i)
    {
      replica = replicas.tablet_list_.at(i);
      if (NULL == replica)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check replica is null");
        break;
      }
      ret = root_table_->find_range(replica->tablet_info_.range_, start_it, end_it);
      if (OB_SUCCESS == ret)
      {
        ObRootTable2::const_iterator iter;
        for (iter = start_it; OB_SUCCESS == ret && iter <= end_it; ++iter)
        {
          int64_t replica_count = 0;
          int64_t find_index = OB_INVALID_INDEX;
          //add zhaoqiong[roottable tablet management]20150302:b
          //for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
          for (int i = 0; i < OB_MAX_COPY_COUNT; ++i)
          //add e
          {
            if (iter->server_info_indexes_[i] != OB_INVALID_INDEX)
            {
              ++replica_count;
              if (server_index == iter->server_info_indexes_[i])
              {
                find_index = i;
              }
            }
          }
          // find the server
          if (find_index != OB_INVALID_INDEX)
          {
            if (replica_count > 1)
            {
              iter->server_info_indexes_[find_index] = OB_INVALID_INDEX;
            }
            else
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "can not remove this replica not safe:index[%d], server[%s]",
                  server_index, cs.to_cstring());
            }
          }
          else
          {
            TBSYS_LOG(WARN, "not find this server in service this tablet:index[%d], server[%s]",
                server_index, cs.to_cstring());
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "not find the tablet replica:range[%s], ret[%d]", to_cstring(replica->tablet_info_.range_), ret);
      }
    }
  }
  // step 2. write commit log if not replay
  if ((OB_SUCCESS == ret) && (false == did_replay))
  {
    if (OB_SUCCESS != (ret = log_worker_->delete_replicas(cs, replicas)))
    {
      TBSYS_LOG(WARN, "log_worker remove tablet replicas failed. err=%d", ret);
    }
  }
  return ret;
}

// @pre root_table_build_mutex_ locked
// @note Do not remove tablet entries in tablet_manager_ for now.
int ObRootServer2::delete_tables(const bool did_replay, const common::ObArray<uint64_t> &deleted_tables)
{
  OB_ASSERT(0 < deleted_tables.count());
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  ObRootTable2* rt1 = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (NULL == rt1)
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *rt1 = *root_table_;
  }
  if (OB_SUCCESS == ret)
  {
    // step 1. modify new root table
    if (OB_SUCCESS != (ret = rt1->delete_tables(deleted_tables)))
    {
      TBSYS_LOG(WARN, "failed to delete tablets, err=%d", ret);
    }
    // step 2. write commit log if not replay
    if ((OB_SUCCESS == ret) && (false == did_replay))
    {
      TBSYS_LOG(TRACE, "write commit log for delete table");
      if (OB_SUCCESS != (ret = log_worker_->remove_table(deleted_tables)))
      {
        TBSYS_LOG(ERROR, "log_worker delete tables failed. err=%d", ret);
      }
    }
    // step 3. switch the new root table
    if (OB_SUCCESS == ret)
    {
      switch_root_table(rt1, NULL);
      rt1 = NULL;
    }
  }
  if (rt1 != NULL)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, rt1);
  }
  return ret;
}

int ObRootServer2::select_cs(const int64_t min_count, ObArray<std::pair<ObServer, int32_t> > &chunkservers)
{
  int ret = OB_SUCCESS;
  int64_t results_num = 0;
  //del bingo [Paxos Cluster.Balance] 20161104:b
  //add lbzhong [Paxos Cluster.Balance] 20160708:b
  /*int32_t master_count = 0;
  tbsys::CRLockGuard guard(cluster_mgr_rwlock_);
  int32_t master_cluster_id = cluster_mgr_.get_master_cluster_id();
  cluster_mgr_.get_master_cluster_tablet_replicas_num(master_count, (int32_t) min_count);*/
  //add:e
  //del:e
  //add bingo [Paxos Cluster.Balance] 20161104:b
  tbsys::CRLockGuard guard(cluster_mgr_rwlock_);
  int32_t replicas_num[OB_CLUSTER_ARRAY_LEN];
  memset(replicas_num, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
  cluster_mgr_.get_cluster_tablet_replicas_num(replicas_num, (int32_t) min_count);
  int32_t ready_num[OB_CLUSTER_ARRAY_LEN];
  memset(ready_num, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
  //add:e
  if (server_manager_.size() <= 0)
  {
    ret = OB_NO_CS_SELECTED;
    TBSYS_LOG(WARN, "check chunk server manager size failed");
  }
  else
  {
    ObServer cs;
    // no need lock select_pos
    static int64_t select_pos = 0;
    if (select_pos >= server_manager_.size())
    {
      select_pos = 0;
    }
    int64_t visit_count = 0;
    do
    {
      ObChunkServerManager::const_iterator it = server_manager_.begin() + select_pos;
      for (; it != server_manager_.end() &&
           //mod bingo [Paxos Cluster.Balance] 20161104:b
           //mod lbzhong [Paxos Cluster.Balance] 20160708:b
           results_num < min_count;
           //results_num < master_count;
           //mod:e
           //mod:e
           ++it)
      {
        ++visit_count;
        ++select_pos;
        //add bingo [Paxos Cluster.Balance] 20161104:b
        int32_t tmp_cluster_id = it->server_.cluster_id_;
        //add:e
        if (it->status_ != ObServerStatus::STATUS_DEAD
            //mod bingo [Paxos Cluster.Balance] 20161104:b
            //add lbzhong [Paxos Cluster.Balance] 20160708:b
            //&& master_cluster_id == it->server_.cluster_id_
            && ready_num[tmp_cluster_id] < replicas_num[tmp_cluster_id]
            //add:e
            //mod:e
            )
        {
          cs = it->server_;
          cs.set_port(it->port_cs_);
          if (OB_SUCCESS != (ret = chunkservers.push_back(std::make_pair(cs, it - server_manager_.begin()))))
          {
            TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
            break;
          }
          else
          {
            ++results_num;
          }
          //add bingo [Paxos Cluster.Balance] 20161104:b
          ready_num[tmp_cluster_id] = ready_num[tmp_cluster_id] + 1;
          //add:e
        }
      } // end while
      //mod bingo [Paxos Cluster.Balance] 20161104:b
      //mod lbzhong [Paxos Cluster.Balance] 20160708:b
      if (results_num < min_count)
      //if (results_num < master_count)
      //mod:e
      //mod:e
      {
        select_pos = 0;
      }
      else
      {
        break;
      }
    } while ((ret == OB_SUCCESS) && (visit_count < server_manager_.size()));
  }
  //LBZ_LOG(INFO, "create empty tablet: master_cluster_id=%ld, min_count=%ld, result_count=%d, select_cs=%ld",
  //        master_cluster_id, min_count, master_count, chunkservers.count());
  if (0 == results_num)
  {
    ret = OB_NO_CS_SELECTED;
    TBSYS_LOG(ERROR, "not find valid chunkserver for create table in each cluster");
  }
  //mod bingo [Paxos Cluster.Balance] 20161104:b
  //mod lbzhong [Paxos Cluster.Balance] 20160708:b
  else if (results_num < min_count)
  //else if (results_num < master_count)
  //mod:e
  {
    TBSYS_LOG(WARN, "find valid chunkserver less than required replica count:select[%ld], required[%ld]",
        results_num, min_count);
  }
  //mod:e
  return ret;
}

int ObRootServer2::fetch_mem_version(int64_t &mem_version)
{
  int ret = OB_SUCCESS;
  ObUps ups_master;
  if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
  {
    TBSYS_LOG(WARN, "failed to get ups master, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = get_rpc_stub().get_last_frozen_version(ups_master.addr_,
          config_.network_timeout, mem_version)))
  {
    TBSYS_LOG(WARN, "failed to get mem version from ups, err=%d", ret);
  }
  else if (0 >= mem_version)
  {
    ret = OB_INVALID_START_VERSION;
    TBSYS_LOG(WARN, "invalid mem version from ups, version=%ld ups=%s",
        mem_version, to_cstring(ups_master.addr_));
  }
  else
  {
    TBSYS_LOG(INFO, "fetch ups mem version=%ld", mem_version);
  }
  return ret;
}

int ObRootServer2::create_new_table(const bool did_replay, const common::ObTabletInfo& tablet,
    const common::ObArray<int32_t> &chunkservers, const int64_t mem_version)
{
  int ret = OB_SUCCESS;
  ObRootTable2* root_table_for_create = NULL;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  root_table_for_create = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (root_table_for_create == NULL)
  {
    TBSYS_LOG(ERROR, "new ObRootTable2 error");
    ret = OB_ERROR;
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *root_table_for_create = *root_table_;
  }
  if (OB_SUCCESS == ret)
  {
    //fixbug:check roottable exist
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if(root_table_->table_is_exist(tablet.range_.table_id_))
      {
        TBSYS_LOG(WARN, "table already exist, table_id=%lu", tablet.range_.table_id_);
        ret = OB_CREATE_TABLE_TWICE;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    // step 1. modify new root table
    if (OB_SUCCESS != (ret = root_table_for_create->create_table(tablet, chunkservers, mem_version)))
    {
      TBSYS_LOG(WARN, "fail to create table. err=%d", ret);
    }
    // step 2. write commit log if not replay
    if ((OB_SUCCESS == ret) && (false == did_replay))
    {
      if (OB_SUCCESS != (ret = log_worker_->add_new_tablet(tablet, chunkservers, mem_version)))
      {
        TBSYS_LOG(WARN, "log_worker add new tablet failed. err=%d", ret);
      }
    }
    // step 3. switch the new root table
    if (OB_SUCCESS == ret)
    {
      switch_root_table(root_table_for_create, NULL);
      root_table_for_create = NULL;
    }
  }
  if (root_table_for_create != NULL)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_create);
    root_table_for_create = NULL;
  }
  return ret;
}

int ObRootServer2::create_empty_tablet(TableSchema &tschema, ObArray<ObServer> &created_cs)
{
  int ret = OB_SUCCESS;
  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "I'm not the master and can not create table");
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_->table_is_exist(tschema.table_id_))
    {
      ret = OB_ENTRY_EXIST;
      TBSYS_LOG(ERROR, "table is already created in root table. name=%s, table_id=%ld",
          tschema.table_name_, tschema.table_id_);
    }
  }
  int64_t mem_version = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = fetch_mem_version(mem_version)))
    {
      TBSYS_LOG(WARN, "fail to get mem_version. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObArray<std::pair<ObServer, int32_t> > chunkservers;
    if (OB_SUCCESS != (ret = select_cs(tschema.replica_num_, chunkservers)))
    {
      TBSYS_LOG(WARN, "failed to select chunkservers, err=%d", ret);
      ret = OB_NO_CS_SELECTED;
    }
    else
    {
      ObTabletInfo tablet;
      tablet.range_.table_id_ = tschema.table_id_;
      tablet.range_.border_flag_.unset_inclusive_start();
      tablet.range_.border_flag_.set_inclusive_end();
      tablet.range_.set_whole_range();
      tschema.create_mem_version_ = mem_version;
      int err = OB_SUCCESS;
      ObArray<int32_t> created_cs_id;
      int32_t created_count = 0;
      for (int64_t i = 0; i < chunkservers.count(); ++i)
      {
        err = worker_->get_rpc_stub().create_tablet(
          chunkservers.at(i).first, tablet.range_, tschema.create_mem_version_,
          config_.network_timeout);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to create tablet, err=%d tid=%lu cs=%s", err,
                    tablet.range_.table_id_, to_cstring(chunkservers.at(i).first));
        }
        else
        {
          ++created_count;
          created_cs.push_back(chunkservers.at(i).first);
          created_cs_id.push_back(chunkservers.at(i).second);
          TBSYS_LOG(INFO, "create tablet replica, table_id=%lu cs=%s version=%ld replica_num=%d",
                    tablet.range_.table_id_, to_cstring(chunkservers.at(i).first),
                    tschema.create_mem_version_, created_count);
        }
      } // end for
      if (0 >= created_count)
      {
        ret = OB_NO_TABLETS_CREATED;
        TBSYS_LOG(WARN, "no tablet created for create:table_id=%lu", tablet.range_.table_id_);
      }
      else
      {
        ret = create_new_table(false, tablet, created_cs_id, mem_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "create new table failed:table[%lu], ret[%d]", tablet.range_.table_id_, ret);
        }
      }
    }
    if (OB_ENTRY_EXIST == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
//add wenghaixing [secondary index static_index_build] 20150317
int ObRootServer2::force_cs_create_index(uint64_t tid, int64_t hist_width, int64_t start_time)
{
    int ret = OB_SUCCESS;
    /*
    ObServer tmp_server;
    if(this->is_master())
    {
        //tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
        ObChunkServerManager::iterator it = this->server_manager_.begin();
            for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
            {
                if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
                      {
                        tmp_server = it->server_;
                        tmp_server.set_port(it->port_cs_);
                        ret = worker_->get_rpc_stub().index_signal_to_cs(tmp_server, tid, hist_width);
                        if (OB_SUCCESS == ret)
                        {
                          TBSYS_LOG(INFO, "force index signal to cs %s", to_cstring(tmp_server));
                        }
                        else
                        {
                          TBSYS_LOG(WARN, "foce index signal  to cs %s failed", to_cstring(tmp_server));
                        }
                      }
            }
    }*/
    tbsys::CWLockGuard guard(index_beat_rwlock_);
    index_beat_.idx_tid = tid;
    index_beat_.status = INDEX_INIT;
    index_beat_.hist_width = hist_width;
    index_beat_.start_time = start_time;
    return ret;
}
//add e

int ObRootServer2::delete_dropped_tables(int64_t & table_count)
{
  OB_ASSERT(balancer_);
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  common::ObArray<uint64_t> deleted_tables;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (schema_manager_for_cache_->get_status() != ObSchemaManagerV2::ALL_TABLES)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "schema_manager_for_cache_ is not ready, version is %ld, table count is %ld",
        schema_manager_for_cache_->get_version(),
        schema_manager_for_cache_->get_table_count());
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    ret = root_table_->get_deleted_table(*schema_manager_for_cache_, *balancer_, deleted_tables);
    if (OB_SUCCESS == ret)
    {
      table_count = deleted_tables.count();
    }
    else
    {
      TBSYS_LOG(WARN, "failed to get deleted table, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret && table_count > 0)
  {
    // no need write log
    ret = delete_tables(true, deleted_tables);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "delete the droped table from root table failed:table_id[%lu], ret[%d]",
          table_id, ret);
    }
  }
  return ret;
}

int ObRootServer2::check_table_exist(const common::ObString & table_name, bool & exist)
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    const ObTableSchema * table = schema_manager_for_cache_->get_table_schema(table_name);
    if (NULL == table)
    {
      exist = false;
    }
    else
    {
      exist = true;
    }
  }
  return ret;
}

int ObRootServer2::switch_ini_schema()
{
  int ret = OB_SUCCESS;
  tbsys::CConfig config;
  if (!local_schema_manager_->parse_from_file(config_.schema_filename, config))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
  }
  else
  {
    ObBootstrap bootstrap(*this);
    ret = bootstrap.bootstrap_ini_tables();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "create all the ini tables failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "create all the ini tables succ");
      //del zhaoqiong [Schema Manager] 20150327:b
      // fire an event to tell all clusters
//      ret = ObRootTriggerUtil::create_table(root_trigger_);
//      if (ret != OB_SUCCESS)
//      {
//        TBSYS_LOG(ERROR, "trigger event for create table failed:ret[%d]", ret);
//      }
//      else
//      {
//        TBSYS_LOG(INFO, "trigger event for create table succ for load ini schema");
//      }
      //del:e
    }
  }
  return ret;
}

//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::refresh_new_schema(int64_t & table_count)
int ObRootServer2::refresh_new_schema(int64_t & table_count, int64_t version)
//mod:e
{
  int ret = OB_SUCCESS;
  // if refresh failed output error log because maybe do succ if reboot
  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  else
  {
    //add zhaoqiong [Schema Manager] 20150327:b
    if (0 != version)
    {
      //trigger refresh schema
      schema_timestamp_ = version;
      schema_service_->set_schema_version(version);
    }
    else if (CORE_SCHEMA_VERSION < schema_timestamp_)
    {
      ret = renew_schema_version();
    }
    // get new schema and update the schema version
    if (OB_SUCCESS == ret)
    {
	//add:e
      ret = get_schema(true, false, *out_schema);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "force refresh schema manager failed:ret[%d]", ret);
      }
      else
      {
        table_count = out_schema->get_table_count();
        TBSYS_LOG(INFO, "force refresh schema manager succ:version[%ld], table_count=%ld",
            out_schema->get_version(), table_count);
      }
    }
  }//add zhaoqiong [Schema Manager] 20150327:b
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}

//add zhaoqiong [Schema Manager] 20150520:b
int ObRootServer2::renew_schema_version()
{
  int ret = OB_SUCCESS;
  if (ObiRole::MASTER == obi_role_.get_role())
  {
    //add refresh schema to ddl_operation(slave do not add record to __all_ddl_operation)
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else
    {
      //only init, avoid not init error
      schema_service_->init(schema_service_scan_helper_, true);
      if (OB_SUCCESS != (ret = schema_service_->refresh_schema()))
      {
        TBSYS_LOG(WARN, "apply refresh schema mutator fail, err=%d", ret);
      }
      else
      {
        schema_timestamp_ = schema_service_->get_schema_version();
      }
    }
  }
  return ret;
}
//add :e
//add zhaoqiong [Schema Manager] 20150327:b
int ObRootServer2::refresh_new_schema(int64_t & table_count, ObSchemaMutator &schema_mutator)
{
  int ret = OB_SUCCESS;
  // if refresh failed output error log because maybe do succ if reboot
  if (OB_SUCCESS != (ret = get_schema_mutator(schema_mutator,table_count)))
  {
    TBSYS_LOG(WARN, "force refresh schema manager failed:ret[%d]", ret);
  }
  return ret;
}
//add:e

/// for sql api
int ObRootServer2::alter_table(common::AlterTableSchema &tschema)
{
  //size_t len = strlen(tschema.table_name_);//delete dolphin [database manager]@20150618

  //ObString table_name((int32_t)len, (int32_t)len, tschema.table_name_);//delete dolphin [database manager]@20150618
  //add dolphin [database manager]@20150618:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  dt.concat(tschema.dbname_,tschema.table_name_);
  //add:e
  bool is_all_merged = false;
  int ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  else if (true == is_all_merged)
  {
    bool exist = false;
    int err = OB_SUCCESS;
    ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150327
    // LOCK BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      //ret = check_table_exist(table_name, exist);//delete dolphin [database manager]@20150618
      ret = check_table_exist(dt, exist);
      if (OB_SUCCESS == ret)
      {
        if (!exist)
        {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "check table not exist:tname[%s]", tschema.table_name_);
        }
      }
      // inner schema table operation
      if (OB_SUCCESS == ret)
      {
        err = ddl_tool_.alter_table(tschema);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "alter table throuth ddl tool failed:tname[%s], err[%d]",
              tschema.table_name_, err);
          ret = err;
        }
      }
      /// refresh the new schema and schema version no mater success or failed
      if (exist)
      {
        int64_t count = 0;
        //mod zhaoqiong [Schema Manager] 20150327:b
        //get schema mutator and refresh local schema
        //err = refresh_new_schema(count);
        err = refresh_new_schema(count,schema_mutator);
        //mod:e
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after alter table failed:"
              "tname[%s], err[%d], ret[%d]", tschema.table_name_, err, ret);
          ret = err;
        }
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      //mod zhaoqiong [Schema Manager] 20150327:b
      //switch schema mutator not the full schema to all servers
      //if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
      //mod:e
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema alter table succ:table[%s]",
            tschema.table_name_);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters
    //del zhaoqiong [Schema Manager] 20150327:b
    //alter table action accord by __all_ddl_operation,
    //do not write to __all_trigger_event any more
//    {
//      err = ObRootTriggerUtil::alter_table(root_trigger_);
//      if (err != OB_SUCCESS)
//      {
//        TBSYS_LOG(ERROR, "trigger event for alter table failed:err[%d], ret[%d]", err, ret);
//        ret = err;
//      }
//    }
    //del:e

  }
  else
  {
    ret = OB_OP_NOT_ALLOW;
    TBSYS_LOG(ERROR, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  return ret;
}

/// for sql api
int ObRootServer2::create_table(bool if_not_exists, const common::TableSchema &tschema)
{

  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "create table, if_not_exists=%c table_name=%s",
      if_not_exists?'Y':'N', tschema.table_name_);
  // just for pass the schema checking
  uint64_t old_table_id = tschema.table_id_;
  if (OB_INVALID_ID == old_table_id)
  {
    const_cast<TableSchema &> (tschema).table_id_ = OB_APP_MIN_TABLE_ID - 1;
  }
  else if (tschema.table_id_ < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
  {
    TBSYS_LOG(USER_ERROR, "create table failed, while table_id[%ld] less than %ld, and drop system table switch is %s",
        tschema.table_id_, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
    ret = OB_OP_NOT_ALLOW;
  }
  //add lbzhong [Paxos Cluster.Balance] 20160707:b
  if(OB_SUCCESS == ret)
  {
    if(OB_DEFAULT_COPY_COUNT == tschema.replica_num_)
    {
      const_cast<common::TableSchema &>(tschema).replica_num_ = cluster_mgr_.get_total_tablet_replicas_num();
    }
    else if(tschema.replica_num_ < cluster_mgr_.get_min_replicas_num())
    {
      ret = OB_ERR_ILLEGAL_VALUE;
      TBSYS_LOG(USER_ERROR, "Illegal replica num (min allowed is %d).", cluster_mgr_.get_min_replicas_num());
    }
    //add bingo [Paxos create table] 20170225:b
    if(0 == tschema.replica_num_)
    {
      int32_t each_cluster_has_one = 0;
      for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
      {
        if(is_cluster_alive_with_cs(cluster_id))
        {
          each_cluster_has_one++;
        }
      }
      const_cast<common::TableSchema &>(tschema).replica_num_ = each_cluster_has_one;
      TBSYS_LOG(INFO,"not supposed to be zero, for at least each cluster has one, tschema.replica_num_=%d",tschema.replica_num_);
    }
    //add:e
  }
  //add:e
  if(OB_SUCCESS == ret)
  {
    if (!tschema.is_valid())
    {
      TBSYS_LOG(WARN, "table schmea is invalid:table_name[%s]", tschema.table_name_);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      bool is_all_merged = false;
      ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
            ret, last_frozen_mem_version_);
      }
      else if (!is_all_merged)
      {
        ret = OB_OP_NOT_ALLOW;
        TBSYS_LOG(ERROR, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
            ret, last_frozen_mem_version_);
      }
    }
  }
    int err = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    size_t len = strlen(tschema.table_name_);
    ObString table_name((int32_t)len, (int32_t)len, tschema.table_name_);
    bool exist = false;
    //add dolphin [database manager]@20150624:b
    ObString dt;
    char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
    dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
    dt.concat(tschema.dbname_,tschema.table_name_);
    //add:e
    ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150327
    // LOCK BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      ret = check_table_exist(/** modify dolphin [database manager]@20150624 table_name*/dt, exist);
      if (OB_SUCCESS == ret)
      {
        if (exist && !if_not_exists)
        {
          ret = OB_ENTRY_EXIST;
          TBSYS_LOG(WARN, "check table already exist:tname[%s]", tschema.table_name_);
        }
        else if (exist)
        {
          TBSYS_LOG(INFO, "check table already exist:tname[%s]", tschema.table_name_);
        }
      }
      // inner schema table operation
      if ((OB_SUCCESS == ret) && !exist)
      {
        if (OB_INVALID_ID == old_table_id)
        {
          const_cast<TableSchema &> (tschema).table_id_ = old_table_id;
        }
        ret = ddl_tool_.create_table(tschema);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "create table throuth ddl tool failed:tname[%s], err[%d]",
              tschema.table_name_, ret);
        }
      }
      /// refresh the new schema and schema version no mater success or failed
      if (OB_SUCCESS == ret && !exist)
      {
        int64_t count = 0;
        //mod zhaoqiong [Schema Manager] 20150327:b
        //get schema mutator and refresh local schema
        //err = refresh_new_schema(count);
        err = refresh_new_schema(count,schema_mutator);
        //mod:e
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after create table failed:"
              "tname[%s], err[%d], ret[%d]", tschema.table_name_, err, ret);
          ret = err;
        }
        else
        {
          TBSYS_LOG(INFO, "refresh new schema manager after create table succ:"
              "err[%d], count[%ld], ret[%d]", err, count, ret);
        }
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      //mod zhaoqiong [Schema Manager] 20150327:b
      //switch schema mutator not the full schema to all servers
      //if (OB_SUCCESS != (err = notify_switch_schema(false)))
      if (OB_SUCCESS != (err = notify_switch_schema(schema_mutator)))
      //mod:e
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema after create table succ:table[%s]",
            tschema.table_name_);
      }
    }
    // fire an event to tell all clusters
    //del zhaoqiong [Schema Manager] 20150327:b
    //create table action accord by __all_ddl_operation,
    //do not write to __all_trigger_event any more
//    if (OB_SUCCESS == ret)
//    {
//      err = ObRootTriggerUtil::create_table(root_trigger_, tschema.table_id_);
//      if (err != OB_SUCCESS)
//      {
//        TBSYS_LOG(ERROR, "trigger event for create table failed:err[%d], ret[%d]", err, ret);
//        ret = err;
//      }
//    }
    //del:e
  }
  const_cast<TableSchema &> (tschema).table_id_ = old_table_id;
  return ret;
}

/// for sql api
//add wenghaixing [secondary index drop index]20141223
int ObRootServer2::drop_index(const ObStrings &tables)
{
  TBSYS_LOG(INFO, "drop index,tables=[%s]",
              to_cstring(tables));
  ObString index_name;
  bool force_update_schema = false, force_update_schema_idx = false;
  bool is_all_merged = false;
  // at least one replica safe
  int ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  else if (true == is_all_merged)
  {
    ///BLOCK first we must close index so that all sql execution cannot read the index
    ///wenghaixing
    ObSchemaMutator modify_schema_mutator,drop_schema_mutator;
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      // mod zhangcd [multi_database.seconary_index] 20150721
      for (int64_t i = 0; i < tables.count() && OB_SUCCESS == ret; ++i)
      {
        ret = tables.get_string(i, index_name);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get table failed:index[%ld], ret[%d]", i, ret);
          break;
        }
        else
        {
          //mod liumz, [bugfix: do not use magic number]:20150604
          //ret = ddl_tool_.modify_index_stat(index_name,3);
          //add liuxiao
          TableSchema table_schema;
          const bool only_core_tables = false;
          // add zhangcd [multi_database.seconary_index] 20150721:b
          ObString db_name;
          ObString short_table_name;
          // mod zhangcd 20150724:b
          char db_name_buffer[OB_MAX_DATBASE_NAME_LENGTH];
          char short_table_name_buffer[OB_MAX_TABLE_NAME_LENGTH];
          // mod:e
          db_name.assign_buffer(db_name_buffer, OB_MAX_DATBASE_NAME_LENGTH);
          short_table_name.assign_buffer(short_table_name_buffer, OB_MAX_TABLE_NAME_LENGTH);
          // add:e
          if(NULL != schema_service_ && NULL != schema_service_scan_helper_)
          {
            schema_service_->init(schema_service_scan_helper_, only_core_tables);
           // mod zhangcd [multi_database.seconary_index] 20150721:b
            bool ret_val = index_name.split_two(db_name, short_table_name, '.');
            if(ret_val && OB_SUCCESS == ret)
            {
              ret = schema_service_->get_table_schema(short_table_name, table_schema, db_name);
              //add liuxiao [secondary index] 20150721
              if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to get_table_schema table_name:%.*s  ret=%d",
                          index_name.length(), index_name.ptr(), ret);
              }
              //add e
            }
            else
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "complete index name split failed! complete index name[%.*s]", index_name.length(), index_name.ptr());
            }
            // mod:e
          }
          else
          {
            ret = OB_ERROR;
            //add liuxiao [secondary index] 20150721
            TBSYS_LOG(WARN, "failed to get schema_service_ ret=%d", ret);
            //add e
          }
          if (OB_SUCCESS == ret)
            //add e
          {
            //add liuxiao [muti database] 20150702
            //ObString db_name = ObString::make_string(table_schema.dbname_);
            //add e
            //mod liumz, [bugfix_drop_index]20150721:b
            //ret = ddl_tool_.modify_index_stat(index_name,table_schema.table_id_,db_name,WRITE_ONLY);
            ret = ddl_tool_.modify_index_stat(short_table_name,table_schema.table_id_,db_name,WRITE_ONLY);
            //mod:e
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "modify this index stat failed:index[%ld], tname[%.*s], ret[%d]",
                        i, index_name.length(), index_name.ptr(), ret);
              break;
            }
            else
            {
              force_update_schema_idx = true;
              TBSYS_LOG(INFO, "modify this index stat succ:index[%ld], tname[%.*s]",
                        i, index_name.length(), index_name.ptr());
            }
          }
          //mod:e
        }
      }
      //add liumz, [obsolete trigger event, use ddl_operation]20150701:b
      if (force_update_schema_idx)
      {
        int64_t count = 0;
        // if refresh failed output error log because maybe do succ if reboot
        int err = refresh_new_schema(count,modify_schema_mutator);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
              "err[%d], ret[%d]", err, ret);
          ret = err;
        }        
      }
      //add:e
      //del liumz, [obsolete trigger event, use ddl_operation]20150701:b
      /*/// refresh the new schema and schema version no mater success or failed
      // if failed maybe restart for sync the local schema according the inner table
      if(force_update_schema_idx)
      {
        int64_t count = 0;
        // if refresh failed output error log because maybe do succ if reboot
        int err = refresh_new_schema(count);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
                    "err[%d], ret[%d]", err, ret);
          ret = err;
        }
      }
      // notify schema update to all servers
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = notify_switch_schema(false)))
        {
          TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
        }
      }
      // only refresh the new schema manager
      // fire an event to tell all clusters
      {
        int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
        //mod liumz, [bugfix: check err, not ret]
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
          ret = err;
        }
      }*/
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret && force_update_schema_idx)
    {
      if(OB_SUCCESS != (ret = notify_switch_schema(modify_schema_mutator)))
      {
         TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    ///BLOCK Now begin to drop index
    ///wenghaixing
    if(OB_SUCCESS == ret)
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      // mod zhangcd [multi_database.seconary_index] 20150721
      for (int64_t i = 0; i < tables.count() && OB_SUCCESS == ret; ++i)
      {
        ret = tables.get_string(i, index_name);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get table failed:index[%ld], ret[%d]", i, ret);
          break;
        }
        else
        {
          bool refresh = false;
          ret = drop_one_index( index_name, refresh);
          //mod liumz, [obsolete trigger event, use ddl_operation]20150701:b
          //if (true == refresh)
          if (true == refresh && OB_SUCCESS == ret)
          //mod:e
          {
            force_update_schema = true;
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "drop this table failed:index[%ld], tname[%.*s], ret[%d]",
                          i, index_name.length(), index_name.ptr(), ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "drop this table succ:index[%ld], tname[%.*s]",
                          i, index_name.length(), index_name.ptr());
          }
        }
      }
      //add liumz, [obsolete trigger event, use ddl_operation]20150701:b
      if (force_update_schema)
      {
        int64_t count = 0;
        // if refresh failed output error log because maybe do succ if reboot
        int err = refresh_new_schema(count,drop_schema_mutator);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
              "err[%d], ret[%d]", err, ret);
          ret = err;
        }
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret && force_update_schema)
    {
      if(OB_SUCCESS != (ret = notify_switch_schema(drop_schema_mutator)))
      {
         TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    //add:e
    //del liumz, [obsolete trigger event, use ddl_operation]20150701:b
    /*/// refresh the new schema and schema version no mater success or failed
    // if failed maybe restart for sync the local schema according the inner table
    if (force_update_schema)
    {
      int64_t count = 0;
      // if refresh failed output error log because maybe do succ if reboot
      int err = refresh_new_schema(count);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
                "err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
      // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters
    {
      int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }*/
  }
  else
  {
    ret = OB_OP_NOT_ALLOW;
    TBSYS_LOG(ERROR, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
          ret, last_frozen_mem_version_);
  }
  return ret;
}

//add e
int ObRootServer2::drop_tables(bool if_exists, const common::ObStrings &tables)
{
  TBSYS_LOG(INFO, "drop table, if_exists=%c tables=[%s]",
            if_exists?'Y':'N', to_cstring(tables));
  ObString table_name;
  bool force_update_schema = false;
  bool is_all_merged = false;
  // at least one replica safe
  //add liuxiao [secondary index] 20150716
  int err = OB_ERROR;
  ObSchemaMutator schema_mutator;
  int64_t count = 0;
  //add:e
  int ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  else if (true == is_all_merged)
  {
    // BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      for (int64_t i = 0; i < tables.count(); ++i)
      {
        ret = tables.get_string(i, table_name);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get table failed:index[%ld], ret[%d]", i, ret);
          break;
        }
        else
        {
          bool refresh = false;
          ret = drop_one_table(if_exists, table_name, refresh);
          //mod zhaoqiong [Schema Manager] 20150526:b
//          if (true == refresh)
          if (true == refresh && OB_SUCCESS == ret)
          //mod:e
          {
            force_update_schema = true;
          }
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "drop this table failed:index[%ld], tname[%.*s], ret[%d]",
                i, table_name.length(), table_name.ptr(), ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "drop this table succ:index[%ld], tname[%.*s]",
                i, table_name.length(), table_name.ptr());
          }
        }
      }
      //add liuxiao [secondary index]20150716
      if (force_update_schema)
      {
        err = refresh_new_schema(count,schema_mutator);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
              "err[%d], ret[%d]", err, ret);
          ret = err;
        }
        //else if(OB_SUCCESS != (err = notify_switch_schema(schema_mutator)))
        //{
        //  TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
        //  ret = err;
        //}
      }
    }
    if(OB_SUCCESS == ret && force_update_schema)
    {
      if(OB_SUCCESS != (err = notify_switch_schema(schema_mutator)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
        ret = err;
      }
    }
    //add e

    //delete liuxiao [secondary index] 20150716
    /*
    /// refresh the new schema and schema version no mater success or failed
    // if failed maybe restart for sync the local schema according the inner table
    if (force_update_schema)
    {
      ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150526
      int64_t count = 0;
      // if refresh failed output error log because maybe do succ if reboot
      //mod zhaoqiong [Schema Manager] 20150327:b
      //get schema mutator and refresh local schema
      //int err = refresh_new_schema(count);
      int err = refresh_new_schema(count,schema_mutator);
      //mod:e
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
            "err[%d], ret[%d]", err, ret);
        ret = err;
      }
      //add zhaoqiong [Schema Manager] 20150526:b
      // notify schema update to all servers
      else if(OB_SUCCESS != (err = notify_switch_schema(schema_mutator)))
      {
         TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
         ret = err;
      }
      //add:e
    }
    *///delete:e
    //del zhaoqiong [Schema Manager] 20150526:b
    // notify schema update to all servers
//    if (OB_SUCCESS == ret)
//    {
//      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
//      {
//        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
//      }
//    }
    //del:e
    // only refresh the new schema manager
    // fire an event to tell all clusters
    //del zhaoqiong [Schema Manager] 20150327:b
    //drop table action accord by __all_ddl_operation,
    //do not write to __all_trigger_event any more
//    {
//      int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
//      if (err != OB_SUCCESS)
//      {
//        TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
//        ret = err;
//      }
//    }
    //del:e

  }
  else
  {
    ret = OB_OP_NOT_ALLOW;
    TBSYS_LOG(ERROR, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  return ret;
}



//add e

//add zhaoqiong [Truncate Table]:20160318:b
int ObRootServer2::truncate_tables(bool if_exists, const common::ObStrings &tables, const common::ObString & user, const common::ObString & comment)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "truncate table, if_exists=%c tables=[%s] user=[%.*s]",
            if_exists?'Y':'N', to_cstring(tables), user.length(), user.ptr());
  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }
  // LOCK BLOCK
  {
    tbsys::CThreadGuard guard(&mutex_lock_);
    ObString table_name;
    for (int64_t i = 0; ret == OB_SUCCESS && i < tables.count(); ++i)
    {
      bool exist = false;
      ret = tables.get_string(i, table_name);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get table failed:index[%ld], ret[%d]", i, ret);
        break;
      }
      else if (OB_SUCCESS == (ret = check_table_exist(table_name, exist)))
      {
        if (!exist && !if_exists)
        {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
        }
        else if (!exist)
        {
          TBSYS_LOG(INFO, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
        }
      }

      const ObTableSchema * table = NULL;
      uint64_t table_id = 0;
      //schema check:
      if (ret == OB_SUCCESS && exist)
      {
        tbsys::CRLockGuard guard(schema_manager_rwlock_);
        if (NULL == schema_manager_for_cache_)
        {
          TBSYS_LOG(WARN, "check schema manager failed");
          ret = OB_INNER_STAT_ERROR;
        }
        else
        {
          table = schema_manager_for_cache_->get_table_schema(table_name);
          table_id = table->get_table_id();
          if (NULL == table)
          {
            ret = OB_ENTRY_NOT_EXIST;
            TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
          }
          else if (table->get_index_helper().tbl_tid != OB_INVALID_ID)
          {
            ret = OB_OP_NOT_ALLOW;
            TBSYS_LOG(USER_ERROR, "truncate table failed, while table_name[%.*s] is not base table", table_name.length(), table_name.ptr());
          }
          else if (table_id < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
          {
            TBSYS_LOG(USER_ERROR, "truncate table failed, while table_id[%ld] less than %ld, and drop system table switch is %s",
                      table_id, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
            ret = OB_OP_NOT_ALLOW;
          }
          else
          {
            IndexList il;
            if(OB_SUCCESS != (ret = schema_manager_for_cache_->get_index_list_for_drop(table_id,il)))
            {
              TBSYS_LOG(ERROR,"generate truncate table_list, err =%d", ret);
              break;
            }
            else
            {
              ret = mutator->trun_tab(table_id, table_name, user, comment);
              for(int64_t m = 0; ret == OB_SUCCESS && m<il.get_count(); m++)
              {
                uint64_t index_tid = OB_INVALID_ID;
                il.get_idx_id(m,index_tid);
                ObString index_name = ObString::make_string(schema_manager_for_cache_->get_table_schema(index_tid)->get_table_name());
                ret = mutator->trun_tab(index_tid, index_name, user, comment);
              }
              if (ret == OB_SUCCESS)
              {
                ret = truncate_one_table(*mutator);
              }
            }
          }
        }
      }
      if ((ret == OB_SUCCESS|| ret == OB_TABLE_UPDATE_LOCKED)&& OB_SUCCESS != (ret = mutator->reset()))
      {
        TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}
//add:e

//add zhaoqiong [Truncate Table]:20160318:b
int ObRootServer2::truncate_one_table( const common::ObMutator &mutator)
{ 
  int ret = OB_SUCCESS;
  if (obi_role_.get_role() == ObiRole::MASTER)
  {
    ObUps ups_master;
    if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
    {
      TBSYS_LOG(WARN, "fail to get ups master. err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().truncate_table(ups_master.addr_,mutator, config_.network_timeout)))
    {
      TBSYS_LOG(ERROR, "fail to truncate table to ups. ups: [%s] err=%d",
                to_cstring(ups_master.addr_), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify ups to truncate table succ! ");
    }
  }
  return ret;
}
//add:e
//add wenghaixing [secondary index drop index]20141223
int ObRootServer2::drop_one_index(const ObString &table_name, bool &refresh)
{
  bool exist = false;
  refresh = false;
  int ret = check_table_exist(table_name, exist);
  if (OB_SUCCESS == ret)
  {
    if(!exist)
    {
      ret = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  // inner schema table operation
  const ObTableSchema * table = NULL;
  uint64_t idx_table_id = 0;
  if ((OB_SUCCESS == ret) && exist)
  {
    tbsys::CRLockGuard guard(schema_manager_rwlock_);
    if (NULL == schema_manager_for_cache_)
    {
      TBSYS_LOG(WARN, "check schema manager failed");
      ret = OB_INNER_STAT_ERROR;
    }
    else
    {
      table = schema_manager_for_cache_->get_table_schema(table_name);
      if (NULL == table)
      {
        ret = OB_ENTRY_NOT_EXIST;
        TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
      }
      else
      {
        idx_table_id = table->get_table_id();
        //mod liumz, [bugfix: index table need not check ddl_system_table_switch]:20150604
        /*if (idx_table_id < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
        {
          TBSYS_LOG(USER_ERROR, "drop table failed, while idx_table_id[%ld] less than %ld, and drop system table switch is %s",
                idx_table_id, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
          ret = OB_OP_NOT_ALLOW;
        }*/
        if (idx_table_id < OB_APP_MIN_TABLE_ID)
        {
          TBSYS_LOG(USER_ERROR, "drop table failed, while idx_table_id[%ld] less than %ld",
                idx_table_id, OB_APP_MIN_TABLE_ID);
          ret = OB_OP_NOT_ALLOW;
        }
        //mod:e
      }
    }
  }
  if ((OB_SUCCESS == ret) && exist)
  {
    refresh = true;
    ret = ddl_tool_.drop_table(table_name);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "drop table throuth ddl tool failed:tname[%.*s], ret[%d]",
            table_name.length(), table_name.ptr(), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "drop table succ:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  //modify liuxiao [secondary index]
  //if ((OB_SUCCESS == ret) && exist)
  //{
  //  ret = ObRootTriggerUtil::drop_tables(root_trigger_, idx_table_id);
  //  if (OB_SUCCESS != ret)
  //  {
  //    TBSYS_LOG(ERROR, "trigger event for drop table failed:table_id[%ld], ret[%d]", idx_table_id, ret);
  //  }
  //}
  //modify:e
  return ret;
}

//add e
int ObRootServer2::drop_one_table(const bool if_exists, const ObString & table_name, bool & refresh)
{
  // TODO lock tool long time
  bool exist = false;
  refresh = false;
  int ret = check_table_exist(table_name, exist);
  if (OB_SUCCESS == ret)
  {
    if (!exist && !if_exists)
    {
      ret = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
    }
    else if (!exist)
    {
      TBSYS_LOG(INFO, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  // inner schema table operation
  const ObTableSchema * table = NULL;
  uint64_t table_id = 0;
  if ((OB_SUCCESS == ret) && exist)
  {
    tbsys::CRLockGuard guard(schema_manager_rwlock_);
    if (NULL == schema_manager_for_cache_)
    {
      TBSYS_LOG(WARN, "check schema manager failed");
      ret = OB_INNER_STAT_ERROR;
    }
    else
    {
      table = schema_manager_for_cache_->get_table_schema(table_name);
      if (NULL == table)
      {
        ret = OB_ENTRY_NOT_EXIST;
        TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
      }
      else
      {
        table_id = table->get_table_id();
        if (table_id < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
        {
          TBSYS_LOG(USER_ERROR, "drop table failed, while table_id[%ld] less than %ld, and drop system table switch is %s",
              table_id, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
          ret = OB_OP_NOT_ALLOW;

        }
      }
    }
  }
  if ((OB_SUCCESS == ret) && exist)
  {
    refresh = true;
    ret = ddl_tool_.drop_table(table_name);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "drop table throuth ddl tool failed:tname[%.*s], ret[%d]",
          table_name.length(), table_name.ptr(), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "drop table succ:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  //del zhaoqiong [Schema Manager] 20150327:b
  //drop table action accord by __all_ddl_operation,
  //do not write to __all_trigger_event any more
//  if ((OB_SUCCESS == ret) && exist)
//  {
//    ret = ObRootTriggerUtil::drop_tables(root_trigger_, table_id);
//    if (ret != OB_SUCCESS)
//    {
//      TBSYS_LOG(ERROR, "trigger event for drop table failed:table_id[%ld], ret[%d]", table_id, ret);
//    }
//  }
  //del:e
  return ret;
}

int ObRootServer2::get_master_ups(ObServer &ups_addr, bool use_inner_port)
{
  int ret = OB_ENTRY_NOT_EXIST;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
    {
      TBSYS_LOG(WARN, "not master ups exist");
    }
    else
    {
      ups_addr = ups_master.addr_;
      if (use_inner_port)
      {
        ups_addr.set_port(ups_master.inner_port_);
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "ups_manager is NULL, check it.");
  }
  return ret;
}

ObServer ObRootServer2::get_update_server_info(bool use_inner_port) const
{
  ObServer server;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    ups_manager_->get_ups_master(ups_master);
    server = ups_master.addr_;
    if (use_inner_port)
    {
      server.set_port(ups_master.inner_port_);
    }
  }
  return server;
}

//add peiouya [Get_masterups_and_timestamp] 20141017:b
int64_t ObRootServer2::get_ups_set_time(const common::ObServer &addr)
{
  int64_t settime = -1;
  if (NULL != ups_manager_)
  {
    settime = ups_manager_->get_ups_set_time(addr);
  }
  return settime;
}
//add 20141017:e

int ObRootServer2::get_table_info(const common::ObString& table_name, uint64_t& table_id, int32_t& max_row_key_length)
{
  int err = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  // not used
  max_row_key_length = 0;
  //common::ObSchemaManagerV2 out_schema;
  //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
  common::ObSchemaManagerV2 *out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  //add:e
  else if (config_.enable_cache_schema)
  {
    TBSYS_LOG(DEBUG, "__enable_cache_schema is on");
    {
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      *out_schema = *schema_manager_for_cache_;
    }
    const ObTableSchema* table_schema = out_schema->get_table_schema(table_name);
    if (NULL == table_schema)
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "table %.*s schema not exist in cached schema", table_name.length(), table_name.ptr());
    }
    else
    {
      table_id = table_schema->get_table_id();
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "__enable_cache_schema is off");
    // not used
    TableSchema table_schema;
    // will handle if table_name is one of the three core tables, then construct directly, or scan ms
    err = schema_service_->get_table_schema(table_name, table_schema);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "faile to get table %.*s schema, err=%d", table_name.length(), table_name.ptr(), err);
    }
    else
    {
      table_id = table_schema.table_id_;
    }
  }
  //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  //add:e
  return err;
}

int ObRootServer2::do_check_point(const uint64_t ckpt_id)
{
  int ret = OB_SUCCESS;

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate root table file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = root_table_->write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write root table to file [%s] failed, err=%d", filename, ret);
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate chunk server list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = server_manager_.write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write chunkserver list to file [%s] failed, err=%d", filename, ret);
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, LOAD_DATA_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate load data list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  /*
  if (ret == OB_SUCCESS && NULL != balancer_)
  {
    ret = balancer_->write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write load data list to file [%s] failed, err=%d", filename, ret);
    }
  }
  */
  //del:e
  return ret;
}

int ObRootServer2::recover_from_check_point(const int server_status, const uint64_t ckpt_id)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "server status recover from check point is %d", server_status);

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate root table file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = root_table_->read_from_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "recover root table from file [%s] failed, err=%d", filename, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "recover root table, file_name=%s, size=%ld", filename, root_table_->end() - root_table_->begin());
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate chunk server list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    int32_t cs_num = 0;
    int32_t ms_num = 0;

    ret = server_manager_.read_from_file(filename, cs_num, ms_num);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "recover chunkserver list from file [%s] failed, err=%d", filename, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "recover server list, cs_num=%d ms_num=%d", cs_num, ms_num);
    }
    if (0 < cs_num)
    {
      first_cs_had_registed_ = true;
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, LOAD_DATA_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate load data file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  /*
  if (ret == OB_SUCCESS && NULL != balancer_)
  {
    if (!FileDirectoryUtils::exists(filename))
    {
      TBSYS_LOG(ERROR, "load data check point file %s not exist, skip it. some loading table task may be lost.", filename);
    }
    else
    {
      ret = balancer_->read_from_file(filename);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "recover load data from file [%s] failed, err=%d", filename, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "recover load data, file_name=%s, size=%ld", filename, root_table_->end() - root_table_->begin());
      }
    }
  }
  */
  //del:e
  return ret;
}

int ObRootServer2::receive_new_frozen_version(const int64_t rt_version, const int64_t frozen_version,
    const int64_t last_frozen_time, bool did_replay)
{
  int ret = OB_SUCCESS;
  UNUSED(rt_version);
  if (config_.is_import)
  {
    if (OB_SUCCESS != (ret = try_create_new_tables(frozen_version)))
    {
      TBSYS_LOG(WARN, "fail to create new table. ");
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = report_frozen_memtable(frozen_version, last_frozen_time, did_replay)))
    {
      TBSYS_LOG(WARN, "fail to deal with forzen_memtable report. err=%d", ret);
      ret = OB_SUCCESS;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "fail to create empty tablet. err =%d", ret);
  }
  return ret;
}

//add liumz, [secondary index static_index_build] 20150629:b
int ObRootServer2::modify_init_index()
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  //int64_t cluster_count = 0;//del liumz, [paxos static index]20170626
  common::ObArray<uint64_t> init_indexes;
  common::ObArray<uint64_t> error_indexes;
  common::ObArray<uint64_t> not_available_indexes;
  common::ObArray<uint64_t> available_indexes;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_mgr->get_all_init_index_tid(init_indexes)))
  {
    TBSYS_LOG(WARN, "get all [INIT] index tid failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_mgr->get_all_staging_index_tid(init_indexes)))
  {
    TBSYS_LOG(WARN, "get all [NOT_AVALIBALE] index list failed, ret=%d", ret);
  }
  else
  {
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, true)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
    }
    /* del liumz, [paxos static index]20170626
    else if (OB_SUCCESS != (ret = schema_service_->get_cluster_count(cluster_count)))
    {
      TBSYS_LOG(WARN, "get cluster count failed, ret=%d", ret);
    }*/
    for (int64_t i = 0; i < init_indexes.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
    {
      IndexStatus stat;
      tid = init_indexes.at(i);
      //mod liumz, [paxos static index]20170626:b
      //if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, cluster_count, stat)))
      if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, get_alive_cluster_num_with_cs(), stat)))
      //mod:e
      {
        if (ERROR == stat)
        {
          ret = error_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
          /*if (OB_SUCCESS != (ret = modify_index_stat(tid, ERROR)))
          {
            TBSYS_LOG(WARN, "modify index[%lu] stat [%d->%d] failed, ret=%d", tid, INDEX_INIT, ERROR, ret);
          }*/
        }
        else if (NOT_AVALIBALE == stat)
        {
          ret = not_available_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
          /*if (OB_SUCCESS != (ret = modify_index_stat(tid, NOT_AVALIBALE)))
          {
            TBSYS_LOG(WARN, "modify index[%lu] stat [%d->%d] failed, ret=%d", tid, INDEX_INIT, NOT_AVALIBALE, ret);
          }*/
        }
        else if (AVALIBALE == stat)
        {
          ret = available_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
        }
      }
      /*else if (OB_EAGAIN == ret)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(WARN, "index's stat has not been modified completely, sleep and retry, ret=%d", ret);
      }*/
    }//end for
    if (OB_SUCCESS == ret && error_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(error_indexes, ERROR)))
      {
        TBSYS_LOG(WARN, "modify index stat to ERROR failed, ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret && not_available_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(not_available_indexes, NOT_AVALIBALE)))
      {
        TBSYS_LOG(WARN, "modify index stat to NOT_AVALIBALE failed, ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret && available_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(available_indexes, AVALIBALE)))
      {
        TBSYS_LOG(WARN, "modify index stat to AVALIBALE failed, ret=%d", ret);
      }
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}

int ObRootServer2::modify_index_stat_amd()
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  //int64_t cluster_count = 0;//del liumz, [paxos static index]20170626
  common::ObArray<uint64_t> all_indexes;
  common::ObArray<uint64_t> error_indexes;
  common::ObArray<uint64_t> not_available_indexes;
  common::ObArray<uint64_t> available_indexes;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_mgr->get_all_index_tid(all_indexes)))
  {
    TBSYS_LOG(WARN, "get all index tid failed, ret=%d", ret);
  }
  else
  {
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, true)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
    }
    /* del liumz, [paxos static index]20170626
    else if (OB_SUCCESS != (ret = schema_service_->get_cluster_count(cluster_count)))
    {
      TBSYS_LOG(WARN, "get cluster count failed, ret=%d", ret);
    }*/
    for (int64_t i = 0; i < all_indexes.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
    {
      IndexStatus stat;
      tid = all_indexes.at(i);
      //mod liumz, [paxos static index]20170626:b
      //if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, cluster_count, stat)))
      if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, get_alive_cluster_num_with_cs(), stat)))
      //mod:e
      {
        if (ERROR == stat)
        {
          ret = error_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
          /*if (OB_SUCCESS != (ret = modify_index_stat(tid, ERROR)))
          {
            TBSYS_LOG(WARN, "modify index[%lu] stat to [%d] failed, ret=%d", tid, ERROR, ret);
          }*/
        }
        else if (NOT_AVALIBALE == stat)
        {
          ret = not_available_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
          /*if (OB_SUCCESS != (ret = modify_index_stat(tid, NOT_AVALIBALE)))
          {
            TBSYS_LOG(WARN, "modify index[%lu] stat to [%d] failed, ret=%d", tid, NOT_AVALIBALE, ret);
          }*/
        }
        else if (AVALIBALE == stat)
        {
          ret = available_indexes.push_back(tid);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
          }
        }
      }
      /*else if (OB_EAGAIN == ret)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(WARN, "index[%lu]'s stat has not been modified completely, sleep and retry, ret=%d", tid, ret);
      }*/
    }//end for
    if (OB_SUCCESS == ret && error_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(error_indexes, ERROR)))
      {
        TBSYS_LOG(WARN, "modify index stat to ERROR failed, ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret && not_available_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(not_available_indexes, NOT_AVALIBALE)))
      {
        TBSYS_LOG(WARN, "modify index stat to NOT_AVALIBALE failed, ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret && available_indexes.count() > 0)
    {
      if (OB_SUCCESS != (ret = modify_index_stat(available_indexes, AVALIBALE)))
      {
        TBSYS_LOG(WARN, "modify index stat to AVALIBALE failed, ret=%d", ret);
      }
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20150529:b
/*
 * modify staging index status in schema
 */
int ObRootServer2::modify_staging_index()
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  int64_t last_frozen_mem_version = last_frozen_mem_version_;
  common::ObArray<uint64_t> staging_indexes;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_mgr->get_all_staging_index_tid(staging_indexes)))
  {
    TBSYS_LOG(WARN, "get all [NOT_AVALIBALE] index list failed, ret=%d", ret);
  }
  else
  {
    ObArray<uint64_t> index_list;
    for (int64_t i = 0; i < staging_indexes.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
    {
      bool is_merged = false;
      tid = staging_indexes.at(i);
      if (OB_SUCCESS != (ret = check_tablet_version_v3(tid, last_frozen_mem_version, 0, is_merged)))
      {
        TBSYS_LOG(WARN, "check_tablet_version_v3 failed:version[%ld], tid[%lu], ret[%d]", last_frozen_mem_version, tid, ret);
      }
      else if (true == is_merged)
      {
        ret = index_list.push_back(tid);
        //ret = modify_index_stat(tid, AVALIBALE);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
        }
      }
    }//end for
    if (OB_SUCCESS == ret && index_list.count() > 0)
    {
      ret = modify_index_stat(index_list, AVALIBALE);
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//add:e

//add liumz, [secondary index static_index_build] 20151208:b
/*
 * modify staging index status in __index_process_info
 */
int ObRootServer2::modify_staging_index_process_info()
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  //int64_t cluster_count = 0;//del liumz, [paxos static index]20170626
  int64_t last_frozen_mem_version = last_frozen_mem_version_;
  common::ObArray<uint64_t> staging_indexes;
  common::ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
  {
    TBSYS_LOG(WARN, "get schema manager failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_mgr->get_all_staging_index_tid(staging_indexes)))
  {
    TBSYS_LOG(WARN, "get all [NOT_AVALIBALE] index list failed, ret=%d", ret);
  }
  else
  {
    //TODO, double check staging_indexes by __index_process_info, ONLY keep NOT_AVALIBALE status, except ERROR status
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, true)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
    }
    /* del liumz, [paxos static index]20170626
    else if (OB_SUCCESS != (ret = schema_service_->get_cluster_count(cluster_count)))
    {
      TBSYS_LOG(WARN, "get cluster count failed, ret=%d", ret);
    }*/
    for (int64_t i = 0; i < staging_indexes.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
    {
      IndexStatus stat;
      tid = staging_indexes.at(i);
      //mod liumz, [paxos static index]20170626:b
      //if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, cluster_count, stat)))
      if(OB_SUCCESS == (ret = schema_service_->get_index_stat(tid, get_alive_cluster_num_with_cs(), stat)))
      //mod:e
      {
        if (NOT_AVALIBALE != stat)
        {
          if (OB_SUCCESS != (ret = staging_indexes.remove(i)))
          {
            TBSYS_LOG(WARN, "remove tid from index list failed, ret=%d", ret);
          }
        }
      }
    }

    ObArray<uint64_t> index_list;
    for (int64_t i = 0; i < staging_indexes.count() && OB_LIKELY(OB_SUCCESS == ret); i++)
    {
      bool is_merged = false;
      tid = staging_indexes.at(i);
      if (OB_SUCCESS != (ret = check_tablet_version_v3(tid, last_frozen_mem_version, 0, is_merged)))
      {
        TBSYS_LOG(WARN, "check_tablet_version_v3 failed:version[%ld], tid[%lu], ret[%d]", last_frozen_mem_version, tid, ret);
      }
      else if (true == is_merged)
      {
        ret = index_list.push_back(tid);
        //ret = modify_index_stat(tid, AVALIBALE);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add index tid to index list failed, tid[%lu], ret=%d", tid, ret);
        }
      }
    }//end for
    if (OB_SUCCESS == ret && index_list.count() > 0)
    {
      //mod liumz, [paxos static index]20170607:b
      bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
      get_alive_cluster_with_cs(is_cluster_alive);
      int64_t count = index_list.count();
      for (int64_t idx = 0; idx < count && OB_SUCCESS == ret; idx++)
      {
        for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; cluster_idx++)
        {
          if (is_cluster_alive[cluster_idx+1])
          {
            if (OB_SUCCESS != (ret = modify_index_process_info(cluster_idx+1, index_list.at(idx), AVALIBALE)))
            {
              TBSYS_LOG(WARN, "modify index process info failed, tid[%lu], ret=%d, cluster_id[%d]", index_list.at(idx), ret, cluster_idx+1);
            }
          }
        }
      }
      //mod:e
    }
  }
  if (schema_mgr != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
  }
  return ret;
}
//add:e

bool ObRootServer2::check_all_tablet_safe_merged(void) const
{
  bool ret = false;
  int64_t last_frozen_mem_version = last_frozen_mem_version_;
  int err = check_tablet_version(last_frozen_mem_version, 0, ret);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check tablet merged failed:version[%ld], ret[%d]", last_frozen_mem_version, err);
  }
  else if (true == ret)
  {
    TBSYS_LOG(TRACE, "check tablet merged succ:version[%ld]", last_frozen_mem_version);
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if (root_table_ != NULL)
      {
        //add liumz, [bugfix: ignore error index when check lost range]20151229:b
        common::ObSchemaManagerV2* out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
        if (NULL == out_schema)
        {
          TBSYS_LOG(WARN, "fail to new schema_manager.");
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != ( err = const_cast<ObRootServer2*>(this)->get_schema(false, false, *out_schema)))
        {
          TBSYS_LOG(WARN, "fail to get schema. ret=%d", err);
        }
        ret = root_table_->check_lost_range(out_schema);//default NULL pointer
        if (out_schema != NULL)
        {
          OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
        }
        //add:e
        //ret = root_table_->check_lost_range();//del liumz, [bugfix: ignore error index when check lost range]20151229
        if (ret == false)
        {
          TBSYS_LOG(WARN, "check having lost tablet range");
        }
        else
        {
          TBSYS_LOG(INFO, "check tablet merged succ:version[%ld], result[%d]", last_frozen_mem_version, ret);
        }
      }
    }

    {//mutex block      
      //tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      /*
       * last_frozen_version_ would not be modified when enter this code block
       * if ret == true, mission will be started and build_index_version will be set before new round freeze
       */
      //add liumz, [bugfix: start index mission here]20150917:b
      if (true == ret)
      {
        if(NULL == worker_)
        {
          TBSYS_LOG(ERROR, "root_worker pointer is NULL");
        }
        else if(last_frozen_mem_version == get_last_frozen_version())
        {
          TBSYS_LOG(INFO, "common merged complete, last frozen version[%ld]",last_frozen_mem_version);          
          if(worker_->get_monitor()->is_start()&&OB_SUCCESS == (err = worker_->get_monitor()->start_mission(last_frozen_mem_version)))
          {
            //worker_->get_monitor()->set_start_version(last_frozen_mem_version);
            ret = false;
          }
        }
      }
      //add:e
    }//block end
  }
  return ret;
}

int ObRootServer2::report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time, bool did_replay)
{
  int ret = OB_SUCCESS;
  //add wenghaixing [secondary index static_index_build.index timing]20150418
  bool need_start_monitor = false;
  //mod liumz, [bugfix_reboot_merge]20150722:b
  if(frozen_version > last_frozen_mem_version_)
  //rs restart, OB_INVALID_VERSION == last_frozen_mem_version_? how continue building index
  //if(frozen_version >= last_frozen_mem_version_ && OB_INVALID_VERSION != last_frozen_mem_version_)
  //mod:e
  {
    need_start_monitor = true;
  }
  //add e
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if ( frozen_version < 0 || frozen_version < last_frozen_mem_version_)
  {
    TBSYS_LOG(WARN, "invalid froze_version, version=%ld last_frozen_version=%ld",
        frozen_version, last_frozen_mem_version_);
    ret = OB_ERROR;
  }
  else
  {
    // check last frozen version merged safely
    if (!did_replay && is_master() && (!check_all_tablet_safe_merged()))
    {
      TBSYS_LOG(WARN, "merge is too slow, last_version=%ld curr_version=%ld",
          last_frozen_mem_version_, frozen_version); //just warn
    }
  }

  if (OB_SUCCESS == ret)
  {
    atomic_exchange((uint64_t*) &last_frozen_mem_version_, frozen_version);
    last_frozen_time_ = did_replay ? last_frozen_time : tbsys::CTimeUtil::getTime();
    TBSYS_LOG(INFO, "frozen_version=%ld last_frozen_time=%ld did_replay=%d",
        last_frozen_mem_version_, last_frozen_time_, did_replay);
  }

  if (OB_SUCCESS == ret)
  {
    if (is_master() && !did_replay)
    {
      log_worker_->sync_us_frozen_version(last_frozen_mem_version_, last_frozen_time_);
    }
  }
  if (OB_SUCCESS != ret && did_replay && last_frozen_mem_version_ >= frozen_version)
  {
    TBSYS_LOG(ERROR, "RS meet a unlegal forzen_version[%ld] while replay. last_frozen_verson=%ld, ignore the err",
        frozen_version, last_frozen_mem_version_);
    ret = OB_SUCCESS;
  }

  //add wenghaixing [secondary index static_index_build.index timing]20150418
  if(OB_SUCCESS == ret && need_start_monitor )
  {
    if(OB_UNLIKELY(NULL == worker_ || NULL == worker_->get_monitor()))
    {
      TBSYS_LOG(WARN,"should not be here, null pointer of worker_ or index_monitor_");
    }
    else
    {
      worker_->get_monitor()->start();
    }
  }
  //add e
  return ret;
}

int ObRootServer2::get_last_frozen_version_from_ups(const bool did_replay)
{
  int64_t frozen_version = -1;
  int ret = OB_SUCCESS;
  ObServer ups = get_update_server_info(false);
  if (0 == ups.get_ipv4())
  {
    TBSYS_LOG(INFO, "no ups right now, sleep for next round");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().get_last_frozen_version(
          ups, config_.network_timeout, frozen_version)))
  {
    TBSYS_LOG(WARN, "failed to get frozen version, err=%d ups=%s", ret, ups.to_cstring());
  }
  else if (0 >= frozen_version)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invalid frozen version=%ld ups=%s", frozen_version, ups.to_cstring());
  }
  else
  {
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    TBSYS_LOG(INFO, "got last frozen version, ups=%s version=%ld", ups.to_cstring(), frozen_version);
    if (last_frozen_mem_version_ != frozen_version && frozen_version > 0)
    {
      atomic_exchange((uint64_t*) &last_frozen_mem_version_, frozen_version);
      if (is_master() && !did_replay)
      {
        log_worker_->sync_us_frozen_version(last_frozen_mem_version_, 0);
      }
    }
  }
  return ret;
}
int64_t ObRootServer2::get_time_stamp_changing() const
{
  return time_stamp_changing_;
}
int64_t ObRootServer2::get_lease() const
{
  return config_.cs_lease_duration_time;
}

int ObRootServer2::request_cs_report_tablet()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "request cs report tablet start:");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().request_report_tablet(it->server_)))
      {
        TBSYS_LOG(WARN, "fail to request cs to report. cs_addr=%s", it->server_.to_cstring());
      }
    }
  }
  return ret;
}

const ObiRole& ObRootServer2::get_obi_role() const
{
  return obi_role_;
}

int ObRootServer2::set_obi_role(const ObiRole& role)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else if (ObiRole::INIT == role.get_role())
  {
    TBSYS_LOG(WARN, "role cannot be INIT");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    obi_role_.set_role(role.get_role());
    TBSYS_LOG(INFO, "set obi role, role=%s", role.get_role_str());
    if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
    {
      if (OB_SUCCESS != (ret = worker_->send_obi_role(obi_role_)))
      {
        TBSYS_LOG(WARN, "fail to set slave_rs's obi role err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ups_manager_->send_obi_role()))
      {
        TBSYS_LOG(WARN, "failed to set updateservers' obi role, err=%d", ret);
      }
    }
  }
  return ret;
}

int ObRootServer2::get_master_ups_config(int32_t &master_master_ups_read_percent
                                         //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                         //, int32_t &slave_master_ups_read_percent
                                         //del:e
                                         ) const
{
  int ret = OB_SUCCESS;
  if (ups_manager_ != NULL)
  {
    ups_manager_->get_master_ups_config(master_master_ups_read_percent
                                        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                        //, slave_master_ups_read_percent
                                        //del:e
                                        );
  }
  return ret;
}

void ObRootServer2::cancel_restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.cancel_restart_all_cs();
}

void ObRootServer2::restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.restart_all_cs();
  if(NULL != balancer_thread_)
  {
    balancer_thread_->wakeup();
  }
  else
  {
    TBSYS_LOG(WARN, "balancer_thread_ is null");
  }
}

int ObRootServer2::shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.shutdown_cs(servers, op);
  if(RESTART == op)
  {
    if(NULL != balancer_thread_)
    {
      balancer_thread_->wakeup();
    }
    else
    {
      TBSYS_LOG(WARN, "balancer_thread_ is null");
    }
  }
  return ret;
}

int ObRootServer2::cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.cancel_shutdown_cs(servers, op);
  return ret;
}

void ObRootServer2::do_stat_common(char *buf, const int64_t buf_len, int64_t& pos)
{
  do_stat_start_time(buf, buf_len, pos);
  do_stat_local_time(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)\n", PACKAGE_STRING, RELEASEID);
  databuff_printf(buf, buf_len, pos, "pid: %d\n", getpid());
  databuff_printf(buf, buf_len, pos, "obi_role: %s\n", obi_role_.get_role_str());
  ob_print_mod_memory_usage();
}

void ObRootServer2::do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  databuff_printf(buf, buf_len, pos, "start_time: %s", ctime(&start_time_));
}

void ObRootServer2::do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  time_t now = time(NULL);
  databuff_printf(buf, buf_len, pos, "local_time: %s", ctime(&now));
}

void ObRootServer2::do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
}

void ObRootServer2::do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos)
{
  tbutil::Time frozen_time = tbutil::Time::microSeconds(last_frozen_time_);
  struct timeval frozen_tv(frozen_time);
  struct tm stm;
  localtime_r(&frozen_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "frozen_time: %lu(%s)", last_frozen_time_, time_buf);
}

void ObRootServer2::do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos)
{
  struct mallinfo minfo = mallinfo();
  databuff_printf(buf, buf_len, pos, "mem: arena=%d ordblks=%d hblkhd=%d uordblks=%d fordblks=%d keepcost=%d",
                  minfo.arena, minfo.ordblks, minfo.hblkhd, minfo.uordblks, minfo.fordblks, minfo.keepcost);
}

void ObRootServer2::do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
}

void ObRootServer2::do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int64_t num = -1;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != tablet_manager_)
  {
    num = tablet_manager_->end() - tablet_manager_->begin();
  }
  databuff_printf(buf, buf_len, pos, "tablet_num: %ld", num);
}

int ObRootServer2::table_exist_in_cs(const uint64_t table_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().table_exist_in_cs(it->server_,
                                                                         config_.network_timeout,
                                                                         table_id, is_exist)))
      {
        TBSYS_LOG(WARN, "fail to check table info in cs[%s], ret=%d",
                  to_cstring(it->server_), ret);
      }
      else
      {
        if (true == is_exist)
        {
          TBSYS_LOG(INFO, "table already exist in chunkserver."
                    " table_id=%lu, cs_addr=%s", table_id, to_cstring(it->server_));
          break;
        }
      }
    }
  }
  return ret;
}
void ObRootServer2::do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "chunkservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_cs_);
      databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
    }
  }
}

void ObRootServer2::do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}

void ObRootServer2::do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "mergeservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->ms_status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_ms_);
      databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
    }
  }
}

//add bingo [Paxos rs_election] 20161012
void ObRootServer2::do_stat_rs(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer servers[OB_MAX_RS_COUNT];
  int32_t server_count = 0;
  rs_node_mgr_->get_all_alive_servers(servers,server_count);
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "rootservers: ");
  for(int32_t i = 0; i < server_count; i++)
  {
    tmp_server = servers[i];
    databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
  }
}
//add:e

void ObRootServer2::do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}
void ObRootServer2::do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_cs(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_ms(buf, buf_len, pos);
  //add bingo [Paxos rs_election] 20161012
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_rs(buf, buf_len, pos);
  //add:e
}

//add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
void ObRootServer2::do_stat_all_server_in_clusters(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer rs[OB_MAX_RS_COUNT];
  int32_t server_count = 0;
  rs_node_mgr_->get_all_alive_servers(rs,server_count);

  bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
  get_alive_cluster(is_cluster_alive);
  for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
  {
    if(is_cluster_alive[cluster_id])
    {
      databuff_printf(buf,buf_len, pos, "Cluster %d: \n\t rs:", cluster_id);
      for(int32_t i = 0; i < server_count; i++)
      {
        if(rs[i].cluster_id_ == cluster_id) databuff_printf(buf,buf_len,pos,"%s ", to_cstring(rs[i]));
      }

      databuff_printf(buf,buf_len, pos, "\n\t ups:");

      if(NULL != ups_manager_)
      {
        ups_manager_->print_in_cluster(buf,buf_len,pos,cluster_id);
      }

      ObServer tmp_server;
      ObChunkServerManager::iterator it = server_manager_.begin();
      databuff_printf(buf,buf_len, pos, "\n\t cs:");
      for (; it != server_manager_.end(); ++it)
      {
        if (it->port_cs_ != 0
            && it->status_ != ObServerStatus::STATUS_DEAD
            && it->server_.cluster_id_ == cluster_id)
        {
          tmp_server = it->server_;
          tmp_server.set_port(it->port_cs_);
          databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
        }
      }

      it = server_manager_.begin();
      databuff_printf(buf,buf_len, pos, "\n\t ms:");
      for (; it != server_manager_.end(); ++it)
      {
        if (it->port_ms_ != 0
            && it->ms_status_ != ObServerStatus::STATUS_DEAD
            && it->server_.cluster_id_ == cluster_id)
        {
          tmp_server = it->server_;
          tmp_server.set_port(it->port_ms_);
          databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
        }
      }
      databuff_printf(buf,buf_len, pos, "\n");
    }
  }
}
//add:e

//add pangtianze [Paxos rs_election] 20150731:b
void ObRootServer2::do_stat_rs_leader(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != rs_node_mgr_)
  {
    rs_node_mgr_->print_leader(buf, buf_len, pos);
  }
  else
  {
    TBSYS_LOG(WARN, "rs_node_mgr is null");
  }
}
void ObRootServer2::do_stat_paxos_changed(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != rs_node_mgr_)
  {
    rs_node_mgr_->if_all_paxos_changed(buf, buf_len, pos, (int32_t)config_.rs_paxos_number);
  }
  else
  {
    TBSYS_LOG(WARN, "rs_node_mgr is null");
  }
}
//add:e

//add chujiajia [Paxos rs_election] 20151229:b
void ObRootServer2::do_stat_quorum_changed(char* buf, const int64_t buf_len, int64_t &pos)
{
  if(!is_master())
  {
    databuff_printf(buf, buf_len, pos, "target rootserver is not leader, please check inputinfo and try again!");
  }
  //mod pangtianze [Paxos rs_election] 20161010:b
  /*
  else if (NULL != ups_manager_)
  {
    ups_manager_->if_all_quorum_changed(buf, buf_len, pos, (int64_t)config_.ups_quorum_scale);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "Unknown error! Please try again!");
  }
  */
  else if (NULL == ups_manager_)
  {
    TBSYS_LOG(WARN, "ups_manager_ is null");
    databuff_printf(buf, buf_len, pos, "Unknown error! Please try again!");
  }
  else if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(WARN, "rs_node_mgr_ is null");
    databuff_printf(buf, buf_len, pos, "Unknown error! Please try again!");
  }
  else
  {
    ups_manager_->if_all_quorum_changed(buf, buf_len, pos, (int64_t)config_.ups_quorum_scale);
    rs_node_mgr_->if_all_quorum_in_rs_changed(buf, buf_len, pos, (int32_t)config_.ups_quorum_scale);
  }
  //mod:e
}
//add:e
//add jintianyang [paxos test] 20160530:b
void ObRootServer2::do_stat_ups_leader(char* buf, const int64_t buf_len, int64_t &pos)
{
    if(!is_master())
    {
      databuff_printf(buf, buf_len, pos, "target rootserver is not leader, please check inputinfo and try again!");
    }
    else if (NULL != ups_manager_)
    {
      ups_manager_->print_ups_leader(buf, buf_len, pos);
    }
    else
    {
      databuff_printf(buf, buf_len, pos, "Unknown error! Please try again!");
    }
}
//add:e

//add bingo [Paxos Cluster.Balance] 20161024:b
void ObRootServer2::do_stat_master_cluster_id(char* buf, const int64_t buf_len, int64_t &pos)
{
  print_master_cluster_id(buf, buf_len, pos);
}
//add:e

void ObRootServer2::do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
}

int64_t ObRootServer2::get_stat_value(const int32_t index)
{
  int64_t ret = 0;
  ObStat *stat = NULL;
  OB_STAT_GET(ROOTSERVER, stat);
  if (NULL != stat)
  {
    ret = stat->get_value(index);
  }
  return ret;
}

void ObRootServer2::do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (last_frozen_time_ == 0)
  {
    databuff_printf(buf, buf_len, pos, "merge: DONE");
  }
  else
  {
    if (check_all_tablet_safe_merged())
    {
      databuff_printf(buf, buf_len, pos, "merge: DONE");
    }
    else
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      int64_t max_merge_duration_us = config_.max_merge_duration_time;
      if (now > last_frozen_time_ + max_merge_duration_us)
      {
        databuff_printf(buf, buf_len, pos, "merge: TIMEOUT");
      }
      else
      {
        databuff_printf(buf, buf_len, pos, "merge: DOING");
      }
    }
  }
}

void ObRootServer2::do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int32_t num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (root_table_ != NULL)
  {
    root_table_->dump_unusual_tablets(last_frozen_mem_version_,
                                      (int32_t)config_.tablet_replicas_num, num);
  }
  databuff_printf(buf, buf_len, pos, "unusual_tablets_num: %d", num);
}

int ObRootServer2::do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  switch(stat_key)
  {
    case OB_RS_STAT_COMMON:
      do_stat_common(buf, buf_len, pos);
      break;
    case OB_RS_STAT_START_TIME:
      do_stat_start_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOCAL_TIME:
      do_stat_local_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_PROGRAM_VERSION:
      databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)", PACKAGE_STRING, RELEASEID);
      break;
    case OB_RS_STAT_PID:
      databuff_printf(buf, buf_len, pos, "pid: %d", getpid());
      break;
    case OB_RS_STAT_MEM:
      do_stat_mem(buf, buf_len, pos);
      break;
    case OB_RS_STAT_RS_STATUS:
      databuff_printf(buf, buf_len, pos, "rs_status: INITED");
      break;
    case OB_RS_STAT_FROZEN_VERSION:
      databuff_printf(buf, buf_len, pos, "frozen_version: %ld", last_frozen_mem_version_);
      break;
    case OB_RS_STAT_SCHEMA_VERSION:
      do_stat_schema_version(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOG_SEQUENCE:
      databuff_printf(buf, buf_len, pos, "log_seq: %lu", log_worker_->get_cur_log_seq());
      break;
    case OB_RS_STAT_LOG_FILE_ID:
      databuff_printf(buf, buf_len, pos, "log_file_id: %lu", log_worker_->get_cur_log_file_id());
      break;
    case OB_RS_STAT_TABLE_NUM:
      do_stat_table_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_TABLET_NUM:
      do_stat_tablet_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_CS:
      do_stat_cs(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS:
      do_stat_ms(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UPS:
      do_stat_ups(buf, buf_len, pos);
      break;
    case OB_RS_STAT_ALL_SERVER:
      do_stat_all_server(buf, buf_len, pos);
      break;
    //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
    case OB_RS_STAT_ALL_SERVER_IN_CLUSTERS:
      do_stat_all_server_in_clusters(buf, buf_len, pos);
      break;
    //add:e
    //add pangtianze [Paxos rs_election] 20150731:bs
    case OB_RS_STAT_PAXOS_CHANGED:
      do_stat_paxos_changed(buf, buf_len, pos);
      break;
    //add:e
    //add chujiajia [Paxos rs_election] 20151229:b
    case OB_RS_STAT_QUORUM_CHANGED:
      do_stat_quorum_changed(buf, buf_len, pos);
      break;
    //add:e
    //add jintianyang [paxos test]  20160530:b
    case OB_RS_STAT_UPS_LEADER:
      do_stat_ups_leader(buf,buf_len,pos);
      //add bingo [Paxos bug fix] 20160928:b
      break;
      //add:e
    //add bingo [Paxos Cluster.Balance] 20161024:b
    case OB_RS_STAT_MASTER_CLUSTER_ID:
      do_stat_master_cluster_id(buf, buf_len, pos);
      break;
    //add:e
    //add:e
    case OB_RS_STAT_FROZEN_TIME:
      do_stat_frozen_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SSTABLE_DIST:
      balancer_->nb_print_balance_infos(buf, buf_len, pos);
      break;
    case OB_RS_STAT_OPS_GET:
      databuff_printf(buf, buf_len, pos, "ops_get: %ld", get_stat_value(INDEX_SUCCESS_GET_COUNT));
      break;
    case OB_RS_STAT_OPS_SCAN:
      databuff_printf(buf, buf_len, pos, "ops_scan: %ld", get_stat_value(INDEX_SUCCESS_SCAN_COUNT));
      break;
    case OB_RS_STAT_FAIL_GET_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_get_count: %ld", get_stat_value(INDEX_FAIL_GET_COUNT));
      break;
    case OB_RS_STAT_FAIL_SCAN_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_scan_count: %ld", get_stat_value(INDEX_FAIL_SCAN_COUNT));
      break;
    case OB_RS_STAT_GET_OBI_ROLE_COUNT:
      databuff_printf(buf, buf_len, pos, "get_obi_role_count: %ld", get_stat_value(INDEX_GET_OBI_ROLE_COUNT));
      break;
    case OB_RS_STAT_MIGRATE_COUNT:
      databuff_printf(buf, buf_len, pos, "migrate_count: %ld", get_stat_value(INDEX_MIGRATE_COUNT));
      break;
    case OB_RS_STAT_COPY_COUNT:
      databuff_printf(buf, buf_len, pos, "copy_count: %ld", get_stat_value(INDEX_COPY_COUNT));
      break;
    case OB_RS_STAT_TABLE_COUNT:
      databuff_printf(buf, buf_len, pos, "table_count: %ld", get_stat_value(INDEX_ALL_TABLE_COUNT));
      break;
    case OB_RS_STAT_TABLET_COUNT:
      databuff_printf(buf, buf_len, pos, "tablet_count: %ld", get_stat_value(INDEX_ALL_TABLET_COUNT));
      break;
    case OB_RS_STAT_ROW_COUNT:
      databuff_printf(buf, buf_len, pos, "row_count: %ld", get_stat_value(INDEX_ALL_ROW_COUNT));
      break;
    case OB_RS_STAT_DATA_SIZE:
      databuff_printf(buf, buf_len, pos, "data_size: %ld", get_stat_value(INDEX_ALL_DATA_SIZE));
      break;
    case OB_RS_STAT_CS_NUM:
      do_stat_cs_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS_NUM:
      do_stat_ms_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MERGE:
      do_stat_merge(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UNUSUAL_TABLETS_NUM:
      do_stat_unusual_tablets_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SHUTDOWN_CS:
      balancer_->nb_print_shutting_down_progress(buf, buf_len, pos);
      break;
    case OB_RS_STAT_REPLICAS_NUM:
    default:
      databuff_printf(buf, buf_len, pos, "unknown or not implemented yet, stat_key=%d", stat_key);
      break;
  }
  return ret;
}

int ObRootServer2::make_checkpointing()
{
  OB_ASSERT(balancer_);
  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  //tbsys::CRLockGuard load_data_guard(balancer_->get_load_data_lock());
  //del:e
  tbsys::CRLockGuard rt_guard(root_table_rwlock_);
  tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
  tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
  int ret = worker_->get_log_manager()->do_check_point();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "failed to make checkpointing, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "made checkpointing");
  }
  return ret;
}

int ObRootServer2::register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  //add pangtianze [Paxos rs_election] 20160928:b
  else if (NULL == get_rs_node_mgr())
  {
      TBSYS_LOG(ERROR, "rs_node_mgr_ is NULL");
      ret = OB_NOT_INIT;
  }
  else if (RS_ELECTION_DOING == get_rs_node_mgr()->get_election_state())
  {
      ret = OB_RS_DOING_ELECTION;
      TBSYS_LOG(ERROR, "register ups error, addr: [%s], inner_port: [%d], "
          "seq_num: [%ld] ret: [%d]", to_cstring(addr), inner_port, log_seq_num, ret);
  }
  //add:e
  else if (OB_ALREADY_REGISTERED ==
           (ret = ups_manager_->register_ups(addr, inner_port,
                                             log_seq_num, lease, server_version)))
  {
    /* by design */
  }
  else if (OB_SUCCESS == ret)
  {
    // do nothing
  }
  else
  {
    TBSYS_LOG(ERROR, "register ups error, addr: [%s], inner_port: [%d], "
        "seq_num: [%ld] ret: [%d]", to_cstring(addr), inner_port, log_seq_num, ret);
  }
  return ret;
}

int ObRootServer2::receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat,
                                              //mod chujiajia [Paxos rs_election] 20151229:b
                                              //const common::ObiRole &obi_role)
                                              const common::ObiRole &obi_role, const int64_t &quorum_scale)
                                              //mod:e
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    //mod chujiajia [Paxos rs_election] 20151229:b
    //ret = ups_manager_->renew_lease(addr, stat, obi_role);
    ret = ups_manager_->renew_lease(addr, stat, obi_role, quorum_scale);
    //mod:e
  }
  return ret;
}

//add pangtianze [Paxos rs_election] 20150613:b
int ObRootServer2::receive_rs_heartbeat_resp(const common::ObMsgRsHeartbeatResp &hb_resp, const int64_t received_time)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = rs_node_mgr_->handle_rs_heartbeat_resp(hb_resp, received_time);
    if (OB_ENTRY_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "update rs info failed, rs=%s", to_cstring(hb_resp.addr_));
    }
  }
  return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20150618:b
int ObRootServer2::receive_rs_leader_broadcast(const common::ObMsgRsLeaderBroadcast lb,
                                               ObMsgRsLeaderBroadcastResp &lb_resq,
                                               const bool is_force)
{
  int ret = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret2 = rs_node_mgr_->handle_leader_broadcast(lb.addr_, lb.lease_, lb.term_, is_force)))
  {
    TBSYS_LOG(WARN, "refuse leader broadcast, src[%s], err=%d", lb.addr_.to_cstring(), ret2);
    rs_node_mgr_->set_leader_broadcast_resp(lb_resq, false);
  }
  else
  {
    rs_node_mgr_->set_leader_broadcast_resp(lb_resq, true);
  }

  return ret;
}
int ObRootServer2::receive_rs_vote_request(const ObMsgRsRequestVote rv, ObMsgRsRequestVoteResp &rv_resp)
{
  int ret = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret2 = rs_node_mgr_->handle_vote_request(rv.addr_, rv.lease_, rv.term_)))
  {
    TBSYS_LOG(WARN, "refuse vote request, src[%s], err=%d", rv.addr_.to_cstring(), ret2);
    rs_node_mgr_->set_vote_request_resp(rv_resp, false, ret2 == OB_ENTRY_NOT_EXIST);
  }
  else
  {
    rs_node_mgr_->set_vote_request_resp(rv_resp, true, false);
  }
  return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20150817:b
int ObRootServer2::receive_rs_vote_request_resp(const common::ObMsgRsRequestVoteResp &rv_resp)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = rs_node_mgr_->handle_vote_request_resp(rv_resp)))
  {
    TBSYS_LOG(WARN, "hand vote request response failed");
  }
  return ret;
}

int ObRootServer2::receive_rs_leader_broadcast_resp(const common::ObMsgRsLeaderBroadcastResp &lb_resp)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = rs_node_mgr_->handle_leader_broadcast_resp(lb_resp)))
  {
    TBSYS_LOG(WARN, "hand leader broadcast response failed");
  }
  return ret;
}
int ObRootServer2::send_rs_vote_request()
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = rs_node_mgr_->request_votes()))
  {
    TBSYS_LOG(ERROR, "send vote request failed, err=%d", ret);
  }
  return ret;
}
int ObRootServer2::send_rs_leader_broadcast()
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (rs_node_mgr_->leader_broadcast()))
  {
    TBSYS_LOG(ERROR, "send leader broadcast failed, err=%d", ret);
  }
  return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20170228:b
int ObRootServer2::sync_refresh_rs_list()
{
    int ret = OB_ERROR;
    const static int64_t retry_times = 3;
    for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
    {
      if (OB_SUCCESS != (ret = refresh_list_to_all()))
      {
        TBSYS_LOG(WARN, "fail to refresh rs list , retry_time=%ld, ret=%d", i, ret);
        sleep(1);
      }
      else
      {
        TBSYS_LOG(INFO, "refrest rs list in all servers success.");
      }
    }
    return ret;
}

int ObRootServer2::refresh_list_to_all()
{
    int ret = OB_SUCCESS;
    ObServer servers[OB_MAX_RS_COUNT];
    int32_t server_count = 0;
    //mod pangtianze [Paxos] 20170705:b:e
    rs_node_mgr_->get_all_servers(servers, server_count);  //wiil send all rs no matter it's alive or not

    //rs list to ups
    {
        ObServer ups_array[OB_MAX_UPS_COUNT];
        int32_t ups_count = 0;
        get_ups_manager()->get_online_ups_array(ups_array, ups_count);
        //ObUps *ups_array = get_ups_manager()->get_ups_array();
        for (int32_t i = 0; OB_SUCCESS == ret && i < OB_MAX_UPS_COUNT; ++i)
        {
            if (ups_array[i].is_valid())
            {
                if (OB_SUCCESS != (ret = worker_->get_rpc_stub().send_rs_list_to_server(
                                       ups_array[i], servers, server_count)))
                {
                    TBSYS_LOG(WARN, "fail to refresh list to ups[%s], err=%d", ups_array[i].to_cstring(), ret);
                }
            }
        }//end for
        if (OB_SUCCESS == ret)
        {
            TBSYS_LOG(DEBUG, "refresh list to all ups succ!");
        }
    }
    //rs list to cs/ms
    if (OB_SUCCESS == ret)
    {
       if (OB_SUCCESS != (ret = refresh_list_to_ms_and_cs(servers, server_count)))
       {
          TBSYS_LOG(WARN, "fail to refresh list to all ms/cs, err=%d", ret);
       }
       else
       {
          TBSYS_LOG(DEBUG, "refresh list to all ms/cs succ!");
       }
    }
    return ret;
}
int ObRootServer2::refresh_list_to_ms_and_cs(ObServer *servers, const int32_t server_count)
{
    int ret = OB_SUCCESS;
    ObServer tmp_server;
    if (this->is_master())
    {
      ObChunkServerManager::iterator it = this->server_manager_.begin();
      for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
      {
        //to cs
        if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
        {
          tmp_server = it->server_;
          tmp_server.set_port(it->port_cs_);
          ret = this->worker_->get_rpc_stub().send_rs_list_to_server(
                      tmp_server, servers, server_count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "send rs list to cs %s", to_cstring(tmp_server));
          }
          else
          {
            TBSYS_LOG(WARN, "send rs list to cs %s failed", to_cstring(tmp_server));
          }
        }
        if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
        {
          //to ms
          tmp_server = it->server_;
          tmp_server.set_port(it->port_ms_);
          ret = this->worker_->get_rpc_stub().send_rs_list_to_server(
                      tmp_server, servers, server_count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "send rs list to ms %s", to_cstring(tmp_server));
          }
          else
          {
            TBSYS_LOG(WARN, "send rs list to ms %s failed", to_cstring(tmp_server));
          }
        }
      } //end for
    } //end if master
    return ret;
}
//add:e
int ObRootServer2::ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->slave_failure(addr, slave_addr);
  }
  return ret;
}

int ObRootServer2::set_ups_config(int32_t read_master_master_ups_percentage
                                  //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                  //, int32_t read_slave_master_ups_percentage
                                  //del:e
                                  //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                                  ,const int32_t is_strong_consistent
                                  //add:e
                                  )
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  //add bingo [Paxos Cluster.Flow.UPS] 20170405:b
  config_.is_strong_consistency_read = (int64_t)is_strong_consistent;
  get_config_mgr()->dump2file();
  get_config_mgr()->get_update_task().write2stat();
  ObServer servers[OB_MAX_RS_COUNT];
  int32_t server_count = 0;
  int32_t succ_count = 0;
  get_rs_node_mgr()->get_all_alive_servers(servers, server_count);
  //send new is_strong_consistent to other rs
  for(int32_t i = 0; i < (server_count - 1); i++)
  {
    ret = get_rpc_stub().send_sync_is_strong_consistent_req(servers[i], config_.network_timeout, config_.is_strong_consistency_read);
    if(ret == OB_SUCCESS)
    {
      succ_count++;
    }
    else
    {
      TBSYS_LOG(WARN, "send sync_is_strong_consistent request to rs[%s] failed! ret=%d.", servers[i].to_cstring(), ret);
    }
  }
  if (succ_count != server_count-1)
  {
     ret = OB_RS_NOT_EXIST;
  }
  else
  {
     TBSYS_LOG(INFO, "change is_strong_consistent in all rootserver succ.");
  }
  //add:e
  if(OB_SUCCESS == ret)
  {
    ret = ups_manager_->set_ups_config(read_master_master_ups_percentage
                                       //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                       //, read_slave_master_ups_percentage
                                       //del:e
                                       //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                                       ,(int32_t)config_.is_strong_consistency_read
                                       //add:e
                                       );
  }
  return ret;
}
int ObRootServer2::set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_config(ups, ms_read_percentage, cs_read_percentage);
  }
  return ret;
}

int ObRootServer2::change_ups_master(const common::ObServer &ups, bool did_force)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_master(ups, did_force);
  }
  return ret;
}

int ObRootServer2::get_ups_list(common::ObUpsList &ups_list
                                //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                , const int32_t cluster_id
                                //add:e
                                )
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->get_ups_list(ups_list
                                     //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                     , cluster_id
                                     //add:e
                                     );
  }
  return ret;
}

int ObRootServer2::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  return server_manager_.serialize_cs_list(buf, buf_len, pos);
}

int ObRootServer2::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos
                                     //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
                                     , const int32_t cluster_id
                                     //add:e
                                     ) const
{
  return server_manager_.serialize_ms_list(buf, buf_len, pos
                                           //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
                                           , cluster_id
                                           //add:e
                                           );
}

int ObRootServer2::serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  const ObServerStatus* it = NULL;
  int ms_num = 0;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it)
  {
    if (ObServerStatus::STATUS_DEAD != it->ms_status_)
    {
      ms_num++;
    }
  }
  ret = serialization::encode_vi32(buf, buf_len, pos, ms_num);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize error");
  }
  else
  {
    int i = 0;
    for (it = server_manager_.begin();
         it != server_manager_.end() && i < ms_num; ++it)
    {
      if (ObServerStatus::STATUS_DEAD != it->ms_status_)
      {
        ObServer addr = it->server_;
        addr.set_port((int32_t)config_.obconnector_port);
        ret = addr.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObServer serialize error, ret=%d", ret);
          break;
        }
        else
        {
          int64_t reserved = 0;
          ret = serialization::encode_vi64(buf, buf_len, pos, reserved);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "encode_vi64 error, ret=%d", ret);
            break;
          }
          else
          {
            ++i;
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::grant_eternal_ups_lease()
{
  int ret = OB_SUCCESS;
  if (NULL == ups_heartbeat_thread_
      || NULL == ups_check_thread_)
  {
    TBSYS_LOG(ERROR, "ups_heartbeat_thread is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ups_check_thread_->stop();
    ups_heartbeat_thread_->stop();
  }
  return ret;
}

int ObRootServer2::cs_import_tablets(const uint64_t table_id, const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id)
  {
    TBSYS_LOG(WARN, "invalid table id");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > tablet_version)
  {
    TBSYS_LOG(WARN, "invalid table version, version=%ld", tablet_version);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t version = tablet_version;
    if (0 == version)
    {
      version = last_frozen_mem_version_;
    }
    int32_t sent_num = 0;
    ObServer cs;
    ObChunkServerManager::iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        cs = it->server_;
        cs.set_port(it->port_cs_);
        // CS鏀跺埌娑堟伅鍚庣珛鍗宠繑鍥炴垚鍔燂紝鐒跺悗鍐嶅畬鎴愬疄闄呯殑瀵煎叆
        if (OB_SUCCESS != (ret = worker_->get_rpc_stub().import_tablets(cs, table_id, version, config_.network_timeout)))
        {
          TBSYS_LOG(WARN, "failed to send msg to cs, err=%d", ret);
          break;
        }
        sent_num++;
      }
    }
    if (OB_SUCCESS == ret && 0 == sent_num)
    {
      TBSYS_LOG(WARN, "no chunkserver");
      ret = OB_DATA_NOT_SERVE;
    }
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "send msg to import tablets, table_id=%lu version=%ld cs_num=%d",
                table_id, version, sent_num);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to import tablets, err=%d table_id=%lu version=%ld cs_num=%d",
                ret, table_id, version, sent_num);
    }
  }
  return ret;
}

bool ObRootServer2::is_master() const
{
  return worker_->get_role_manager()->is_master();
}
//add pangtianze [Paxos rs_election] 20170717:b
bool ObRootServer2::is_slave() const
{
   return worker_->get_role_manager()->is_slave();
}
//add:e
//add chujiajia [Paxos rs_election] 20151210:b
common::ObRoleMgr::Role ObRootServer2::get_rs_role()
{
  return worker_->get_role_manager()->get_role();
}
//add:e

int ObRootServer2::receive_hb(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms, const ObRole role
                              //add pangtianze [Paxos rs_election] 20170420:b
                              ,const bool valid_flag
                              //add:e
                              )
{
  int64_t  now = tbsys::CTimeUtil::getTime();
  int err = server_manager_.receive_hb(server, now, role == OB_MERGESERVER ? true : false, is_listen_ms, sql_port
                                       //add pangtianze [Paxos rs_election] 20170420:b
                                       ,false
                                       ,valid_flag
                                       //add:e
                                       );
  // realive or regist again
  if (2 == err)
  {
    if (role != OB_MERGESERVER)
    {
      TBSYS_LOG(INFO, "receive cs alive heartbeat. need add commit log and force to report tablet");
      if (OB_SUCCESS != (err = log_worker_->regist_cs(server, "hb server version null", now)))
      {
        TBSYS_LOG(WARN, "fail to write log for regist_cs. err=%d", err);
      }
      else
      {
        err = worker_->get_rpc_stub().request_report_tablet(server);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "fail to force cs to report tablet info. server=%s, err=%d", server.to_cstring(), err);
        }
      }
    }
    TBSYS_LOG(INFO, "receive server alive heartbeat without version info:server[%s]", server.to_cstring());
    if (is_master())
    {
      if (OB_CHUNKSERVER == role)
      {
        commit_task(SERVER_ONLINE, OB_CHUNKSERVER, server, 0, "hb server version null");
      }
      // not listener merge server
      else if (!is_listen_ms)
      {
        commit_task(SERVER_ONLINE, OB_MERGESERVER, server, sql_port, "hb server version null");
      }
    }
  }
  return err;
}

int ObRootServer2::force_heartbeat_all_servers(void)
{ // only used in boot strap, so no need to lock last_frozen_mem_version_
  int ret = OB_SUCCESS;
  ObServer tmp_server;
  if (this->is_master())
  {
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    ObChunkServerManager::iterator it = this->server_manager_.begin();
    for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_cs_);
        //mod liumz, [bugfix: heartbeat deserialize faild when bootstrap]20160520:b
        /*ret = worker_->get_rpc_stub().heartbeat_to_cs(tmp_server,
            config_.cs_lease_duration_time, last_frozen_mem_version_,
            get_schema_version(), get_config_version());*/
        ret = worker_->get_rpc_stub().heartbeat_to_cs_with_index(tmp_server,
            config_.cs_lease_duration_time, last_frozen_mem_version_,
            get_schema_version(), get_config_version(), get_index_beat());
        //mod:e

        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "force hearbeat to cs %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "foce heartbeat to cs %s failed", to_cstring(tmp_server));
        }
      }
      if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_ms_);
        ret = worker_->get_rpc_stub().heartbeat_to_ms(tmp_server,
            config_.cs_lease_duration_time, last_frozen_mem_version_,
            get_schema_version(), get_obi_role(),
            get_privilege_version(), get_config_version());
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "force hearbeat to ms %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "foce heartbeat to ms %s failed", to_cstring(tmp_server));
        }
      }
    } //end for
  } //end if master
  return ret;
}

int ObRootServer2::force_sync_schema_all_servers(const ObSchemaManagerV2 &schema)
{
  int ret = OB_SUCCESS;
  ObServer tmp_server;
  if (this->is_master())
  {
    ObChunkServerManager::iterator it = this->server_manager_.begin();
    for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_cs_);
        ret = this->worker_->get_rpc_stub().switch_schema(tmp_server,
            schema, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "sync schema to cs %s, version[%ld]", to_cstring(tmp_server), schema.get_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema cs %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to cs %s failed", to_cstring(tmp_server));
        }
      }
      if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
      {
        //hb to ms
        tmp_server = it->server_;
        tmp_server.set_port(it->port_ms_);
        ret = this->worker_->get_rpc_stub().switch_schema(tmp_server,
            schema, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          // do nothing
          TBSYS_LOG(DEBUG, "sync schema to ms %s, version[%ld]", to_cstring(tmp_server), schema.get_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema ms %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to ms %s failed", to_cstring(tmp_server));
        }
      }
    } //end for
  } //end if master
  return ret;
}
//add zhaoqiong [Schema Manager] 20150327:b
int ObRootServer2::force_sync_schema_mutator_all_servers(const common::ObSchemaMutator &schema_mutator)
{
  int ret = OB_SUCCESS;
  ObServer tmp_server;
  if (this->is_master())
  {
    ObChunkServerManager::iterator it = this->server_manager_.begin();
    for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
    {
      //for cs
      if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_cs_);
        ret = this->worker_->get_rpc_stub().switch_schema_mutator(tmp_server,
            schema_mutator, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "sync schema to cs %s, version[%ld]", to_cstring(tmp_server), schema_mutator.get_end_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema cs %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to cs %s failed", to_cstring(tmp_server));
        }
      }
      //for ms
      if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
      {
        //hb to ms
        tmp_server = it->server_;
        tmp_server.set_port(it->port_ms_);
        ret = this->worker_->get_rpc_stub().switch_schema_mutator(tmp_server,
            schema_mutator, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          // do nothing
          TBSYS_LOG(DEBUG, "sync schema to ms %s, version[%ld]", to_cstring(tmp_server), schema_mutator.get_end_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema ms %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to ms %s failed", to_cstring(tmp_server));
        }
      }
    } //end for
  } //end if master
  return ret;
}
//add:e
//for bypass
int ObRootServer2::prepare_bypass_process(common::ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_table_id = 0;
  {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    tbsys::CThreadGuard mutex_guard(&mutex_lock_);
    ret = schema_service_->get_max_used_table_id(max_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get max used table id. ret=%d", ret);
    }
    else
    {
      ret = get_new_table_id(max_table_id, table_name_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to prepare for bypass");
      }
      else
      {
        ret = ddl_tool_.update_max_table_id(max_table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to update max table id. ret=%d,max_table_id=%ld", ret, max_table_id);
        }
      }
    }
  }
  int64_t count = 0;
  ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150327
  //mod zhaoqiong [Schema Manager] 20150327:b
  //get schema mutator and refresh local schema
  //if (OB_SUCCESS != (ret = refresh_new_schema(count)))
  if (OB_SUCCESS != (ret = refresh_new_schema(count,schema_mutator)))
  //mod:e
  {
    TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
  }
  else
  {
    //mod zhaoqiong [Schema Manager] 20150327:b
    //switch schema mutator not the full schema to all servers
    //if (OB_SUCCESS != (ret = notify_switch_schema(false)))
    if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
    //mod:e
    {
      TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify switch schema after modify table_id");
      write_schema_to_file();
    }
  }
  return ret;
}
int ObRootServer2::write_schema_to_file()
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 *out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_schema(false, false, *out_schema)))
    {
      TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    uint64_t max_table_id = 0;
    ret = schema_service_->get_max_used_table_id(max_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get max used table id, ret=%d", ret);
    }
    else
    {
      out_schema->set_max_table_id(max_table_id);
    }
  }
  if (OB_SUCCESS == ret)
  {

    ret = out_schema->write_to_file(SCHEMA_FILE_NAME);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "write schema to file. ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "write schema to file success.");
    }
  }
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}
int ObRootServer2:: get_new_table_id(uint64_t &max_table_id, ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  if (table_name_id.count() <= 0)
  {
    TBSYS_LOG(WARN, "invalid argument. table_name_id.count=%ld", table_name_id.count());
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < table_name_id.count(); i++)
    {
      table_name_id.at(i).second = ++max_table_id;
    }
  }
  return ret;
}

int ObRootServer2::slave_clean_root_table()
{
  int ret = OB_SUCCESS;

  { // lock root table
    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CWLockGuard guard(root_table_rwlock_);
    TBSYS_LOG(INFO, "slave rootserver start to clean root table, and set bypass version to zero");
    root_table_->clear();
  }

  { // lock frozen version
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    bypass_process_frozen_men_version_ = 0;
  }
  return ret;
}

int ObRootServer2::clean_root_table()
{
  int ret = OB_SUCCESS;
  ObBypassTaskInfo table_name_id;
  table_name_id.set_operation_type(CLEAN_ROOT_TABLE);
  ret = start_bypass_process(table_name_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to start clean roottable. ret=%d", ret);
  }
  else
  {
    //bugfix
    //send clean_root_table status to slave rootserver
    ret = log_worker_->clean_root_table();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write log for clean_root_table. ret=%d", ret);
    }
  }
  return ret;
}

int ObRootServer2::start_bypass_process(ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  bool is_all_merged = false;
  if (CLEAN_ROOT_TABLE != table_name_id.get_operation_type())
  {
    //check rs stat
    ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
    if (!is_all_merged)
    {
      TBSYS_LOG(WARN, "rootserer refuse to do bypass process. state=%s", state_.get_state_str());
      ret = OB_RS_STATE_NOT_ALLOW;
    }
  }
  //淇濊瘉瀵煎叆姹囨姤鐨勮繃绋嬩腑涓茶杩涜
  if (OB_SUCCESS == ret)
  {
    ret = state_.set_bypass_flag(true);
    if (OB_SUCCESS != ret)
    {
      ret = OB_EAGAIN;
      TBSYS_LOG(WARN, "already have some operaiton task doing. retry later.");
    }
    else
    {
      TBSYS_LOG(INFO, "opration process start. type=%d", table_name_id.get_operation_type());
    }
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    ret = operation_helper_.start_operation(schema_manager_for_cache_, table_name_id, last_frozen_mem_version_);
    if (OB_SUCCESS != ret)
    {
      state_.set_bypass_flag(false);
      TBSYS_LOG(WARN, "fail to start operation process. ,set false.ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(TRACE, "operation_helper start to operaiton success.");
    }
  }
  if (OB_SUCCESS == ret)
  {
    //璁板綍褰撳墠鐨刦rozen_version锛岄槻姝S閲嶅惎鍚庣姸鎬佷涪澶憋紝閫犳垚CS寮?濮嬫瘡鏃ュ悎骞?
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    if (0 >= bypass_process_frozen_men_version_)
    {
      bypass_process_frozen_men_version_ = last_frozen_mem_version_;
      TBSYS_LOG(INFO, "operation process start. bypass process frozen_mem_version=%ld", bypass_process_frozen_men_version_);
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    operation_duty_.init(worker_);
    if (OB_SUCCESS != (ret = timer_.schedule(operation_duty_, 30 * 1000 * 1000, false)))
    {
      TBSYS_LOG(WARN, "fail to schedule operation duty, ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "start to schedule check_process.");
    }
  }
  return ret;
}

int ObRootServer2::check_bypass_process()
{
  int ret = OB_SUCCESS;
  if (!state_.get_bypass_flag())
  {
    TBSYS_LOG(WARN, "rootserver have no bypass duty now.");
    ret = OB_ERROR;
  }
  OperationType type;
  if (OB_SUCCESS == ret)
  {
    ret = operation_helper_.check_process(type);
    if (OB_BYPASS_TIMEOUT == ret)
    {
      TBSYS_LOG(WARN, "bypass process timeout. check it");
      state_.set_bypass_flag(false);
      tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      bypass_process_frozen_men_version_ = -1;
      //set_root_server_state();
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
    else if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "operation process finished. operation_type = %d", type);
      state_.set_bypass_flag(false);
      tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      //set_root_server_state();
      bypass_process_frozen_men_version_ = 0;
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
    else
    {
      if (OB_SUCCESS != timer_.schedule(operation_duty_, 30 * 1000 * 1000, false))
      {
        TBSYS_LOG(WARN, "fail to schedule operation duty, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "scheule operation_duty success. time=30000000");
      }
    }
  }
  return ret;
}

int ObRootServer2::bypass_meta_data_finished(const OperationType type, ObRootTable2 *root_table,
    ObTabletInfoManager *tablet_manager, common::ObSchemaManagerV2 *schema_mgr)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "bypass new meta data is ready. try to refresh root_server's");
  switch(type)
  {
    case IMPORT_TABLE:
      {
        common::ObArray<uint64_t> delete_tables;
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(WARN, "operation over, use new root table fail. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = switch_bypass_schema(schema_mgr, delete_tables)))
        {
          TBSYS_LOG(WARN, "swtich bypass schema fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = operation_helper_.set_delete_table(delete_tables)))
        {
          TBSYS_LOG(WARN, "fail to set delete table. ret=%d", ret);
        }
      }
      break;
    case IMPORT_ALL:
      {
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(WARN, "fail to use new root table. ret=%d", ret);
        }
      }
      break;
    case CLEAN_ROOT_TABLE:
      {
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(INFO, "fail to use new root table, ret=%d", ret);
        }
      }
      break;
    default:
      TBSYS_LOG(WARN, "invalid type. type=%d", type);
      break;
  }
  return ret;
}
const char* ObRootServer2::get_bypass_state() const
{
  if (const_cast<ObRootServerState&>(state_).get_bypass_flag())
  {
    return "DOING";
  }
  else if (!const_cast<ObRootServerState&>(state_).get_bypass_flag() && 0 < bypass_process_frozen_men_version_)
  {
    return "RETRY";
  }
  else if (0 == bypass_process_frozen_men_version_)
  {
    return "DONE";
  }
  else if (-1 == bypass_process_frozen_men_version_)
  {
    return "INTERRUPT";
  }
  else
  {
    return "ERROR";
  }
}
int ObRootServer2::switch_bypass_schema(common::ObSchemaManagerV2 *schema_manager, common::ObArray<uint64_t> &delete_tables)
{
  int ret = OB_SUCCESS;
  delete_tables.clear();
  if (NULL == schema_manager)
  {
    TBSYS_LOG(ERROR, "invalid schema_manager. is NULL");
    ret = OB_ERROR;
  }
  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_schema(false, false, *out_schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "force refresh schema manager failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = get_deleted_tables(*out_schema, *schema_manager, delete_tables);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get delete tables. ret=%d", ret);
    }
  }
  common::ObArray<TableSchema> table_schema_array;
  common::ObArray<TableSchema> new_table_schema;
  if (OB_SUCCESS == ret)
  {
    ret = common::ObSchemaHelper::transfer_manager_to_table_schema(*out_schema, table_schema_array);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to transfer manager to table_schema, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = common::ObSchemaHelper::transfer_manager_to_table_schema(*schema_manager, new_table_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to transfer manager to table_schema, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    for(int64_t i = 0; i < delete_tables.count(); i++)
    {
      uint64_t new_table_id = 0;
      uint64_t old_table_id = delete_tables.at(i);
      TBSYS_LOG(INFO, "to delete table is=%ld", old_table_id);
      int64_t table_index = -1;
      char* table_name = NULL;
      //add dolphin [database manager]@20150613
      ObString dt;
      char* db_name = NULL;
      for (int64_t j = 0; j < table_schema_array.count(); j++)
      {
        if (table_schema_array.at(j).table_id_ == old_table_id)
        {
          table_name = table_schema_array.at(j).table_name_;
          db_name = table_schema_array.at(j).dbname_;
          table_index = j;
          break;
        }
      }
      if (table_index == -1 || table_name == NULL)
      {
        TBSYS_LOG(WARN, "fail to find table schema, table_id=%ld, table_name=%p", old_table_id, table_name);
        ret = OB_ERROR;
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "to delete table name=%s, table_id=%ld", table_name, old_table_id);
      }
      bool find = false;
      //add dolphin [database manager]@20150613:b
      char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
      dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
      dt.concat(db_name,table_name);
      //add:e
      for (int64_t j = 0; j < new_table_schema.count(); j++)
      {
        if (OB_SUCCESS == strncmp(new_table_schema.at(j).table_name_, /** modify dolphin [database manager]@20150613 table_name */ dt.ptr(), strlen(table_name)))
        {
          find = true;
          new_table_id = new_table_schema.at(j).table_id_;
          break;
        }
      }
      if (find == false)
      {
        TBSYS_LOG(WARN, "fail to find new_table_id in schema_manager. table_name=%s", table_name);
        ret = OB_ERROR;
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "get new table_id for table. table_name=%s, table_id=%ld",
            table_name, new_table_id);
      }
      TBSYS_LOG(INFO, "bypass process start modify table id in inner table. table_index=%ld, table_name=%s, new_table_id=%ld",
          table_index, table_schema_array.at(table_index).table_name_, new_table_id);
      ret = ddl_tool_.modify_table_id(table_schema_array.at(table_index), new_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to modify table id. table_name=%s", table_name);
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "modify table id for bypass table success. table_name=%s, new_table_id=%ld",
            table_name, new_table_id);
      }
    }
  }
  int64_t count = 0;
  ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150327
  //mod zhaoqiong [Schema Manager] 20150327:b
  //get schema mutator and refresh local schema
  //if (OB_SUCCESS != (ret = refresh_new_schema(count)))
  if (OB_SUCCESS != (ret = refresh_new_schema(count,schema_mutator)))
  //mod:e
  {
    TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
  }
  else
  {
    //mod zhaoqiong [Schema Manager] 20150327:b
    //switch schema mutator not the full schema to all servers
    //if (OB_SUCCESS != (ret = notify_switch_schema(false)))
    if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
    //mod:e
    {
      TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify switch schema after modify table_id");
      write_schema_to_file();
    }
  }
  return ret;
}

int ObRootServer2::use_new_root_table(ObRootTable2 *root_table, ObTabletInfoManager* tablet_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == root_table || NULL == tablet_manager)
  {
    TBSYS_LOG(ERROR, "invalid argument. root_table=%p, tablet_manager=%p",
        root_table, tablet_manager);
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CThreadGuard root_guard(&root_table_build_mutex_);
    switch_root_table(root_table, tablet_manager);
    make_checkpointing();
    root_table = NULL;
    tablet_manager = NULL;
    dump_root_table();
  }
  return ret;
}
bool ObRootServer2::is_bypass_process()
{
  return state_.get_bypass_flag();
}
int ObRootServer2::cs_load_sstable_done(const ObServer &cs,
    const common::ObTableImportInfoList &table_list, const bool is_load_succ)
{
  int32_t cs_index = get_server_index(cs);
  return operation_helper_.cs_load_sstable_done(cs_index, table_list, is_load_succ);
}

int ObRootServer2::cs_delete_table_done(const ObServer &cs,
    const uint64_t table_id, const bool is_succ)
{
  int ret = OB_SUCCESS;
  UNUSED(cs);
  UNUSED(table_id);
  UNUSED(is_succ);
  //del lbzhong [Paxos Cluster.Balance] 20160705:b
  /*
  if (is_loading_data())
  {
    if (is_succ)
    {
      TBSYS_LOG(INFO, "cs finish delete table. table_id=%ld, cs=%s", table_id, to_cstring(cs));
    }
    else
    {
      TBSYS_LOG(ERROR, "fail to delete table for bypass. table_id=%ld, cs=%s, ret=%d",
          table_id, to_cstring(cs), ret);
    }
  }
  else
  {
  */
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "rootserver not in bypass process.");
  //} //del lbzhong [Paxos Cluster.Balance] 20160705:b:e
  return ret;
}

void ObRootServer2::set_bypass_version(const int64_t version)
{// should only used in log worker during log replay
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  bypass_process_frozen_men_version_ = version;
}

ObRootOperationHelper* ObRootServer2::get_bypass_operation()
{
  return &operation_helper_;
}

int ObRootServer2::unlock_frozen_version()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if (bypass_process_frozen_men_version_ == 0)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "frozen mem version is not locked, no need to unlock");
  }

  bypass_process_frozen_men_version_ = 0;
  if (OB_SUCCESS != log_worker_->set_frozen_version_for_brodcast(0))
  {
    TBSYS_LOG(ERROR, "failed to log unlock_frozen_version");
  }
  return ret;
}

int ObRootServer2::lock_frozen_version()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if (bypass_process_frozen_men_version_ > 0)
  {
    ret = OB_EAGAIN;
    TBSYS_LOG(WARN, "frozen mem version is already lock, no need to lock again");
  }
  else
  {
    bypass_process_frozen_men_version_ = last_frozen_mem_version_;
    if (OB_SUCCESS != log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_))
    {
      TBSYS_LOG(ERROR, "failed to log lock_frozen_version, last_frozen_mem_version_ =%ld", last_frozen_mem_version_);
    }
  }
  return ret;
}

int64_t ObRootServer2::get_frozen_version_for_cs_heartbeat() const
{
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  int64_t ret = last_frozen_mem_version_;
  if (bypass_process_frozen_men_version_ > 0)
  {
    TBSYS_LOG(DEBUG, "rootserver in bypass process. broadcast version=%ld", bypass_process_frozen_men_version_);
    ret = bypass_process_frozen_men_version_;
  }
  else
  {
    ret = last_frozen_mem_version_;
  }
  return ret;
}

int ObRootServer2::change_table_id(const int64_t table_id, const int64_t new_table_id)
{
  int ret = OB_SUCCESS;
  {
    tbsys::CWLockGuard schema_guard(schema_manager_rwlock_);
    tbsys::CThreadGuard guard(&mutex_lock_);
    uint64_t max_table_id = new_table_id;
    if (max_table_id == 0)
    {
      ret = schema_service_->get_max_used_table_id(max_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get max used table id. ret=%d", ret);
      }
      else
      {
        ++max_table_id;
        TBSYS_LOG(INFO, "new max_table_id = %ld, update to inner table.", max_table_id);
        ret = ddl_tool_.update_max_table_id(max_table_id);
        if (OB_SUCCESS!= ret)
        {
          TBSYS_LOG(WARN, "fail to update max table id. ret=%d, max_table_id=%ld", ret, max_table_id);
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      TableSchema table_schema;
      ret = get_table_schema(table_id, table_schema);
      TBSYS_LOG(INFO, "table name=%s, new_table_id=%ld", table_schema.table_name_, max_table_id);

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get table schema. table_id=%ld, ret=%d", table_id, ret);
      }
      else if (OB_SUCCESS != (ret = ddl_tool_.modify_table_id(table_schema, max_table_id)))
      {
        TBSYS_LOG(WARN, "fail to change table_id.table_id=%ld, ret=%d", max_table_id, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "change table id success. table name=%s, new_table_id=%ld", table_schema.table_name_, max_table_id);
      }
    }
  }
  int64_t count = 0;
  ObSchemaMutator schema_mutator;//add zhaoqiong [Schema Manager] 20150327
  if (OB_SUCCESS == ret)
  {
    //mod zhaoqiong [Schema Manager] 20150327:b
    //get schema mutator and refresh local schema
    //if (OB_SUCCESS != (ret = refresh_new_schema(count)))
    if (OB_SUCCESS != (ret = refresh_new_schema(count,schema_mutator)))
    //mod:e
    {
      TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
    }
    else
    {
      //mod zhaoqiong [Schema Manager] 20150327:b
      //switch schema mutator not the full schema to all servers
      //if (OB_SUCCESS != (ret = refresh_new_schema(count)))
      if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
      //mod:e
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema after modify table_id");
        write_schema_to_file();
      }
    }
  }
  return ret;
}

int ObRootServer2::add_range_for_load_data(const common::ObList<ObNewRange*> &range_table)
{ // check range list before call this method
  int ret = OB_SUCCESS;
  ObTabletReportInfoList report_info;
  for (common::ObList<ObNewRange*>::const_iterator it = range_table.begin();
      it != range_table.end() && OB_SUCCESS == ret; ++it)
  {
    ObTabletReportInfo tablet;
    tablet.tablet_info_.range_.table_id_ = (*it)->table_id_;
    tablet.tablet_info_.range_.border_flag_ = (*it)->border_flag_;
    tablet.tablet_info_.range_.start_key_ = (*it)->start_key_;
    tablet.tablet_info_.range_.end_key_ = (*it)->end_key_;
    ret = report_info.add_tablet(tablet);
    if (ret == OB_ARRAY_OUT_OF_RANGE)
    {
      TBSYS_LOG(DEBUG, "report info list is full, add to root table new");
      ret = add_range_to_root_table(report_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add range to root table. ret=%d", ret);
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "report info list full, clean it");
        report_info.reset();
        ret = report_info.add_tablet(tablet);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add range to report list, ret=%d", ret);
          break;
        }
      }
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to add range to report list, ret=%d", ret);
      break;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = add_range_to_root_table(report_info);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to add range to root table. ret=%d", ret);
    }
  }
  return ret;
}

int ObRootServer2::add_range_to_root_table(const ObTabletReportInfoList &tablets, const bool is_replay_log)
{
  int ret = OB_SUCCESS;
  ObServer server;
  int32_t server_index = -1;
  int64_t frozen_mem_version = last_frozen_mem_version_;
  TBSYS_LOG_US(INFO, "[NOTICE] add range for load data, count=%ld, version=%ld",
      tablets.tablet_list_.get_array_index(), frozen_mem_version);
  if (is_master() && !is_replay_log)
  {
    log_worker_->add_range_for_load_data(tablets);
  }
  ret = got_reported(tablets, server_index, frozen_mem_version, true, is_replay_log);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to add tablets range to root table. ret=%d", ret);
  }
  return ret;
}

int ObRootServer2::change_table_id(const ObString& table_name, const uint64_t new_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t old_table_id = 0;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  //modify dolphin [database manager]@20150616:b
  //ret = schema_service_->get_table_schema(table_name, table_schema);
  ret = schema_service_->get_table_schema(table_name, table_schema,ObString::make_string(""));
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs of the master cluster can do change table id");
  }
  else
  {
    old_table_id = table_schema.table_id_;
    ret = change_table_id(old_table_id, new_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to change table id for load data. new_table_id=%lu, old_table_id=%lu, ret=%d",
          new_table_id, old_table_id, ret);
    }
  }
  return ret;
}

int ObRootServer2::get_table_id(const ObString table_name, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  //modify dolphin [database manager]@20150616:b
  //ret = schema_service_->get_table_schema(table_name, table_schema);
  ret = schema_service_->get_table_schema(table_name, table_schema);
  //modify:e
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else
  {
    table_id = table_schema.table_id_;
  }
  return ret;
}

int ObRootServer2::load_data_done(const ObString table_name, const uint64_t old_table_id)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  ret = schema_service_->get_table_schema(table_name, table_schema);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else if (!is_master())
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs can do load_data_done");
  }
  else if (table_schema.table_id_ == old_table_id)
  {
    ret = OB_ERR_SYS;
    TBSYS_LOG(ERROR, "old table id must not same as current table id, cant do load_data_done. table_name=%s, table_id=%lu",
        to_cstring(table_name), old_table_id);
  }

  //delete old table in roottable
  if (OB_SUCCESS == ret)
  {
    ObArray<uint64_t> delete_table;
    delete_table.push_back(old_table_id);
    ret = delete_tables(false, delete_table);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to delete tables in roottable for load_data_done, table_id=%lu, ret=%d", old_table_id, ret);
    }
  }

  //delete sstable in chunkserver
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(server_manager_rwlock_);
    ObChunkServerManager::const_iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (OB_SUCCESS != get_rpc_stub().request_cs_delete_table(it->server_, old_table_id, config_.network_timeout))
        {
          TBSYS_LOG(ERROR, "fail to request cs to delete table,table_id=%lu. cs_addr=%s", old_table_id, it->server_.to_cstring());
        }
      }
    }
  }
  return ret;
}

//bypass process error, clean all data
int ObRootServer2::load_data_fail(const uint64_t new_table_id)
{
  int ret = OB_SUCCESS;
  if (!is_master())
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs can do load_data_done");
  }
  else
  {
    //delete roottable
    ObArray<uint64_t> delete_table;
    delete_table.push_back(new_table_id);
    ret = delete_tables(false, delete_table);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to delete tables in roottable, table_id=%ld, ret=%d", new_table_id, ret);
    }
  }

  //delete sstable in chunkserver
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(server_manager_rwlock_);
    ObChunkServerManager::const_iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (OB_SUCCESS != get_rpc_stub().request_cs_delete_table(it->server_, new_table_id, config_.network_timeout))
        {
          TBSYS_LOG(ERROR, "fail to request cs to delete table,table_id=%lu. cs_addr=%s", new_table_id, it->server_.to_cstring());
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::init_first_meta()
{
  int ret = OB_SUCCESS;
  ObTabletMetaTableRow first_meta_row;
  first_meta_row.set_tid(OB_FIRST_TABLET_ENTRY_TID);
  first_meta_row.set_table_name(ObString::make_string(FIRST_TABLET_TABLE_NAME));
  first_meta_row.set_start_key(ObRowkey::MIN_ROWKEY);
  first_meta_row.set_end_key(ObRowkey::MAX_ROWKEY);
  ObTabletReplica r;
  r.version_ = 1;
  ObServer cs;
  r.cs_ = cs;
  r.row_count_ = 0;
  r.occupy_size_ = 0;
  r.checksum_ = 0;
  if (OB_SUCCESS != (ret = first_meta_row.add_replica(r)))
  {
    TBSYS_LOG(ERROR, "failed to add replica for the meta row, err=%d", ret);
  }
  else
  {
    ret = first_meta_->init(first_meta_row);
    if (OB_INIT_TWICE == ret || OB_SUCCESS == ret)
    {
      TBSYS_LOG(WARN, "failed to init first meta, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "init first meta success.");
    }
  }
  return ret;
}

int ObRootServer2::start_import(const ObString& table_name, const uint64_t table_id, ObString& uri)
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  int64_t start_time = tbsys::CTimeUtil::getTime();
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_.master_root_server_ip, static_cast<int32_t>(config_.master_root_server_port));

  TBSYS_LOG(INFO, "[import] receive request: start import table=%.*s with new table_id=%lu, uri=%.*s",
      table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master of master cluster, cant start import");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (config_.enable_load_data == false)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_stub.import(master_rs, table_name, table_id, uri, start_time, config_.import_rpc_timeout);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "succeed to start import, master_rs=%s, table_name=%.*s, table_id=%lu, uri=%.*s start_time=%ld",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to start import, master_rs=%s, table_name=%.*s,"
          " table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id,
          uri.length(), uri.ptr(), start_time, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < slave_count; ++i)
    {
      ret = rpc_stub.import(slave_rs[i], table_name, table_id, uri, start_time, config_.import_rpc_timeout);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "succeed to start import, slave_rs[%ld]=%s, "
            "table_name=%.*s, table_id=%lu, uri=%.*s, start_time=%ld",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(),
            table_id, uri.length(), uri.ptr(), start_time);
      }
      else
      {
        TBSYS_LOG(WARN, "failed to start import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(),
            table_id, uri.length(), uri.ptr(), start_time, ret);
        break;
      }
    }
  }

  if (OB_SUCCESS == ret)
  { // set status as prepare
    ret = balancer_->start_set_import_status(table_name, table_id, ObLoadDataInfo::PREPARE);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to start set import status as prepare, ret=%d", ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != balancer_)
    {
      int tmp_ret = balancer_->start_set_import_status(table_name, table_id, ObLoadDataInfo::FAILED);
      if (OB_SUCCESS != tmp_ret)
      {
        TBSYS_LOG(WARN, "failed to rollback import, ret=%d", tmp_ret);
      }
    }
    TBSYS_LOG(ERROR, "[import] failed to start import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time, ret);
  }
  else
  {
    TBSYS_LOG(INFO, "[import] succeed to start import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);
  }

  return ret;
}

int ObRootServer2::import(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "receive request: import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld",
      table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant import, table_name=%.*s, new table_id=%lu, uri=%.*s",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
  }
  else if (config_.enable_load_data == false)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->add_load_table(table_name, table_id, uri, start_time)))
  {
    TBSYS_LOG(ERROR, "fail to add import table=%.*s with new table_id=%lu, uri=%.*s, ret=%d",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
  }

  return ret;
}

int ObRootServer2::start_kill_import(const ObString& table_name, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_.master_root_server_ip, (int32_t)config_.master_root_server_port);

  TBSYS_LOG(INFO, "[import] receive request to kill import table: table_name=%.*s table_id=%lu",
      table_name.length(), table_name.ptr(), table_id);

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "this rs is not master of marster cluster, cannot kill import task");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = balancer_->start_set_import_status(table_name, table_id, ObLoadDataInfo::KILLED)))
  {
    TBSYS_LOG(WARN, "failed to kill import, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "[import] succeed to kill import table: table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else
  {
    TBSYS_LOG(ERROR, "[import] failed to kill import table: table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
  }

  return ret;
}

int ObRootServer2::get_import_status(const ObString& table_name,
    const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status)
{
  int ret = OB_SUCCESS;

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant get import status");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->get_import_status(table_name, table_id, status)))
  {
    TBSYS_LOG(ERROR, "[import] fail to get import status table %.*s with table_id=%lu, ret=%d",
        table_name.length(), table_name.ptr(), table_id, ret);
  }

  return ret;
}

int ObRootServer2::set_import_status(const ObString& table_name,
    const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus status)
{
  int ret = OB_SUCCESS;

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant set import status");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->set_import_status(table_name, table_id, status)))
  {
    if (status == ObLoadDataInfo::FAILED || status == ObLoadDataInfo::KILLED)
    {
      TBSYS_LOG(WARN, "fail to set import status table %.*s with table_id=%lu, status=%s, ret=%d",
          table_name.length(), table_name.ptr(), table_id, ObLoadDataInfo::trans_status(status), ret);
    }
    else
    {
      TBSYS_LOG(ERROR, "fail to set import status table %.*s with table_id=%lu, status=%s, ret=%d",
          table_name.length(), table_name.ptr(), table_id, ObLoadDataInfo::trans_status(status), ret);
    }
  }

  return ret;
}

int ObRootServer2::set_bypass_flag(const bool flag)
{
  return state_.set_bypass_flag(flag);
}

bool ObRootServer2::is_loading_data() const
{
  OB_ASSERT(balancer_);
  return balancer_->is_loading_data();
}

//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::trigger_create_table(const uint64_t table_id/* =0 */)
//{
//  int ret = OB_SUCCESS;
//  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
//  ObSchemaManagerV2 * new_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
//  if (NULL == out_schema || NULL == new_schema)
//  {
//    ret = OB_MEM_OVERFLOW;
//    TBSYS_LOG(ERROR, "allocate new schema failed");
//  }
//  if (OB_SUCCESS == ret && 0 == table_id)
//  {
//    ret = get_schema(false, false, *out_schema);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(WARN, "fail to get schema, ret=%d", ret);
//    }
//  }
//  if (OB_SUCCESS == ret)
//  {
//    ret = get_schema(true, false, *new_schema);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
//    }
//  }
//  common::ObArray<uint64_t> new_tables;
//  TableSchema table_schema;
//  if (OB_SUCCESS == ret && 0 == table_id)
//  {
//    //old style, trigger doesn't specify table id to create table
//    ret = get_deleted_tables(*new_schema, *out_schema, new_tables);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(WARN, "fail to get new table for trigger create table. ret=%d", ret);
//    }
//  }
//  else if (OB_SUCCESS == ret && table_id > 0)
//  {
//    //trigger specified table id to create table
//    ret = new_tables.push_back(table_id);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(WARN, "failed to push_back table id into table list, ret=%d", ret);
//    }
//  }
//  if (OB_SUCCESS == ret)
//  {
//    if (0 == new_tables.count())
//    {
//      TBSYS_LOG(WARN, "no table to create, create_table_count=0");
//      ret = OB_ERROR;
//    }
//    else if (new_tables.count() > 1)
//    {
//      TBSYS_LOG(WARN, "not support batch create table! careful!");
//      ret = OB_NOT_SUPPORTED;
//    }
//    else
//    {
//      TBSYS_LOG(INFO, "trigger create table, input_table_id=%lu, create_table_id=%lu",
//          table_id, new_tables.at(0));
//      ret = get_table_schema(new_tables.at(0), table_schema);
//      if (OB_SUCCESS != ret)
//      {
//        TBSYS_LOG(ERROR, "fail to get table schema. table_id=%ld, ret=%d", new_tables.at(0), ret);
//      }
//    }
//  }
//  if (OB_SUCCESS == ret)
//  {
//    ObArray<ObServer> created_cs;
//    ret = create_empty_tablet(table_schema, created_cs);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(ERROR, "fail to create emtyp tablet. table_id=%ld, ret=%d", table_schema.table_id_, ret);
//    }
//  }

//  if (OB_SUCCESS == ret)
//  {
//    int64_t count = 0;
//    ret = renew_user_schema(count);
//    if (OB_SUCCESS != ret)
//    {
//      TBSYS_LOG(WARN, "fail to renew user schema. ret=%d", ret);
//    }
//    else
//    {
//      TBSYS_LOG(INFO, "renew schema after trigger create table success. table_count=%ld", count);
//    }
//  }
//  if (new_schema != NULL)
//  {
//    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, new_schema);
//  }
//  if (out_schema != NULL)
//  {
//    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
//  }
//  return ret;
//}
int ObRootServer2::trigger_create_table(const uint64_t table_id, int64_t schema_version)
{
  int ret = OB_SUCCESS;
  //int64_t old_schema_version = 0;
  bool has_mutator = true;
  bool normal_table = true;

  if (table_id <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "table_id is invalid ,table_id=%ld",table_id);
  }
  else if (schema_version > schema_timestamp_)
  {
    //old_schema_version = schema_timestamp_;
    schema_timestamp_ = schema_version;
    //make schema_service_ has the same schema timestamp with RS
    //used for switch slave to master check time sync
    schema_service_->set_schema_version(schema_timestamp_);//?TODO
  }
  else
  {
    has_mutator = false;
    TBSYS_LOG(WARN, "odl schema_version=%ld, new schema_version=%ld",schema_timestamp_, schema_version);
  }

  if (table_id < OB_APP_MIN_TABLE_ID)
  {
    //system table, it will create after receive boot strap trigger
    normal_table = false;
    has_mutator = false;
  }

  ObSchemaMutator schema_mutator;
  if (OB_SUCCESS == ret && has_mutator)
  {
    int64_t table_count = 0;
    ret = get_schema_mutator(schema_mutator,table_count);
    if (OB_SUCCESS != ret)
    {
      ret = OB_INNER_STAT_ERROR;
      //schema_timestamp_ = old_schema_version;
      TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
    }
  }

  TableSchema table_schema;
  if (OB_SUCCESS == ret && normal_table)
  {
    ret = get_table_schema(table_id, table_schema);
    if (OB_SUCCESS != ret)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "fail to get table schema. table_id=%ld, ret=%d", table_id, ret);
    }

    //mod liumz, [secondary index static_index_build] 20150630:b
    //if (OB_SUCCESS == ret)
    if (OB_SUCCESS == ret && OB_INVALID_ID == table_schema.ih_.tbl_tid)
    //mod:e
    {
      ObArray<ObServer> created_cs;
      ret = create_empty_tablet(table_schema, created_cs);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fail to create emtyp tablet. table_id=%ld, ret=%d", table_schema.table_id_, ret);
      }
    }
  }


  if (OB_SUCCESS == ret && has_mutator)
  {
    if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
    {
      TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify switch schema after create table succ:table[%s]",
          table_schema.table_name_);
    }
  }
  else if (OB_INNER_STAT_ERROR == ret && !has_mutator)
  {
    ret = OB_SUCCESS;
    TBSYS_LOG(WARN, "table maybe already droped, tid= %ld", table_id);
  }

  return ret;
}
//mod:e
//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::trigger_drop_table(const uint64_t table_id)
int ObRootServer2::trigger_drop_table(const uint64_t table_id, int64_t schema_version)
//mod:e
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> delete_table;
  //add zhaoqiong [Schema Manager] 20150327:b
  bool has_mutator = true;
  if (table_id <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "argument is invalid ,table_id=%ld, schema_version=%ld,schema_timestamp_= %ld",table_id, schema_version, schema_timestamp_);
  }
  else if (schema_version <= schema_timestamp_)
  {
    has_mutator = false;
  }
  else
  {
    schema_timestamp_ = schema_version;
  }
  //add:e
  if (OB_SUCCESS == ret)
  {
    delete_table.push_back(table_id);
    ret = delete_tables(false, delete_table);

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to delete tables.ret=%d", ret);
	  //add zhaoqiong [Schema Manager] 20150327:b
      if (!has_mutator)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(WARN, "table maybe not create, tid= %ld", table_id);
      }
    }
    else if (has_mutator)
    {
      TBSYS_LOG(INFO, "delete table for trigger drop table success. table_id=%ld", table_id);
      ObSchemaMutator schema_mutator;
      if (OB_SUCCESS == ret)
      {
        int64_t table_count = 0;
        ret = get_schema_mutator(schema_mutator,table_count);
        if (OB_SUCCESS != ret)
        {
          ret = OB_INNER_STAT_ERROR;
          TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
        {
          TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "notify switch schema after drop table succ:table_id[%ld]",table_id);
        }
      }
	  //add:e
    }
    else
    {
      TBSYS_LOG(INFO, "delete table for trigger drop table success. table_id=%ld", table_id);
    }
  }
  return ret;
}

//add pangtianze [Paxos rs_election] 20150615:b
int ObRootServer2::receive_rs_heartbeat(const common::ObMsgRsHeartbeat hb)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = rs_node_mgr_->handle_rs_heartbeat(hb);
  }
  return ret;
}
int ObRootServer2::get_rs_term(int64_t &term)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    //get current rs term
    term = rs_node_mgr_->get_my_term();
  }
  return ret;
}
ObRootElectionNode::ObRsNodeRole ObRootServer2::get_rs_node_role()
{
  ObRootElectionNode::ObRsNodeRole role = ObRootElectionNode::OB_INIT;
  if (NULL == rs_node_mgr_)
  {
    int err = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL, err = %d", err);
  }
  else
  {
    //get rs election node role
    role = rs_node_mgr_->get_my_role();
  }
  return role;
}
//add:e
//add pangtianze [Paxos rs_election] 20170302:b
bool ObRootServer2::is_in_vote_phase()
{
    bool ret = false;
    if (NULL == rs_node_mgr_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    }
    else if (1 == (ret = rs_node_mgr_->get_election_phase()))
    {
        /// 1 means in vote phase
        ret = true;
    }
    return ret;

}
bool ObRootServer2::is_in_broadcast_phase()
{
    bool ret = false;
    if (NULL == rs_node_mgr_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    }
    else if (2 == (ret = rs_node_mgr_->get_election_phase()))
    {
        /// 2 means in broadcast phase
        ret = true;
    }
    return ret;
}

//add:e
//add pangtianze [Paxos rs_election] 20150619:b
int ObRootServer2::switch_master_to_slave(){
  int ret = OB_SUCCESS;
  rs_node_mgr_->set_as_follower();
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "worker is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = worker_->switch_to_slave()))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "switch to slave err! err = %d", ret);
  }
  else
  {
    //add pangtianze [Paxos rs_election] 20170717:b:e
    //after switch to slave, wait enouth time and then can do electoin
    int64_t now = tbsys::CTimeUtil::getTime();
    rs_node_mgr_->rs_update_lease(now + config_.lease_interval_time);
    TBSYS_LOG(INFO, "set new lease after switch to slave, lease=%ld", now + config_.lease_interval_time);
    //add:e

    char server_version[OB_SERVER_VERSION_LENGTH] = "";
    get_package_and_svn(server_version, sizeof(server_version));
    get_rs_node_mgr()->refresh_inner_table(SERVER_ONLINE, worker_->get_self_rs(), common::ObRoleMgr::SLAVE, server_version);
  }
  return ret;
}
//add:e

//add zhaoqiong [Schema Manager] 20150327:b
int ObRootServer2::trigger_alter_table(const uint64_t table_id, int64_t schema_version)
{
    int ret = OB_SUCCESS;

    int64_t count = 0;
    ObSchemaMutator schema_mutator;
    if (table_id <= 0)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "argument is invalid ,table_id=%ld, schema_version=%ld,schema_timestamp_= %ld",table_id, schema_version, schema_timestamp_);
    }
    else if (schema_version <= schema_timestamp_)
    {
      TBSYS_LOG(WARN, "schema do not need mutator");
    }
    else
    {
      schema_timestamp_ = schema_version;

      if (OB_SUCCESS != (ret = refresh_new_schema(count,schema_mutator)))
      {
        TBSYS_LOG(WARN, "refresh new schema manager after alter table failed. table_id=%ldl, err=%d", table_id, ret);
      }
      else
      {
        if (OB_SUCCESS != (ret = notify_switch_schema(schema_mutator)))
        {
          TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "notify switch schema after alter table, table_id = %ld",table_id);
        }
      }
    }
    return ret;
}
//add:e

int ObRootServer2::check_schema()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ret = renew_user_schema(count);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to renew user schema. ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(schema_manager_rwlock_);
    ret = schema_manager_for_cache_->write_to_file(TMP_SCHEMA_LOCATION);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write schema to file. location = %s, ret=%d", TMP_SCHEMA_LOCATION, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObSchemaManagerV2 *schema = NULL;
    tbsys::CConfig config;
    int64_t now = tbsys::CTimeUtil::getTime();
    if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, now)))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    else if (!schema->parse_from_file(TMP_SCHEMA_LOCATION, config))
    {
      TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
      ret = OB_SCHEMA_ERROR;
    }
    else
    {
      TBSYS_LOG(INFO, "parse schema file success.");
    }
    //add liumz, bugfix: free schema, 20150519:b
    if (schema != NULL)
    {
      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema);
    }
    //add:e
  }
  if (common::FileDirectoryUtils::exists(TMP_SCHEMA_LOCATION))
  {
    //common::FileDirectoryUtils::delete_file(TMP_SCHEMA_LOCATION);
  }
  return ret;
}
//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::force_drop_table(const uint64_t table_id)
int ObRootServer2::force_drop_table(const uint64_t table_id, int64_t schema_version)
//mod:e
{
  int ret = OB_SUCCESS;
  int64_t table_count = 0;
  //mod zhaoqiong [Schema Manager] 20150327:b
  //ret = trigger_drop_table(table_id);
  ret = trigger_drop_table(table_id,schema_version);
  //mod:e
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to force drop tablet. table_id=%ld, ret=%d", table_id, ret);
  }
  else if (OB_SUCCESS != (ret = refresh_new_schema(table_count)))
  {
    TBSYS_LOG(WARN, "fail to refresh new schema.ret=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "force to drop table success. table_id=%ld", table_id);
  }
  return ret;
}
//mod zhaoqiong [Schema Manager] 20150327:b
//int ObRootServer2::force_create_table(const uint64_t table_id)
int ObRootServer2::force_create_table(const uint64_t table_id, int64_t schema_version)
//mod:e
{
  int ret = OB_SUCCESS;
  //mod zhaoqiong [Schema Manager] 20150327:b
  //ret = trigger_create_table(table_id);
  ret = trigger_create_table(table_id,schema_version);
  //mod:e
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to force create tablet. table_id=%ld, ret=%d", table_id, ret);
  }
  return ret;
}

int ObRootServer2::get_ms(ObServer& ms_server)
{
  int ret = OB_SUCCESS;

  ObChunkServerManager::const_iterator it = server_manager_.get_serving_ms();
  if (server_manager_.end() == it)
  {
    TBSYS_LOG(WARN, "no serving ms found, failed to do update_data_source_proxy");
    ret = OB_MS_NOT_EXIST;
  }
  else
  {
    ms_server = it->server_;
    ms_server.set_port(it->port_ms_);
  }
  return ret;
}

int ObRootServer2::delete_tablets(const common::ObTabletReportInfoList& to_delete_list)
{
  int ret = OB_SUCCESS;
  common::ObTabletReportInfoList delete_list;
  common::ObTabletReportInfo * replica = NULL;
  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  bool found = false;
  int64_t replica_count = 0;
  TBSYS_LOG(INFO, "to delete_list size=%ld", to_delete_list.tablet_list_.get_array_index());
  for (int64_t i=0; OB_SUCCESS == ret && i < to_delete_list.tablet_list_.get_array_index(); ++i)
  {
    replica = to_delete_list.tablet_list_.at(i);
    if (NULL == replica)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "check replica is null");
      break;
    }

    const int32_t server_idx = replica->tablet_location_.chunkserver_.get_port();
    if (server_idx == OB_INVALID_INDEX)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "server_idx must not -1, range=%s", to_cstring(replica->tablet_info_.range_));
      break;
    }

    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if (OB_SUCCESS != (ret = root_table_->find_range(replica->tablet_info_.range_, start_it, end_it)))
      {
        TBSYS_LOG(WARN, "failed to find range, range=%s, ret=%d", to_cstring(replica->tablet_info_.range_), ret);
        break;
      }
      else if (start_it != end_it)
      {
        TBSYS_LOG(WARN, "ignore delete unsplited raplica");
      }
      else
      {
        found = false;
        replica_count = 0;
        //add zhaoqiong[roottable tablet management]20150302:b
        //for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
        for (int i = 0; i < OB_MAX_COPY_COUNT; ++i)
        //add e
        {
          if (start_it->server_info_indexes_[i] != OB_INVALID_INDEX)
          {
            ++replica_count;
            if (server_idx == start_it->server_info_indexes_[i])
            {
              found = true;
            }
          }
        }
        if (!found && replica_count > 0)
        {
          if (OB_SUCCESS != (ret = delete_list.add_tablet(*replica)))
          {
            TBSYS_LOG(WARN, "failed to add to delete list, ret=%d", ret);
            break;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "ignore delete tablet range=%s server_idx=%d",
              to_cstring(replica->tablet_info_.range_), server_idx);
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = ObRootUtil::delete_tablets(
            worker_->get_rpc_stub(), server_manager_, delete_list, config_.network_timeout)))
    {
      TBSYS_LOG(WARN, "failed to delete_tablets, ret=%d", ret);
    }
  }

  return ret;
}

//add liumz, [bugfix: cchecksum too large]20161109:b
int ObRootServer2::clean_old_checksum_v2(int64_t current_version)
{
  return worker_->clean_checksum_info(3, current_version);
}
//add:e

//add liuxiao [secondary index] 20150316
/*
 *娓呯悊__all_cchecksum_info绯荤粺琛?
 *灏嗕綆浜庡綋鍓嶇増鏈?3涓増鏈殑column checksumn淇℃伅鍒犻櫎
 */

int ObRootServer2::clean_old_checksum(int64_t current_version)
{
    UNUSED(current_version);
    int ret = OB_SUCCESS;
    if(NULL == schema_service_)
    {
      TBSYS_LOG(ERROR, "can not clean old cchecksum schema is null");
      ret = OB_ERROR;
    }
    //add liuxiao [secondary index] 20150713
    else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
    {
      TBSYS_LOG(WARN, "failed to init schema_service_,ret[%d]", ret);
    }
    //add e
    else if(OB_SUCCESS != (ret = schema_service_->clean_checksum_info(3, current_version)))
    {
      TBSYS_LOG(ERROR, "failed to clean old check sum,ret[%d]", ret);
    }
    return ret;
}

int ObRootServer2::boot_strap_for_create_all_cchecksum_info()
{
    int ret = OB_SUCCESS;
    int64_t table_count = 0;
    ObBootstrap bootstrap(*this);

    if(OB_SUCCESS != (ret = bootstrap.create_systable_all_cchecksum()))
    {
      TBSYS_LOG(ERROR, "create __all_cchecksum_table, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = renew_user_schema(table_count)))
    {
      TBSYS_LOG(ERROR, "failed to refesh user schema, ret=%d", ret);
    }
    else if(this->get_obi_role().get_role() == ObiRole::MASTER)
    {
      if(OB_SUCCESS != (ret = ObRootTriggerUtil::slave_create_all_checksum(root_trigger_)))
      {
        TBSYS_LOG(ERROR, "slave failed to create __all_cchecksum_table, ret=%d", ret);
      }
    }
    return ret;
}


//mod liumz, [paxos static index]20170626:b
//int ObRootServer2::check_column_checksum(const int64_t index_table_id, bool &is_right)
int ObRootServer2::check_column_checksum(const int32_t cluster_id, const int64_t index_table_id, bool &is_right)
//mod:e
{
    //检验column checksum
    int ret = OB_SUCCESS;
    uint64_t org_table_id = OB_INVALID_ID;
    int64_t version = this->get_last_frozen_version();
    ObSchemaManagerV2* schema_mgr = get_local_schema();
    ObTableSchema* table_schema = NULL;

    if(OB_SUCCESS != (ret = get_schema(false, false, *schema_mgr)))
    {
      TBSYS_LOG(WARN, "get schema manager failed.");
    }
    else
    {
      table_schema = schema_mgr->get_table_schema(index_table_id);
      if(NULL != table_schema)
      {
        org_table_id = table_schema->get_index_helper().tbl_tid;//获取原表id
        if(OB_INVALID == org_table_id)
        {
          TBSYS_LOG(ERROR, "ERROR table_id, id=%ld", org_table_id);
          ret = OB_ERROR;
        }
        //add liuxiao [secondary index] 20150713
        else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
        {
          TBSYS_LOG(WARN, "failed to init schema_service_,ret[%d]", ret);
        }
        //add e
        //mod liumz, [paxos static index]20170607:b
        //else if(OB_SUCCESS != ( ret = schema_service_->check_column_checksum(org_table_id,index_table_id, config_.cluster_id, version, is_right)))
        else if(OB_SUCCESS != ( ret = schema_service_->check_column_checksum(org_table_id,index_table_id, cluster_id, version, is_right)))
        //mod:e
        {
          TBSYS_LOG(ERROR, "failed to check col check sum, ret=%d", ret);
          ret = OB_ERROR;
        }
      }
      else
      {
        ret = OB_SCHEMA_ERROR;
        TBSYS_LOG(WARN, "table_schema is null");
      }
    }
    return ret;
}

//add liumz, [paxos static index]20170626:b
//int ObRootServer2::get_cchecksum_info(const ObNewRange new_range,const int64_t required_version,ObString& cchecksum)
int ObRootServer2::get_cchecksum_info(const ObNewRange new_range,const int64_t required_version,const int32_t cluster_id,ObString& cchecksum)
//add:e
{
    int ret = OB_SUCCESS;
    if(NULL == schema_service_)
    {
      TBSYS_LOG(ERROR, "schema_service_ is null");
      ret = OB_ERROR;
    }
    //add liuxiao [secondary index] 20150713
    else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
    {
      TBSYS_LOG(WARN, "failed to init schema_service_,ret[%d]", ret);
    }
    //add e
    //mod pangtianze, liumz [second index for Paxos] 20170626:b
    else if(OB_SUCCESS != ( ret = schema_service_->get_checksum_info(new_range,cluster_id,required_version,cchecksum)))
    //else if(OB_SUCCESS != ( ret = schema_service_->get_checksum_info(new_range,OB_INVALID_ID,required_version,cchecksum)))
    //mod:e
    {
        TBSYS_LOG(WARN, "failed to get old check sum for cs merge, tablet[%s], ret=%d", to_cstring(new_range), ret);
    }
    return ret;
}

//add wenghaixing [secondary index.cluster]20150629
int ObRootServer2::fetch_index_stat(const int64_t &index_tid, const int64_t &cluster_id, int64_t &status)
{
  int ret = OB_SUCCESS;
  uint64_t tid = (uint64_t)index_tid;
  //IndexStatus stat;
  if(NULL == schema_service_)
  {
    TBSYS_LOG(ERROR, "schema_service_ is null");
    ret = OB_ERROR;
  }
  //add liuxiao [secondary index] 20150713
  else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
  {
    TBSYS_LOG(WARN, "failed to init schema_service_,ret[%d]", ret);
  }
  //add e
  else if(OB_SUCCESS != (ret = schema_service_->fetch_index_stat(tid, cluster_id, status)))
  {
    TBSYS_LOG(WARN, "get index stat failed,ret[%d]", ret);
  }
  return ret;
}
//add e

//add liumz, [paxos static index]20170626:b
int ObRootServer2::get_index_stat(const uint64_t &index_tid, const int64_t &cluster_num, IndexStatus &status)
{
  int ret = OB_SUCCESS;
  if(NULL == schema_service_)
  {
    TBSYS_LOG(ERROR, "schema_service_ is null");
    ret = OB_ERROR;
  }
  //add liuxiao [secondary index] 20150713
  else if (OB_SUCCESS != (ret = schema_service_->init(schema_service_scan_helper_, false)))
  {
    TBSYS_LOG(WARN, "failed to init schema_service_,ret[%d]", ret);
  }
  //add e
  else if(OB_SUCCESS != (ret = schema_service_->get_index_stat(index_tid, cluster_num, status)))
  {
    TBSYS_LOG(WARN, "get index stat failed,ret[%d]", ret);
  }
  return ret;
}
//add:e

//add liuxiao [secondary index migrate index tablet] 0150410
//add liuxiao [secondary index 20150410] 20150410
bool ObRootServer2::check_create_index_over()
{
  //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
  //return worker_->check_create_index_over();
  if(NULL == worker_)
  {
    return false;
  }
  else
  {
    return worker_->check_create_index_over();
  }
  //modify e
}
//add e
//add e

//add liumz, [secondary index version management]20160413:b
int64_t ObRootServer2::get_build_index_version()
{
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "root worker is NULL!");
    return OB_INVALID_VERSION;
  }
  else
  {
    return worker_->get_build_index_version();
  }
}
bool ObRootServer2::is_last_finished()
{
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "root worker is NULL!");
    return false;
  }
  else
  {
    return worker_->is_last_finished();
  }
}
//add:e

//add wenghaixing [secondary index static_index_build]20150528
IndexBeat ObRootServer2::get_index_beat()
{
  return index_beat_;
}

void ObRootServer2::reset_index_beat()
{
  index_beat_.reset();
}

void ObRootServer2::set_index_beat(uint64_t index_tid, IndexStatus status, int64_t hist_width)
{
  index_beat_.idx_tid = index_tid;
  index_beat_.status = status;
  index_beat_.hist_width = hist_width;
}
//add e
//add pangtianze [Paxos rs_election] 20161010:b
int ObRootServer2::receive_change_paxos_num_request(const common::ObMsgRsChangePaxosNum change_paxos_num)
{
    int ret = OB_SUCCESS;
    if (NULL == rs_node_mgr_)
    {
      TBSYS_LOG(WARN, "rs_node_mgr is NULL");
      ret = OB_NOT_INIT;
    }
    else if (OB_SUCCESS != (ret = rs_node_mgr_->handle_change_config_request(change_paxos_num.addr_, change_paxos_num.term_, change_paxos_num.new_paxos_num_)))
    {
      TBSYS_LOG(WARN, "handle change config request failed, err=%d", ret);
    }
    return ret;
}
//add:e
//add pangtianze [Paxos rs_election] 20161010:b
int ObRootServer2::receive_new_quorum_scale_request(const ObMsgRsNewQuorumScale new_quorum_scale)
{
  int ret = OB_SUCCESS;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  if(new_quorum_scale.term_ >= rs_node_mgr_->get_my_term())
  {
    rs_node_mgr_->set_my_term(new_quorum_scale.term_);
    set_quorum_scale(new_quorum_scale.new_ups_quorum_scale_);
    rs_node_mgr_->set_my_quorum_scale(new_quorum_scale.new_ups_quorum_scale_);
    TBSYS_LOG(INFO, "Rootserver save new quorum scale, new_quorum_scale = %d, ups_quorum_scale = %ld", new_quorum_scale.new_ups_quorum_scale_, (int64_t)config_.ups_quorum_scale);
  }
  else
  {
    ret = OB_INVALID_TERM;
    TBSYS_LOG(WARN, "new_quorum_scale msg is invalid! ignored the msg.");
  }
  return ret;
}
bool ObRootServer2::check_all_rs_registered()
{
  int ret = false;
  if (NULL == rs_node_mgr_)
  {
    TBSYS_LOG(ERROR, "rs_node_mgr is NULL");
  }
  else
  {
    ret = rs_node_mgr_->check_all_rs_alive();
  }
  return ret;
}
//add:e

//add bingo [Paxos Cluster.Balance] 20161118:b
void ObRootServer2::get_alive_cluster_with_cs(bool *is_cluster_alive_with_cs)
{
   memset(is_cluster_alive_with_cs, false, OB_CLUSTER_ARRAY_LEN * sizeof(bool));
   server_manager_.is_cluster_alive_with_cs(is_cluster_alive_with_cs);
}
//add:e

//add liumz, [paxos static index]20170626:b
void ObRootServer2::get_alive_cluster_with_cs(bool *is_cluster_alive_with_cs) const
{
   memset(is_cluster_alive_with_cs, false, OB_CLUSTER_ARRAY_LEN * sizeof(bool));
   server_manager_.is_cluster_alive_with_cs(is_cluster_alive_with_cs);
}
int ObRootServer2::get_alive_cluster_num_with_cs()
{
  int ret = 0;
  bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
  get_alive_cluster_with_cs(is_cluster_alive);
  for (int i = 1; i < OB_CLUSTER_ARRAY_LEN; i++)
  {
    if (is_cluster_alive[i])
      ret++;
  }
  return ret;
}
//add:e

//add bingo [Paxos Cluster.Balance] 20170407:b
void ObRootServer2::get_alive_cluster_with_ms_and_cs(bool *is_cluster_alive_with_ms_cs)
{
  memset(is_cluster_alive_with_ms_cs, false, OB_CLUSTER_ARRAY_LEN * sizeof(bool));
  server_manager_.is_cluster_alive_with_ms_and_cs(is_cluster_alive_with_ms_cs);
}
//add:e

//add lbzhong [Paxos Cluster.Balance] 20160708:b
void ObRootServer2::get_alive_cluster(bool *is_cluster_alive)
{
  memset(is_cluster_alive, false, OB_CLUSTER_ARRAY_LEN * sizeof(bool));
  rs_node_mgr_->is_cluster_alive_with_rs(is_cluster_alive);
  ups_manager_->is_cluster_alive_with_ups(is_cluster_alive);
  server_manager_.is_cluster_alive_with_ms_cs(is_cluster_alive);
}

bool ObRootServer2::is_cluster_alive(const int32_t cluster_id)
{
  bool is_alive = false;
  if(rs_node_mgr_->is_cluster_alive_with_rs(cluster_id))
  {
    is_alive = true;
  }
  else if(ups_manager_->is_cluster_alive_with_ups(cluster_id))
  {
    is_alive = true;
  }
  else if(server_manager_.is_cluster_alive_with_ms_cs(cluster_id))
  {
    is_alive = true;
  }
  return is_alive;
}

bool ObRootServer2::is_cluster_alive_with_cs(const int32_t cluster_id)
{
  return server_manager_.is_cluster_alive_with_cs(cluster_id);
}

int ObRootServer2::set_master_cluster_id(const int32_t cluster_id)
{
  tbsys::CWLockGuard guard(cluster_mgr_rwlock_);
  return cluster_mgr_.set_master_cluster_id(cluster_id);
}

int32_t ObRootServer2::get_master_cluster_id() const
{
  tbsys::CRLockGuard guard(cluster_mgr_rwlock_);
  return cluster_mgr_.get_master_cluster_id();
}

int ObRootServer2::get_cluster_tablet_replicas_num(int32_t *replicas_num) const
{
  tbsys::CRLockGuard guard(cluster_mgr_rwlock_);
  return cluster_mgr_.get_cluster_tablet_replicas_num(replicas_num);
}

int ObRootServer2::load_cluster_replicas_num()
{
  int ret = OB_SUCCESS;

  //add pangtianze [Paxos Cluster.Balance] 20170620:b
  const char *cluster_replica_num_param = config_.cluster_replica_num;
  int32_t replicas_num[OB_CLUSTER_ARRAY_LEN] = {0};
  ret = cluster_mgr_.parse_config_value(cluster_replica_num_param, replicas_num);
  if (OB_SUCCESS == ret)
  {
    ret = cluster_mgr_.check_total_replica_num(replicas_num);
  }
  //add:e

  for(int32_t cluster_id = 1; OB_SUCCESS == ret && cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
  {
    TBSYS_LOG(INFO, "cluster_id=%d, replica_num=%d", cluster_id, replicas_num[cluster_id]);
  }

  if(OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(cluster_mgr_rwlock_);
    if(OB_SUCCESS != (ret = cluster_mgr_.set_replicas_num(replicas_num)))
    {
      TBSYS_LOG(WARN, "fail to set replicas num, ret=%d", ret);
    }
  }
  return ret;
}

void ObRootServer2::dump_balancer_info(const int64_t table_id)
{
  //add bingo [Paxos Cluster.Balance bug fix] 20160923
  OB_ASSERT(balancer_);
  //add:e
  TBSYS_LOG(INFO, "begin to dump balancer info, table_id=%ld", table_id);
  balancer_->print_balancer_info(table_id);
}

//add bingo [Paxos Cluster.Balance] 20161024:b
void ObRootServer2::print_master_cluster_id(char *buf, const int64_t buf_len, int64_t& pos)
{
  if(get_rs_role()== ObRoleMgr::MASTER)
  {
    int32_t master_cluster_id = get_master_cluster_id();
    databuff_printf(buf, buf_len, pos, "master_cluster_id=%d", master_cluster_id);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "target rootserver is not MASTER, please check the command!");
  }
}
//add:e

//add bingo [Paxos set_boot_ok] 20170315:b
int ObRootServer2::set_boot_ok()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "bootstrap set boot ok, according do rs_admin");
  boot_state_.set_boot_ok();
  return ret;
}
//add:e
