
#ifndef OCEANBASE_PACKET_H_
#define OCEANBASE_PACKET_H_

#include "easy_io_struct.h"
#include "ob_record_header.h" // for ObRecordHeader
#include "data_buffer.h"
//#include "ob_malloc.h"
#include "ob_memory_pool.h"
#include "thread_buffer.h"
#include "ob_trace_id.h"

namespace oceanbase
{
  namespace common
  {
    enum PacketCode
    {
      OB_GET_REQUEST = 101,
      OB_GET_RESPONSE = 102,
      OB_BEGIN_MERGE = 103,
      OB_LOAD_NEW_TABLETS = 104,
      OB_DROP_MEM_TABLE = 107,
      OB_PREPARE_SCHEMA = 108,
      OB_HEARTBEAT = 110,
      // 0.4 version
      OB_MERGE_SERVER_HEARTBEAT = 111,
      OB_TRANSFER_TABLETS = 113,
      OB_RESULT = 114,
      OB_BATCH_GET_REQUEST = 115,
      OB_BATCH_GET_RESPONSE = 116,
      OB_HEARTBEAT_RESPONSE = 119,

      OB_SCAN_REQUEST = 122,
      OB_SCAN_RESPONSE = 123,
      OB_CREATE_MEMTABLE_INDEX = 124,
      OB_CS_IMPORT_TABLETS = 131,
      OB_CS_IMPORT_TABLETS_RESPONSE = 132,
      OB_GET_ROW_CHECKSUM = 133,
      OB_GET_ROW_CHECKSUM_RESPONSE = 134,

      OB_PING_REQUEST = 151,
      OB_PING_RESPONSE = 152,
      OB_SET_SYNC_LIMIT_REQUEST = 153,
      OB_SET_SYNC_LIMIT_RESPONSE = 154,
      OB_RENEW_LEASE_REQUEST = 155,
      OB_RENEW_LEASE_RESPONSE = 156,
      OB_GRANT_LEASE_REQUEST = 157,
      OB_GRANT_LEASE_RESPONSE = 158,
      OB_CLEAR_REQUEST = 161,
      OB_CLEAR_RESPONSE = 162,

      OB_GET_OBI_ROLE = 163,
      OB_GET_OBI_ROLE_RESPONSE = 164,
      OB_SET_OBI_ROLE = 165,
      OB_SET_OBI_ROLE_RESPONSE = 166,
      OB_RS_ADMIN = 167,
      OB_RS_ADMIN_RESPONSE = 168,
      OB_CHANGE_LOG_LEVEL = 169,
      OB_CHANGE_LOG_LEVEL_RESPONSE = 170,
      OB_RS_STAT = 171,
      OB_RS_STAT_RESPONSE = 172,
      OB_GET_OBI_CONFIG = 173,
      OB_GET_OBI_CONFIG_RESPONSE = 174,
      OB_SET_OBI_CONFIG = 175,
      OB_SET_OBI_CONFIG_RESPONSE = 176,
      OB_SET_UPS_CONFIG = 177,
      OB_SET_UPS_CONFIG_RESPONSE = 178,
      OB_GET_CS_LIST = 179,
      OB_GET_CS_LIST_RESPONSE = 180,
      OB_GET_MS_LIST = 181,
      OB_GET_MS_LIST_RESPONSE = 182,
      OB_GET_CLOG_CURSOR = 183,
      OB_GET_CLOG_CURSOR_RESPONSE = 184,
      OB_GET_CLOG_MASTER = 185,
      OB_GET_CLOG_MASTER_RESPONSE = 186,


      OB_RS_UPS_REVOKE_LEASE = 187, // see ObMsgRevokeLease
      OB_RS_UPS_REVOKE_LEASE_RESPONSE = 188, // ObResultCode
      OB_RS_UPS_REGISTER = 189, // see ObMsgUpsRegister
      OB_RS_UPS_REGISTER_RESPONSE = 190, // see ObMsgUpsRegisterResp
      OB_RS_UPS_SLAVE_FAILURE = 191, // see ObMsgUpsSlaveFailure
      OB_RS_UPS_SLAVE_FAILURE_RESPONSE = 192, // ObResultCode
      OB_RS_UPS_HEARTBEAT = 193, // see ObMsgUpsHeartbeat
      OB_RS_UPS_HEARTBEAT_RESPONSE = 194, // see ObMsgUpsHeartbeatResp
      OB_CHANGE_UPS_MASTER = 195,
      OB_CHANGE_UPS_MASTER_RESPONSE = 196,
      OB_RS_SHUTDOWN_SERVERS = 197,
      OB_RS_SHUTDOWN_SERVERS_RESPONSE = 198, // ObResultCode
      OB_RS_GET_MAX_LOG_SEQ = 199,
      OB_RS_GET_MAX_LOG_SEQ_RESPONSE = 200,

      OB_START_MERGE = 201,
      OB_START_MERGE_RESPONSE = 202,
      OB_DROP_OLD_TABLETS = 203,
      OB_DROP_OLD_TABLETS_RESPONSE = 204,
      OB_FETCH_SCHEMA = 205,
      OB_FETCH_SCHEMA_RESPONSE = 206,
      OB_REPORT_TABLETS = 207,
      OB_REPORT_TABLETS_RESPONSE = 208,
      OB_WAITING_JOB_DONE = 209,
      OB_WAITING_JOB_DONE_RESPONSE = 210,
      OB_GET_UPDATE_SERVER_INFO = 211,
      OB_GET_UPDATE_SERVER_INFO_RES = 212,
      OB_SERVER_REGISTER = 213,
      OB_SERVER_REGISTER_RESPONSE = 214,
      OB_MIGRATE_OVER = 215,
      OB_MIGRATE_OVER_RESPONSE = 216,
      OB_CS_MIGRATE = 217,
      OB_CS_MIGRATE_RESPONSE = 218,
      OB_CS_CREATE_TABLE = 219,
      OB_CS_CREATE_TABLE_RESPONSE = 220,
      OB_REPORT_CAPACITY_INFO = 221,
      OB_REPORT_CAPACITY_INFO_RESPONSE = 222,
      OB_FETCH_SCHEMA_VERSION = 223,
      OB_FETCH_SCHEMA_VERSION_RESPONSE = 224,
      OB_FREEZE_MEM_TABLE = 225,
      OB_FREEZE_MEM_TABLE_RESPONSE = 226,
      OB_REQUIRE_HEARTBEAT = 227,
      OB_DUMP_CS_INFO = 229,
      OB_DUMP_CS_INFO_RESPONSE = 230,
      OB_CS_FETCH_SSTABLE_DIST = 231,
      OB_CS_FETCH_SSTABLE_DIST_RESPONSE  = 232,
      OB_CS_LOAD_BYPASS_SSTABLE = 233,
      OB_CS_LOAD_BYPASS_SSTABLE_RESPONSE = 234,
      OB_CS_LOAD_BYPASS_SSTABLE_DONE = 235,
      OB_CS_LOAD_BYPASS_SSTABLE_DONE_RESPONSE = 236,
      OB_CS_DELETE_TABLE = 237,
      OB_CS_DELETE_TABLE_RESPONSE = 238,
      OB_CS_DELETE_TABLE_DONE = 239,
      OB_CS_DELETE_TABLE_DONE_RESPONSE = 240,
      OB_CS_FETCH_BLOOM_FILTER = 241,
      OB_CS_FETCH_BLOOM_FILTER_RESPONSE = 242,
      OB_CS_FETCH_DATA = 243,
      OB_CS_FETCH_DATA_RESPONSE = 244,
      OB_CS_UNINSTALL_DISK = 245,
      OB_CS_UNINSTALL_DISK_RESPONSE = 246,
      OB_CS_SHOW_DISK = 247,
      OB_CS_SHOW_DISK_RESPONSE = 248,
      OB_CS_INSTALL_DISK = 249,
      OB_CS_INSTALL_DISK_RESPONSE = 250,
      //add zhaoqiong [Schema Manager] 20150327:b
      OB_FETCH_SCHEMA_NEXT = 251,
      OB_FETCH_SCHEMA_NEXT_RESPONSE = 252,
      //add:e
      //add zhaoqiong [Truncate Table]:20160318:b
      OB_CHECK_INCREMENTAL_RANGE = 253,
      OB_CHECK_INCREMENTAL_RANGE_RESPONSE = 254,
      //add:e
      OB_CS_GET_MIGRATE_DEST_LOC = 260,
      OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE = 261,
      OB_CS_DUMP_TABLET_IMAGE = 262,
      OB_CS_DUMP_TABLET_IMAGE_RESPONSE = 263,
      OB_CS_START_GC = 264,
      OB_SWITCH_SCHEMA = 265,
      OB_SWITCH_SCHEMA_RESPONSE = 266,
      //add zhaoqiong [Schema Manager] 20150327:b
      OB_SWITCH_SCHEMA_MUTATOR = 340,
      OB_SWITCH_SCHEMA_MUTATOR_RESPONSE = 341,
      OB_SWITCH_TMP_SCHEMA = 342,
      OB_SWITCH_TMP_SCHEMA_RESPONSE = 343,
      //add:e
      //add pangtianze [Paxos rs_election] 20150731:b
      OB_SET_ELECTION_PRIORIYT = 346,
      OB_SET_ELECTION_PRIORIYT_RESPONSE = 347,
      OB_SET_SPECIFIED_RS_PRIORIYT = 348,
      OB_SET_SPECIFIED_RS_PRIORIYT_RESPONSE = 349,
      //add:e
      //add pangtianze [Paxos rs_election] 20150806:b
      OB_RS_ADMIN_SET_LEADER = 350,
      OB_RS_ADMIN_SET_LEADER_RESPONSE = 351,
      OB_RS_SET_LEADER = 352,
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607020:b
      OB_RS_DUMP_BALANCER_INFO = 353,
      OB_RS_DUMP_BALANCER_INFO_RESPONSE = 354,
      OB_GET_REPLICA_NUM = 357,
      OB_GET_REPLICA_NUM_RESPONSE = 358,
      //add:e
      //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      OB_GET_CLUSTER_UPS = 359,
      OB_GET_CLUSTER_UPS_RESPONSE = 360,
      //add:e
      //add pangtianze [Paxos rs_election] 20160919:b
      OB_RS_GET_UPS_ROLE = 361,
      OB_RS_GET_UPS_ROLE_RESPONSE = 362,
      //add:e
      //add bingo [Paxos rs_election] 20161009
      OB_MASTER_GET_ALL_PRIORITY = 363,
      OB_MASTER_GET_ALL_PRIORITY_RESPONSE = 364,
      OB_GET_RS_PRIORITY = 365,
      OB_GET_RS_PRIORITY_RESPONSE = 366,
      //add:e
	  //add bingo [Paxos Cluster.Balance] 20161020:b
      OB_SET_MASTER_CLUSTER_ID = 367,
      OB_SET_MASTER_CLUSTER_ID_RESPONSE = 368,
      //add:e
      //add bingo [Paxos rs management] 20170301:b
      OB_GET_RS_LEADER = 369,
      OB_GET_RS_LEADER_RESPONSE = 370,
      //add:e
	  //add pangtianze [Paxos rs_election] 20170228:b
      OB_REFRESH_RS_LIST = 371,
      OB_REFRESH_RS_LIST_REGPONSE = 372,
      //add:e
      //add bingo [Paxos rs management] 20170614:b
      OB_DUMP_SSTABLE_INFO = 373,
      OB_DUMP_SSTABLE_INFO_RESPONSE = 374,
      //add:e
      //add bingo [Paxos table replica] 20170620:b
      OB_GET_TABLE_REPLICA_NUM = 375,
      OB_GET_TABLE_REPLICA_NUM_RESPONSE = 375,
      //add:e

      OB_UPDATE_SERVER_REPORT_FREEZE = 267,
      OB_GET_UPDATE_SERVER_INFO_FOR_MERGE = 268,
      OB_GET_MERGE_DELAY_INTERVAL = 269,
      OB_GET_MERGE_DELAY_INTERVAL_RES = 270,
      OB_GET_UPS = 271,
      OB_GET_UPS_RESPONSE = 272, // @see ObUpsList in ob_ups_info.h
      OB_GET_CLIENT_CONFIG = 273,
      OB_GET_CLIENT_CONFIG_RESPONSE = 274, // @see ObClientConfig is ob_client_config.h
      OB_CS_DELETE_TABLETS = 275,             // @see ObTabletInfoList
      OB_CS_DELETE_TABLETS_RESPONSE = 276,
      OB_STOP_SERVER = 277,
      OB_STOP_SERVER_RESPONSE = 278,
      OB_RS_RESTART_SERVERS = 279,
      OB_RS_RESTART_SERVERS_RESPONSE = 280,
      OB_CS_SHOW_PARAM = 281,
      OB_RS_REQUEST_REPORT_TABLET = 282,
      OB_RS_DUMP_CS_TABLET_INFO = 283,
      OB_RS_DUMP_CS_TABLET_INFO_RESPONSE = 284,
      OB_GET_PROXY_LIST = 285,
      OB_GET_PROXY_LIST_RESPONSE = 286,
      OB_RS_FORCE_CS_REPORT = 287,
      OB_RS_FORCE_CS_REPORT_RESPONSE = 288,
      OB_RS_INNER_MSG_DELETE_TABLET = 289,
      OB_RS_SPLIT_TABLET = 290,
      OB_RS_SPLIT_TABLET_RESPONSE = 291,
      OB_RS_FETCH_SPLIT_RANGE = 292,
      OB_RS_FETCH_SPLIT_RANGE_RESPONSE = 293,
      OB_CS_CHECK_TABLET = 294,
      OB_CS_CHECK_TABLET_RESPONSE = 295,
      OB_CS_MERGE_TABLETS = 296,             // @see ObTabletInfoList
      OB_CS_MERGE_TABLETS_RESPONSE = 297,
      OB_CS_MERGE_TABLETS_DONE = 298,
      OB_CS_MERGE_TABLETS_DONE_RESPONSE = 299,
      OB_CS_SYNC_ALL_IMAGES = 300,

      OB_WRITE = 301,
      OB_WRITE_RES = 302,
      OB_SEND_LOG = 303,
      OB_SEND_LOG_RES = 304,
      OB_SYNC_SCHEMA = 305,
      OB_SYNC_SCHEMA_RES = 306,
      OB_SLAVE_REG = 307,
      OB_SLAVE_REG_RES = 308,
      OB_GET_MASTER = 309,
      OB_GET_MASTER_RES = 310,
      OB_SLAVE_QUIT = 311,
      OB_SLAVE_QUIT_RES = 312,
      OB_LSYNC_FETCH_LOG = 313,
      OB_LSYNC_FETCH_LOG_RES = 314,
      OB_LOGIN = 315,
      OB_LOGIN_RES = 316,

      //add zhaoqiong [fixed for Backup]:20150811
      OB_BACKUP_REG = 317,
      OB_BACKUP_REG_RES = 318,
      //add:e
      // ms mutate packet
      OB_MS_MUTATE = 320,
      OB_MS_MUTATE_RESPONSE = 321,
      OB_INTERNAL_WRITE = 322,
      OB_INTERNAL_WRITE_RESPONSE = 323,
      OB_FAKE_WRITE_FOR_KEEP_ALIVE = 324,
      OB_SET_MASTER_UPS_CONFIG = 325,
      OB_SET_MASTER_UPS_CONFIG_RESPONSE = 326,
      OB_GET_MASTER_UPS_CONFIG = 327,
      OB_GET_MASTER_UPS_CONFIG_RESPONSE = 328,

      /* 0.4 ms register */
      OB_MERGE_SERVER_REGISTER = 329,
      OB_MERGE_SERVER_REGISTER_RESPONSE = 330,

      OB_FETCH_STATS = 401,
      OB_FETCH_STATS_RESPONSE = 402,

      /// OB_GET_CLOG_CURSOR = 1183,
      /// OB_GET_CLOG_CURSOR_RESPONSE = 1184,
      /// OB_GET_CLOG_MASTER = 1185,
      /// OB_GET_CLOG_MASTER_RESPONSE = 1186,

      OB_GET_LOG_SYNC_DELAY_STAT = 403,
      OB_GET_LOG_SYNC_DELAY_STAT_RESPONSE = 404,

      OB_SQL_SCAN_REQUEST = 405,
      OB_SQL_SCAN_RESPONSE = 406,
      //OB_UPDATE_SERVER_REPORT_PRIVILEGE_VERSION = 407,
      //OB_UPDATE_SERVER_REPORT_PRIVILEGE_VERSION_RESPONSE = 408,

      OB_SQL_GET_REQUEST = 409,
      OB_SQL_GET_RESPONSE = 410,

      OB_NOP_PKT = 415,
      OB_MALLOC_STRESS = 416,
      OB_MALLOC_STRESS_RESPONSE = 417,
      OB_COMMIT_END = 418,
      OB_DIRECT_PING = 419,
      OB_DIRECT_PING_RESPONSE = 420,
      OB_FETCH_LOG = 420,
      OB_FETCH_LOG_RESPONSE = 421,
      OB_PREFETCH_LOG = 422,
      OB_FILL_LOG_CURSOR = 423,
      OB_FILL_LOG_CURSOR_RESPONSE = 424,
      OB_GET_CLOG_STATUS = 430,
      OB_GET_CLOG_STATUS_RESPONSE = 431,

      OB_NEW_GET_REQUEST = 432,
      OB_NEW_GET_RESPONSE = 433,
      OB_NEW_SCAN_REQUEST = 434,
      OB_NEW_SCAN_RESPONSE = 435,

      /* get master ob instance rs packet code */
      OB_GET_MASTER_OBI_RS = 466,
      OB_GET_MASTER_OBI_RS_RESPONSE = 467,

      OB_SET_CONFIG = 470,
      OB_SET_CONFIG_RESPONSE = 471,
      OB_GET_CONFIG = 472,
      OB_GET_CONFIG_RESPONSE = 473,

      // schema service
      OB_CREATE_TABLE = 500,
      OB_CREATE_TABLE_RESPONSE = 501,
      OB_DROP_TABLE = 502,
      OB_DROP_TABLE_RESPONSE = 503,
      OB_ALTER_TABLE = 504,
      OB_ALTER_TABLE_RESPONSE = 505,
      //add zhaoqiong [Truncate Table]:20160318:b
      OB_TRUNCATE_TABLE = 506,
      OB_TRUNCATE_TABLE_RESPONSE = 507,
      //add:e

      // trigger event
      OB_HANDLE_TRIGGER_EVENT = 600,
      OB_HANDLE_TRIGGER_EVENT_RESPONSE = 601,
      //add zhaoqiong [Schema Manager] 20150327:b
      OB_HANDLE_DDL_TRIGGER_EVENT = 602,
      OB_HANDLE_DDL_TRIGGER_EVENT_RESPONSE = 603,
      //add:e
	  //add pangtianze [Paxos ups_replication] 20150603
      OB_RS_GET_MAX_LOG_SEQ_AND_TERM = 604,
      OB_RS_GET_MAX_LOG_SEQ_AND_TERM_RESPONSE = 605,
      //add:e
      //add chujiajia [Paxos rs_election] 20151015:b
      OB_RS_RS_SHARE_INFO = 606,
      OB_RS_RS_SHARE_INFO_RESPONSE = 607,
      //add:e
      //add pangtianze [Paxos rs_election] 20150603
      OB_RS_RS_HEARTBEAT = 608,
      OB_RS_RS_HEARTBEAT_RESPONSE =609,
      OB_RS_VOTE_REQUEST = 610,
      OB_RS_VOTE_REQUEST_RESPONSE = 611,
      OB_RS_LEADER_BROADCAST = 612,
      OB_RS_LEADER_BROADCAST_RESPONSE = 613,
      OB_RS_SLAVE_INIT_FIRST_META = 614,
      OB_RS_SLAVE_INIT_FIRST_META_RESPONSE = 615,
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      OB_CHANGE_RS_PAXOS_NUMBER = 616,
      OB_CHANGE_RS_PAXOS_NUMBER_RESPONSE = 617,
      //add:e
      //add pangtianze [Paxos rs_election] 20150806
      OB_RS_EXCUTE_VOTE = 618,
      OB_RS_EXCUTE_VOTE_RESPONSE = 619,
      OB_RS_EXCUTE_BROADCAST = 620,
      OB_RS_EXCUTE_BROADCAST_RESPONSE = 621,
      //add:e
      //add pangtianze [Paxos rs_election] 20150814:b
      OB_RS_LEADER_BROADCAST_TASK =622,
      OB_RS_VOTE_REQUEST_TASK = 623,
      //add:e
      //add chujiajia [Paxos rs_election] 20151030:b
      OB_RS_CHANGE_PAXOS_NUM_REQUEST = 624,
      OB_RS_CHANGE_PAXOS_NUM_RESPONSE = 625,
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
      OB_CHANGE_UPS_QUORUM_SCALE = 632,
      OB_CHANGE_UPS_QUORUM_SCALE_RESPONSE = 633,
      OB_RS_CHANGE_UPS_QUORUM_SCALE_REQUEST = 634,
      OB_RS_CHANGE_UPS_QUORUM_SCALE_RESPONSE = 635,
      //add:e
      //add pangtianze [Paxos rs_switch] 20170208:b
      OB_FORCE_SERVER_REGIST = 638,
      //add:e
      //add bingo [Paxos Cluster.Flow.UPS] 20170405:b
      OB_SYNC_IS_STRONG_CONSISTENT = 639,
      OB_SYNC_IS_STRONG_CONSISTENT_RESPONSE = 640,
      //add:e

      //add zhaoqiong [fixed for Backup]:20150811
      //backup service
      OB_BACKUP_DATABASE = 700,
      OB_BACKUP_DATABASE_RESPONSE = 701,
      OB_BACKUP_TABLE = 702,
      OB_BACKUP_TABLE_RESPONSE = 703,
      OB_BACKUP_ABORT = 704,
      OB_BACKUP_ABORT_RESPONSE = 705,
      OB_CHECK_BACKUP_PROCESS = 706,
      OB_CHECK_BACKUP_PROCESS_RESPONSE = 707,
      OB_CHECK_BACKUP_PRIVILEGE = 708,
      OB_CHECK_BACKUP_PRIVILEGE_RESPONSE = 709,
      //add:e
	  
      //ups
      OB_UPS_DUMP_TEXT_MEMTABLE = 1227,
      OB_UPS_DUMP_TEXT_MEMTABLE_RESPONSE = 1228,
      OB_UPS_DUMP_TEXT_SCHEMAS = 1229,
      OB_UPS_DUMP_TEXT_SCHEMAS_RESPONSE = 1230,
      OB_UPS_FORCE_FETCH_SCHEMA = 1231,
      OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE = 1232,
      OB_UPS_RELOAD_CONF = 1233,
      OB_UPS_RELOAD_CONF_RESPONSE = 1234,
      OB_UPS_MEMORY_WATCH = 1235,
      OB_UPS_MEMORY_WATCH_RESPONSE = 1236,
      OB_UPS_MEMORY_LIMIT_SET= 1237,
      OB_UPS_MEMORY_LIMIT_SET_RESPONSE = 1238,
      OB_UPS_CLEAR_ACTIVE_MEMTABLE = 1239,
      OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE = 1240,
      OB_UPS_GET_LAST_FROZEN_VERSION = 1241,
      OB_UPS_GET_LAST_FROZEN_VERSION_RESPONSE = 1242,
      OB_UPS_CHANGE_VIP_REQUEST = 1243,
      OB_UPS_CHANGE_VIP_RESPONSE = 1244,
      OB_UPS_GET_BLOOM_FILTER = 1245,
      OB_UPS_GET_BLOOM_FILTER_RESPONSE = 1246,
      OB_UPS_PRIV_QUEUE_CONF_SET = 1247,
      OB_UPS_PRIV_QUEUE_CONF_SET_RESPONSE = 1248,
      OB_UPS_STORE_MEM_TABLE = 1249,
      OB_UPS_STORE_MEM_TABLE_RESPONSE = 1250,
      OB_UPS_DROP_MEM_TABLE = 1251,
      OB_UPS_DROP_MEM_TABLE_RESPONSE = 1252,
      OB_UPS_ERASE_SSTABLE = 1253,
      OB_UPS_ERASE_SSTABLE_RESPONSE = 1254,
      OB_UPS_ASYNC_HANDLE_FROZEN = 1255,
      OB_UPS_ASYNC_REPORT_FREEZE = 1256,
      OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE = 1257,
      OB_UPS_MINOR_FREEZE_MEMTABLE = 1258,
      OB_UPS_MINOR_FREEZE_MEMTABLE_RESPONSE = 1259,
      OB_UPS_LOAD_NEW_STORE = 1260,
      OB_UPS_LOAD_NEW_STORE_RESPONSE = 1261,
      OB_UPS_RELOAD_ALL_STORE = 1262,
      OB_UPS_RELOAD_ALL_STORE_RESPONSE = 1263,
      OB_UPS_RELOAD_STORE = 1264,
      OB_UPS_RELOAD_STORE_RESPONSE = 1265,
      OB_UPS_UMOUNT_STORE = 1266,
      OB_UPS_UMOUNT_STORE_RESPONSE = 1267,
      OB_UPS_FORCE_REPORT_FROZEN_VERSION = 1268,
      OB_UPS_FORCE_REPORT_FROZEN_VERSION_RESPONSE = 1269,
      OB_UPS_SWITCH_COMMIT_LOG = 1270,
      OB_UPS_SWITCH_COMMIT_LOG_RESPONSE = 1271,
      OB_UPS_GET_TABLE_TIME_STAMP = 1272,
      OB_UPS_GET_TABLE_TIME_STAMP_RESPONSE = 1273,
      OB_UPS_ENABLE_MEMTABLE_CHECKSUM = 1274,
      OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE = 1275,
      OB_UPS_DISABLE_MEMTABLE_CHECKSUM = 1276,
      OB_UPS_DISABLE_MEMTABLE_CHECKSUM_RESPONSE = 1277,
      OB_UPS_ASYNC_FORCE_DROP_MEMTABLE = 1278,
      OB_UPS_DELAY_DROP_MEMTABLE = 1279,
      OB_UPS_DELAY_DROP_MEMTABLE_RESPONSE = 1280,
      OB_UPS_IMMEDIATELY_DROP_MEMTABLE = 1281,
      OB_UPS_IMMEDIATELY_DROP_MEMTABLE_RESPONSE = 1282,
      OB_UPS_ASYNC_SWITCH_SKEY = 1283,
      OB_UPS_KEEP_ALIVE = 1284,
      OB_UPS_RS_KEEP_ALIVE = 1285,
      OB_UPS_GET_SLAVE_INFO = 1286,
      OB_UPS_GET_SLAVE_INFO_RESPONSE = 1287,
      OB_UPS_ASYNC_LOAD_BYPASS = 1289,
      //OB_UPS_LOAD_BYPASS = 1290,
      //OB_UPS_LOAD_BYPASS_RESPONSE = 1291,
      OB_UPS_ASYNC_GRANT_KEEP_ALIVE = 1292,
      OB_UPS_ASYNC_CHECK_LEASE = 1293,
      OB_UPS_ASYNC_CHECK_KEEP_ALIVE = 1294,
      OB_UPS_ASYNC_REPLAY_LOG = 1295,
      OB_UPS_CLEAR_FATAL_STATUS = 1296,
      OB_UPS_CLEAR_FATAL_STATUS_RESPONSE =  1297,
      OB_UPS_MINOR_LOAD_BYPASS = 1298,
      OB_UPS_MINOR_LOAD_BYPASS_RESPONSE = 1299,
      OB_UPS_MAJOR_LOAD_BYPASS = 1300,
      OB_UPS_MAJOR_LOAD_BYPASS_RESPONSE = 1301,
      OB_UPS_ASYNC_CHECK_CUR_VERSION = 1302,
      OB_UPS_ASYNC_UPDATE_SCHEMA = 1303,
      OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE = 1304,
      OB_UPS_ASYNC_WRITE_SCHEMA = 1305,
      OB_UPS_ASYNC_KILL_ZOMBIE = 1306,
      OB_UPS_SHOW_SESSIONS = 1307,
      OB_UPS_SHOW_SESSIONS_RESPONSE = 1308,
      OB_UPS_KILL_SESSION = 1309,
      OB_UPS_KILL_SESSION_RESPONSE = 1310,
      OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM = 1311,
      //add wangjiahao [Paxos ups_replication_tmplog] :b
      OB_UPS_ASYNC_REPLAY_TMP_LOG = 1312,
      //add :e

      OB_GET_CLOG_STAT = 1340,
      OB_GET_CLOG_STAT_RESPONSE = 1341,
      //add lbzhong [Clog Monitor] 20151218:b
      OB_CLOG_MONITOR_GET_UPS_LIST = 1342,
      OB_CLOG_MONITOR_GET_UPS_LIST_RESPONSE = 1343,
      OB_CLOG_MONITOR_GET_CLOG_STAT = 1344,
      OB_CLOG_MONITOR_GET_CLOG_STAT_RESPONSE = 1345,
      //add:e

      OB_RS_CHECK_TABLET_MERGED = 1500,
      OB_RS_CHECK_TABLET_MERGED_RESPONSE = 1501,
      OB_RS_GET_LAST_FROZEN_VERSION = 1502,
      OB_RS_GET_LAST_FROZEN_VERSION_RESPONSE = 1503,
      OB_RS_CHECK_ROOTTABLE = 1504,
      OB_RS_CHECK_ROOTTABLE_RESPONSE = 1505,

      OB_GET_INSTANCE_ROLE = 2048,
      OB_GET_INSTANCE_ROLE_RESPONSE = 2049,
      OB_SET_INSTANCE_ROLE = 2050,
      OB_SET_INSTANCE_ROLE_RESPONSE = 2051,
      OB_UPDATE_OCM_REQUEST = 2052,
      OB_UPDATE_OCM_REQUEST_RESPONSE = 2053,
      OB_OCM_CHANGE_STATUS = 2054,
      OB_OCM_CHANGE_STATUS_RESPONSE = 2055,

      OB_MMS_SERVER_REGISTER = 2501,
      OB_MMS_SLAVE_DOWN = 2502,
      OB_MMS_NODE_REGISTER_RESPONSE = 2503,
      OB_MMS_SLAVE_FAILURE_RESPONSE = 2504,
      OB_MMS_HEART_BEAT = 2505,
      OB_MMS_TRANSFER_2_MASTER = 2506,
      OB_MMS_STOP_SLAVE = 2507,
      OB_MMS_HEART_BEAT_RESPONSE = 2508,
      OB_MMS_SLAVE_DOWN_RESPONSE = 2509,
      OB_MMS_TRANSFER_2_MASTER_RESPONSE = 2510,

      /// application session
      OB_LIST_SESSIONS_REQUEST = 2511,
      OB_LIST_SESSIONS_RESPONSE = 2512,
      OB_KILL_SESSION_REQUEST = 2513,
      OB_KILL_SESSION_RESPONSE = 2514,

      //rootserver sesscion
      OB_SET_OBI_ROLE_TO_SLAVE = 3000,
      OB_SET_OBI_ROLE_TO_SLAVE_RESPONSE = 3001,
      OB_RS_INNER_MSG_CHECK_TASK_PROCESS = 3002,
      OB_RS_PREPARE_BYPASS_PROCESS = 3003,
      OB_RS_PREPARE_BYPASS_PROCESS_RESPONSE = 3004,
      OB_RS_START_BYPASS_PROCESS = 3005,
      OB_RS_START_BYPASS_PROCESS_RESPONSE = 3006,
      OB_WRITE_SCHEMA_TO_FILE = 3012,
      OB_WRITE_SCHEMA_TO_FILE_RESPONSE = 3013,
      OB_CHANGE_TABLE_ID = 3014,
      OB_CHANGE_TABLE_ID_RESPONSE = 3015,
      OB_GET_BOOT_STATE = 3016,
      OB_GET_BOOT_STATE_RESPONSE = 3017,
      OB_FORCE_CREATE_TABLE_FOR_EMERGENCY = 3018,
      OB_FORCE_CREATE_TABLE_FOR_EMERGENCY_RESPONSE = 3019,
      OB_FORCE_DROP_TABLE_FOR_EMERGENCY = 3020,
      OB_FORCE_DROP_TABLE_FOR_EMERGENCY_RESPONSE = 3021,
      OB_RS_ADMIN_READ_ROOT_TABLE = 3022,

      // sql
      OB_SQL_EXECUTE = 4000,
      OB_SQL_EXECUTE_RESPONSE = 4001,
      OB_PHY_PLAN_EXECUTE = 4002,
      OB_PHY_PLAN_EXECUTE_RESPONSE = 4003,
      OB_START_TRANSACTION = 4004,
      OB_START_TRANSACTION_RESPONSE = 4005,
      OB_END_TRANSACTION = 4006,
      OB_END_TRANSACTION_RESPONSE = 4007,

      // sql session
      OB_SQL_KILL_SESSION = 4008,
      OB_SQL_KILL_SESSION_RESPONSE = 4009,

      // ob file service
      OB_SEND_FILE_REQUEST = 5000,
      OB_SEND_FILE_REQUEST_RESPONSE = 5001,

      //rs extra packet code
      OB_RS_INNER_MSG_AFTER_RESTART = 6001,

      //add huangjianwei [Paxos rs_switch] 20160726:b
      OB_RS_GET_SERVER_STATUS = 6002,
      //add:e

      // import data source
      OB_RS_ADMIN_START_IMPORT = 9001,
      OB_RS_ADMIN_START_IMPORT_RESPONSE = 9002,
      OB_FETCH_RANGE_TABLE = 9003,
      OB_FETCH_RANGE_TABLE_RESPONSE = 9004,
      OB_RS_ADMIN_START_KILL_IMPORT = 9005,
      OB_RS_ADMIN_START_KILL_IMPORT_RESPONSE = 9006,
      OB_RS_IMPORT = 9007,
      OB_RS_IMPORT_RESPONSE = 9008,
      OB_RS_GET_IMPORT_STATUS = 9011,
      OB_RS_GET_IMPORT_STATUS_RESPONSE = 9012,
      OB_RS_SET_IMPORT_STATUS = 9013,
      OB_RS_SET_IMPORT_STATUS_RESPONSE = 9014,
      OB_RS_NOTIFY_SWITCH_SCHEMA = 9015,
      OB_RS_NOTIFY_SWITCH_SCHEMA_RESPONSE = 9016,

      /// network session
      OB_SESSION_NEXT_REQUEST = 9999,
      OB_SESSION_NEXT_RESPONSE = 10000,
      OB_SESSION_END = 10001,

      //modify peiouya [Get_masterups_and_timestamp] 20141017:b
      OB_GET_UPDATE_SERVER_INFO_AND_TIMESTAMP = 11000,
      OB_GET_UPDATE_SERVER_INFO_AND_TIMESTAMP_RES,
      //add 20141017:e
      //add wenghaixing [secondary index] 20141110
      OB_CREATE_INDEX_COMMAND=12000,
      OB_CS_CREATE_INDEX_SIGNAL=12001,
      //add e
      //add wenghaixing [secondary index drop index]20141223
      OB_DROP_INDEX=12002,
      //add e
      //add liumz, [secondary index static_index_build] 20150320:b
      OB_REPORT_HISTOGRAMS=12003,
      OB_REPORT_HISTOGRAMS_RESPONSE=12004,
      //add:e
      //add liuxiao [secondary index col checksum] 20150401
      //csÎÊrsÒª¾ÉÁÐÐ£ÑéºÍµÄÊÕ·¢°ücode
      OB_GET_OLD_TABLET_COLUMN_CHECKSUM = 12005,
      OB_GET_OLD_TABLET_COLUMN_CHECKSUM_RESPONSE = 12006,
      //add e
	  //add wenghaixing [secondary index static_index_build.exceptional_handle] 20150409
      OB_WHIPPING_WOK = 12007,
      OB_WHIPPING_WOK_RESPONSE = 12008,
      //add e
      //add wenghaixing [secondary index.cluster]20150630
      OB_FETCH_INIT_INDEX = 12009,
      OB_FETCH_INIT_INDEX_RESPONSE = 12010,
      //add e
      //add wenghaixing [secondary index cluster.p2]20150630
      OB_FETCH_INDEX_STAT = 12011,
      OB_FETCH_INDEX_STAT_RESPONSE = 12012,
      //add e
      OB_PACKET_NUM, // do not fill value
    };

    enum ServerFlag
    {
      OB_CLIENT_FLAG = 1,
      OB_CHUNK_SERVER_FLAG = 2,
      OB_UPDATE_SERVER_FLAG = 3,
      OB_ROOT_SERVER_FLAG = 4,
      OB_SELF_FLAG = 5,
    };

    enum PacketPriority {
      NORMAL_PRI = 0,
      LOW_PRI = 1,
    };
    //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
    struct LockWaitPara
    {
      LockWaitPara()
      {
         lock_wait_timeout_ = 3000000;// 3s
         last_conflict_lock_time_ = 0;
         need_response_ = true;
      }
      int64_t lock_wait_timeout_;
      int64_t last_conflict_lock_time_;
      bool need_response_;
    };
    //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
    class ObPacket
    {
      friend class ObPacketQueue;
      public:
        static const int16_t OB_PACKET_CHECKSUM_MAGIC = static_cast<int16_t>(0xBCDE);
        static uint32_t global_chid;
      public:
        ObPacket();
        virtual ~ObPacket();
        virtual void free();

        uint32_t get_channel_id() const;
        void set_channel_id(uint32_t chid);

        int get_packet_len() const;
        void set_packet_len(int length);

        void set_no_free();
        int32_t get_packet_code() const;
        void set_packet_code(const int32_t packet_code);

        int32_t get_target_id() const;
        void set_target_id(const int32_t target_id);

        int64_t get_session_id() const;
        void set_session_id(const int64_t session_id);

        int64_t get_expire_time() const;

        int32_t get_api_version() const;
        void set_api_version(const int32_t api_version);

        void set_data(const ObDataBuffer& buffer);
        ObDataBuffer* get_buffer();

        ObDataBuffer* get_inner_buffer();

        int32_t get_data_length() const;
        void set_data_length(int32_t datalen);

        void set_source_timeout(const int64_t& timeout);
        int64_t get_source_timeout() const;
        void set_receive_ts(const int64_t receive_ts);
        int64_t get_receive_ts() const;
        void set_packet_priority(const int32_t priority);
        int32_t get_packet_priority() const;

        ObPacket* get_next() const;

        easy_request_t* get_request() const;
        void set_request(easy_request_t *r);

        int serialize();
        /*serialize packet to buffer*/
        int serialize(ObDataBuffer *buffer);
        //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
        int serialize(ObDataBuffer *buffer, int64_t cmt_log_seq);
        int64_t get_cmt_log_seq();
        //add :e
        int deserialize();

        /**
         * encode tbnet header, obpacket header, record header,
         *              data into output buffer
         * @param output   pointer to request output buffer
         * @param len      buffer length
         *
         * caller should make sure output has enough content for encode
         */
        bool encode(char* output, int64_t len);

        int64_t get_packet_buffer_offset() const;
        void set_packet_buffer_offset(const int64_t buffer_offset);

        ObDataBuffer* get_packet_buffer();
        const ObDataBuffer* get_packet_buffer() const;
        void set_packet_buffer(char* buffer, const int64_t buffer_length);

        int64_t get_header_size() const;
        void set_trace_id(const uint64_t &trace_id);
        uint64_t get_trace_id() const;
        void set_req_sign(const uint64_t req_sign);
        uint64_t get_req_sign() const;

        void set_ob_packet_header_size(const uint16_t ob_packet_header_size);
        uint16_t get_ob_packet_header_size() const;

        static uint64_t &tsi_req_sign()
        {
          static __thread bool first_invoke = true;
          static __thread uint64_t req_sign = 0;
          if (first_invoke)
          {
            TBSYS_LOG(INFO, "[TSI_REQ_SIGN] %p", &req_sign);
            first_invoke = false;
          }
          return req_sign;
        };
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        int64_t get_lock_wait_timeout() const
        {
            return lock_wait_para_.lock_wait_timeout_;
        }
        void set_lock_wait_timeout(int64_t max_wait_time)
        {
            lock_wait_para_.lock_wait_timeout_ = max_wait_time;
        }
        int64_t get_last_conflict_lock_time() const
        {
            return lock_wait_para_.last_conflict_lock_time_;
        }
        void set_last_conflict_lock_time(const int64_t last_conflict_lock_time)
        {
            lock_wait_para_.last_conflict_lock_time_ = last_conflict_lock_time;
        }
        bool get_need_response()
        {
            return lock_wait_para_.need_response_;
        }
        void set_need_response(const bool need_response)
        {
            lock_wait_para_.need_response_ = need_response;
        }
        tbsys::CThreadMutex* get_lock_for_lock_wait_para()
        {
          return &lock_for_lock_wait_para_;
        }
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e

      private:
        int do_check_sum();
        //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
        int do_check_sum(const int64_t cmt_log_seq);
        //add :e
        int do_sum_check();
        int __deserialize();

      private:
        // indicates whether free is needed or not.
        // free is not needed if the packet is used by server;
        // free is needed if the packet is used by client.
        bool no_free_;

        uint32_t chid_;         //tbnet header channel id
        int pcode_;             //tbnet header packet code
        int packet_len_;          //tbnet header total packet len not include tbnet header
        mutable uint16_t ob_packet_header_size_;
        int16_t api_version_;
        int32_t timeout_;      // encode, decode
        int32_t data_length_;   //real data length
        int32_t priority_;
        int32_t target_id_;
        int64_t receive_ts_;
        int64_t session_id_;   // encode, decode
        int64_t buffer_offset_;
        ObDataBuffer buffer_; // user buffer holder
        ObDataBuffer inner_buffer_; // packet inner buffer
        ObRecordHeader header_;
        easy_request_t *req_;       //request pointer for sendreponse

        ObPacket* _next;         //tbnet::Packet* _next  ObPacketQueue using _next;
        int64_t expire_time_;    //compatible with tbnet packet int64_t _expireTime;
        bool alloc_inner_mem_; // alloc from out_mem_pool_ or not
        uint64_t trace_id_;
        uint64_t req_sign_;
        //TraceId trace_id_;
        static ObVarMemPool out_mem_pool_;
        LockWaitPara lock_wait_para_;  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
        tbsys::CThreadMutex lock_for_lock_wait_para_;  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
    };
  } /* common */
} /* oceanbas */

#endif /* end of include guard: OCEANBASE_PACKET_H_ */
