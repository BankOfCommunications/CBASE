/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_server.h is for what ...
 *
 * Version: ***: ob_mysql_server.h  Tue Jul 17 10:33:39 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_SERVER_H_
#define OB_MYSQL_SERVER_H_

#include "ob_mysql_packet_queue_handler.h"
#include "common/thread_buffer.h"
#include "easy_io_struct.h"
#include "ob_mysql_loginer.h"
#include "ob_mysql_command_queue_thread.h"
#include "sql/ob_sql_session_info.h"
#include "common/ob_define.h"
#include "ob_mysql_callback.h"
#include "easy_io.h"
#include "ob_mysql_define.h"
#include "ob_mysql_result_set.h"
#include "ob_mysql_util.h"
#include <pthread.h>
#include "sql/ob_sql.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "mergeserver/ob_merge_server_service.h"
#include "common/ob_se_array.h"
#include "common/ob_resource_pool.h"
#include "sql/ob_sql_session_mgr.h"
#include "mergeserver/ob_merge_server_config.h"
#include "mergeserver/ob_bloom_filter_task_queue_thread.h"
#include "sql/ob_sql_query_cache.h"
#include "common/ob_timer.h"
#include "sql/ob_ps_store.h"
#include "common/ob_fifo_stream.h"
//del by maosy [Delete_Update_Function]
//extern int64_t iud_affect_rows;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911 /*Exp:set the affected rows*/
//extern int is_iud;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911 /*Exp:the signal to indicate that the stm is insert...select... */
//del e
namespace oceanbase
{
  namespace common
  {
    class ObMergerSchemaManager;
    class ObTabletLocationCacheProxy;
  } // end namespace common
  namespace mergeserver
  {
    class ObMergerRpcProxy;
    class ObMergerRootRpcProxy;
    class ObMergerAsyncRpcStub;
  } // end namespace mergeserver

  namespace obmysql
  {
    typedef int ObMySQLSessionKey;
    //This server is not inherit from baseserver
    //ObBaseServer used to send obpacket
    class ObMySQLLoginer;
    class ObMySQLServer : public ObMySQLPacketQueueHandler
    {
      public:
        struct MergeServerEnv
        {
          mergeserver::ObMergerRpcProxy  *rpc_proxy_;
          mergeserver::ObMergerRootRpcProxy * root_rpc_;
          mergeserver::ObMergerAsyncRpcStub   *async_rpc_;
          common::ObMergerSchemaManager *schema_mgr_;
          common::ObTabletLocationCacheProxy *cache_proxy_;
          common::ObPrivilegeManager *privilege_mgr_;
          const mergeserver::ObMergeServerService *merge_service_;
          common::ObStatManager *stat_mgr_;

          MergeServerEnv():
            rpc_proxy_(NULL),
            root_rpc_(NULL),
            async_rpc_(NULL),
            schema_mgr_(NULL),
            cache_proxy_(NULL),
            privilege_mgr_(NULL),
            merge_service_(NULL),
            stat_mgr_(NULL)
          {
          }
        };

      public:
        static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024 * 1024 *2; //2MB

        ObMySQLServer();
        ~ObMySQLServer();
        /**
         * post packet async, copy data into request buffer && wakeup ioth to send packet
         * @param     req  request pointer
         * @param     packet  packet to send
         * @param     seq  packe sequence
         * @return    return OB_SUCCESS if encode successful
         *            else return OB_ERROR
         */
        int post_packet(easy_request_t* req, ObMySQLPacket* packet, uint8_t seq);
        int get_privilege(ObPrivilege * p_privilege);
        int load_system_params(sql::ObSQLSessionInfo& session);
        common::DefaultBlockAllocator* get_block_allocator();

        /**
         * This function called by on_connect to handle client login
         *
         */
        int login_handler(easy_connection_t* c);

        int handle_packet(ObMySQLCommandPacket* packet);

        /**
         * initialize server and start work/io thread
         */
        int start(bool need_wait = true);

        void destroy();

        void stop();

        /** wait for eio stop*/
        void wait();

        /*set obmysql param using mergeserver param*/
        void set_config(const mergeserver::ObMergeServerConfig *config);
        void set_env(MergeServerEnv &env);
        const mergeserver::ObMergeServerConfig *get_config() const {return config_;};
        const MergeServerEnv& get_env() const{return env_;};

        sql::ObSQLSessionMgr* get_session_mgr()
        {
          return &session_mgr_;
        }

        ObServer* get_server()
        {
          return &self_;
        }


        int set_io_thread_count(const int32_t io_thread_count);

        int set_work_thread_count(const int32_t work_thread_count);

        int submit_session_delete_task(ObMySQLSessionKey key);
        bool has_too_many_sessions() const;
        typedef common::ObPooledAllocator<sql::ObSQLSessionInfo, ObMalloc, ObSpinLock> SessionPool;
        SessionPool& get_session_pool() {return session_pool_;};
        ObPsStore* get_ps_store()
        {
          return &ps_store_;
        }
        ObFIFOStream& get_packet_recorder() {return packet_recorder_;};
      private:
        int initialize();

        /** wait for packt queue thread*/
        void wait_for_queue();

        /** called when server is stop*/
        int do_request();

        /** perf stat */
        inline int do_stat(bool is_compound_stmt, ObBasicStmt::StmtType stmt_type, int64_t consumed_time);

        int set_port(const int32_t port);
        int set_task_queue_size(const int32_t task_queue_size);

        /**
         * handle request
         * 1. call ob sql engine with sql statement in packet
         * 2. send response to mysql client
         * @param packet   request packet pop from queue
         * @param args     always NULL
         *
         */
        int handle_packet_queue(ObMySQLCommandPacket* packet, void* args);

      private:

        common::ThreadSpecificBuffer::Buffer* get_buffer() const;

        //handle request
        int do_com_query(ObMySQLCommandPacket* packet);

        int do_com_quit(ObMySQLCommandPacket* packet);

        int do_com_prepare(ObMySQLCommandPacket* packet);

        int do_com_execute(ObMySQLCommandPacket* packet);

        int do_com_close_stmt(ObMySQLCommandPacket* packet);

        int do_com_reset_stmt(ObMySQLCommandPacket* packet);

        int do_com_ping(ObMySQLCommandPacket* packet);

        int do_com_delete_session(ObMySQLCommandPacket* packet);

        //TODO just skip request to be deleted
        int do_unsupport(ObMySQLCommandPacket* packet);

        /**
         * init environment
         * get session and sql context
         * @param packet [in] input packet
         * @context[in/out]         sql context for sql engine
         * @schema_version[in/out]  get latest schema version
         * @result[in/out]          set message if error happend
         */
        int init_sql_env(const ObMySQLCommandPacket& packet, ObSqlContext &context,
                         int64_t &schema_version, ObMySQLResultSet &result);
        void cleanup_sql_env(ObSqlContext &context, ObMySQLResultSet &result, bool unlock = true);

        /**
         * Parse param in COM_STMT_EXECUTE packet
         * COM_STMT_EXECUTE
         * payload:
         *  1              [17] COM_STMT_EXECUTE
         *  4              stmt-id
         *  1              flags
         *  4              iteration-count
         *    if num-params > 0:
         *  n              NULL-bitmap, length: (num-params+7)/8
         *  1              new-params-bound-flag
         *    if new-params-bound-flag == 1:
         *  n              type of each parameter, length: num-params * 2
         *  n              value of each parameter
         *
         * @param context[in]      context of this query
         * @param packet[in]       COM_STMT_EXECUTE packet pointer
         * @param params[in/out]   ObArray<ObObj> params
         * @param stmt_id[in/out]  statement id of COM_STMT_EXECUTE packet
         *
         * @return int             return OB_SUCCESS if parse params success, else return OB_ERROR
         */
        int parse_execute_params(ObSqlContext *context, ObMySQLCommandPacket *packet, ObIArray<ObObj> &params, uint32_t &stmt_id, ObIArray<EMySQLFieldType> &params_type);

        /**
         * get param value from buffer
         *
         * @param data[in/out]     data buffer
         * @param param[in/out]    param object
         * @param type             param type
         *
         * @return int             return OB_SUCCESS if parse params success, else return OB_ERROR
         */
        int get_param_value(char *&data, ObObj &param, uint8_t type);


        /**
         * get timestamp value from buffer
         *
         * @param data[in/out]     data buffer
         * @param param[in/out]    param object
         *
         * @return int             return OB_SUCCESS if parse params success, else return OB_ERROR
         */
        int get_mysql_timestamp_value(char *&data, ObObj &param);

        /**
         * parse result rest send packet to client
         *
         * This method will handle all error during send response,
         * Ex: result open failed, client quit.....
         *
         * TEXT PROTOCOL
         * if (result has row)
         * 1. RESULT HEADER packet
         * 2. FIELD packets
         * 3. EOF packet
         * 4. ROW packets in text protocol
         * 5. EOF packet
         * else
         * 1. OK packet
         *
         * BINARY PROTOCOL
         * if (result has row)
         * 1. FIELD count
         * 2. FIELD packets
         * 3. EOF packet
         * 4. ROW packets in binary protocol
         * 5. EOF packet
         * else
         * 1. OK packet
         *
         * @param packet  request packet
         * @param result  result set returned by sql proxy
         * @param type    prottocol type
         * @return int    return OB_SUCCESS if all packets sended
         *                else return OB_ERROR
         */
        int send_response(ObMySQLCommandPacket* packet, ObMySQLResultSet* result,
                          MYSQL_PROTOCOL_TYPE type, sql::ObSQLSessionInfo *session);

        /**
         * Parse result set and send COM_STMT_PREPARE_RESPONSE
         *
         * This method will handle all error during send response,
         * Ex: result open failed, client quit....
         *
         * Fields see http://dev.mysql.com/doc/internals/en/prepared-statements.html#com-stmt-prepare
         *   status (1) -- [00] OK
         *   statement_id (4) -- statement-id
         *   num_columns (2) -- number of columns
         *   num_params (2) -- number of params
         *   reserved_1 (1) -- [00] filler
         *   warning_count (2) -- number of warnings
         *   param_def_block (string.var_len) -- if num_params > 0
         *   num_params * Protocol::ColumnDefinition
         *   EOF_Packet
         *   column_def_block (string.var_len) -- if num_columns > 0
         *   num_colums * Protocol::ColumnDefinition
         *   EOF_Packet
         *
         * @param packet        request packet
         * @param result        result set from sql engine
         * @return int          return OB_SUCCESS if COM_STMT_PREPARE_RESPONSE or
         *                                           ERROR packet sended
         *                                        else return OB_ERROR
         */
        int send_stmt_prepare_response(ObMySQLCommandPacket* packet, ObMySQLResultSet* result,
                                       sql::ObSQLSessionInfo *session);


        /**
         * Send error packet as response
         * @param packet      which req to response
         * @param result      sql result set
         *
         * @return int        return OB_SUCCESS if send error packet successful
         *                                      else return OB_ERROR/OB_INVALID_ARGUMENT
         */
        int send_error_packet(ObMySQLCommandPacket* packet,
                              ObMySQLResultSet *result);

        /**
         * Send statement prepare response packet as response
         * Following with column desc && param desc packet if exist
         * @param packet      which req to response
         * @param result      sql result set
         *
         * @return int        return OB_SUCCESS if send error packet successful
         *                                      else return OB_ERROR/OB_INVALID_ARGUMENT
         */
        int send_spr_packet(ObMySQLCommandPacket* packet,
                            ObMySQLResultSet* result);


        /**
         * Send error packet as response
         * @param packet      which req to response
         * @param result      sql result set
         *
         * @return int        return OB_SUCCESS if send error packet successful
         *                                      else return OB_ERROR/OB_INVALID_ARGUMENT
         */
        int send_ok_packet(ObMySQLCommandPacket* packet,
                           ObMySQLResultSet *result, uint16_t server_status);

        //优化发送结果集
        //TODO 涉及到的packet 都要加上encode方法
        /**
         * 发送结果集给客户端
         * 尽可能的把结果集合并到一个数据包发送给客户端
         * @param    req    request pointer
         * @param    result query result
         * @param    type   protocol type
         * @param    charset
         *
         * @return   int    OB_SUCCESS if send successful
         *                  else return OB_ERROR
         */
        int send_result_set(easy_request_t *req, ObMySQLResultSet *result,
                            MYSQL_PROTOCOL_TYPE type, uint16_t server_status, uint16_t charset);

        /**
         * 发送绑定变量结果集
         * 尽可能的把结果集合并到一个数据包发送给客户端
         * @param    packet request packet
         * @param    result query result
         *
         * @return   int    OB_SUCCESS if send successful
         *                  else return OB_ERROR
         */
        int send_stmt_prepare_result_set(ObMySQLCommandPacket *packet, ObMySQLResultSet *result,
                                         uint16_t server_status, uint16_t charset);

        /**
         * 同步发送已经链接到req->output上的数据
         * 包全部序列化到线程私有buffer
         * @param    req   request pointer
         *
         * @return   int   OB_SUCCESS if send successful
         *                 else return OB_ERROR
         */
        int send_raw_packet(easy_request_t *req);

        /**
         * 异步发送已经链接到req->output上的数据
         * 包全部序列化到libeasy message的buffer里
         * @param    req   request pointer
         *
         * @return   int   OB_SUCCESS if send successful
         *                 else return OB_ERROR
         */
        int post_raw_packet(easy_request_t *req);

        int post_raw_bytes(easy_request_t * req, const char * bytes, int64_t len);

        int process_spr_packet(easy_buf_t *&buff, int64_t &buff_pos,
                               easy_request_t *req, ObMySQLResultSet *result);
        int process_resheader_packet(easy_buf_t *&buff, int64_t &buff_pos,
                                     easy_request_t *req, ObMySQLResultSet *result);
        int process_field_packets(easy_buf_t *&buff, int64_t &buff_pos,
                                  easy_request_t *req, ObMySQLResultSet *result, bool is_field, uint16_t charset);
        int process_eof_packet(easy_buf_t *&buff, int64_t &buff_pos,
                               easy_request_t *req, ObMySQLResultSet *result, uint16_t server_status);

        /**
         * Send field/params packets as response
         * This method will try to serialize all rows into one packet
         * if there are no space to store one row any more, call send_raw_packet, then resue message buffer
         * @param buff
         * @param req         request pointer
         * @param result      sql result set
         * @param wait_obj    wait object
         * @param is_field    true means send field otherwise send param
         *
         * @return int        return OB_SUCCESS if send error packet successful
         *                                      else return OB_ERROR/OB_INVALID_ARGUMENT
         */
        int process_row_packets(easy_buf_t *&buff, int64_t &buff_pos,
                                easy_request_t *req, ObMySQLResultSet *result, MYSQL_PROTOCOL_TYPE type);

        /**
         * 处理结果集中的单个数据包
         * 把包序列化到指定的buff中去 如果buff不够的
         * 先把之前的包拷贝到线程buffer中等待发送完，再重用buffer把包序列化
         * @param buff               buff pointer
         * @param req                req pointer
         * @param packet             待发送的数据包
         */
        int process_single_packet(easy_buf_t *&buff, int64_t &buff_pos, easy_request_t *req, ObMySQLPacket *packet);
        //end of 优化发送结果集

        int access_cache(ObMySQLCommandPacket * packet, uint32_t stmt_id,
            ObSqlContext & context, const ObIArray<ObObj> & param);

        int fill_cache();
        int store_query_string(const ObMySQLCommandPacket& packet, ObSqlContext &context);
        inline void wait_client_obj(easy_client_wait_t& client_wait)
        {
          pthread_mutex_lock(&client_wait.mutex);
          if (client_wait.done_count == 0)
          {
            //unlock同时wait在cond上，等待网络框架将其唤醒,唤醒的时候同时获得锁
            pthread_cond_wait(&client_wait.cond, &client_wait.mutex);
          }
          pthread_mutex_unlock(&client_wait.mutex);
        }

        inline int check_param(ObMySQLCommandPacket *packet)
        {
          int ret = OB_SUCCESS;
          if (NULL == packet)
          {
            TBSYS_LOG(ERROR, "should not reach here invalid argument packet is %p", packet);
            ret = OB_INVALID_ARGUMENT;
          }
          else if (NULL == packet->get_request())
          {
            TBSYS_LOG(ERROR, "should not reach here invalid argument request is %p", packet->get_request());
            ret = OB_INVALID_ARGUMENT;
          }
          return ret;
        }

        inline int check_req(easy_request_t *req)
        {
          int ret = OB_ERROR;
          if (OB_LIKELY(NULL != req && NULL != req->ms
                        && NULL != req->ms->c))
          {
            ret = OB_SUCCESS;
          }
          return ret;
        }

        int trans_exe_command_with_type(ObSqlContext* context, ObMySQLCommandPacket* packet, char* buf, int32_t &len, bool &trans_flag);

        static void easy_on_ioth_start(void *arg)
        {
          if (arg != NULL)
          {
            ObMySQLServer *server = reinterpret_cast<ObMySQLServer*>(arg);
            server->on_ioth_start();
          }
          
        }
        void on_ioth_start()
        {
          int64_t affinity_start_cpu = config_->obmysql_io_thread_start_cpu;
          int64_t affinity_end_cpu = config_->obmysql_io_thread_end_cpu;
          if (0 <= affinity_start_cpu
              && affinity_start_cpu <= affinity_end_cpu)
          {
            static volatile int64_t cpu = 0;
            int64_t local_cpu = __sync_fetch_and_add(&cpu, 1) % (affinity_end_cpu - affinity_start_cpu + 1) + affinity_start_cpu;
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(local_cpu, &cpuset);
            int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            TBSYS_LOG(INFO, "io thread setaffinity tid=%ld ret=%d cpu=%ld start=%ld end=%ld",
                      GETTID(), ret, local_cpu, affinity_start_cpu, affinity_end_cpu);
          }
        }


      private:
        static const int64_t SEQ_OFFSET = 3; /* offset of seq in MySQL packet */
      private:
        int32_t io_thread_count_;
        int32_t work_thread_count_;
        int32_t task_queue_size_;
        int32_t port_;
        //char dev_name_[DEV_NAME_LENGTH];

        const mergeserver::ObMergeServerConfig *config_;
        //runing flag
        bool stop_;
        //thread specific buffers
        common::ThreadSpecificBuffer response_buffer_;
        //login hander
        ObMySQLLoginer login_handler_;
        //working thread
        ObMySQLCommandQueueThread command_queue_thread_;

        //working thread handle close rquest
        ObMySQLCommandQueueThread close_command_queue_thread_;


        //libeasy stuff
        easy_io_t* eio_;
        easy_io_handler_pt handler_;

        // environment of mergeserver
        MergeServerEnv env_;
        // session info pool
        SessionPool session_pool_;
        // session manager
        sql::ObSQLSessionMgr session_mgr_;
        // global block allocator
        common::DefaultBlockAllocator block_allocator_;

        ObServer self_;

        typedef ObSEArray<ObObj, OB_PREALLOCATED_NUM>           ParamArray;
        typedef ObSEArray<EMySQLFieldType, OB_PREALLOCATED_NUM> ParamTypeArray;
        typedef common::ObResourcePool<ParamArray, 1>           ParamArrayPool;
        typedef common::ObResourcePool<ParamTypeArray, 1>       ParamTypeArrayPool;
        ParamArrayPool param_pool_;
        ParamTypeArrayPool param_type_pool_;

        ObSQLIdMgr sql_id_mgr_;
        ObSQLQueryCache query_cache_;
        common::ObTimer timer_;
        // ps store
        ObPsStore ps_store_;

        static const int64_t FIFO_STREAM_BUFF_SIZE = 100LL*1024*1024;
        common::ObFIFOStream packet_recorder_;
    };
  }
}
#endif
