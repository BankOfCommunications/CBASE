/* (C) 2010-2012 Alibaba Group Holding Limited. 
 * 
 * This program is free software: you can redistribute it and/or 
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation.
 * 
 * Version: 0.1 
 * 
 * Authors: 
 *    Wu Di <lide.wd@taobao.com>
 */

#ifndef OB_PROFILE_H_
#define OB_PROFILE_H_

namespace oceanbase
{
  namespace common
  {
    /* common */
#define PCODE " pcode=[%d]"
#define SELF " self=[%s]"
#define PEER " peer=[%s]"
#define RET " ret=[%d]"
#define TRACE_ID " trace_id=[%ld]"
#define SOURCE_CHANNEL_ID " source_chid=[%u]"
#define CHANNEL_ID " chid=[%u]"
    /* 异步rpc发起时间*/
#define ASYNC_RPC_START_TIME " async_rpc_start_time=[%ld]"
    /*异步rpc结束时间，对应关系由chid来对应*/
#define ASYNC_RPC_END_TIME " async_rpc_end_time=[%ld]"
    /*同步rpc发起时间*/
#define SYNC_RPC_START_TIME " sync_rpc_start_time=[%ld]"
    /*同步rpc结束时间*/
#define SYNC_RPC_END_TIME " sync_rpc_end_time=[%ld]"
    /*网络线程接到包的时间*/
#define PACKET_RECEIVED_TIME " packet_received_time=[%ld]"
    /*请求在queue中等待的时间 单位：微秒*/
#define WAIT_TIME_US_IN_QUEUE " wait_time_us_in_queue=[%ld]"
    /*实际处理一个包的开始时间，不包括排队时间等*/
#define HANDLE_PACKET_START_TIME " handle_packet_start_time=[%ld]"
    /*实际处理一个包的结束时间*/
#define HANDLE_PACKET_END_TIME " handle_packet_end_time=[%ld]"

    /* updateserver */
    /*写请求在写队列中等待的时间,单位：微秒*/
#define WAIT_TIME_US_IN_WRITE_QUEUE " wait_time_us_in_write_queue=[%ld]"
    /*读事务，写事务 在读写混合队列中等待的时间，单位：微秒*/
#define WAIT_TIME_US_IN_RW_QUEUE " wait_time_us_in_rw_queue=[%ld]"
    /*写事务 在提交队列中等待的时间，单位：微秒*/
#define WAIT_TIME_US_IN_COMMIT_QUEUE " wait_time_us_in_commit_queue=[%ld]"
    /*UPS处理一个请求的开始时间，即从网络线程接到请求包到回复结果的整个过程*/
#define UPS_REQ_START_TIME " req_start_time=[%ld]"
    /*UPS处理一个请求的结束时间, 这个时间减去上面的时间就是UPS处理一个请求花费的总的时间*/
#define UPS_REQ_END_TIME " req_end_time=[%ld]"
    /*UPS响应的ObScanner的size大小，单位：字节*/
#define SCANNER_SIZE_BYTES " scanner_size_bytes=[%ld]"


   /* mergeserver */
    /*parser解析sql语句生成逻辑执行计划花费的时间， 单位：微秒*/
#define SQL_TO_LOGICALPLAN_TIME_US " sql_to_logicalplan_time_us=[%ld]"
    /*逻辑执行计划生成物理执行计划的时间， 单位：微秒*/
#define LOGICALPLAN_TO_PHYSICALPLAN_TIME_US " logicalplan_to_physicalplan_time_us=[%ld]"
    /*mysql 包在 ObMySQL的队列中等待的时间 单位：微秒*/
#define WAIT_TIME_US_IN_SQL_QUEUE " wait_time_us_in_sql_queue=[%ld]"
    /*客户端发过来的sql请求*/
#define SQL " sql=[%.*s]"
    /*处理一个SQL请求花费的时间，不包括在ObMySQL队列中等待的时间*/
#define HANDLE_SQL_TIME_MS " handle_sql_time_ms=[%ld]"
    /*obmysql的工作线程和IO线程交互所花费的时间, 只和SELECT相关*/
#define IO_CONSUMED_TIME_US " io_consumed_time_us=[%d]"
#define SEND_RESPONSE_MEM_TIME_US " send_response_mem_time_us=[%d]"
#define STMT_EXECUTE_TIME_US " stmt_execute_us=[%ld]"
#define PARSE_EXECUTE_PARAMS_TIME_US " parse_execute_params=[%ld]"
#define CLOSE_PREPARE_RESULT_SET_TIME_US " close_prepare_result_set=[%ld]"
#define CLEAN_SQL_ENV_TIME_US " clean_sql_env_time_us=[%ld]"
#define OPEN_RESULT_SET " open_result_set_time_us=[%ld]"
#define OPEN_SQL_GET_REQUEST " open_sql_get_request_time_us=[%ld]"
#define OPEN_RPC_SCAN " open_rpc_scan_time_us=[%ld]"
#define CONS_SQL_GET_REQUEST " cons_sql_get_request=[%ld]"
#define CONS_SQL_SCAN_REQUEST " cons_sql_scan_request=[%ld]"
#define CREATE_SCAN_PARAM " create_scan_param_time_us=[%ld]"
#define SET_PARAM " set_param_time_us=[%ld]"
#define NEW_OB_SQL_SCAN_PARAM " new_ob_sql_scan_param_time_us=[%ld]"
#define NEW_OB_MS_SQL_SCAN_REQUEST " new_ob_ms_sql_scan_request=[%ld]" 
#define RESET_SUB_REQUESTS " reset_sub_requests_time_us=[%ld]"
    /*ms scan root table的开始时间和结束时间*/
#define SCAN_ROOT_TABLE_START_TIME " scan_root_table_start_time=[%ld]"
#define SCAN_ROOT_TABLE_END_TIME " scan_root_table_end_time=[%ld]"
  }// common
}// oceanbase

#endif
