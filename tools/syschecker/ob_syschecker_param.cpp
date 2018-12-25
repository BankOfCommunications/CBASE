/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_param.cpp for parse parameter. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <strings.h>
#include <tblog.h>
#include <tbsys.h>
#include "common/ob_malloc.h"
#include "ob_syschecker_param.h"

namespace 
{
  static const char* OBRS_SECTION = "root_server";
  static const char* OBRS_IP = "vip";
  static const char* OBRS_PORT = "port";

  static const char* OBUPS_SECTION  = "update_server";
  static const char* OBUPS_IP = "vip";
  static const char* OBUPS_PORT = "port";

  static const char* OBMS_SECTION  = "merge_server";
  static const char* OBMS_MERGE_SERVER_STR = "merge_server_str";
  static const char* OBMS_CHUNK_SERVER_STR = "chunk_server_str";

  static const char* OBSC_SECTION = "syschecker";
  static const char* OBSC_WRITE_THREAD_COUNT = "write_thread_count";
  static const char* OBSC_READ_THREAD_COUNT = "read_thread_count";
  static const char* OBSC_NETWORK_TIMEOUT = "network_timeout";
  static const char* OBSC_SYSCHECKER_COUNT = "syschecker_count";
  static const char* OBSC_SYSCHECKER_NO = "syschecker_no";
  static const char* OBSC_SPECIFIED_READ_PARAM = "specified_read_param";
  static const char* OBSC_OPERATE_FULL_ROW = "operate_full_row";
  static const char* OBSC_STAT_DUMP_INTERVAL = "stat_dump_interval";
  static const char* OBSC_PERF_TEST = "perf_test";
  static const char* OBSC_SQL_READ = "sql_read";
  static const char* OBSC_CHECK_RESULT = "check_result";
  static const char* OBSC_READ_TABLE_TYPE = "read_table_type";
  static const char* OBSC_WRITE_TABLE_TYPE = "write_table_type";
  static const char* OBSC_GET_ROW_CNT = "get_row_cnt";
  static const char* OBSC_SCAN_ROW_CNT = "scan_row_cnt";
  static const char* OBSC_UPDATE_ROW_CNT = "update_row_cnt";
}

namespace oceanbase 
{ 
  namespace syschecker 
  {
    using namespace common;

    ObSyscheckerParam::ObSyscheckerParam()
    {
      network_time_out_ = 0;
      write_thread_count_ = 0;
      read_thread_count_ = 0;
      syschecker_count_ = 0;
      syschecker_no_ = 0;
      specified_read_param_ = 0;
      operate_full_row_ = 0;
      stat_dump_interval_ = 0;
      perf_test_ = 0;
      check_result_ = 0;
      read_table_type_ = 0;
      write_table_type_ = 0;
      get_row_cnt_ = 0;
      scan_row_cnt_ = 0;
      update_row_cnt_ = 0;
    }

    ObSyscheckerParam::~ObSyscheckerParam()
    {
    }

    int ObSyscheckerParam::load_string(char* dest, const int64_t size, 
                                       const char* section, const char* name, 
                                       bool not_null)
    {
      int ret           = OB_SUCCESS;
      const char* value = NULL;

      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        value = TBSYS_CONFIG.getString(section, name);
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(WARN, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(WARN, "%s.%s too long, length (%lu) > %ld", 
                    section, name, strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memset(dest, 0, size);
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
    }

    int ObSyscheckerParam::parse_server(char* str, common::ObArray<common::ObServer>& servers)
    {
      int ret           = OB_SUCCESS;
      char *str_ptr     = NULL;
      int32_t port      = 0;
      char *begin_ptr   = NULL;
      char *end_ptr     = str + strlen(str);
      char *ptr         = NULL;
      int64_t length    = 0;
      ObServer server;
      char buffer[OB_MAX_IP_SIZE];

      if (NULL == str)
      {
        TBSYS_LOG(WARN, "invalid param, str=%p", str);
        ret = OB_ERROR;
      }
    
      /**
       * Servers list format is like this. For examle:
       * "127.0.0.1:11108,127.0.0.1:11109"
       */
      for (begin_ptr = str; begin_ptr != end_ptr && OB_SUCCESS == ret;) 
      {
        port = 0;
        str_ptr = index(begin_ptr, ',');

        if (NULL != str_ptr) 
        {
          memcpy(buffer, begin_ptr, str_ptr - begin_ptr);
          buffer[str_ptr - begin_ptr]= '\0';
          begin_ptr = str_ptr + 1;
        } 
        else 
        {
          length= strlen(begin_ptr);
          memcpy(buffer, begin_ptr, length);
          buffer[length] = '\0';
          begin_ptr = end_ptr;
        }

        ptr = index(buffer, ':');
        if (NULL != ptr) {
          ptr[0] = '\0';
          ptr++;
          port = static_cast<int32_t>(strtol(ptr, (char**)NULL, 10));
        }

        server.set_ipv4_addr(buffer, port);
        ret = servers.push_back(server);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "invalid merge server ip=%s, port=%d.", buffer, port);
          break;
        }

        if (isspace(*begin_ptr))
        {
          begin_ptr++;
        }
      }

      return ret;
    }

    int ObSyscheckerParam::load_server()
    {
      int ret = OB_SUCCESS;
      char server_str[OB_MAX_MERGE_SERVER_STR_SIZE];

      if (OB_SUCCESS != (ret = load_string(server_str, OB_MAX_MERGE_SERVER_STR_SIZE, 
              OBMS_SECTION, OBMS_MERGE_SERVER_STR, false)))
      {
      }
      else if (server_str[0] != 0 && OB_SUCCESS != (ret = parse_server(server_str, merge_servers_)))
      {
        TBSYS_LOG(WARN, "failed to parse merge server string");
      }

      if (OB_SUCCESS != (ret = load_string(server_str, OB_MAX_MERGE_SERVER_STR_SIZE, 
              OBMS_SECTION, OBMS_CHUNK_SERVER_STR, false)))
      {
      }
      else if (server_str[0] != 0 && OB_SUCCESS != (ret = parse_server(server_str, chunk_servers_)))
      {
        TBSYS_LOG(WARN, "failed to parse merge server string");
      }

      return ret;
    }

    int ObSyscheckerParam::load_from_config()
    {
      int ret                     = OB_SUCCESS;
      int32_t root_server_port    = 0;
      int32_t update_server_port  = 0;
      char root_server_ip[OB_MAX_IP_SIZE];
      char update_server_ip[OB_MAX_IP_SIZE];

      //load root server
      if (OB_SUCCESS == ret)
      {
        ret = load_string(root_server_ip, OB_MAX_IP_SIZE, 
                          OBRS_SECTION, OBRS_IP, true);
      }
      if (OB_SUCCESS == ret)
      {
        root_server_port = TBSYS_CONFIG.getInt(OBRS_SECTION, 
                                               OBRS_PORT, 0);
        if (root_server_port <= 0)
        {
          TBSYS_LOG(WARN, "root server port=%d cann't <= 0.", 
                    root_server_port);
          ret = OB_INVALID_ARGUMENT;
        }
      }   
      if (OB_SUCCESS == ret)
      {
        bool res = root_server_.set_ipv4_addr(root_server_ip, 
                                              root_server_port);
        if (!res)
        {
          TBSYS_LOG(WARN, "root server ip=%s, port=%d is invalid.", 
                    root_server_ip, root_server_port);
          ret = OB_ERROR;
        }
      }

      //load update server
      if (OB_SUCCESS == ret)
      {
        ret = load_string(update_server_ip, OB_MAX_IP_SIZE, 
                          OBUPS_SECTION, OBUPS_IP, true);
      }
      if (OB_SUCCESS == ret)
      {
        update_server_port = TBSYS_CONFIG.getInt(OBUPS_SECTION, 
                                                 OBUPS_PORT, 0);
        if (update_server_port <= 0)
        {
          TBSYS_LOG(WARN, "update server port=%d cann't <= 0.", 
                    update_server_port);
          ret = OB_INVALID_ARGUMENT;
        }
      }   
      if (OB_SUCCESS == ret)
      {
        bool res = update_server_.set_ipv4_addr(update_server_ip, 
                                                update_server_port);
        if (!res)
        {
          TBSYS_LOG(WARN, "update server ip=%s, port=%d is invalid.", 
                    update_server_ip, update_server_port);
          ret = OB_ERROR;
        }
      }

      // load merge server
      if (OB_SUCCESS == ret)
      {
        ret = load_server();
      }

      if (OB_SUCCESS == ret)
      {
        write_thread_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                  OBSC_WRITE_THREAD_COUNT, 
                                                  DEFAULT_WRITE_THREAD_COUNT);
        if (write_thread_count_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker write thread count=%ld can't <= 0." ,
              write_thread_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        read_thread_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                 OBSC_READ_THREAD_COUNT, 
                                                 DEFAULT_READ_THERAD_COUNT);
        if (read_thread_count_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker read thread count=%ld can't <= 0." ,
              read_thread_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (read_thread_count_ <= 0 && write_thread_count_ <= 0)
      {
        TBSYS_LOG(WARN, "both read_thread_count_ and write_thread_count_ is "
                        "invalid, read_thread_count_=%ld, write_thread_count_=%ld",
                  read_thread_count_, write_thread_count_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        network_time_out_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_NETWORK_TIMEOUT, 
                                                DEFAULT_TIMEOUT);
        if (network_time_out_ <= 0)
        {
          TBSYS_LOG(WARN, "syschecker network timeout=%ld can't <= 0." ,
                    network_time_out_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        syschecker_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_SYSCHECKER_COUNT, 
                                                DEFAULT_SYSCHECKER_COUNT);
        if (syschecker_count_ <= 0)
        {
          TBSYS_LOG(WARN, "syschecker syschecker count=%ld can't <= 0." ,
              syschecker_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        syschecker_no_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                             OBSC_SYSCHECKER_NO, 0);
        if (syschecker_no_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker syschecker no=%ld can't < 0." ,
              syschecker_no_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        specified_read_param_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                    OBSC_SPECIFIED_READ_PARAM, 0);
        if (specified_read_param_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker specified_read_param_=%ld can't < 0." ,
              specified_read_param_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        operate_full_row_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_OPERATE_FULL_ROW, 0);
        if (operate_full_row_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker operate_full_row_=%ld can't < 0." ,
              operate_full_row_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        stat_dump_interval_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_STAT_DUMP_INTERVAL, 0);
        if (stat_dump_interval_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker stat_dump_interval_=%ld can't < 0." ,
              stat_dump_interval_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        perf_test_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_PERF_TEST, 
                                         DEFAULT_PERF_TEST);
        if (perf_test_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker perf_test_=%ld can't < 0." ,
              perf_test_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        is_sql_read_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_SQL_READ, 
                                         DEFAULT_SQL_READ);
        if (is_sql_read_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker is_sql_read_=%ld can't < 0." ,
              is_sql_read_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        check_result_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_CHECK_RESULT, 
                                            DEFAULT_CHECK_RESULT);
        if (check_result_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker check_result_=%ld can't < 0." ,
              check_result_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        read_table_type_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_READ_TABLE_TYPE, 
                                               DEFAULT_READ_TABLE_TYPE);
        if (read_table_type_ < 0 || read_table_type_ > 2)
        {
          TBSYS_LOG(WARN, "syschecker read_table_type_=%ld can't < 0 or > 2, "
                          "0:all_table, 1:wide_table, 2: join_table" ,
              read_table_type_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        write_table_type_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_WRITE_TABLE_TYPE, 
                                                DEFAULT_WRITE_TABLE_TYPE);
        if (write_table_type_ < 0 || write_table_type_ > 2)
        {
          TBSYS_LOG(WARN, "syschecker write_table_type_=%ld can't < 0 or > 2, "
                          "0:all_table, 1:wide_table, 2: join_table" ,
              write_table_type_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        get_row_cnt_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_GET_ROW_CNT, 
                                          DEFAULT_GET_ROW_CNT);
        if (get_row_cnt_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker get_row_cnt_=%ld can't < 0." ,
              get_row_cnt_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        scan_row_cnt_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_SCAN_ROW_CNT, 
                                            DEFAULT_SCAN_ROW_CNT);
        if (scan_row_cnt_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker scan_row_cnt_=%ld can't < 0." ,
              scan_row_cnt_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        update_row_cnt_ = TBSYS_CONFIG.getInt(OBSC_SECTION, OBSC_UPDATE_ROW_CNT, 
                                              DEFAULT_UPDATE_ROW_CNT);
        if (update_row_cnt_ < 0)
        {
          TBSYS_LOG(WARN, "syschecker update_row_cnt_=%ld can't < 0." ,
              update_row_cnt_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret && perf_test_ > 0 && 0 == get_row_cnt_
          && 0 == scan_row_cnt_ && 0 == update_row_cnt_)
      {
        TBSYS_LOG(WARN, "specify perf_test_ is true, but not specify op cnt, "
                        "perf_test_=%ld, get_row_cnt_=%ld, scan_row_cnt_=%ld, udpate_row_cnt_=%ld",
          perf_test_, get_row_cnt_, scan_row_cnt_, update_row_cnt_);
        ret = OB_INVALID_ARGUMENT;
      }

      return ret;
    }

    void ObSyscheckerParam::dump_param()
    {

      fprintf(stderr, "root_server: %s\n", to_cstring(root_server_));
      fprintf(stderr, "update_server:%s\n", to_cstring(update_server_));

      for (int64_t i = 0; i < merge_servers_.count(); ++i)
      {
        fprintf(stderr, "merge_server[%ld]: addr:%s\n", i, to_cstring(merge_servers_.at(i)));
      }
      for (int64_t i = 0; i < chunk_servers_.count(); ++i)
      {
        fprintf(stderr, "chunk_server[%ld]: addr:%s\n", i, to_cstring(chunk_servers_.at(i)));
      }

      fprintf(stderr, "    network_time_out: %ld \n"
                      "    write_thread_count: %ld \n"
                      "    read_thread_count: %ld \n"
                      "    syschecker_count: %ld \n"
                      "    syschecker_no: %ld \n"
                      "    specified_read_param: %ld \n"
                      "    operate_full_row: %ld \n"
                      "    stat_dump_interval: %ld \n"
                      "    perf_test_:%ld \n"
                      "    check_result_:%ld \n"
                      "    read_table_type_:%ld \n"
                      "    write_table_type_:%ld \n"
                      "    get_row_cnt_:%ld \n"
                      "    scan_row_cnt_:%ld \n"
                      "    update_row_cnt_:%ld \n",
              network_time_out_, write_thread_count_, read_thread_count_,
              syschecker_count_, syschecker_no_, specified_read_param_, 
              operate_full_row_, stat_dump_interval_, perf_test_,
              check_result_, read_table_type_, write_table_type_, 
              get_row_cnt_, scan_row_cnt_, update_row_cnt_);
    }
  } // end namespace syschecker
} // end namespace oceanbase
