////===================================================================
 //
 // ob_log_mysql_adaptor.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-07-23 by Yubai (yubai.lk@alipay.com) 
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
#include "ob_log_mysql_adaptor.h"
#include "obmysql/ob_mysql_util.h"      // get_mysql_type

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    int ObLogMysqlAdaptor::TableSchemaContainer::init(const ObLogSchema *schema, uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if (NULL == (total_schema = schema) || OB_INVALID_ID == table_id)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(table_id)))
      {
        TBSYS_LOG(ERROR, "get_table_schema fail, table_id=%lu", table_id);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (column_schema_array = total_schema->get_table_schema(table_id, column_number)))
      {
        TBSYS_LOG(ERROR, "get_table_schema fail, table_id=%lu", table_id);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        inited = true;
      }

      return ret;
    }

    ObLogMysqlAdaptor::ObLogMysqlAdaptor() : inited_(false),
                                             mysql_port_(0),
                                             mysql_addr_(NULL),
                                             mysql_user_(NULL),
                                             mysql_password_(NULL),
                                             read_consistency_(NULL),
                                             ob_mysql_(NULL),
                                             schema_version_(OB_INVALID_VERSION),
                                             tc_info_map_(),
                                             table_id_array_(NULL),
                                             allocator_()
    {
      int tmp_ret = OB_SUCCESS;

      allocator_.set_mod_id(ObModIds::OB_LOG_MYSQL_ADAPTOR);

      // Initialize FIFOAllocator in constructor.
      // NOTE: [0.4.2] FIFOAllocator has BUG: It cannot be re-initialized after being destroyed.
      // So we have to initialize FIFOAllocator in constructor and destroy it in destructor.
      // TODO: Move this to init() function in [0.5] version
      if (OB_SUCCESS != (tmp_ret = allocator_.init(MYSQL_ADAPTOR_SIZE_TOTAL_LIMIT,
            MYSQL_ADAPTOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(ERROR, "initialize FIFOAllocator fail, ret=%d, total_limit=%ld, "
            "hold_limit=%ld, page_size=%ld",
            tmp_ret, MYSQL_ADAPTOR_SIZE_TOTAL_LIMIT,
            MYSQL_ADAPTOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE);

        abort();
      }
    }

    ObLogMysqlAdaptor::~ObLogMysqlAdaptor()
    {
      destroy();

      // NOTE: Destroy FIFOAllocator to avoid re-initialization
      // TODO: Move this to destroy() function in [0.5] version
      allocator_.destroy();
    }

    int ObLogMysqlAdaptor::get_tsi_mysql_adaptor(ObLogMysqlAdaptor *&mysql_adaptor,
        const ObLogConfig &config,
        const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;

      ObLogMysqlAdaptor *tmp_mysql_adaptor = GET_TSI_MULT(ObLogMysqlAdaptor, TSI_LIBOBLOG_MYSQL_ADAPTOR);
      if (NULL == tmp_mysql_adaptor)
      {
        TBSYS_LOG(ERROR, "GET_TSI_MULT get ObLogMysqlAdaptor fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (! tmp_mysql_adaptor->is_inited())
      {
        ret = tmp_mysql_adaptor->init(config, total_schema);
        if (OB_SUCCESS != ret)
        {
          if (OB_SCHEMA_ERROR == ret)
          {
            TBSYS_LOG(WARN, "initialize MySQL Adaptor with schema (%ld,%ld) fail, ret=%d",
                total_schema->get_version(), total_schema->get_timestamp(), ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "failed to initialize ObLogMysqlAdaptor, ret=%d", ret);
          }

          tmp_mysql_adaptor = NULL;
        }
      }

      if (OB_SUCCESS == ret)
      {
        mysql_adaptor = tmp_mysql_adaptor;
      }

      return ret;
    }

    int ObLogMysqlAdaptor::init(const ObLogConfig &config, const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (mysql_addr_ = config.get_mysql_addr())
              || 0 >= (mysql_port_ = config.get_mysql_port())
              || NULL == (mysql_user_ = config.get_mysql_user())
              || NULL == (mysql_password_ = config.get_mysql_password())
              || NULL == (table_id_array_ = config.get_select_table_id_array())
              || NULL == total_schema)
      {
        TBSYS_LOG(ERROR, "invalid configuration or arguments: mysql_addr=%s, mysql_port=%d "
            "mysql_user=%s, mysql_password=%s, table_id_array_=%p, total_schema=%p",
            NULL == mysql_addr_ ? "" : mysql_addr_,
            mysql_port_,
            NULL == mysql_user_ ? "" : mysql_user_,
            NULL == mysql_password_ ? "" : mysql_password_,
            table_id_array_, total_schema);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_ob_mysql_()))
      {
        TBSYS_LOG(ERROR, "init_ob_mysql_ fail, ret=%d", ret);
      }
      else
      {
        // If read_consistency_ is NULL, we will not provide read consistency
        read_consistency_ = config.get_query_back_read_consistency();
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "Initialize MySQL Adaptor based on schema version=%ld, timestamp=%ld",
            total_schema->get_version(), total_schema->get_timestamp());

        // Create tc_info_map_
        if (0 != tc_info_map_.create(TC_INFO_MAP_SIZE))
        {
          TBSYS_LOG(WARN, "tc_info_map_ create fail, size=%ld", TC_INFO_MAP_SIZE);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, total_schema)))
        {
          TBSYS_LOG(WARN, "parse table selected fail with schema (%ld,%ld), ret=%d",
              total_schema->get_version(), total_schema->get_timestamp(), ret);
        }
        else
        {
          schema_version_ = total_schema->get_version();
          inited_ = true;
        }
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogMysqlAdaptor::destroy()
    {
      inited_ = false;

      destroy_tc_info_map_();

      destroy_ob_mysql_();

      schema_version_ = OB_INVALID_VERSION;

      read_consistency_ = NULL;
      mysql_password_ = NULL;
      mysql_user_ = NULL;
      mysql_addr_ = NULL;
      mysql_port_ = 0;

      table_id_array_ = NULL;

      ob_mysql_thread_end();
      TBSYS_LOG(INFO, "Thread %lu: mysql adaptor calls mysql_thread_end", pthread_self());
    }

    bool ObLogMysqlAdaptor::is_time_type(const ObObjType type)
    {
      bool bret = false;
      if (ObDateTimeType == type
          || ObPreciseDateTimeType == type
          || ObCreateTimeType == type
          || ObModifyTimeType == type)
      {
        bret = true;
      }
      return bret;
    }

    int ObLogMysqlAdaptor::operator ()(const char *tb_name, const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = NULL;

      if (NULL == tb_name || NULL == total_schema)
      {
        TBSYS_LOG(ERROR, "invalid argument: tb_name=%s, total_schema=%p",
            NULL == tb_name ? "NULL" : tb_name, total_schema);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(tb_name)))
      {
        TBSYS_LOG(WARN, "get_table_schema fail, tb_name=%s", tb_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = add_tc_info_(table_schema->get_table_id(), total_schema)))
      {
        if (OB_SCHEMA_ERROR != ret)
        {
          TBSYS_LOG(ERROR, "add_tc_info_ fail, tb_name=%s, table_id=%lu, total_schema=%p, ret=%d",
              tb_name, table_schema->get_table_id(), total_schema, ret);
        }
      }

      return ret;
    }

    int ObLogMysqlAdaptor::query_whole_row(const uint64_t table_id,
        const common::ObRowkey &rowkey,
        const ObLogSchema *asked_schema,
        const IObLogColValue *&list)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_INVALID_ID == table_id || NULL == asked_schema)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid arguments, table_id=%lu, asked_schema=%p", table_id, asked_schema);
      }
      else if (OB_INVALID_VERSION == schema_version_ || asked_schema->get_version() > schema_version_)
      {
        // Update all TCInfo based on new schema
        if (OB_SUCCESS != (ret = update_all_tc_info_(asked_schema)))
        {
          if (OB_SCHEMA_ERROR != ret)
          {
            TBSYS_LOG(ERROR, "update_all_tc_info_ fail, ret=%d", ret);
          }

          // NOTE: Schema version should be set as invalid value because TCInfo data has been
          // in uncertain state.
          schema_version_ = OB_INVALID_VERSION;
        }
        else
        {
          schema_version_ = asked_schema->get_version();
        }
      }
      else if (asked_schema->get_version() < schema_version_)
      {
        TBSYS_LOG(ERROR, "invalid arguments: asked schema version %ld is less than local schema version %ld",
            asked_schema->get_version(), schema_version_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = query_data_(table_id, rowkey, asked_schema, list);
        if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret && OB_SCHEMA_ERROR != ret)
        {
          TBSYS_LOG(ERROR, "query_data_ fail, table_id=%lu, rowkey='%s', ret=%d", table_id, to_cstring(rowkey), ret);
        }
      }

      if (OB_SCHEMA_ERROR == ret)
      {
        TBSYS_LOG(DEBUG, "query data fail with schema version=%ld, timestamp=%ld. table_id=%lu, "
            "schema should be updated.",
            asked_schema->get_version(), asked_schema->get_timestamp(), table_id);
      }

      return ret;
    }

    int ObLogMysqlAdaptor::query_data_(const uint64_t table_id,
        const common::ObRowkey &rowkey,
        const ObLogSchema *asked_schema,
        const IObLogColValue *&list)
    {
      OB_ASSERT(inited_ && OB_INVALID_ID != table_id && NULL != asked_schema
          && OB_INVALID_VERSION != schema_version_
          && asked_schema->get_version() == schema_version_);

      int ret = OB_SUCCESS;
      int32_t retry_times = 0;
      TCInfo *tc_info = NULL;

      while (MAX_MYSQL_FAIL_RETRY_TIMES >= ++retry_times)
      {
        tc_info = NULL;
        if (OB_SUCCESS != (ret = get_tc_info_(table_id, tc_info)))
        {
          TBSYS_LOG(ERROR, "get_tc_info fail, table_id=%lu", table_id);
          break;
        }
        else if (OB_NEED_RETRY != (ret = execute_query_(tc_info, rowkey)))
        {
          break;
        }

        if (MAX_MYSQL_FAIL_RETRY_TIMES <= retry_times)
        {
          TBSYS_LOG(ERROR, "execute_query_ fail, table_id=%lu, re-prepare retry times=%d/%d",
              table_id, retry_times, MAX_MYSQL_FAIL_RETRY_TIMES);

          // 重试固定次数后仍然失败，则强制要求外部继续重试
          ret = OB_NEED_RETRY;
          break;
        }

        // Re-Prepare when execute fails
        if (OB_SUCCESS != (ret = update_all_tc_info_(asked_schema)))
        {
          if (OB_SCHEMA_ERROR != ret)
          {
            TBSYS_LOG(ERROR, "fail to re-prepare, update_all_tc_info_ fail, ret=%d", ret);
          }

          // NOTE: Schema version should be set as invalid value because TCInfo data has been
          // in uncertain state.
          schema_version_ = OB_INVALID_VERSION;
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (1 < retry_times)
        {
          TBSYS_LOG(WARN, "execute_query_ success after retry times=%d/%d", retry_times, MAX_MYSQL_FAIL_RETRY_TIMES);
        }

        list = tc_info->res_list;
      }
      else if (OB_ENTRY_NOT_EXIST != ret && OB_SCHEMA_ERROR != ret)
      {
        TBSYS_LOG(WARN, "execute_query_ fail, retry times=%d/%d, table_id=%lu, rowkey='%s', ret=%d",
            retry_times, MAX_MYSQL_FAIL_RETRY_TIMES, table_id, to_cstring(rowkey), ret);
      }

      return ret;
    }

    int ObLogMysqlAdaptor::add_tc_info_(uint64_t table_id, const ObLogSchema *total_schema)
    {
      OB_ASSERT(OB_INVALID_ID != table_id && NULL != total_schema);

      int ret = OB_SUCCESS;
      int hash_ret = 0;
      TCInfo *tc_info = NULL;

      // Construct a new TCInfo
      if (OB_SUCCESS != (ret = construct_tc_info_(tc_info, table_id, total_schema)))
      {
        if (OB_SCHEMA_ERROR != ret)
        {
          TBSYS_LOG(ERROR, "construct_tc_info_ fail, table_id=%lu, tc_info=%p, total_schema=%p, ret=%d",
              table_id, tc_info, total_schema, ret);
        }

        tc_info = NULL;
      }
      // Fill TCInfoMap and return error if exists
      else if (hash::HASH_INSERT_SUCC != (hash_ret = tc_info_map_.set(table_id, tc_info)))
      {
        if (hash::HASH_EXIST == hash_ret)
        {
          tc_info = NULL;
        }
        if (-1 == (hash_ret = tc_info_map_.set(table_id, tc_info, 1)))
        {
          TBSYS_LOG(ERROR, "set tc_info_map_ fail, table_id=%lu, tc_info=%p, hash_ret=%d",
              table_id, tc_info, hash_ret);
        }

        destruct_tc_info_(tc_info);
        tc_info = NULL;
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        TBSYS_LOG(DEBUG, "MYSQL ADAPTOR: ADD TABLE=%lu TCInfo", table_id);
      }

      return ret;
    }

    int ObLogMysqlAdaptor::update_all_tc_info_(const ObLogSchema *total_schema)
    {
      OB_ASSERT(inited_ && NULL != total_schema);

      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "MYSQL ADAPTOR: UPDATE ALL TCInfo, TABLE COUNT=%ld, SCHEMA=%ld",
          table_id_array_->count(), total_schema->get_version());

      // NOTE: First destruct all TCInfo
      destruct_all_tc_info_();

      // NOTE: Then destroy ObSQLMySQL handle
      destroy_ob_mysql_();

      // NOTE: Then re-init ObSQLMySQL handle
      if (OB_SUCCESS != (ret = init_ob_mysql_()))
      {
        TBSYS_LOG(ERROR, "init_ob_mysql_ fail, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "MYSQL ADAPTOR: RE-INIT ObSQLMySQL HANDLE SUCCESS");

        for (int64_t index = 0; OB_SUCCESS == ret && index < table_id_array_->count(); index++)
        {
          uint64_t table_id = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = table_id_array_->at(index, table_id)))
          {
            TBSYS_LOG(ERROR, "get table ID from table_id_array_ fail, index=%ld, count=%ld, ret=%d",
                index, table_id_array_->count(), ret);
            break;
          }
          else if (OB_SUCCESS != (ret = add_tc_info_(table_id, total_schema)))
          {
            if (OB_SCHEMA_ERROR != ret)
            {
              TBSYS_LOG(WARN, "add_tc_info_ fail, table_id=%lu, ret=%d", table_id, ret);
            }

            break;
          }
        }
      }

      return ret;
    }

    int ObLogMysqlAdaptor::get_tc_info_(const uint64_t table_id, TCInfo *&tc_info)
    {
      OB_ASSERT(OB_INVALID_ID != table_id && NULL == tc_info);

      int ret = OB_SUCCESS;
      TCInfo *tmp_tc_info = NULL;
      int hash_ret = tc_info_map_.get(table_id, tmp_tc_info);
      if (hash::HASH_NOT_EXIST == hash_ret)
      {
        // TODO: Support drop/create the same table
        TBSYS_LOG(ERROR, "table ID=%lu does not exist", table_id);
        ret = OB_NOT_SUPPORTED;
      }
      else if (-1 == hash_ret || NULL == tmp_tc_info)
      {
        TBSYS_LOG(ERROR, "get TCInfo from tc_info_map_ fail, table_id=%lu, tc_info=%p, hash_ret=%d",
            table_id, tmp_tc_info, hash_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        tc_info = tmp_tc_info;
      }

      return ret;
    }

    int ObLogMysqlAdaptor::set_mysql_stmt_params_(TCInfo *tc_info, const common::ObRowkey &rowkey)
    {
      OB_ASSERT(NULL != tc_info);

      int ret = OB_SUCCESS;

      if (rowkey.get_obj_cnt() != tc_info->rk_column_num)
      {
        TBSYS_LOG(WARN, "invalid data: rowkey column number not match, rowkey number in mutator=%ld, "
            "rowkey number in schema=%ld, rowkey='%s', ps sql='%s'",
            rowkey.get_obj_cnt(), tc_info->rk_column_num, to_cstring(rowkey), tc_info->sql);

        ret = OB_INVALID_DATA;
      }
      else
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey.get_obj_cnt(); i++)
        {
          tc_info->params_is_null[i] = 0;
          ObObjType obj_type = rowkey.get_obj_ptr()[i].get_type();

          int32_t mysql_type = INT32_MAX;
          uint8_t num_decimals = 0;
          uint32_t length = 0;
          if (OB_SUCCESS == (ret = obmysql::ObMySQLUtil::get_mysql_type(
                  obj_type,
                  (obmysql::EMySQLFieldType&)mysql_type,
                  num_decimals,
                  length)))
          {
            tc_info->params[i].is_null = &(tc_info->params_is_null[i]);
            tc_info->params[i].buffer_type = (enum_field_types)mysql_type;
          }

          if (ObNullType == obj_type)
          {
            tc_info->params_is_null[i] = 1;
            tc_info->params[i].buffer = NULL;
            tc_info->params[i].buffer_length = 0;
            tc_info->params[i].buffer_type = MYSQL_TYPE_NULL;
          }
          else if (is_time_type(obj_type))
          {
            int64_t tm_raw = *(int64_t*)rowkey.get_obj_ptr()[i].get_data_ptr();
            time_t tm_s = (time_t)tm_raw;
            int64_t usec = 0;
            if (ObDateTimeType != rowkey.get_obj_ptr()[i].get_type())
            {
              tm_s = (time_t)(tm_raw / 1000000);
              usec = tm_raw % 1000000;
            }
            struct tm tm;
            localtime_r(&tm_s, &tm);
            tc_info->tm_data[i].year = tm.tm_year + 1900;
            tc_info->tm_data[i].month = tm.tm_mon + 1;
            tc_info->tm_data[i].day = tm.tm_mday;
            tc_info->tm_data[i].hour = tm.tm_hour;
            tc_info->tm_data[i].minute = tm.tm_min;
            tc_info->tm_data[i].second = tm.tm_sec;
            tc_info->tm_data[i].second_part = usec;
            tc_info->params[i].buffer = &tc_info->tm_data[i];
            tc_info->params[i].buffer_length = sizeof(tc_info->tm_data[i]);
          }
          else if (ObIntType == obj_type || ObVarcharType == obj_type)
          {
            tc_info->params[i].buffer = const_cast<void*>(rowkey.get_obj_ptr()[i].get_data_ptr());
            tc_info->params[i].buffer_length = rowkey.get_obj_ptr()[i].get_data_length();
          }
          else
          {
            TBSYS_LOG(ERROR, "unknown rowkey column type=%s,%d, not support",
                ob_obj_type_str(obj_type), obj_type);
            ret = OB_NOT_SUPPORTED;
            break;
          }
        }
      }

      return ret;
    }

    int ObLogMysqlAdaptor::execute_query_(TCInfo *tc_info, const common::ObRowkey &rowkey)
    {
      OB_ASSERT(NULL != tc_info);

      int ret = OB_SUCCESS;
      int mysql_ret = 0;

      if (OB_SUCCESS != (ret = set_mysql_stmt_params_(tc_info, rowkey)))
      {
        TBSYS_LOG(ERROR, "set_mysql_stmt_params_ fail, rowkey='%s', ps sql='%s', ret=%d",
            to_cstring(rowkey), tc_info->sql, ret);
      }
      else if (0 != ob_mysql_stmt_bind_param(tc_info->stmt, tc_info->params))
      {
        TBSYS_LOG(WARN, "mysql_stmt_bind_param fail, errno=%u, %s",
            ob_mysql_stmt_errno(tc_info->stmt), ob_mysql_stmt_error(tc_info->stmt));

        ret = OB_ERR_UNEXPECTED;
      }
      //TODO oceanbase crash retry
      else if (0 != TIMED_FUNC(ob_mysql_stmt_execute, 1000000, tc_info->stmt))
      {
        TBSYS_LOG(WARN, "mysql_stmt_execute fail, errno=%u, %s",
            ob_mysql_stmt_errno(tc_info->stmt), ob_mysql_stmt_error(tc_info->stmt));

        if (is_schema_error_(ob_mysql_stmt_errno(tc_info->stmt)))
        {
          // TCInfo should be updated with newer schema
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          // NOTE: Retry when falling into any other error state.
          ret = OB_NEED_RETRY;
        }
      }
      else if (0 != ob_mysql_stmt_bind_result(tc_info->stmt, tc_info->res_idx))
      {
        TBSYS_LOG(WARN, "mysql_stmt_bind_result fail, errno=%u, %s",
            ob_mysql_stmt_errno(tc_info->stmt), ob_mysql_stmt_error(tc_info->stmt));

        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != (mysql_ret = ob_mysql_stmt_fetch(tc_info->stmt)))
      {
        if (MYSQL_NO_DATA == mysql_ret)
        {
          // NOTE: Query back empty data
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (MYSQL_DATA_TRUNCATED == mysql_ret)
        {
          ret = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "invalid data: query back results are truncated. rowkey='%s', ps sql='%s'",
              to_cstring(rowkey), tc_info->sql);

          TBSYS_LOG(INFO, "************** TRUNCATED DATA INFORMATION BEGIN **************");
          TBSYS_LOG(INFO, "ROWKEY='%s'", to_cstring(rowkey));
          TBSYS_LOG(INFO, "SQL='%s'", tc_info->sql);
          int64_t index = 1;
          IObLogColValue *res = tc_info->res_list;
          std::string res_str;
          while (NULL != res)
          {
            if (! res->isnull())
            {
              res->to_str(res_str);
            }
            else
            {
              res_str = "";
            }

            TBSYS_LOG(INFO, "COLUMN[%ld]: BUF_LEN=%ld, DATA_LEN=%ld, DATA%s%s",
                index, res->get_buffer_length(), res->get_length(),
                res->isnull() ? " is " : "=",
                res->isnull() ? "null" : res_str.c_str());

            res = res->get_next();
            index++;
          }
          TBSYS_LOG(INFO, "************** TRUNCATED DATA INFORMATION END **************");
        }
        else
        {
          TBSYS_LOG(WARN, "mysql_stmt_fetch fail, mysql_ret=%d, errno=%u, %s",
              mysql_ret, ob_mysql_stmt_errno(tc_info->stmt), ob_mysql_stmt_error(tc_info->stmt));

          ret = OB_ERR_UNEXPECTED;
        }
      }

      // NOTE: Releases memory associated with the result set produced by execution of the prepared statement.
      ob_mysql_stmt_free_result(tc_info->stmt);

      return ret;
    }

    int ObLogMysqlAdaptor::construct_tc_info_(TCInfo *&tc_info, uint64_t table_id, const ObLogSchema *total_schema)
    {
      OB_ASSERT(NULL == tc_info && OB_INVALID_ID != table_id && NULL != total_schema && NULL != ob_mysql_);

      int ret = OB_SUCCESS;
      TCInfo *tmp_tc_info = NULL;
      TableSchemaContainer tsc;
      tsc.reset();

      if (NULL == (tmp_tc_info = (TCInfo *)allocator_.alloc(sizeof(TCInfo))))
      {
        TBSYS_LOG(ERROR, "alloc TCInfo fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = tsc.init(total_schema, table_id)))
      {
        TBSYS_LOG(ERROR, "set TableSchemaContainer fail, table_id=%lu, ret=%d", table_id, ret);
      }
      else
      {
        (void)memset(tmp_tc_info, 0, sizeof(TCInfo));

        // Initialize PS SQL
        if (OB_SUCCESS != (ret = init_ps_sql_(tmp_tc_info->sql,
                TCInfo::PS_SQL_BUFFER_SIZE,
                tsc)))
        {
          TBSYS_LOG(ERROR, "init_ps_sql_ fail, table_id=%lu, ret=%d", table_id, ret);
        }
        else if (OB_SUCCESS != (ret = init_mysql_stmt_(tmp_tc_info->stmt,
                tmp_tc_info->sql,
                static_cast<int64_t>(strlen(tmp_tc_info->sql)))))
        {
          if (OB_SCHEMA_ERROR != ret)
          {
            TBSYS_LOG(ERROR, "init_mysql_stmt_ fail, sql=%s, ret=%d", tmp_tc_info->sql, ret);
          }
          tmp_tc_info->stmt = NULL;
        }
        else if (OB_SUCCESS != (ret = init_mysql_params_(tmp_tc_info->params,
                tmp_tc_info->tm_data,
                tmp_tc_info->params_is_null,
                tsc)))
        {
          TBSYS_LOG(ERROR, "init_mysql_params_ fail, ret=%d", ret);
          tmp_tc_info->params = NULL;
          tmp_tc_info->tm_data = NULL;
          tmp_tc_info->params_is_null = NULL;
        }
        else if (OB_SUCCESS != (ret = init_mysql_result_(tmp_tc_info->res_idx,
                tmp_tc_info->res_list,
                tsc)))
        {
          TBSYS_LOG(ERROR, "init_mysql_result_ fail, ret=%d", ret);
          tmp_tc_info->res_idx = NULL;
          tmp_tc_info->res_list = NULL;
        }
        else
        {
          tmp_tc_info->rk_column_num = tsc.table_schema->get_rowkey_info().get_size();
        }
      }

      if (NULL != tmp_tc_info)
      {
        if (OB_SUCCESS != ret)
        {
          destruct_tc_info_(tmp_tc_info);
          tmp_tc_info = NULL;
        }
        else
        {
          tc_info = tmp_tc_info;
        }
      }

      return ret;
    }

    void ObLogMysqlAdaptor::destruct_tc_info_(TCInfo *tc_info)
    {
      if (NULL != tc_info)
      {
        // Destroy mysql result
        destroy_mysql_result_(tc_info->res_idx, tc_info->res_list);
        tc_info->res_idx = NULL;
        tc_info->res_list = NULL;

        // Destroy mysql params
        destroy_mysql_params_(tc_info->params, tc_info->tm_data, tc_info->params_is_null);
        tc_info->params = NULL;
        tc_info->tm_data = NULL;
        tc_info->params_is_null = NULL;

        // Destroy mysql stmt
        destroy_mysql_stmt_(tc_info->stmt);
        tc_info->stmt = NULL;

        // Free TCInfo
        tc_info->clear();
        allocator_.free(reinterpret_cast<char *>(tc_info));
        tc_info = NULL;
      }
    }

    int ObLogMysqlAdaptor::init_ps_sql_(char *sql, int64_t size, const TableSchemaContainer &tsc)
    {
      OB_ASSERT(0 < size && tsc.inited);

      int ret = OB_SUCCESS;
      int column_num = tsc.column_number;
      const ObLogSchema *total_schema = tsc.total_schema;
      const ObColumnSchemaV2 *column_schema = tsc.column_schema_array;
      const ObTableSchema *table_schema = tsc.table_schema;
      uint64_t table_id = tsc.table_schema->get_table_id();

      int64_t pos = 0;
      if (NULL != read_consistency_)
      {
        // Add read consistency
        databuff_printf(sql, size, pos, "select /*+read_consistency(%s)*/ ", read_consistency_);
      }
      else
      {
        databuff_printf(sql, size, pos, "select ");
      }

      for (int64_t i = 0; i < column_num; i++)
      {
        databuff_printf(sql, size, pos, "%s", column_schema[i].get_name());
        if (i < (column_num - 1))
        {
          databuff_printf(sql, size, pos, ",");
        }
        else
        {
          databuff_printf(sql, size, pos, " ");
        }
      }

      databuff_printf(sql, size, pos, "from %s where ", table_schema->get_table_name());

      for (int64_t i = 0; i < table_schema->get_rowkey_info().get_size(); i++)
      {
        uint64_t column_id = table_schema->get_rowkey_info().get_column(i)->column_id_;
        const ObColumnSchemaV2 *rk_column_schema = total_schema->get_column_schema(table_id, column_id);
        databuff_printf(sql, size, pos, "%s=?", rk_column_schema->get_name());
        if (i < (table_schema->get_rowkey_info().get_size() - 1))
        {
          databuff_printf(sql, size, pos, " and ");
        }
        else
        {
          databuff_printf(sql, size, pos, ";");
        }
      }

      TBSYS_LOG(INFO, "build sql succ, table_id=%lu [%s]", table_id, sql);

      return ret;
    }

    int ObLogMysqlAdaptor::init_mysql_stmt_(MYSQL_STMT *&stmt, const char *sql, int64_t sql_len)
    {
      OB_ASSERT(NULL != sql && 0 < sql_len && NULL != ob_mysql_);

      int ret = OB_SUCCESS;
      MYSQL_STMT *tmp_stmt = NULL;

      if (NULL == (tmp_stmt = ob_mysql_stmt_init(ob_mysql_)))
      {
        TBSYS_LOG(WARN, "ob_mysql_stmt_init_ fail, errno=%u, %s",
            ob_mysql_errno(ob_mysql_), ob_mysql_error(ob_mysql_));
        // NOTE: Retry when ob_mysql_stmt_init fail
        ret = OB_NEED_RETRY;
      }
      else if (0 != ob_mysql_stmt_prepare(tmp_stmt, sql, sql_len))
      {
        TBSYS_LOG(WARN, "ob_mysql_stmt_prepare fail, errno=%u, %s",
            ob_mysql_stmt_errno(tmp_stmt), ob_mysql_stmt_error(tmp_stmt));

        if (is_schema_error_(ob_mysql_stmt_errno(tmp_stmt)))
        {
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          // 如果Prepare失败，则强制要求重试
          ret = OB_NEED_RETRY;
        }
      }

      if (OB_SUCCESS != ret && NULL == tmp_stmt)
      {
        destroy_mysql_stmt_(tmp_stmt);
        tmp_stmt = NULL;
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
      else
      {
        stmt = tmp_stmt;
      }

      return ret;
    }

    void ObLogMysqlAdaptor::destroy_mysql_stmt_(MYSQL_STMT *stmt)
    {
      if (NULL != stmt)
      {
        ob_mysql_stmt_close(stmt);
        stmt = NULL;
      }
    }

    int ObLogMysqlAdaptor::init_mysql_params_(MYSQL_BIND *&params,
        MYSQL_TIME *&tm_data,
        my_bool *&params_is_null,
        const TableSchemaContainer &tsc)
    {
      OB_ASSERT(tsc.inited);

      MYSQL_BIND *tmp_params = NULL;
      MYSQL_TIME *tmp_tm_data = NULL;
      my_bool *tmp_params_is_null = NULL;

      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = tsc.table_schema;
      int64_t rk_num = table_schema->get_rowkey_info().get_size();

      if (NULL == (tmp_params = (MYSQL_BIND*)allocator_.alloc(sizeof(MYSQL_BIND) * rk_num)))
      {
        TBSYS_LOG(WARN, "alloc params fail, size=%ld", rk_num);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (tmp_tm_data = (MYSQL_TIME*)allocator_.alloc(sizeof(MYSQL_TIME) * rk_num)))
      {
        TBSYS_LOG(WARN, "alloc tm_data fail, size=%ld", rk_num);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (tmp_params_is_null = (my_bool*)allocator_.alloc(sizeof(my_bool) * rk_num)))
      {
        TBSYS_LOG(WARN, "alloc params_is_null fail, size=%ld", rk_num);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        (void)memset(tmp_params, 0, sizeof(MYSQL_BIND) * rk_num);
        (void)memset(tmp_tm_data, 0, sizeof(MYSQL_TIME) * rk_num);
        (void)memset(tmp_params_is_null, 0, sizeof(my_bool) * rk_num);

        for (int64_t i = 0; OB_SUCCESS == ret && i < rk_num; i++)
        {
          int32_t mysql_type = INT32_MAX;
          uint8_t num_decimals = 0;
          uint32_t length = 0;
          if (OB_SUCCESS == (ret = obmysql::ObMySQLUtil::get_mysql_type(
                  table_schema->get_rowkey_info().get_column(i)->type_,
                  (obmysql::EMySQLFieldType&)mysql_type,
                  num_decimals,
                  length)))
          {
            tmp_params[i].is_null = &(tmp_params_is_null[i]);
            tmp_params[i].buffer_type = (enum_field_types)mysql_type;
          }
          else
          {
            TBSYS_LOG(WARN, "obmysql::ObMySQLUtil::get_mysql_type():  "
                "trans ob_type=%d to mysql_type=%d fail, ret=%d",
                table_schema->get_rowkey_info().get_column(i)->type_, mysql_type, ret);
            break;
          }
        }
      }

      if (OB_SUCCESS != ret || (NULL == tmp_params || NULL == tmp_tm_data))
      {
        destroy_mysql_params_(tmp_params, tmp_tm_data, tmp_params_is_null);
        tmp_params = NULL;
        tmp_tm_data = NULL;
        tmp_params_is_null = NULL;

        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
      else
      {
        params = tmp_params;
        tm_data = tmp_tm_data;
        params_is_null = tmp_params_is_null;
      }

      return ret;
    }

    void ObLogMysqlAdaptor::destroy_mysql_params_(MYSQL_BIND *params, MYSQL_TIME *tm_data, my_bool *params_is_null)
    {
      if (NULL != params_is_null)
      {
        allocator_.free(reinterpret_cast<char *>(params_is_null));
        params_is_null = NULL;
      }

      if (NULL != tm_data)
      {
        allocator_.free(reinterpret_cast<char *>(tm_data));
        tm_data = NULL;
      }

      if (NULL != params)
      {
        allocator_.free(reinterpret_cast<char *>(params));
        params = NULL;
      }
    }

    int ObLogMysqlAdaptor::init_mysql_result_(MYSQL_BIND *&res_idx,
        IObLogColValue *&res_list,
        const TableSchemaContainer &tsc)
    {
      OB_ASSERT(NULL == res_idx && NULL == res_list && tsc.inited);

      int ret = OB_SUCCESS;
      MYSQL_BIND *tmp_res_idx = NULL;
      IObLogColValue *tmp_res_list = NULL;
      int32_t column_num = tsc.column_number;
      const ObColumnSchemaV2 *column_schema = tsc.column_schema_array;

      if (NULL == (tmp_res_idx = (MYSQL_BIND*)allocator_.alloc(sizeof(MYSQL_BIND) * column_num)))
      {
        TBSYS_LOG(WARN, "alloc res_idx fail, size=%d", column_num);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        (void)memset(tmp_res_idx, 0, sizeof(MYSQL_BIND) * column_num);
        tmp_res_list = NULL;

        for (int32_t i = column_num - 1; i >= 0; i--)
        {
          IObLogColValue *cv = NULL;
          if (ObIntType == column_schema[i].get_type())
          {
            cv = (IObLogColValue*)allocator_.alloc(sizeof(IntColValue));
            new(cv) IntColValue();
          }
          else if (is_time_type(column_schema[i].get_type()))
          {
            cv = (IObLogColValue*)allocator_.alloc(sizeof(TimeColValue));
            new(cv) TimeColValue();
          }
          else if (ObVarcharType == column_schema[i].get_type())
          {
            // FIXME: OB 0.4中存储的Varchar值长度可能大于其Schema中指定的长度，为了避免数据截断，
            // 此处一律采用Varchar长度最大值代表Varchar长度
            int64_t column_size = OB_MAX_VARCHAR_LENGTH;
#if 0
            int64_t column_size = column_schema[i].get_size();

            if (0 > column_size)
            {
              column_size = OB_MAX_VARCHAR_LENGTH;
            }
#endif
            cv = (IObLogColValue*)allocator_.alloc(sizeof(VarcharColValue) + column_size);
            new(cv) VarcharColValue(column_size);
          }
          else
          {
            TBSYS_LOG(ERROR, "unknown type: %s,%d, not supported",
                ob_obj_type_str(column_schema[i].get_type()), column_schema[i].get_type());
            ret = OB_NOT_SUPPORTED;
            break;
          }

          int32_t mysql_type = INT32_MAX;
          uint8_t num_decimals = 0;
          uint32_t length = 0;
          if (NULL != cv
              && OB_SUCCESS == (ret = obmysql::ObMySQLUtil::get_mysql_type(column_schema[i].get_type(),
                  (obmysql::EMySQLFieldType&)mysql_type,
                  num_decimals,
                  length)))
          {
            tmp_res_idx[i].buffer_type = (enum_field_types)mysql_type;
            tmp_res_idx[i].buffer_length = cv->get_buffer_length();
            tmp_res_idx[i].buffer = cv->get_data_ptr();
            tmp_res_idx[i].length = cv->get_length_ptr();
            tmp_res_idx[i].is_null = cv->get_isnull_ptr();
            TBSYS_LOG(DEBUG, "init cv list, buffer_type=%d buffer_length=%ld buffer=%p length=%p is_null=%p",
                tmp_res_idx[i].buffer_type,
                tmp_res_idx[i].buffer_length,
                tmp_res_idx[i].buffer,
                tmp_res_idx[i].length,
                tmp_res_idx[i].is_null);

            cv->set_mysql_type(mysql_type);
            cv->set_next(tmp_res_list);
            tmp_res_list = cv;
          }
          else
          {
            TBSYS_LOG(WARN, "obmysql::ObMySQLUtil::get_mysql_type():  "
                "trans ob_type=%d to mysql_type=%d fail, ret=%d",
                column_schema[i].get_type(), mysql_type, ret);
            break;
          }
        }
      }

      if (OB_SUCCESS != ret || NULL == tmp_res_idx || NULL == tmp_res_list)
      {
        destroy_mysql_result_(tmp_res_idx, tmp_res_list);
        tmp_res_idx = NULL;
        tmp_res_list = NULL;

        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
      else
      {
        res_idx = tmp_res_idx;
        res_list = tmp_res_list;
      }

      return ret;
    }

    void ObLogMysqlAdaptor::destroy_mysql_result_(MYSQL_BIND *res_idx, IObLogColValue *res_list)
    {
      IObLogColValue *cvs = res_list;

      while (NULL != cvs)
      {
        IObLogColValue *cv = cvs;
        cvs = cvs->get_next();

        cv->~IObLogColValue();
        allocator_.free(reinterpret_cast<char *>(cv));
        cv = NULL;
      }

      if (NULL != res_idx)
      {
        allocator_.free(reinterpret_cast<char *>(res_idx));
        res_idx = NULL;
      }
    }

    void ObLogMysqlAdaptor::destruct_all_tc_info_()
    {
      TBSYS_LOG(DEBUG, "MYSQL ADAPTOR: destruct_all_tc_info");

      TCInfoMap::iterator tc_map_iter;
      for (tc_map_iter = tc_info_map_.begin(); tc_map_iter != tc_info_map_.end(); tc_map_iter++)
      {
        TCInfo *info_iter = tc_map_iter->second;
        if (NULL != info_iter)
        {
          destruct_tc_info_(info_iter);
          info_iter = NULL;
        }

        tc_map_iter->second = NULL;
      }

      tc_info_map_.clear();
    }

    void ObLogMysqlAdaptor::destroy_tc_info_map_()
    {
      destruct_all_tc_info_();
      tc_info_map_.destroy();
    }

    int ObLogMysqlAdaptor::init_ob_mysql_()
    {
      OB_ASSERT(NULL == ob_mysql_ && NULL != mysql_addr_ && NULL != mysql_user_
          && NULL != mysql_password_ && 0 < mysql_port_);

      int ret = OB_SUCCESS;

      TBSYS_LOG(DEBUG, "MYSQL ADAPTOR: init_ob_mysql");

      if (NULL == (ob_mysql_ = ob_mysql_init(NULL)))
      {
        TBSYS_LOG(WARN, "ob_mysql_init fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        int32_t retry_times = 0;

        while (MAX_MYSQL_FAIL_RETRY_TIMES >= ++retry_times)
        {
          if (ob_mysql_ == ob_mysql_real_connect(ob_mysql_,
                mysql_addr_,
                mysql_user_,
                mysql_password_,
                OB_MYSQL_DATABASE,
                mysql_port_,
                NULL, 0))
          {
            break;
          }

          if (MAX_MYSQL_FAIL_RETRY_TIMES <= retry_times)
          {
            TBSYS_LOG(WARN, "ob_mysql_real_connect fail, retry_times=%d/%d, "
                "mysql_addr=%s, mysql_port=%d, errno=%u, %s",
                retry_times, MAX_MYSQL_FAIL_RETRY_TIMES,
                mysql_addr_, mysql_port_, ob_mysql_errno(ob_mysql_), ob_mysql_error(ob_mysql_));

            // 选择连接失败时，要求重试
            ret = OB_NEED_RETRY;
            break;
          }
        }

        if (OB_SUCCESS == ret && 1 < retry_times)
        {
          TBSYS_LOG(WARN, "ob_mysql_real_connect success after retry %d/%d times",
              retry_times, MAX_MYSQL_FAIL_RETRY_TIMES);
        }
      }

      return ret;
    }

    void ObLogMysqlAdaptor::destroy_ob_mysql_()
    {
      TBSYS_LOG(DEBUG, "MYSQL ADAPTOR: destroy_ob_mysql");

      if (NULL != ob_mysql_)
      {
        ob_mysql_close(ob_mysql_);
        ob_mysql_ = NULL;
      }
    }
  }
}

