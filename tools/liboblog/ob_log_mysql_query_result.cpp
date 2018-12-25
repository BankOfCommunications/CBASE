////===================================================================
 //
 // ob_log_mysql_query_result.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-05-13 by XiuMing (wanhong.wwh@alipay.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   MySQL query result used to store query result
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_log_mysql_query_result.h"
#include "ob_log_mysql_adaptor.h"       // IObLogColValue

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogMysqlQueryResult::ObLogMysqlQueryResult() : inited_(false),
                                                     err_code_(OB_SUCCESS),
                                                     rows_(NULL),
                                                     total_row_count_(0),
                                                     valid_row_count_(0),
                                                     all_rows_added_(false),
                                                     all_row_value_filled_(false),
                                                     need_query_row_count_(0),
                                                     query_completed_row_count_(0),
                                                     row_query_time_sum_(0),
                                                     allocator_(),
                                                     string_pool_(NULL),
                                                     cond_(),
                                                     row_array_lock_()
    {
      allocator_.set_mod_id(ObModIds::OB_LOG_MYSQL_QUERY_RESULT);
    }

    ObLogMysqlQueryResult::~ObLogMysqlQueryResult()
    {
      destroy();
    }

    int ObLogMysqlQueryResult::get_tsi_mysql_query_result(ObLogMysqlQueryResult *&query_result,
        int64_t default_row_count,
        ObLogStringPool *string_pool)
    {
      int ret = OB_SUCCESS;

      ObLogMysqlQueryResult *tmp_query_result = GET_TSI_MULT(ObLogMysqlQueryResult, TSI_LIBOBLOG_MYSQL_QUERY_RESULT);
      if (NULL == tmp_query_result)
      {
        TBSYS_LOG(ERROR, "GET_TSI_MULT get ObLogMysqlQueryResult fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (! tmp_query_result->is_inited())
      {
        ret = tmp_query_result->init(default_row_count, string_pool);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize query result fail, default_row_count=%ld, string_pool=%p, ret=%d",
              default_row_count, string_pool, ret);

          tmp_query_result = NULL;
        }
      }

      if (OB_SUCCESS == ret)
      {
        query_result = tmp_query_result;
      }

      return ret;
    }

    int ObLogMysqlQueryResult::init(int64_t default_row_count, ObLogStringPool *string_pool)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= default_row_count || NULL == (string_pool_ = string_pool))
      {
        TBSYS_LOG(ERROR, "invalid argument, default_row_count=%ld, string_pool=%p",
            default_row_count, string_pool);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_SIZE_TOTAL_LIMIT,
              ALLOCATOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(ERROR, "initialize FIFOAllocator fail, ret=%d, total_limit=%ld, hold_limit=%ld, page_size=%ld",
            ret, ALLOCATOR_SIZE_TOTAL_LIMIT, ALLOCATOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE);
      }
      // NOTE: Here use ob_malloc to allocate memory for "rows_" but not use the FIFOAllocator "allocator_".
      // Because "rows_" will not be freed frequently that is not suitable for FIFOAllocator.
      else if (NULL == (rows_ = (ResultRowValue *)ob_malloc(default_row_count * sizeof(ResultRowValue),
              ObModIds::OB_LOG_MYSQL_QUERY_RESULT)))
      {
        TBSYS_LOG(ERROR, "alloc memory for mysql query result row array fail, size=%ld",
            default_row_count * sizeof(ResultRowValue));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        err_code_ = OB_SUCCESS;
        total_row_count_ = default_row_count;
        valid_row_count_ = 0;
        all_rows_added_ = false;
        all_row_value_filled_ = false;
        need_query_row_count_ = 0;
        query_completed_row_count_ = 0;
        row_query_time_sum_ = 0;

        (void)memset(rows_, 0, default_row_count * sizeof(ResultRowValue));

        inited_ = true;
      }

      return ret;
    }

    void ObLogMysqlQueryResult::destroy()
    {
      inited_ = false;

      clear();

      ob_free(rows_);
      rows_ = NULL;

      err_code_ = OB_SUCCESS;
      total_row_count_ = 0;
      valid_row_count_ = 0;
      all_rows_added_ = false;
      all_row_value_filled_ = false;
      need_query_row_count_ = 0;
      query_completed_row_count_ = 0;
      row_query_time_sum_ = 0;

      allocator_.destroy();
      string_pool_ = NULL;
    }

    void ObLogMysqlQueryResult::clear()
    {
      SpinRLockGuard guard(row_array_lock_);

      // Clear all valid rows
      if (NULL != rows_ && 0 < valid_row_count_)
      {
        // Destroy all valid row value
        for (int64_t index = 0; index < valid_row_count_; index++)
        {
          if (ResultRowValue::MYSQL_QUERY_VALUE == rows_[index].value_type_)
          {
            destroy_query_value_list_(rows_[index].value_.query_value_list_);
          }
        }

        (void)memset(rows_, 0, valid_row_count_ * sizeof(ResultRowValue));
      }

      err_code_ = OB_SUCCESS;
      valid_row_count_ = 0;
      all_rows_added_ = false;
      all_row_value_filled_ = false;
      need_query_row_count_ = 0;
      query_completed_row_count_ = 0;
      row_query_time_sum_ = 0;
    }

    int ObLogMysqlQueryResult::get_query_value(int64_t index, ObLogStrColValue *&query_value_list)
    {
      int ret = OB_SUCCESS;

      if (! inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 > index || valid_row_count_ <= index)
      {
        TBSYS_LOG(ERROR, "invalid argument, index=%ld, valid_row_count=%ld", index, valid_row_count_);
        ret = OB_INVALID_ARGUMENT;
      }
      // NOTE: Current result should be ready
      else if (! is_ready())
      {
        TBSYS_LOG(ERROR, "mysql query result is not ready, all_rows_added=%s, all_row_value_filled=%s, "
            "need_query_row_count=%ld, query_completed_row_count=%ld",
            all_rows_added_ ? "Y" : "N", all_row_value_filled_ ? "Y" : "N",
            need_query_row_count_, query_completed_row_count_);

        ret = OB_NEED_RETRY;
      }
      else if (OB_SUCCESS != err_code_)
      {
        // If some rows have error during querying, all rows are considered as invalid.
        ret = err_code_;
        query_value_list = NULL;
        TBSYS_LOG(ERROR, "all query values are invalid. err_code=%d", err_code_);
      }
      else
      {
        SpinRLockGuard guard(row_array_lock_);

        int value_type = rows_[index].value_type_;

        // NOTE: value type should not be REFERENCE_VALUE
        if (ResultRowValue::REFERENCE_VALUE == value_type)
        {
          TBSYS_LOG(ERROR, "unexcepted error: there is reference value in ready result. index=%ld, "
              "valid_row_count=%ld, need_query_row_count=%ld, query_completed_row_count=%ld, "
              "all_rows_added=%s, all_row_value_filled=%s",
              index, valid_row_count_, need_query_row_count_, query_completed_row_count_,
              all_rows_added_ ? "Y" : "N", all_row_value_filled_ ? "Y" : "N");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (ResultRowValue::MYSQL_QUERY_VALUE == value_type)
        {
          query_value_list = rows_[index].value_.query_value_list_;
          ret = rows_[index].err_code_;
        }
        // rows that need not query
        else if (ResultRowValue::INVALID_VALUE == value_type)
        {
          query_value_list = NULL;
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(ERROR, "unknown mysql query result value type: %d", rows_[index].value_type_);
          ret = OB_NOT_SUPPORTED;
        }
      }

      return ret;
    }

    int ObLogMysqlQueryResult::fill_query_value(int64_t index,
        const IObLogColValue *col_value_list,
        int err_code,
        int64_t query_time)
    {
      int ret = OB_SUCCESS;

      SpinRLockGuard guard(row_array_lock_);

      if (! inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 > index || index >= valid_row_count_)
      {
        TBSYS_LOG(ERROR, "invalid argument, index=%ld, valid_row_count=%ld", index, valid_row_count_);
        ret = OB_NOT_INIT;
      }
      else if (ResultRowValue::MYSQL_QUERY_VALUE != rows_[index].value_type_)
      {
        TBSYS_LOG(ERROR, "unexcepted error: the %ldth row type is %d but not MYSQL_QUERY_VALUE(%d)",
            index, rows_[index].value_type_, ResultRowValue::MYSQL_QUERY_VALUE);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        rows_[index].err_code_ = err_code;

        // Update global error code "err_code_" if it has never been set
        // NOTE: OB_ENTRY_NOT_EXIST means query value is empty and it is not a ERROR.
        if (OB_SUCCESS != err_code && OB_ENTRY_NOT_EXIST != err_code)
        {
          // 当全局错误码没有设置，或者全局错误码是OB_NEED_RETRY时，覆盖全局错误码
          // FIXME: OB_NEED_RETRY是最低优先级错误，其他错误都需要退出程序，而OB_NEED_RETRY不需要
          if (OB_SUCCESS == err_code_ || OB_NEED_RETRY == err_code_)
          {
            TBSYS_LOG(INFO, "QUERY_BACK: set query result error code: %d, orignal error code: %d", err_code_, err_code);
            err_code_ = err_code;
          }
        }

        if (NULL != col_value_list)
        {
          // Construct query value list based on queried column value list
          ObLogStrColValue *query_value_list = NULL;
          if (OB_SUCCESS != (ret = construct_query_value_list_(query_value_list, col_value_list)))
          {
            TBSYS_LOG(ERROR, "construct_query_value_list_ fail, ret=%d, col_value_list=%p", ret, col_value_list);
          }
          else
          {
            rows_[index].value_.query_value_list_ = query_value_list;
          }
        }

        ATOMIC_ADD(&row_query_time_sum_, query_time);

        // Update count of rows whose query has been completed
        ATOMIC_INC(&query_completed_row_count_);

        // Signal threads that wait for result rows ready
        cond_.signal();
      }

      return ret;
    }

    int ObLogMysqlQueryResult::add_row(const uint64_t table_id,
        const common::ObRowkey &rowkey,
        const ObDmlType dml_type,
        bool &need_query)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (all_rows_added_)
      {
        TBSYS_LOG(ERROR, "operation not allowed: all rows have been added. no rows could be added any more. "
            "table_id=%lu, rowkey='%s', dml_type=%d",
            table_id, to_cstring(rowkey), dml_type);

        ret = OB_OP_NOT_ALLOW;
      }
      else if (valid_row_count_ >= total_row_count_ && (OB_SUCCESS != (ret = enlarge_row_array_())))
      {
        TBSYS_LOG(ERROR, "enlarge_row_array_ fail, ret=%d", ret);
      }
      else
      {
        SpinRLockGuard guard(row_array_lock_);

        int64_t cur_index = valid_row_count_;

        rows_[cur_index].rowkey_ = &rowkey;
        rows_[cur_index].table_id_ = table_id;
        rows_[cur_index].err_code_ = OB_SUCCESS;

        // Only "REPLACE" and "UPDATE" statements need query
        if (OB_DML_REPLACE == dml_type || OB_DML_UPDATE == dml_type)
        {
          need_query = true;
          int64_t reference_index = -1;

          // NOTE: Rows with same rowkey should be queried only once.
          // Here we try to find the rows with same rowkey that need query.
          for (int64_t index = cur_index - 1; index >= 0; index--)
          {
            // NOTE: rowkey of different tables cannot be compared.
            if (table_id == rows_[index].table_id_ && rowkey == *(rows_[index].rowkey_))
            {
              if (ResultRowValue::MYSQL_QUERY_VALUE == rows_[index].value_type_)
              {
                reference_index = index;
                break;
              }
              else if (ResultRowValue::REFERENCE_VALUE == rows_[index].value_type_)
              {
                reference_index = rows_[index].value_.reference_index_;
                break;
              }
            }
          }

          if (0 <= reference_index)
          {
            rows_[cur_index].value_type_ = ResultRowValue::REFERENCE_VALUE;
            rows_[cur_index].value_.reference_index_ = reference_index;
            need_query = false;
          }
          else
          {
            rows_[cur_index].value_type_ = ResultRowValue::MYSQL_QUERY_VALUE;
            rows_[cur_index].value_.query_value_list_ = NULL;
          }
        }
        // "INSERT" and "DELETE" statements need not query
        else
        {
          need_query = false;
          rows_[cur_index].value_type_ = ResultRowValue::INVALID_VALUE;
        }

        // Update counter of rows that need query
        if (need_query)
        {
          ATOMIC_INC(&need_query_row_count_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        // Update valid row count
        valid_row_count_++;
      }

      return ret;
    }

    int ObLogMysqlQueryResult::wait_for_ready(int64_t timeout)
    {
      int ret = OB_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();

      // Wait for all rows added
      while (! all_rows_added_)
      {
        int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
        if (0 >= wait_time)
        {
          TBSYS_LOG(WARN, "wait for all rows added timeout, timeout=%ld, wait=%ld",
              timeout, timeout - wait_time);
          ret = OB_PROCESS_TIMEOUT;
          break;
        }

        cond_.timedwait(wait_time);
      }

      // Wait for all rows filled with valid values
      if (OB_SUCCESS == ret && ! all_row_value_filled_)
      {
        // Wait for all rows that need query are queried back
        // NOTE: "need_query_row_count_" is valid only after all rows are added
        while (need_query_row_count_ > query_completed_row_count_)
        {
          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 >= wait_time)
          {
            TBSYS_LOG(WARN, "wait for all rows queried back timeout, timeout=%ld, wait=%ld, "
                "need_query_row_count=%ld, query_completed_row_count=%ld",
                timeout, timeout - wait_time, need_query_row_count_, query_completed_row_count_);

            ret = OB_PROCESS_TIMEOUT;
            break;
          }

          cond_.timedwait(wait_time);
        }

        if (OB_SUCCESS == ret)
        {
          // NOTE: So far, all rows that need query are queried back.
          // Now fill all referenced rows with valid value
          if (OB_SUCCESS != (ret = fill_all_reference_rows_()))
          {
            TBSYS_LOG(ERROR, "fill_all_reference_rows_ fail, ret=%d", ret);
          }
          else
          {
            all_row_value_filled_ = true;
          }
        }
      }

      return ret;
    }

    int ObLogMysqlQueryResult::fill_all_reference_rows_()
    {
      OB_ASSERT(inited_ && all_rows_added_ && need_query_row_count_ == query_completed_row_count_);

      int ret = OB_SUCCESS;

      SpinRLockGuard guard(row_array_lock_);

      // Do nothing when query result has error
      if (OB_SUCCESS == err_code_)
      {
        for (int64_t index = 0; index < valid_row_count_; index++)
        {
          if (OB_SUCCESS == rows_[index].err_code_ && ResultRowValue::REFERENCE_VALUE == rows_[index].value_type_)
          {
            int64_t reference_index = rows_[index].value_.reference_index_;

            TBSYS_LOG(DEBUG, "QUERY_BACK: fill reference row[%ld] with row[%ld], table=%lu",
                 index, reference_index, rows_[index].table_id_);

            if (OB_SUCCESS != (ret = copy_row_value_(rows_[index], rows_[reference_index])))
            {
              TBSYS_LOG(ERROR, "copy_row_value_ fail, ret=%d, table_id=%lu, rowkey=%s, index=%ld, reference index=%ld",
                  ret, rows_[index].table_id_, to_cstring(*(rows_[index].rowkey_)), index, reference_index);
              break;
            }
          }
        }
      }

      return ret;
    }

    int ObLogMysqlQueryResult::copy_row_value_(ResultRowValue &dest, const ResultRowValue &src)
    {
      OB_ASSERT(dest.table_id_ == src.table_id_ && *(dest.rowkey_) == *(src.rowkey_) // two row value are same row
          && ResultRowValue::MYSQL_QUERY_VALUE == src.value_type_    // source row value type must be MYSQL_QUERY_VALUE
          && ResultRowValue::REFERENCE_VALUE == dest.value_type_);   // dest row value type must be REFERENCE_VALUE

      int ret = OB_SUCCESS;

      ObLogStrColValue *dest_query_value_list = NULL;
      const ObLogStrColValue *src_col_value = src.value_.query_value_list_;

      if (NULL != src_col_value)
      {
        ObLogStrColValue *last_col_value = NULL;

        // Copy every string column value
        while (NULL != src_col_value)
        {
          ObLogStrColValue *dest_col_value = (ObLogStrColValue *)allocator_.alloc(sizeof(ObLogStrColValue));
          if (NULL == dest_col_value)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(ERROR, "allocate memory for ObLogStrColValue fail, size=%ld", sizeof(ObLogStrColValue));
            break;
          }

          dest_col_value->str_val_ = NULL;
          dest_col_value->next_ = NULL;
          if (NULL != src_col_value->str_val_)
          {
            // NOTE: Allocate string from string pool
            if (NULL == (dest_col_value->str_val_ = (std::string *)string_pool_->alloc()))
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(WARN, "allocate std::string from string pool fail");
              break;
            }

            // Copy string value
            dest_col_value->str_val_->assign(*(src_col_value->str_val_));
          }

          if (NULL != last_col_value)
          {
            last_col_value->next_ = dest_col_value;
          }

          last_col_value = dest_col_value;
          if (NULL == dest_query_value_list)
          {
            dest_query_value_list = dest_col_value;
          }

          src_col_value = src_col_value->next_;
        }
      }

      if (OB_SUCCESS == ret)
      {
        dest.err_code_ = src.err_code_;
        dest.value_type_ = ResultRowValue::MYSQL_QUERY_VALUE;
        dest.value_.query_value_list_ = dest_query_value_list;
      }
      else if (NULL != dest_query_value_list)
      {
        // NOTE: Handle ERROR: Destroy query value list and free string value
        bool free_str_values = true;
        destroy_query_value_list_(dest_query_value_list, free_str_values);
      }

      return ret;
    }

    int ObLogMysqlQueryResult::enlarge_row_array_()
    {
      OB_ASSERT(NULL != rows_ && 0 < total_row_count_);

      // NOTE: Add exclusive lock to prevent other threads reading row array
      SpinWLockGuard guard(row_array_lock_);

      int ret = OB_SUCCESS;
      ResultRowValue *new_rows = NULL;
      int64_t old_count = total_row_count_;
      int64_t new_count = old_count * 2;

      if (NULL == (new_rows = (ResultRowValue *)ob_malloc(new_count * sizeof(ResultRowValue),
              ObModIds::OB_LOG_MYSQL_QUERY_RESULT)))
      {
        TBSYS_LOG(ERROR, "allocate memory for ResultRowValue fail, count=%ld, size=%ld",
            new_count, new_count * sizeof(ResultRowValue));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        // Copy old rows
        (void)memcpy(new_rows, rows_, old_count * sizeof(ResultRowValue));

        // Clear new rows
        (void)memset(new_rows + old_count, 0, (new_count - old_count) * sizeof(ResultRowValue));

        ob_free(rows_);
        rows_ = new_rows;
        total_row_count_ = new_count;
      }

      TBSYS_LOG(INFO, "QUERY_BACK: ENLARGE ROW ARRAY: count(old=%ld,new=%ld,valid=%ld), ret=%d",
          old_count, new_count, valid_row_count_, ret);

      return ret;
    }

    int ObLogMysqlQueryResult::construct_query_value_list_(ObLogStrColValue *&query_value_list,
        const IObLogColValue *col_value_list)
    {
      OB_ASSERT(NULL != col_value_list);

      int ret = OB_SUCCESS;
      const IObLogColValue *col_value = col_value_list;
      ObLogStrColValue *last_str_col_value = NULL;

      query_value_list = NULL;
      while (NULL != col_value)
      {
        ObLogStrColValue *str_col_value = (ObLogStrColValue *)allocator_.alloc(sizeof(ObLogStrColValue));
        if (NULL == str_col_value)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "allocate memory for ObLogStrColValue fail, size=%ld", sizeof(ObLogStrColValue));
          break;
        }

        str_col_value->str_val_ = NULL;
        str_col_value->next_ = NULL;
        if (! col_value->isnull())
        {
          // NOTE: Allocate string from string pool
          if (NULL == (str_col_value->str_val_ = (std::string *)string_pool_->alloc()))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "allocate std::string from string pool fail");
            break;
          }
          col_value->to_str(*(str_col_value->str_val_));
        }

        if (NULL != last_str_col_value)
        {
          last_str_col_value->next_ = str_col_value;
        }

        last_str_col_value = str_col_value;
        if (NULL == query_value_list)
        {
          query_value_list = str_col_value;
        }

        col_value = col_value->get_next();
      }

      if (OB_SUCCESS != ret && NULL != query_value_list)
      {
        // NOTE: Here we must free string values back to string pool or else they will not be reused.
        bool free_str_values = true;
        destroy_query_value_list_(query_value_list, free_str_values);
        query_value_list = NULL;
      }

      return ret;
    }

    void ObLogMysqlQueryResult::destroy_query_value_list_(ObLogStrColValue *query_value_list, bool free_str_values)
    {
      ObLogStrColValue *query_value = query_value_list;
      while (NULL != query_value)
      {
        ObLogStrColValue *next = query_value->next_;

        // Free string value back to string pool
        if (OB_UNLIKELY(free_str_values && (NULL != query_value->str_val_)))
        {
          string_pool_->free(static_cast<ObLogString*>(query_value->str_val_));
        }

        query_value->str_val_ = NULL;
        query_value->next_ = NULL;
        allocator_.free(query_value);
        query_value = next;
      }
    }
  }
}
