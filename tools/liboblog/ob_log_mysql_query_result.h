////===================================================================
 //
 // ob_log_mysql_query_result.h liboblog / Oceanbase
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

#ifndef OCEANBASE_LIBOBLOG_MYSQL_QUERY_RESULT_H_
#define OCEANBASE_LIBOBLOG_MYSQL_QUERY_RESULT_H_

#include "common/ob_define.h"
#include "common/ob_queue_thread.h"   // ObCond
#include "common/ob_rowkey.h"         // ObRowkey
#include "common/ob_fifo_allocator.h" // FIFOAllocator
#include "ob_log_common.h"            // ObLogStringPool

namespace oceanbase
{
  namespace liboblog
  {
    struct ObLogStrColValue
    {
      std::string *str_val_;
      ObLogStrColValue *next_;
    };

    class IObLogColValue;

    /// ObLogMysqlQueryResult is a query result container which contains all
    /// rows' query value within a transaction.
    ///
    /// @note ObLogMysqlQueryResult object should be thread specific.
    ///       1. Its value can be filled concurrently by multiple query threads in IObLogMysqlQueryEngine
    ///          by calling "fill_query_value()"
    ///       2. The other public functional methods provided can only be called by same single thread just
    ///          like the following way:
    ///          1) clear result:                            clear()
    ///          2) add rows of target transaction(mutator): add_row()
    ///          3) mark all rows added:                     mark_all_rows_added()
    ///          4) wait for all rows' value filled:         wait_for_ready()
    ///          5) get specific row's query value:          get_query_value()
    ///
    class ObLogMysqlQueryResult
    {
      struct ResultRowValue
      {
        // Table ID and rowkey decide one unique ROW
        uint64_t table_id_;               ///< current table id
        const common::ObRowkey *rowkey_;  ///< current row key

        enum ValueType
        {
          INVALID_VALUE = 0,     ///< value type of rows that need not query and value is invalid
          MYSQL_QUERY_VALUE,     ///< value type of rows that need query and value is query value
          REFERENCE_VALUE,       ///< value type of rows that need query and value refers to row with same rowkey
        } value_type_;

        union
        {
          ObLogStrColValue *query_value_list_; ///< MYSQL_QUERY_VALUE type
          int64_t reference_index_;            ///< REFERENCE_VALUE type
        } value_;

        // OB_ENTRY_NOT_EXIST: means query value is empty
        // OB_SCHEMA_ERROR: means schema is invalid during querying
        // other error codes: mean query fails
        int err_code_;     ///< error code occured during querying
      };

      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t ALLOCATOR_SIZE_TOTAL_LIMIT = 4L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_SIZE_HOLD_LIMIT = ALLOCATOR_PAGE_SIZE * 2;

      public:
        ObLogMysqlQueryResult();
        virtual ~ObLogMysqlQueryResult();

      public:
        /// @brief Get thread specific instance of mysql query result
        /// @param[out] query_result returned thread specific query result
        /// @param[in] default_row_count default row count in result
        /// @param[in] string_pool string pool used to allocate std::string object
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        static int get_tsi_mysql_query_result(ObLogMysqlQueryResult *&query_result,
            int64_t default_row_count,
            ObLogStringPool *string_pool);

      public:
        /// @brief Initialize query result
        /// @param default_row_count default row count in result
        /// @param string_pool string pool used to allocate std::string object
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init(int64_t default_row_count, ObLogStringPool *string_pool);

        /// @brief Destroy query result
        void destroy();

        /// @brief Clear result data
        void clear();

        /// @brief Get query value from "ready" result
        /// @param[in] index target row index
        /// @param[out] query_value_list returned query value list
        /// @retval OB_SUCCESS on success
        /// @retval OB_ENTRY_NOT_EXIST when current row is queried empty value
        /// @retval OB_NEED_RETRY when current result is not ready
        /// @retval other error code when query value is invalid
        /// @note
        ///   1. Caller had better use this function to get query value of the rows that need query.
        ///      As to rows such as INSERT or DELETE statements that need not query, "query_value_list" will
        ///      return NULL and "err_code" will return OB_SUCCESS.
        ///   2. If some rows have errors, all rows are considered as invalid
        int get_query_value(int64_t index, ObLogStrColValue *&query_value_list);

        /// @brief Fill query value
        ///        It will convert "IObLogColValue" to "ObLogStrColValue" and store it into target row.
        /// @param index target row index
        /// @param col_value_list queried column value list
        /// @param err_code error code generated during querying
        /// @param query_time query time of target row
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int fill_query_value(int64_t index, const IObLogColValue *col_value_list, int err_code, int64_t query_time);

        /// @brief Add a new row
        /// @param[in] table_id target table ID
        /// @param[in] rowkey target row key
        /// @param[in] dml_type row dml type
        /// @param[out] need_query returned bool flag used to mark whether current row need query
        /// @retval OB_SUCCESS on success
        /// @retval OB_OP_NOT_ALLOW when "all_row_added" has been marked before adding current row
        /// @retval other error code on fail
        /// @note 1. This function must be called before calling "mark_all_rows_added()".
        ///       2. Rows must be added in sequence
        int add_row(const uint64_t table_id,
            const common::ObRowkey &rowkey,
            const common::ObDmlType dml_type,
            bool &need_query);

        /// @brief Wait for result ready that all rows are filled with valid values
        /// @param timeout max wait time
        /// @retval OB_PROCESS_TIMEOUT when time is out
        /// @retval OB_SUCCESS when all result rows are ready
        int wait_for_ready(int64_t timeout);

        /// @brief Mark all rows added
        /// @note This function makes a promise that no rows will be added subsequently.
        void mark_all_rows_added()
        {
          all_rows_added_ = true;

          // Signal threads which wait for all rows added
          cond_.signal();
        }

        int64_t to_string(char* buf, const int64_t len) const
        {
          int64_t pos = 0;
          common::databuff_printf(buf, len, pos,
              "MysqlQueryResult{err_code=%d, row_count(total=%ld,valid=%ld,need_query=%ld,query_completed=%ld), "
              "all_rows_added=%s, all_row_value_filled=%s, row_query_time_sum=%ld}",
              err_code_, total_row_count_, valid_row_count_, need_query_row_count_, query_completed_row_count_,
              all_rows_added_ ? "Y" : "N", all_row_value_filled_ ? "Y" : "N", row_query_time_sum_);

          return pos;
        }

      public:
        bool is_inited() const { return inited_; }
        bool is_ready() const { return all_rows_added_ && all_row_value_filled_; }

        int get_error_code() const { return err_code_; }
        int64_t get_valid_row_count() const { return valid_row_count_; }
        int64_t get_need_query_row_count() const { return need_query_row_count_; }
        int64_t get_row_query_time_sum() const { return row_query_time_sum_; }

      private:
        /// @brief Construct query value list based on queried column value list
        /// @param[out] query_value_list returned constructed query value list
        /// @param[in] target queried column value list
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int construct_query_value_list_(ObLogStrColValue *&query_value_list,
            const IObLogColValue *col_value_list);

        /// @brief Destroy query value list
        /// @param query_value_list target query value list
        /// @param free_str_values whether to free string values in ObLogStrColValue
        /// @note String value in ObLogStrColValue should not be freed by default.
        ///       They are used and freed outside.
        ///       Only under error situations we can free the string values.
        void destroy_query_value_list_(ObLogStrColValue *query_value_list, bool free_str_values = false);

        /// @brief Enlarge row array
        /// @retval OB_SUCCESS on success
        /// @retval other error code on fail
        /// @note row array will be enlarge the double size: 100->200, 800->1600
        int enlarge_row_array_();

        /// @brief Fill all reference rows with query value based on value of rows that need query
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        /// @note This function should be called after all rows that need query are filled with valid value
        int fill_all_reference_rows_();

        /// @brief Copy src row value to dest row value.
        ///        dest row value type will change to MYSQL_QUERY_VALUE
        /// @param dest target row value
        /// @param src source row value
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        /// @note 1. src and dest contain same rowkey
        ///       2. src row value type must be MYSQL_QUERY_VALUE
        ///       3. dest row value type must be REFERENCE_VALUE
        int copy_row_value_(ResultRowValue &dest, const ResultRowValue &src);

      private:
        bool inited_;
        int err_code_;                               ///< global error code

        ResultRowValue *rows_;                       ///< result row value array
        int64_t total_row_count_;                    ///< total result row count
        int64_t valid_row_count_;                    ///< valid result row count in row array

        volatile bool all_rows_added_;               ///< bool flag used to mark all result rows are added
        volatile bool all_row_value_filled_;         ///< bool flag used_to mark all rows' value are filled
        volatile int64_t need_query_row_count_;      ///< count of rows that need query
        volatile int64_t query_completed_row_count_; ///< count of rows whose query has been completed
        volatile int64_t row_query_time_sum_;        ///< query time sum of all rows that need query

        common::FIFOAllocator allocator_;            ///< FIFO allocator
        ObLogStringPool *string_pool_;               ///< string pool used to alloc string value
        common::ObCond cond_;                        ///< condition variable

        common::SpinRWLock row_array_lock_;          ///< read & write lock for row array
    };
  }
}

#endif
