////===================================================================
 //
 // ob_log_formator.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_FORMATOR_H_
#define  OCEANBASE_LIBOBLOG_FORMATOR_H_

#include "common/ob_define.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_queue_thread.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_filter.h"
#include "ob_log_meta_manager.h"
#include "ob_log_mysql_adaptor.h"
#include "common/ob_bit_set.h"    // ObBitSet
#include "ob_log_common.h"
#include "ob_log_binlog_record.h" // ObLogBinlogRecord
#include "ob_log_mysql_query_engine.h"    // ObLogStrColValue, IObLogMysqlQueryEngine
#include "common/utility.h"       // databuff_printf

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogObserver
    {
      public:
        IObLogObserver() : inc_num_(1) {};
        virtual ~IObLogObserver() {};
      public:
        virtual int add_mutator(ObLogMutator &mutator, volatile bool &loop_stop) = 0;
        uint64_t inc_num() {return inc_num_++;};
      private:
        uint64_t inc_num_;
    };

    class IObLogFormator : public IObLogObserver
    {
      public:
        virtual ~IObLogFormator() {};

      public:
        virtual int init(const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder,
            IObLogMysqlQueryEngine *query_engine,
            IObLogFilter *log_filter) = 0;

        virtual void destroy() = 0;

        virtual int next_record(IBinlogRecord **record, const int64_t timeout_us) = 0;

        virtual void release_record(IBinlogRecord *record) = 0;

        virtual int add_mutator(ObLogMutator &mutator, volatile bool &loop_stop) = 0;
    };

    class ObLogDmlStmt;
    class ObLogDmlMeta;
    class ObLogDenseFormator : public IObLogFormator
    {
      struct HeartbeatInfo
      {
        int64_t timestamp_;
        int64_t signature_;

        HeartbeatInfo() : timestamp_(-1), signature_(-1) {}
        HeartbeatInfo(int64_t timestamp, int64_t signature) : timestamp_(timestamp), signature_(signature) {}
        ~HeartbeatInfo() { timestamp_ = -1; signature_ = -1; }

        int64_t to_string(char* buf, const int64_t len) const
        {
          int ret = common::OB_SUCCESS;
          int64_t pos = 0;
          common::databuff_printf(buf, len, pos, "HeartbeatInfo=[timestamp=%ld,signature=%ld]", timestamp_, signature_);
          return ret;
        }

        int64_t hash() const
        {
          return signature_;
        }

        bool operator == (const HeartbeatInfo &hb_info) const
        {
          return hb_info.timestamp_ == timestamp_ && hb_info.signature_ == signature_;
        }
      };

      struct RowValue
      {
        int64_t column_num_;

        std::string *columns_[common::OB_MAX_COLUMN_NUMBER]; // column values

        // Identify whether item is row key. DEFAULT false
        bool is_rowkey_[common::OB_MAX_COLUMN_NUMBER];

        // Identify whether item is in DML statement. DEFAULT false
        bool is_changed_[common::OB_MAX_COLUMN_NUMBER];

        void reset()
        {
          column_num_ = 0;
          (void)memset(columns_, 0, sizeof(columns_));
          (void)memset(is_rowkey_, 0, sizeof(is_rowkey_));
          (void)memset(is_changed_, 0, sizeof(is_changed_));
        }
      };

      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t WAIT_VLIST_TIMEOUT = 1L * 1000000L;
      static const int64_t COND_SPIN_WAIT_NUM = 40000;
      static const int32_t MAX_CACHED_HEARTBEAT_NUM = 40000;
      static const int64_t PRINT_HEARTBEAT_INFO_INTERVAL = 30L * 1000000L;
      static const int64_t DEFAULT_TRANSACTION_STATEMENT_COUNT = 100;
      static const int64_t FORMATOR_RETRY_TIMEWAIT = 500L * 1000L;
      static const int64_t SCHEMA_UPDATE_FAIL_TIMEWAIT = 100L * 1000L;

      // HEARTBEAT HeartbeatInfo <---> HEARTBEAT count
      typedef common::hash::ObHashMap<HeartbeatInfo, int32_t> HeartbeatInfoMap;
      public:
        ObLogDenseFormator();
        ~ObLogDenseFormator();
      public:
        int init(const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder,
            IObLogMysqlQueryEngine *query_engine,
            IObLogFilter *log_filter);
        void destroy();
        int next_record(IBinlogRecord **record, const int64_t timeout);
        void release_record(IBinlogRecord *record);
        int add_mutator(ObLogMutator &mutator, volatile bool &loop_stop);

      private:
        /// @brief Initialize BinlogRecord list
        /// @param max_binlogrecord_num max BinlogRecord number
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_binlogrecord_list_(const int64_t max_binlogrecord_num);

        /// @brief Destroy BinlogRecord list
        void destroy_binlogrecord_list_();

        /// @brief Release ObLogBinlogRecord
        /// @param oblog_br target ObLogBinlogRecord
        void release_oblog_record_(ObLogBinlogRecord *oblog_br);

        /// @brief Format mutator which is a heartbeat
        /// @param mutator source data container
        /// @param loop_stop shared variable to control stop state
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval !OB_SUCCESS and !OB_IN_STOP_STATE on error
        int format_heartbeat_mutator_(ObLogMutator &mutator, volatile bool &loop_stop);

        /// @brief Update HEARTBEAT information and check whether can generate HEARTBEAT
        /// @param[in] hb_info HEARTBEAT information
        /// @param[out] can_generate_heartbeat returned flag used to show whether can generate HEARTBEAT
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int update_and_check_heartbeat_info_(const HeartbeatInfo &hb_info, bool &can_generate_heartbeat);

        /// @brief Fetch BinlogRecord
        /// @param loop_stop shared variable to control stop state
        /// @return ObLogBinlogRecord pointer
        ObLogBinlogRecord *fetch_binlog_record_(volatile bool &loop_stop);

        /// @brief Print HEARTBEAT information
        void print_heartbeat_info_();

        /// @brief Format mutator filled with DML statements
        /// @param loop_stop shared variable to control stop state
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        int format_dml_mutator_(ObLogMutator &mutator, volatile bool &loop_stop);

        /// @brief Create transaction BEGIN/COMMIT BinlogRecord
        /// @param[in] type BinlogRecord type
        /// @param[in] mutator transaction data container
        /// @param[out] br_list_tail returned BinlogRecord list tail
        /// @param[in] br_index_in_trans  BinlogRecord index in transaction
        /// @param[in] loop_stop shared variable to control stop state
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval other error state on fail
        int fill_trans_barrier_(const RecordType type,
            const ObLogMutator &mutator,
            ObLogBinlogRecord *&br_list_tail,
            uint64_t br_index_in_trans,
            volatile bool &loop_stop);

        /// @brief Format all DML statements in mutator based on specific schema
        /// @param[in] mutator target mutator
        /// @param[in] loop_stop shared variable to control stop state
        /// @param[in] total_schema specific schema
        /// @param[out] br_list_tail returned BinlogRecord list tail
        /// @param[out] stmt_count formated DML statements count
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when specific schema is invalid and should be updated.
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        int format_all_dml_statements_(ObLogMutator &mutator,
            volatile bool &loop_stop,
            const ObLogSchema *total_schema,
            ObLogBinlogRecord *&br_list_tail,
            uint64_t &stmt_count);

        /// @brief Query back all statements in target mutator with specific schema and return query result
        /// @param[in] mutator target mutator
        /// @param[in] total_schema specific schema
        /// @param[in] loop_stop shared variable to control stop state
        /// @param[out] returned query result
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when specific schema is invalid and should updated
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval other error code on fail
        int query_back_(ObLogMutator &mutator,
            const ObLogSchema *total_schema,
            volatile bool &loop_stop,
            ObLogMysqlQueryResult *&query_result);

        /// @brief Filter unused cells of mutator
        /// @param mutator target mutator
        /// @retval OB_SUCCESS when finding next valid cell filled with data of selected table
        /// @retval OB_ITER_END when iterating to the end of mutator and finding no valid cell
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        int filter_cells_(ObLogMutator &mutator);

        /// @brief Iterate mutator cell to next row_finished cell
        /// @param mutator target mutator
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        int mutator_iterate_to_row_finish_cell_(ObLogMutator &mutator);

        /// @brief Format a whole DML statement to BinlogRecord based on specific schema
        /// @param[in] index selected statement(row) index in mutator
        /// @param[in] mutator source DML data container
        /// @param[in] loop_stop shared variable to control stop state
        /// @param[in] last_dml_meta meta data of last DML statement
        /// @param[in] total_schema specific schema
        /// @param[in] query_result mysql query result which contains all statements' query value
        /// @param[out] binlog_record returned BinlogRecord which holds the formating data
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval OB_SCHEMA_ERROR when specific schema is invalid and should updated
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        /// @note This function should be called when current mutator cell is the row_changed
        ///       cell and contains user selected table data.
        int format_dml_statement_(int64_t index,
            ObLogMutator &mutator,
            volatile bool &loop_stop,
            ObLogDmlMeta &last_dml_meta,
            const ObLogSchema *total_schema,
            ObLogMysqlQueryResult *query_result,
            ObLogBinlogRecord *&binlog_record);

        /// @brief Construct DML statement based on specific schema
        /// @param[out] dml_stmt returned constructed DML statement object
        /// @param[in] mutator source data container
        /// @param[in] total_schema specific schema
        /// @param[in] last_dml_meta_ptr meta data of last DML statement
        /// @param[in] stmt_index_in_trans current DML statement index in transaction
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error state on error
        int construct_dml_stmt_(ObLogDmlStmt *&dml_stmt,
            ObLogMutator &mutator,
            const ObLogSchema *total_schema,
            ObLogDmlMeta *last_dml_meta_ptr,
            uint64_t stmt_index_in_trans);

        /// @brief Get query value of current statement from query result
        /// @param[in] index current statement index
        /// @param[in] dml_stmt initialized DML statement: ObLogDmlStmt
        /// @param[in] query_result query result which contains all statements' query value
        /// @param[out] query_value returned queried string column value list
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when schema is invalid and should be updated.
        /// @retval other error state on error
        /// @note This function should be called only under query back mode and
        ///       this function may modify dml_stmt to change DML type to DELETE.
        int get_query_value_(int64_t index,
            ObLogDmlStmt *dml_stmt,
            ObLogMysqlQueryResult *query_result,
            ObLogStrColValue *&query_value);

        /// @brief Construct RowValue based ObLogDmlStmt and IObLogColValue
        /// @param[out] row_value returned constructed RowValue
        /// @param[in] dml_stmt target ObLogDmlStmt
        /// @param[in] query_value queried string column value
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int construct_row_value_(RowValue *&row_value,
            const ObLogDmlStmt *dml_stmt,
            ObLogStrColValue *query_value);

        /// @brief Fill RowValue with ObLogDmlStmt and queried back column value
        /// @param row_value target RowValue
        /// @param dml_stmt DML statement
        /// @param query_value queried string column value
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int fill_row_value_(RowValue *row_value,
            const ObLogDmlStmt *dml_stmt,
            ObLogStrColValue *query_value);

        /// @brief Fill queried string column value into RowValue
        /// @param row_value target RowValue
        /// @param query_value queried string column value
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int fill_row_value_query_value_(RowValue *row_value,
            ObLogStrColValue *query_value);

        /// @brief Fill row key information into RowValue
        /// @param row_value target RowValue
        /// @param fill_value whether to fill value into RowValue
        /// @param dml_stmt DML statement
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int fill_row_value_rowkey_(RowValue *row_value, bool fill_value, const ObLogDmlStmt *dml_stmt);

        /// @brief Fill normal columns information into RowValue
        /// @param row_value target RowValue
        /// @param fill_value whether to fill value into RowValue
        /// @param dml_stmt DML statement
        /// @retval OB_SUCCESS on success
        /// @retval !OB_SUCCESS on fail
        int fill_row_value_normal_column_(RowValue *row_value, bool fill_value, const ObLogDmlStmt *dml_stmt);

        /// @brief Fill obj into RowValue
        /// @param row_value target RowValue
        /// @param obj source object
        /// @param column_index column index
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int fill_row_value_obj_(RowValue *row_value, const common::ObObj &obj, const int32_t column_index);

        /// @brief Convert ObObj to c-style string
        /// @param v target ObObj
        /// @param v_len target object length
        /// @retval target c-style string
        static const char *value2str_(const common::ObObj &v, int64_t &v_len);

        /// @brief Construct Binlog Record based on ObLogDmlStmt and RowValue
        /// @param[out] returned Binlog Record
        /// @param[in] dml_stmt target ObLogDmlStmt
        /// @param[in] row_value target RowValue
        /// @param[in] loop_stop shared variable to control stop state
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval ! OB_SUCCESS and ! OB_IN_STOP_STATE on fail
        int construct_binlog_record_(ObLogBinlogRecord *&binlog_record,
            const ObLogDmlStmt *dml_stmt,
            const RowValue *row_value,
            volatile bool &loop_stop);

        /// @brief Get a free BinlogRecord and initialize it based on ObLogDmlStmt
        /// @param[out] binlog_record target BinlogRecord
        /// @param[in] dml_stmt target DML statement
        /// @param[in] loop_stop shared variable to control stop state
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval !OB_SUCCESS and !OB_IN_STOP_STATE on error
        int init_binlog_record_(ObLogBinlogRecord *&binlog_record,
            const ObLogDmlStmt *dml_stmt,
            volatile bool &loop_stop);

        /// @brief Fill data into BinlogRecord with RowValue
        /// @param binlog_record target BinlogRecord
        /// @param row_value target RowValue which contains valid data
        /// @param dml_type DML operation type
        /// @retval OB_SUCCESS on success
        /// @retval !OB_SUCCESS on fail
        int fill_binlog_record_(ObLogBinlogRecord *binlog_record,
            const RowValue *row_value,
            common::ObDmlType dml_type);

        /// @brief Format DELETE DML data into BinlogRecord based on RowValue
        /// @param binlog_record target binlog_record
        /// @param row_value RowValue which holds source data
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int format_dml_delete_(ObLogBinlogRecord *binlog_record, const RowValue *row_value);

        /// @brief Format INSERT DML data into BinlogRecord based on RowValue
        /// @param binlog_record target binlog_record
        /// @param row_value RowValue which holds source data
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int format_dml_insert_(ObLogBinlogRecord *binlog_record, const RowValue *row_value);

        /// @brief Format REPLACE or UPDATE DML data into BinlogRecord based on RowValue
        /// @param binlog_record target binlog_record
        /// @param row_value RowValue which holds source data
        /// @param is_dml_replace true if it is REPLACE, false if it is UPDATE
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int format_dml_replace_update_(ObLogBinlogRecord *binlog_record,
            const RowValue *row_value,
            const bool is_dml_replace);

        /// @brief Destruct RowValue
        /// @param row_value target RowValue
        /// @param free_items bool value used to determine whether to free RowValue items
        void destruct_row_value_(RowValue *row_value, bool free_items);

        /// @brief Destruct ObLogDmlStmt
        /// @param dml_stmt target ObLogDmlStmt
        void destruct_dml_stmt_(ObLogDmlStmt *dml_stmt);

        /// @brief Release ObLogBinlogRecord List
        /// @param br_list target ObLogBinlogRecord list
        void release_oblog_record_list_(ObLogBinlogRecord *br_list);

        /// @brief Print user queue state
        void print_user_queue_state_();

        int update_schema_(const ObLogSchema *total_schema, const ObLogSchema *&new_schema, volatile bool &loop_stop);
      private:
        bool inited_;

        // Configuration
        bool need_query_back_;
        int formator_thread_num_;
        IObLogFilter *log_filter_;
        const ObLogConfig *config_;
        IObLogSchemaGetter *schema_getter_;
        IObLogDBNameBuilder *db_name_builder_;
        IObLogTBNameBuilder *tb_name_builder_;
        IObLogMysqlQueryEngine *query_engine_;

        // HEARTBEAT information
        common::ObSpinLock heartbeat_info_lock_;
        HeartbeatInfoMap heartbeat_info_map_;

        // BinlogRecord management
        common::ObCond p_cond_;
        common::ObCond v_cond_;
        ObLogBinlogRecord *br_list_iter_;
        common::ObFixedQueue<ObLogBinlogRecord> p_list_;
        common::ObFixedQueue<ObLogBinlogRecord> v_list_;

        // Resource Pool
        ObLogStringPool string_pool_;

        // Allocator
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_FORMATOR_H_
