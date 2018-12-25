////===================================================================
 //
 // ob_log_mysql_adaptor.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_
#define  OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_

#include <sstream>
#include <string>
#include "libobsql.h"      // ob_sql*
#include "hash/ob_hashmap.h"
#include "page_arena.h"
#include "ob_log_schema_getter.h" // ObLogSchema
#include "ob_log_common.h"
#include "common/ob_fifo_allocator.h" // FIFOAllocator

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogColValue
    {
      public:
        enum ColType
        {
          CT_INT = 0,
          CT_TIME = 1,
          CT_VARCHAR = 2,
        };
      public:
        IObLogColValue(const ColType type) : type_(type), mysql_type_(MYSQL_TYPE_NULL), next_(NULL) {};
        virtual ~IObLogColValue() {};
      public:
        virtual int64_t get_buffer_length() const = 0;
        virtual int64_t get_length() const = 0;
        virtual void *get_data_ptr() = 0;
        virtual uint64_t *get_length_ptr() {return NULL;};
        virtual void to_str(std::string &str) const  = 0;

        my_bool *get_isnull_ptr() {return &is_null_;};
        my_bool isnull() const {return is_null_;};
        void set_mysql_type(const int32_t mysql_type) {mysql_type_ = mysql_type;};
        int32_t get_mysql_type() {return mysql_type_;};
        void set_next(IObLogColValue *next) {next_ = next;};
        IObLogColValue *get_next() {return next_;};
        const IObLogColValue *get_next() const {return next_;};
      protected:
        const ColType type_;
        my_bool is_null_;
        int32_t mysql_type_;
        IObLogColValue *next_;
    };

    class ObLogMysqlAdaptor
    {
      class IntColValue : public IObLogColValue
      {
        public:
          IntColValue() : IObLogColValue(CT_INT), i_(0) {};
          ~IntColValue() {};
        protected:
          void to_str(std::string &str) const
          {
            if (!is_null_)
            {
              oss_.clear();
              oss_.str("");
              oss_ << i_;
              str = oss_.str();
            }
          }
        public:
          int64_t get_buffer_length() const {return get_length();};
          int64_t get_length() const {return sizeof(i_);};
          void *get_data_ptr() {return &i_;};
        private:
          mutable std::ostringstream oss_;
          int64_t i_;
      };
      class TimeColValue : public IObLogColValue
      {
        public:
          TimeColValue() : IObLogColValue(CT_TIME) {};
          ~TimeColValue() {};
        protected:
          void to_str(std::string &str) const
          {
            if (! is_null_)
            {
              oss_.str("");
              oss_.clear();
              oss_.fill('0');
              oss_.width(4); oss_ << t_.year   << "-";
              oss_.width(2); oss_ << t_.month  << "-";
              oss_.width(2); oss_ << t_.day    << " ";
              oss_.width(2); oss_ << t_.hour   << ":";
              oss_.width(2); oss_ << t_.minute << ":";
              oss_.width(2); oss_ << t_.second << ".";
              oss_.width(6); oss_ << t_.second_part;
              str = oss_.str();
            }
          };
        public:
          int64_t get_buffer_length() const {return get_length();};
          int64_t get_length() const {return sizeof(t_);};
          void *get_data_ptr() {return &t_;};
        private:
          mutable std::ostringstream oss_;
          MYSQL_TIME t_;
      };
      class VarcharColValue : public IObLogColValue
      {
        public:
          VarcharColValue(const uint64_t buffer_length) : IObLogColValue(CT_VARCHAR),
                                                          buffer_length_(buffer_length),
                                                          data_length_(buffer_length)
          {};
          ~VarcharColValue() {buffer_length_ = 0; data_length_ = 0;};

        public:
          void to_str(std::string &str) const
          {
            if (! is_null_)
            {
              str.assign(v_, data_length_);
            }
          }
        public:
          int64_t get_buffer_length() const { return buffer_length_; };
          int64_t get_length() const { return data_length_; };
          uint64_t *get_length_ptr() {return &data_length_;};
          void *get_data_ptr() { return v_; };
        private:
          uint64_t buffer_length_;    // length of allocated buffer to hold data
          uint64_t data_length_;      // real data length
          char v_[0];
      };

      struct TableSchemaContainer
      {
        const ObLogSchema *total_schema;
        const common::ObTableSchema *table_schema;
        const common::ObColumnSchemaV2 *column_schema_array;
        int32_t column_number;
        bool inited;

        void reset()
        {
          inited = false;
          total_schema = NULL;
          table_schema = NULL;
          column_schema_array = NULL;
          column_number = 0;
        }

        int init(const ObLogSchema *schema, uint64_t table_id);
      };

      struct TCInfo
      {
        static const int64_t PS_SQL_BUFFER_SIZE = 4096;
        char sql[PS_SQL_BUFFER_SIZE];
        int64_t rk_column_num;

        MYSQL_STMT *stmt;
        IObLogColValue *res_list;

        MYSQL_BIND *params;
        MYSQL_TIME *tm_data;
        my_bool *params_is_null;      // is_null variable correspoind to params

        MYSQL_BIND *res_idx;

        void clear()
        {
          res_idx = NULL;
          tm_data = NULL;
          params = NULL;

          res_list = NULL;
          stmt = NULL;

          rk_column_num = 0;
          (void)memset(sql, 0, sizeof(sql));
        }
      };

      // TABLE ID <---> TCInfo
      typedef common::hash::ObHashMap<uint64_t, TCInfo *> TCInfoMap;

      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t MYSQL_ADAPTOR_SIZE_TOTAL_LIMIT = 4L * 1024L * 1024L * 1024L;
      static const int64_t MYSQL_ADAPTOR_SIZE_HOLD_LIMIT = ALLOCATOR_PAGE_SIZE * 2;

      static const int64_t TC_INFO_MAP_SIZE = DEFAULT_MAX_TABLE_NUM;
      static const int32_t MAX_MYSQL_FAIL_RETRY_TIMES = 100;

      public:
        /// @brief Get thread specific instance of MySQL Adaptor based on specific schema
        /// @param[out] mysql_adaptor returned MySQL Adaptor object
        /// @param[in] config configureation
        /// @param[in] total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when schema is invalid and should be updated
        /// @retval other error state on fail
        static int get_tsi_mysql_adaptor(ObLogMysqlAdaptor *&mysql_adaptor,
            const ObLogConfig &config,
            const ObLogSchema *total_schema);

      public:
        ObLogMysqlAdaptor();
        ~ObLogMysqlAdaptor();
      public:
        /// @brief Initialize MySQL Adaptor based on specific schema
        /// @param config configuration
        /// @param total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when schema is invalid and should be updated
        /// @retval other error state on fail
        int init(const ObLogConfig &config, const ObLogSchema *total_schema);

        void destroy();
        static bool is_time_type(const common::ObObjType type);

        bool is_inited() { return inited_; }

        /// @brief Function called by ObLogConfig::parse_tb_select() used to parse table
        /// @param tb_name target table name
        /// @param total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when specific schema is invalid and should be updated
        /// @retval other return value on fail
        int operator ()(const char *tb_name, const ObLogSchema *total_schema);

        /// @brief Query back a whole row of target table based on specific schema version
        /// @param[in] table_id target table ID
        /// @param[in] rowkey target table rowkey data
        /// @param[in] asked_schema specific schema
        /// @param[out] list returned column values queried back
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when asked_schema is invalid and should be updated
        /// @retval OB_ENTRY_NOT_EXIST when querying back empty value
        /// @retval OB_INVALID_DATA when rowkey is invalid or query back invalid data
        /// @retval other error code on fail
        int query_whole_row(const uint64_t table_id,
            const common::ObRowkey &rowkey,
            const ObLogSchema *asked_schema,
            const IObLogColValue *&list);

      private:
        inline bool is_schema_error_(unsigned int errno_mysql) const
        {
          int err_no = static_cast<int>(errno_mysql) * -1;

          // FIXME: Extend the SCHEMA_ERROR situations in which DDL happens.
          //    1. OB_ERR_TABLE_UNKNOWN: target table does not exist
          //    2. OB_ERR_COLUMN_UNKNOWN: target column does not exist
          return (common::OB_ERR_COLUMN_UNKNOWN == err_no || common::OB_ERR_TABLE_UNKNOWN == err_no);
        }

      private:
        /// @brief Query data based on specific schema version
        /// @param[in] table_id target table ID
        /// @param[in] rowkey target table rowkey data
        /// @param[in] asked_schema specific schema
        /// @param[out] list returned column values queried back
        /// @retval OB_SUCCESS on success
        /// @retval OB_ENTRY_NOT_EXIST when querying back empty value
        /// @retval OB_SCHEMA_ERROR when target schema is invalid and should be updated
        /// @retval OB_INVALID_DATA when rowkey is invalid or query back invalid data
        /// @retval other error code on fail
        int query_data_(const uint64_t table_id,
            const common::ObRowkey &rowkey,
            const ObLogSchema *asked_schema,
            const IObLogColValue *&list);

        /// @brief Add TCInfo of target table based on specific schema.
        /// @param table_id target table ID
        /// @param total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when target schema is invalid and should be updated.
        /// @retval ! OB_SUCCESS on fail
        int add_tc_info_(uint64_t table_id, const ObLogSchema *total_schema);

        /// @brief Update ALL TCInfo of ALL tables based on specific schema
        /// @param total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when target schema is invalid and should be updated.
        /// @retval ! OB_SUCCESS on fail
        int update_all_tc_info_(const ObLogSchema *total_schema);

        /// @brief Destruct all TCInfo in tc_info_map_
        void destruct_all_tc_info_();

        /// @brief Get TCInfo of target table
        /// @param[in] table_id target table ID
        /// @param[out] tc_info returned TCInfo object pointer
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int get_tc_info_(const uint64_t table_id, TCInfo *&tc_info);

        /// @brief Set mysql statement parameters with specific row key data
        /// @param tc_info TCInfo which holds the mysql statement parameters
        /// @param rowkey specific row key
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when rowkey is invalid
        /// @retval OB_NOT_SUPPORTED when rowkey contains unsupported column type
        /// @retval other error code on fail
        int set_mysql_stmt_params_(TCInfo *tc_info, const common::ObRowkey &rowkey);

        /// @brief Execute query
        /// @param tc_info TCInfo which contains ObSQLMySQL information and query result
        /// @param rowkey target table rowkey data
        /// @retval OB_SUCCESS on success
        /// @retval OB_ENTRY_NOT_EXIST when querying back empty value
        /// @retval OB_SCHEMA_ERROR when mysql_stmt_execute() fails because of schema error
        ///         which means TCInfo should be updated with newer schema.
        /// @retval OB_NEED_RETRY when mysql_stmt_execute() fails because of unknown error
        /// @retval OB_INVALID_DATA when rowkey is invalid or query back invalid data
        /// @retval other error code on fail
        int execute_query_(TCInfo *tc_info, const common::ObRowkey &rowkey);

        /// @brief Construct TCInfo for target table based on specific schema
        /// @param[out] tc_info returned TCInfo object pointer
        /// @param[in] table_id target table ID
        /// @param[in] total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when target schema is invalid and should be updated
        /// @retval ! OB_SUCCESS on fail
        int construct_tc_info_(TCInfo *&tc_info, uint64_t table_id, const ObLogSchema *total_schema);

        /// @brief Destruct TCInfo
        /// @param tc_info target TCInfo
        void destruct_tc_info_(TCInfo *tc_info);

        /// @brief Initialize prepare-execute SQL statement
        /// @param sql SQL statement buffer
        /// @param size SQL statement buffer size
        /// @param tsc table schema container
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_ps_sql_(char *sql, int64_t size, const TableSchemaContainer &tsc);

        /// @brief Initialize mysql statement: MYSQL_STMT
        /// @param[out] stmt returned MYSQL_STMT
        /// @param[in] sql SQL statement
        /// @param[in] sql_len SQL statement length
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when schema is invalid and failed to execute prepare statement
        /// @retval ! OB_SUCCESS on fail
        int init_mysql_stmt_(MYSQL_STMT *&stmt, const char *sql, int64_t sql_len);

        /// @brief Destroy mysql statement: MYSQL_STMT
        /// @param stmt MYSQL_STMT
        void destroy_mysql_stmt_(MYSQL_STMT *stmt);

        /// @brief Initialize mysql parameters: MYSQL_BIND and MYSQL_TIME
        /// @param[out] params returned MYSQL_BIND
        /// @param[out] tm_data returned MYSQL_TIME
        /// @param[out] params_is_null "is_null" field for MYSQL_BIND
        /// @param[in] tsc table schema container
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_mysql_params_(MYSQL_BIND *&params,
            MYSQL_TIME *&tm_data,
            my_bool *&params_is_null,
            const TableSchemaContainer &tsc);

        /// @brief Destroy mysql parameters
        /// @param params target MYSQL_BIND
        /// @param tm_data target MYSQL_TIME
        /// @param params_is_null "is_null" field for MYSQL_BIND
        void destroy_mysql_params_(MYSQL_BIND *params, MYSQL_TIME *tm_data, my_bool *params_is_null);

        /// @brief Initialize mysql result: MYSQL_BIND and IObLogColValue
        /// @param[out] res_idx returned MYSQL_BIND
        /// @param[out] res_list returned IObLogColValue
        /// @param[in] tsc table schema container
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_mysql_result_(MYSQL_BIND *&res_idx, IObLogColValue *&res_list,
            const TableSchemaContainer &tsc);

        /// @brief Destroy mysql result: MYSQL_BIND and IObLogColValue
        /// @param res_idx target MYSQL_BIND
        /// @param res_list target IObLogColValue
        void destroy_mysql_result_(MYSQL_BIND *res_idx, IObLogColValue *res_list);

        /// @brief Destroy TCInfo Map
        void destroy_tc_info_map_();

        /// @brief Initialize ObSQLMySQL
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_ob_mysql_();

        /// @brief Destroy ObSQLMySQL
        void destroy_ob_mysql_();

      private:
        bool inited_;

        // Configuration
        int mysql_port_;
        const char *mysql_addr_;
        const char *mysql_user_;
        const char *mysql_password_;
        const char *read_consistency_;

        // Unique MYSQL connection
        ObSQLMySQL *ob_mysql_;

        // Schema
        int64_t schema_version_;

        // TCInfo Map
        TCInfoMap tc_info_map_;

        // Table ID array
        const TableIDArray *table_id_array_;

        // Allocator
        common::FIFOAllocator allocator_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_
