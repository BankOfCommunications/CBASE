////===================================================================
 //
 // ob_log_dml_info.h liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-01-24 by XiuMing (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   Declaration file for DML operation information
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_DML_INFO_H_
#define  OCEANBASE_LIBOBLOG_DML_INFO_H_

#include "common/ob_define.h"
#include "common/ob_bit_set.h"        // ObBitSet
#include "common/ob_mutator.h"        // ObMutatorCellInfo
#include "common/ob_array.h"          // ObArray
#include "common/ob_rowkey.h"         // ObRowkey
#include "common/ob_common_param.h"   // ObCellInfo
#include "ob_log_common.h"            // OB_LOG_INVALID_PARTITION

class ITableMeta;
namespace oceanbase
{
  namespace liboblog
  {
    /// DML statement meta information used to distinguish different DML statement
    class ObLogDmlMeta
    {
      private:
        common::ObDmlType dml_type_;
        uint64_t table_id_;
        uint64_t tb_partition_;
        uint64_t db_partition_;

        // NOTE: Here use column ID bitset other than column index bitset to support DDL: add/delete columns
        common::ObBitSet<common::OB_ALL_MAX_COLUMN_ID> modified_column_id_bitset_;

      public:
        ObLogDmlMeta() : dml_type_(common::OB_DML_UNKNOW),
                         table_id_(common::OB_INVALID_ID),
                         tb_partition_(OB_LOG_INVALID_PARTITION),
                         db_partition_(OB_LOG_INVALID_PARTITION),
                         modified_column_id_bitset_()
        {
          modified_column_id_bitset_.clear();
        }

        virtual ~ObLogDmlMeta() {}

        void reset()
        {
          dml_type_ = common::OB_DML_UNKNOW;
          table_id_ = common::OB_INVALID_ID;
          tb_partition_ = OB_LOG_INVALID_PARTITION;
          db_partition_ = OB_LOG_INVALID_PARTITION;
          modified_column_id_bitset_.clear();
        }

        void clear_modified_columns() { modified_column_id_bitset_.clear(); }

        void set_dml_type(common::ObDmlType dml_type) { dml_type_ = dml_type; }
        const common::ObDmlType get_dml_type() const { return dml_type_; }

        void set_table_id(uint64_t table_id) { table_id_ = table_id; }
        const uint64_t get_table_id() const { return table_id_; }

        void set_db_partition(uint64_t db_partition) { db_partition_ = db_partition; }
        const uint64_t get_db_partition() const { return db_partition_; }

        void set_tb_partition(uint64_t tb_partition) { tb_partition_ = tb_partition; }
        const uint64_t get_tb_partition() const { return tb_partition_; }

        bool add_modified_column(uint64_t column_id)
        {
          int bret = true;

          if (column_id > common::OB_ALL_MAX_COLUMN_ID)
          {
            TBSYS_LOG(ERROR, "invalid column ID %lu, OB_ALL_MAX_COLUMN_ID=%lu",
                column_id, common::OB_ALL_MAX_COLUMN_ID);

            bret = false;
          }
          else
          {
            bret = modified_column_id_bitset_.add_member(static_cast<int32_t>(column_id));
          }

          return bret;
        }

        const common::ObBitSet<common::OB_ALL_MAX_COLUMN_ID> &get_modified_columns() const
        {
          return modified_column_id_bitset_;
        }

        ObLogDmlMeta & operator=(const ObLogDmlMeta &dml_meta)
        {
          dml_type_ = dml_meta.get_dml_type();
          table_id_ = dml_meta.get_table_id();
          db_partition_ = dml_meta.get_db_partition();
          tb_partition_ = dml_meta.get_tb_partition();
          modified_column_id_bitset_ = dml_meta.get_modified_columns();

          return *this;
        }

        bool operator==(const ObLogDmlMeta &dml_meta)
        {
          return dml_type_ == dml_meta.get_dml_type()
            && table_id_ == dml_meta.get_table_id()
            && db_partition_ == dml_meta.get_db_partition()
            && tb_partition_ == dml_meta.get_tb_partition()
            && modified_column_id_bitset_ == dml_meta.get_modified_columns();
        }
    }; // end of class ObLogDmlMeta

    class ObLogSchema;
    class IObLogPartitioner;
    class IObLogMetaManager;
    class ObLogMutator;
    /// A whole DML statement which holds mutator cell informations
    class ObLogDmlStmt
    {
      public:
        /// @brief Construct a ObLogDmlStmt and initialize it
        /// @param[out] dml_stmt  target returned ObLogDmlStmt instance
        /// @param[in]  total_schema specific schema
        /// @param[in]  log_partitioner partitioner used to partition DML statement
        /// @param[in]  meta_manager meta information manager
        /// @param[in]  mutator source data container
        /// @param[in]  last_dml_meta_ptr dml meta data of last DML statement
        /// @param[in]  stmt_index_in_trans dml statement index in transaction
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error code on fail
        static int construct_dml_stmt(ObLogDmlStmt *&dml_stmt,
            const ObLogSchema *total_schema,
            IObLogPartitioner *log_partitioner,
            IObLogMetaManager *meta_manager,
            ObLogMutator &mutator,
            ObLogDmlMeta *last_dml_meta_ptr,
            uint64_t stmt_index_in_trans);

        /// @brief Destruct ObLogDmlStmt instance
        /// @param dml_stmt target ObLogDmlStmt
        static void destruct_dml_stmt(ObLogDmlStmt *dml_stmt);

      public:
        ObLogDmlStmt();
        virtual ~ObLogDmlStmt();

        /// @brief Initialize ObLogDmlStmt based on specific schema
        /// @param total_schema target schema
        /// @param log_partitioner parititoner
        /// @param meta_manager meta information manager
        /// @param mutator source data container
        /// @param last_dml_meta_ptr dml meta data of last DML statement
        /// @param stmt_index_in_trans dml statement index in transaction
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error code on fail
        int init(const ObLogSchema *total_schema,
            IObLogPartitioner *log_partitioner,
            IObLogMetaManager *meta_manager,
            ObLogMutator &mutator,
            ObLogDmlMeta *last_dml_meta_ptr,
            uint64_t stmt_index_in_trans);

        void change_dml_type_to_delete();

        void destroy();

        bool is_inited() const { return inited_; }

        void reset();

        bool get_first_in_log_event() const;

      // Private member function
      private:
        /// @brief Initialize DML statement data based on mutator and log_partitioner
        /// @param mutator source data container
        /// @param log_partitioner partitioner used to partition DML statement
        /// @param meta_manager meta information manager
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error code on fail
        int init_data_(ObLogMutator &mutator,
            IObLogPartitioner *log_partitioner,
            IObLogMetaManager *meta_manager);

        /// @brief Initialize basic data and return first cell
        /// @param[in] mutator source data container
        /// @param[in] log_partitioner partitioner used to partition DML statement
        /// @param[in] meta_manager meta information manager
        /// @param[out] first_cell returned first cell of row
        /// @param[out] irf returned is_row_finished flag
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error code on fail
        int init_basic_data_(ObLogMutator &mutator,
            IObLogPartitioner *log_partitioner,
            IObLogMetaManager *meta_manager,
            common::ObMutatorCellInfo *&first_cell,
            bool &irf);

        /// @brief Get first cell of DML statement
        /// @param[in]  mutator cell container
        /// @param[out] first_cell_ptr returned first cell information
        /// @param[out] irf_ptr returned is_row_finished
        /// @param[out] dml_type_ptr returned DML type
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error code on fail
        int get_first_cell_(ObLogMutator &mutator,
            common::ObMutatorCellInfo **first_cell_ptr,
            bool *irf_ptr,
            common::ObDmlType *dml_type_ptr);
      // Public member variable
      public:
        common::ObArray<const common::ObCellInfo *> normal_columns_;    ///< columns that is not rowkey

        uint64_t id_;                                                   ///< data ID
        uint64_t table_id_;                                             ///< table ID
        common::ObRowkey *row_key_;                                     ///< row key
        common::ObDmlType dml_type_;                                    ///< DML statement type
        uint64_t checkpoint_;                                           ///< log ID
        int64_t timestamp_;                                             ///< timestamp

        uint64_t db_partition_;                                         ///< DB partition
        uint64_t tb_partition_;                                         ///< table partition

        ITableMeta *table_meta_;                                        ///< table meta information

        ObLogDmlMeta cur_dml_meta_;                                     ///< current DML statement meta information
        ObLogDmlMeta *last_dml_meta_ptr_;                               ///< meta information of last DML statement

        const ObLogSchema *total_schema_;                               ///< schema

        int32_t column_num_;                                            ///< column number

        uint64_t stmt_index_in_trans_;                                  ///< statement index in transaction

      // Private member variables
      private:
        bool inited_;
    }; // end class ObLogDmlStmt
  } // end namespace liboblog
} // end namespace oceanbase
#endif
