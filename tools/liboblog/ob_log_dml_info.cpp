////===================================================================
 //
 // ob_log_dml_info.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-01-24 by XiuMing (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   Implementation file for DML operation information
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_log_dml_info.h"
#include "ob_log_schema_getter.h"         // ObLogSchema
#include "ob_log_meta_manager.h"          // IObLogMetaManager
#include "ob_log_partitioner.h"           // IObLogPartitioner
#include "ob_log_filter.h"                // ObLogMutator
#include "ob_log_common.h"                // OB_LOG_INVALID_TIMESTAMP ...
#include "common/ob_tsi_factory.h"        // GET_TSI_MULT

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogDmlStmt::ObLogDmlStmt() : normal_columns_(),
                                   cur_dml_meta_(),
                                   inited_(false)
    {
      reset();
    }

    ObLogDmlStmt::~ObLogDmlStmt()
    {
      destroy();
    }

    int ObLogDmlStmt::construct_dml_stmt(ObLogDmlStmt *&dml_stmt,
        const ObLogSchema *total_schema,
        IObLogPartitioner *log_partitioner,
        IObLogMetaManager *meta_manager,
        ObLogMutator &mutator,
        ObLogDmlMeta *last_dml_meta_ptr,
        uint64_t stmt_index_in_trans)
    {
      int ret = OB_SUCCESS;

      dml_stmt = common::GET_TSI_MULT(ObLogDmlStmt, TSI_LIBOBLOG_DML_STMT);
      if (NULL != dml_stmt)
      {
        dml_stmt->reset();

        if (OB_SUCCESS != (ret = dml_stmt->init(total_schema,
                log_partitioner,
                meta_manager,
                mutator,
                last_dml_meta_ptr,
                stmt_index_in_trans)))
        {
          TBSYS_LOG(ERROR, "fail to initialize ObLogDmlStmt, ret=%d", ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "GET_TSI_MULT fail: TSI_LIBOBLOG_DML_STMT, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }

      return ret;
    }

    void ObLogDmlStmt::destruct_dml_stmt(ObLogDmlStmt *dml_stmt)
    {
      if (NULL != dml_stmt)
      {
        dml_stmt->destroy();
      }
    }

    void ObLogDmlStmt::reset()
    {
      inited_ = false;
      normal_columns_.clear();

      table_id_ = OB_INVALID_ID;
      row_key_ = NULL;
      dml_type_ = OB_DML_UNKNOW;
      checkpoint_ = OB_LOG_INVALID_CHECKPOINT;
      timestamp_ = OB_LOG_INVALID_TIMESTAMP;

      db_partition_ = OB_LOG_INVALID_PARTITION;
      tb_partition_ = OB_LOG_INVALID_PARTITION;

      table_meta_ = NULL;
      cur_dml_meta_.reset();
      last_dml_meta_ptr_ = NULL;

      column_num_ = 0;

      total_schema_ = NULL;

      stmt_index_in_trans_ = 0;
    }

    void ObLogDmlStmt::destroy()
    {
      reset();
    }

    int ObLogDmlStmt::init(const ObLogSchema *total_schema,
        IObLogPartitioner *log_partitioner,
        IObLogMetaManager *meta_manager,
        ObLogMutator &mutator,
        ObLogDmlMeta *last_dml_meta_ptr,
        uint64_t stmt_index_in_trans)
    {
      int ret = OB_SUCCESS;

      if (NULL == (total_schema_ = total_schema)
          || NULL == (last_dml_meta_ptr_ = last_dml_meta_ptr)
          || NULL == log_partitioner
          || NULL == meta_manager
          || ! mutator.is_dml_mutator())
      {
        TBSYS_LOG(ERROR, "invalid arguments: total_schema=%p, log_partitioner=%p, "
            "meta_manager=%p, mutator.is_dml_mutator()=%s, last_dml_meta_ptr=%p",
            total_schema, log_partitioner, meta_manager,
            mutator.is_dml_mutator() ? "true" : "false",
            last_dml_meta_ptr);

        ret = OB_INVALID_ARGUMENT;
      }
      // Initialize data based on mutator
      else if (OB_SUCCESS != (ret = init_data_(mutator, log_partitioner, meta_manager)))
      {
        TBSYS_LOG(ERROR, "fails to initialize data, ret=%d", ret);
      }
      else
      {
        stmt_index_in_trans_ = stmt_index_in_trans;
        inited_ = true;
      }

      return ret;
    }

    void ObLogDmlStmt::change_dml_type_to_delete()
    {
      dml_type_ = OB_DML_DELETE;
      normal_columns_.clear();

      // Change DML Meta
      cur_dml_meta_.clear_modified_columns();
      cur_dml_meta_.set_dml_type(dml_type_);
    }

    int ObLogDmlStmt::init_data_(ObLogMutator &mutator,
        IObLogPartitioner *log_partitioner,
        IObLogMetaManager *meta_manager)
    {
      OB_ASSERT(NULL != total_schema_ && NULL != log_partitioner
          && NULL != meta_manager && mutator.is_dml_mutator());

      int ret = OB_SUCCESS;
      ObMutatorCellInfo *first_cell = NULL;
      bool irf = false;

      if (OB_SUCCESS != (ret = init_basic_data_(mutator, log_partitioner, meta_manager, first_cell, irf)))
      {
        TBSYS_LOG(ERROR, "init_basic_data_ fail, ret=%d", ret);
      }
      else if (OB_INVALID_ID != first_cell->cell_info.column_id_)
      {
        ObMutatorCellInfo *cell = first_cell;

        // Iterate mutator cell until row finishs
        do
        {
          if (NULL != total_schema_->get_column_schema(table_id_, cell->cell_info.column_id_))
          {
            if (OB_SUCCESS != (ret = normal_columns_.push_back(&(cell->cell_info))))
            {
              TBSYS_LOG(ERROR, "normal_columns_.push_back fails, ret=%d", ret);
              break;
            }
            else if (! cur_dml_meta_.add_modified_column(cell->cell_info.column_id_))
            {
              TBSYS_LOG(ERROR, "ObLogDmlMeta::add_modified_column fail, column_id=%lu",
                  cell->cell_info.column_id_);

              ret = OB_ERR_UNEXPECTED;
              break;
            }
          }
          else
          {
            // NOTE: Ignore columns that does not exist in current schema.
            TBSYS_LOG(INFO, "mutator (TM=%ld, CP=%lu): ignore column %ld of table %ld, schema=(%ld,%ld) ",
                timestamp_, checkpoint_, cell->cell_info.column_id_,
                cell->cell_info.table_id_,
                total_schema_->get_version(), total_schema_->get_timestamp());
          }

          if (irf) break;

          if (OB_SUCCESS != (ret = mutator.get_mutator().next_cell()))
          {
            TBSYS_LOG(ERROR, "iterate to row finished cell fail, next_cell() return %d"
                "mutator: TM=%ld, CP=%lu",
                ret, timestamp_, checkpoint_);
            ret = OB_INVALID_DATA;
            break;
          }

          cell = NULL;
          irf = false;
          if (OB_SUCCESS != (ret = mutator.get_mutator_cell(&cell, NULL, &irf, NULL))
                || NULL == cell)
          {
            TBSYS_LOG(ERROR, "get_mutator_cell fail, ret=%d, cell=%p", ret, cell);
            ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
            break;
          }
          else if (OB_INVALID_ID == cell->cell_info.column_id_)
          {
            TBSYS_LOG(ERROR, "cell column ID is invalid, mutator: TM=%ld, CP=%lu, cell='%s'",
                timestamp_, checkpoint_, print_cellinfo(&cell->cell_info));
            ret = OB_INVALID_DATA;
            break;
          }
        } while (OB_SUCCESS == ret);
      }
      else
      {
        // Do nothing when cell is a NOP cell
      }

      return ret;
    }

    int ObLogDmlStmt::init_basic_data_(ObLogMutator &mutator,
        IObLogPartitioner *log_partitioner,
        IObLogMetaManager *meta_manager,
        ObMutatorCellInfo *&first_cell,
        bool &irf)
    {
      OB_ASSERT(NULL != total_schema_ && NULL != log_partitioner && NULL != meta_manager
          && mutator.is_dml_mutator() && NULL == first_cell);

      int ret = OB_SUCCESS;
      ObDmlType dml_type = OB_DML_UNKNOW;

      if (OB_SUCCESS != (ret = get_first_cell_(mutator, &first_cell, &irf, &dml_type)))
      {
        TBSYS_LOG(ERROR, "get_first_cell_ fail, ret=%d", ret);
      }
      // Get column number
      else if (NULL == (total_schema_->get_table_schema(first_cell->cell_info.table_id_, column_num_)))
      {
        // TODO: support drop table
        TBSYS_LOG(ERROR, "get_table_schema fail, table_id=%lu", first_cell->cell_info.table_id_);
        ret = OB_ERR_UNEXPECTED;
      }
      // Get database and table partition number
      else if (OB_SUCCESS != (ret = log_partitioner->partition(first_cell, &db_partition_, &tb_partition_)))
      {
        TBSYS_LOG(ERROR, "ObLogPartitioner::partition() fails, schema=(%ld,%ld), mutator TM=%ld, CP=%lu"
            "ret=%d, cell_info='%s'",
            total_schema_->get_version(), total_schema_->get_timestamp(),
            mutator.get_mutate_timestamp(), mutator.get_log_id(),
            ret, print_cellinfo(&(first_cell->cell_info)));
      }
      // Get ITableMeta based on specific schema
      else if (OB_SUCCESS != (ret = meta_manager->get_table_meta(table_meta_,
              first_cell->cell_info.table_id_,
              db_partition_,
              tb_partition_,
              total_schema_)))
      {
        TBSYS_LOG(ERROR, "get_table_meta fail, table_id=%lu, db_partition=%lu, "
            "tb_partition=%lu, total_schema=%p, ret=%d",
            first_cell->cell_info.table_id_,
            db_partition_,
            tb_partition_,
            total_schema_,
            ret);
      }
      else
      {
        // Initialize basic information
        id_ = mutator.get_num();
        row_key_ = &(first_cell->cell_info.row_key_);
        table_id_ = first_cell->cell_info.table_id_;
        dml_type_ = dml_type;
        checkpoint_ = mutator.get_log_id();
        timestamp_ = mutator.get_mutate_timestamp();

        cur_dml_meta_.reset();
        cur_dml_meta_.set_table_id(table_id_);
        cur_dml_meta_.set_db_partition(db_partition_);
        cur_dml_meta_.set_tb_partition(tb_partition_);
        cur_dml_meta_.set_dml_type(dml_type_);
      }

      return ret;
    }

    int ObLogDmlStmt::get_first_cell_(ObLogMutator &mutator,
        ObMutatorCellInfo **first_cell_ptr,
        bool *irf_ptr,
        ObDmlType *dml_type_ptr)
    {
      OB_ASSERT(NULL != first_cell_ptr && NULL != irf_ptr && NULL != dml_type_ptr);

      int ret = OB_SUCCESS;
      bool irc = false;
      bool irf = false;
      ObMutatorCellInfo *cell = NULL;
      ObDmlType dml_type = OB_DML_UNKNOW;

      if (OB_SUCCESS != (ret = mutator.get_mutator_cell(&cell, &irc, &irf, &dml_type))
            || NULL == cell)
      {
        TBSYS_LOG(ERROR, "get_mutator_cell fail, ret=%d, cell=%p", ret, cell);
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
      else if (!irc)
      {
        TBSYS_LOG(ERROR, "mutator cell is not the first changed cell, TM=%ld, CP=%lu, cell='%s'",
            mutator.get_mutate_timestamp(), mutator.get_log_id(), print_cellinfo(&cell->cell_info));
        ret = OB_INVALID_DATA;
      }
      else if (OB_INVALID_ID == cell->cell_info.table_id_)
      {
        TBSYS_LOG(ERROR, "mutator cell table id is invalid, TM=%ld, CP=%lu, cell='%s'",
            mutator.get_mutate_timestamp(), mutator.get_log_id(), print_cellinfo(&cell->cell_info));
        ret = OB_INVALID_DATA;
      }
      // NOTE: DELETE cell or NOP cell should be at the end of the row
      else if ((OB_DML_DELETE == dml_type || OB_INVALID_ID == cell->cell_info.column_id_) && !irf)
      {
        TBSYS_LOG(ERROR, "mutator has NOP column but has multiple cells, TM=%ld, CP=%lu, "
            "table=%lu, column=%lu, dml_type=%s, cell='%s'",
            mutator.get_mutate_timestamp(), mutator.get_log_id(),
            cell->cell_info.table_id_, cell->cell_info.column_id_,
            str_dml_type(dml_type), print_cellinfo(&cell->cell_info));
        ret = OB_INVALID_DATA;
      }
      else
      {
        *first_cell_ptr = cell;
        *irf_ptr = irf;
        *dml_type_ptr = dml_type;
      }

      return ret;
    }

    bool ObLogDmlStmt::get_first_in_log_event() const
    {
      bool bret = true;

      if (inited_ && NULL != last_dml_meta_ptr_)
      {
        bret = ! (*last_dml_meta_ptr_ == cur_dml_meta_);
      }

      return bret;
    }
  } // namespace liboblog
} // namespace oceanbase
