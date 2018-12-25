////===================================================================
 //
 // ob_log_filter.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
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

#include "ob_log_filter.h"
#include "ob_log_schema_getter.h"   // ObLogSchema IObLogSchemaGetter
#include "ob_log_common.h"          // MAX_*

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    int ObLogMutator::get_mutator_cell(ObMutatorCellInfo **cell)
    {
      int ret = get_mutator().get_cell(cell);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "mutator get_cell fail, invalid data, timestamp=%ld, checkpoint=%lu, ret=%d",
            get_mutate_timestamp(), get_log_id(), ret);
        ret = OB_INVALID_DATA;
      }

      return ret;
    }

    int ObLogMutator::get_mutator_cell(ObMutatorCellInfo** cell, bool* is_row_changed, bool* is_row_finished, ObDmlType *dml_type)
    {
      int ret = get_mutator().get_cell(cell, is_row_changed, is_row_finished, dml_type);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "mutator get_cell fail, invalid data, timestamp=%ld, checkpoint=%lu, ret=%d",
            get_mutate_timestamp(), get_log_id(), ret);
        ret = OB_INVALID_DATA;
      }

      return ret;
    }

    ObLogTableFilter::ObLogTableFilter() : inited_(false),
                                           prev_(NULL),
                                           tid_filter_(),
                                           last_mutator_timestamp_(OB_LOG_INVALID_TIMESTAMP)
    {
    }

    ObLogTableFilter::~ObLogTableFilter()
    {
      destroy();
    }

    int ObLogTableFilter::init(IObLogFilter *filter, const ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (prev_ = filter)
              || NULL == schema_getter)
      {
        TBSYS_LOG(WARN, "invalid param filter=%p schema_getter=%p", filter, schema_getter);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_tid_filter_(config, *schema_getter)))
      {
        TBSYS_LOG(WARN, "init_tid_filter fail, ret=%d", ret);
      }
      else
      {
        last_mutator_timestamp_ = 0;
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogTableFilter::destroy()
    {
      inited_ = false;
      last_mutator_timestamp_ = OB_LOG_INVALID_TIMESTAMP;
      tid_filter_.destroy();
      prev_ = NULL;
    }

    int ObLogTableFilter::next_mutator(ObLogMutator **mutator, const int64_t timeout)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == mutator)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        int64_t end_time = start_time + timeout;
        int64_t cur_timeout = timeout;
        ObLogMutator *tmp_mutator = NULL;
        while (true)
        {
          ret = prev_->next_mutator(&tmp_mutator, cur_timeout);
          if (OB_SUCCESS == ret)
          {
            if (NULL == tmp_mutator)
            {
              TBSYS_LOG(ERROR, "unexpected error: next_mutator return true but with NULL mutator value");
              ret = OB_ERR_UNEXPECTED;
              break;
            }

            // Update last mutator timestamp
            // NOTE: mutate timestamp is valid only for NORMAL UPS mutator
            if (tmp_mutator->is_dml_mutator())
            {
              last_mutator_timestamp_ = tmp_mutator->get_mutate_timestamp();
            }

            if (OB_SUCCESS != (ret = filter_mutator_(*tmp_mutator)))
            {
              TBSYS_LOG(ERROR, "filter_mutator_ fail, ret=%d", ret);
              break;
            }
            else if (! tmp_mutator->is_filter_info_valid())
            {
              TBSYS_LOG(ERROR, "filter_mutator_ fail, mutator filter info is invalid");
              ret = OB_ERR_UNEXPECTED;
              break;
            }
            else if (tmp_mutator->get_filter_info().contain_ddl_operations_
                || tmp_mutator->get_filter_info().contain_table_selected_)
            {
              *mutator = tmp_mutator;
              break;
            }
            else
            {
              release_mutator(tmp_mutator);
              tmp_mutator = NULL;
            }
          }

          cur_timeout = end_time - tbsys::CTimeUtil::getTime();
          // TIMEOUT
          if (0 >= cur_timeout)
          {
            ret = OB_PROCESS_TIMEOUT;
            break;
          }
        }

        if (OB_SUCCESS != ret && NULL != tmp_mutator)
        {
          release_mutator(tmp_mutator);
          tmp_mutator = NULL;
        }

        if (OB_SUCCESS == ret && NULL != (*mutator) && (*mutator)->is_filter_info_valid()
            && (*mutator)->get_filter_info().contain_table_selected_)
        {
          wait_to_process_mutator_(**mutator);
        }
      }
      return ret;
    }

    void ObLogTableFilter::release_mutator(ObLogMutator *mutator)
    {
      if (inited_)
      {
        prev_->release_mutator(mutator);
      }
    }

    bool ObLogTableFilter::contain(const uint64_t table_id)
    {
      return (hash::HASH_EXIST == tid_filter_.exist(table_id));
    }

    int ObLogTableFilter::init_tid_filter_(const ObLogConfig &config, IObLogSchemaGetter &schema_getter)
    {
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = schema_getter.get_schema();
      if (NULL == total_schema)
      {
        TBSYS_LOG(WARN, "get total schema fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != tid_filter_.create(DEFAULT_MAX_TABLE_NUM))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, total_schema);
      }
      if (NULL != total_schema)
      {
        schema_getter.revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    int ObLogTableFilter::operator ()(const char *tb_name, const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = total_schema->get_table_schema(tb_name);
      int hash_ret = 0;
      if (NULL == table_schema)
      {
        TBSYS_LOG(WARN, "table_name=%s not find in schema", tb_name);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (hash::HASH_INSERT_SUCC != (hash_ret = tid_filter_.set(table_schema->get_table_id()))
              && hash::HASH_EXIST != hash_ret)
      {
        TBSYS_LOG(WARN, "set to tid_filter fail, hash_ret=%d table_name=%s table_id=%lu",
                  hash_ret, tb_name, table_schema->get_table_id());
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        TBSYS_LOG(INFO, "add tid_filter succ, table_name=%s table_id=%lu",
                  tb_name, table_schema->get_table_id());
      }
      return ret;
    }

    int ObLogTableFilter::filter_mutator_(ObLogMutator &mutator) const
    {
      OB_ASSERT(inited_);

      int ret = OB_SUCCESS;
      bool contain_table_selected = true;
      bool contain_ddl_operations = true;

      if (! mutator.is_dml_mutator())
      {
        contain_table_selected = false;
        contain_ddl_operations = false;
      }
      else
      {
        uint64_t last_table_id = OB_INVALID_ID;

        contain_table_selected = false;
        contain_ddl_operations = false;

        while ((! contain_ddl_operations || ! contain_table_selected)
            && OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
        {
          ObMutatorCellInfo *cell = NULL;
          if (OB_SUCCESS != (ret = mutator.get_mutator_cell(&cell)))
          {
            TBSYS_LOG(WARN, "get_mutator_cell fail, ret=%d", ret);
          }
          else if (NULL == cell)
          {
            TBSYS_LOG(WARN, "unexpected error cell is null pointer");
            ret = OB_ERR_UNEXPECTED;
          }
          else if (last_table_id == cell->cell_info.table_id_)
          {
            continue;
          }
          else
          {
            if (! contain_table_selected)
            {
              contain_table_selected = (hash::HASH_EXIST == tid_filter_.exist(cell->cell_info.table_id_));
            }

            // NOTE: If there are DML operations to three inner tables, we consider that
            // they are DDL operations.
            //   1. __all_all_column
            //   2. __first_tablet_entry,
            //   3. __all_join_info,
            if (! contain_ddl_operations)
            {
              contain_ddl_operations = (OB_ALL_ALL_COLUMN_TID == cell->cell_info.table_id_
                  || OB_FIRST_TABLET_ENTRY_TID == cell->cell_info.table_id_
                  || OB_ALL_JOIN_INFO_TID == cell->cell_info.table_id_);
            }

            last_table_id = cell->cell_info.table_id_;
          }
        }
        mutator.get_mutator().reset_iter();

        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }

      if (OB_SUCCESS == ret)
      {
        mutator.set_filter_info(contain_table_selected, contain_ddl_operations);
      }

      return ret;
    }

    void ObLogTableFilter::wait_to_process_mutator_(const ObLogMutator &mutator) const
    {
      static int64_t timewait_sum = 0;
      static int64_t total_count = 0, need_wait_count = 0;
      const int64_t STATISTIC_INTERVAL = 10L * 1000000L;
      const int64_t RESET_STATISTIC_INTERVAL = 24 * 3600 * 1000000L;

      int64_t min_process_timestamp = mutator.get_min_process_timestamp();
      int64_t cur_timestamp = 0;

      total_count++;
      while (min_process_timestamp > (cur_timestamp = tbsys::CTimeUtil::getTime()))
      {
        uint32_t timewait_us = static_cast<uint32_t>(min_process_timestamp - cur_timestamp);

        timewait_sum += timewait_us;
        need_wait_count++;

        TBSYS_LOG(DEBUG, "MUTATOR INFO: WAIT=%u, MIN_PROCESS_TM=%ld, SYS_TM=%ld, "
            "MUTATE_TIMESTAMP=%ld, CHECKPOINT=%ld",
            timewait_us, min_process_timestamp, cur_timestamp,
            mutator.get_mutate_timestamp(),
            mutator.get_log_id());

        // NOTE: usleep may be interrupted, so we do not trust usleep
        usleep(timewait_us);

        if (REACH_TIME_INTERVAL(STATISTIC_INTERVAL))
        {
          TBSYS_LOG(INFO, "ROUTER WAIT: AVG=%ld us, RATIO=%ld%% (%ld/%ld) SUM=%ld sec",
              0 < need_wait_count ? timewait_sum / need_wait_count : 0,
              0 < total_count ? (need_wait_count * 100 / total_count) : 0,
              total_count, need_wait_count, timewait_sum / 1000000);
        }

        if (REACH_TIME_INTERVAL(RESET_STATISTIC_INTERVAL))
        {
          TBSYS_LOG(INFO, "ROUTER WAIT: RESET, SUM=%ld sec, TOTAL_COUNT=%ld, WAIT_COUNT=%ld",
              timewait_sum / 1000000, total_count, need_wait_count);

          timewait_sum = 0;
          total_count = 0;
          need_wait_count = 0;
        }
      }

      return;
    }
  }
}

