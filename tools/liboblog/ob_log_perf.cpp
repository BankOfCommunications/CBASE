////===================================================================
 //
 // ob_log_router.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-12-16 by XiuMing (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   This file is the implementation of "ob_log_perf.h".
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_log_perf.h"
#include "ob_log_common.h"
#include <time.h>         // ctime

#define PRINT_PERF(str, ...) \
  do { \
    if (print_to_stderr_) \
    { \
      fprintf(stderr, str "\n", ##__VA_ARGS__); \
    } \
\
    TBSYS_LOG(INFO, str, ##__VA_ARGS__); \
  } while (0)

#ifdef PERF_DEBUG

#define TABLE_PERF_DEBUG() \
  do { \
    PerfMap::iterator iter; \
    const ObLogSchema *total_schema = schema_getter_->get_schema(); \
    for (iter = tb_perf_map_.begin(); iter != tb_perf_map_.end(); iter++) \
    { \
      const char *tb_name = total_schema->get_table_schema(iter->first)->get_table_name(); \
      TBSYS_LOG(INFO, "DEBUG: %s(): tb_perf_map_: key=%lu, name=%s value=(%lu,%lu)", \
          __FUNCTION__, iter->first, tb_name, iter->second.trans_count_, iter->second.record_count_); \
    } \
 \
    for (iter = last_tb_perf_map_.begin(); iter != last_tb_perf_map_.end(); iter++) \
    { \
      const char *tb_name = total_schema->get_table_schema(iter->first)->get_table_name(); \
      TBSYS_LOG(INFO, "DEBUG: %s(): last_tb_perf_map_: key=%lu, name=%s value=(%lu,%lu)", \
          __FUNCTION__, iter->first, tb_name, iter->second.trans_count_, iter->second.record_count_); \
    } \
    if (NULL != total_schema) \
    { \
      schema_getter_->revert_schema(total_schema); \
    } \
  } while (0)

#else

#define TABLE_PERF_DEBUG()

#endif

namespace oceanbase
{
  using namespace oceanbase::common;

  namespace liboblog
  {
    ObLogPerf::ObLogPerf()
      : db_perf_map_(),
        tb_perf_map_(),
        total_trans_count_(0),
        last_db_perf_map_(),
        last_tb_perf_map_(),
        last_total_trans_count_(0),
        last_print_time_(0),
        inited_(false),
        started_(false),
        print_to_stderr_(false),
        schema_getter_(NULL)
    {
    }

    ObLogPerf::~ObLogPerf()
    {
      destroy();
    }

    int ObLogPerf::init(ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      const char *tb_select = NULL;
      void *args = NULL;

      if (started_ || inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (schema_getter_ = schema_getter))
      {
        TBSYS_LOG(ERROR, "Invalid argument: schema_getter = NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (tb_select = config.get_tb_select()))
      {
        TBSYS_LOG(WARN, "tb_select null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != db_perf_map_.create(MAX_DB_PARTITION_NUM))
      {
        TBSYS_LOG(ERROR, "failed to create DB perf map, size=%lu",
            MAX_DB_PARTITION_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != last_db_perf_map_.create(MAX_DB_PARTITION_NUM))
      {
        TBSYS_LOG(ERROR, "failed to create DB perf map, size=%lu",
            MAX_DB_PARTITION_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != tb_perf_map_.create(DEFAULT_MAX_TABLE_NUM))
      {
        TBSYS_LOG(ERROR, "failed to create table perf map, size=%d",
            DEFAULT_MAX_TABLE_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != last_tb_perf_map_.create(DEFAULT_MAX_TABLE_NUM))
      {
        TBSYS_LOG(ERROR, "failed to create table perf map, size=%d",
            DEFAULT_MAX_TABLE_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      // Initialize tb_perf_map_ and last_tb_perf_map_ using operator() function
      else if (OB_SUCCESS != (ret = ObLogConfig::parse_tb_select(tb_select, *this, args)))
      {
        TBSYS_LOG(ERROR, "failed to parse table selected, ret=%d", ret);
      }
      else
      {
        total_trans_count_ = 0;
        last_total_trans_count_ = 0;
        last_print_time_ = 0;
        started_ = false;
        inited_ = true;
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }

      return ret;
    } // end of init

    int ObLogPerf::register_db_partition(const uint64_t db_partition)
    {
      int ret = OB_SUCCESS;

      PerfUnit pu;
      pu.reset();

      int hash_ret = 0;

      if (hash::HASH_INSERT_SUCC != (hash_ret = db_perf_map_.set(db_partition, pu)))
      {
        TBSYS_LOG(WARN, "failed to add DB perf data, hash_ret=%d db_partition=%lu",
            hash_ret, db_partition);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (hash::HASH_INSERT_SUCC != (hash_ret = last_db_perf_map_.set(db_partition, pu)))
      {
        TBSYS_LOG(WARN, "failed to add DB perf data, hash_ret=%d db_partition=%lu",
            hash_ret, db_partition);
        ret = OB_ERR_UNEXPECTED;
      }

      return ret;
    }

    void ObLogPerf::withdraw_db_partition(const uint64_t partition)
    {
      if (inited_)
      {
        last_db_perf_map_.erase(partition);
        db_perf_map_.erase(partition);
      }
    }

    int ObLogPerf::operator() (const char *tb_name, void * args)
    {
      UNUSED(args);

      PerfUnit pu;
      pu.reset();
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = NULL;
      const ObTableSchema *table_schema = NULL;

      if (NULL == tb_name)
      {
        TBSYS_LOG(ERROR, "%s(): invalid arguments", __FUNCTION__);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(ERROR, "schema_getter_ get NULL schema");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(tb_name)))
      {
        TBSYS_LOG(ERROR, "table %s schema is NULL", tb_name);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        uint64_t table_id = table_schema->get_table_id();

        // Overwrite pattern
        int flag = 1;
        int hash_ret = tb_perf_map_.set(table_id, pu, flag);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(WARN, "failed to add perf data into table perf map, tb_name=%s, table_id=%lu hash_ret=%d",
              tb_name, table_id, hash_ret);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (-1 == (hash_ret = last_tb_perf_map_.set(table_id, pu, flag)))
        {
          TBSYS_LOG(WARN, "failed to add perf data into table perf map, tb_name=%s, table_id=%lu hash_ret=%d",
              tb_name, table_id, hash_ret);
          ret = OB_ERR_UNEXPECTED;
        }

      }

      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }

      TABLE_PERF_DEBUG();

      return ret;
    }

    void ObLogPerf::destroy()
    {
      stop();

      inited_ = false;
      db_perf_map_.destroy();
      tb_perf_map_.destroy();
      last_db_perf_map_.destroy();
      last_tb_perf_map_.destroy();
      total_trans_count_ = 0;
      last_total_trans_count_ = 0;
      last_print_time_ = 0;
      schema_getter_ = NULL;
    }

    int ObLogPerf::start()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "ObLogPerf has not been initialized");
        ret = OB_NOT_INIT;
      }
      else
      {
        started_ = true;
      }

      return ret;
    }

    void ObLogPerf::stop()
    {
      started_ = false;
    }

    int ObLogPerf::add_perf_(PerfMap &perf_map, const uint64_t key, const PerfUnit &pu)
    {
      int ret = OB_SUCCESS;

      if (! started_)
      {
        TBSYS_LOG(WARN, "ObLogPerf has not been started");
        ret = OB_NOT_SUPPORTED;
      }
      else
      {
        PerfUnit tmp_pu;
        tmp_pu.reset();

        int hash_ret = perf_map.get(key, tmp_pu);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(WARN, "failed to get perf data, key=%lu, hast_ret=%d", key, hash_ret);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          if (hash::HASH_NOT_EXIST == hash_ret)
          {
            tmp_pu.reset();
          }

          tmp_pu.trans_count_ += pu.trans_count_;
          tmp_pu.record_count_ += pu.record_count_;

          // Overwrite pattern
          int flag = 1;
          hash_ret = perf_map.set(key, tmp_pu, flag);
          if (-1 == hash_ret)
          {
            TBSYS_LOG(WARN, "failed to add perf data, key=%lu, hash_ret=%d", key, hash_ret);
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }

      return ret;
    }

    int ObLogPerf::add_db_perf(const uint64_t db_partition, const PerfUnit &pu)
    {
      return add_perf_(db_perf_map_, db_partition, pu);
    }

    int ObLogPerf::add_tb_perf(const uint64_t table_id, const PerfUnit &pu)
    {
      int ret = add_perf_(tb_perf_map_, table_id, pu);

      TABLE_PERF_DEBUG();

      return ret;
    }

    int ObLogPerf::set_total_trans_count(const uint64_t trans_count)
    {
      int ret = OB_SUCCESS;

      if (! started_)
      {
        TBSYS_LOG(WARN, "ObLogPerf has not been started");
        ret = OB_NOT_SUPPORTED;
      }
      else
      {
        if (total_trans_count_ > trans_count)
        {
          TBSYS_LOG(WARN, "%s(): total transaction count is inverted: "
              "old value = %lu, new value = %lu",
              __FUNCTION__, total_trans_count_, trans_count);
        }

        total_trans_count_ = trans_count;
      }

      return ret;
    }

    void ObLogPerf::print_perf_info()
    {
      int ret = OB_SUCCESS;

      if (! started_)
      {
        TBSYS_LOG(WARN, "ObLogPerf has not been started");
      }
      else if (0 == last_print_time_)
      {
        last_print_time_ = tbsys::CTimeUtil::getTime();

        ret = sync_perf_data_to_last_();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to sync perf data to the last, ret=%d", ret);
        }
      }
      else
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        int64_t delta_time = cur_time - last_print_time_;

        if (MIN_PRINT_INTERVAL_TIME > delta_time)
        {
          TBSYS_LOG(WARN, "%s(): print too fast! Interval=%ld, MIN_PRINT_INTERVAL_TIME=%ld",
              __FUNCTION__, delta_time, MIN_PRINT_INTERVAL_TIME);
        }
        else
        {
          PRINT_PERF("======================= * STATISTICS * =======================");
          char buf[30];
          time_t tm = time(NULL);
          (void)ctime_r(&tm, buf);
          buf[24] = '\0';
          PRINT_PERF("| %s", buf);
          PRINT_PERF("|---------------------- * TOTAL VIEW * -----------------------");
          PRINT_PERF("| INTERVAL:  %7.2f seconds", static_cast<double>(cur_time - last_print_time_) / 1e6);
          PRINT_PERF("| TOTAL TPS: %7.2f (Fetched From OceanBase)",
              compute_rate_(total_trans_count_, last_total_trans_count_, cur_time, last_print_time_));

          PRINT_PERF("|-------------------------------------------------------------");
          PRINT_PERF("|");
          PRINT_PERF("|------------------- * DB PARTITION VIEW * -------------------");

          bool is_db_perf_map = true;
          ret = print_perf_map_(is_db_perf_map, cur_time);
          if (OB_SUCCESS != ret)
          {
            PRINT_PERF("ERROR: failed to print DB perf data, ret=%d", ret);
          }
          else
          {
            PRINT_PERF("|-------------------------------------------------------------");
            PRINT_PERF("|");
            PRINT_PERF("|---------------------- * TABLE VIEW * -----------------------");
            is_db_perf_map = false;
            ret = print_perf_map_(is_db_perf_map, cur_time);
            if (OB_SUCCESS != ret)
            {
              PRINT_PERF("ERROR: failed to print table perf data, ret=%d", ret);
            }
            else
            {
              PRINT_PERF("|-------------------------------------------------------------");
              PRINT_PERF("|");
              PRINT_PERF("===================== * END STATISTICS * =====================");
              last_print_time_ = cur_time;
              ret = sync_perf_data_to_last_();
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "failed to sync perf data to the last, ret=%d", ret);
              }
            }
          }
        }
      }
    }

    int ObLogPerf::sync_perf_data_to_last_()
    {
      int ret = OB_SUCCESS;

      last_total_trans_count_ = total_trans_count_;

      PerfMap::iterator iter;

      for (iter = db_perf_map_.begin(); OB_SUCCESS == ret && iter != db_perf_map_.end(); iter++)
      {
        // Overwrite pattern
        int flag = 1;
        int hash_ret = last_db_perf_map_.set(iter->first, iter->second, flag);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(ERROR, "failed to add perf data into last DB perf map, "
              "db_partition=%lu, trans_count=%lu, record_count=%lu, hash_ret=%d",
              iter->first, iter->second.trans_count_, iter->second.record_count_, hash_ret);

          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }

      for (iter = tb_perf_map_.begin(); OB_SUCCESS == ret && iter != tb_perf_map_.end(); iter++)
      {
        // Overwrite pattern
        int flag = 1;
        int hash_ret = last_tb_perf_map_.set(iter->first, iter->second, flag);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(ERROR, "failed to add perf data into last table perf map, "
              "table_id=%lu, trans_count=%lu, record_count=%lu, hash_ret=%d",
              iter->first, iter->second.trans_count_, iter->second.record_count_, hash_ret);

          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }

      return ret;
    } // end of sync_perf_data_to_last_

    int ObLogPerf::print_perf_map_(bool is_db_perf_map, int64_t cur_time)
    {
      OB_ASSERT(started_);

      int hash_ret = 0;
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = NULL;
      const ObTableSchema *table_schema = NULL;

      PerfMap &perf_map =  is_db_perf_map ? db_perf_map_ : tb_perf_map_;
      PerfMap &last_perf_map = is_db_perf_map ? last_db_perf_map_ : last_tb_perf_map_;

      PerfUnit pu;
      pu.reset();

      // Prepare schema only for table perf map
      if (! is_db_perf_map)
      {
        if (NULL == (total_schema = schema_getter_->get_schema()))
        {
          TBSYS_LOG(ERROR, "schema_getter_ get NULL schema");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      PerfMap::iterator iter;
      for (iter = perf_map.begin(); OB_SUCCESS == ret && iter != perf_map.end(); iter++)
      {
        hash_ret = last_perf_map.get(iter->first, pu);
        if (hash::HASH_EXIST == hash_ret)
        {
          // Print DB perf map
          if (is_db_perf_map)
          {
            PRINT_PERF("| DB %4lu: TPS = %7.2f,      RPS = %7.2f",
                iter->first,
                compute_rate_(iter->second.trans_count_, pu.trans_count_, cur_time, last_print_time_),
                compute_rate_(iter->second.record_count_, pu.record_count_, cur_time, last_print_time_));
          }
          // Print table perf map
          else
          {
            const char *tb_name  = NULL;
            if (NULL == (table_schema = total_schema->get_table_schema(iter->first)))
            {
              TBSYS_LOG(ERROR, "table %lu schema is NULL", iter->first);
              ret = OB_ERR_UNEXPECTED;
              break;
            }

            if (NULL == (tb_name = table_schema->get_table_name()))
            {
              TBSYS_LOG(ERROR, "table %lu name is NULL", iter->first);
              ret = OB_ERR_UNEXPECTED;
              break;
            }

            PRINT_PERF("| TABLE %4s: TPS = %7.2f,      RPS = %7.2f",
                tb_name,
                compute_rate_(iter->second.trans_count_, pu.trans_count_, cur_time, last_print_time_),
                compute_rate_(iter->second.record_count_, pu.record_count_, cur_time, last_print_time_));
          }
        }
        else if (hash::HASH_NOT_EXIST == hash_ret)
        {
          TBSYS_LOG(WARN, "%s %lu does not exist in last perf map",
              is_db_perf_map ? "DB" : "Table",
              iter->first);
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to get %s %lu last perf data",
              is_db_perf_map ? "DB" : "Table",
              iter->first);

          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }

      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }

      return ret;
    } // end of print_perf_map_
  } // end of namespace liboblog
} // end of namespace oceanbase
