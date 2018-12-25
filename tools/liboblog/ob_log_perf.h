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
 //   This file defines data structures for performance statistics.
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_LIBOBLOG_PERF_H_
#define  OCEANBASE_LIBOBLOG_PERF_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_config.h"
#include "ob_log_schema_getter.h"

namespace oceanbase
{
  using namespace oceanbase::common;

  namespace liboblog
  {
    struct PerfUnit
    {
      uint64_t trans_count_;    ///< transaction count
      uint64_t record_count_;   ///< records count

      void reset()
      {
        trans_count_ = 0;
        record_count_ = 0;
      }
    };

    class ObLogPerf
    {
      public:
        static const int64_t MIN_PRINT_INTERVAL_TIME = 500000;
        typedef common::hash::ObHashMap<uint64_t, PerfUnit> PerfMap;

      public:
        ObLogPerf();
        virtual ~ObLogPerf();

      public:
        int init(ObLogConfig &config, IObLogSchemaGetter *schema_getter);
        void destroy();
        int start();
        void stop();

        /// @brief Register DB partition and initialize DB perf map
        /// @param db_partition target DB partition number
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval !OB_SUCCESS on fail
        /// @note This function should be called after init() and before start()
        int register_db_partition(const uint64_t db_partition);

        /// @brief Withdraw DB perf map entry
        /// @param db_partition target DB partition number
        void withdraw_db_partition(const uint64_t db_partition);

        /// @brief Callback function for ObLogConfig::parse_tb_select() used to initialize table perf map
        /// @param tb_name table name
        /// @param args unused argument
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int operator() (const char *tb_name, void* args);

        /// @brief Add DB perf data
        /// @param db_partition DataBase partition number
        /// @param pu target perf data
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_NOT_SUPPORTED on not started
        /// @retval OB_ERR_UNEXPECTED on other unexpected error
        int add_db_perf(const uint64_t db_partition, const PerfUnit &pu);

        /// @brief Add table perf data
        /// @param table_id table ID
        /// @param pu target perf data
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_NOT_SUPPORTED on not started
        /// @retval OB_ERR_UNEXPECTED on other unexpected error
        int add_tb_perf(const uint64_t table_id, const PerfUnit &pu);

        /// @brief Set total transaction count
        /// @param trans_count target transaction count
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_NOT_SUPPORTED on not started
        int set_total_trans_count(const uint64_t trans_count);

        /// @brief Print perf information into LOG and stderr
        void print_perf_info();

        void set_print_to_stderr(bool print_to_stderr)
        {
          print_to_stderr_ = print_to_stderr;
        }

      private:
        /// @brief Common function to add data to perf map
        /// @param perf_map target perf map
        /// @param key key value
        /// @param pu target value
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_NOT_SUPPORTED on not started
        /// @retval OB_ERR_UNEXPECTED on other unexpected error
        int add_perf_(PerfMap &perf_map, const uint64_t key, const PerfUnit &pu);

        /// @brief Print perf map: DB perf map or table perf map
        /// @param is_db_perf_map whether to print DB perf map, otherwise print table perf map
        /// @param cur_time current time
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int print_perf_map_(bool is_db_perf_map, int64_t cur_time);

        /// @brief Sync current perf data to last perf data
        /// @return sync state
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int sync_perf_data_to_last_();

        /// @brief Common function to compute rate
        /// @param cur_value current value
        /// @param last_value last value
        /// @param cur_time current time
        /// @param last_time last time
        /// @return rate
        inline double compute_rate_(const uint64_t cur_value,
            const uint64_t last_value,
            const int64_t cur_time,
            const int64_t last_time) const
        {
          double delta_value = static_cast<double>(cur_value - last_value);
          double delta_time = static_cast<double>(cur_time - last_time) / 1000000;

          return (0 == delta_time) ? 0 : (delta_value / delta_time);
        }

     // Private member variables
      private:
        PerfMap db_perf_map_;     ///< HashMap: DB partition number <--> PerfUnit
        PerfMap tb_perf_map_;     ///< HashMap: table name <--> PerfUnit
        uint64_t total_trans_count_;

        PerfMap last_db_perf_map_;
        PerfMap last_tb_perf_map_;
        uint64_t last_total_trans_count_;

        int64_t last_print_time_;
        bool inited_;
        bool started_;
        bool print_to_stderr_;
        IObLogSchemaGetter *schema_getter_;
    }; // end of class ObLogPerf
  } // end of namespace liboblog
} // end of namespace oceanbase
#endif // end of OCEANBASE_LIBOBLOG_PERF_H_
