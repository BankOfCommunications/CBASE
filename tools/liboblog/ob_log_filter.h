////===================================================================
 //
 // ob_log_filter.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_FILTER_H_
#define  OCEANBASE_LIBOBLOG_FILTER_H_

#include "common/ob_define.h"
#include "common/ob_log_entry.h"         // LogCommand
#include "common/hash/ob_hashset.h"
#include "updateserver/ob_ups_mutator.h"
#include "ob_log_config.h"
#include "ob_log_meta_manager.h"
#include "common/ob_mutator.h" // ObMutatorCellInfo

namespace oceanbase
{
  namespace liboblog
  {
    struct FilterInfo
    {
      bool is_valid_;
      bool contain_table_selected_;
      bool contain_ddl_operations_;

      void reset()
      {
        is_valid_ = false;
        contain_table_selected_ = true;
        contain_ddl_operations_ = true;
      }

      void set(bool contain_table_selected, bool contain_ddl_operations)
      {
        contain_table_selected_ = contain_table_selected;
        contain_ddl_operations_ = contain_ddl_operations;
        is_valid_ = true;
      }
    };

    class ObLogMutator : public updateserver::ObUpsMutator
    {
      public:
        static const int64_t UPS_NOP_LOG_INTERVAL = 500L * 1000L;

      public:
        ObLogMutator()
          : is_heartbeat_(false),
            log_id_(0),
            num_(0),
            db_partition_(0),
            cmd_(common::OB_LOG_UNKNOWN),
            signature_(-1),
            min_process_timestamp_(-1)
        {
          filter_info_.reset();
          signature_ = tbsys::CTimeUtil::getTime();
          min_process_timestamp_ = tbsys::CTimeUtil::getTime();
        }

        ~ObLogMutator() { reset(); }
      public:
        void set_log_id(const uint64_t log_id) { log_id_ = log_id; }
        uint64_t get_log_id() const { return log_id_; }
        void set_num(const uint64_t num) {num_ = num;}
        uint64_t get_num() const {return num_;}

        void set_heartbeat(bool is_heartbeat) { is_heartbeat_ = is_heartbeat; }
        bool is_heartbeat() const { return is_heartbeat_; }

        void set_db_partition(const uint64_t db_partition) { db_partition_ = db_partition; }
        uint64_t get_db_partition() const { return db_partition_; }

        void set_cmd_type(const common::LogCommand &cmd) { cmd_ = cmd; }
        const common::LogCommand &get_cmd_type() const { return cmd_; }

        bool is_nop_mutator() const
        {
          return (! is_heartbeat_) && common::OB_LOG_NOP == cmd_;
        }

        bool is_dml_mutator() const
        {
          return (! is_heartbeat_) && common::OB_LOG_UPS_MUTATOR == cmd_ && is_normal_mutator();
        }

        void set_signature(int64_t signature) { signature_ = signature; }
        int64_t get_signature() const { return signature_; }

        void set_min_process_timestamp(int64_t min_process_timestamp) { min_process_timestamp_ = min_process_timestamp; }
        int64_t get_min_process_timestamp() const { return min_process_timestamp_; }

        void reset()
        {
          is_heartbeat_ = false;
          log_id_ = 0;
          num_ = 0;
          db_partition_ = 0;
          cmd_ = common::OB_LOG_UNKNOWN;
          filter_info_.reset();
          signature_ = -1;
          min_process_timestamp_ = -1;

          clear();
        }

        bool is_filter_info_valid() const { return filter_info_.is_valid_; }

        void set_filter_info(bool contain_table_selected, bool contain_ddl_operations)
        {
          filter_info_.set(contain_table_selected, contain_ddl_operations);
        }

        const FilterInfo &get_filter_info() const { return filter_info_; }

        /// @brief Get mutator cell
        /// @param[out] cell returned cell
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when errors occur
        int get_mutator_cell(common::ObMutatorCellInfo **cell);

        /// @brief Get mutator cell
        /// @param[out] cell returned cell
        /// @param[out] is_row_changed bool value used to identify whether is row changed
        /// @param[out] is_row_finished bool value used to identify whether is row finished
        /// @param[out] dml_type DML operation type
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when errors occur
        int get_mutator_cell(common::ObMutatorCellInfo** cell,
            bool* is_row_changed,
            bool* is_row_finished,
            common::ObDmlType *dml_type);

      public:
        bool     is_heartbeat_;   ///< heartbeat mark
        uint64_t log_id_;
        uint64_t num_;
        uint64_t db_partition_;
        common::LogCommand cmd_;          ///< Log Command Type

        FilterInfo filter_info_;

        int64_t signature_;

        // NOTE: If current timestamp is little than min_process_timestamp_, this mutator
        // can not be processed by ROUTER
        int64_t min_process_timestamp_;       ///< minmum process timestamp for ROUTER
    };

    class IObLogFilter
    {
      public:
        virtual ~IObLogFilter() {};
      public:
        virtual int init(IObLogFilter *filter,
            const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter) {UNUSED(filter); UNUSED(config); UNUSED(schema_getter); return common::OB_SUCCESS;};

        virtual void destroy() = 0;

        virtual int next_mutator(ObLogMutator **mutator, const int64_t timeout) = 0;

        virtual void release_mutator(ObLogMutator *mutator) = 0;

        virtual bool contain(const uint64_t table_id) = 0;

        virtual uint64_t get_trans_count() const = 0;

        virtual int64_t get_last_mutator_timestamp() = 0;
    };

    class ObLogTableFilter : public IObLogFilter
    {
      typedef common::hash::ObHashSet<uint64_t> TIDFilter;
      public:
        ObLogTableFilter();
        ~ObLogTableFilter();
      public:
        // TODO support erase table then re-add the same
        int init(IObLogFilter *filter,
            const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter);
        void destroy();
        /// @note next_mutator will return OB_INVALID_DATA when get a mutator with invalid data
        int next_mutator(ObLogMutator **mutator, const int64_t timeout);
        void release_mutator(ObLogMutator *mutator);
        bool contain(const uint64_t table_id);

        uint64_t get_trans_count() const
        {
          uint64_t trans_count = 0;

          if (inited_ && NULL != prev_)
            trans_count = prev_->get_trans_count();

          return trans_count;
        }

        /// @brief Get last mutator's timestamp including filtered mutator
        /// @return asked timestamp or 0 when not initialized
        int64_t get_last_mutator_timestamp()
        {
          return inited_ ? last_mutator_timestamp_ : 0;
        }

      public:
        int operator ()(const char *tb_name, const ObLogSchema *total_schema);
      private:
        /// @brief Filter mutator to find that whether mutator is filled with table data we selected
        /// @param mutator target mutator
        /// @retval true while being filtered
        /// @retval false while being no filtered
        int filter_mutator_(ObLogMutator &mutator) const;
        int init_tid_filter_(const ObLogConfig &config, IObLogSchemaGetter &schema_getter);

        /// @brief Wait suitable time to process target mutator
        /// @param mutator target mutator
        void wait_to_process_mutator_(const ObLogMutator &mutator) const;
      private:
        bool inited_;
        IObLogFilter *prev_;
        TIDFilter tid_filter_;
        volatile int64_t last_mutator_timestamp_;
    };

  }
}

#endif //OCEANBASE_LIBOBLOG_FILTER_H_

