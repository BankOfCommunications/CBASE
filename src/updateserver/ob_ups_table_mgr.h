/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ups_tablet_mgr.h,v 0.1 2010/09/14 10:11:15 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_UPS_TABLET_MGR_H__
#define __OCEANBASE_CHUNKSERVER_UPS_TABLET_MGR_H__

#include "tbrwlock.h"
#include "common/ob_define.h"
#include "common/ob_spin_lock.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/bloom_filter.h"
#include "common/ob_schema.h"
#include "common/ob_merger.h"
#include "ob_ups_mutator.h"
#include "ob_memtable.h"
#include "ob_schema_mgrv2.h"
#include "ob_ups_utils.h"
#include "ob_table_mgr.h"
#include "ob_ups_cache.h"
#include "common/ob_meta_cache.h"
#include "common/ob_token.h"
#include "ob_trans_mgr.h"
#include "ob_ups_log_utils.h"
#include "ob_sessionctx_factory.h"
#include "common/ob_new_scanner.h"

namespace oceanbase
{
  namespace updateserver
  {
    // UpsTableMgr manages the active and frozen memtable of UpdateServer.
    // It also acts as an entry point for apply/replay/get/scan operation.
    class ObUpsRpcStub;
    struct UpsTableMgrTransHandle
    {
      MemTableTransDescriptor trans_descriptor;
      TableItem *cur_memtable;
      UpsTableMgrTransHandle() : trans_descriptor(0), cur_memtable(NULL)
      {
      };
    };
    const static int64_t MT_REPLAY_OP_NULL = 0x0;
    const static int64_t MT_REPLAY_OP_CREATE_INDEX = 0x0000000000000001;
    const static int64_t FLAG_MAJOR_LOAD_BYPASS = 0x0000000000000001;
    const static int64_t FLAG_MINOR_LOAD_BYPASS = 0x0000000000000002;
    class ObIUpsTableMgr
    {
      public:
        virtual ~ObIUpsTableMgr() {};
      public:
        virtual int apply(RWSessionCtx &session_ctx, common::ObIterator &iter, const common::ObDmlType dml_type) = 0;
        virtual UpsSchemaMgr &get_schema_mgr() = 0;
    };
    class ObUpsTableMgr : public ObIUpsTableMgr
    {
      friend class TestUpsTableMgrHelper;
      friend bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key);
      const static int64_t LOG_BUFFER_SIZE = 1024 * 1024 * 2;
      public:
      struct FreezeParamHeader
      {
        int32_t version;
        int32_t reserve1;
        int64_t reserve2;
        int64_t reserve3;
        char buf[];
      };
      struct FreezeParamV1
      {
        static const int64_t Version = 1;
        uint64_t active_version;     // freeze之前的activce memtable的version
        uint64_t new_log_file_id;
        int64_t op_flag;
      };
      struct FreezeParamV2
      {
        static const int64_t Version = 2;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;
        uint64_t new_log_file_id;
        int64_t op_flag;
      };
      struct FreezeParamV3
      {
        static const int64_t Version = 3;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;
        uint64_t new_log_file_id;
        int64_t time_stamp;
        int64_t op_flag;
      };
      struct FreezeParamV4
      {
        static const int64_t Version = 4;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;     // 新版本的active_version和frozen_version 16:16:32个字节表示
        uint64_t new_log_file_id;
        int64_t time_stamp;
        int64_t op_flag;
      };
      struct CurFreezeParam
      {
        typedef FreezeParamV4 FreezeParam;
        FreezeParamHeader header;
        FreezeParam param;
        CurFreezeParam()
        {
          memset(this, 0, sizeof(CurFreezeParam));
          header.version = FreezeParam::Version;
        };
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
        {
          int ret = common::OB_SUCCESS;
          if ((pos + (int64_t)sizeof(*this)) > buf_len)
          {
            ret = common::OB_ERROR;
          }
          else
          {
            memcpy(buf + pos, this, sizeof(*this));
            pos += sizeof(*this);
          }
          return ret;
        };
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
        {
          int ret = common::OB_SUCCESS;
          if ((pos + (int64_t)sizeof(*this)) > buf_len)
          {
            ret = common::OB_ERROR;
          }
          else
          {
            memcpy(this, buf + pos, sizeof(*this));
            pos += sizeof(*this);
          }
          return ret;
        };
      };
      struct DropParamHeader
      {
        int32_t version;
        int32_t reserve1;
        int64_t reserve2;
        int64_t reserve3;
        char buf[];
      };
      struct DropParamV1
      {
        int64_t frozen_version;     // 要drop掉的frozen memtable的version
      };
      struct CurDropParam
      {
        DropParamHeader header;
        DropParamV1 param;
        CurDropParam()
        {
          memset(this, 0, sizeof(CurDropParam));
          header.version = 1;
        };
      };

      public:
        ObUpsTableMgr(common::ObILogWriter &log_writer);
        ~ObUpsTableMgr();
        int init();
        int reg_table_mgr(SSTableMgr &sstable_mgr);
        inline TableMgr* get_table_mgr()
        {
          return &table_mgr_;
        }

      public:
        int add_memtable_uncommited_checksum(uint64_t *checksum);
        int check_checksum(const uint64_t checksum2check,
                           const uint64_t checksum_before_mutate,
                           const uint64_t checksum_after_mutate);
        int start_transaction(const MemTableTransType type,
                              UpsTableMgrTransHandle &handle);
        int end_transaction(UpsTableMgrTransHandle &handle, bool rollback);
        int pre_process(const bool using_id, common::ObMutator& ups_mutator, const common::IToken *token);
        int apply(const bool using_id, UpsTableMgrTransHandle &handle, ObUpsMutator &ups_mutator, common::ObScanner *scanner);
        int apply(const bool using_id, RWSessionCtx &session_ctx, ILockInfo &lock_info, common::ObMutator &mutator);
        int apply(RWSessionCtx &session_ctx, common::ObIterator &iter, const common::ObDmlType dml_type);
        int replay(ObUpsMutator& ups_mutator, const ReplayType replay_type);
        int replay_mgt_mutator(ObUpsMutator& ups_mutator, const ReplayType replay_type);
        int set_schemas(const CommonSchemaManagerWrapper &schema_manager);
        int switch_schemas(const CommonSchemaManagerWrapper &schema_manager);
        //add zhaoqiong [Schema Manager] 20150327:b

        /**
         * @brief for replay/apply log OB_UPS_WRITE_SCHEMA_NEXT,
         *        freeze memtable, follow schema log
         *        if this is last part, do replay_freeze_memtable
         * @param buf:log_data
         * @param buf_len:log_data_len
         * @param pos: deserialize start pos of buf
         * @param replay_type: RT_LOCAL/RT_APPLY
         * @return OB_SUCCESS on success
         */
        int replay_mgt_next(const char* buf, const int64_t buf_len, int64_t& pos, const ReplayType replay_type);

        /**
         * @brief for replay/apply log OB_UPS_SWITCH_SCHEMA_NEXT,
         *        the follow schema part, should append to local schema
         * @param buf:log_data
         * @param data_len:log_data_len
         * @param pos: deserialize start pos of buf
         * @return OB_SUCCESS on success
         */
        int set_schema_next(const char* buf, const int64_t data_len, int64_t& pos);

        /**
         * @brief recevie schema mutator from RS,
         *        apply schema_mutator to local schema
         *        and write schema mutator log
         * @param schema_mutator: schema mutator
         * @return OB_SUCCESS on success
         */
        int switch_schema_mutator(const ObSchemaMutator &schema_mutator);
        /**
         * @brief get current schema_version
         * @return schema_version
         */
        int64_t get_schema_version() {return schema_mgr_.get_version();}

        /**
         * @brief reference of tmp_schema_mgr_
         */
        CommonSchemaManagerWrapper &get_tmp_schema() {return tmp_schema_mgr_;}

        /**
         * @brief assign tmp_schema_mgr_ to schema_mgr_
         * @return
         */
        int switch_tmp_schemas();
        bool lock_tmp_schema(){return __sync_bool_compare_and_swap(&tmp_schema_mutex_,0,1);}
        bool unlock_tmp_schema(){return __sync_bool_compare_and_swap(&tmp_schema_mutex_,1,0);}
        //add:e
        int get_active_memtable_version(uint64_t &version);

        //add zhaoqiong [Truncate Table]:20170704:b
        int get_last_truncate_timestamp_in_active(uint32_t uid, uint64_t table_id, int64_t &timestamp);
		//add:e
        int get_last_frozen_memtable_version(uint64_t &version);
        int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp);
        int get_oldest_memtable_size(int64_t &size, uint64_t &major_version);
        UpsSchemaMgr &get_schema_mgr()
        {
          return schema_mgr_;
        };
        void dump_memtable(const common::ObString &dump_dir);
        void dump_schemas();

        void set_replay_checksum_flag(const bool if_check)
        {
          TBSYS_LOG(INFO, "replay checksum flag switch from %s to %s", STR_BOOL(check_checksum_), STR_BOOL(if_check));
          check_checksum_ = if_check;
        }

        // do not impl in ups v0.2
        int create_index();
        int get_frozen_bloomfilter(const uint64_t version, common::TableBloomFilter &table_bf);

      public:
        // Gets a list of cells.
        //
        // @param [in] get_param param used to get data
        // @param [out] scanner result data of get operation.
        // @return OB_SUCCESS if success, other error code if error occurs.
        int get(const common::ObGetParam& get_param, ObScanner& scanner, const int64_t start_time, const int64_t timeout);
        int get(const BaseSessionCtx &session_ctx,
                const common::ObGetParam& get_param,
                common::ObScanner& scanner,
                const int64_t start_time,
                const int64_t timeout);
        // Scans row range.
        //
        // @param [in] scan_param param used to scan data
        // @param [out] scanner result data of scan operation
        // @return OB_SUCCESS if success, other error code if error occurs.
        int scan(const common::ObScanParam& scan_param, ObScanner& scanner, const int64_t start_time, const int64_t timeout);
        int scan(const BaseSessionCtx &session_ctx,
                const common::ObScanParam &scan_param,
                common::ObScanner &scanner,
                const int64_t start_time,
                const int64_t timeout);

        int new_get(const common::ObGetParam& get_param, common::ObCellNewScanner& scanner, const int64_t start_time, const int64_t timeout);
        int new_get(BaseSessionCtx &session_ctx,
                    const common::ObGetParam& get_param,
                    common::ObCellNewScanner& scanner,
                    const int64_t start_time,
                    const int64_t timeout,
                    const sql::ObLockFlag lock_flag = sql::LF_NONE);

        int new_scan(const common::ObScanParam& scan_param, common::ObCellNewScanner& scanner, const int64_t start_time, const int64_t timeout);
        int new_scan(BaseSessionCtx &session_ctx,
                    const common::ObScanParam &scan_param,
                    common::ObCellNewScanner &scanner,
                    const int64_t start_time,
                    const int64_t timeout);

        //virtual int rpc_get(common::ObGetParam &get_param, common::ObScanner &scanner, const int64_t timeouut);


        int load_sstable_bypass(SSTableMgr &sstable_mgr, int64_t &loaded_num);
        int check_cur_version();
        int commit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum);

        void update_merged_version(ObUpsRpcStub &rpc_stub, const common::ObServer &root_server, const int64_t timeout_us);

      public:
        bool need_auto_freeze() const;
        int freeze_memtable(const TableMgr::FreezeType freeze_type, uint64_t &frozen_version, bool &report_version_changed,
                            const common::ObPacket *resp_packet = NULL);
        void store_memtable(const bool all);
        void drop_memtable(const bool force);
        void erase_sstable(const bool force);
        void get_memtable_memory_info(TableMemInfo &mem_info);
        void log_memtable_memory_info();
        void set_memtable_attr(const MemTableAttr &memtable_attr);
        int get_memtable_attr(MemTableAttr &memtable_attr);
        void update_memtable_stat_info();
        int clear_active_memtable();
        int sstable_scan_finished(const int64_t minor_num_limit);
        int check_sstable_id();
        void log_table_info();
        template <typename T>
        int flush_obj_to_log(const common::LogCommand log_command, T &obj);
        int write_start_log();
        void set_warm_up_percent(const int64_t warm_up_percent);
        int get_schema(const uint64_t major_version, CommonSchemaManagerWrapper &sm);
        int get_sstable_range_list(const uint64_t major_version, const uint64_t table_id, TabletInfoList &ti_list);
        int fill_commit_log(ObUpsMutator &ups_mutator, TraceLog::LogBuffer &tlog_buffer);
        int flush_commit_log(TraceLog::LogBuffer &tlog_buffer);

      private:
        int write_schema(const CommonSchemaManagerWrapper &schema_manager);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief write schema mutator to ups log
         */
        int write_schema_mutator(const ObSchemaMutator &schema_mutator);
        //add:e
        int set_mutator_(ObUpsMutator &mutator);
        template<class T>
        int get_(const common::ObGetParam& get_param, T& scanner, const int64_t start_time, const int64_t timeout);
        template<class T>
        int get_(const BaseSessionCtx &session_ctx,
                const common::ObGetParam& get_param,
                T& scanner,
                const int64_t start_time,
                const int64_t timeout,
                const sql::ObLockFlag lock_flag = sql::LF_NONE);

        template<class T>
        int scan_(const common::ObScanParam& scan_param, T& scanner, const int64_t start_time, const int64_t timeout);
        template<class T>
        int scan_(const BaseSessionCtx &session_ctx,
                  const common::ObScanParam& scan_param,
                  T& scanner,
                  const int64_t start_time,
                  const int64_t timeout);

        template<class T>
        int get_(TableList &table_list, const common::ObGetParam& get_param, T& scanner,
                const int64_t start_time, const int64_t timeout);
        template<class T>
        int get_(const BaseSessionCtx &session_ctx,
                TableList &table_list,
                const common::ObGetParam& get_param,
                T& scanner,
                const int64_t start_time,
                const int64_t timeout,
                const sql::ObLockFlag lock_flag);

        template<class T>
        int get_row_(TableList &table_list, const int64_t first_cell_idx, const int64_t last_cell_idx,
            const common::ObGetParam& get_param, T& scanner,
            const int64_t start_time, const int64_t timeout);
        template<class T>
        int get_row_(const BaseSessionCtx &session_ctx,
                    TableList &table_list,
                    const int64_t first_cell_idx,
                    const int64_t last_cell_idx,
                    const common::ObGetParam& get_param,
                    T& scanner,
                    const int64_t start_time,
                    const int64_t timeout,
                    const sql::ObLockFlag lock_flag);

        template<class T>
        int add_to_scanner_(common::ObIterator& ups_merger, T& scanner,
            int64_t& row_count, const int64_t start_time, const int64_t timeout, const int64_t result_limit_size = UINT64_MAX);
      private:
        int check_permission_(common::ObMutator &mutator, const common::IToken &token);
        int trans_name2id_(common::ObMutator &mutator);
        int fill_commit_log_(ObUpsMutator &ups_mutator, TraceLog::LogBuffer &tlog_buffer);
        int flush_commit_log_(TraceLog::LogBuffer &tlog_buffer);
        int handle_freeze_log_(ObUpsMutator &ups_mutator, const ReplayType replay_type);

      private:
        static const int64_t RPC_RETRY_TIMES = 3;             // rpc retry times used by client wrapper
        static const int64_t RPC_TIMEOUT = 2 * 1000L * 1000L; // rpc timeout used by client wrapper

      private:
        ThreadSpecificBuffer my_thread_buffer_;
        char *log_buffer_;
        common::ObSpinLock schema_lock_;
        UpsSchemaMgr schema_mgr_;
        TableMgr table_mgr_;
        bool check_checksum_;
        bool has_started_;
        uint64_t last_bypass_checksum_;
        //add zhaoqiong [Schema Manager] 20150327:b
        common::ObSpinLock  tmp_schema_lock_;//used for lock tmp_schema when replay log
        int32_t tmp_schema_mutex_;//used for lock tmp_schema when get full schema from system table
        CommonSchemaManagerWrapper tmp_schema_mgr_;//save part schema from replay log
        //add:e

    };
  }
}

#endif //__UPS_TABLET_MGR_H__

