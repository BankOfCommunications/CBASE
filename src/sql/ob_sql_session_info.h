/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_session_info.h is for what ...
 *
 * Version: ***: ob_sql_session_info.h  Mon Oct  8 10:30:53 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SESSION_INFO_H_
#define OB_SQL_SESSION_INFO_H_

#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/hash/ob_hashmap.h"
#include "ob_result_set.h"
#include "WarningBuffer.h"
#include "common/ob_stack_allocator.h"
#include "common/ob_range.h"
#include "common/ob_list.h"
#include "common/page_arena.h"
#include "common/ob_pool.h"
#include "easy_io_struct.h"
#include "common/ob_pooled_allocator.h"
#include "common/ob_cur_time.h"
#include "ob_ps_store.h"
#include "ob_sql_config_provider.h"
namespace oceanbase
{
  namespace sql
  {
    enum ObSQLSessionState
    {
      SESSION_SLEEP,
      QUERY_ACTIVE,
      QUERY_KILLED,
      SESSION_KILLED
    };

    class ObPsSessionInfo
    {
    public:
      static const int64_t COMMON_PARAM_NUM = 12;
      //mod liuzy [datetime func] 20150910:b
      /*Exp: 修改cur_time_的初始化*/
//      ObPsSessionInfo(): sql_id_(0), cur_time_(NULL)
      ObPsSessionInfo(): sql_id_(0)
      {
        //add liuzy [datetime func] 20150910:b
        for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
        {
          cur_time_[i]->~ObObj();
          cur_time_[i] = NULL;
        }
      }
      //mod 20150910:e
      ~ObPsSessionInfo()
      {
        for (int64_t idx = 0; idx < params_.count(); ++idx)
        {
          param_pool_->free(params_.at(idx));
        }
      }

      void init(common::ObPooledAllocator<ObObj, common::ObWrapperAllocator> *param_pool)
      {
        param_pool_ = param_pool;
      }

      ObObj* alloc()
      {
        return param_pool_->alloc();
      }

      int64_t sql_id_;
      common::ObSEArray<obmysql::EMySQLFieldType, COMMON_PARAM_NUM> params_type_; /* params type */
      common::ObSEArray<common::ObObj*, COMMON_PARAM_NUM> params_;                /* params */
      //mod liuzy [datetime func] 20150911::b
      //pos 0:cur_time 1:cur_date 2:cur_time_hms
//      common::ObObj* cur_time_;
      common::ObObj* cur_time_[ObResultSet::CUR_SIZE];
      //mod 20150911:e
      common::ObPooledAllocator<ObObj, ObWrapperAllocator> *param_pool_;
    };

    class ObSQLSessionInfo: public common::ObVersionProvider
    {
      public:
        static const int64_t APPROX_MEM_USAGE_PER_SESSION = 256*1024L; // 256KB ~= 4 * OB_COMMON_MEM_BLOCK_SIZE
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint64_t, ObResultSet*>::AllocType, common::ObWrapperAllocator> IdPlanMapAllocer;
        typedef common::hash::ObHashMap<uint64_t,
                                        ObResultSet*,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<uint64_t>,
                                        common::hash::equal_to<uint64_t>,
                                        IdPlanMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > IdPlanMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, uint64_t>::AllocType, common::ObWrapperAllocator> NamePlanIdMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        uint64_t,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        NamePlanIdMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > NamePlanIdMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, common::ObObj>::AllocType, common::ObWrapperAllocator> VarNameValMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        common::ObObj,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        VarNameValMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > VarNameValMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, std::pair<common::ObObj*, common::ObObjType> >::AllocType, common::ObWrapperAllocator> SysVarNameValMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        std::pair<common::ObObj*, common::ObObjType>,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        SysVarNameValMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > SysVarNameValMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint32_t, ObPsSessionInfo*>::AllocType, common::ObWrapperAllocator> IdPsInfoMapAllocer;
        typedef common::hash::ObHashMap<uint32_t,
                                        ObPsSessionInfo*,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<uint32_t>,
                                        common::hash::equal_to<uint32_t>,
                                        IdPsInfoMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > IdPsInfoMap;
      public:
        ObSQLSessionInfo();
        ~ObSQLSessionInfo();

        int init(common::DefaultBlockAllocator &block_allocator);
        void destroy();
        int set_peer_addr(const char* addr);
        const char* get_peer_addr() const{return addr_;}
        uint64_t get_session_id() const{return session_id_;}
        void set_session_id(uint64_t id){session_id_ = id;}
        void set_conn(easy_connection_t *conn){conn_ = conn;}
        int set_ps_store(ObPsStore *store);
        void set_current_result_set(ObResultSet *cur_result_set){cur_result_set_ = cur_result_set;}
        void set_query_start_time(int64_t time) {cur_query_start_time_ = time;}
        int store_query_string(const common::ObString &stmt);
        const tbsys::WarningBuffer& get_warnings_buffer() const{return warnings_buf_;}
        uint64_t get_new_stmt_id(){return (common::atomic_inc(&next_stmt_id_));}
        IdPlanMap& get_id_plan_map(){return id_plan_map_;}
        SysVarNameValMap& get_sys_var_val_map(){return sys_var_val_map_;}
        ObResultSet* get_current_result_set(){return cur_result_set_;}
        const common::ObString get_current_query_string() const;
        int64_t get_query_start_time() const {return cur_query_start_time_;}
        const ObSQLSessionState get_session_state() const{return state_;}
        easy_connection_t *get_conn() const{return conn_;}
        const char* get_session_state_str()const;
        void set_session_state(ObSQLSessionState state) {state_ = state;}
        const common::ObString& get_user_name(){return user_name_;}
        common::ObStringBuf& get_parser_mem_pool(){return parser_mem_pool_;}
        common::StackAllocator& get_transformer_mem_pool(){return transformer_mem_pool_;}
        common::ObArenaAllocator* get_transformer_mem_pool_for_ps();
        void free_transformer_mem_pool_for_ps(common::ObArenaAllocator* arena);
        //int32_t read_lock() {return pthread_rwlock_rdlock(&rwlock_);}
        //int32_t write_lock() {return pthread_rwlock_wrlock(&rwlock_);}
        int32_t try_read_lock(){return pthread_rwlock_tryrdlock(&rwlock_);}
        int32_t try_write_lock(){return pthread_rwlock_trywrlock(&rwlock_);}
        int32_t unlock(){return pthread_rwlock_unlock(&rwlock_);}
        void mutex_lock() {return mutex_.lock();}
        void mutex_unlock() {return mutex_.unlock();}
        int store_plan(const common::ObString& stmt_name, ObResultSet& result_set);
        int remove_plan(const uint64_t& stmt_id);
        /**
         * 根据ObPsStoreItem信息 在session中插入一条pssessioninfo记录，如果这个session已经绑定过item->sql_id_对应的语句
         * 那么使用id_mgr 获取新id 返回新的id
         *
         */
        int store_ps_session_info(ObPsStoreItem *item, uint64_t &stmt_id);
        int store_ps_session_info(ObResultSet& result_set);

        int remove_ps_session_info(const uint64_t stmt_id);
        int close_all_stmt();
        int get_ps_session_info(const int64_t, ObPsSessionInfo*& info);
        int store_params_type(int64_t stmt_id, const common::ObIArray<obmysql::EMySQLFieldType> &params_type);
        int replace_variable(const common::ObString& var, const common::ObObj& val);
        int remove_variable(const common::ObString& var);
        int update_system_variable(const common::ObString& var, const common::ObObj& val);
        int load_system_variable(const common::ObString& name, const common::ObObj& type, const common::ObObj& value);
        int get_variable_value(const common::ObString& var, common::ObObj& val) const;
        int get_sys_variable_value(const common::ObString& var, common::ObObj& val) const;
        const common::ObObj* get_variable_value(const common::ObString& var) const;
        const common::ObObj* get_sys_variable_value(const common::ObString& var) const;
        bool variable_exists(const common::ObString& var);
        bool sys_variable_exists(const common::ObString& var);
        bool plan_exists(const common::ObString& stmt_name, uint64_t *stmt_id = NULL);
        ObResultSet* get_plan(const uint64_t& stmt_id) const;
        ObResultSet* get_plan(const common::ObString& stmt_name) const;
        int set_username(const common::ObString & user_name);
        void set_warnings_buf();
        int64_t to_string(char* buffer, const int64_t length) const;
        void set_trans_id(const common::ObTransID &trans_id){trans_id_ = trans_id;};
        const common::ObTransID& get_trans_id(){return trans_id_;};
        void set_version_provider(const common::ObVersionProvider *version_provider){version_provider_ = version_provider;};
        const common::ObVersion get_frozen_version() const {return version_provider_->get_frozen_version();};
        void set_config_provider(const ObSQLConfigProvider *config_provider){ config_provider_ = config_provider;}
        bool is_read_only() const {return config_provider_->is_read_only();};
        //add liumz, [multi_database.priv_manage]20150708:b
        bool is_regrant_priv() const {return config_provider_->is_regrant_priv();};
        //add:e
        /**
         * @pre 系统变量存在的情况下
         * @synopsis 根据变量名，取得这个变量的类型
         *
         * @param var_name
         *
         * @returns
         */
        common::ObObjType get_sys_variable_type(const common::ObString &var_name);
        void set_autocommit(bool autocommit) {is_autocommit_ = autocommit;};
        bool get_autocommit() const {return is_autocommit_;};
        void set_interactive(bool is_interactive) {is_interactive_ = is_interactive;}
        bool get_interactive() const { return is_interactive_; }
        // get system variable value
        bool is_create_sys_table_disabled() const;
        int update_session_timeout();
        void update_last_active_time() { last_active_time_ = tbsys::CTimeUtil::getTime(); }
        bool is_timeout();
        uint16_t get_charset();
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
        void set_read_times(BatchReadTimes read_times)
        {
            trans_id_.read_times_ = read_times ;
        }
        int64_t get_read_times()
        {
            return trans_id_.read_times_;
        }
   //add by maosy  20161210
    //add peiouya  [NotNULL_check] [JHOBv0.1] 20131222:b
    int variable_constrain_check(const bool val_params_constraint, const common::ObString& var);
    //add 20131222:e
        void set_curr_trans_start_time(int64_t t) {curr_trans_start_time_ = t;};
        int64_t get_curr_trans_start_time() const {return curr_trans_start_time_;};
        void set_curr_trans_last_stmt_time(int64_t t) {curr_trans_last_stmt_time_ = t;};
        int64_t get_curr_trans_last_stmt_time() const {return curr_trans_last_stmt_time_;};
        //add wenghaixing [database manage]20150615
        const common::ObString& get_db_name(){return db_name_;}
        int set_db_name(const common::ObString& db_name);
        //add e
      private:
        int insert_ps_session_info(uint64_t sql_id, int64_t pcount, uint64_t &new_sql_id, bool has_cur_time=false);
      private:
        static const int64_t MAX_STORED_PLANS_COUNT = 10240;
        static const int64_t MAX_IPADDR_LENGTH = 64;
        static const int64_t SMALL_BLOCK_SIZE = 4*1024LL;
        static const int64_t MAX_CUR_QUERY_LEN = 384;
      private:
        pthread_rwlock_t rwlock_; //读写锁保护session在有读的时候不会被析构
        tbsys::CThreadMutex mutex_;//互斥同一个session上的多次query请求
        uint64_t session_id_;
        tbsys::WarningBuffer warnings_buf_;
        uint64_t user_id_;
        common::ObString user_name_;
        //add wenghaixing [database manage]20150610
        common::ObString db_name_;
        //add e
        uint64_t next_stmt_id_;
        ObResultSet *cur_result_set_;
        ObSQLSessionState state_;
        easy_connection_t *conn_;
        char addr_[MAX_IPADDR_LENGTH];
        int64_t cur_query_start_time_;
        char cur_query_[MAX_CUR_QUERY_LEN];
        volatile int64_t cur_query_len_;
        common::ObTransID trans_id_;
        bool is_autocommit_;
        bool is_interactive_;
        int64_t last_active_time_;
        int64_t curr_trans_start_time_;
        int64_t curr_trans_last_stmt_time_;
        const common::ObVersionProvider *version_provider_;
        const ObSQLConfigProvider *config_provider_;

        common::ObSmallBlockAllocator<> block_allocator_;

        common::ObStringBuf name_pool_; // for statement names and variables names
        common::ObStringBuf parser_mem_pool_; // reuse for each parsing
        common::StackAllocator transformer_mem_pool_; // for non-ps transformer

        IdPlanMapAllocer id_plan_map_allocer_;
        IdPlanMap id_plan_map_; // statement-id -> physical-plan
        NamePlanIdMapAllocer stmt_name_id_map_allocer_;
        NamePlanIdMap stmt_name_id_map_; // statement-name -> statement-id
        VarNameValMapAllocer var_name_val_map_allocer_;
        VarNameValMap var_name_val_map_; // user variables
        SysVarNameValMapAllocer sys_var_val_map_allocer_;
        SysVarNameValMap sys_var_val_map_; // system variables
        IdPsInfoMapAllocer id_psinfo_map_allocer_;
        IdPsInfoMap  id_psinfo_map_; // stmt-id -> ObPsSessionInfo

        ObPsStore *ps_store_;   /* point to global ps store */
        // PS related
        common::ObPool<common::ObWrapperAllocator> arena_pointers_;
        common::ObPooledAllocator<ObResultSet, common::ObWrapperAllocator> result_set_pool_;
        common::ObPooledAllocator<ObPsSessionInfo, common::ObWrapperAllocator> ps_session_info_pool_;
        common::ObPooledAllocator<ObObj, common::ObWrapperAllocator> ps_session_info_param_pool_;
    };

    inline const common::ObString ObSQLSessionInfo::get_current_query_string() const
    {
      common::ObString ret;
      ret.assign_ptr(const_cast<char*>(cur_query_), static_cast<int32_t>(cur_query_len_));
      return ret;
    }
  }
}

#endif
