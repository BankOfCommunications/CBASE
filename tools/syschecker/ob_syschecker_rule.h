/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_rule.h for define syschecker test rule. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_RULE_H_
#define OCEANBASE_SYSCHECKER_RULE_H_

#include "ob_syschecker_schema.h"
#include "common/ob_object.h"
#include "ob_syschecker_param.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    static const int64_t MAX_SYSCHECKER_ROWKEY_LEN = sizeof(int64_t) * 2;
    static const int64_t MAX_SYSCHECKER_ROWKEY_COLUMN_COUNT =   2;
    static const int64_t MAX_SYSCHECKER_RULE_NAME_LEN = 32;
    static const int64_t MAX_SYSCHECKER_OP_ROW = 1024;
    static const int64_t ROWKEY_PREFIX_SIZE = sizeof(int64_t);
    static const int64_t ROWKEY_SUFFIX_SIZE = sizeof(int64_t);
    static const int64_t READ_OP_COUNT = 2;
    static const int64_t WRITE_OP_COUNT = 5;
    static const int64_t WRITE_OP_FULL_ROW_COUNT = 4;
    static const int64_t CELL_WRITE_OP_COUNT = 3;
    static const int64_t MIN_OP_CELL_COUNT = 2;
    static const int64_t MIN_TEST_VARCHAR_LEN = 8;
    static const int64_t PARAM_DATA_BUF_SIZE = 2 * 1024 * 1024;  //2M

    static const int64_t MAX_GET_ROW_COUNT = 100; //1000;
    static const int64_t SCAN_PARAM_ROW_COUNT = 2;
    static const int64_t MAX_SCAN_ROW_COUNT = 100; //1000;
    static const int64_t MAX_INVALID_ROW_COUNT = 10;
   // static const int64_t MAX_WRITE_ROW_COUNT = 100; //100;
    static const int64_t MAX_WRITE_ROW_COUNT = 3; //5;

    extern const char* READ_PARAM_FILE;
    extern const char* WRITE_PARAM_FILE;
    extern const char* OP_TYPE_STR[];
    extern const char* OP_GEN_MODE_STR[];
    extern const char* OBJ_TYPE_STR[];
    
    enum ObOpType 
    {
      OP_NULL = 0,
      OP_ADD, 
      OP_UPDATE,
      OP_INSERT,
      OP_DELETE,
      OP_MIX,   //mixed operation with add, update, insert and delete
      OP_GET,
      OP_SCAN,
      OP_SQL_GET,
      OP_SQL_SCAN,
      OP_MAX,
    };

    enum ObGenMode
    {
      GEN_NULL = 0,
      GEN_RANDOM,
      GEN_SEQ,
      GEN_COMBO_RANDOM,
      GEN_SPECIFIED,
      GEN_NEW_KEY,
      GEN_VALID_WRITE,
      GEN_INVALID_WRITE,
      GEN_VALID_ADD,
      GEN_FROM_CONFIG,
    };

    enum ObTableType
    {
      ALL_TABLE = 0,
      WIDE_TABLE,
      JOIN_TABLE,
    };

    //how to generate the op param
    struct ObOpParamGen 
    {
      ObOpParamGen();
      ~ObOpParamGen();
      void reset();
      void display() const;
      NEED_SERIALIZE_AND_DESERIALIZE;

      ObGenMode gen_op_;
      ObGenMode gen_table_name_;
      ObGenMode gen_row_key_;
      ObGenMode gen_row_count_;
      ObGenMode gen_cell_count_;
      ObGenMode gen_cell_;
    };

    struct ObOpCellParam 
    {
      void reset();
      void display() const;
      NEED_SERIALIZE_AND_DESERIALIZE;

      ObOpType op_type_;
      ObGenMode gen_cell_;
      common::ObObjType cell_type_;
      int32_t varchar_len_;
      int64_t key_prefix_;
      common::ObString column_name_;
      uint64_t column_id_;
      union
      {
        int64_t int_val_;
        int64_t ext_val_;
        float float_val_;
        double double_val_;
        common::ObDateTime time_val_;
        common::ObPreciseDateTime precisetime_val_;
        common::ObModifyTime modifytime_val_;
        common::ObCreateTime createtime_val_;
        const char* varchar_val_;
      } value_;
    };

    struct ObOpRowParam
    {
      void reset();
      void display(const bool detail = true) const;
      NEED_SERIALIZE_AND_DESERIALIZE;

      ObOpType op_type_;
      int32_t cell_count_;
      int32_t rowkey_len_;
      common::ObObj rowkey_[MAX_SYSCHECKER_ROWKEY_COLUMN_COUNT];
      ObOpCellParam cell_[common::OB_MAX_COLUMN_NUMBER];
    };

    struct ObOpParam
    {
      ObOpParam();
      ~ObOpParam();
      void reset();
      void display() const;
      int ensure_space(const int64_t size);
      int write_param_to_file(const char* file_name);
      int load_param_from_file(const char* file_name);
      NEED_SERIALIZE_AND_DESERIALIZE;

      bool invalid_op_;
      bool is_read_;
      bool is_wide_table_;
      ObOpType op_type_;
      int64_t row_count_;
      common::ObString table_name_;
      uint64_t table_id_;
      ObOpParamGen param_gen_;
      ObOpRowParam row_[MAX_SYSCHECKER_OP_ROW];

      bool loaded_;   //only for load param from file, after load param from file, it's true
      char* data_buf_;
      int32_t data_buf_size_;
      int32_t data_len_;
    };

    struct ObOpRule
    {
      bool invalid_op_;
      char rule_name_[MAX_SYSCHECKER_RULE_NAME_LEN];
      ObOpParamGen param_gen_;
    };

    class ObSyscheckerRule
    {
    public:
      ObSyscheckerRule(ObSyscheckerSchema& syschecker_schema);
      ~ObSyscheckerRule();

      int init(const ObSyscheckerParam& param);

      int get_next_write_param(ObOpParam& write_param);
      int get_next_read_param(ObOpParam& read_param);

      const int64_t get_cur_max_prefix() const;
      int64_t add_cur_max_prefix(const int64_t prefix);

      const int64_t get_cur_max_suffix() const;
      int64_t add_cur_max_suffix(const int64_t suffix);

      const ObSyscheckerSchema& get_schema() const { return syschecker_schema_; }

    private:
      int init_random_block();
      ObOpType get_random_read_op(const uint64_t random_num);
      ObOpType get_config_read_op(const uint64_t random_num);
      ObOpType get_random_write_op(const uint64_t random_num);
      ObOpType get_cell_random_write_op(const uint64_t random_num);

      int set_op_type(ObOpParam& op_param, const uint64_t random_num);
      int set_table_name(ObOpParam& op_param, const uint64_t random_num);

      int set_row_count(ObOpParam& op_param, const uint64_t random_num);
      void set_row_op_type(const ObOpParam& op_param, ObOpRowParam& row_param,
                           const uint64_t random_num);
      int set_row_key(const ObOpParam& op_param, ObOpRowParam& row_param, 
                      const uint64_t random_num, int64_t& prefix, int64_t& suffix);
      int encode_row_key(const ObOpParam& op_param, ObOpRowParam& row_param,
                         const int64_t prefix, const int64_t suffix);
      int set_cell_count_per_row(const ObOpParam& op_param, ObOpRowParam& row_param, 
                                 const uint64_t random_num);
      int set_rows_info(ObOpParam& op_param, const uint64_t random_num);

      void set_cell_op_type(const ObOpRowParam& row_param, ObOpCellParam& cell_param,
                            const uint64_t random_num);
      int set_cells_info(const ObOpParam& op_param, ObOpRowParam& row_param, 
                         const int64_t prefix, const uint64_t random_num);
      int set_cell_pair(const ObOpParam& op_param, const ObOpRowParam& row_param, 
                        ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                        const uint64_t random_num, const int64_t column_idx);

      int set_column_name(const ObColumnPair& column_pair, ObOpCellParam& org_cell,
                          ObOpCellParam& aux_cell);
      int set_cell_type(const ObColumnPair& column_pair, ObOpCellParam& org_cell,
                        ObOpCellParam& aux_cell);
      int set_addable_cell_value(ObOpCellParam& org_cell, ObOpCellParam& aux_cell);
      int set_new_cell_value(const ObColumnPair& column_pair, ObOpCellParam& org_cell, 
                             ObOpCellParam& aux_cell, const uint64_t random_num);
      int set_cell_varchar(const ObColumnPair& column_pair, ObOpCellParam& org_cell, 
                           ObOpCellParam& aux_cell, const uint64_t random_num);
      int set_cell_value(const ObColumnPair& column_pair, ObOpCellParam& org_cell, 
                         ObOpCellParam& aux_cell, const uint64_t random_num);

      int set_random_wt_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const bool is_read, 
                             const int64_t column_idx);
      int set_random_jt_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const bool is_read,
                             const int64_t column_idx);
      int set_valid_wwt_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const int64_t column_idx);
      int set_valid_jwt_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const int64_t column_idx);
      int set_invalid_wut_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                               const uint64_t random_num, const int64_t column_idx);
      int set_invalid_jut_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                               const uint64_t random_num, const int64_t column_idx);
      int set_valid_wat_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const int64_t column_idx);
      int set_valid_jat_cell(ObOpCellParam& org_cell, ObOpCellParam& aux_cell, 
                             const uint64_t random_num, const int64_t column_idx);

      int check_cell_value(ObOpCellParam& org_cell, ObOpCellParam& aux_cell);
      int check_addable_cell_value(ObOpCellParam& org_cell, ObOpCellParam& aux_cell);
      int check_new_cell_value(ObOpCellParam& org_cell, ObOpCellParam& aux_cell);

      int set_specify_param(ObOpParam& op_param);

    private:
      static const int64_t MAX_WRITE_RULE_NUM = 128;
      static const int64_t MAX_READ_RULE_NUM = 128;
      static const int64_t RANDOM_BLOCK_SIZE = 4 * 1024 * 1024;  //4M
      static const char* ALPHA_NUMERICS;
      static const int64_t ALPHA_NUMERICS_SIZE = 64;
      static const int64_t PRIME_NUM[];
      static const int64_t PRIME_NUM_COUNT = 168;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSyscheckerRule);

      bool inited_;
      volatile uint64_t cur_max_prefix_; //current maximum prefix of wide table row key
      volatile uint64_t cur_max_suffix_; //current maximun suffix of wide table row key
      ObSyscheckerSchema& syschecker_schema_;
      ObOpRule write_rule_[MAX_WRITE_RULE_NUM];
      int64_t write_rule_count_;
      ObOpRule read_rule_[MAX_READ_RULE_NUM];
      int64_t read_rule_count_;
      char* random_block_;
      int64_t random_block_size_;
      int64_t syschecker_count_;
      int64_t syschecker_no_;
      bool is_specified_read_param_;
      bool operate_full_row_;
      bool perf_test_;
      bool is_sql_read_;
      int read_table_type_;
      int write_table_type_;
      int64_t get_row_cnt_;
      int64_t scan_row_cnt_;
      int64_t update_row_cnt_;
    };

    //utiliy function
    void hex_dump_rowkey(const void* data, const int32_t size, 
                         const bool char_type);
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_RULE_H_
