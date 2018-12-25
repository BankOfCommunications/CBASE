/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_schema.h for define syschecker schema 
 * manager. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_SCHEMA_H_
#define OCEANBASE_SYSCHECKER_SCHEMA_H_

#include "common/ob_schema.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    struct ObColumnPair
    {
      const common::ObColumnSchemaV2* org_; //original column 
      const common::ObColumnSchemaV2* aux_; //auxiliary column
    };

    class ObSyscheckerSchema
    {
    public:
      ObSyscheckerSchema();
      ~ObSyscheckerSchema();

      int init();

      const int64_t get_wt_column_count() const;
      const int64_t get_wt_writable_column_count() const;
      const int64_t get_wt_unwritable_column_count() const;
      const int64_t get_wt_addable_column_count() const;
      
      const int64_t get_jt_column_count() const;
      const int64_t get_jt_writable_column_count() const;
      const int64_t get_jt_unwritable_column_count() const;
      const int64_t get_jt_addable_column_count() const;

      const common::ObSchemaManagerV2& get_schema_manager() const;
      common::ObSchemaManagerV2& get_schema_manager();
      const common::ObRowkeyInfo& get_rowkey_info(const uint64_t table_id) const;
      bool is_rowkey_column(const uint64_t table_id, const uint64_t column_id) const;
      bool is_prefix_column(const uint64_t table_id, const uint64_t column_id) const;

      const common::ObTableSchema* get_wt_schema() const;
      const common::ObTableSchema* get_jt_schema() const;
      const common::ObString get_wt_name() const;
      const common::ObString get_jt_name() const;

      const ObColumnPair* get_wt_column(const int64_t index) const;
      const ObColumnPair* get_wt_writable_column(const int64_t index) const;
      const ObColumnPair* get_wt_unwritable_column(const int64_t index) const;
      const ObColumnPair* get_wt_addable_column(const int64_t index) const;

      const ObColumnPair* get_jt_column(const int64_t index) const;
      const ObColumnPair* get_jt_writable_column(const int64_t index) const;
      const ObColumnPair* get_jt_unwritable_column(const int64_t index) const;
      const ObColumnPair* get_jt_addable_column(const int64_t index) const;

      const common::ObString get_column_name(const common::ObColumnSchemaV2& column) const;

    private:
      const common::ObColumnSchemaV2* find_aux_column_schema(
        const common::ObTableSchema* shema, const char* org_column_name);
      int init_table_schema();
      int init_wt_column();
      int init_jt_column();

    public:
      static const int64_t TABLE_COUNT = 2;

    private:
      static const char* AUX_NAME_PREFIX;
      static const int64_t AUX_NAME_PREFIX_SIZE = 4;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSyscheckerSchema);

      bool inited_;
      common::ObSchemaManagerV2 schema_manager_;

      //wide table schema
      const common::ObTableSchema* wt_schema_;
      //wide table column count, exclude auxiliary column
      int64_t wt_column_count_; 
      //wide table column pair 
      ObColumnPair wt_column_[common::OB_MAX_COLUMN_NUMBER];
      //writable column size of wide table, exclude auxiliary column
      int64_t wt_writable_column_count_; 
      //writable column pair in wide table
      ObColumnPair wt_writable_column_[common::OB_MAX_COLUMN_NUMBER];
      //unwritable column size of wide table, exclude auxiliary column
      int64_t wt_unwritable_column_count_;
      //unwritable column pair in wide table
      ObColumnPair wt_unwritable_column_[common::OB_MAX_COLUMN_NUMBER];
      //wide table addable column count, not include auxiliary column
      int64_t wt_addable_column_count_;  
      //wide table addable column pair in wide table
      ObColumnPair wt_addable_column_[common::OB_MAX_COLUMN_NUMBER];

      //join table schema
      const common::ObTableSchema* jt_schema_;
      //join table column count, exclude auxiliary column
      int64_t jt_column_count_; 
      //join table column pair 
      ObColumnPair jt_column_[common::OB_MAX_COLUMN_NUMBER];
      //writable column size of join table, exclude auxiliary column
      int64_t jt_writable_column_count_; 
      //writable column pair in join table
      ObColumnPair jt_writable_column_[common::OB_MAX_COLUMN_NUMBER];
      //unwritable column size of join table, exclude auxiliary column
      int64_t jt_unwritable_column_count_;
      //unwritable column pair in join table
      ObColumnPair jt_unwritable_column_[common::OB_MAX_COLUMN_NUMBER];
      //join table addable column count, exclude auxiliary column
      int64_t jt_addable_column_count_;   
      //join table addable column index in join table
      ObColumnPair jt_addable_column_[common::OB_MAX_COLUMN_NUMBER];
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_SCHEMA_H_
