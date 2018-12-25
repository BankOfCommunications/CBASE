/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_schema.h for persistent schema. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMAV1_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMAV1_H_

#include "common/ob_define.h"

namespace oceanbase 
{
  namespace sstable 
  {
    struct ObSSTableSchemaHeaderV1 
    {
      int16_t column_count_;    //column count
      int16_t reserved16_[3];   //must be 0

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObSSTableSchemaColumnDefV1
    {
      uint64_t column_name_id_;     //column name id
      int32_t  column_value_type_;  //type of column value ObObj
      int32_t  reserved_;           //reserved, must be 0

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class ObSSTableSchemaV1
    {
    public:
      ObSSTableSchemaV1();
      ~ObSSTableSchemaV1();

      /**
       * get column count in the sstable schema 
       * 
       * @return int64_t return column count
       */
      int64_t get_column_count() const;

      /**
       * get one column def by column index
       * 
       * @param index the column def index to get
       * 
       * @return ObSSTableSchemaColumnDef* if find, return the pointer 
       *         of the column, else return NULL
       */
      const ObSSTableSchemaColumnDefV1* get_column_def(const int32_t index) const;

      /**
       * find column id if it exists in sstable schema
       * 
       * @param column_id column id to find
       * 
       * @return int if find, return the index of the column id, else 
       *         return -1
       */
      int32_t find_column_id(const uint64_t column_id) const;

      /**
       * add one column define into schema, the column define must be 
       * in ascending order by column id 
       * 
       * @param column_def column define, includes column id and 
       *                   column type
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_column_def(ObSSTableSchemaColumnDefV1& column_def);

      /**
       * reset scheam in order to reuse it
       */
      void reset();

      NEED_SERIALIZE_AND_DESERIALIZE;

    private:
      ObSSTableSchemaColumnDefV1 column_def_[common::OB_MAX_COLUMN_NUMBER];
      ObSSTableSchemaHeaderV1 schema_header_;
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMAV1_H_
