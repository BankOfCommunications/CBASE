/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_macro_define.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SCHEMA_MACRO_DEFINE_H_
#define _OB_SCHEMA_MACRO_DEFINE_H_

#define ADD_COLUMN_SCHEMA_FULL(col_name, col_id, rowkey_id, data_type, data_len, rowkey_len, order, nullable) \
{ \
  ColumnSchema column; \
\
  if(OB_SUCCESS == ret) \
  { \
    int64_t col_name_len = strlen(col_name); \
    if(col_name_len <= OB_MAX_COLUMN_NAME_LENGTH - 1) \
    { \
      memcpy(column.column_name_, col_name, col_name_len); \
      column.column_name_[col_name_len] = '\0'; \
    } \
    else \
    { \
      ret = OB_SIZE_OVERFLOW; \
      TBSYS_LOG(WARN, "col name is too length[%ld]", col_name_len); \
    } \
  } \
\
  if(OB_SUCCESS == ret) \
  { \
    column.column_id_ = col_id; \
    column.column_group_id_ = OB_DEFAULT_COLUMN_GROUP_ID; \
    column.rowkey_id_ = rowkey_id; \
    column.join_table_id_ = OB_INVALID_ID; \
    column.join_column_id_ = OB_INVALID_ID; \
    column.data_type_ = data_type;  \
    column.data_length_ = data_len; \
    column.length_in_rowkey_ = rowkey_len; \
    column.order_in_rowkey_ = order; \
    column.data_precision_ = 0; \
    column.data_scale_ = 0; \
    column.nullable_ = nullable; \
  } \
\
  /*add lijianqiang [sequence create] 20150419:b */ \
  if (OB_SUCCESS == ret && ObDecimalType == data_type && OB_ALL_SEQUENCE_TID == table_schema.table_id_) \
  { \
    column.data_precision_ = 31; \
  } \
  /*add 20150419:e*/ \
\
  if(OB_SUCCESS == ret) \
  { \
    ret = table_schema.add_column(column); \
    if (OB_SUCCESS != ret) TBSYS_LOG(WARN, "add column failed, err=%d", ret); \
  } \
}

#define ADD_COLUMN_SCHEMA(col_name, col_id, rowkey_id, data_type, data_len, nullable) \
  ADD_COLUMN_SCHEMA_FULL(col_name, col_id, rowkey_id, data_type, data_len, data_len, 1, nullable);

#define ADD_SERVER(num) \
  ADD_COLUMN_SCHEMA("server" #num "_ipv4", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_IPV4, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_ipv6_high", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_IPV6_HIGH, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_ipv6_low", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_IPV6_LOW, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_port", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_IP_PORT, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_version", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_VERSION, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_row_count", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_ROW_COUNT, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\
  ADD_COLUMN_SCHEMA("server" #num "_size", /*column_name*/ \
                    first_tablet_entry_cid::REPLICA##num##_SIZE, /*column_id*/ \
                    0, /*rowkey_id*/ \
                    ObIntType,  /*column_type*/ \
                    sizeof(int64_t), /*column length*/ \
                    false); /*is nullable*/ \
\

#endif //_OB_SCHEMA_MACRO_DEFINE_H_
