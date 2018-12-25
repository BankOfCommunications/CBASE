/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_COMMON_OB_SCHEMA_HELPER_
#define OCEANBASE_COMMON_OB_SCHEMA_HELPER_

#include "ob_schema_service.h"
#include "ob_schema.h"
namespace oceanbase
{
  namespace common
  {
    class ObSchemaHelper
    {
      public:
        ObSchemaHelper();
        ~ObSchemaHelper();

        static int get_left_offset_array(
            const ObArray<TableSchema>& table_schema_array,
            const int64_t left_table_index,
            const uint64_t right_table_id,
            uint64_t *left_column_offset_array,
            int64_t &size);
        static int get_table_by_id(
            const ObArray<TableSchema>& table_schema_array,
            const uint64_t table_id,
            int64_t &index);
        static int transfer_manager_to_table_schema(
            const ObSchemaManagerV2 &schema_manager,
            ObArray<TableSchema> &table_schema_array);
        static int get_join_info_by_offset_array(
            const ObColumnSchemaV2::ObJoinInfo join_info,
            const ObRowkeyInfo &rowkey_info,
            const char* left_table_name,
            const int64_t left_table_name_length,
            const ObArray<ColumnSchema>& left_column_array,
            const uint64_t left_table_id,
            const ObSchemaManagerV2 *schema_manager,
            ObArray<JoinInfo>& join_array);
    };
  }
}

#endif

