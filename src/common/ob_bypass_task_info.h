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
#ifndef OCEANBASE_COMMON_OB_BYPASS_STRUCT_H
#define OCEANBASE_COMMON_OB_BYPASS_STRUCT_H
#include "ob_define.h"
#include "utility.h"
#include "tbsys.h"
#include "ob_array.h"
#include "tblog.h"
#include "ob_string_buf.h"
namespace oceanbase
{
  namespace common
  {
    enum OperationType
    {
      INVALID_TYPE = -1,
      IMPORT_ALL = 0,
      IMPORT_TABLE = 1,
      CLEAN_ROOT_TABLE = 2,
    };
    class ObBypassTaskInfo
    {
      public:
        ObBypassTaskInfo();
        ~ObBypassTaskInfo();
        std::pair<common::ObString, uint64_t>& at(int64_t index);
        int64_t count()const;
        void clear();
        int push_back(std::pair<common::ObString, uint64_t> info);
        uint64_t get_table_id(const char* table_name) const;
        void set_operation_type(OperationType operation_type);
        const OperationType get_operation_type() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObBypassTaskInfo);
      private:
        OperationType operation_type_;
        ObArray<std::pair<common::ObString, uint64_t> > name_id_array_;
        ObStringBuf buff_;
    };
  }
}

#endif

