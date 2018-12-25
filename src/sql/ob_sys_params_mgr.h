/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sys_params_mgr.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_SYS_PARAMS_MGR_H_
#define OCEANBASE_SQL_SYS_PARAMS_MGR_H_

#include "ob_phy_operator.h"
#include "ob_logical_plan.h"
#include "ob_physical_plan.h"
#include "ob_multi_logic_plan.h"
#include "common/ob_list.h"

namespace oceanbase
{
  namespace sql
  {
    const char* STR_SYS_PAREAMS = "sys_params";
    const char* STR_SORT_MEM_SIZE_LIMIT = "sort_mem_size_limit";
    const char* STR_GROUP_MEM_SIZE_LIMIT = "group_mem_size_limit";
    // FIX ME, 
    // 1. MIN value
    static const int64_t MIN_SORT_MEM_SIZE_LIMIT = 10000; // 10M
    static const int64_t MIN_GROUP_MEM_SIZE_LIMIT = 10000; // 10M
    // 2. MAX value, -1 means no limit
    
    class ObSysParamsMgr
    {
      public:
        ObSysParamsMgr();
        virtual ~ObSysParamsMgr();

        void set_sort_mem_size_limit(const int64_t size);
        void set_gorup_mem_size_limit(const int64_t size);

        int parse_from_file(const char* file_name);

        int64_t get_sort_mem_size_limit() const;
        int64_t get_group_mem_size_limit() const;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSysParamsMgr);

      private:
        int64_t sort_mem_size_limit_;
        int64_t group_mem_size_limit_;
    };

    inline int64_t ObSysParamsMgr::get_sort_mem_size_limit() const
    {
      return sort_mem_size_limit_;
    }

    inline int64_t ObSysParamsMgr::get_group_mem_size_limit() const
    {
      return group_mem_size_limit_;
    }

    void ObSysParamsMgr::set_sort_mem_size_limit(const int64_t size)
    {
      if (size >= MIN_SORT_MEM_SIZE_LIMIT)
        sort_mem_size_limit_ = size;
      else if (size < 0)
        sort_mem_size_limit_ = -1;
      else
      {
        ; // lease than MIN value, no change
      }
    }

    void ObSysParamsMgr::set_gorup_mem_size_limit(const int64_t size)
    {
      if (size >= MIN_GROUP_MEM_SIZE_LIMIT)
        group_mem_size_limit_ = size;
      else if (size < 0)
        group_mem_size_limit_ = -1;
      else
      {
        ; // lease than MIN value, no change
      }
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_SYS_PARAMS_MGR_H_ */



