/**
 * (C) 2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * libobjoin.h : Header of implementation of join functions
 *
 * Authors:
 *   Yanran <yanran.hfs@taobao.com>
 *
 */

#ifndef _LIBOBJOIN_H__
#define _LIBOBJOIN_H__

#include "oceanbase.h"
#include <vector>
#include <common/ob_buffer.h>

using namespace oceanbase::common;

/**
 * 表示JOIN操作的结构体
 * 允许表示JOIN多个表的操作
 */
struct st_ob_join
{
  /**
   * 描述JOIN一张表的操作
   */
  class st_res_join_desc
  {
    public:
      char* res_columns;
      std::vector<std::string> res_column_list;
      char* join_table;
      char* join_column;
      char* as_name;
    public:
      st_res_join_desc();
      st_res_join_desc(const char* new_res_columns,
                       const char* new_join_table,
                       const char* new_join_column,
                       const char* new_as_name);
      ~st_res_join_desc();
      st_res_join_desc(const st_res_join_desc& copy);
      void set_res_columns(const char* new_res_columns);
      void set_join_table(const char* new_join_table);
      void set_join_column(const char* new_join_column);
      /**
       * 设置as_name成员
       * set_as_name需要在join_table和join_column两个字段
       * 正确设置后再调用。因为，当new_as_name为空时，
       * as_name使用默认值"join_table.join_column"
       */
      void set_as_name(const char* new_as_name);
      void clear();
  };
  typedef std::vector<st_res_join_desc> JoinDescList;
  JoinDescList join_list;
};

typedef struct st_ob_join OB_JOIN;

/**
 * @brief 执行JOIN操作
 *
 * @param res_st 有效的指向原始结果OB_RES结构体的指针
 * @param join_st 有效的指向OB_JOIN结构体的指针
 * @retval OB_RES* 有效的指向OB_RES结构体的指针
 * @retval NULL 失败
 *
 * Example:
 * @include join_demo.c
 */
OB_ERR_CODE ob_exec_res_join(OB* ob,
                             OB_RES* res_st,
                             OB_JOIN* join_st,
                             OB_RES* &final_res);

int64_t datetime_to_int(OB_DATETIME v);

/**
 * 从结果行内取出需要JOIN的外键
 */
OB_ERR_CODE extract_foreign_key(OB_ROW* row,
                                std::vector<std::string> res_column_list,
                                sbuffer<> &rowkey);

OB_ERR_CODE push_to_join_get_st(OB_GET* get_st,
                                const sbuffer<> &rowkey,
                                const char* join_table,
                                const char* join_column);

OB_ERR_CODE prepare_join_get_st(OB_JOIN* join_st,
                                OB_RES* res,
                                OB_GET* get_st);

OB_ERR_CODE append_to_res(const char* table, int64_t table_len,
                          const char* row_key, int64_t row_key_len,
                          const char* column, int64_t column_len,
                          const OB_VALUE* v, OB_RES* res);

OB_ERR_CODE append_cell_to_res(OB_CELL* cell, OB_RES* res);

OB_ERR_CODE append_row_to_res(OB_ROW* row, OB_RES* res);

OB_ERR_CODE append_join_cell_to_res(OB_RES* join_res,
                                    OB_CELL* &cell,
                                    const char* res_table,
                                    int64_t res_table_len,
                                    const char* res_row_key,
                                    int64_t res_row_key_len,
                                    const sbuffer<> &rowkey,
                                    OB_JOIN::JoinDescList::iterator &join_it,
                                    OB_RES* res);

OB_ERR_CODE append_join_res(OB_JOIN* join_st,
                            OB_RES* res,
                            OB_RES* join_res,
                            OB_RES* final_res);

OB_ERR_CODE ob_exec_res_join(OB_RES* res_st,
                             OB_JOIN* join_st,
                             OB_RES* &final_res);

/**
 * @brief 获取OB_JOIN结构体
 *
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_JOIN* 有效的指向OB_RES结构体的指针
 * @retval NULL 失败
 *
 * Example:
 * @include join_demo.c
 */
OB_JOIN* ob_acquire_join_st(OB* ob);

/**
 * @brief 释放OB_JOIN结构体
 *
 * @param ob 有效的指向OB结构体的指针
 * @param join_st 有效的指向OB_JOIN结构体的指针
 * @retval void
 *
 * Example:
 * @include join_demo.c
 */
void ob_release_join_st(OB* ob, OB_JOIN* join_st);

#endif //_LIBOBJOIN_H__
