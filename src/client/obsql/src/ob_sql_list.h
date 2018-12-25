/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_list.h is for what ...
 *
 * Version: ***: ob_sql_list.h  Sat Nov 24 19:06:29 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_LIST_H_
#define OB_SQL_LIST_H_



/**
 * 列表
 */
#include "ob_sql_define.h"
#include <string.h>
#include <stdint.h>
OB_SQL_CPP_START
enum ObSQLListType
{
  OBSQLCONN = 0,
  OBSQLCONNLIST,
};

typedef struct ob_sql_list_node
{
  void *data_;
  ob_sql_list_node *prev_;
  ob_sql_list_node *next_;
} ObSQLListNode;

typedef struct ob_sql_list
{
  ObSQLListType type_;
  int32_t size_;
  ObSQLListNode *head_;
  ObSQLListNode *tail_;
} ObSQLList;

typedef struct ob_sql_conn_list
{
  ObSQLList free_list_;
  ObSQLList used_list_;
} ObSQLConnList;

int ob_sql_list_init(ObSQLList *list, ObSQLListType type);

int ob_sql_list_add_before(ObSQLList *list, ObSQLListNode *pos, ObSQLListNode *node);
int ob_sql_list_add_after(ObSQLList *list, ObSQLListNode *pos, ObSQLListNode *node);
int ob_sql_list_del(ObSQLList *list, ObSQLListNode *node);
int ob_sql_list_add_head(ObSQLList *list, ObSQLListNode *node);
int ob_sql_list_add_tail(ObSQLList *list, ObSQLListNode *node);
int ob_sql_list_empty(ObSQLList &list);




OB_SQL_CPP_END

#endif
