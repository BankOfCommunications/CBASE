/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_select_method.h is for what ...
 *
 * Version: ***: ob_sql_select_method.h  Sat Nov 24 15:34:28 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SELECT_METHOD_H_
#define OB_SQL_SELECT_METHOD_H_

#include "ob_sql_define.h"


OB_SQL_CPP_START
#include "ob_sql_struct.h"

/* 连接选择策略 */
/**
 * 从mergeserver pool中选出一台mergeserver
 * 即一个mysql连接池
 * @param[in] pool         mergeserver pool
 * @param[in] sql          sql语句
 * @param[in] length       sql语句的长度
 * @return mysql_pool      选出的mergerserver上的连接池
 *
 */
typedef ObDataSource* (*Select_ms_method)(ObClusterInfo *pool, const char* sql,
                                           unsigned long length);

/**
 * 从连接池中选出一条连接
 *
 * @param[in] pool         mysql conn pool
 *
 * @return  conn           选出的mysql的连接
 *
 */
typedef ObSQLConn* (*Select_conn_method)(ObDataSource *pool);

typedef struct select_method_set
{
  Select_ms_method select_ms;
  Select_conn_method select_conn;
} ObSQLSelectMethodSet;

OB_SQL_CPP_END
#endif
