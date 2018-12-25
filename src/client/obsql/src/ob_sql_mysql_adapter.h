/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_fake_sql.h is for what ...
 *
 * Version: ***: ob_fake_sql.h  Thu Nov 15 11:36:52 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */


/**
 * WARNING!!!
 * We heve to reinterpret MYSQL struct in libmysql
 * Inorder to compatible with older versions
 * MYSQL 这个结构成员在不同的libmysql版本之间有变化
 * 为了兼容这种变化 有两种方式
 * 1、重新解释前面比较稳定的成员比如
 *    char *host, *user, *passwd .....
 *
 * 2、自己指定偏移位置 比如offset为8的地方存放MYSQL_POOL的指针
 *    使用这种方式即使libmysql不同版本的host user passwd成员的位置变化了也没关系
 *    只要MYSQL的大小能够容纳下我们要重新解释的成员
 *    把MYSQL结构强转成ObMySQL
 * 使用第二种方式
 *    st_mysql + 0 --------------  MySQLConnUser*
 *    st_mysql + 8 --------------
 * MYSQL point to ob_sql_list_t of MySQLConn
 * real MySQL struct was manager in a list based pool
 * MYSQL unused2 point to real MYSQL used when mysql_init return real MYSQL object
 * add real MYSQL to cluster->server->conn pool when call mysql_real_connect
 */
#ifndef OB_SQL_MYSQL_ADAPTER_H_
#define OB_SQL_MYSQL_ADAPTER_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_list.h"
#include "ob_sql_real_func.h"
#include <pthread.h>
#include <string.h>
#include "ob_sql_select_method_impl.h"
#include "ob_sql_struct.h"
#include "ob_sql_global.h"

/* 一致性标记 */
void MYSQL_FUNC(mysql_set_consistence)(MYSQL_TYPE *mysql);
void MYSQL_FUNC(mysql_unset_consistence)(MYSQL_TYPE *mysql);
my_bool is_consistence(ObSQLMySQL *mysql);

/* 根据错误码判断是否需要重试 */
my_bool need_retry(ObSQLMySQL *mysql);

/* 重试标记 */
void MYSQL_FUNC(mysql_set_retry)(ObSQLMySQL *mysql);
void MYSQL_FUNC(mysql_unset_retry)(ObSQLMySQL *mysql);
my_bool is_retry(ObSQLMySQL *mysql);

/* 事务标志 */
void MYSQL_FUNC(mysql_set_in_transaction)(MYSQL_TYPE *mysql);
void MYSQL_FUNC(mysql_unset_in_transaction)(MYSQL_TYPE *mysql);
my_bool is_in_transaction(ObSQLMySQL *mysql);

OB_SQL_CPP_END
#endif
