/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   Author XiuMing
 *   Email: wanhong.wwh@alibaba-inc.com
 *     -some work detail if you want
 *
 */

#ifndef OB_SQL_LIBOBSQL_H_
#define OB_SQL_LIBOBSQL_H_

#include <stdint.h>
#include <mysql/mysql.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OB_SQL_SUCCESS 0
#define OB_SQL_ERROR -1
#define OB_SQL_INVALID_ARGUMENT -2

#define OB_SQL_MAX_USER_NAME_LEN 128
#define OB_SQL_MAX_PASSWD_LEN 128
#define OB_SQL_MAX_CLUSTER_ADDRESS_LEN 1024

typedef struct ob_sql_config
{
  /// 由多个"IP:PORT"组成的集群地址，采用逗号","分隔
  char cluster_address_[OB_SQL_MAX_CLUSTER_ADDRESS_LEN];

  char username_[OB_SQL_MAX_USER_NAME_LEN];
  char passwd_[OB_SQL_MAX_PASSWD_LEN];
  int32_t min_conn_;
  int32_t max_conn_;
  bool read_slave_only_;
} ObSQLConfig;

// Macros
#ifdef __cplusplus
# define OB_SQL_CPP_START extern "C" {
# define OB_SQL_CPP_END }
#else
# define OB_SQL_CPP_START
# define OB_SQL_CPP_END
#endif

#ifdef PRELOAD
#define MYSQL_FUNC(func)  func
#define MYSQL_TYPE        MYSQL
#else
#define MYSQL_FUNC(func)  ob_##func
#define MYSQL_TYPE        ObSQLMySQL
#endif

int ob_sql_init(const ObSQLConfig &config);
void ob_sql_destroy();

int ob_sql_update_cluster_address(const char *cluster_address);

typedef struct ob_sql_mysql ObSQLMySQL;

#ifndef PRELOAD
#define ob_mysql_library_init ob_mysql_server_init
#define ob_mysql_library_end ob_mysql_server_end
#endif

//================MySQL Client API==================================
int STDCALL MYSQL_FUNC(mysql_server_init)(int, char **, char **);
void STDCALL MYSQL_FUNC(mysql_server_end)(void);
MYSQL_PARAMETERS *STDCALL MYSQL_FUNC(mysql_get_parameters)(void);
my_bool STDCALL MYSQL_FUNC(mysql_thread_init)(void);
void STDCALL MYSQL_FUNC(mysql_thread_end)(void);
my_ulonglong STDCALL MYSQL_FUNC(mysql_num_rows)(MYSQL_RES *);
unsigned int STDCALL MYSQL_FUNC(mysql_num_fields)(MYSQL_RES *);
my_bool STDCALL MYSQL_FUNC(mysql_eof)(MYSQL_RES *res);
MYSQL_FIELD *STDCALL MYSQL_FUNC(mysql_fetch_field_direct)(MYSQL_RES *res, unsigned int fieldnr);
MYSQL_FIELD * STDCALL MYSQL_FUNC(mysql_fetch_fields)(MYSQL_RES *res);
MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_row_tell)(MYSQL_RES *res);
MYSQL_FIELD_OFFSET STDCALL MYSQL_FUNC(mysql_field_tell)(MYSQL_RES *res);
unsigned int STDCALL MYSQL_FUNC(mysql_field_count)(MYSQL_TYPE *mysql);
my_ulonglong STDCALL MYSQL_FUNC(mysql_affected_rows)(MYSQL_TYPE *mysql);
my_ulonglong STDCALL MYSQL_FUNC(mysql_insert_id)(MYSQL_TYPE *mysql);
unsigned int STDCALL MYSQL_FUNC(mysql_errno)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_error)(MYSQL_TYPE *mysql);
const char *STDCALL MYSQL_FUNC(mysql_sqlstate)(MYSQL_TYPE *mysql);
unsigned int STDCALL MYSQL_FUNC(mysql_warning_count)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_info)(MYSQL_TYPE *mysql);
unsigned long STDCALL MYSQL_FUNC(mysql_thread_id)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_character_set_name)(MYSQL_TYPE *mysql);
int STDCALL MYSQL_FUNC(mysql_set_character_set)(MYSQL_TYPE *mysql, const char *csname);
MYSQL_TYPE * STDCALL MYSQL_FUNC(mysql_init)(MYSQL_TYPE *mysql);
my_bool STDCALL MYSQL_FUNC(mysql_ssl_set)(MYSQL_TYPE *mysql, const char *key, const char *cert, const char *ca, const char *capath, const char *cipher);
const char * STDCALL MYSQL_FUNC(mysql_get_ssl_cipher)(MYSQL_TYPE *mysql);
my_bool STDCALL MYSQL_FUNC(mysql_change_user)(MYSQL_TYPE *mysql, const char *user, const char *passwd, const char *db);
MYSQL_TYPE * STDCALL MYSQL_FUNC(mysql_real_connect)(MYSQL_TYPE *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long clientflag);
int STDCALL MYSQL_FUNC(mysql_select_db)(MYSQL_TYPE *mysql, const char *db);
int STDCALL MYSQL_FUNC(mysql_query)(MYSQL_TYPE *mysql, const char *q);
int STDCALL MYSQL_FUNC(mysql_send_query)(MYSQL_TYPE *mysql, const char *q, unsigned long length);
int STDCALL MYSQL_FUNC(mysql_real_query)(MYSQL_TYPE *mysql, const char *q, unsigned long length);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_store_result)(MYSQL_TYPE *mysql);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_use_result)(MYSQL_TYPE *mysql);
void STDCALL MYSQL_FUNC(mysql_get_character_set_info)(MYSQL_TYPE *mysql, MY_CHARSET_INFO *charset);
int STDCALL MYSQL_FUNC(mysql_shutdown)(MYSQL_TYPE *mysql, enum mysql_enum_shutdown_level shutdown_level);
int STDCALL MYSQL_FUNC(mysql_dump_debug_info)(MYSQL_TYPE *mysql);
int STDCALL MYSQL_FUNC(mysql_refresh)(MYSQL_TYPE *mysql, unsigned int refresh_options);
int STDCALL MYSQL_FUNC(mysql_kill)(MYSQL_TYPE *mysql,unsigned long pid);
int STDCALL MYSQL_FUNC(mysql_set_server_option)(MYSQL_TYPE *mysql, enum enum_mysql_set_option option);
int STDCALL MYSQL_FUNC(mysql_ping)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_stat)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_get_server_info)(MYSQL_TYPE *mysql);
const char * STDCALL MYSQL_FUNC(mysql_get_client_info)(void);
unsigned long STDCALL MYSQL_FUNC(mysql_get_client_version)(void);
const char * STDCALL MYSQL_FUNC(mysql_get_host_info)(MYSQL_TYPE *mysql);
unsigned long STDCALL MYSQL_FUNC(mysql_get_server_version)(MYSQL_TYPE *mysql);
unsigned int STDCALL MYSQL_FUNC(mysql_get_proto_info)(MYSQL_TYPE *mysql);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_dbs)(MYSQL_TYPE *mysql,const char *wild);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_tables)(MYSQL_TYPE *mysql,const char *wild);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_processes)(MYSQL_TYPE *mysql);
//int STDCALL MYSQL_FUNC(mysql_options)(MYSQL_TYPE *mysql,enum mysql_option option, const void *arg);
void STDCALL MYSQL_FUNC(mysql_free_result)(MYSQL_RES *result);
void STDCALL MYSQL_FUNC(mysql_data_seek)(MYSQL_RES *result, my_ulonglong offset);
MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_row_seek)(MYSQL_RES *result, MYSQL_ROW_OFFSET offset);
MYSQL_FIELD_OFFSET STDCALL MYSQL_FUNC(mysql_field_seek)(MYSQL_RES *result, MYSQL_FIELD_OFFSET offset);
MYSQL_ROW STDCALL MYSQL_FUNC(mysql_fetch_row)(MYSQL_RES *result);
unsigned long * STDCALL MYSQL_FUNC(mysql_fetch_lengths)(MYSQL_RES *result);
MYSQL_FIELD * STDCALL MYSQL_FUNC(mysql_fetch_field)(MYSQL_RES *result);
MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_fields)(MYSQL_TYPE *mysql, const char *table, const char *wild);
unsigned long STDCALL MYSQL_FUNC(mysql_escape_string)(char *to,const char *from, unsigned long from_length);
unsigned long STDCALL MYSQL_FUNC(mysql_hex_string)(char *to,const char *from, unsigned long from_length);
unsigned long STDCALL MYSQL_FUNC(mysql_real_escape_string)(MYSQL_TYPE *mysql, char *to,const char *from, unsigned long length);
void STDCALL MYSQL_FUNC(mysql_debug)(const char *debug);
void STDCALL MYSQL_FUNC(myodbc_remove_escape)(MYSQL_TYPE *mysql,char *name);
unsigned int STDCALL MYSQL_FUNC(mysql_thread_safe)(void);
my_bool STDCALL MYSQL_FUNC(mysql_embedded)(void);
my_bool STDCALL MYSQL_FUNC(mysql_read_query_result)(MYSQL_TYPE *mysql);
MYSQL_STMT * STDCALL MYSQL_FUNC(mysql_stmt_init)(MYSQL_TYPE *mysql);
int STDCALL MYSQL_FUNC(mysql_stmt_prepare)(MYSQL_STMT *stmt, const char *query, unsigned long length);
int STDCALL MYSQL_FUNC(mysql_stmt_execute)(MYSQL_STMT *stmt);
int STDCALL MYSQL_FUNC(mysql_stmt_fetch)(MYSQL_STMT *stmt);
int STDCALL MYSQL_FUNC(mysql_stmt_fetch_column)(MYSQL_STMT *stmt, MYSQL_BIND *bind_arg, unsigned int column, unsigned long offset);
int STDCALL MYSQL_FUNC(mysql_stmt_store_result)(MYSQL_STMT *stmt);
unsigned long STDCALL MYSQL_FUNC(mysql_stmt_param_count)(MYSQL_STMT * stmt);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_attr_set)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *attr);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_attr_get)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *attr);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_bind_param)(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_bind_result)(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_close)(MYSQL_STMT * stmt);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_reset)(MYSQL_STMT * stmt);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_free_result)(MYSQL_STMT *stmt);
my_bool STDCALL MYSQL_FUNC(mysql_stmt_send_long_data)(MYSQL_STMT *stmt, unsigned int param_number, const char *data, unsigned long length);
MYSQL_RES *STDCALL MYSQL_FUNC(mysql_stmt_result_metadata)(MYSQL_STMT *stmt);
MYSQL_RES *STDCALL MYSQL_FUNC(mysql_stmt_param_metadata)(MYSQL_STMT *stmt);
unsigned int STDCALL MYSQL_FUNC(mysql_stmt_errno)(MYSQL_STMT * stmt);
const char *STDCALL MYSQL_FUNC(mysql_stmt_error)(MYSQL_STMT * stmt);
const char *STDCALL MYSQL_FUNC(mysql_stmt_sqlstate)(MYSQL_STMT * stmt);
MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_stmt_row_seek)(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET offset);
MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_stmt_row_tell)(MYSQL_STMT *stmt);
void STDCALL MYSQL_FUNC(mysql_stmt_data_seek)(MYSQL_STMT *stmt, my_ulonglong offset);
my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_num_rows)(MYSQL_STMT *stmt);
my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_affected_rows)(MYSQL_STMT *stmt);
my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_insert_id)(MYSQL_STMT *stmt);
unsigned int STDCALL MYSQL_FUNC(mysql_stmt_field_count)(MYSQL_STMT *stmt);
my_bool STDCALL MYSQL_FUNC(mysql_commit)(MYSQL_TYPE * mysql);
my_bool STDCALL MYSQL_FUNC(mysql_rollback)(MYSQL_TYPE * mysql);
my_bool STDCALL MYSQL_FUNC(mysql_autocommit)(MYSQL_TYPE * mysql, my_bool auto_mode);
my_bool STDCALL MYSQL_FUNC(mysql_more_results)(MYSQL_TYPE *mysql);
int STDCALL MYSQL_FUNC(mysql_next_result)(MYSQL_TYPE *mysql);
void STDCALL MYSQL_FUNC(mysql_close)(MYSQL_TYPE *sock);
//void my_init(void);

#ifndef MYSQLCLIENT55
const char * MYSQL_FUNC(mysql_get_ssl_cipher)(MYSQL_TYPE *mysql);
void MYSQL_FUNC(mysql_set_local_infile_default)(MYSQL_TYPE *mysql);
void MYSQL_FUNC(mysql_set_local_infile_handler)(MYSQL_TYPE *mysql, int (*local_infile_init)(void **, const char *, void *), int (*local_infile_read)(void *, char *, unsigned int), void (*local_infile_end)(void *), int (*local_infile_error)(void *, char*, unsigned int), void *userdata);
int STDCALL MYSQL_FUNC(mysql_stmt_next_result)(MYSQL_STMT *stmt);
struct st_mysql_client_plugin * MYSQL_FUNC(mysql_client_find_plugin)(MYSQL_TYPE *mysql, const char *name, int type);
struct st_mysql_client_plugin * MYSQL_FUNC(mysql_client_register_plugin)(MYSQL_TYPE *mysql, struct st_mysql_client_plugin *plugin);
struct st_mysql_client_plugin * MYSQL_FUNC(mysql_load_plugin)(MYSQL_TYPE *mysql, const char *name, int type, int argc, ...);
struct st_mysql_client_plugin * MYSQL_FUNC(mysql_load_plugin_v)(MYSQL_TYPE *mysql, const char *name, int type, int argc, va_list args);
int MYSQL_FUNC(mysql_plugin_options)(struct st_mysql_client_plugin *plugin, const char *option, const void *value);
#endif

#ifdef __cplusplus
}
#endif
#endif
