/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_real_fuc.h is for what ...
 *
 * Version: ***: ob_sql_real_func.h  Tue Nov 20 16:56:39 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */

#ifndef OB_SQL_REAL_FUNC_H_
#define OB_SQL_REAL_FUNC_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START
#include <mysql/mysql.h>
#include <stdarg.h>

typedef struct st_func_set
{
  int STDCALL (* real_mysql_server_init)(int, char **, char **);
  void STDCALL(* real_mysql_server_end)(void);
  MYSQL_PARAMETERS *STDCALL (* real_mysql_get_parameters)(void);
  my_bool STDCALL (* real_mysql_thread_init)(void);
  void STDCALL (* real_mysql_thread_end)(void);
  my_ulonglong STDCALL (* real_mysql_num_rows)(MYSQL_RES *);
  unsigned int STDCALL (* real_mysql_num_fields)(MYSQL_RES *);
  my_bool STDCALL (* real_mysql_eof)(MYSQL_RES *res);
  MYSQL_FIELD *STDCALL (* real_mysql_fetch_field_direct)(MYSQL_RES *res, unsigned int fieldnr);
  MYSQL_FIELD * STDCALL (* real_mysql_fetch_fields)(MYSQL_RES *res);
  MYSQL_ROW_OFFSET STDCALL (* real_mysql_row_tell)(MYSQL_RES *res);
  MYSQL_FIELD_OFFSET STDCALL (* real_mysql_field_tell)(MYSQL_RES *res);
  unsigned int STDCALL (* real_mysql_field_count)(MYSQL *mysql);
  my_ulonglong STDCALL (* real_mysql_affected_rows)(MYSQL *mysql);
  my_ulonglong STDCALL (* real_mysql_insert_id)(MYSQL *mysql);
  unsigned int STDCALL (* real_mysql_errno)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_error)(MYSQL *mysql);
  const char *STDCALL (* real_mysql_sqlstate)(MYSQL *mysql);
  unsigned int STDCALL (* real_mysql_warning_count)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_info)(MYSQL *mysql);
  unsigned long STDCALL (* real_mysql_thread_id)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_character_set_name)(MYSQL *mysql);
  int STDCALL (* real_mysql_set_character_set)(MYSQL *mysql, const char *csname);
  MYSQL * STDCALL (* real_mysql_init)(MYSQL *mysql);
  my_bool STDCALL (* real_mysql_ssl_set)(MYSQL *mysql, const char *key, const char *cert, const char *ca, const char *capath, const char *cipher);
  my_bool STDCALL (* real_mysql_change_user)(MYSQL *mysql, const char *user, const char *passwd, const char *db);
  MYSQL * STDCALL (* real_mysql_real_connect)(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long clientflag);
  int STDCALL (* real_mysql_select_db)(MYSQL *mysql, const char *db);
  int STDCALL (* real_mysql_query)(MYSQL *mysql, const char *q);
  int STDCALL (* real_mysql_send_query)(MYSQL *mysql, const char *q, unsigned long length);
  int STDCALL (* real_mysql_real_query)(MYSQL *mysql, const char *q, unsigned long length);
  MYSQL_RES * STDCALL (* real_mysql_store_result)(MYSQL *mysql);
  MYSQL_RES * STDCALL (* real_mysql_use_result)(MYSQL *mysql);
  void STDCALL (* real_mysql_get_character_set_info)(MYSQL *mysql, MY_CHARSET_INFO *charset);
  int STDCALL (* real_mysql_shutdown)(MYSQL *mysql, enum mysql_enum_shutdown_level shutdown_level);
  int STDCALL (* real_mysql_dump_debug_info)(MYSQL *mysql);
  int STDCALL (* real_mysql_refresh)(MYSQL *mysql, unsigned int refresh_options);
  int STDCALL (* real_mysql_kill)(MYSQL *mysql,unsigned long pid);
  int STDCALL (* real_mysql_set_server_option)(MYSQL *mysql, enum enum_mysql_set_option option);
  int STDCALL (* real_mysql_ping)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_stat)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_get_server_info)(MYSQL *mysql);
  const char * STDCALL (* real_mysql_get_client_info)(void);
  unsigned long STDCALL (* real_mysql_get_client_version)(void);
  const char * STDCALL (* real_mysql_get_host_info)(MYSQL *mysql);
  unsigned long STDCALL (* real_mysql_get_server_version)(MYSQL *mysql);
  unsigned int STDCALL (* real_mysql_get_proto_info)(MYSQL *mysql);
  MYSQL_RES * STDCALL (* real_mysql_list_dbs)(MYSQL *mysql,const char *wild);
  MYSQL_RES * STDCALL (* real_mysql_list_tables)(MYSQL *mysql,const char *wild);
  MYSQL_RES * STDCALL (* real_mysql_list_processes)(MYSQL *mysql);
  int STDCALL (* real_mysql_options)(MYSQL *mysql,enum mysql_option option, const void *arg);
  void STDCALL (* real_mysql_free_result)(MYSQL_RES *result);
  void STDCALL (* real_mysql_data_seek)(MYSQL_RES *result, my_ulonglong offset);
  MYSQL_ROW_OFFSET STDCALL (* real_mysql_row_seek)(MYSQL_RES *result, MYSQL_ROW_OFFSET offset);
  MYSQL_FIELD_OFFSET STDCALL (* real_mysql_field_seek)(MYSQL_RES *result, MYSQL_FIELD_OFFSET offset);
  MYSQL_ROW STDCALL (* real_mysql_fetch_row)(MYSQL_RES *result);
  unsigned long * STDCALL (* real_mysql_fetch_lengths)(MYSQL_RES *result);
  MYSQL_FIELD * STDCALL (* real_mysql_fetch_field)(MYSQL_RES *result);
  MYSQL_RES * STDCALL (* real_mysql_list_fields)(MYSQL *mysql, const char *table, const char *wild);
  unsigned long STDCALL (* real_mysql_escape_string)(char *to,const char *from, unsigned long from_length);
  unsigned long STDCALL (* real_mysql_hex_string)(char *to,const char *from, unsigned long from_length);
  unsigned long STDCALL (* real_mysql_real_escape_string)(MYSQL *mysql, char *to,const char *from, unsigned long length);
  void STDCALL (* real_mysql_debug)(const char *debug);
  void STDCALL (* real_myodbc_remove_escape)(MYSQL *mysql,char *name);
  unsigned int STDCALL (* real_mysql_thread_safe)(void);
  my_bool STDCALL (* real_mysql_embedded)(void);
  my_bool STDCALL (* real_mysql_read_query_result)(MYSQL *mysql);
  MYSQL_STMT * STDCALL (* real_mysql_stmt_init)(MYSQL *mysql);
  int STDCALL (* real_mysql_stmt_prepare)(MYSQL_STMT *stmt, const char *query, unsigned long length);
  int STDCALL (* real_mysql_stmt_execute)(MYSQL_STMT *stmt);
  int STDCALL (* real_mysql_stmt_fetch)(MYSQL_STMT *stmt);
  int STDCALL (* real_mysql_stmt_fetch_column)(MYSQL_STMT *stmt, MYSQL_BIND *bind_arg, unsigned int column, unsigned long offset);
  int STDCALL (* real_mysql_stmt_store_result)(MYSQL_STMT *stmt);
  unsigned long STDCALL (* real_mysql_stmt_param_count)(MYSQL_STMT * stmt);
  my_bool STDCALL (* real_mysql_stmt_attr_set)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *attr);
  my_bool STDCALL (* real_mysql_stmt_attr_get)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *attr);
  my_bool STDCALL (* real_mysql_stmt_bind_param)(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
  my_bool STDCALL (* real_mysql_stmt_bind_result)(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
  my_bool STDCALL (* real_mysql_stmt_close)(MYSQL_STMT * stmt);
  my_bool STDCALL (* real_mysql_stmt_reset)(MYSQL_STMT * stmt);
  my_bool STDCALL (* real_mysql_stmt_free_result)(MYSQL_STMT *stmt);
  my_bool STDCALL (* real_mysql_stmt_send_long_data)(MYSQL_STMT *stmt, unsigned int param_number, const char *data, unsigned long length);
  MYSQL_RES *STDCALL (* real_mysql_stmt_result_metadata)(MYSQL_STMT *stmt);
  MYSQL_RES *STDCALL (* real_mysql_stmt_param_metadata)(MYSQL_STMT *stmt);
  unsigned int STDCALL (* real_mysql_stmt_errno)(MYSQL_STMT * stmt);
  const char *STDCALL (* real_mysql_stmt_error)(MYSQL_STMT * stmt);
  const char *STDCALL (* real_mysql_stmt_sqlstate)(MYSQL_STMT * stmt);
  MYSQL_ROW_OFFSET STDCALL (* real_mysql_stmt_row_seek)(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET offset);
  MYSQL_ROW_OFFSET STDCALL (* real_mysql_stmt_row_tell)(MYSQL_STMT *stmt);
  void STDCALL (* real_mysql_stmt_data_seek)(MYSQL_STMT *stmt, my_ulonglong offset);
  my_ulonglong STDCALL (* real_mysql_stmt_num_rows)(MYSQL_STMT *stmt);
  my_ulonglong STDCALL (* real_mysql_stmt_affected_rows)(MYSQL_STMT *stmt);
  my_ulonglong STDCALL (* real_mysql_stmt_insert_id)(MYSQL_STMT *stmt);
  unsigned int STDCALL (* real_mysql_stmt_field_count)(MYSQL_STMT *stmt);
  my_bool STDCALL (* real_mysql_commit)(MYSQL * mysql);
  my_bool STDCALL (* real_mysql_rollback)(MYSQL * mysql);
  my_bool STDCALL (* real_mysql_autocommit)(MYSQL * mysql, my_bool auto_mode);
  my_bool STDCALL (* real_mysql_more_results)(MYSQL *mysql);
  int STDCALL (* real_mysql_next_result)(MYSQL *mysql);
  void STDCALL (* real_mysql_close)(MYSQL *sock);

//NEW API MYSQL5.5
  const char * (* real_mysql_get_ssl_cipher)(MYSQL *mysql);
  void (* real_mysql_set_local_infile_default)(MYSQL *mysql);
  void (* real_mysql_set_local_infile_handler)(MYSQL *mysql, int (*local_infile_init)(void **, const char *, void *), int (*local_infile_read)(void *, char *, unsigned int), void (*local_infile_end)(void *), int (*local_infile_error)(void *, char*, unsigned int), void *userdata);
  int STDCALL (* real_mysql_stmt_next_result)(MYSQL_STMT *stmt);
  struct st_mysql_client_plugin * (* real_mysql_client_find_plugin)(MYSQL *mysql, const char *name, int type);
  struct st_mysql_client_plugin * (* real_mysql_client_register_plugin)(MYSQL *mysql, struct st_mysql_client_plugin *plugin);
  struct st_mysql_client_plugin * (* real_mysql_load_plugin)(MYSQL *mysql, const char *name, int type, int argc, ...);
  struct st_mysql_client_plugin * (* real_mysql_load_plugin_v)(MYSQL *mysql, const char *name, int type, int argc, va_list args);
  int (* real_mysql_plugin_options)(struct st_mysql_client_plugin *plugin, const char *option, const void *value);
//END NEW API MYSQL5.5
}FuncSet;

/*
  Load real libmysql function from dynamic library

  RETURN
    0	ok
    1	error
*/
int init_func_set(FuncSet *set);
void destroy_func_set(FuncSet *set);
OB_SQL_CPP_END
#endif
