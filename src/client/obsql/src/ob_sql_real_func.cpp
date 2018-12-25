#include <dlfcn.h>
#include "ob_sql_real_func.h"
#include <stdio.h>
#include <stdlib.h>
#include "ob_sql_global.h"
#include "tblog.h"
#include <string.h>   // memset

unsigned long native_client_version;
char report_version_buffer[512];

void *handle = NULL;
const char *LIBMYSQLCLIENT_FILE = "libmysqlclient_r.so";

int init_func_set(FuncSet *set)
{
  int ret = 0;
  char *error;

  if (NULL != handle)
  {
    TBSYS_LOG(WARN, "init_func_set twice");
    ret = OB_SQL_ERROR;
  }
  else
  {
    handle = dlopen(LIBMYSQLCLIENT_FILE, RTLD_LAZY);
    if (!handle)
    {
      TBSYS_LOG(ERROR, "dlopen \"%s\" fail, %s", LIBMYSQLCLIENT_FILE, dlerror());
      ret = OB_SQL_ERROR;
    }

    // Clear any existing error
    dlerror();

    (void)memset(set, 0, sizeof(FuncSet));
  }

  //load function from libmysqlclient.so
  if (0 == ret)
  {
    set->real_mysql_server_init = reinterpret_cast<int (*)(int, char**, char**)>(dlsym(handle, "mysql_server_init"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_server_end = reinterpret_cast<void (*)()>(dlsym(handle, "mysql_server_end"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  

  if (0 == ret)
  {
    set->real_mysql_get_parameters = reinterpret_cast<MYSQL_PARAMETERS* (*)()>(dlsym(handle, "mysql_get_parameters"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
 
  if (0 == ret)
  {
    set->real_mysql_thread_init = reinterpret_cast<my_bool (*)()>(dlsym(handle, "mysql_thread_init"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  

  if (0 == ret)
  {
    set->real_mysql_thread_end = reinterpret_cast<void (*)()>(dlsym(handle, "mysql_thread_end"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_num_rows = reinterpret_cast<my_ulonglong (*)(MYSQL_RES*)>(dlsym(handle, "mysql_num_rows"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_num_fields = reinterpret_cast<unsigned int (*)(MYSQL_RES*)>(dlsym(handle, "mysql_num_fields"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_eof = reinterpret_cast<my_bool (*)(MYSQL_RES*)>(dlsym(handle, "mysql_eof"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_fetch_field_direct = reinterpret_cast<MYSQL_FIELD* (*)(MYSQL_RES*, unsigned int)>(dlsym(handle, "mysql_fetch_field_direct"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_fetch_fields = reinterpret_cast<MYSQL_FIELD* (*)(MYSQL_RES*)>(dlsym(handle, "mysql_fetch_fields"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_row_tell = reinterpret_cast<MYSQL_ROWS* (*)(MYSQL_RES*)>(dlsym(handle, "mysql_row_tell"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
  
    }
  }

  if (0 == ret)
  {
    set->real_mysql_field_tell = reinterpret_cast<MYSQL_FIELD_OFFSET (*)(MYSQL_RES*)>(dlsym(handle, "mysql_field_tell"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_field_count = reinterpret_cast<unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_field_count"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_affected_rows = reinterpret_cast<my_ulonglong (*)(MYSQL*)>(dlsym(handle, "mysql_affected_rows"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_insert_id = reinterpret_cast<my_ulonglong (*)(MYSQL*)>(dlsym(handle, "mysql_insert_id"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_errno = reinterpret_cast<unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_errno"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_error = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_error"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_sqlstate = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_sqlstate"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_warning_count = reinterpret_cast<unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_warning_count"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_info = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_thread_id = reinterpret_cast<long unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_thread_id"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_character_set_name = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_character_set_name"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_set_character_set = reinterpret_cast<int (*)(MYSQL*, const char*)>(dlsym(handle, "mysql_set_character_set"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_init = reinterpret_cast<MYSQL* (*)(MYSQL*)>(dlsym(handle, "mysql_init"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_ssl_set = reinterpret_cast<my_bool (*)(MYSQL*, const char*, const char*, const char*, const char*, const char*)>(dlsym(handle, "mysql_ssl_set"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1; 
    }
  }

  if (0 == ret)
  {
    set->real_mysql_change_user = reinterpret_cast<my_bool (*)(MYSQL*, const char*, const char*, const char*)>(dlsym(handle, "mysql_change_user"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_real_connect = reinterpret_cast<MYSQL* (*)(MYSQL*, const char*, const char*, const char*, const char*, unsigned int, const char*, long unsigned int)>(dlsym(handle, "mysql_real_connect"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }

  if (0 == ret)
  {
    set->real_mysql_select_db = reinterpret_cast<int (*)(MYSQL*, const char*)>(dlsym(handle, "mysql_select_db"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_query = reinterpret_cast<int (*)(MYSQL*, const char*)>(dlsym(handle, "mysql_query"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_send_query = reinterpret_cast<int (*)(MYSQL*, const char*, long unsigned int)>(dlsym(handle, "mysql_send_query"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_real_query = reinterpret_cast<int (*)(MYSQL*, const char*, long unsigned int)>(dlsym(handle, "mysql_real_query"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_store_result = reinterpret_cast<MYSQL_RES* (*)(MYSQL*)>(dlsym(handle, "mysql_store_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_use_result = reinterpret_cast<MYSQL_RES* (*)(MYSQL*)>(dlsym(handle, "mysql_use_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_get_character_set_info = reinterpret_cast<void (*)(MYSQL*, MY_CHARSET_INFO*)>(dlsym(handle, "mysql_get_character_set_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_shutdown = reinterpret_cast<int (*)(MYSQL*, mysql_enum_shutdown_level)>(dlsym(handle, "mysql_shutdown"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_dump_debug_info = reinterpret_cast<int (*)(MYSQL*)>(dlsym(handle, "mysql_dump_debug_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_refresh = reinterpret_cast<int (*)(MYSQL*, unsigned int)>(dlsym(handle, "mysql_refresh"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_kill = reinterpret_cast<int (*)(MYSQL*, long unsigned int)>(dlsym(handle, "mysql_kill"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_set_server_option = reinterpret_cast<int (*)(MYSQL*, enum_mysql_set_option)>(dlsym(handle, "mysql_set_server_option"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_ping = reinterpret_cast<int (*)(MYSQL*)>(dlsym(handle, "mysql_ping"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stat = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_stat"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_get_server_info = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_get_server_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_get_client_info = reinterpret_cast<const char* (*)()>(dlsym(handle, "mysql_get_client_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_get_client_version = reinterpret_cast<long unsigned int (*)()>(dlsym(handle, "mysql_get_client_version"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }

  if (0 == ret)
  {
    native_client_version = (*set->real_mysql_get_client_version)();
    set->real_mysql_get_host_info = reinterpret_cast<const char* (*)(MYSQL*)>(dlsym(handle, "mysql_get_host_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }

  if (0 == ret)
  {
    set->real_mysql_get_server_version = reinterpret_cast<long unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_get_server_version"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_get_proto_info = reinterpret_cast<unsigned int (*)(MYSQL*)>(dlsym(handle, "mysql_get_proto_info"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_list_dbs = reinterpret_cast<MYSQL_RES* (*)(MYSQL*, const char*)>(dlsym(handle, "mysql_list_dbs"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_list_tables = reinterpret_cast<MYSQL_RES* (*)(MYSQL*, const char*)>(dlsym(handle, "mysql_list_tables"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_list_processes = reinterpret_cast<MYSQL_RES* (*)(MYSQL*)>(dlsym(handle, "mysql_list_processes"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_options = reinterpret_cast<int (*)(MYSQL*, mysql_option, const void*)>(dlsym(handle, "mysql_options"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  

  if (0 == ret)
  {
    set->real_mysql_free_result = reinterpret_cast<void (*)(MYSQL_RES*)>(dlsym(handle, "mysql_free_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_data_seek = reinterpret_cast<void (*)(MYSQL_RES*, my_ulonglong)>(dlsym(handle, "mysql_data_seek"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_row_seek = reinterpret_cast<MYSQL_ROWS* (*)(MYSQL_RES*, MYSQL_ROWS*)>(dlsym(handle, "mysql_row_seek"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_field_seek = reinterpret_cast<MYSQL_FIELD_OFFSET (*)(MYSQL_RES*, MYSQL_FIELD_OFFSET)>(dlsym(handle, "mysql_field_seek"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_fetch_row = reinterpret_cast<char** (*)(MYSQL_RES*)>(dlsym(handle, "mysql_fetch_row"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_fetch_lengths = reinterpret_cast<long unsigned int* (*)(MYSQL_RES*)>(dlsym(handle, "mysql_fetch_lengths"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_fetch_field = reinterpret_cast<MYSQL_FIELD* (*)(MYSQL_RES*)>(dlsym(handle, "mysql_fetch_field"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_list_fields = reinterpret_cast<MYSQL_RES* (*)(MYSQL*, const char*, const char*)>(dlsym(handle, "mysql_list_fields"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_escape_string = reinterpret_cast<long unsigned int (*)(char*, const char*, long unsigned int)>(dlsym(handle, "mysql_escape_string"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_hex_string = reinterpret_cast<long unsigned int (*)(char*, const char*, long unsigned int)>(dlsym(handle, "mysql_hex_string"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_real_escape_string = reinterpret_cast<long unsigned int (*)(MYSQL*, char*, const char*, long unsigned int)>(dlsym(handle, "mysql_real_escape_string"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;   
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_debug = reinterpret_cast<void (*)(const char*)>(dlsym(handle, "mysql_debug"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_myodbc_remove_escape = reinterpret_cast<void (*)(MYSQL*, char*)>(dlsym(handle, "myodbc_remove_escape"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_thread_safe = reinterpret_cast<unsigned int (*)()>(dlsym(handle, "mysql_thread_safe"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_embedded = reinterpret_cast<my_bool (*)()>(dlsym(handle, "mysql_embedded"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_read_query_result = reinterpret_cast<my_bool (*)(MYSQL*)>(dlsym(handle, "mysql_read_query_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_init = reinterpret_cast<MYSQL_STMT* (*)(MYSQL*)>(dlsym(handle, "mysql_stmt_init"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_prepare = reinterpret_cast<int (*)(MYSQL_STMT*, const char*, long unsigned int)>(dlsym(handle, "mysql_stmt_prepare"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_execute = reinterpret_cast<int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_execute"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_fetch = reinterpret_cast<int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_fetch"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_fetch_column = reinterpret_cast<int (*)(MYSQL_STMT*, MYSQL_BIND*, unsigned int, long unsigned int)>(dlsym(handle, "mysql_stmt_fetch_column"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;   
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_store_result = reinterpret_cast<int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_store_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_param_count = reinterpret_cast<long unsigned int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_param_count"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_attr_set = reinterpret_cast<my_bool (*)(MYSQL_STMT*, enum_stmt_attr_type, const void*)>(dlsym(handle, "mysql_stmt_attr_set"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_attr_get = reinterpret_cast<my_bool (*)(MYSQL_STMT*, enum_stmt_attr_type, void*)>(dlsym(handle, "mysql_stmt_attr_get"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_bind_param = reinterpret_cast<my_bool (*)(MYSQL_STMT*, MYSQL_BIND*)>(dlsym(handle, "mysql_stmt_bind_param"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_bind_result = reinterpret_cast<my_bool (*)(MYSQL_STMT*, MYSQL_BIND*)>(dlsym(handle, "mysql_stmt_bind_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_close = reinterpret_cast<my_bool (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_close"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_reset = reinterpret_cast<my_bool (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_reset"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_free_result = reinterpret_cast<my_bool (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_free_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_send_long_data = reinterpret_cast<my_bool (*)(MYSQL_STMT*, unsigned int, const char*, long unsigned int)>(dlsym(handle, "mysql_stmt_send_long_data"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;   
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_result_metadata = reinterpret_cast<MYSQL_RES* (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_result_metadata"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_param_metadata = reinterpret_cast<MYSQL_RES* (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_param_metadata"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_errno = reinterpret_cast<unsigned int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_errno"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_error = reinterpret_cast<const char* (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_error"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_sqlstate = reinterpret_cast<const char* (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_sqlstate"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_row_seek = reinterpret_cast<MYSQL_ROWS* (*)(MYSQL_STMT*, MYSQL_ROWS*)>(dlsym(handle, "mysql_stmt_row_seek"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_row_tell = reinterpret_cast<MYSQL_ROWS* (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_row_tell"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_data_seek = reinterpret_cast<void (*)(MYSQL_STMT*, my_ulonglong)>(dlsym(handle, "mysql_stmt_data_seek"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_num_rows = reinterpret_cast<my_ulonglong (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_num_rows"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_affected_rows = reinterpret_cast<my_ulonglong (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_affected_rows"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_insert_id = reinterpret_cast<my_ulonglong (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_insert_id"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_stmt_field_count = reinterpret_cast<unsigned int (*)(MYSQL_STMT*)>(dlsym(handle, "mysql_stmt_field_count"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_commit = reinterpret_cast<my_bool (*)(MYSQL*)>(dlsym(handle, "mysql_commit"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_rollback = reinterpret_cast<my_bool (*)(MYSQL*)>(dlsym(handle, "mysql_rollback"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_autocommit = reinterpret_cast<my_bool (*)(MYSQL*, my_bool)>(dlsym(handle, "mysql_autocommit"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_more_results = reinterpret_cast<my_bool (*)(MYSQL*)>(dlsym(handle, "mysql_more_results"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  
  if (0 == ret)
  {
    set->real_mysql_next_result = reinterpret_cast<int (*)(MYSQL*)>(dlsym(handle, "mysql_next_result"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }
  

  if (0 == ret)
  {
    set->real_mysql_close = reinterpret_cast<void (*)(MYSQL*)>(dlsym(handle, "mysql_close"));
    if ((error = dlerror()) != NULL)  {
      fprintf (stderr, "%s\n", error);
      ret = -1;
    }
  }

  if (OB_SQL_CLIENT_VERSION_55_MIN < native_client_version)
  {
    
    if (0 == ret)
    {
      set->real_mysql_get_ssl_cipher = reinterpret_cast<const char * (*)(MYSQL *)>(dlsym(handle, "mysql_get_ssl_cipher"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;
      }
    }
    

    if (0 == ret)
    {
      set->real_mysql_set_local_infile_default = reinterpret_cast<void (*)(MYSQL *)>(dlsym(handle, "mysql_set_local_infile_default"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;
      }
    }
    

    if (0 == ret)
    {
      set->real_mysql_set_local_infile_handler = reinterpret_cast<void (*)(MYSQL *, int (*)(void **, const char *, void *), int (*)(void *, char *, unsigned int), void (*)(void *), int (*)(void *, char*, unsigned int), void *)>(dlsym(handle, "mysql_set_local_infile_handler"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;     
      }
    }

    
    if (0 == ret)
    {
      set->real_mysql_stmt_next_result = reinterpret_cast<int (*)(MYSQL_STMT *)>(
        dlsym(handle, "mysql_stmt_next_result"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;     
      }
    }

    
    if (0 == ret)
    {
      set->real_mysql_client_find_plugin = reinterpret_cast<struct st_mysql_client_plugin * (*)(MYSQL *, const char *, int)>(
        dlsym(handle, "mysql_client_find_plugin"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;     
      }
    }
    
    if (0 == ret)
    {
      set->real_mysql_client_register_plugin = reinterpret_cast<struct st_mysql_client_plugin * (*)(MYSQL *, struct st_mysql_client_plugin *)>(
        dlsym(handle, "mysql_client_register_plugin"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;     
      }
    }

    if (0 == ret)
    {
      set->real_mysql_load_plugin = reinterpret_cast<struct st_mysql_client_plugin * (*)(MYSQL *, const char *, int, int, ...)>(
        dlsym(handle, "mysql_load_plugin"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;
      }
    }

    
    if (0 == ret)
    {
      set->real_mysql_load_plugin_v = reinterpret_cast<struct st_mysql_client_plugin * (*)(MYSQL *, const char *, int, int, va_list)>(
        dlsym(handle, "mysql_load_plugin_v"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1;     
      }
    }

    
    if (0 == ret)
    {
      set->real_mysql_plugin_options = reinterpret_cast<int (*)(struct st_mysql_client_plugin *, const char *, const void *)>(
        dlsym(handle, "mysql_plugin_options"));
      if ((error = dlerror()) != NULL)  {
        fprintf (stderr, "%s\n", error);
        ret = -1; 
      }
    }
  }

  if (OB_SQL_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "load MYSQLCLIENT \"%s\" success", LIBMYSQLCLIENT_FILE);
  }

  return ret;
}

void destroy_func_set(FuncSet *set)
{
  if (NULL != handle)
  {
    TBSYS_LOG(INFO, "unload MYSQLCLIENT \"%s\" begin", LIBMYSQLCLIENT_FILE);
    (void)memset(set, 0, sizeof(FuncSet));
    dlclose(handle);
    handle = NULL;
    TBSYS_LOG(INFO, "unload MYSQLCLIENT \"%s\" end", LIBMYSQLCLIENT_FILE);
  }
}
