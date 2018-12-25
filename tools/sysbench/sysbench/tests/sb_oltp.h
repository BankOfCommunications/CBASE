/* Copyright (C) 2004 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef SB_OLTP_H
#define SB_OLTP_H

#include <stdint.h>
#include "sb_list.h"

#define MAX_COLUMN_SIZE 512
#define MAX_VAR_COUNT 512
#define MAX_COLUMN_COUNT 512
#define MAX_SQL_LEN 10240
#define MAX_TABLE_COUNT 1024

/* Column type */
typedef enum
{
  INT,
  STRING,
  DATE,
  ERROR
}sb_column_type;
/* prepare insert rule */
typedef struct 
{
   char name[256];
   union
   {
       int64_t i_start;
       char s_start[MAX_COLUMN_SIZE];
   }start;
   sb_column_type type;
   int32_t step;
} sb_column_rule_t;

/* SQL query types definiton */
typedef enum
  {
  SB_SQL_QUERY_NULL,
  SB_SQL_QUERY_LOCK,
  SB_SQL_QUERY_UNLOCK,
  
  SB_SQL_QUERY_READ_START,  
  SB_SQL_QUERY_POINT,
  SB_SQL_QUERY_JOIN_POINT, // junyue
  SB_SQL_QUERY_JOIN_RANGE, // junyue
  SB_SQL_QUERY_JOIN_RANGE_SUM, // junyue
  SB_SQL_QUERY_JOIN_RANGE_ORDER, // junyue
  SB_SQL_QUERY_JOIN_RANGE_DISTINCT, // junyue
  SB_SQL_QUERY_CALL,
  SB_SQL_QUERY_RANGE,
  SB_SQL_QUERY_RANGE_SUM,
  SB_SQL_QUERY_RANGE_ORDER,
  SB_SQL_QUERY_RANGE_DISTINCT,
  SB_SQL_QUERY_DELETE,
  SB_SQL_QUERY_INSERT,
  SB_SQL_QUERY_UPDATE_NON_INDEX,
  SB_SQL_QUERY_REPLACE,
  SB_SQL_QUERY_UPDATE_INDEX,

  SB_SQL_QUERY_READ_END,  // it's a bug

  SB_SQL_QUERY_RECONNECT
} sb_sql_query_type_t;

/* For prepare command */
typedef struct
{
  uint32_t table_num;
  char table_names[MAX_TABLE_COUNT][MAX_SQL_LEN];
  char rule[MAX_TABLE_COUNT][MAX_SQL_LEN];
} sb_prepare_t;

/* For cleanup command */
typedef struct
{
  char drop_sql[MAX_SQL_LEN];
} sb_cleanup_t;


/* variables in sql statement */
typedef struct
{
  sb_column_type sql_vars[MAX_VAR_COUNT];
  int var_bind[MAX_VAR_COUNT];
  char expr[MAX_VAR_COUNT][20];

  int count;
} sb_sql_val_list_t;

typedef struct
{
  char point_sql[MAX_SQL_LEN];
  char range_sql[MAX_SQL_LEN];
  char sum_sql[MAX_SQL_LEN];
  char distinct_sql[MAX_SQL_LEN];
  char orderby_sql[MAX_SQL_LEN];
 
  char point_join_sql[MAX_SQL_LEN];
  char range_join_sql[MAX_SQL_LEN];
  char sum_join_sql[MAX_SQL_LEN];
  char distinct_join_sql[MAX_SQL_LEN];
  char orderby_join_sql[MAX_SQL_LEN];
  
  char update_key_sql[MAX_SQL_LEN];
  char update_nonkey_sql[MAX_SQL_LEN];
  char delete_sql[MAX_SQL_LEN];
  char insert_sql[MAX_SQL_LEN];
  char replace_sql[MAX_SQL_LEN];
  
  sb_sql_val_list_t point_sql_vars;
  sb_sql_val_list_t range_sql_vars;
  sb_sql_val_list_t sum_sql_vars;
  sb_sql_val_list_t distinct_sql_vars;
  sb_sql_val_list_t orderby_sql_vars;
  
  sb_sql_val_list_t point_join_sql_vars;
  sb_sql_val_list_t range_join_sql_vars;
  sb_sql_val_list_t sum_join_sql_vars;
  sb_sql_val_list_t distinct_join_sql_vars;
  sb_sql_val_list_t orderby_join_sql_vars;
  
  sb_sql_val_list_t update_key_sql_vars;
  sb_sql_val_list_t update_nonkey_sql_vars;
  sb_sql_val_list_t delete_sql_vars;
  sb_sql_val_list_t insert_sql_vars;
  sb_sql_val_list_t replace_sql_vars;

  int32_t point_sql_rows;
  int32_t range_sql_rows;
  int32_t sum_sql_rows;
  int32_t distinct_sql_rows;
  int32_t orderby_sql_rows;

  int32_t point_join_sql_rows;
  int32_t range_join_sql_rows;
  int32_t sum_join_sql_rows;
  int32_t distinct_join_sql_rows;
  int32_t orderby_join_sql_rows;

  int32_t update_key_sql_rows;
  int32_t update_nonkey_sql_rows;
  int32_t delete_sql_rows;
  int32_t insert_sql_rows;
  int32_t replace_sql_rows;

} sb_run_t;


/* SQL queries definitions */
typedef struct
{
    union {
        int32_t  i_var;
        char     s_var[MAX_COLUMN_SIZE];
    }value;
    uint64_t data_length;
    sb_column_type type;

}sb_sql_bind_variables_t;

typedef struct
{

  sb_sql_bind_variables_t vars[MAX_VAR_COUNT];
  int32_t count;

} sb_varlist_in_query_t;

/* DML */
/*
typedef struct
{
  //int id;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_point_t;

typedef struct
{
  //int from;
  //int to;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_range_t;

typedef struct
{
  //int id;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_update_t;

typedef struct
{
  //int id;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_delete_t;

typedef struct
{
  //int id;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_insert_t;

typedef struct
{
  //int id;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_join_point_t;

typedef struct
{
  //int from;
  //int to;
  sb_sql_bind_variables_t vars[MAX_COLUMN_SIZE];
  int32_t count;
} sb_sql_query_join_range_t;
*/
typedef struct
{
  int thread_id;
  int nthreads;
} sb_sql_query_call_t;

/* SQL request definition */

typedef struct
{
  sb_sql_query_type_t type;
  uint32_t        num_times;   /* times to repeat the query */
  uint32_t        think_time;  /* sleep this time before executing query */
  int32_t  nrows;       /* expected number of rows in a result set */
  sb_varlist_in_query_t  *varlist;
/*
  union
  {
    sb_sql_query_point_t   point_query;
    sb_sql_query_range_t   range_query;
    sb_sql_query_update_t  update_query;
    sb_sql_query_delete_t  delete_query;
    sb_sql_query_insert_t  insert_query;
    sb_sql_query_join_point_t    join_point_query; // junyue
    sb_sql_query_join_range_t    join_range_query; // junyue
  } u;
*/
  sb_list_item_t listitem;  /* this struct can be stored in a linked list */
} sb_sql_query_t;

typedef struct
{
  sb_list_t *queries;  /* list of actual queries to perform */
} sb_sql_request_t;

int register_test_oltp(sb_list_t *tests);

/* add by junyue */
typedef struct
{
  sb_prepare_t prepare;
  sb_run_t run;
  sb_cleanup_t cleanup;

} sb_schema_t;
#endif
