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

#ifndef SB_TRXTRX_H
#define SB_TRXTRX_H

#include "sb_list.h"


/** MAX_STRING_LEN must bigger than MAX_CHECK_LEN 10 */
#define MAX_STRING_LEN 300 

#define MAX_CHECK_LEN 250 
/* SQL query types definiton */

typedef enum
  {
  SB_TRXSQL_QUERY_NULL,
  SB_TRXSQL_QUERY_LOCK,
  SB_TRXSQL_QUERY_ROLLBACK,
  SB_TRXSQL_QUERY_UNLOCK,
  SB_TRXSQL_QUERY_POINT, // select start
  SB_TRXSQL_QUERY_JOIN_POINT, // junyue
  SB_TRXSQL_QUERY_JOIN_RANGE, // junyue
  SB_TRXSQL_QUERY_JOIN_RANGE_SUM, // junyue
  SB_TRXSQL_QUERY_JOIN_RANGE_ORDER, // junyue
  SB_TRXSQL_QUERY_JOIN_RANGE_DISTINCT, // junyue
  SB_TRXSQL_QUERY_CALL,
  SB_TRXSQL_QUERY_RANGE,
  SB_TRXSQL_QUERY_RANGE_SUM,
  SB_TRXSQL_QUERY_RANGE_ORDER,
  SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_STR1,
  SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_INT1,
  SB_TRXSQL_QUERY_SELECT_FOR_DELETE_INSERT,
  SB_TRXSQL_QUERY_RANGE_DISTINCT, // select end
  SB_TRXSQL_QUERY_UPDATE_INT1,
  SB_TRXSQL_QUERY_UPDATE_INT2,
  SB_TRXSQL_QUERY_UPDATE_STR1,
  SB_TRXSQL_QUERY_UPDATE_STR2,
  SB_TRXSQL_QUERY_REPLACE,
  SB_TRXSQL_QUERY_DELETE,
  SB_TRXSQL_QUERY_INSERT,
  SB_TRXSQL_QUERY_NINSERT,
  SB_TRXSQL_QUERY_RECONNECT,

  SB_TRXSQL_QUERY_UPDATE_INTEGER,
  SB_TRXSQL_QUERY_UPDATE_INIT_INTEGER,
  SB_TRXSQL_QUERY_CHECK_INTEGER,
  SB_TRXSQL_QUERY_REPLACE_STRING,
  SB_TRXSQL_QUERY_UPDATE_STRING_S3,
  SB_TRXSQL_QUERY_UPDATE_STRING_S4,
  SB_TRXSQL_QUERY_UPDATE_SUBSTRING,
  SB_TRXSQL_QUERY_CHECK_STRING,
  SB_TRXSQL_QUERY_INSERT_CORRECT,
  SB_TRXSQL_QUERY_CHECK_INSERT,

  SB_TRXSQL_QUERY_DELETE_TABLE_1,
  SB_TRXSQL_QUERY_INSERT_TABLE_1,
  SB_TRXSQL_QUERY_UPDATE_TABLE_2,
  SB_TRXSQL_QUERY_CHECK_MULTI_TABLE
} sb_trxsql_query_type_t;

/* SQL queries definitions */

typedef struct
{
  uint32_t id;
} sb_trxsql_query_point_t;

typedef struct
{
  uint32_t from;
  uint32_t to;
} sb_trxsql_query_range_t;

typedef struct
{
  uint32_t id;
} sb_trxsql_query_update_t;

typedef struct
{
  uint32_t id;
} sb_trxsql_query_delete_t;

typedef struct
{
  uint32_t id;
} sb_trxsql_query_insert_t;

typedef struct
{
  uint32_t id;
} sb_trxsql_query_replace_t;
/* junyue start */
typedef struct
{
  uint32_t id;
} sb_trxsql_query_join_point_t;

typedef struct
{
  uint32_t from;
  uint32_t to;
} sb_trxsql_query_join_range_t;
/* junyue end */


typedef struct
{
    int32_t value;
    int32_t result;
    double  fvalue;
    double  fresult;
    uint32_t id;
} sb_trxsql_query_update_integer;

typedef struct
{
    int32_t value;
    uint32_t id;
    double   fvalue;
} sb_trxsql_query_update_init_integer;

typedef struct
{
    uint32_t id;
} sb_trxsql_query_check_integer;

typedef struct
{
    char str[MAX_STRING_LEN];
    uint32_t id;
    unsigned long strlen;
}sb_trxsql_query_update_string_s3;

typedef struct
{
    char str[MAX_STRING_LEN];
    uint32_t id;
}sb_trxsql_query_update_string_s4;

typedef struct
{
    char str[MAX_STRING_LEN];
    uint32_t id;
    unsigned long strlen;
}sb_trxsql_query_update_substring;

typedef struct
{
    uint32_t id;
}sb_trxsql_query_check_string;

typedef struct
{
    uint32_t pk1;
    uint32_t pk2;
    int32_t  ivalue;
    double   fvalue;
    char     svalue[MAX_STRING_LEN];
    unsigned long strlen;
    int32_t  hash_value;
}sb_trxsql_query_insert_correct;

typedef struct
{
    uint32_t pk;
}sb_trxsql_query_delete_table_1;

typedef struct
{
    uint32_t pk;
    uint32_t ivalue;
    double   dvalue;
    char     svalue[MAX_STRING_LEN];
    int32_t  hash_value;
    unsigned long strlen;
}sb_trxsql_query_insert_table_1;

typedef struct
{
    uint32_t pk;
    uint32_t ivalue;
    double   dvalue;
    char     svalue[MAX_STRING_LEN];
    unsigned long strlen;
    int32_t  hash_value;
}sb_trxsql_query_update_table_2;

typedef struct
{
    uint32_t id;
}sb_trxsql_query_check_insert_correct;

typedef struct
{
    uint32_t id;
}sb_trxsql_query_check_table;

typedef struct
{
  uint32_t thread_id;
  uint32_t nthreads;
} sb_trxsql_query_call_t;

/* SQL request definition */

typedef struct
{
  char sql_id[50];
  sb_trxsql_query_type_t type;
  uint32_t        num_times;   /* times to repeat the query */
  uint32_t        think_time;  /* sleep this time before executing query */
  uint64_t  nrows;       /* expected number of rows in a result set */
  union
  {
    sb_trxsql_query_point_t   point_query;
    sb_trxsql_query_range_t   range_query;
    sb_trxsql_query_update_t  update_query;
    sb_trxsql_query_delete_t  delete_query;
    sb_trxsql_query_insert_t  insert_query;
    sb_trxsql_query_replace_t  replace_query;
    sb_trxsql_query_join_point_t    join_point_query; // junyue
    sb_trxsql_query_join_range_t    join_range_query; // junyue

    sb_trxsql_query_update_integer update_integer;
    sb_trxsql_query_update_init_integer update_init_integer;
    sb_trxsql_query_check_integer check_integer;
    sb_trxsql_query_insert_correct  insert_correct;
    sb_trxsql_query_check_insert_correct check_insert;
    sb_trxsql_query_update_string_s3 update_string_s3;
    sb_trxsql_query_update_string_s4 update_string_s4;
    sb_trxsql_query_update_substring update_substring;
    sb_trxsql_query_check_string check_string;

    sb_trxsql_query_insert_table_1 insert_table_1;
    sb_trxsql_query_delete_table_1 delete_table_1;
    sb_trxsql_query_update_table_2 update_table_2;
    sb_trxsql_query_check_table check_table;
  } u; 
  sb_list_item_t listitem;  /* this struct can be stored in a linked list */
} sb_trxsql_query_t;

typedef struct
{
  sb_list_t *queries;  /* list of actual queries to perform */
} sb_trxsql_request_t;

int register_test_trx(sb_list_t *tests);

#endif
