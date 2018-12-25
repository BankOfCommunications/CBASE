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

#include <stdint.h>
#include <ctype.h>
#include <math.h>
#include "simon_trxtest.h"

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif


#include "sysbench.h"
#include "db_driver.h"
#include "sb_trx.h"


#define GET_RANDOM_ID(void) ((*rnd_func)(void))

/* How many rows to insert in a single query (used for test DB creation) */
#define INSERT_ROWS 10000

/* How many rows to insert before COMMITs (used for test DB creation) */
#define ROWS_BEFORE_COMMIT 1000

/* Maximum query length */
#define MAX_QUERY_LEN 1024

/* Large prime number to generate unique set of random numbers in delete test */
#define LARGE_PRIME 2147483647

/* Command line arguments definition */
static sb_arg_t oltp_args[] =
{
	{"oltp-reconnect-mode", "reconnect mode {session,transaction,query,random}", SB_ARG_TYPE_STRING,
		"session"},
	{"oltp-read-only", "generate only 'read' queries (do not modify database)", SB_ARG_TYPE_FLAG, "off"},
	{"oltp-skip-trx", "skip BEGIN/COMMIT statements", SB_ARG_TYPE_FLAG, "off"},
	{"oltp-range-size", "range size for range queries", SB_ARG_TYPE_INT, "100"},
	{"oltp-point-selects", "number of point selects", SB_ARG_TYPE_INT, "0"},
	{"oltp-simple-ranges", "number of simple ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-sum-ranges", "number of sum ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-order-ranges", "number of ordered ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-distinct-ranges", "number of distinct ranges", SB_ARG_TYPE_INT, "0"},
	/* junyue start */
	{"oltp-join-point-selects", "number of join point selects", SB_ARG_TYPE_INT, "0"}, 
	{"oltp-join-simple-ranges", "number of join simple ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-join-sum-ranges", "number of join sum ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-join-order-ranges", "number of join ordered ranges", SB_ARG_TYPE_INT, "0"},
	{"oltp-join-distinct-ranges", "number of join distinct ranges", SB_ARG_TYPE_INT, "0"},
	/* junyue end */
	{"oltp-index-updates", "number of index update", SB_ARG_TYPE_INT, "1"},
	{"oltp-replaces", "number of replace", SB_ARG_TYPE_INT, "0"},
	{"oltp-ninserts", "number of new rows", SB_ARG_TYPE_INT, "1"},
	{"oltp-deletes", "number of delete rows(the same number rows will be inserted)", SB_ARG_TYPE_INT, "1"},
	{"oltp-non-index-updates", "number of non-index updates", SB_ARG_TYPE_INT, "1"},
	{"oltp-auto-inc", "whether AUTO_INCREMENT (or equivalent) should be used on id column",
		SB_ARG_TYPE_FLAG, "on"},
	{"oltp-connect-delay", "time in microseconds to sleep after connection to database", SB_ARG_TYPE_INT,
		"10000"},
	{"oltp-user-delay-min", "minimum time in microseconds to sleep after each request",
		SB_ARG_TYPE_INT, "0"},
	{"oltp-user-delay-max", "maximum time in microseconds to sleep after each request",
		SB_ARG_TYPE_INT, "0"},
	{"oltp-table-name", "name of test table", SB_ARG_TYPE_STRING, "sbtest"},
	{"oltp-table-name-1", "name of test table", SB_ARG_TYPE_STRING, "sbtesta"},
	{"oltp-table-name-2", "name of test table", SB_ARG_TYPE_STRING, "sbtestb"},
	{"oltp-table-size", "number of records in test table", SB_ARG_TYPE_INT, "10000"},
    {"oltp-table-size-1", "number of records in test table1", SB_ARG_TYPE_INT, "200"},
    {"oltp-table-size-2", "number of records in test table2", SB_ARG_TYPE_INT, "200"},
	{"oltp-row-offset", "row offset in test table, used to repeat preparing", SB_ARG_TYPE_INT, "0"},

	{"oltp-dist-type", "random numbers distribution {uniform,gaussian,special}", SB_ARG_TYPE_STRING,
		"special"},
	{"oltp-dist-iter", "number of iterations used for numbers generation", SB_ARG_TYPE_INT, "12"},
	{"oltp-dist-pct", "percentage of values to be treated as 'special' (for special distribution)",
		SB_ARG_TYPE_INT, "1"},
	{"oltp-dist-res", "percentage of 'special' values to use (for special distribution)",
		SB_ARG_TYPE_INT, "75"},
	{"oltp-simon-host","simon ip", SB_ARG_TYPE_STRING, ""},
	{"oltp-simon-port","simon port", SB_ARG_TYPE_INT, "7466"},
	{"oltp-simon-cluster", "simon cluster", SB_ARG_TYPE_STRING, "ob_bench"},
	{NULL, NULL, SB_ARG_TYPE_NULL, NULL}
};

/* Random numbers distributions */
typedef enum
{
	DIST_TYPE_UNIFORM,
	DIST_TYPE_GAUSSIAN,
	DIST_TYPE_SPECIAL
} oltp_dist_t;

/*
   Some code in get_request_*() depends on the order in which the following
   constants are defined
 */
typedef enum {
	RECONNECT_SESSION,
	RECONNECT_QUERY,
	RECONNECT_TRANSACTION,
	RECONNECT_RANDOM,
	RECONNECT_LAST
} reconnect_mode_t;

typedef struct
{
	reconnect_mode_t reconnect_mode;
	uint32_t     read_only;
	uint32_t     skip_trx;
	uint32_t     auto_inc;
	uint32_t     range_size;
	uint32_t     point_selects;
	uint32_t     simple_ranges;
	uint32_t     sum_ranges;
	uint32_t     order_ranges;
	uint32_t     distinct_ranges;
	uint32_t     index_updates;
	uint32_t     replaces;
	uint32_t     non_index_updates;
	uint32_t     ninserts;
	uint32_t     deletes;
	uint32_t     connect_delay;
	uint32_t     user_delay_min;
	uint32_t     user_delay_max;
	char             *table_name;
	uint32_t     table_size;
	uint32_t     row_offset;
	oltp_dist_t       dist_type;
	uint32_t     dist_iter;
	uint32_t     dist_pct;
	uint32_t     dist_res;
	uint32_t     join_point_selects; // junyue
	uint32_t     join_simple_ranges; // junyue
	uint32_t     join_sum_ranges; // junyue
	uint32_t     join_order_ranges; // junyue
	uint32_t     join_distinct_ranges; // junyue
	char             *simon_host; //junyue
	uint32_t     simon_port; //junyue
	char             *simon_cluster; //junyue

    uint32_t    update_integer;
    char*       table_name_1;
    uint32_t    table_size_1;
    char*       table_name_2;
    uint32_t    table_size_2;
} oltp_args_t;

/* Test statements structure */
typedef struct
{
	db_stmt_t *lock;
	db_stmt_t *unlock;
    db_stmt_t *rollback;
	db_stmt_t *point;
	db_stmt_t *call;
	db_stmt_t *range;
	db_stmt_t *range_sum;
	db_stmt_t *range_order;
	db_stmt_t *range_distinct;
	db_stmt_t *select_for_update_int1;
	db_stmt_t *update_int1;
	db_stmt_t *update_int2;
	db_stmt_t *select_for_update_str1;
	db_stmt_t *update_str1;
	db_stmt_t *update_str2;
	db_stmt_t *select_for_delete_insert;
	db_stmt_t *delete_my;
	db_stmt_t *insert;
	db_stmt_t *ninsert;
	db_stmt_t *replace;
	db_stmt_t *join_point; // junyue
	db_stmt_t *join_range; // junyue
	db_stmt_t *join_range_sum; // junyue
	db_stmt_t *join_range_order; // junyue
	db_stmt_t *join_range_distinct; // junyue

    db_stmt_t *update_integer;
    db_stmt_t *update_init_integer;
    db_stmt_t *replace_string;
    db_stmt_t *update_string_s3;
    db_stmt_t *update_string_s4;
    db_stmt_t *update_substring;
    db_stmt_t *insert_correct;

    db_stmt_t *delete_table_1;
    db_stmt_t *insert_table_1;
    db_stmt_t *update_table_2;
} oltp_stmt_set_t;

/* Bind buffers for statements */
typedef struct
{
	sb_trxsql_query_point_t  point;
	sb_trxsql_query_range_t  range;
	sb_trxsql_query_range_t  range_sum;
	sb_trxsql_query_range_t  range_order;
	sb_trxsql_query_range_t  range_distinct;
	sb_trxsql_query_update_t update_int1;
	sb_trxsql_query_update_t update_int2;
	sb_trxsql_query_update_t update_str1;
	sb_trxsql_query_update_t update_str2;
	sb_trxsql_query_delete_t delete_my;
	sb_trxsql_query_insert_t insert;
	sb_trxsql_query_insert_t ninsert;
	sb_trxsql_query_replace_t replace;
	sb_trxsql_query_call_t   call;
	sb_trxsql_query_join_point_t   join_point; // junyue
	sb_trxsql_query_join_range_t   join_range; // junyue
	sb_trxsql_query_join_range_t   join_range_sum; // junyue
	sb_trxsql_query_join_range_t   join_range_order; // junyue
	sb_trxsql_query_join_range_t   join_range_distinct; // junyue

    sb_trxsql_query_update_integer update_integer;
    sb_trxsql_query_update_init_integer update_init_integer;
    sb_trxsql_query_check_integer check_integer;
    sb_trxsql_query_update_string_s3 update_string_s3;
    sb_trxsql_query_update_string_s4 update_string_s4;
    sb_trxsql_query_update_substring update_substring;
    sb_trxsql_query_check_string check_string;
    sb_trxsql_query_insert_correct insert_correct;

    sb_trxsql_query_delete_table_1 delete_table_1;
    sb_trxsql_query_insert_table_1 insert_table_1;
    sb_trxsql_query_update_table_2 update_table_2;

	/* Buffer for the 'c' table field in update_str1 and insert queries */
	char                  c[120];
	unsigned long         c_len;
	/* Buffer for the 'pad' table field in insert query */
	char                  pad[60];
	unsigned long         pad_len;
} oltp_bind_set_t;

/* OLTP test commands */
static int oltp_cmd_help(void);
static int oltp_cmd_prepare(void);
static int oltp_cmd_cleanup(void);

/* OLTP test operations */
static int oltp_init(void);
static void oltp_print_mode(void);
static sb_request_t oltp_get_request(int);
static int oltp_execute_request(sb_request_t *, int);
static void oltp_print_stats(void);
static db_conn_t *oltp_connect(void);
static int oltp_disconnect(db_conn_t *);
static int oltp_reconnect(int thread_id);
static int oltp_done(void);
static int oltp_watch(void);
int32_t hash_correct(int32_t ivalue, double fvalue, char* svalue);

static sb_test_t oltp_test =
{
	"trx",
	"trx data consistance test",
	{
		oltp_init,
		oltp_watch,
		NULL,
		oltp_print_mode,
		oltp_get_request,
		oltp_execute_request,
		oltp_print_stats,
		NULL,
		NULL,
		oltp_done
	},
	{
		oltp_cmd_help,
		oltp_cmd_prepare,
		NULL,
		oltp_cmd_cleanup
	},
	oltp_args,
	{NULL, NULL}
};

/* Global variables */
static simon::TrxTestPerHostBlurbC **blurbs = NULL; /* simon monitor */
static oltp_args_t args;                  /* test args */
static uint32_t (*rnd_func)(void);    /* pointer to random numbers generator */
static uint32_t req_performed;        /* number of requests done */
static db_conn_t **connections;           /* database connection pool */
static oltp_stmt_set_t *statements;       /* prepared statements pool */
static oltp_bind_set_t *bind_bufs;        /* bind buffers pool */
static reconnect_mode_t *reconnect_modes; /* per-thread reconnect modes */
static db_driver_t *driver;               /* current database driver */
static drv_caps_t driver_caps;            /* driver capabilities */

/* Statistic counters */
static int read_ops;
static int write_ops;
static int other_ops;
static int transactions;
static int deadlocks;

static sb_timer_t *exec_timers;
static sb_timer_t *fetch_timers;

/* Random seed used to generate unique random numbers */
static uint64_t rnd_seed;
/* Mutex to protect random seed */
static pthread_mutex_t    rnd_mutex;

/* Variable to pass is_null flag to drivers */

static char oltp_is_null = 1;

/* Watch thread */
pthread_t watcher;

/* watch data consistance */
static int  oltp_watch(void);

/* Parse command line arguments */
static int parse_arguments(void);

/* Random number generators */
static uint32_t rnd_func_uniform(void);
static uint32_t rnd_func_gaussian(void);
static uint32_t rnd_func_special(void);
static uint32_t get_unique_random_id(void);
static uint32_t get_real_unique_random_id(void);

/* SQL request generators */
static sb_request_t get_request_complex(int);

/* Adds a 'reconnect' request to the list of SQL queries */
static inline int add_reconnect_req(sb_list_t *list);

/* Get random 'user think' time */
static int get_think_time(void);

/* Generate SQL statement from query */
static db_stmt_t *get_sql_statement_trx(sb_trxsql_query_t *, int);

/* Prepare a set of statements for test */
static int prepare_stmt_set_trx(oltp_stmt_set_t *, oltp_bind_set_t *, db_conn_t *);

/* Close a set of statements */
void close_stmt_set(oltp_stmt_set_t *set);

void gen_random_string(char str[MAX_STRING_LEN])
{
    int length = abs(rand()) % (MAX_STRING_LEN - MAX_CHECK_LEN - 10) + MAX_CHECK_LEN;
    for(int i = 0; i < length; ++i)
        str[i]  = 'a' + abs(rand()) % 26;

    str[length]= '\0';
}

double gen_random_double()
{
    return sb_rnd() * 1.0 / 0x3fffffff * 0xfffffff;
}

void* run_watch_thread(void*)
{
	pthread_detach(pthread_self());
	log_text(LOG_NOTICE, "Start watch thread ...");

	db_conn_t *con;
	char      query[256];

	uint32_t range = GET_RANDOM_ID();
	if (range + args.range_size > args.table_size)
		range = args.table_size - args.range_size;
	if (range < 1)
		range = 1;     

	snprintf(query, sizeof(query), "select i1,i2,s1,s2 from %s where pk1 between %d and %d;", args.table_name, range, range + args.range_size - 1);

	db_error_t          rc;
	db_result_set_t *result = NULL;

	int sum = -1;
	char s1[MAX_COLUMN_SIZE] , s2[MAX_COLUMN_SIZE];

	con = oltp_connect();
	while(1) {
		while (con == NULL) {
			log_text(LOG_FATAL, "connect to server failed");
			con = oltp_connect();
		}
		result = db_query(con, query);
		if (result == NULL)
		{
			log_text(LOG_FATAL, "Execute query %s failed", query);
			oltp_disconnect(con);
			con = NULL;
		}
		else  {

			rc = (db_error_t)db_store_results(result);
			if (rc == SB_DB_ERROR_DEADLOCK)
			{
				db_free_results(result);
				log_text(LOG_FATAL, "Error fetching result: `%s`", query);
				continue;
			}
			else if (rc != SB_DB_ERROR_NONE)
			{
				log_text(LOG_FATAL, "Error fetching result: `%s`", query);
				continue;
			}
			int nrows = db_num_rows(result);

			if(nrows != args.range_size) {
				log_text(LOG_FATAL,"Error fetching rows return %d, expect %d", nrows, args.range_size);
			}
			db_row_t *row;
			while(row  = db_fetch_row(result))
			{
				if(!row->values[0] || !row->values[1] || !row->values[2] || !row->values[3]) {
					log_text(LOG_FATAL,"db_fetch_row return row with NULL columns!!!");
					db_free_results(result);
					oltp_disconnect(con);
					exit(0);
				}
				sum = atoi(row->values[0]);
				int i1 = atoi(row->values[0]);
				int i2 = atoi(row->values[1]);
				int c1 = i1 + i2;
				int c2 = strcmp(row->values[2], row->values[3]);
				if(c1 != 0 || c2 != 0) {
					log_text(LOG_FATAL, "row = %p; i1 = %d; i2 = %d; s1 = %s; s2 = %s\n", row, i1, i2, row->values[2], row->values[3]);
				}
				/*
				   else {
				   log_text(LOG_DEBUG, "row = %p; i1 = %d; i2 = %d; s1 = %s; s2 = %s\n", row, i1, i2, row->values[2], row->values[3]);
				   }
				 */
			}
			db_free_results(result);
		}

		usleep(10000);
	}
	if(con) oltp_disconnect(con);
	return NULL;
}

int oltp_watch(void)
{
	return pthread_create(&watcher,NULL,&run_watch_thread,NULL);
}


int register_test_trx(sb_list_t *tests)
{
	/* Register database API */
	if (db_register())
		return 1;

	/* Register OLTP test */
	SB_LIST_ADD_TAIL(&oltp_test.listitem, tests);

	return 0;
}


int oltp_cmd_help(void)
{
	db_print_help();

	return 0;
}

int prepare_tables(db_conn_t* conn, char* table_name, uint32_t size)
{
    char query[MAX_QUERY_LEN];
    sprintf(query,  "create table %s(\
             pk                 int primary key,\
             ivalue             int, \
             dvalue             double, \
             svalue             varchar(10), \
             dt                 DateTime, \
             hash_check         int\
             );",
            table_name);
    if(db_query(conn, query) == NULL)
    {
        log_text(LOG_FATAL, "create table %s fail.\n", table_name);
        return 1;
    }

    uint32_t rows = 5;
    uint32_t pkcnt = 0;
    for(uint32_t i = 0; i < size; i += rows)
    {
        sprintf(query, "insert into %s(pk, ivalue, dvalue, svalue, hash_check) values", table_name);
        uint32_t len = strlen(query);
 
        for(uint32_t j = 0; j < rows; ++j)
        {
            char insert_str[MAX_QUERY_LEN];
            sprintf(insert_str, "(%d, 0, 0, 'a', 0),", ++pkcnt);
            snprintf(query + len, sizeof(insert_str), insert_str);
            len += strlen(insert_str);
        }
        query[len - 1 ]= '\0';

        if(db_query(conn, query) == NULL)
        {
            log_text(LOG_FATAL, "insert data into table %s fail. ", table_name);
            return 1;
        }
    }

    return 0;
}

int oltp_cmd_prepare(void)
{
	db_conn_t      *con;
	char           *query = NULL;
	uint32_t   query_len;
	uint32_t   i;
	uint32_t   j;
	uint32_t   n;
	unsigned long  nrows;
	unsigned long  commit_cntr = 0;
	char           insert_str[MAX_QUERY_LEN];
	char           *pos;
	char           *table_options_str;
    
  	if (parse_arguments())
		return 1;

    int32_t pkcnt = args.row_offset;
	/* Get database capabilites */
	/* junyue start */
	if (db_describe(driver, &driver_caps, NULL))
	{
		log_text(LOG_FATAL, "failed to get database capabilities!");
		return 1;
	}
	/* junyue end */


	/* Create database connection */
	con = oltp_connect();
	if (con == NULL)
		return 1;

	/* Determine if database supports multiple row inserts */
	if (driver_caps.multi_rows_insert)
		nrows = INSERT_ROWS;
	else
		nrows = 1;

	/* Prepare statement buffer */
	if (args.auto_inc)
		snprintf(insert_str, sizeof(insert_str),
				"(0,' ','qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt')");
	else
		snprintf(insert_str, sizeof(insert_str),
				"(%d,0,' ','qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt')",
				args.table_size);

	query_len = MAX_QUERY_LEN + nrows * (strlen(insert_str) + 1);
	query = (char *)malloc(query_len);
	if (query == NULL)
	{
		log_text(LOG_FATAL, "memory allocation failure!");
		goto error;
	}

	/* Create test table */
	log_text(LOG_NOTICE, "Creating table '%s'...", args.table_name);
	table_options_str = driver_caps.table_options_str;
	/*
	   snprintf(query, query_len,
	   "CREATE TABLE %s ("
	   "id %s %s NOT NULL %s, "
	   "k integer %s DEFAULT '0' NOT NULL, "
	   "c char(120) DEFAULT '' NOT NULL, "
	   "pad char(60) DEFAULT '' NOT NULL, "
	   "PRIMARY KEY  (id) "
	   ") %s",
	   args.table_name,
	   (args.auto_inc && driver_caps.serial) ? "SERIAL" : "INTEGER",
	   driver_caps.unsigned_int ? "UNSIGNED" : "",
	   (args.auto_inc && driver_caps.auto_increment) ? "AUTO_INCREMENT" : "",
	   driver_caps.unsigned_int ? "UNSIGNED" : "",
	   (table_options_str != NULL) ? table_options_str : ""
	   );
	 */

	// sprintf(query, "create table %s(pk1 int, pk2 int, i1 int, i2 int, s1 varchar, s2 varchar, ct createtime, mt modifytime, result varchar, primary key(pk1,pk2))",args.table_name);

	sprintf(query, "create table %s(\
	   pk1 int primary key, \ 
	   pk2 int , \
	   i1  int, \
	   i2  int, \
	   s1  varchar(%d), \
	   s2  varchar(%d), \
	   ct  DateTime, \
	   mt  DateTime, \
       i3  int, \
       i4  int, \
       s3  varchar(%d),\
       s4  varchar(%d),\
	   f1  double, \
	   f2  double, \
	   check_int int, \
       check_str varchar(%d), \
       check_date DateTime, \
       check_float double, \
       check_hash int, \
       check_type int \
	)", args.table_name, MAX_STRING_LEN, MAX_STRING_LEN, MAX_STRING_LEN, MAX_STRING_LEN, MAX_STRING_LEN);

	if (db_query(con, query) == NULL)
	{
		log_text(LOG_FATAL, "failed to create test table");
		/* junyue start */
		if(!args.row_offset) 
		{
			goto error;
		}
		/* junyue end */
	}  

	/* Fill test table with data */
	log_text(LOG_NOTICE, "Creating %d records in table '%s'...", args.table_size,
			args.table_name);

   	for (i = args.row_offset; i < args.table_size; i += nrows)
	{
		/* Build query */
		n = snprintf(query, query_len, "REPLACE INTO %s(pk1,pk2,i1,i2,s1,s2,i3,i4,f1,f2,s3,s4,check_str, check_float, check_int, check_hash, check_type) VALUES ",
				args.table_name);
		if (n >= query_len)
		{
			log_text(LOG_FATAL, "query is too long!");
			goto error;
		}
		pos = query + n;
		for (j = 0; j < nrows; j++)
		{
			if ((unsigned)(pos - query) >= query_len)
			{
				log_text(LOG_FATAL, "query is too long!");
				goto error;
			}

			int pk1 = pkcnt++;
			int pk2 = pk1;
			int i1  = pk1;
			int i2  = i1-2*i1;
			snprintf(insert_str, sizeof(insert_str),
					"(%d,%d,%d,%d,'ABCD','ABCD',0,0,0,0,'aa','aa','aa',  0, 0, %d, 0)",pk1,pk2,i1,i2, hash_correct(0,0,"aa"));

			if (j == nrows - 1 || i+j == args.table_size -1)
				n = snprintf(pos, query_len - (pos - query), "%s", insert_str);
			else
				n = snprintf(pos, query_len - (pos - query), "%s,", insert_str);
			if (n >= query_len - (pos - query))
			{
				log_text(LOG_FATAL, "query is too long!");
				goto error;
			}
			if (i+j == args.table_size - 1)
				break;
			pos += n;
		}

		/* Execute query */
		if (db_query(con, query) == NULL)
		{
			log_text(LOG_FATAL, "failed to create test table!");
			goto error;
		}

		if (driver_caps.needs_commit)
		{
			commit_cntr += nrows;
			if (commit_cntr >= ROWS_BEFORE_COMMIT)
			{
				if (db_query(con, "COMMIT") == NULL)
				{
					log_text(LOG_FATAL, "failed to commit inserted rows!");
					goto error;
				}
				commit_cntr -= ROWS_BEFORE_COMMIT;
			}
		}
	}

	if (driver_caps.needs_commit && db_query(con, "COMMIT") == NULL)
	{
		if (db_query(con, "COMMIT") == NULL)
		{
			log_text(LOG_FATAL, "failed to commit inserted rows!");
			return 1;
		}
	}

    log_text(LOG_ALERT, "total %d record has inserted!\n", pkcnt);
    if(prepare_tables(con, args.table_name_1, args.table_size_1))
        return 1;
    if(prepare_tables(con, args.table_name_2, args.table_size_2))
        return 1;
    
	oltp_disconnect(con);
	return 0;

error:
	oltp_disconnect(con);
	if (query != NULL)
		free(query);

	return 1;
}

int oltp_cmd_cleanup(void)
{
	db_conn_t *con;
	char      query[256];

	if (parse_arguments())
		return 1;

	/* Get database capabilites */
	if (db_describe(driver, &driver_caps, NULL))
	{
		log_text(LOG_FATAL, "failed to get database capabilities!");
		//return 1; // junyue
	}

	/* Create database connection */
	con = oltp_connect();
	if (con == NULL)
		return 1;

	/* Drop the test table */
	log_text(LOG_NOTICE, "Dropping table '%s'...", args.table_name);
	snprintf(query, sizeof(query), "DROP TABLE %s", args.table_name);
	if (db_query(con, query) == NULL)
	{
		oltp_disconnect(con);
		return 1;
	}
    sprintf(query, "DROP TABLE %s", args.table_name_1);
    if(db_query(con, query) == NULL)
    {
        oltp_disconnect(con);
        return 1;
    }
    sprintf(query, "DROP TABLE %s", args.table_name_2);
    if(db_query(con, query) == NULL)
    {
        oltp_disconnect(con);
        return 1;
    }

	oltp_disconnect(con);
	log_text(LOG_INFO, "Done.");

	return 0;
}

void insert_data_thread();

int oltp_init(void)
{

	db_conn_t    *con;
	uint32_t thread_id;
	char         query[MAX_QUERY_LEN];
	int          simon_enable = 0;

	if (parse_arguments())
		return 1;

	if(args.simon_host)
	{
		simon::Simon_trxtestContextC &ctx= simon::Simon_trxtestContextC::getInstance();
		if(!ctx.startMonitoring(args.simon_cluster, args.simon_host, args.simon_port)) 
		{
			log_text(LOG_FATAL, "failed to start simon");
		}
		else
		{

			log_text(LOG_NOTICE, "start simon succeed");
			simon_enable = 1;

		}
	}
	else {
		log_text(LOG_NOTICE, "start trxtest without simon");
	}
	/* Get database capabilites */
	/* junyue start */
	if (db_describe(driver, &driver_caps, args.table_name))
	{
		log_text(LOG_FATAL, "failed to get database capabilities!");
		//return 1;
	}
	/* junyue end */

	/* Allocate database connection pool */
	connections = (db_conn_t **)malloc(sb_globals.num_threads * sizeof(db_conn_t *));
	if (connections == NULL)
	{
		log_text(LOG_FATAL, "failed to allocate DB connection pool!");
		return 1;
	}

	/* Create database connections */
	for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
	{
		connections[thread_id] = oltp_connect();
		if (connections[thread_id] == NULL)
		{
			log_text(LOG_FATAL, "thread#%d: failed to connect to database server, aborting...",
					thread_id);
			return 1;
		}
	}

	simon::TrxTestPerHostBlurbC* my_blurbs[sb_globals.num_threads]; 
	if(simon_enable)
	{
		char machine[512] = {0}; 
		size_t len;
		gethostname(machine, 512);
		blurbs = my_blurbs;
		log_text(LOG_NOTICE, "trxtest run on host:%s", machine);
		for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
		{
			blurbs[thread_id] = new simon::TrxTestPerHostBlurbC();
			blurbs[thread_id]->setHostname(machine);
		}
	}

	/* Allocate statements pool */
	statements = (oltp_stmt_set_t *)calloc(sb_globals.num_threads,
			sizeof(oltp_stmt_set_t));
	if (statements == NULL)
	{
		log_text(LOG_FATAL, "failed to allocate statements pool!");
		return 1;
	}

	/* Allocate bind buffers for each thread */
	bind_bufs = (oltp_bind_set_t *)calloc(sb_globals.num_threads,
			sizeof(oltp_bind_set_t));
	/* Prepare statements for each thread */
	for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
	{
		if (prepare_stmt_set_trx(statements + thread_id, bind_bufs + thread_id,
					connections[thread_id]))
		{
			log_text(LOG_FATAL, "thread#%d: failed to prepare statements for test",
					thread_id);
			return 1;
		}
	}

	/* Per-thread reconnect modes */
	if (!(reconnect_modes = (reconnect_mode_t *)calloc(sb_globals.num_threads,
					sizeof(reconnect_mode_t))))
		return 1;

	/* Initialize internal timers if we are in the debug mode */
	if (sb_globals.debug)
	{
		exec_timers = (sb_timer_t *)malloc(sb_globals.num_threads * sizeof(sb_timer_t));
		fetch_timers = (sb_timer_t *)malloc(sb_globals.num_threads * sizeof(sb_timer_t));
		for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
		{
			sb_timer_init(exec_timers + thread_id);
			sb_timer_init(fetch_timers + thread_id);
		}
	}

    insert_data_thread();
	return 0;
}

void* prepare_data_ps(void*);
void* prepare_table_1_2_data_ps(void*);

typedef struct
{
    char* name;
    uint32_t* size;
} table_info;

/** insert data thread   */ 
void insert_data_thread()
{
    pthread_t *pth = (pthread_t*)malloc(sizeof(pthread_t));
    pthread_create(pth, NULL, prepare_data_ps,NULL);

    pthread_t *pth_table_1 = (pthread_t*)malloc(sizeof(pthread_t));
    table_info* table1 = (table_info*)malloc(sizeof(table_info));
    table1->size = &args.table_size_1;
    table1->name = args.table_name_1;
    pthread_create(pth_table_1, NULL, prepare_table_1_2_data_ps, table1);

    pthread_t *pth_table_2 = (pthread_t*)malloc(sizeof(pthread_t));
    table_info* table2 = (table_info*)malloc(sizeof(table_info));
    table2->size = &args.table_size_2;
    table2->name = args.table_name_2;
    pthread_create(pth_table_2, NULL, prepare_table_1_2_data_ps, table2);
}

typedef struct
{
    union{
        int32_t i1;
        double f1;
        char s1[MAX_STRING_LEN];
        int32_t hash;
    } value;
    unsigned long strlen;
} param_bind_t;

#define MAX_FIELD 8 
#define MAX_SQL_LEN  (1<<21)

void* prepare_table_1_2_data_ps(void* arg)
{
    char* insert_sql = (char*)malloc(sizeof(char) * MAX_SQL_LEN);
    pthread_detach(pthread_self());
    table_info* tinfo = (table_info*)arg;
    int const field = 5;
    uint32_t num_of_row = 1000;
    db_conn_t* conn;
    param_bind_t* param_bind_list = (param_bind_t*) malloc(sizeof(param_bind_t) * num_of_row * field);
    if(param_bind_list == NULL)
    {
        log_text(LOG_FATAL, "allocate memory fail. ");
        pthread_exit(NULL);
    }
    db_bind_t* db_bind = (db_bind_t*) malloc(sizeof(db_bind_t) * num_of_row * field);
    if(db_bind == NULL)
    {
        log_text(LOG_FATAL, "allocate db_bind memory fail.");
        pthread_exit(NULL);
    }
    sprintf(insert_sql, "replace into %s(pk, ivalue, dvalue, svalue, hash_check) values", tinfo->name);
    int len = strlen(insert_sql);
    char* sql_slice = "(?,?,?,?,?),";
    int sql_slice_len = strlen(sql_slice);
    for(int i = 0; i < num_of_row; ++i)
    {
        sprintf(insert_sql + len, sql_slice);
        len += sql_slice_len;
    }
    insert_sql[len-1]='\0';

    for( int i = 0; i < num_of_row; ++i)
    {
        int id = i * field + 0;
        db_bind[id].type = DB_TYPE_INT;
        db_bind[id].is_null = 0;
        db_bind[id].data_len = 0;
        db_bind[id].buffer = &param_bind_list[id].value.i1;

        id = i * field + 1;
        db_bind[id].type = DB_TYPE_INT;
        db_bind[id].is_null = 0;
        db_bind[id].data_len = 0;
        db_bind[id].buffer = &param_bind_list[id].value.i1; 

        id = i * field + 2;
        db_bind[id].type = DB_TYPE_DOUBLE;
        db_bind[id].is_null = 0;
        db_bind[id].data_len = 0;
        db_bind[id].buffer = &param_bind_list[id].value.f1;

        id = i * field + 3;
        db_bind[id].type = DB_TYPE_VARCHAR;
        db_bind[id].is_null = 0;
        db_bind[id].data_len = &param_bind_list[id].strlen;
        db_bind[id].buffer = &param_bind_list[id].value.s1;

        id = i * field + 4;
        db_bind[id].type = DB_TYPE_INT;
        db_bind[id].is_null = 0;
        db_bind[id].buffer = &param_bind_list[id].value.hash;
    }

connect_retry:
    /** connect and prepare statement and bind */
    conn = oltp_connect();
    if(conn == NULL)
    {
        log_text(LOG_FATAL, "get connection fail!");
        pthread_exit(NULL);
    } 

    db_stmt_t* stmt = db_prepare(conn, insert_sql);
    if(stmt == NULL)
    {
        log_text(LOG_FATAL, "get stmt fail!");
        pthread_exit(NULL); 
    }  
    if(db_bind_param(stmt, db_bind, num_of_row * field))
    {
        log_text(LOG_FATAL, "bind insert fail!\n");
        pthread_exit(NULL); 
    } 


    int num_of_insert = 0;
    /** generate data and insert */
    while(1)
    {
        uint32_t pk = *tinfo->size - 1;
        for(int i = 0; i < num_of_row;  ++i)
        {
            pk++;
            int id = i * field + 0;
            param_bind_list[id].value.i1 = pk;

            id++;
            param_bind_list[id].value.i1 = pk;

            id++;
            double dvalue = gen_random_double();
            param_bind_list[id].value.f1 = dvalue;

            id++;
            char str[MAX_STRING_LEN];
            gen_random_string(str);
            strcpy(param_bind_list[id].value.s1, str);
            param_bind_list[id].strlen = strlen(str);

            id++;
            param_bind_list[id].value.hash = hash_correct(pk, dvalue, str);
        }

        db_result_set* rs = db_execute(stmt);
        if(rs == NULL)
        {
            oltp_disconnect(conn); 
            goto connect_retry;
        }
        else
        {   
            num_of_insert++;
            *tinfo->size += num_of_row;
            db_free_results(rs);
        }

        if(num_of_insert % 1000 == 0)
        {
            log_text(LOG_ALERT, "table [%s] has inserted %d rows. \n", tinfo->name, *tinfo->size);
        }
    }

    oltp_disconnect(conn);
    free(param_bind_list);
    free(tinfo);
    free(db_bind);
    free(insert_sql);
    return NULL;
}

void* prepare_data_ps(void*)
{
    char* insert_sql = (char*)malloc(sizeof(char) * MAX_SQL_LEN);
  
    uint32_t num_of_row = 1000;
    db_conn_t* conn;
    param_bind_t* param_bind_list =  (param_bind_t*) malloc(sizeof(param_bind_t) * num_of_row * MAX_FIELD);
    if(param_bind_list == NULL)
    {
        log_text(LOG_FATAL, "allocate memory fail in prepare_data_ps!\n");
        pthread_exit(NULL);
    }

    db_bind_t* db_bind = (db_bind_t*) malloc(sizeof(db_bind_t) * num_of_row * MAX_FIELD);
    if(db_bind == NULL)
    {
        log_text(LOG_FATAL, "allocate db_bind_t memory fail\n");
        pthread_exit(NULL);
    }

    /** build sql */
    sprintf(insert_sql, "replace into %s(pk1, pk2, i1, i2, i3, i4, f1, f2, s1, s2, s3, s4, check_type) values", args.table_name);

    int len = strlen(insert_sql);
    char* sql_slice = "(?,?,0,0,0,0,?,?,?,?,?,?,0),";
    int sql_slice_len = strlen(sql_slice);
    for(int i = 0; i < num_of_row; ++i)
    {
        sprintf(insert_sql + len, sql_slice);
        len += sql_slice_len;
    }
    insert_sql[len - 1]= '\0';

    /** bind param */
    for(int i = 0; i < num_of_row; ++i)
    {
        for(int j = 0; j < 2; ++j)
        {
            int id = i * MAX_FIELD + j;
            db_bind[id].type = DB_TYPE_INT;
            db_bind[id].is_null = 0;
            db_bind[id].data_len = 0;
            db_bind[id].buffer = &param_bind_list[id].value.i1;
        }
        for(int j = 2; j < 4; ++j)
        {
            int id = i * MAX_FIELD + j;
            db_bind[id].type = DB_TYPE_DOUBLE;
            db_bind[id].is_null = 0;
            db_bind[id].data_len = 0;
            db_bind[id].buffer = &param_bind_list[id].value.f1;
        }
        for(int j = 4; j < 8; ++j)
        {
            int id = i * MAX_FIELD + j;
            db_bind[id].type = DB_TYPE_VARCHAR;
            db_bind[id].is_null = 0;
            db_bind[id].data_len = &param_bind_list[id].strlen;
            db_bind[id].buffer = &param_bind_list[id].value.s1;
        }
    }
connect_retry:
    /** connect and prepare statement and bind */
    conn = oltp_connect();
    if(conn == NULL)
    {
        log_text(LOG_FATAL, "get connection fail!");
        pthread_exit(NULL);
    } 

    db_stmt_t* stmt = db_prepare(conn, insert_sql);
    if(stmt == NULL)
    {
        log_text(LOG_FATAL, "get stmt fail!");
        pthread_exit(NULL); 
    }  
    if(db_bind_param(stmt, db_bind, num_of_row * MAX_FIELD))
    {
        log_text(LOG_FATAL, "bind insert fail!\n");
        pthread_exit(NULL); 
    } 


    int num_of_insert = 0;
    /** generate data and insert */
    while(1)
    {
        num_of_insert++;
        int32_t pk = args.table_size - 1;
        for(int i = 0; i < num_of_row; ++i)
        {
            pk++;
            for(int j = 0; j < 2; ++j)
            {
                int id = i * MAX_FIELD + j;
                param_bind_list[id].value.i1 = pk;
            }
            for(int j = 2; j < 4; ++j)
            {
                int id = i * MAX_FIELD + j;
                param_bind_list[id].value.f1 = gen_random_double();
            }

            char str[MAX_STRING_LEN];
            gen_random_string(str);
            for(int j = 4; j < 8; ++j)
            {
                int id = i * MAX_FIELD + j;
                strcpy(param_bind_list[id].value.s1, str);
                param_bind_list[id].strlen = strlen(str);
            }
        }
        db_result_set* rs = db_execute(stmt);
        if(rs == NULL)
        {
            int rc = db_errno(conn);
            if(rc == 2006 || rc == 2013)
            {
                if(oltp_disconnect(conn))
                {
                    log_text(LOG_ALERT, "close connection fail!\n");
                }

                log_text(LOG_ALERT, "[%d] insert thread reconnection!\n", &conn);
                goto connect_retry;
            }
        }
        else
        {     
            args.table_size += num_of_row;
            db_free_results(rs);
        }
        //usleep(100);
        if(num_of_insert % 500 == 0)
            log_text(LOG_ALERT, "num of insert rows %d\n", args.table_size);
    }

    free(param_bind_list);
    free(db_bind);
    free(insert_sql);
    return NULL;
}

int oltp_done(void)
{
	uint32_t thread_id;

	/* Close statements and database connections */
	for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
	{
		close_stmt_set(statements + thread_id);
		oltp_disconnect(connections[thread_id]);
	}

	/* Deallocate connection pool */
	free(connections);

	for (thread_id = 0; thread_id < sb_globals.num_threads && blurbs; thread_id++)
	{
		delete blurbs[thread_id];
	}

	free(bind_bufs);
	free(statements);

	/* kill watcher */
	log_text(LOG_NOTICE, "Terminate watch thread");
	pthread_cancel(watcher);

	return 0;
}


void oltp_print_mode(void)
{
	log_text(LOG_NOTICE, "Doing TRX test.");

	log_text(LOG_NOTICE, "Running mixed OLTP test");

	if (args.read_only)
		log_text(LOG_NOTICE, "Doing read-only test");

	switch (args.dist_type) {
		case DIST_TYPE_UNIFORM:
			log_text(LOG_NOTICE, "Using Uniform distribution");
			break;
		case DIST_TYPE_GAUSSIAN:
			log_text(LOG_NOTICE, "Using Normal distribution (%d iterations)",
					args.dist_iter);
			break;
		case DIST_TYPE_SPECIAL:
			log_text(LOG_NOTICE, "Using Special distribution (%d iterations,  "
					"%d pct of values are returned in %d pct cases)",
					args.dist_iter, args.dist_pct, args.dist_res);
			break;
		default:
			log_text(LOG_WARNING, "Unknown distribution!");
			break;
	}

	if (args.skip_trx)
		log_text(LOG_NOTICE, "Skipping BEGIN/COMMIT");
	else
		log_text(LOG_NOTICE, "Using \"%s%s\" for starting transactions",
				driver_caps.transactions ? "BEGIN" : "LOCK TABLES",
				(driver_caps.transactions) ? "" :
				((args.read_only) ? " READ" : " WRITE"));

	if (args.auto_inc)
		log_text(LOG_NOTICE, "Using auto_inc on the id column");
	else
		log_text(LOG_NOTICE, "Not using auto_inc on the id column");

	if (sb_globals.max_requests > 0)
		log_text(LOG_NOTICE,
				"Maximum number of requests for OLTP test is limited to %d",
				sb_globals.max_requests);
	if (sb_globals.validate)
		log_text(LOG_NOTICE, "Validation mode enabled");
}


sb_request_t oltp_get_request(int tid)
{
	sb_request_t sb_req;

	if (sb_globals.max_requests > 0 && req_performed >= sb_globals.max_requests)
	{
		sb_req.type = SB_REQ_TYPE_NULL;
		return sb_req;
	}
	return get_request_complex(tid);
}


inline int add_reconnect_req(sb_list_t *list)
{
	sb_trxsql_query_t *query;

	query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	query->num_times = 1;
	query->type = SB_TRXSQL_QUERY_RECONNECT;
	query->nrows = 0;
	query->think_time = get_think_time();
	SB_LIST_ADD_TAIL(&query->listitem, list);
	return 0;
}

int32_t hash_correct(int32_t ivalue, double fvalue, char* svalue)
{
    char str[MAX_STRING_LEN * 3];
    int32_t hash = 1;
    sprintf(str, "%d%.6lf%s\n", ivalue, fvalue, svalue);
    char* s = str;
    while(*s)
    { 
        hash = hash* 31 + *s;
        s++;
    }
    return hash & 0x7fffffff;
}

int append_request_multi_table(sb_sql_request_t* sql_req, reconnect_mode_t rmode)
{
    sb_trxsql_query_t      *query;
    char str[MAX_STRING_LEN];

    int32_t id =  GET_RANDOM_ID();
    id %= args.table_size_1;
    id %= args.table_size_2;
    if(id == 0) id = 10;
    double fvalue;

    if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			return 0;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_DELETE_TABLE_1;
    query->u.delete_table_1.pk = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_INSERT_TABLE_1;
    query->u.insert_table_1.pk = id;
    query->u.insert_table_1.ivalue = id;
    query->u.insert_table_1.dvalue = (fvalue = gen_random_double());
    gen_random_string(str);
    strcpy(query->u.insert_table_1.svalue, str);
    query->u.insert_table_1.strlen = strlen(str);
    query->u.insert_table_1.hash_value = hash_correct(id, fvalue, str);
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_UPDATE_TABLE_2;
    query->u.update_table_2.pk = id;
    query->u.update_table_2.ivalue = id;
    query->u.update_table_2.dvalue = (fvalue = gen_random_double());
    gen_random_string(str);
    query->u.update_table_2.strlen = strlen(str);
    strcpy(query->u.update_table_2.svalue, str);
    query->u.update_table_2.hash_value = hash_correct(id, fvalue, str);
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    if (!args.skip_trx)
	{
		/* Generate commit */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
            return 0;
		query->type = SB_TRXSQL_QUERY_UNLOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_TRANSACTION)
			add_reconnect_req(sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_CHECK_MULTI_TABLE;
    query->u.check_table.id = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    return 1;
}

int append_request_insert_correct(sb_sql_request_t* sql_req, reconnect_mode_t rmode)
{
    sb_trxsql_query_t      *query;
    char str[MAX_STRING_LEN];
    int32_t id = GET_RANDOM_ID();
    int32_t ivalue;
    double fvalue;

    if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			return 0;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_DELETE;
    query->u.delete_query.id = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_INSERT_CORRECT;
    query->u.insert_correct.pk1 = id;
    query->u.insert_correct.pk2 = id;
    query->u.insert_correct.ivalue = (ivalue = rand());
    query->u.insert_correct.fvalue = (fvalue = gen_random_double());
    gen_random_string(str);
    strcpy(query->u.insert_correct.svalue, str);
    query->u.insert_correct.strlen = strlen(str);
    query->u.insert_correct.hash_value = hash_correct(ivalue,fvalue,str);
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    if (!args.skip_trx)
	{
		/* Generate commit */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
            return 0;
		query->type = SB_TRXSQL_QUERY_UNLOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_TRANSACTION)
			add_reconnect_req(sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_CHECK_INSERT;
    query->u.check_insert.id = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    return 1;
}

int append_request_string(sb_sql_request_t* sql_req, reconnect_mode_t rmode)
{
    sb_trxsql_query_t      *query;
    int32_t id = GET_RANDOM_ID();
    char str[MAX_STRING_LEN];

    if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			return 0;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;
    query->type = SB_TRXSQL_QUERY_UPDATE_STRING_S3;
    query->num_times = 1;
    query->think_time = get_think_time();
    query->u.update_string_s3.id = id;
    gen_random_string(str);
    strcpy(query->u.update_string_s3.str, str);
    query->u.update_string_s3.strlen = strlen(str);
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

	query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;
    query->type = SB_TRXSQL_QUERY_UPDATE_STRING_S4;
    query->num_times = 1;
    query->think_time = get_think_time();
    query->u.update_string_s4.id = id;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

	query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;
    query->type = SB_TRXSQL_QUERY_UPDATE_SUBSTRING;
    query->num_times = 1;
    query->think_time = get_think_time();
    query->u.update_substring.id = id;
    strcpy(query->u.update_substring.str,str);
    for(int i = 0; i < MAX_CHECK_LEN; ++i)
        query->u.update_substring.str[i] = toupper(query->u.update_substring.str[i]);
    query->u.update_substring.str[MAX_CHECK_LEN]='\0';
    query->u.update_substring.strlen = MAX_CHECK_LEN;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    if (!args.skip_trx)
	{
		/* Generate commit */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
            return 0;
		query->type = SB_TRXSQL_QUERY_UNLOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_TRANSACTION)
			add_reconnect_req(sql_req->queries);
	}

    if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			return 0;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;
    query->type = SB_TRXSQL_QUERY_UPDATE_SUBSTRING;
    query->num_times = 1;
    query->think_time = get_think_time();
    query->u.update_substring.id = id;
    strcpy(query->u.update_substring.str,str);
    query->u.update_substring.str[MAX_CHECK_LEN]='\0';
    query->u.update_substring.strlen = MAX_CHECK_LEN;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
    if(query == NULL)
        return 0;
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_ROLLBACK;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if(rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries);

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_CHECK_STRING;
    query->u.check_string.id = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);

    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    return 1;
}


/** test intenger */ 
int append_request_integer(sb_sql_request_t* sql_req, reconnect_mode_t rmode) 
{
    sb_trxsql_query_t      *query;
    int j, i;
    int id = GET_RANDOM_ID();

    int32_t init_value = sb_rnd() % 0xffffff;
    double init_fvalue = gen_random_double();
    /** the final result value */
    int32_t result_value = init_value;
    double  result_fvalue = init_fvalue;
    int32_t tmp;
    double ftmp;

    if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			return 0;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_UPDATE_INIT_INTEGER;
    query->u.update_init_integer.id = id;
    query->u.update_init_integer.value = init_value;
    query->u.update_init_integer.fvalue = init_fvalue; 
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries);

   	for(i = 0; i < args.update_integer + 5; i++)
    {
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
    	if (query == NULL)
 		    return 0;
        tmp = sb_rnd() % 0xffffff;
        ftmp = gen_random_double() ;

        query->num_times = 1;
        query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_UPDATE_INTEGER;
 		query->u.update_integer.id = id;

        query->u.update_integer.value = tmp;
        result_value += tmp;
        query->u.update_integer.result = result_value;

        query->u.update_integer.fvalue = ftmp;
        result_fvalue += ftmp;
        query->u.update_integer.fresult = result_fvalue;
		query->nrows = 1;
 		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    	if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

    if (!args.skip_trx)
	{
		/* Generate commit */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
            return 0;
		query->type = SB_TRXSQL_QUERY_UNLOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_TRANSACTION)
			add_reconnect_req(sql_req->queries);
	}

    /** check the result after commit */
    query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	if (query == NULL)
        return 0;	
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_TRXSQL_QUERY_CHECK_INTEGER;
    query->u.check_integer.id = id;
    query->nrows = 1;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    if (rmode == RECONNECT_QUERY)
        add_reconnect_req(sql_req->queries); 

    return 1;
}


sb_request_t get_request_complex(int tid)
{
	sb_request_t        sb_req;
	sb_sql_request_t    *sql_req = &sb_req.u.sql_request;
	sb_trxsql_query_t      *query;
	sb_list_item_t      *pos;
	sb_list_item_t      *tmp;
	uint32_t        i;
	uint32_t        range;
	reconnect_mode_t    rmode;
	int64_t update_ints[512] = {0};

	sb_req.type = SB_REQ_TYPE_SQL;

	sql_req->queries = (sb_list_t *)malloc(sizeof(sb_list_t));
	if (sql_req->queries == NULL)
	{
		log_text(LOG_FATAL, "cannot allocate SQL query!");
		sb_req.type = SB_REQ_TYPE_NULL;
		return sb_req;
	}
	SB_LIST_INIT(sql_req->queries);

	if (args.reconnect_mode == RECONNECT_RANDOM)
	{
		rmode = reconnect_modes[tid];
		reconnect_modes[tid] = (reconnect_mode_t)(sb_rnd() % RECONNECT_RANDOM);
		if (rmode == RECONNECT_SESSION &&
				reconnect_modes[tid] != RECONNECT_SESSION)
			add_reconnect_req(sql_req->queries);
		rmode = reconnect_modes[tid];
	}
	else
		rmode = args.reconnect_mode;

	if (!args.skip_trx)
	{
		/* Generate BEGIN statement */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->type = SB_TRXSQL_QUERY_LOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}
	/* junyue start */
	/* Generate set of join point selects */
	for(i = 0; i < args.join_point_selects; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_JOIN_POINT;
		query->u.join_point_query.id = GET_RANDOM_ID();
		query->nrows = 1;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue end */


	/* Generate set of point selects */
	for(i = 0; i < args.point_selects; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_POINT;
		query->u.point_query.id = GET_RANDOM_ID();
		query->nrows = 1;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue start */
	/* Generate join range queries */
	for(i = 0; i < args.join_simple_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_JOIN_RANGE;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;     
		query->u.join_range_query.from = range;
		query->u.join_range_query.to = range + args.range_size - 1;
		query->nrows = args.range_size;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue end */


	/* Generate range queries */
	for(i = 0; i < args.simple_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_RANGE;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;     
		query->u.range_query.from = range;
		query->u.range_query.to = range + args.range_size - 1;
		query->nrows = args.range_size;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue start */
	/* Generate sum range queries */
	for(i = 0; i < args.join_sum_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_JOIN_RANGE_SUM;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;
		query->u.join_range_query.from = range;
		query->u.join_range_query.to = range + args.range_size - 1;
		query->nrows = 1;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue end */


	/* Generate sum range queries */
	for(i = 0; i < args.sum_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_RANGE_SUM;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;
		query->u.range_query.from = range;
		query->u.range_query.to = range + args.range_size - 1;
		query->nrows = 1;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* junyue start */
	/* Generate join ordered range queries */
	for(i = 0; i < args.join_order_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_JOIN_RANGE_ORDER;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;
		query->u.join_range_query.from = range;
		query->u.join_range_query.to = range + args.range_size - 1;
		query->nrows = args.range_size;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue start */


	/* Generate ordered range queries */
	for(i = 0; i < args.order_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_RANGE_ORDER;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;
		query->u.range_query.from = range;
		query->u.range_query.to = range + args.range_size - 1;
		query->nrows = args.range_size;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* junyue start */
	/* Generate join distinct range queries */
	for(i = 0; i < args.join_distinct_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_JOIN_RANGE_DISTINCT;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;     
		query->u.join_range_query.from = range;
		query->u.join_range_query.to = range + args.range_size;
		query->nrows = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* junyue end */


	/* Generate distinct range queries */
	for(i = 0; i < args.distinct_ranges; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_RANGE_DISTINCT;
		range = GET_RANDOM_ID();
		if (range + args.range_size > args.table_size)
			range = args.table_size - args.range_size;
		if (range < 1)
			range = 1;     
		query->u.range_query.from = range;
		query->u.range_query.to = range + args.range_size;
		query->nrows = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* Skip all write queries for read-only test mode */
	if (args.read_only)
		goto readonly;

	/* Generate INT FOR UPDATE  */
	for (i = 0; i < args.index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_INT1;
		update_ints[i] = GET_RANDOM_ID();
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* Generate int1 update */
	for (i = 0; i < args.index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_UPDATE_INT1;
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* Generate int2 update */
	for (i = 0; i < args.index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_UPDATE_INT2;
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* Generate non-index update */
	for (i = 0; i < args.non_index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_STR1;
		update_ints[i] = GET_RANDOM_ID();
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* Generate non-index update */
	for (i = 0; i < args.non_index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_UPDATE_STR1;
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* Generate non-index update */
	for (i = 0; i < args.non_index_updates; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_UPDATE_STR2;
		query->u.update_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}
	/* Generate replace update */
	for (i = 0; i < args.replaces; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_REPLACE;
		query->u.replace_query.id = GET_RANDOM_ID();
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}

	/* FIXME: generate one more UPDATE with the same ID as DELETE/INSERT to make
	   PostgreSQL work */
/*	
	   query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
	   if (query == NULL)
	   goto memfail;
	   query->num_times = 1;
	   query->think_time = get_think_time();
	   query->type = SB_TRXSQL_QUERY_UPDATE_INT1;
	   range = GET_RANDOM_ID();
	   query->u.update_query.id = range;
	   SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	 */

	range = GET_RANDOM_ID();

	for (i = 0; i < args.deletes; i++)
	{
		// Generate select for delete 
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_SELECT_FOR_DELETE_INSERT;
		//  FIXME  range = GET_RANDOM_ID(); 
		update_ints[i] = GET_RANDOM_ID();
		query->u.delete_query.id = range;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

	for (i = 0; i < args.deletes; i++)
	{
		// Generate delete 
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_DELETE;
		// FIXME  range = GET_RANDOM_ID(); 
		query->u.delete_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}
	// Generate insert with same value 
	for (i = 0; i < args.deletes; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_INSERT;
		query->u.insert_query.id = update_ints[i];
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
	}

	 //Generate new insert 
	for (i = 0; i < args.ninserts; i++)
	{
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->num_times = 1;
		query->think_time = get_think_time();
		query->type = SB_TRXSQL_QUERY_NINSERT;
		uint32_t t = get_real_unique_random_id();
		query->u.insert_query.id = t + args.table_size;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_QUERY)
			add_reconnect_req(sql_req->queries);
	}     

readonly:

	if (!args.skip_trx)
	{
		/* Generate commit */
		query = (sb_trxsql_query_t *)malloc(sizeof(sb_trxsql_query_t));
		if (query == NULL)
			goto memfail;
		query->type = SB_TRXSQL_QUERY_UNLOCK;
		query->num_times = 1;
		query->think_time = 0;
		SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
		if (rmode == RECONNECT_TRANSACTION)
			add_reconnect_req(sql_req->queries);
	}
    
    if(!append_request_integer(sql_req, rmode))
        goto memfail;

    if(!append_request_string(sql_req, rmode))
        goto memfail;
    
    if(!append_request_insert_correct(sql_req, rmode))
        goto memfail;
    
    if(!append_request_multi_table(sql_req, rmode))
        goto memfail;
	/* return request */
	req_performed++;
	return sb_req;

	/* Handle memory allocation failures */
memfail:
	log_text(LOG_FATAL, "cannot allocate SQL query!");
	SB_LIST_FOR_EACH_SAFE(pos, tmp, sql_req->queries)
	{
		query = SB_LIST_ENTRY(pos, sb_trxsql_query_t, listitem);
		free(query);
	}
	free(sql_req->queries);
	sb_req.type = SB_REQ_TYPE_NULL;
	return sb_req;
}


int check_execute_multi_table(sb_trxsql_query_t* que, db_conn_t* conn)
{
 	db_result_set_t *rs = NULL;
    char      query[MAX_QUERY_LEN];
    
    snprintf(query, MAX_QUERY_LEN, "select %s.ivalue, %s.dvalue, %s.svalue, %s.hash_check, %s.ivalue, %s.dvalue, %s.svalue, %s.hash_check from %s, %s where %s.pk = %d and %s.pk = %d and %s.pk = %s.pk", args.table_name_1, args.table_name_1, args.table_name_1, args.table_name_1, args.table_name_2, args.table_name_2, args.table_name_2, args.table_name_2, args.table_name_1, args.table_name_2, args.table_name_1, que->u.check_table.id, args.table_name_2, que->u.check_table.id, args.table_name_1, args.table_name_2);

    rs = db_query(conn, query);
    if(rs == NULL)
    {
        log_text(LOG_FATAL,"the execute result is error, return NULL, expect one record!");
        return 0; 
    }

	db_error_t rc = (db_error_t)db_store_results(rs);
    if (rc == SB_DB_ERROR_DEADLOCK)
    {
        db_free_results(rs);
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    } else if (rc != SB_DB_ERROR_NONE)
    {
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    }
			
    int nrows = db_num_rows(rs);
    if(nrows != 1)
    {
        log_text(LOG_FATAL, "execute sql `%s` result has %d recored , expect 1 record.", query, nrows);
        return 0;
    }
    else
    {
        db_row_t* row = db_fetch_row(rs);
        for(int i = 0; i < 8; ++i)
            if(row->values[i] == NULL)
            {
                log_text(LOG_FATAL, "check multi table fail.  %d value is NULL", i);
                return 0;
            }
        int32_t ivalue = atoi(row->values[0]);
        double dvalue = atof(row->values[1]);

        int32_t hash = hash_correct(ivalue, dvalue, row->values[2]);
        int32_t db_hash = atoi(row->values[3]);

        if(hash != db_hash)
        {
            log_text(LOG_FATAL, "the first table check error! ivalues is: %s\n dvalue is: %s\n svalue is: %s\n db_hash is: %s\n pk is: %d\n hash is %d", row->values[0], row->values[1], row->values[2], row->values[3], que->u.check_table.id, hash);
            return 0;
        } 

        ivalue = atoi(row->values[4]);
        dvalue = atof(row->values[5]);
        hash = hash_correct(ivalue, dvalue, row->values[6]);
        db_hash = atoi(row->values[7]);
    
        if(hash != db_hash)
        {
            log_text(LOG_FATAL, "the second table check error! ivalues is: %s\n dvalue is: %s\n svalue is: %s\n db_hash is: %s\n pk is: %d\n hash is %d", row->values[4], row->values[5], row->values[6], row->values[7], que->u.check_table.id, hash);
            return 0;
        } 
    }
        
    db_free_results(rs); 
    return 1;
}


int check_execute_insert_correct(sb_trxsql_query_t* que, db_conn_t* conn)
{
 	db_result_set_t *rs = NULL;
    char      query[MAX_QUERY_LEN];
    
    snprintf(query, MAX_QUERY_LEN, "select i4, f2, s3, check_hash, check_type from %s where pk1 = %d", args.table_name, que->u.check_insert.id);

    rs = db_query(conn, query);
    if(rs == NULL)
    {
        log_text(LOG_FATAL,"the execute result is error, return NULL, expect one record!");
        return 0; 
    }

	db_error_t rc = (db_error_t)db_store_results(rs);
    if (rc == SB_DB_ERROR_DEADLOCK)
    {
        db_free_results(rs);
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    } else if (rc != SB_DB_ERROR_NONE)
    {
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    }
			
    int nrows = db_num_rows(rs);
    if(nrows != 1)
    {
        log_text(LOG_FATAL, "execute sql `%s` result has %d recored , expect 1 record.", query, nrows);
        return 0;
    }
    else
    {
        db_row_t* row = db_fetch_row(rs);
        int check_type = atoi(row->values[4]);
        if(check_type != 3)
            return 1;
        if(row->values[0] == NULL || row->values[1] == NULL ||
                row->values[2] == NULL || row->values[3] == NULL)
        {
            log_text(LOG_FATAL, "insert error, return NULL value!\n %s\n %s\n %s\n %s\n", row->values[0], row->values[1], row->values[2], row->values[3]);
            return 0;
        }

       int32_t ivalue = atoi(row->values[0]);
        double dvalue = atof(row->values[1]);

        int32_t hash = hash_correct(ivalue, dvalue, row->values[2]);
        int32_t db_hash = atoi(row->values[3]);

        if(hash != db_hash)
        {
            log_text(LOG_FATAL, "insert error! ivalues is: %s\n dvalue is: %s\n svalue is: %s\n db_hash is: %s\n pk is: %s\n hash is %d", row->values[0], row->values[1], row->values[2], row->values[3], row->values[4], hash);
            return 0;
        } 
        else
        {
//            log_text(LOG_ALERT, "-----> insert check ok!");
        }
    }
        
    db_free_results(rs); 
    return 1;
}

int check_execute_request_string(sb_trxsql_query_t* que, db_conn_t* conn)
{
 	db_result_set_t *rs = NULL;
    char      query[MAX_QUERY_LEN];
    
    snprintf(query, MAX_QUERY_LEN, "select s4, check_str, check_type from %s where  pk1 = %d", args.table_name, que->u.check_string.id);

    rs = db_query(conn, query);
    if(rs == NULL)
    {
        log_text(LOG_FATAL,"the execute result is error, return NULL!");
        return 0; 
    }

	db_error_t rc = (db_error_t)db_store_results(rs);
    if (rc == SB_DB_ERROR_DEADLOCK)
    {
        db_free_results(rs);
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    } else if (rc != SB_DB_ERROR_NONE)
    {
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    }
			
    int nrows = db_num_rows(rs);
    if(nrows != 1)
    {
        log_text(LOG_FATAL, "execute sql `%s` result has %d recored , expect 1 record.", query, nrows);
        return 0;
    }
    else
    {
        db_row_t* row = db_fetch_row(rs);
        char* s1 = row->values[0];
        char* fresult = row->values[1];
        int check_type = atoi(row->values[2]);

        if(check_type != 2)
            return 1;

        if(strcmp(s1, fresult))
        {
            log_text(LOG_FATAL, "execute sql '%s', the string test check fail.", query);
            return 0;
        }
        else
        {
//            log_text(LOG_ALERT, "-----> string check ok!");
        }
    }
        
    db_free_results(rs); 
    return 1;
}

/**  check integer test result */
int check_execute_request_integer(sb_trxsql_query_t* que, db_conn_t* conn)
{ 
 	db_result_set_t *rs = NULL;
    char      query[MAX_QUERY_LEN];
    
    snprintf(query, MAX_QUERY_LEN, "select f1, check_float, i3, check_int, check_type from %s where pk1 = %d", args.table_name, que->u.check_integer.id);

    rs = db_query(conn, query);
    if(rs == NULL)
    {
        log_text(LOG_FATAL,"the execute result is error, return NULL!");
        return 0; 
    }

	db_error_t rc = (db_error_t)db_store_results(rs);
    if (rc == SB_DB_ERROR_DEADLOCK)
    {
        db_free_results(rs);
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    } else if (rc != SB_DB_ERROR_NONE)
    {
        log_text(LOG_FATAL, "Error fetching result: `%s`", query);
        return 0;
    }
			
    int nrows = db_num_rows(rs);
    if(nrows != 1)
    {
        log_text(LOG_FATAL, "execute sql `%s` result empty, expect 1 record.", query);
        return 0;
    }
    else
    {
        db_row_t* row = db_fetch_row(rs);
        char* s1 = row->values[0];
        char* fresult = row->values[1];
        char* i3 = row->values[2];
        char* check_int = row->values[3];
        int check_type = atoi(row->values[4]);

        if(check_type != 1)
            return 1; 

        if(s1 == NULL || fresult == NULL || i3 == NULL || check_int == NULL)
            return 0;

        double fv1 = atof(s1), fv2 = atof(fresult);
        int iv1 = atoi(i3), iv2 = atoi(check_int);
        if(iv1 != iv2)
        {
            log_text(LOG_FATAL, "execute sql '%s', the integer test check fail.", query);
            return 0;
        }
        if(fabs(fv1 - fv2) > 1e-5)
        {
            log_text(LOG_FATAL, "execute sql '%s', the float test check fail.", query);
            return 0;
        }

      //  log_text(LOG_ALERT, "------> integer and float check ok!");
    }
        
    db_free_results(rs); 
    return 1;
}


/*
 * We measure read operations, write operations and transactions
 * performance. The time is counted for atomic operations as user might sleep
 * before some of them.
 */
int oltp_execute_request(sb_request_t *sb_req, int thread_id)
{
	db_stmt_t           *stmt;
	sb_sql_request_t    *sql_req = &sb_req->u.sql_request;
	db_error_t          rc;
	db_result_set_t     *rs;
	sb_list_item_t      *pos;
	sb_list_item_t      *tmp;
	sb_trxsql_query_t      *query;
    sb_trxsql_query_t *timeout_query = NULL;
	uint32_t        i;
	uint32_t        local_read_ops=0;
	uint32_t        local_write_ops=0;
	uint32_t        local_other_ops=0;
	uint32_t        local_deadlocks=0;

	uint32_t        local_update_int_ops = 0;  
	uint32_t        local_update_str_ops = 0;  
	uint32_t        local_select_for_update_ops = 0;  
	uint32_t        local_delete_ops = 0;  
	uint32_t        local_insert_ops = 0;  
	uint32_t        local_ninsert_ops = 0;  

	uint32_t        local_update_int_latency = 0;  
	uint32_t        local_update_str_latency = 0;  
	uint32_t        local_select_for_update_latency = 0;  
	uint32_t        local_delete_latency = 0;  
	uint32_t        local_insert_latency = 0;  
	uint32_t        local_ninsert_latency = 0;  


	int                 retry = 0;
	log_msg_t           msg;
	log_msg_oper_t      op_msg;
	uint64_t  nrows;

	/* Prepare log message */
	msg.type = LOG_MSG_TYPE_OPER;
	msg.data = &op_msg;

	/* measure the time for transaction */
	LOG_EVENT_START(msg, thread_id);

	do  /* deadlock handling */
	{
		if(retry) log_text(LOG_DEBUG, "Transaction(%p) start ... retry(%d)", sb_req, retry);
		retry = 0;
		SB_LIST_FOR_EACH(pos, sql_req->queries)
		{
			query = SB_LIST_ENTRY(pos, sb_trxsql_query_t, listitem);
			uint64_t local_request_latency  = 0;
TIMEOUT_RETRY:
			for(i = 0; i < query->num_times; i++)
			{
				/* emulate user thinking */
				if (query->think_time > 0)
					usleep(query->think_time); 

				if (query->type == SB_TRXSQL_QUERY_RECONNECT)
				{
					if (oltp_reconnect(thread_id))
						return 1;
					continue;
				}
                if(query->type == SB_TRXSQL_QUERY_CHECK_INTEGER)
                {
                    if(!check_execute_request_integer(query, connections[thread_id]))
                        return 1;
                    continue;
                }
                if(query->type == SB_TRXSQL_QUERY_CHECK_STRING)
                {
                    if(!check_execute_request_string(query, connections[thread_id]))
                        return 1;
                    continue;
                }
                if(query->type == SB_TRXSQL_QUERY_CHECK_INSERT)
                {
                    if(!check_execute_insert_correct(query, connections[thread_id]))
                        return 1;
                    continue;
                }
                if(query->type == SB_TRXSQL_QUERY_CHECK_MULTI_TABLE)
                {
                    if(!check_execute_multi_table(query, connections[thread_id]))
                        return 1;
                    continue;
                }

				/* find prepared statement */
				stmt = get_sql_statement_trx(query, thread_id);
				if (stmt == NULL)
				{
					log_text(LOG_FATAL, "unknown SQL query type: %d!", query->type);
					sb_globals.error = 1;
					return 1;
				}

				if (sb_globals.debug)
					sb_timer_start(exec_timers + thread_id);

				struct timeval st,ed;
				gettimeofday(&st, NULL);

				rs = db_execute(stmt);
				gettimeofday(&ed, NULL);

				local_request_latency += (ed.tv_sec * 1000000 - st.tv_sec * 1000000 + ed.tv_usec - st.tv_usec)/1000;

				log_text(LOG_DEBUG, "Execute query `%s` took %d ms", stmt->query, (ed.tv_sec * 1000000 - st.tv_sec * 1000000 + ed.tv_usec - st.tv_usec)/1000);

				if (sb_globals.debug)
					sb_timer_stop(exec_timers + thread_id);


				if (rs == NULL)
				{
					rc = db_errno(connections[thread_id]);
					if (rc != SB_DB_ERROR_DEADLOCK && rc != SB_DB_ERROR_FREEZE &&
                            rc != SB_DB_ERROR_TIMEOUT)
					{
						log_text(LOG_FATAL, "database error, exiting...");
						/* exiting, forget about allocated memory */
						sb_globals.error = 1;
						return 1; 
					}
                    else if( rc == SB_DB_ERROR_FREEZE)
                    {
                        retry = 1;
                        break; 
                    } 
                    /** 超时处理 */
                    else if( rc == SB_DB_ERROR_TIMEOUT)
                    {
                        /** 先暂停1秒，然后重新执行*/
                        sleep(1);
                        goto TIMEOUT_RETRY;
                    }
					else
					{ 
						if(stmt) log_text(LOG_FATAL, "ERROR Thread %d Lock Timeout", thread_id);
						local_deadlocks++;
						retry = 1;
						return 1;  
						/* exit for loop */
						break;  
					}
				}
                /** check result */
                

				/* junyue: SB_TRXSQL_QUERY_JOIN must meet*/
				if (query->type >= SB_TRXSQL_QUERY_POINT &&
						query->type <= SB_TRXSQL_QUERY_RANGE_DISTINCT) /* select query */
				{
					if (sb_globals.debug)
						sb_timer_start(fetch_timers + thread_id);

					rc = (db_error_t)db_store_results(rs);

					if (sb_globals.debug)
						sb_timer_stop(fetch_timers + thread_id);


					if (rc == SB_DB_ERROR_DEADLOCK)
					{
						db_free_results(rs);
						local_deadlocks++;
						retry = 1;
						log_text(LOG_FATAL, "Thread %d ERROR db_store_results deadlock", thread_id);
						return 1; //add by junyue
						break;  
					}
					else if (rc != SB_DB_ERROR_NONE)
					{
						log_text(LOG_FATAL, "Thread %d Error fetching result: `%s`", thread_id, stmt);
						/* exiting, forget about allocated memory */
						sb_globals.error = 1;
						return 1; 
					}

					/* Validate the result set if requested */
					if (sb_globals.validate && query->nrows > 0)
					{
						nrows = db_num_rows(rs);
						if (nrows != query->nrows)
							log_text(LOG_WARNING,
									"Number of received rows mismatch, expected: %ld, actual: %ld",
									(long )query->nrows, (long)nrows);
					}

				}
				db_free_results(rs);
			}

			/* count operation statistics */
			switch(query->type) {
				case SB_TRXSQL_QUERY_POINT:
				case SB_TRXSQL_QUERY_RANGE:
				case SB_TRXSQL_QUERY_RANGE_SUM:
				case SB_TRXSQL_QUERY_RANGE_ORDER:
				case SB_TRXSQL_QUERY_RANGE_DISTINCT:
				case SB_TRXSQL_QUERY_JOIN_POINT: // junyue
				case SB_TRXSQL_QUERY_JOIN_RANGE: // junyue
				case SB_TRXSQL_QUERY_JOIN_RANGE_SUM: // junyue
				case SB_TRXSQL_QUERY_JOIN_RANGE_ORDER: // junyue
				case SB_TRXSQL_QUERY_JOIN_RANGE_DISTINCT: // junyue
					local_read_ops += query->num_times;
					break;
				case SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_INT1:
				case SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_STR1:
				case SB_TRXSQL_QUERY_SELECT_FOR_DELETE_INSERT:
					local_select_for_update_ops += query->num_times;
					local_select_for_update_latency += local_request_latency;
					local_read_ops += query->num_times;
					break;
				case SB_TRXSQL_QUERY_UPDATE_INT1:
				case SB_TRXSQL_QUERY_UPDATE_INT2:
					local_update_int_ops += query->num_times;
					local_update_int_latency += local_request_latency;
					local_write_ops += query->num_times;
					break;
				case SB_TRXSQL_QUERY_UPDATE_STR1:
				case SB_TRXSQL_QUERY_UPDATE_STR2:
					local_update_str_ops += query->num_times;
					local_update_str_latency += local_request_latency;
					local_write_ops += query->num_times;
					break;
				case SB_TRXSQL_QUERY_DELETE:
					local_delete_ops += query->num_times;
					local_delete_latency += local_request_latency;
					local_write_ops += query->num_times;
					break;
				case SB_TRXSQL_QUERY_INSERT:
					local_insert_ops += query->num_times;
					local_write_ops += query->num_times;
					local_insert_latency += local_request_latency;
					break;
				case SB_TRXSQL_QUERY_NINSERT:
					local_ninsert_ops += query->num_times;
					local_write_ops += query->num_times;
					local_ninsert_latency += local_request_latency;
					break;
				case SB_TRXSQL_QUERY_REPLACE:
					local_write_ops += query->num_times;
					break;
				default: 
					local_other_ops += query->num_times;
			}   
			if (retry)
				break;  /* break transaction execution if deadlock */
		}
	} while(retry); /* retry transaction in case of deadlock */

	LOG_EVENT_STOP(msg, thread_id);

	SB_THREAD_MUTEX_LOCK();
	read_ops += local_read_ops;
	write_ops += local_write_ops;
	other_ops += local_other_ops;
	transactions++;
	deadlocks += local_deadlocks;
	SB_THREAD_MUTEX_UNLOCK();

	log_text(LOG_DEBUG, "Thread %d deadlocks: %d, read_ops: %d, write_ops: %d, other_ops: %d, time: %d ms",thread_id, local_deadlocks, local_read_ops, local_write_ops, local_other_ops, sb_globals.op_timers[thread_id].my_time/1000000);
	if(sb_globals.op_timers[thread_id].my_time/1000000 > 1000) {
		log_text(LOG_NOTICE,"Thread %d Dangerous!!! %d s",thread_id, sb_globals.op_timers[thread_id].my_time/1000000000);
	}
	if(blurbs == NULL && 1 == 0) 
	{
		uint64_t elapsed = sb_globals.op_timers[thread_id].my_time/1000000; // ms

		blurbs[thread_id]->incrRequestCount(local_read_ops + local_write_ops + local_other_ops);
		blurbs[thread_id]->setRequestLatency((local_read_ops + local_write_ops + local_other_ops)?elapsed/(local_read_ops + local_write_ops + local_other_ops):0);

		blurbs[thread_id]->incrTrxCount(1);
		blurbs[thread_id]->setTrxLatency(elapsed);

		blurbs[thread_id]->incrSelectForUpdateCount(local_select_for_update_ops);
		blurbs[thread_id]->setSelectForUpdateLatency(local_select_for_update_ops?local_select_for_update_latency/local_select_for_update_ops:0);

		blurbs[thread_id]->incrUpdateIntCount(local_update_int_ops);
		blurbs[thread_id]->setUpdateIntLatency(local_update_int_ops?local_update_int_latency/local_update_int_ops:0);

		blurbs[thread_id]->incrUpdateStrCount(local_update_str_ops);
		blurbs[thread_id]->setUpdateStrLatency(local_update_str_ops?local_update_str_latency/local_update_str_ops:0);

		blurbs[thread_id]->incrInsertCount(local_insert_ops);
		blurbs[thread_id]->setInsertLatency(local_insert_ops?local_insert_latency/local_insert_ops:0);

		blurbs[thread_id]->incrDeleteCount(local_delete_ops);
		blurbs[thread_id]->setDeleteLatency(local_delete_ops?local_delete_latency/local_delete_ops:0);

		blurbs[thread_id]->incrNewInsertCount(local_ninsert_ops);
		blurbs[thread_id]->setNewInsertLatency(local_ninsert_ops?local_ninsert_latency/local_ninsert_ops:0);

		blurbs[thread_id]->update();
	}
	/* Free list of queries */
	SB_LIST_FOR_EACH_SAFE(pos, tmp, sql_req->queries)
	{
		query = SB_LIST_ENTRY(pos, sb_trxsql_query_t, listitem);
		free(query);
	}
	free(sql_req->queries);

	return 0;
}


void oltp_print_stats(void)
{
	double       total_time;
	uint32_t i;
	sb_timer_t   exec_timer;
	sb_timer_t   fetch_timer;

	total_time = NS2SEC(sb_timer_value(&sb_globals.exec_timer));

	log_text(LOG_NOTICE, "OLTP test statistics:");
	log_text(LOG_NOTICE, "    queries performed:");
	log_text(LOG_NOTICE, "        read:                            %d",
			read_ops);
	log_text(LOG_NOTICE, "        write:                           %d",
			write_ops);
	log_text(LOG_NOTICE, "        other:                           %d",
			other_ops);
	log_text(LOG_NOTICE, "        total:                           %d",
			read_ops + write_ops + other_ops);
	log_text(LOG_NOTICE, "    transactions:                        %-6d"
			" (%.2f per sec.)", transactions, transactions / total_time);
	log_text(LOG_NOTICE, "    deadlocks:                           %-6d"
			" (%.2f per sec.)", deadlocks, deadlocks / total_time);
	log_text(LOG_NOTICE, "    read/write requests:                 %-6d"
			" (%.2f per sec.)", read_ops + write_ops,
			(read_ops + write_ops) / total_time);  
	log_text(LOG_NOTICE, "    other operations:                    %-6d"
			" (%.2f per sec.)", other_ops, other_ops / total_time);

	if (sb_globals.debug)
	{
		sb_timer_init(&exec_timer);
		sb_timer_init(&fetch_timer);

		for (i = 0; i < sb_globals.num_threads; i++)
		{
			exec_timer = merge_timers(&exec_timer, exec_timers + i);
			fetch_timer = merge_timers(&fetch_timer, fetch_timers + i);
		}

		log_text(LOG_DEBUG, "");
		log_text(LOG_DEBUG, "Query execution statistics:");
		log_text(LOG_DEBUG, "    min:                                %.4fs",
				NS2SEC(get_min_time(&exec_timer)));
		log_text(LOG_DEBUG, "    avg:                                %.4fs",
				NS2SEC(get_avg_time(&exec_timer)));
		log_text(LOG_DEBUG, "    max:                                %.4fs",
				NS2SEC(get_max_time(&exec_timer)));
		log_text(LOG_DEBUG, "  total:                                %.4fs",
				NS2SEC(get_sum_time(&exec_timer)));

		log_text(LOG_DEBUG, "Results fetching statistics:");
		log_text(LOG_DEBUG, "    min:                                %.4fs",
				NS2SEC(get_min_time(&fetch_timer)));
		log_text(LOG_DEBUG, "    avg:                                %.4fs",
				NS2SEC(get_avg_time(&fetch_timer)));
		log_text(LOG_DEBUG, "    max:                                %.4fs",
				NS2SEC(get_max_time(&fetch_timer)));
		log_text(LOG_DEBUG, "  total:                                %.4fs",
				NS2SEC(get_sum_time(&fetch_timer)));
	}
}


db_conn_t *oltp_connect(void)
{
	db_conn_t *con;

	con = db_connect(driver);
	if (con == NULL)
	{
		log_text(LOG_FATAL, "failed to connect to database server!");
		return NULL;
	}

	if (args.connect_delay > 0)
		usleep(args.connect_delay);

	return con;
}


int oltp_disconnect(db_conn_t *con)
{
	return db_disconnect(con);
}


int oltp_reconnect(int thread_id)
{
	close_stmt_set(statements + thread_id);
	if (oltp_disconnect(connections[thread_id]))
		return 1;
	if (!(connections[thread_id] = oltp_connect()))
		return 1;
	if (prepare_stmt_set_trx(statements + thread_id, bind_bufs + thread_id,
				connections[thread_id]))
	{
		log_text(LOG_FATAL, "thread#%d: failed to prepare statements for test",
				thread_id);
		return 1;
	}

	return 0;
}


/* Parse command line arguments */


int parse_arguments(void)
{
	char           *s;

	s = sb_get_value_string("oltp-reconnect-mode");
	if (!strcasecmp(s, "session"))
		args.reconnect_mode = RECONNECT_SESSION;
	else if (!strcasecmp(s, "query"))
		args.reconnect_mode = RECONNECT_QUERY;
	else if (!strcasecmp(s, "transaction"))
		args.reconnect_mode = RECONNECT_TRANSACTION;
	else if (!strcasecmp(s, "random"))
		args.reconnect_mode = RECONNECT_RANDOM;
	else
	{
		log_text(LOG_FATAL, "Invalid value for --trx-reconnect-mode: '%s'", s);
		return 1;
	}


	args.read_only = sb_get_value_flag("oltp-read-only");
	args.skip_trx = sb_get_value_flag("oltp-skip-trx");
	args.auto_inc = sb_get_value_flag("oltp-auto-inc");
	args.range_size = sb_get_value_int("oltp-range-size");
	args.point_selects = sb_get_value_int("oltp-point-selects");
	args.simple_ranges = sb_get_value_int("oltp-simple-ranges");
	args.sum_ranges = sb_get_value_int("oltp-sum-ranges");
	args.order_ranges = sb_get_value_int("oltp-order-ranges");
	args.distinct_ranges = sb_get_value_int("oltp-distinct-ranges");
	/* junyue start */
	args.join_point_selects = sb_get_value_int("oltp-join-point-selects"); 
	args.join_simple_ranges = sb_get_value_int("oltp-join-simple-ranges");
	args.join_sum_ranges = sb_get_value_int("oltp-join-sum-ranges");
	args.join_order_ranges = sb_get_value_int("oltp-join-order-ranges");
	args.join_distinct_ranges = sb_get_value_int("oltp-join-distinct-ranges");

	args.simon_host = sb_get_value_string("oltp-simon-host");
	args.simon_port = sb_get_value_int("oltp-simon-port");
	args.simon_cluster = sb_get_value_string("oltp-simon-cluster");
	/* junyue end */

    /** new add */
    args.update_integer = sb_get_value_int("oltp-update-integer");

	args.index_updates = sb_get_value_int("oltp-index-updates");
	args.non_index_updates = sb_get_value_int("oltp-non-index-updates");
	args.replaces = sb_get_value_int("oltp-replaces");
	args.ninserts = sb_get_value_int("oltp-ninserts");
	args.deletes = sb_get_value_int("oltp-deletes");

	args.connect_delay = sb_get_value_int("oltp-connect-delay");
	args.user_delay_min = sb_get_value_int("oltp-user-delay-min");
	args.user_delay_max = sb_get_value_int("oltp-user-delay-max");
	args.table_name = sb_get_value_string("oltp-table-name");
    args.table_name_1 = sb_get_value_string("oltp-table-name-1");
    args.table_name_2 = sb_get_value_string("oltp-table-name-2");
	args.table_size = sb_get_value_int("oltp-table-size");
    args.table_size_1 = sb_get_value_int("oltp-table-size-1");
    args.table_size_2 = sb_get_value_int("oltp-table-size-2");
	args.row_offset = sb_get_value_int("oltp-row-offset");


	s = sb_get_value_string("oltp-dist-type");
	if (!strcmp(s, "uniform"))
	{
		args.dist_type = DIST_TYPE_UNIFORM;
		rnd_func = &rnd_func_uniform;
	}
	else if (!strcmp(s, "gaussian"))
	{
		args.dist_type = DIST_TYPE_GAUSSIAN;
		rnd_func = &rnd_func_gaussian;
	}
	else if (!strcmp(s, "special"))
	{
		args.dist_type = DIST_TYPE_SPECIAL;
		rnd_func = &rnd_func_special;
	}
	else
	{
		log_text(LOG_FATAL, "Invalid random numbers distribution: %s.", s);
		return 1;
	}

	args.dist_iter = sb_get_value_int("oltp-dist-iter");
	args.dist_pct = sb_get_value_int("oltp-dist-pct");
	args.dist_res = sb_get_value_int("oltp-dist-res");

	/* Select driver according to command line arguments */
	driver = db_init(NULL);
	if (driver == NULL)
	{
		log_text(LOG_FATAL, "failed to initialize database driver!");
		return 1;
	}

	return 0;
}




/* Close a set of statements for the test */

void close_stmt_set(oltp_stmt_set_t *set)
{
	db_close(set->lock);
	db_close(set->unlock);
	db_close(set->point);
	db_close(set->call);
	db_close(set->range);
	db_close(set->range_sum);
	db_close(set->range_order);
	db_close(set->range_distinct);
	db_close(set->update_int1);
	db_close(set->update_str1);
	db_close(set->update_int2);
	db_close(set->update_str2);
	db_close(set->delete_my);
	db_close(set->insert);
    db_close(set->update_integer);
    db_close(set->update_init_integer);
	memset(set, 0, sizeof(oltp_stmt_set_t));
}


/* Prepare a set of statements for transactional test */
int prepare_stmt_set_trx(oltp_stmt_set_t *set, oltp_bind_set_t *bufs, db_conn_t *conn)
{
	db_bind_t binds[11];
	char      query[MAX_QUERY_LEN];

    snprintf(query, MAX_QUERY_LEN, "ROLLBACK");
    set->rollback = db_prepare(conn, query);
    if(set->rollback == NULL)
        return 1;

	/* Prepare the join point statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT j1.* from %s as j1,%s as j2 where j1.pk1=j2.pk2 and j1.pk1=?",args.table_name, args.table_name);
	set->join_point = db_prepare(conn, query);
	if (set->join_point == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->join_point.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	if (db_bind_param(set->join_point, binds, 1))
		return 1;


	/* Prepare the point statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT * from %s where pk1=?",
			args.table_name);
	set->point = db_prepare(conn, query);
	if (set->point == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->point.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	if (db_bind_param(set->point, binds, 1))
		return 1;

	/* Prepare the range statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT * from %s where pk1 between ? and ?",
			args.table_name);
	set->range = db_prepare(conn, query);
	if (set->range == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->range.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->range.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->range, binds, 2))
		return 1;

	/* Prepare the range_sum statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT SUM(i1),SUM(i2) from %s where pk1 between ? and ?", args.table_name);
	set->range_sum = db_prepare(conn, query);
	if (set->range_sum == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->range_sum.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->range_sum.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->range_sum, binds, 2))
		return 1;

	/* Prepare the range_order statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT * from %s where pk1 between ? and ? order by s1",
			args.table_name);
	set->range_order = db_prepare(conn, query);
	if (set->range_order == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->range_order.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->range_order.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->range_order, binds, 2))
		return 1;

	/* Prepare the range_distinct statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT DISTINCT s1 from %s where pk1 between ? and ? order by s1",
			args.table_name);
	set->range_distinct = db_prepare(conn, query);
	if (set->range_distinct == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->range_distinct.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->range_distinct.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->range_distinct, binds, 2))
		return 1;

	/* junyue start */

	/* Prepare the join range statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT j1.* from %s as j1,%s as j2 where j1.pk1=j2.pk1 and j1.pk1 between ? and ?",args.table_name, args.table_name);
	set->join_range = db_prepare(conn, query);
	if (set->join_range == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->join_range.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->join_range.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->join_range, binds, 2))
		return 1;

	/* Prepare the join range_sum statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT SUM(j1.i1) from %s as j1, %s as j2 where j1.pk1=j2.pk1 and j1.pk1 between ? and ?", args.table_name,args.table_name);
	set->join_range_sum = db_prepare(conn, query);
	if (set->join_range_sum == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->join_range_sum.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->join_range_sum.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->join_range_sum, binds, 2))
		return 1;

	/* Prepare the join range_order statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT j1.* from %s as j1, %s as j2 where j1.pk1=j2.pk1 and j1.pk1 between ? and ? order by j1.s1",
			args.table_name, args.table_name);
	set->join_range_order = db_prepare(conn, query);
	if (set->join_range_order == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->join_range_order.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->join_range_order.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->join_range_order, binds, 2))
		return 1;

	/* Prepare the join range_distinct statement */
	snprintf(query, MAX_QUERY_LEN,
			"SELECT DISTINCT j1.s1 from %s as j1, %s as j2 where j1.pk1 = j2.pk1 and j1.pk1 between ? and ? order by j1.s1",
			args.table_name, args.table_name);
	set->join_range_distinct = db_prepare(conn, query);
	if (set->join_range_distinct == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->join_range_distinct.from;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->join_range_distinct.to;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->join_range_distinct, binds, 2))
		return 1;
	/* junyue end */


	/* Prepare the prepare_for_update_int1 statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT * from %s where pk1=? and pk2=? FOR UPDATE",
			args.table_name);
	set->select_for_update_str1 = db_prepare(conn, query);
	if (set->select_for_update_str1 == NULL)
		return 1;

	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->update_str1.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_str1.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->select_for_update_str1, binds, 2))
		return 1;

	/* Prepare the prepare_for_update_s1 statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT * from %s where pk1=? and pk2=? FOR UPDATE",
			args.table_name);
	set->select_for_update_int1 = db_prepare(conn, query);
	if (set->select_for_update_int1 == NULL)
		return 1;

	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->update_int1.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_int1.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->select_for_update_int1, binds, 2))
		return 1;

	/* Prepare the prepare_for_update_s1 statement */
	snprintf(query, MAX_QUERY_LEN, "SELECT * from %s where pk1=? and pk2=? FOR UPDATE",
			args.table_name);
	set->select_for_delete_insert = db_prepare(conn, query);
	if (set->select_for_delete_insert == NULL)
		return 1;

	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->delete_my.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->delete_my.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->select_for_delete_insert, binds, 2))
		return 1;

	/* Prepare the update_int1 statement */
	snprintf(query, MAX_QUERY_LEN, "UPDATE %s set i1=i1+1 where pk1=? and pk2=?",
			args.table_name);
	set->update_int1 = db_prepare(conn, query);
	if (set->update_int1 == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->update_int1.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_int1.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->update_int1, binds, 2))
		return 1;

	/* Prepare the update_int1 statement */
	snprintf(query, MAX_QUERY_LEN, "UPDATE %s set i2=i2-1 where pk1=? and pk2=?",
			args.table_name);
	set->update_int2 = db_prepare(conn, query);
	if (set->update_int2 == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->update_int1.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_int1.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->update_int2, binds, 2))
		return 1;

	/* Prepare the update_str1 statement */
	snprintf(query, MAX_QUERY_LEN,
			"UPDATE %s set s1=? where pk1=? and pk2=?",
			args.table_name);
	set->update_str1 = db_prepare(conn, query);
	if (set->update_str1 == NULL)
		return 1;
	snprintf(bufs->c, 120, "%lu-%lu-%lu-%lu-%lu-%lu-%lu-%lu-%lu-%lu",
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd(),
			(unsigned long)sb_rnd());
	bufs->c_len = strlen(bufs->c);
	binds[0].type = DB_TYPE_CHAR;
	binds[0].buffer = bufs->c;
	binds[0].data_len = &bufs->c_len;
	binds[0].is_null = 0;
	binds[0].max_len = 120;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_str1.id;
	binds[1].data_len = 0;
	binds[1].is_null = 0;
	binds[2].type = DB_TYPE_INT;
	binds[2].buffer = &bufs->update_str1.id;
	binds[2].data_len = 0;
	binds[2].is_null = 0;
	if (db_bind_param(set->update_str1, binds, 3))
		return 1;



	/* Prepare the update_str1 statement */
	snprintf(query, MAX_QUERY_LEN,
			"UPDATE %s set s2=? where pk1=? and pk2=?",
			args.table_name);
	set->update_str2 = db_prepare(conn, query);
	if (set->update_str2 == NULL)
		return 1;

	binds[0].type = DB_TYPE_CHAR;
	binds[0].buffer = bufs->c;
	binds[0].data_len = &bufs->c_len;
	binds[0].is_null = 0;
	binds[0].max_len = 120;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->update_str1.id;
	binds[1].data_len = 0;
	binds[1].is_null = 0;
	binds[2].type = DB_TYPE_INT;
	binds[2].buffer = &bufs->update_str1.id;
	binds[2].data_len = 0;
	binds[2].is_null = 0;
	if (db_bind_param(set->update_str2, binds, 3))
		return 1;

	/*
	   Non-index update statement is re-bound each time because of the string
	   parameter
	 */

	/* Prepare the delete statement */
	snprintf(query, MAX_QUERY_LEN, "DELETE from %s where pk1=? and pk2=?",
			args.table_name);
	set->delete_my = db_prepare(conn, query);
	if (set->delete_my == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->delete_my.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->delete_my.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->delete_my, binds, 2))
		return 1;

	/* Prepare the replace statement */
	snprintf(query, MAX_QUERY_LEN, "REPLACE INTO %s(pk1,pk2,i1,i2,s1,s2,s3,s4,check_str,f1,f2,check_int,check_float,check_hash,check_type) values(?,?,0,0,'efgh','efgh', 'aa','aa','aa',0,0,0,0,%d,0)", args.table_name, hash_correct(0,0,"aa"));
	set->replace = db_prepare(conn, query);
	if (set->replace == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->replace.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->replace.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->replace, binds, 2))
		return 1;

	/* Prepare the insert statement */
	snprintf(query, MAX_QUERY_LEN, "INSERT INTO %s(pk1,pk2,i1,i2,s1,s2,s3,s4,check_str,f1,f2,check_int, check_float, check_hash,check_type) values(?,?,0,0,'abcd','abcd','aa', 'aa', 'aa', 0, 0, 0, 0, %d,0)", args.table_name, hash_correct(0,0,"aa"));
	set->insert = db_prepare(conn, query);
	if (set->insert == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->insert.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->insert.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->insert, binds, 2))
		return 1;

	/* Prepare the ninsert statement */
	snprintf(query, MAX_QUERY_LEN, "REPLACE INTO %s(pk1,pk2,i1,i2,s1,s2,s3,s4,check_str,f1,f2,check_int,check_float,check_hash,check_type) values(?,?,0,0,'nopqrst','nopqrst','aa','aa','aa',0,0,0,0,%d,0)", args.table_name, hash_correct(0,0,"aa"));
	set->ninsert = db_prepare(conn, query);
	if (set->ninsert == NULL)
		return 1;
	binds[0].type = DB_TYPE_INT;
	binds[0].buffer = &bufs->ninsert.id;
	binds[0].is_null = 0;
	binds[0].data_len = 0;
	binds[1].type = DB_TYPE_INT;
	binds[1].buffer = &bufs->ninsert.id;
	binds[1].is_null = 0;
	binds[1].data_len = 0;
	if (db_bind_param(set->ninsert, binds, 2))
		return 1;

    snprintf(query, MAX_QUERY_LEN, "update %s set i3 = ?, f1 = ? where pk1 = ?", args.table_name);
    set->update_init_integer = db_prepare(conn, query);
    if(set->update_init_integer == NULL)
        return 0;
    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->update_init_integer.value;
    binds[0].is_null = 0;
    binds[0].data_len = 0;
    
    binds[1].type = DB_TYPE_DOUBLE;
    binds[1].buffer = &bufs->update_init_integer.fvalue;
    binds[1].is_null = 0;
    binds[1].data_len = 0;
    
    binds[2].type = DB_TYPE_INT;
    binds[2].buffer = &bufs->update_init_integer.id;
    binds[2].is_null = 0;
    binds[2].data_len = 0;
    if(db_bind_param(set->update_init_integer, binds, 3))
        return 1;


    
    snprintf(query, MAX_QUERY_LEN, "update %s set i3 = i3 + ? , check_int = ?, f1 = f1 + ?, check_float = ?, check_type = 1  where pk1 = ?", args.table_name);    
    set->update_integer = db_prepare(conn, query);
    if(set->update_integer == NULL) 
        return 1;
    
    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->update_integer.value;
    binds[0].is_null = 0;
    binds[0].data_len = 0;

    binds[1].type = DB_TYPE_INT;
    binds[1].buffer = &bufs->update_integer.result;
    binds[1].is_null = 0;
    binds[1].data_len = 0;

    binds[2].type = DB_TYPE_DOUBLE;
    binds[2].buffer = &bufs->update_integer.fvalue;
    binds[2].is_null = 0;
    binds[2].data_len = 0;

    binds[3].type = DB_TYPE_DOUBLE;
    binds[3].buffer = &bufs->update_integer.fresult;
    binds[3].is_null = 0;
    binds[3].data_len = 0;
    
    binds[4].type = DB_TYPE_INT;
    binds[4].buffer = &bufs->update_integer.id;
    binds[4].is_null = 0;
    binds[4].data_len = 0;
    if(db_bind_param(set->update_integer, binds, 5))
        return 1;

   
    snprintf(query, MAX_QUERY_LEN, "update %s set s3=? where pk1 = ?", args.table_name);
    set->update_string_s3 = db_prepare(conn, query);
    if(set->update_string_s3 == NULL)
        return 1;

    binds[0].type = DB_TYPE_VARCHAR;
    binds[0].buffer = &bufs->update_string_s3.str;
    binds[0].is_null = 0;
    binds[0].data_len = &bufs->update_string_s3.strlen;

    binds[1].type = DB_TYPE_INT;
    binds[1].buffer = &bufs->update_string_s3.id;
    binds[1].is_null = 0;
    binds[1].data_len = 0;
    if(db_bind_param(set->update_string_s3, binds, 2))
        return 1;
    
    snprintf(query, MAX_QUERY_LEN, "update %s set s3 = upper(s3) where pk1 = ?", args.table_name);
    set->update_string_s4 = db_prepare(conn, query);
    if(set->update_string_s4 == NULL)
        return 1;

    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->update_string_s4.id;
    binds[0].is_null = 0;
    binds[0].data_len = 0;
    if(db_bind_param(set->update_string_s4, binds, 1))
        return 1; 

    snprintf(query, MAX_QUERY_LEN, "update %s set check_str = ?, s4 = substr(s3, 1, %d), check_type = 2 where pk1 = ?", args.table_name, MAX_CHECK_LEN);
    set->update_substring = db_prepare(conn, query);
    if(set->update_substring == NULL)
        return 1;

    binds[0].type = DB_TYPE_VARCHAR;
    binds[0].buffer = &bufs->update_substring.str;
    binds[0].is_null = 0;
    binds[0].data_len = &bufs->update_substring.strlen;
    binds[1].type = DB_TYPE_INT;
    binds[1].buffer = &bufs->update_substring.id;
    binds[1].is_null = 0;
    binds[1].data_len = 0;
    if(db_bind_param(set->update_substring, binds, 2))
        return 1;

    snprintf(query, MAX_QUERY_LEN, "insert into %s(pk1, pk2, i4, f2, s3, check_hash, i1, i2, s1, s2, f1, i3, s4, check_int, check_float, check_str, check_type) values(?,?,?,?,?,?,0,0,'aa','aa',0,0,'aa',0,0, 'aa', 3)", args.table_name);
    set->insert_correct = db_prepare(conn, query);
    if(set->insert_correct == NULL)
        return 1;

    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->insert_correct.pk1;
    binds[0].is_null = 0;
    binds[0].data_len = 0;

    binds[1].type = DB_TYPE_INT;
    binds[1].buffer = &bufs->insert_correct.pk2;
    binds[1].is_null = 0;
    binds[1].data_len = 0;

    binds[2].type = DB_TYPE_INT;
    binds[2].buffer = &bufs->insert_correct.ivalue;
    binds[2].is_null = 0;
    binds[2].data_len = 0;

    binds[3].type = DB_TYPE_DOUBLE;
    binds[3].buffer = &bufs->insert_correct.fvalue;
    binds[3].is_null = 0;
    binds[3].data_len = 0;
    
    binds[4].type = DB_TYPE_VARCHAR;
    binds[4].buffer = &bufs->insert_correct.svalue;
    binds[4].is_null = 0;
    binds[4].data_len = &bufs->insert_correct.strlen;

    binds[5].type = DB_TYPE_INT;
    binds[5].buffer = &bufs->insert_correct.hash_value;
    binds[5].is_null = 0;
    binds[5].data_len = 0;

    if(db_bind_param(set->insert_correct, binds, 6))
        return 1;

    snprintf(query, MAX_QUERY_LEN, "delete from %s where pk = ?", args.table_name_1);
    set->delete_table_1 = db_prepare(conn, query);
    if(set->delete_table_1 == NULL)
        return 1;
    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->delete_table_1.pk;
    binds[0].is_null = 0;
    binds[0].data_len = 0;
    if(db_bind_param(set->delete_table_1, binds, 1))
        return 1;

    snprintf(query, MAX_QUERY_LEN, "insert into %s (pk, ivalue, dvalue, svalue, hash_check) values( ?, ?, ?, ?, ?)", args.table_name_1);
    set->insert_table_1 = db_prepare(conn, query);
    if(set->insert_table_1 == NULL)
        return 1;
    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->insert_table_1.pk;
    binds[0].is_null = 0;
    binds[0].data_len = 0;

    binds[1].type = DB_TYPE_INT;
    binds[1].buffer = &bufs->insert_table_1.ivalue;
    binds[1].is_null = 0;
    binds[1].data_len = 0;

    binds[2].type = DB_TYPE_DOUBLE;
    binds[2].buffer = &bufs->insert_table_1.dvalue;
    binds[2].is_null = 0;
    binds[2].data_len = 0;

    binds[3].type = DB_TYPE_VARCHAR;
    binds[3].buffer = &bufs->insert_table_1.svalue;
    binds[3].is_null = 0;
    binds[3].data_len = &bufs->insert_table_1.strlen;

    binds[4].type = DB_TYPE_INT;
    binds[4].buffer = &bufs->insert_table_1.hash_value;
    binds[4].is_null = 0;
    binds[4].data_len = 0;
    if(db_bind_param(set->insert_table_1,binds, 5))
        return 1;

    snprintf(query, MAX_QUERY_LEN, "update %s set ivalue = ?, dvalue = ?, svalue = ?, hash_check = ? where pk = ?", args.table_name_2);
    set->update_table_2 = db_prepare(conn, query);
    if(set->update_table_2 == NULL)
        return 1;
    binds[0].type = DB_TYPE_INT;
    binds[0].buffer = &bufs->update_table_2.ivalue;
    binds[0].is_null = 0;
    binds[0].data_len = 0;

    binds[1].type = DB_TYPE_DOUBLE;
    binds[1].buffer = &bufs->update_table_2.dvalue;
    binds[1].is_null = 0;
    binds[1].data_len = 0;

    binds[2].type = DB_TYPE_VARCHAR;
    binds[2].buffer = &bufs->update_table_2.svalue;
    binds[2].is_null = 0;
    binds[2].data_len = &bufs->update_table_2.strlen;

    binds[3].type = DB_TYPE_INT;
    binds[3].buffer = &bufs->update_table_2.hash_value;
    binds[3].is_null = 0;
    binds[3].data_len = 0;

    binds[4].type = DB_TYPE_INT;
    binds[4].buffer = &bufs->update_table_2.pk;
    binds[4].is_null = 0;
    binds[4].data_len = 0;

    if(db_bind_param(set->update_table_2, binds, 5))
        return 1;

    if (args.skip_trx)
		return 0;

	/* Prepare the lock statement */
	if (driver_caps.transactions)
		strncpy(query, "BEGIN", MAX_QUERY_LEN);
	else
	{
		if (args.read_only)
			snprintf(query, MAX_QUERY_LEN, "LOCK TABLES %s READ", args.table_name);
		else
			snprintf(query, MAX_QUERY_LEN, "LOCK TABLES %s WRITE", args.table_name);
	}
	set->lock = db_prepare(conn, query);
	if (set->lock == NULL)
		return 1;

	/* Prepare the unlock statement */
	if (driver_caps.transactions)
		strncpy(query, "COMMIT", MAX_QUERY_LEN);
	else
		strncpy(query, "UNLOCK TABLES", MAX_QUERY_LEN);
	set->unlock = db_prepare(conn, query);
	if (set->unlock == NULL)
		return 1;

	return 0;
}


/* Generate SQL statement from query for transactional test */


db_stmt_t *get_sql_statement_trx(sb_trxsql_query_t *query, int thread_id)
{
	db_stmt_t       *stmt = NULL;
	db_bind_t       binds[2];
	oltp_bind_set_t *buf = bind_bufs + thread_id;

	switch (query->type) {
		case SB_TRXSQL_QUERY_LOCK:
			stmt = statements[thread_id].lock;
			break;

        case SB_TRXSQL_QUERY_ROLLBACK:
            stmt = statements[thread_id].rollback;
            break;

		case SB_TRXSQL_QUERY_UNLOCK:
			stmt = statements[thread_id].unlock;
			break;

		case SB_TRXSQL_QUERY_POINT:
			stmt = statements[thread_id].point;
			buf->point.id = query->u.point_query.id;
			break;
			/* junyue start */ 
		case SB_TRXSQL_QUERY_JOIN_POINT:
			stmt = statements[thread_id].join_point;
			buf->join_point.id = query->u.join_point_query.id;
			break;
		case SB_TRXSQL_QUERY_JOIN_RANGE:
			stmt = statements[thread_id].join_range;
			buf->join_range.from = query->u.join_range_query.from;
			buf->join_range.to = query->u.join_range_query.to;
			break;

		case SB_TRXSQL_QUERY_JOIN_RANGE_SUM:
			stmt = statements[thread_id].join_range_sum;
			buf->join_range_sum.from = query->u.join_range_query.from;
			buf->join_range_sum.to = query->u.join_range_query.to;
			break;

		case SB_TRXSQL_QUERY_JOIN_RANGE_ORDER:
			stmt = statements[thread_id].join_range_order;
			buf->join_range_order.from = query->u.join_range_query.from;
			buf->join_range_order.to = query->u.join_range_query.to;
			break;

		case SB_TRXSQL_QUERY_JOIN_RANGE_DISTINCT:
			stmt = statements[thread_id].join_range_distinct;
			buf->join_range_distinct.from = query->u.join_range_query.from;
			buf->join_range_distinct.to = query->u.join_range_query.to;
			break;


			/* junyue end */ 

		case SB_TRXSQL_QUERY_RANGE:
			stmt = statements[thread_id].range;
			buf->range.from = query->u.range_query.from;
			buf->range.to = query->u.range_query.to;
			break;

		case SB_TRXSQL_QUERY_RANGE_SUM:
			stmt = statements[thread_id].range_sum;
			buf->range_sum.from = query->u.range_query.from;
			buf->range_sum.to = query->u.range_query.to;
			break;

		case SB_TRXSQL_QUERY_RANGE_ORDER:
			stmt = statements[thread_id].range_order;
			buf->range_order.from = query->u.range_query.from;
			buf->range_order.to = query->u.range_query.to;
			break;

		case SB_TRXSQL_QUERY_RANGE_DISTINCT:
			stmt = statements[thread_id].range_distinct;
			buf->range_distinct.from = query->u.range_query.from;
			buf->range_distinct.to = query->u.range_query.to;
			break;

		case SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_INT1:
			stmt = statements[thread_id].select_for_update_int1;
			buf->update_int1.id = query->u.update_query.id;
			break;

		case SB_TRXSQL_QUERY_UPDATE_INT1:
			stmt = statements[thread_id].update_int1;
			buf->update_int1.id = query->u.update_query.id;
			break;

		case SB_TRXSQL_QUERY_SELECT_FOR_UPDATE_STR1:
			stmt = statements[thread_id].select_for_update_str1;
			buf->update_str1.id = query->u.update_query.id;
			break;

		case SB_TRXSQL_QUERY_UPDATE_STR1:
			stmt = statements[thread_id].update_str1;
			buf->update_str1.id = query->u.update_query.id;
			break;
		case SB_TRXSQL_QUERY_UPDATE_INT2:
			stmt = statements[thread_id].update_int2;
			buf->update_int1.id = query->u.update_query.id;
			break;

		case SB_TRXSQL_QUERY_UPDATE_STR2:
			stmt = statements[thread_id].update_str2;
			buf->update_str1.id = query->u.update_query.id;
			break;

		case SB_TRXSQL_QUERY_REPLACE:
			stmt = statements[thread_id].replace;
			buf->replace.id = query->u.replace_query.id;
			break;
		case SB_TRXSQL_QUERY_SELECT_FOR_DELETE_INSERT:
			stmt = statements[thread_id].select_for_delete_insert;
			buf->delete_my.id = query->u.delete_query.id;
			break;
		case SB_TRXSQL_QUERY_DELETE:
			stmt = statements[thread_id].delete_my;
			buf->delete_my.id = query->u.delete_query.id;
			break;

		case SB_TRXSQL_QUERY_INSERT:
			stmt = statements[thread_id].insert;
			buf->insert.id = query->u.insert_query.id;
			break;
		case SB_TRXSQL_QUERY_NINSERT:
			stmt = statements[thread_id].ninsert;
			buf->ninsert.id = query->u.insert_query.id;
			break;

        case SB_TRXSQL_QUERY_UPDATE_INTEGER:
            stmt = statements[thread_id].update_integer; 
            buf->update_integer.id = query->u.update_integer.id;
            buf->update_integer.value = query->u.update_integer.value;
            buf->update_integer.result = query->u.update_integer.result;
            buf->update_integer.fvalue = query->u.update_integer.fvalue;
            buf->update_integer.fresult = query->u.update_integer.fresult;
            break;
        case SB_TRXSQL_QUERY_UPDATE_INIT_INTEGER:
            stmt = statements[thread_id].update_init_integer;
            buf->update_init_integer.id = query->u.update_init_integer.id;
            buf->update_init_integer.value = query->u.update_init_integer.value;
            buf->update_init_integer.fvalue = query->u.update_init_integer.fvalue;
            break;

        case SB_TRXSQL_QUERY_UPDATE_STRING_S3:
            stmt = statements[thread_id].update_string_s3;
            buf->update_string_s3.id = query->u.update_string_s3.id;
            strcpy(buf->update_string_s3.str, query->u.update_string_s3.str);
            buf->update_string_s3.strlen = strlen(buf->update_string_s3.str);
            break;

        case SB_TRXSQL_QUERY_UPDATE_STRING_S4:
            stmt = statements[thread_id].update_string_s4;
            buf->update_string_s4.id = query->u.update_string_s4.id;
            strcpy(buf->update_string_s4.str, query->u.update_string_s4.str);
            break;

        case SB_TRXSQL_QUERY_UPDATE_SUBSTRING:
            stmt = statements[thread_id].update_substring;
            buf->update_substring.id = query->u.update_substring.id;
            strcpy(buf->update_substring.str, query->u.update_substring.str);
            buf->update_substring.strlen = strlen(buf->update_substring.str);
            break;
        case SB_TRXSQL_QUERY_INSERT_CORRECT:
            stmt = statements[thread_id].insert_correct;
            buf->insert_correct.pk1 = query->u.insert_correct.pk1;
            buf->insert_correct.pk2 = query->u.insert_correct.pk2;
            buf->insert_correct.ivalue = query->u.insert_correct.ivalue;
            buf->insert_correct.fvalue = query->u.insert_correct.fvalue;
            strcpy(buf->insert_correct.svalue, query->u.insert_correct.svalue);
            buf->insert_correct.strlen = strlen(buf->insert_correct.svalue);
            buf->insert_correct.hash_value = query->u.insert_correct.hash_value;
            break;
        case SB_TRXSQL_QUERY_DELETE_TABLE_1:
            stmt = statements[thread_id].delete_table_1;
            buf->delete_table_1.pk = query->u.delete_table_1.pk;
            break;
        case SB_TRXSQL_QUERY_INSERT_TABLE_1:
            stmt = statements[thread_id].insert_table_1;
            buf->insert_table_1.pk = query->u.insert_table_1.pk;
            buf->insert_table_1.ivalue = query->u.insert_table_1.ivalue;
            buf->insert_table_1.dvalue = query->u.insert_table_1.dvalue;
            strcpy(buf->insert_table_1.svalue, query->u.insert_table_1.svalue);
            buf->insert_table_1.strlen = query->u.insert_table_1.strlen;
            buf->insert_table_1.hash_value = query->u.insert_table_1.hash_value;
            break;
        case SB_TRXSQL_QUERY_UPDATE_TABLE_2:
            stmt = statements[thread_id].update_table_2;
            buf->update_table_2.pk = query->u.update_table_2.pk;
            buf->update_table_2.ivalue = query->u.update_table_2.ivalue;
            buf->update_table_2.dvalue = query->u.update_table_2.dvalue;
            strcpy(buf->update_table_2.svalue, query->u.update_table_2.svalue);
            buf->update_table_2.strlen = query->u.update_table_2.strlen;
            buf->update_table_2.hash_value = query->u.update_table_2.hash_value;
            break;
    	default:
			return NULL;
	}

	return stmt;
}


/* uniform distribution */


uint32_t rnd_func_uniform(void)
{
	return 1 + sb_rnd() % args.table_size;
}


/* gaussian distribution */


uint32_t rnd_func_gaussian(void)
{
	int          sum;
	uint32_t i;

	for(i=0, sum=0; i < args.dist_iter; i++)
		sum += (1 + sb_rnd() % args.table_size);

	return sum / args.dist_iter;
}


/* 'special' distribution */


uint32_t rnd_func_special(void)
{
	int          sum = 0;
	uint32_t i;
	uint32_t d;
	uint32_t res;
	uint32_t range_size;

	if (args.table_size == 0)
		return 0;

	/* Increase range size for special values. */
	range_size = args.table_size * (100 / (100 - args.dist_res));

	/* Generate evenly distributed one at this stage  */
	res = (1 + sb_rnd() % range_size);

	/* For first part use gaussian distribution */
	if (res <= args.table_size)
	{
		for(i = 0; i < args.dist_iter; i++)
		{
			sum += (1 + sb_rnd() % args.table_size);
		}
		return sum / args.dist_iter;  
	}

	/*
	 * For second part use even distribution mapped to few items 
	 * We shall distribute other values near by the center
	 */
	d = args.table_size * args.dist_pct / 100;
	if (d < 1)
		d = 1;
	res %= d;

	/* Now we have res values in SPECIAL_PCT range of the data */
	res += (args.table_size / 2 - args.table_size * args.dist_pct / (100 * 2));

	return res;
}

uint32_t get_real_unique_random_id(void)
{
	uint32_t res;
	pthread_mutex_lock(&rnd_mutex);
	res = (uint32_t) (rnd_seed % (LARGE_PRIME-1)) + 1;
	rnd_seed += LARGE_PRIME;
	pthread_mutex_unlock(&rnd_mutex);

	return res;
}

/* Generate unique random id */
uint32_t get_unique_random_id(void)
{
	uint32_t res;

	pthread_mutex_lock(&rnd_mutex);
	res = (uint32_t) (rnd_seed % args.table_size) + 1;
	rnd_seed += LARGE_PRIME;
	pthread_mutex_unlock(&rnd_mutex);

	return res;
}


int get_think_time(void)
{
	int t = args.user_delay_min;

	if (args.user_delay_min < args.user_delay_max)
		t += sb_rnd() % (args.user_delay_max - args.user_delay_min);

	return t; 
}

