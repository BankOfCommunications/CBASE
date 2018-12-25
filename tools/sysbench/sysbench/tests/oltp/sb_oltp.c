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
#include <stdlib.h>
#include "simon_sysbench.h"

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif


#include "sysbench.h"
#include "db_driver.h"
#include "sb_oltp.h"

#include "SimpleIni.h"

#define GET_RANDOM_ID() (hash((*rnd_func)()))

#define VAR_NAME(section, xx) schema.section.xx
#define VAR_N(section, xx,rows) schema.section.xx##_##rows

#define READ_TO_STRING(section, key, var) \
	sql = ini.GetValue(#section, key , NULL); \
if(!sql) {  \
	return 1; \
} \
else { \
	strcpy(VAR_NAME(section,var),sql) ; \
	log_text(LOG_DEBUG, "["#section"] %s  statement: %s",key ,VAR_NAME(section,var)); \
}


#define READ_TO_INT(section, key, var) \
	sql = ini.GetValue(#section, key , NULL); \
if(!sql) {  \
	return 1; \
} \
else { \
	VAR_NAME(section,var) = atoi(sql) ; \
	log_text(LOG_DEBUG, "["#section"] "key" statement: %d", VAR_NAME(section,var)); \
}

#define READ_TO_ARRAY(section, key, var) \
	sql = ini.GetValue(#section, key , NULL); \
if(!sql) {  \
	return 1; \
} \
else { \
	int i =0; \
	char *save; \
	char *p = (char*)sql; \
	p = strtok_r(p, ";", &save); \
	while(p){ \
		strcpy(VAR_NAME(section,var)[i],p) ; \
		log_text(LOG_DEBUG, "["#section"] "key" statement: %s", VAR_NAME(section,var)[i]); \
		i++; \
		p = strtok_r(NULL, ";", &save); \
	}\
}

#define READ_TO_SQL_ST(section, key, var, vars) \
	sql = ini.GetValue(#section, key , NULL); \
if(!sql) {  \
	return 1; \
} \
else { \
	char *save,*sve; \
	char *p = (char*)sql; \
	p = strtok_r(p, ";", &save); \
	strcpy(VAR_NAME(section,var),p);  \
	log_text(LOG_DEBUG, "["#section"] "key" statement: %s", p); \
	p = strtok_r(NULL, ";", &save); \
	char *q = p; \
	q = strtok_r(q, ",", &sve); \
        int rows  = atoi(q); \
        VAR_N(section,var,rows) = rows; \
	q = strtok_r(NULL, ",", &sve); \
	int count = atoi(q); \
	VAR_NAME(section,vars).count = count ; \
	log_text(LOG_DEBUG, "["#section"] "key" varaible count: %d", count); \
	q = strtok_r(NULL, ",", &sve); \
	int i = 0; \
	while(q) \
	{ \
		if(strcmp("int", q) == 0)  { \
			VAR_NAME(section,vars).sql_vars[i] = INT; \
			q = strtok_r(NULL, ",", &sve); \
                        char* sep = strchr(q,'=');\
                        if(sep) {\
                            *sep = 0;\
                            sep++;\
			    strcpy(VAR_NAME(section,vars).expr[i], sep); \
                        }\
                        else { \
			    memset(VAR_NAME(section,vars).expr[i],0,20);\
                        }\
			VAR_NAME(section,vars).var_bind[i] = atoi(q);\
			log_text(LOG_DEBUG, "["#section"] "key" varaible type(int:0, string:1, date:2): %d bind: %d expr: %s", INT, VAR_NAME(section,vars).var_bind[i], VAR_NAME(section,vars).expr[i]); \
		} \
		else if(strcmp("string", q) == 0)  { \
			VAR_NAME(section,vars).sql_vars[i] = STRING; \
			q = strtok_r(NULL, ",", &sve); \
			VAR_NAME(section,vars).var_bind[i] = atoi(q);\
			log_text(LOG_DEBUG, "["#section"] "key" varaible type(int:0, string:1, date:2): %d bind: %d", STRING, VAR_NAME(section,vars).var_bind[i]); \
		} \
		else if(strcmp("date", q) == 0)  { \
			VAR_NAME(section,vars).sql_vars[i] = DATE; \
			q = strtok_r(NULL, ",", &sve); \
			VAR_NAME(section,vars).var_bind[i] = atoi(q);\
			log_text(LOG_DEBUG, "["#section"] "key" varaible type(int:0, string:1, date:2): %d bind: %d", DATE, VAR_NAME(section,vars).var_bind[i]); \
		} \
		else { \
			VAR_NAME(section,vars).sql_vars[i] = ERROR; \
			q = strtok_r(NULL, ",", &sve); \
			VAR_NAME(section,vars).var_bind[i] = atoi(q);\
			log_text(LOG_DEBUG, "["#section"] "key" varaible type(int:0, string:1, date:2): %d", ERROR, VAR_NAME(section,vars).var_bind[i]); \
		} \
		i++; \
		q = strtok_r(NULL, ",", &sve); \
	}\
} 

/* How many rows to insert in a single query (used for test DB creation) */
#define INSERT_ROWS 100

/* How many rows to insert before COMMITs (used for test DB creation) */
#define ROWS_BEFORE_COMMIT 1000

/* Maximum query length */
#define MAX_QUERY_LEN 10240

/* Large prime number to generate unique set of random numbers in delete test */
#define LARGE_PRIME 2147483647

/* Command line arguments definition */
static sb_arg_t oltp_args[] =
{
	{"oltp-test-mode", "test type to use {simple,complex,nontrx,sp}", SB_ARG_TYPE_STRING, "complex"},
	{"oltp-reconnect-mode", "reconnect mode {session,transaction,query,random}", SB_ARG_TYPE_STRING,
		"session"},
	{"oltp-sp-name", "name of store procedure to call in SP test mode", SB_ARG_TYPE_STRING, ""},
	{"oltp-read-only", "generate only 'read' queries (do not modify database)", SB_ARG_TYPE_FLAG, "off"},
	{"oltp-skip-trx", "skip BEGIN/COMMIT statements", SB_ARG_TYPE_FLAG, "off"},
	{"oltp-range-size", "scale step", SB_ARG_TYPE_INT, "0"},
	{"oltp-point-selects", "number of point selects", SB_ARG_TYPE_INT, "1"},
	{"oltp-simple-ranges", "number of simple ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-sum-ranges", "number of sum ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-order-ranges", "number of ordered ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-distinct-ranges", "number of distinct ranges", SB_ARG_TYPE_INT, "1"},

	/* junyue start */
	{"oltp-join-point-selects", "number of join point selects", SB_ARG_TYPE_INT, "1"}, 
	{"oltp-join-simple-ranges", "number of join simple ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-join-sum-ranges", "number of join sum ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-join-order-ranges", "number of join ordered ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-join-distinct-ranges", "number of join distinct ranges", SB_ARG_TYPE_INT, "1"},
	{"oltp-schema-path", "schema path", SB_ARG_TYPE_STRING, "oltp.sql"},
	/* junyue end */

	{"oltp-index-updates", "number of index update", SB_ARG_TYPE_INT, "1"},
	{"oltp-non-index-updates", "number of non-index updates", SB_ARG_TYPE_INT, "1"},
	{"oltp-replaces", "number of replaces", SB_ARG_TYPE_INT, "1"},
	{"oltp-deletes", "number of deletes/inserts", SB_ARG_TYPE_INT, "1"},
	{"oltp-nontrx-mode",
		"mode for non-transactional test {select, update_key, update_nokey, insert, delete, replace}",
		SB_ARG_TYPE_STRING, "select"},
	{"oltp-auto-inc", "whether AUTO_INCREMENT (or equivalent) should be used on id column",
		SB_ARG_TYPE_FLAG, "on"},
	{"oltp-connect-delay", "time in microseconds to sleep after connection to database", SB_ARG_TYPE_INT,
		"10000"},
	{"oltp-user-delay-min", "minimum time in microseconds to sleep after each request",
		SB_ARG_TYPE_INT, "0"},
	{"oltp-user-delay-max", "maximum time in microseconds to sleep after each request",
		SB_ARG_TYPE_INT, "0"},
	{"oltp-table-name", "name of test table", SB_ARG_TYPE_STRING, "sbtest"},
	{"oltp-table-size", "number of records in test table", SB_ARG_TYPE_INT, "10000"},
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
	{"oltp-insert-row", "rows per insert", SB_ARG_TYPE_INT, "1000"},
	{"oltp-scale-thread", "scale thread number", SB_ARG_TYPE_INT, "13"},
	{NULL, NULL, SB_ARG_TYPE_NULL, NULL}
};



/* Test modes */
typedef enum
{
	TEST_MODE_SIMPLE,
	TEST_MODE_COMPLEX,
	TEST_MODE_NONTRX,
	TEST_MODE_SP,
	TEST_MODE_JOIN // junyue
} oltp_mode_t;

/* Modes for 'non-transactional' test */
typedef enum
{
	NONTRX_MODE_SELECT,
	NONTRX_MODE_UPDATE_KEY,
	NONTRX_MODE_UPDATE_NOKEY,
	NONTRX_MODE_INSERT,
	NONTRX_MODE_REPLACE,
	NONTRX_MODE_DELETE
} nontrx_mode_t;

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
	oltp_mode_t      test_mode;
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
	uint32_t     non_index_updates;
	uint32_t     replaces;
	uint32_t     deletes;
	nontrx_mode_t    nontrx_mode;
	uint32_t     connect_delay;
	uint32_t     user_delay_min;
	uint32_t     user_delay_max;
	char             *table_name;
	char             *sp_name;
	uint32_t     table_size;
	uint32_t     row_offset;
	oltp_dist_t      dist_type;
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
	char             *schema_path; //junyue
	uint32_t     rows_per_insert; 
} oltp_args_t;

/* Test statements structure */
typedef struct
{
	db_stmt_t *lock;
	db_stmt_t *unlock;
	db_stmt_t *point;
	db_stmt_t *call;
	db_stmt_t *range;
	db_stmt_t *range_sum;
	db_stmt_t *range_order;
	db_stmt_t *range_distinct;
	db_stmt_t *update_index;
	db_stmt_t *update_non_index;
	db_stmt_t *delete_row;
	db_stmt_t *insert;
	db_stmt_t *replace;
	db_stmt_t *join_point; // junyue
	db_stmt_t *join_range; // junyue
	db_stmt_t *join_range_sum; // junyue
	db_stmt_t *join_range_order; // junyue
	db_stmt_t *join_range_distinct; // junyue
} oltp_stmt_set_t;

/* Bind buffers for statements */
typedef struct
{
	sb_varlist_in_query_t  point;
	sb_varlist_in_query_t  range;
	sb_varlist_in_query_t  range_sum;
	sb_varlist_in_query_t  range_order;
	sb_varlist_in_query_t  range_distinct;
	sb_varlist_in_query_t update_index;
	sb_varlist_in_query_t update_non_index;
	sb_varlist_in_query_t delete_row;
	sb_varlist_in_query_t insert;
	sb_varlist_in_query_t replace; 
	sb_sql_query_call_t   call;
	sb_varlist_in_query_t   join_point; // junyue
	sb_varlist_in_query_t   join_range; // junyue
	sb_varlist_in_query_t   join_range_sum; // junyue
	sb_varlist_in_query_t   join_range_order; // junyue
	sb_varlist_in_query_t   join_range_distinct; // junyue
	/* Buffer for the 'c' table field in update_non_index and insert queries */
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
static bool check_data(db_result_set_t*, sb_sql_query_t* );
static void* prepare_rows(void*);
static void* prepare_rows_using_ps(void* arg);
static int oltp_scale(void);

static sb_test_t oltp_test =
{
	"oltp",
	"OLTP test",
	{
		oltp_init,
		oltp_scale,
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
static simon::SysbenchPerHostBlurbC **blurbs = NULL; /* simon monitor */
static simon::SysbenchPerHostBlurbC *prepare_blurbs[200]; /* simon monitor */
static oltp_args_t args;                  /* test args */
static uint32_t (*rnd_func)(void);    /* pointer to random numbers generator */
static uint32_t req_performed;        /* number of requests done */
static db_conn_t **connections;           /* database connection pool */
static oltp_stmt_set_t *statements;       /* prepared statements pool */
static oltp_bind_set_t *bind_bufs;        /* bind buffers pool */
static reconnect_mode_t *reconnect_modes; /* per-thread reconnect modes */
static db_driver_t *driver;               /* current database driver */
static drv_caps_t driver_caps;            /* driver capabilities */
static sb_schema_t schema;                /* schema */
static sb_column_rule_t col_rules[MAX_TABLE_COUNT][MAX_COLUMN_COUNT];   /* column rules */

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

/* Parse command line arguments */
static int parse_arguments(void);

/* Random number generators */
static uint32_t rnd_func_uniform(void);
static uint32_t rnd_func_gaussian(void);
static uint32_t rnd_func_special(void);
static uint32_t get_unique_random_id(void);
static uint32_t hash(uint32_t);

/* SQL request generators */
static sb_request_t get_request_simple(int);
static sb_request_t get_request_complex(int);
static sb_request_t get_request_nontrx(int);
static sb_request_t get_request_sp(int);
static sb_request_t get_request_join(int); //junyue

/* Adds a 'reconnect' request to the list of SQL queries */
static inline int add_reconnect_req(sb_list_t *list);

/* Get random 'user think' time */
static int get_think_time(void);

/* Generate SQL statement from query */
static db_stmt_t *get_sql_statement(sb_sql_query_t *, int);
static db_stmt_t *get_sql_statement_trx(sb_sql_query_t *, int);
static db_stmt_t *get_sql_statement_nontrx(sb_sql_query_t *, int);
static db_stmt_t *get_sql_statement_sp(sb_sql_query_t *, int);
static db_stmt_t *get_sql_statement_join(sb_sql_query_t *, int); // junyue

/* Prepare a set of statements for test */
static int prepare_stmt_set(oltp_stmt_set_t *, oltp_bind_set_t *, db_conn_t *);
static int prepare_stmt_set_trx(oltp_stmt_set_t *, oltp_bind_set_t *, db_conn_t *);
static int prepare_stmt_set_nontrx(oltp_stmt_set_t *, oltp_bind_set_t *, db_conn_t *);
static int prepare_stmt_set_sp(oltp_stmt_set_t *, oltp_bind_set_t *, db_conn_t *);

/* Close a set of statements */
void close_stmt_set(oltp_stmt_set_t *set);

int register_test_oltp(sb_list_t *tests)
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


int rule_init()
{
	for(int t =0 ; t < schema.prepare.table_num; t++) 
	{

		for(int i = 0; i < MAX_COLUMN_COUNT; i++) 
		{
			memset(&col_rules[t][i], 0, sizeof(col_rules[t][i]));
		}

		char* p = (char*) malloc(2 * MAX_QUERY_LEN);
		char* q = p;
		memset(p, 0, 2 * MAX_QUERY_LEN);
		strcpy(p,schema.prepare.rule[t]);
		char* save = NULL;
		p = strtok_r(p,";",&save);
		int idx = -1; 
		while(p) 
		{
			idx++;
			char* ls = NULL;
			char* c = (char*) malloc(1024*2);
			char* fc = c;
			memset(c,0,1024*2);
			strcpy(c,p);
			log_text(LOG_DEBUG, "parse column: %s", c);
			c = strtok_r(c, "(", &ls);

			strcpy(col_rules[t][idx].name, c);

			log_text(LOG_DEBUG, "column name: %s", c);
			c = strtok_r(NULL, "(", &ls);
			log_text(LOG_DEBUG, "column rule: %s", c);

			//*c = 0; 
			//c++;
			*(c+strlen(c)-1) = 0;
			log_text(LOG_DEBUG, "column rule after remove (): %s", c);

			c = strtok_r(c, ",", &ls);
			log_text(LOG_DEBUG, "start: %s", c);
			if(*c == '\''){
				strncpy(col_rules[t][idx].start.s_start, c+1, strlen(c)-2);
                                col_rules[t][idx].start.s_start[strlen(c)-2] = 0;
				col_rules[t][idx].type = STRING;
			}
			else {
				col_rules[t][idx].start.i_start = atoi(c);
				col_rules[t][idx].type = INT;
			}

			c = strtok_r(NULL, ",", &ls);
			log_text(LOG_DEBUG, "step: %s", c);
			col_rules[t][idx].step = atoi(c);

			free(fc);
			p = strtok_r(NULL,";", &save);
		}
		free(q);
	}
	return 0;
}


int schema_init()
{

	CSimpleIniA ini;
	//ini.SetUnicode();
	ini.LoadFile(args.schema_path);

	const char * sql = NULL;
	READ_TO_INT(prepare, "table_num", table_num);
	READ_TO_ARRAY(prepare, "table_names", table_names);
	for ( int t = 0 ; t < schema.prepare.table_num; t++) {
		READ_TO_STRING(prepare, schema.prepare.table_names[t], rule[t]);
	}

	READ_TO_SQL_ST(run, "point_query", point_sql, point_sql_vars);
	READ_TO_SQL_ST(run, "range_query", range_sql, range_sql_vars);
	READ_TO_SQL_ST(run, "range_sum_query", sum_sql, sum_sql_vars);
	READ_TO_SQL_ST(run, "range_orderby_query", orderby_sql, orderby_sql_vars);
	READ_TO_SQL_ST(run, "range_distinct_query", distinct_sql,distinct_sql_vars);
	READ_TO_SQL_ST(run, "point_join_query", point_join_sql, point_join_sql_vars);
	READ_TO_SQL_ST(run, "range_join_query", range_join_sql, range_join_sql_vars);
	READ_TO_SQL_ST(run, "range_join_sum_query", sum_join_sql, sum_join_sql_vars);
	READ_TO_SQL_ST(run, "range_join_orderby_query", orderby_join_sql, orderby_join_sql_vars);
	READ_TO_SQL_ST(run, "range_join_distinct_query", distinct_join_sql, distinct_join_sql_vars);
	READ_TO_SQL_ST(run, "delete", delete_sql, delete_sql_vars);
	READ_TO_SQL_ST(run, "insert", insert_sql, insert_sql_vars);
	READ_TO_SQL_ST(run, "replace", replace_sql, replace_sql_vars);
	READ_TO_SQL_ST(run, "update_key", update_key_sql, update_key_sql_vars);
	READ_TO_SQL_ST(run, "update_nonkey", update_nonkey_sql, update_nonkey_sql_vars);

	READ_TO_STRING(cleanup, "drop", drop_sql);

	return rule_init();
}


int oltp_cmd_prepare(void)
{
    int          simon_enable = 0;
    if (parse_arguments())
        return 1;
    if(schema_init())
        return 1;


    if(args.simon_host)
    {
        simon::Simon_sysbenchContextC &ctx= simon::Simon_sysbenchContextC::getInstance();
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
        log_text(LOG_NOTICE, "start sysbench without simon");
    }

    /* Get database capabilites */
    /* junyue start */
    if (db_describe(driver, &driver_caps, NULL))
    {
        log_text(LOG_FATAL, "failed to get database capabilities!");
        return 1;
    }
    /* junyue end */
    /*
    simon::SysbenchPerHostBlurbC* my_blurbs[schema.prepare.table_num]; 
    if(simon_enable)
    {
        char machine[512] = {0}; 
        size_t len;
        gethostname(machine, 512);
        prepare_blurbs = my_blurbs;
        log_text(LOG_NOTICE, "sysbench run on host:%s", machine);
        for (int t = 0; t < schema.prepare.table_num; t++)
        {
            prepare_blurbs[t] = new simon::SysbenchPerHostBlurbC();
            prepare_blurbs[t]->setHostname(machine);
        }
    }
   */
    pthread_t *pth  = (pthread_t*) malloc(sizeof(pthread_t) * schema.prepare.table_num);
    int *tid = (int*)malloc(sizeof(int) * schema.prepare.table_num);       
    for(int t = 0; t < schema.prepare.table_num; t++)
    {
        *(tid+t) = t;
        log_text(LOG_NOTICE, "Creating (%d - %d) records in table '%s'...", args.row_offset, args.table_size,
                schema.prepare.table_names[t]);
        pthread_create(&pth[t], NULL, prepare_rows, (void*)(tid+t));
    }

    for(int t = 0 ; t < schema.prepare.table_num; t++)
    {
        pthread_join(pth[t],NULL);
    }
    /*
    for(int t = 0 ; simon_enable && t < schema.prepare.table_num; t++)
    {
        delete prepare_blurbs[t];
    }
    */
    /*        
              if (driver_caps.needs_commit && db_query(con, "COMMIT") == NULL)
              {
              if (db_query(con, "COMMIT") == NULL)
              {
              log_text(LOG_FATAL, "failed to commit inserted rows!");
              return 1;
              }
              }
     */
    //oltp_disconnect(con);
    free(pth);
    free(tid);

    return 0;

    //error:
    //oltp_disconnect(con);
    //if (query != NULL)
    //	free(query);

    //return 1;
}

int oltp_cmd_cleanup(void)
{
    db_conn_t *con;
    char      query[10240];

    if (parse_arguments())
        return 1;

    if(schema_init())
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
    log_text(LOG_NOTICE, "Dropping table ... sql '%s'", schema.cleanup.drop_sql);
    /*
       snprintf(query, sizeof(query), "DROP TABLE %s", args.table_name);
     */
    strcpy(query, schema.cleanup.drop_sql);
    if (db_query(con, query) == NULL)
    {
        oltp_disconnect(con);
        return 1;
    }

    oltp_disconnect(con);
    log_text(LOG_INFO, "Done.");

    return 0;
}

int oltp_init(void)
{

    db_conn_t    *con;
    uint32_t thread_id;
    char         query[MAX_QUERY_LEN];
    int          simon_enable = 0;

    if (parse_arguments())
        return 1;

    if(schema_init())
        return 1;

    if(args.simon_host)
    {
        simon::Simon_sysbenchContextC &ctx= simon::Simon_sysbenchContextC::getInstance();
        if(!ctx.startMonitoring(args.simon_cluster, args.simon_host, args.simon_port)) 
        {
            log_text(LOG_FATAL, "failed to start simon");
        }
        else
        {

            log_text(LOG_NOTICE, "start scale simon succeed");
            simon_enable = 1;
        }
    }
    else {
        log_text(LOG_NOTICE, "start sysbench without simon");
    }
    /* Get database capabilites */
    /* junyue start */
    if (db_describe(driver, &driver_caps, schema.prepare.table_names[0]))
    {
        log_text(LOG_FATAL, "failed to get database capabilities!");
        //return 1;
    }
    /* junyue end */

    /* Truncate table in case of nontrx INSERT test */
    /*
       if (args.test_mode == TEST_MODE_NONTRX && args.nontrx_mode == NONTRX_MODE_INSERT)
       {
       con = oltp_connect();
       if (con == NULL)
       return 1;
       snprintf(query, sizeof(query), "TRUNCATE TABLE %s", args.table_name);
       if (db_query(con, query) == NULL)
       {
       log_text(LOG_FATAL, "failed to truncate table %s, have to drop and create ...", args.table_name);
       log_text(LOG_NOTICE, "drop and create table %s", args.table_name);
       if(db_query(con, schema.cleanup.drop_sql) == NULL)
       {
       log_text(LOG_FATAL, "failed to drop table %s", args.table_name);
       return 1;
       }
       else 
       {
       if(db_query(con, schema.prepare.create_sql) == NULL)
       {
       log_text(LOG_FATAL, "failed to create table %s", args.table_name);
       return 1;
       }
       }
       }
       oltp_disconnect(con);
       }
     */
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

    simon::SysbenchPerHostBlurbC* my_blurbs[sb_globals.num_threads]; 
    if(simon_enable)
    {
        char machine[512] = {0}; 
        size_t len;
        gethostname(machine, 512);
        blurbs = my_blurbs;
        log_text(LOG_NOTICE, "sysbench run on host:%s", machine);
        for (thread_id = 0; thread_id < sb_globals.num_threads; thread_id++)
        {
            blurbs[thread_id] = new simon::SysbenchPerHostBlurbC();
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
        if (prepare_stmt_set(statements + thread_id, bind_bufs + thread_id,
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

    /* Initialize random seed for non-transactional delete test */
    if (args.test_mode == TEST_MODE_NONTRX)
    {
        rnd_seed = LARGE_PRIME;
        pthread_mutex_init(&rnd_mutex, NULL);
    }

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

    return 0;
}


int oltp_done(void)
{
    uint32_t thread_id;

    if (args.test_mode == TEST_MODE_NONTRX)
        pthread_mutex_destroy(&rnd_mutex);

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
    if(args.range_size) {
        log_text(LOG_NOTICE, "Fixbug!!! to stop all scale threads and destory prepare blurbs");
        /*
        for(int t = 0 ; prepare_blurbs && t < schema.prepare.table_num; t++)
        {
            delete prepare_blurbs[t];
        }
        */
    }
    /*
       pthread_cancel(scale_thr);
       if(scale_blurb)
       {
       delete scale_blurb;
       }
     */
    return 0;
}


void oltp_print_mode(void)
{
    log_text(LOG_NOTICE, "Doing OLTP test.");

    switch (args.test_mode) {
        case TEST_MODE_SIMPLE:
            log_text(LOG_NOTICE, "Running simple OLTP test");
            break;
        case TEST_MODE_COMPLEX:
            log_text(LOG_NOTICE, "Running mixed OLTP test");
            break;
        case TEST_MODE_NONTRX:
            log_text(LOG_NOTICE, "Running non-transactional test");
            break;
        case TEST_MODE_SP:
            log_text(LOG_NOTICE, "Running stored procedure test");
            return;
            break;
        case TEST_MODE_JOIN:
            log_text(LOG_NOTICE, "Running join OLTP test");
            break;
        default:
            log_text(LOG_WARNING, "Unknown OLTP test mode!");
            break;
    }

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

    switch (args.test_mode) {
        case TEST_MODE_SIMPLE:
            return get_request_simple(tid);
        case TEST_MODE_COMPLEX:
            return get_request_complex(tid);
        case TEST_MODE_NONTRX:
            return get_request_nontrx(tid);
        case TEST_MODE_SP:
            return get_request_sp(tid);
            /* junyue start */
        case TEST_MODE_JOIN:
            return get_request_join(tid);
            /* junyue end */
        default:
            log_text(LOG_FATAL, "unknown test mode: %d!", args.test_mode);
            sb_req.type = SB_REQ_TYPE_NULL;
    }

    return sb_req;
}


inline int add_reconnect_req(sb_list_t *list)
{
    sb_sql_query_t *query;

    query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
    query->num_times = 1;
    query->type = SB_SQL_QUERY_RECONNECT;
    query->nrows = 0;
    query->think_time = get_think_time();
    SB_LIST_ADD_TAIL(&query->listitem, list);
    return 0;
}

sb_request_t get_request_sp(int tid)
{
    sb_request_t     sb_req;
    sb_sql_request_t *sql_req = &sb_req.u.sql_request;
    sb_sql_query_t   *query;

    (void)tid; /* unused */

    sb_req.type = SB_REQ_TYPE_SQL;

    sql_req->queries = (sb_list_t *)malloc(sizeof(sb_list_t));
    query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
    if (sql_req->queries == NULL || query == NULL)
    {
        log_text(LOG_FATAL, "cannot allocate SQL query!");
        sb_req.type = SB_REQ_TYPE_NULL;
        return sb_req;
    }

    SB_LIST_INIT(sql_req->queries);
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_SQL_QUERY_CALL;
    query->nrows = 0;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);

    if (args.reconnect_mode == RECONNECT_QUERY ||
            (args.reconnect_mode == RECONNECT_RANDOM && sb_rnd() % 2))
        add_reconnect_req(sql_req->queries);

    req_performed++;

    return sb_req;
}

/* junyue start */
sb_request_t get_request_join(int tid)
{
    sb_request_t        sb_req;
    sb_sql_request_t    *sql_req = &sb_req.u.sql_request;
    sb_sql_query_t      *query;

    (void)tid; /* unused */

    sb_req.type = SB_REQ_TYPE_SQL;

    sql_req->queries = (sb_list_t *)malloc(sizeof(sb_list_t));
    query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
    if (sql_req->queries == NULL || query == NULL)
    {
        log_text(LOG_FATAL, "cannot allocate SQL query!");
        sb_req.type = SB_REQ_TYPE_NULL;
        return sb_req;
    }

    SB_LIST_INIT(sql_req->queries);
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_SQL_QUERY_JOIN_POINT;

    query->varlist = &((bind_bufs+tid)->join_point);
    for( int j = 0; j < query->varlist->count; j++) 
    {
        if(query->varlist->vars[j].type == INT) {
            //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.point_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.point_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.point_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.point_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
        }
        else {
            strcpy(query->varlist->vars[j].value.s_var, "Fix me");
            query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
        }
    }
    query->nrows = schema.run.point_join_sql_rows;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);

    if (args.reconnect_mode == RECONNECT_QUERY ||
            (args.reconnect_mode == RECONNECT_RANDOM && sb_rnd() % 2))
        add_reconnect_req(sql_req->queries);

    req_performed++;

    return sb_req;


}
/* junyue end */
sb_request_t get_request_simple(int tid)
{
    sb_request_t        sb_req;
    sb_sql_request_t    *sql_req = &sb_req.u.sql_request;
    sb_sql_query_t      *query;

    (void)tid; /* unused */

    sb_req.type = SB_REQ_TYPE_SQL;

    sql_req->queries = (sb_list_t *)malloc(sizeof(sb_list_t));
    query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
    if (sql_req->queries == NULL || query == NULL)
    {
        log_text(LOG_FATAL, "cannot allocate SQL query!");
        sb_req.type = SB_REQ_TYPE_NULL;
        return sb_req;
    }

    SB_LIST_INIT(sql_req->queries);
    query->num_times = 1;
    query->think_time = get_think_time();
    query->type = SB_SQL_QUERY_POINT;
    //query->u.point_query.id = GET_RANDOM_ID();
    query->varlist = &((bind_bufs+tid)->point);
    for( int j = 0; j < query->varlist->count; j++) 
    {
        if(query->varlist->vars[j].type == INT) {
            //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.point_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.point_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.point_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.point_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
        }
        else {
            strcpy(query->varlist->vars[j].value.s_var, "Fix me");
            query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
        }
    }

    query->nrows = schema.run.point_sql_rows;
    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);

    if (args.reconnect_mode == RECONNECT_QUERY ||
            (args.reconnect_mode == RECONNECT_RANDOM && sb_rnd() % 2))
        add_reconnect_req(sql_req->queries);

    req_performed++;

    return sb_req;
}


sb_request_t get_request_complex(int tid)
{
    sb_request_t        sb_req;
    sb_sql_request_t    *sql_req = &sb_req.u.sql_request;
    sb_sql_query_t      *query;
    sb_list_item_t      *pos;
    sb_list_item_t      *tmp;
    uint32_t        i;
    uint32_t        range;
    reconnect_mode_t    rmode;

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
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->type = SB_SQL_QUERY_LOCK;
        query->num_times = 1;
        query->think_time = 0;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    }
    /* junyue start */
    /* Generate set of join point selects */
    for(i = 0; i < args.join_point_selects; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_JOIN_POINT;
        query->varlist = &((bind_bufs+tid)->join_point);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                if(schema.run.point_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.point_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.point_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.point_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        //query->u.join_point_query.id = GET_RANDOM_ID();
        query->nrows = schema.run.point_join_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue end */


    /* Generate set of point selects */
    for(i = 0; i < args.point_selects; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_POINT;
        query->varlist = &((bind_bufs+tid)->point);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                if(schema.run.point_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.point_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.point_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.point_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        //query->u.point_query.id = GET_RANDOM_ID();
        query->nrows = schema.run.point_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue start */
    /* Generate join range queries */
    for(i = 0; i < args.join_simple_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_JOIN_RANGE;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;     
        query->varlist = &((bind_bufs+tid)->join_range);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.range_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.range_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.range_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.range_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        /*
           query->u.join_range_query.from = range;
           query->u.join_range_query.to = range + args.range_size - 1;
         */
        query->nrows = schema.run.range_join_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue end */


    /* Generate range queries */
    for(i = 0; i < args.simple_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_RANGE;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;     
        query->varlist = &((bind_bufs+tid)->range);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.range_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                    //printf("%d ---- %d\n", j, query->varlist->vars[j].value.i_var);
                }
                else {
                    char* sep = strchr(schema.run.range_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.range_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.range_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                       //printf("bd=%d  %d === %d   sep+1:%s\n",bd, j, query->varlist->vars[j].value.i_var, sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        /*
           query->u.range_query.from = range;
           query->u.range_query.to = range + args.range_size - 1;
         */
        query->nrows = schema.run.range_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue start */
    /* Generate sum range queries */
    for(i = 0; i < args.join_sum_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_JOIN_RANGE_SUM;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;
        query->varlist = &((bind_bufs+tid)->join_range_sum);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.sum_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.sum_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.sum_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.sum_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        /*
           query->u.join_range_query.from = range;
           query->u.join_range_query.to = range + args.range_size - 1;
         */
        query->nrows = schema.run.sum_join_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue end */


    /* Generate sum range queries */
    for(i = 0; i < args.sum_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_RANGE_SUM;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;
        query->varlist = &((bind_bufs+tid)->range_sum);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.sum_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.sum_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.sum_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.sum_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        // query->u.range_query.from = range;
        //query->u.range_query.to = range + args.range_size - 1;
        query->nrows = schema.run.sum_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }

    /* junyue start */
    /* Generate join ordered range queries */
    for(i = 0; i < args.join_order_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_JOIN_RANGE_ORDER;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        // /   range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;
        query->varlist = &((bind_bufs+tid)->join_range_order);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.orderby_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.orderby_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.orderby_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.orderby_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        //query->u.join_range_query.from = range;
        //query->u.join_range_query.to = range + args.range_size - 1;
        query->nrows = schema.run.range_join_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue start */


    /* Generate ordered range queries */
    for(i = 0; i < args.order_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_RANGE_ORDER;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;
        query->varlist = &((bind_bufs+tid)->range_order);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {

            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //
                //left = !left;
                if(schema.run.orderby_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.orderby_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.orderby_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.orderby_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        //query->u.range_query.from = range;
        //query->u.range_query.to = range + args.range_size - 1;
        query->nrows = schema.run.orderby_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }

    /* junyue start */
    /* Generate join distinct range queries */
    for(i = 0; i < args.join_distinct_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_JOIN_RANGE_DISTINCT;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;     
        query->varlist = &((bind_bufs+tid)->join_range_distinct);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.distinct_join_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.distinct_join_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.distinct_join_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.distinct_join_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }



        //query->u.join_range_query.from = range;
        //query->u.join_range_query.to = range + args.range_size;
        query->nrows = schema.run.distinct_join_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }
    /* junyue end */


    /* Generate distinct range queries */
    for(i = 0; i < args.distinct_ranges; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_RANGE_DISTINCT;
        range = GET_RANDOM_ID();
        //if (range + args.range_size > args.table_size)
        //    range = args.table_size - args.range_size;
        //if (range < 1)
        //    range = 1;     
        //query->u.range_query.from = range;
        //query->u.range_query.to = range + args.range_size;
        query->varlist = &((bind_bufs+tid)->range_distinct);
        bool left = true;
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = (left == true )?range:range + args.range_size - 1;
                //left = !left;
                if(schema.run.distinct_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.distinct_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.distinct_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.distinct_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        query->nrows = schema.run.distinct_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }

    /* Skip all write queries for read-only test mode */
    if (args.read_only)
        goto readonly;

    /* Generate index update */
    for (i = 0; i < args.index_updates; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_UPDATE_INDEX;
        query->varlist = &((bind_bufs+tid)->update_index);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.update_key_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.update_key_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.update_key_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.update_key_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        query->nrows = schema.run.update_key_sql_rows;
        //query->u.update_query.id = GET_RANDOM_ID();
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }

    /* Generate non-index update */
    for (i = 0; i < args.non_index_updates; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_UPDATE_NON_INDEX;
        //  query->u.update_query.id = GET_RANDOM_ID();
        query->varlist = &((bind_bufs+tid)->update_non_index);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.update_nonkey_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.update_nonkey_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.update_nonkey_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.update_nonkey_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        query->nrows = schema.run.update_nonkey_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }

    /* Generate replaces */
    for (i = 0; i < args.replaces; i++)
    {
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_REPLACE;
        //  query->u.update_query.id = GET_RANDOM_ID();
        query->varlist = &((bind_bufs+tid)->replace);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.replace_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.replace_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.replace_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.replace_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }

        query->nrows = schema.run.replace_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_QUERY)
            add_reconnect_req(sql_req->queries);
    }




    for(i = 0; i < args.deletes; i++)  
    {
        range = GET_RANDOM_ID();
        /* Generate delete */
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_DELETE;
        query->varlist = &((bind_bufs+tid)->delete_row);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                query->varlist->vars[j].value.i_var = range;
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        query->nrows = schema.run.delete_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        /* Generate insert with same value */
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->num_times = 1;
        query->think_time = get_think_time();
        query->type = SB_SQL_QUERY_INSERT;
        //query->u.insert_query.id = range;
        query->varlist = &((bind_bufs+tid)->insert);
        for( int j = 0; j < query->varlist->count; j++) 
        {
            if(query->varlist->vars[j].type == INT) {
                query->varlist->vars[j].value.i_var = range;
            }
            else {
                strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
            }
        }
        query->nrows = schema.run.insert_sql_rows;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
    }
readonly:

    if (!args.skip_trx)
    {
        /* Generate commit */
        query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
        if (query == NULL)
            goto memfail;
        query->type = SB_SQL_QUERY_UNLOCK;
        query->num_times = 1;
        query->think_time = 0;
        SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);
        if (rmode == RECONNECT_TRANSACTION)
            add_reconnect_req(sql_req->queries);
    }

    /* return request */
    req_performed++;
    return sb_req;

    /* Handle memory allocation failures */
memfail:
    log_text(LOG_FATAL, "cannot allocate SQL query!");
    SB_LIST_FOR_EACH_SAFE(pos, tmp, sql_req->queries)
    {
        query = SB_LIST_ENTRY(pos, sb_sql_query_t, listitem);
        free(query);
    }
    free(sql_req->queries);
    sb_req.type = SB_REQ_TYPE_NULL;
    return sb_req;
}


sb_request_t get_request_nontrx(int tid)
{
    sb_request_t        sb_req;
    sb_sql_request_t    *sql_req = &sb_req.u.sql_request;
    sb_sql_query_t      *query;

    (void)tid; /* unused */

    sb_req.type = SB_REQ_TYPE_SQL;

    sql_req->queries = (sb_list_t *)malloc(sizeof(sb_list_t));
    if (sql_req->queries == NULL)
    {
        log_text(LOG_FATAL, "cannot allocate SQL query!");
        sb_req.type = SB_REQ_TYPE_NULL;
        return sb_req;
    }
    SB_LIST_INIT(sql_req->queries);

    query = (sb_sql_query_t *)malloc(sizeof(sb_sql_query_t));
    if (query == NULL)
        goto memfail;
    query->num_times = 1;
    query->think_time = get_think_time();

    switch (args.nontrx_mode) {
        case NONTRX_MODE_SELECT:
            query->type = SB_SQL_QUERY_POINT;
            //query->u.point_query.id = GET_RANDOM_ID();
            query->varlist = &((bind_bufs+tid)->point);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                    //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.point_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.point_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.point_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.point_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
                }
            }
            query->nrows = schema.run.point_sql_rows;
            break;
        case NONTRX_MODE_UPDATE_KEY:
            query->type = SB_SQL_QUERY_UPDATE_INDEX;
            query->varlist = &((bind_bufs+tid)->update_index);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                 //   query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.update_key_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.update_key_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.update_key_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.update_key_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);

                }
            }
            query->nrows = schema.run.update_key_sql_rows;
            break;
        case NONTRX_MODE_UPDATE_NOKEY:
            query->type = SB_SQL_QUERY_UPDATE_NON_INDEX;
            //query->u.update_query.id = GET_RANDOM_ID();
            query->varlist = &((bind_bufs+tid)->update_non_index);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                  //  query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.update_nonkey_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.update_nonkey_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.update_nonkey_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.update_nonkey_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);

                }
            }
            query->nrows = schema.run.update_nonkey_sql_rows;
            break;
        case NONTRX_MODE_INSERT:
            query->type = SB_SQL_QUERY_INSERT;
            //query->u.update_query.id = GET_RANDOM_ID();
            query->varlist = &((bind_bufs+tid)->insert);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                    //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                    //query->varlist->vars[j].value.i_var = get_unique_random_id();
                if(schema.run.insert_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = get_unique_random_id();
                }
                else {
                    char* sep = strchr(schema.run.insert_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.insert_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.insert_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
                }
            }
            query->nrows = schema.run.insert_sql_rows;
            break;
        case NONTRX_MODE_REPLACE:
            query->type = SB_SQL_QUERY_REPLACE;
            query->varlist = &((bind_bufs+tid)->replace);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                    //query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                if(schema.run.replace_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = GET_RANDOM_ID();
                }
                else {
                    char* sep = strchr(schema.run.replace_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.replace_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.replace_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
                }
            }
            query->nrows = schema.run.replace_sql_rows;
            break;
        case NONTRX_MODE_DELETE:
            query->type = SB_SQL_QUERY_DELETE;
            query->varlist = &((bind_bufs+tid)->delete_row);
            for( int j = 0; j < query->varlist->count; j++) 
            {
                if(query->varlist->vars[j].type == INT) {
                    //query->varlist->vars[j].value.i_var = get_unique_random_id();
                if(schema.run.delete_sql_vars.expr[j][0] == 0) {
                    query->varlist->vars[j].value.i_var = get_unique_random_id();
                }
                else {
                    char* sep = strchr(schema.run.delete_sql_vars.expr[j], '+');
                    if(!sep) {
                       log_text(LOG_ALERT, "invalid expression : %s", schema.run.delete_sql_vars.expr[j]);
                    }
                    else {
                       int bd = atoi(schema.run.delete_sql_vars.expr[j]);
                       query->varlist->vars[j].value.i_var = query->varlist->vars[bd-1].value.i_var + atoi(sep+1);
                    }
                }
                }
                else {
                    strcpy(query->varlist->vars[j].value.s_var, "Fix me");
                    query->varlist->vars[j].data_length = strlen(query->varlist->vars[j].value.s_var);
                }
            }
            query->nrows = schema.run.delete_sql_rows;
            break;
        default:
            log_text(LOG_FATAL, "unknown mode for non-transactional test!");
            free(query);
            sb_req.type = SB_REQ_TYPE_NULL;
            break;
    }

    SB_LIST_ADD_TAIL(&query->listitem, sql_req->queries);

    if (args.reconnect_mode == RECONNECT_QUERY ||
            (args.reconnect_mode == RECONNECT_RANDOM && sb_rnd() % 2))
        add_reconnect_req(sql_req->queries);

    /* return request */
    req_performed++;
    return sb_req;

    /* Handle memory allocation failures */
memfail:
    log_text(LOG_FATAL, "cannot allocate SQL query!");
    if (query)
        free(query);
    free(sql_req->queries);
    sb_req.type = SB_REQ_TYPE_NULL;
    return sb_req;
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
    sb_sql_query_t      *query;
    uint32_t        i;
    uint32_t        local_read_ops=0;
    uint32_t        local_write_ops=0;
    uint32_t        local_other_ops=0;
    uint32_t        local_deadlocks=0;
    int                 retry;
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
        retry = 0;
        SB_LIST_FOR_EACH(pos, sql_req->queries)
        {
            query = SB_LIST_ENTRY(pos, sb_sql_query_t, listitem);

            for(i = 0; i < query->num_times; i++)
            {
                /* emulate user thinking */
                if (query->think_time > 0)
                    usleep(query->think_time); 

                if (query->type == SB_SQL_QUERY_RECONNECT)
                {
                    if (oltp_reconnect(thread_id))
                        return 1;
                    continue;
                }

                /* find prepared statement */
                stmt = get_sql_statement(query, thread_id);
                if (stmt == NULL)
                {
                    log_text(LOG_FATAL, "unknown SQL query type: %d!", query->type);
                    sb_globals.error = 1;
                    return 1;
                }

                if (sb_globals.debug)
                    sb_timer_start(exec_timers + thread_id);
                rs = db_execute(stmt);

                if (sb_globals.debug)
                    sb_timer_stop(exec_timers + thread_id);


                if (rs == NULL)
                {
                    rc = db_errno(connections[thread_id]);
                    if (rc != SB_DB_ERROR_DEADLOCK)
                    {
                        //log_text(LOG_FATAL, "database error, exiting...");
                        //log_text(LOG_ALERT, "query failed, omit ...");
                        /*
                        if(rc == 2006 || rc == 2003) 
                        {
                            log_text(LOG_FATAL, "connect failed, try to reconnect");
                            // sb_globals.error = 1;
                            // return 1; 
                            while (oltp_reconnect(thread_id)) {
                                log_text(LOG_FATAL, "reconnect failed, retry after 1 second");
                                sleep(1);
                                //return 1;
                            }
                        }
                        */
                        
                        log_text(LOG_ALERT, "rc => %d", rc);
                        if(rc == 12) {
                            log_text(LOG_ALERT, "query timeout, omit ...");
                        }
                        else {
                            log_text(LOG_ALERT, "other error, exit ...");
                            sb_globals.error = 1;
                            return 1;
                        }
                    }  
                    continue;
                }

                /* junyue: SB_SQL_QUERY_JOIN must meet*/
                if(stmt->query != NULL && strncasecmp(stmt->query,"select",strlen("select")) == 0)
                    //if (query->type > SB_SQL_QUERY_READ_START && 
                    //		query->type < SB_SQL_QUERY_READ_END) /* select query */
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
                        break;  
                    }
                    else if (rc != SB_DB_ERROR_NONE)
                    {
                        log_text(LOG_FATAL, "rc: %d Error fetching result: `%s`",rc, stmt->query);
                        //continue;
                        /* exiting, forget about allocated memory */

                        //sb_globals.error = 1;
                        //return 1; 
                    }

                    /* Validate the result set if requested */
                    if (sb_globals.validate && query->nrows > 0)
                    {
                        if(!check_data(rs, query)){
                            log_text(LOG_WARNING, "result mismatch, query: %s", stmt->query);
                        }

                    }

                }
                db_free_results(rs);
            }

            /* count operation statistics */
            switch(query->type) {
                case SB_SQL_QUERY_POINT:
                case SB_SQL_QUERY_RANGE:
                case SB_SQL_QUERY_RANGE_SUM:
                case SB_SQL_QUERY_RANGE_ORDER:
                case SB_SQL_QUERY_RANGE_DISTINCT:
                case SB_SQL_QUERY_JOIN_POINT: // junyue
                case SB_SQL_QUERY_JOIN_RANGE: // junyue
                case SB_SQL_QUERY_JOIN_RANGE_SUM: // junyue
                case SB_SQL_QUERY_JOIN_RANGE_ORDER: // junyue
                case SB_SQL_QUERY_JOIN_RANGE_DISTINCT: // junyue
                    local_read_ops += query->num_times;
                    break;
                case SB_SQL_QUERY_UPDATE_INDEX:
                case SB_SQL_QUERY_UPDATE_NON_INDEX:
                case SB_SQL_QUERY_DELETE:
                case SB_SQL_QUERY_INSERT:
                case SB_SQL_QUERY_REPLACE:
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

    if(blurbs) 
    {
        uint64_t recent_latency = 0;
        if(local_read_ops + local_write_ops + local_other_ops > 0)
        {
            recent_latency = sb_globals.op_timers[thread_id].my_time/1000000/(local_read_ops + local_write_ops + local_other_ops);
        }
        blurbs[thread_id]->incrRequestCount(local_read_ops + local_write_ops + local_other_ops);
        blurbs[thread_id]->setRequestLatency(recent_latency);
        blurbs[thread_id]->update();
    }
    /* Free list of queries */
    SB_LIST_FOR_EACH_SAFE(pos, tmp, sql_req->queries)
    {
        query = SB_LIST_ENTRY(pos, sb_sql_query_t, listitem);
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
    if (prepare_stmt_set(statements + thread_id, bind_bufs + thread_id,
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

    s = sb_get_value_string("oltp-test-mode");
    if (!strcmp(s, "simple"))
        args.test_mode = TEST_MODE_SIMPLE;
    else if (!strcmp(s, "complex"))
        args.test_mode = TEST_MODE_COMPLEX;
    else if (!strcmp(s, "nontrx"))
        args.test_mode = TEST_MODE_NONTRX;
    else if (!strcmp(s, "sp"))
        args.test_mode = TEST_MODE_SP;
    /* junyue start */
    else if (!strcmp(s, "join"))
        args.test_mode = TEST_MODE_JOIN;
    /* junyue end */
    else
    {
        log_text(LOG_FATAL, "Invalid OLTP test mode: %s.", s);
        return 1;
    }

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
        log_text(LOG_FATAL, "Invalid value for --oltp-reconnect-mode: '%s'", s);
        return 1;
    }

    args.sp_name = sb_get_value_string("oltp-sp-name");
    if (args.test_mode == TEST_MODE_SP && args.sp_name == NULL)
    {
        log_text(LOG_FATAL, "Name of stored procedure must be specified with --oltp-sp-name "
                "in SP test mode");
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

    args.schema_path = sb_get_value_string("oltp-schema-path");
    args.simon_host = sb_get_value_string("oltp-simon-host");
    args.simon_port = sb_get_value_int("oltp-simon-port");
    args.simon_cluster = sb_get_value_string("oltp-simon-cluster");
    args.rows_per_insert = sb_get_value_int("oltp-insert-row");

    /* junyue end */
    args.index_updates = sb_get_value_int("oltp-index-updates");
    args.non_index_updates = sb_get_value_int("oltp-non-index-updates");
    args.replaces = sb_get_value_int("oltp-replaces");
    args.deletes = sb_get_value_int("oltp-deletes");

    s = sb_get_value_string("oltp-nontrx-mode");
    if (!strcmp(s, "select"))
        args.nontrx_mode = NONTRX_MODE_SELECT;
    else if (!strcmp(s, "update_key"))
        args.nontrx_mode = NONTRX_MODE_UPDATE_KEY;
    else if (!strcmp(s, "update_nokey"))
        args.nontrx_mode = NONTRX_MODE_UPDATE_NOKEY;
    else if (!strcmp(s, "insert"))
        args.nontrx_mode = NONTRX_MODE_INSERT;
    else if (!strcmp(s, "delete"))
        args.nontrx_mode = NONTRX_MODE_DELETE;
    else if (!strcmp(s, "replace"))
        args.nontrx_mode = NONTRX_MODE_REPLACE;
    else
    {
        log_text(LOG_FATAL, "Invalid value of oltp-nontrx-mode: %s", s);
        return 1;
    }

    args.connect_delay = sb_get_value_int("oltp-connect-delay");
    args.user_delay_min = sb_get_value_int("oltp-user-delay-min");
    args.user_delay_max = sb_get_value_int("oltp-user-delay-max");
    args.table_name = sb_get_value_string("oltp-table-name");
    args.table_size = sb_get_value_int("oltp-table-size");
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


/* Prepare a set of statements for the test */


int prepare_stmt_set(oltp_stmt_set_t *set, oltp_bind_set_t *bufs, db_conn_t *conn)
{
    if (args.test_mode == TEST_MODE_NONTRX)
        return prepare_stmt_set_nontrx(set, bufs, conn);
    else if (args.test_mode == TEST_MODE_COMPLEX ||
            args.test_mode == TEST_MODE_SIMPLE  ||
            args.test_mode == TEST_MODE_JOIN) // junyue
        return prepare_stmt_set_trx(set, bufs, conn);

    return prepare_stmt_set_sp(set, bufs, conn);
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
    db_close(set->update_index);
    db_close(set->update_non_index);
    db_close(set->delete_row);
    db_close(set->insert);
    memset(set, 0, sizeof(oltp_stmt_set_t));
}


/* Generate SQL statement from query */


db_stmt_t *get_sql_statement(sb_sql_query_t *query, int thread_id)
{
    if (args.test_mode == TEST_MODE_NONTRX)
        return get_sql_statement_nontrx(query, thread_id);
    else if (args.test_mode == TEST_MODE_COMPLEX ||
            args.test_mode == TEST_MODE_SIMPLE)
        return get_sql_statement_trx(query, thread_id);
    else if(args.test_mode == TEST_MODE_JOIN)
        return get_sql_statement_join(query, thread_id);
    return get_sql_statement_sp(query, thread_id);
}


/* Prepare a set of statements for SP test */


int prepare_stmt_set_sp(oltp_stmt_set_t *set, oltp_bind_set_t *bufs, db_conn_t *conn)
{
    db_bind_t params[2];
    char      query[MAX_QUERY_LEN];

    /* Prepare CALL statement */
    snprintf(query, MAX_QUERY_LEN, "CALL %s(?,?)", args.sp_name);
    set->call = db_prepare(conn, query);
    if (set->call == NULL)
        return 1;
    params[0].type = DB_TYPE_INT;
    params[0].buffer = &bufs->call.thread_id;
    params[0].is_null = 0;
    params[0].data_len = 0;
    params[1].type = DB_TYPE_INT;
    params[1].buffer = &bufs->call.nthreads;
    params[1].is_null = 0;
    params[1].data_len = 0;
    if (db_bind_param(set->call, params, 2))
        return 1;
    return 0;
}

/* Prepare a set of statements for transactional test */


int prepare_stmt_set_trx(oltp_stmt_set_t *set, oltp_bind_set_t *bufs, db_conn_t *conn)
{
    db_bind_t binds[11];
    char      query[MAX_QUERY_LEN];
    int       count; // variable count
    int       i = 0;
    /* Prepare the join point statement */
    strcpy(query, schema.run.point_join_sql);
    set->join_point = db_prepare(conn, query);
    if (set->join_point == NULL)
        return 1;
    bufs->join_point.count = schema.run.point_join_sql_vars.count;
    for(i = 0; i < schema.run.point_join_sql_vars.count; i++)
    {
        int bind_pos = schema.run.point_join_sql_vars.var_bind[i] - 1;

        if(schema.run.point_join_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->join_point.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->join_point.vars[bind_pos].data_length;
            bufs->join_point.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->join_point.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->join_point.id;
       binds[0].is_null = 0;
       binds[0].data_len = 0;

     */
    if (db_bind_param(set->join_point, binds, bufs->join_point.count))
        return 1;
    /* junyue end */


    /* Prepare the point statement */

    strcpy(query, schema.run.point_sql);

    set->point = db_prepare(conn, query);
    if (set->point == NULL)
        return 1;

    bufs->point.count = schema.run.point_sql_vars.count;

    for(i = 0; i < schema.run.point_sql_vars.count; i++)
    {
        int bind_pos = schema.run.point_sql_vars.var_bind[i] - 1;
        if(schema.run.point_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->point.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->point.vars[bind_pos].data_length;
            bufs->point.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->point.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->point.id;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
     */
    if (db_bind_param(set->point, binds, bufs->point.count))
        return 1;

    /* Prepare the range statement */
    //snprintf(query, MAX_QUERY_LEN, "SELECT c from %s where id between ? and ?",
    //         args.table_name);
    strcpy(query, schema.run.range_sql);
    set->range = db_prepare(conn, query);
    if (set->range == NULL)
        return 1;

    bufs->range.count = schema.run.range_sql_vars.count;

    for(i = 0; i < schema.run.range_sql_vars.count; i++)
    {
        int bind_pos = schema.run.range_sql_vars.var_bind[i] - 1;
        if(schema.run.range_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->range.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->range.vars[bind_pos].data_length;
            bufs->range.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->range.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->range.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->range.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->range, binds, bufs->range.count))
        return 1;

    /* Prepare the range_sum statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT SUM(k) from %s where id between ? and ?", args.table_name);
    strcpy(query, schema.run.sum_sql);
    set->range_sum = db_prepare(conn, query);
    if (set->range_sum == NULL)
        return 1;

    bufs->range_sum.count = schema.run.sum_sql_vars.count;

    for(i = 0; i < schema.run.sum_sql_vars.count; i++)
    {
        int bind_pos = schema.run.sum_sql_vars.var_bind[i] - 1;
        if(schema.run.sum_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->range_sum.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->range_sum.vars[bind_pos].data_length;
            bufs->range_sum.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->range_sum.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->range_sum.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->range_sum.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */

    if (db_bind_param(set->range_sum, binds, bufs->range_sum.count))
        return 1;

    /* Prepare the range_order statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT c from %s where id between ? and ? order by c",
    //         args.table_name);
    strcpy(query, schema.run.orderby_sql);
    set->range_order = db_prepare(conn, query);
    if (set->range_order == NULL)
        return 1;

    bufs->range_order.count = schema.run.orderby_sql_vars.count;

    for(i = 0; i < schema.run.orderby_sql_vars.count; i++)
    {
        int bind_pos = schema.run.orderby_sql_vars.var_bind[i] - 1;
        if(schema.run.orderby_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->range_order.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->range_order.vars[bind_pos].data_length;
            bufs->range_order.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->range_order.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->range_order.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->range_order.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->range_order, binds, bufs->range_order.count))
        return 1;

    /* Prepare the range_distinct statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT DISTINCT c from %s where id between ? and ? order by c",
    //         args.table_name);
    strcpy(query, schema.run.distinct_sql);
    set->range_distinct = db_prepare(conn, query);
    if (set->range_distinct == NULL)
        return 1;

    bufs->range_distinct.count = schema.run.distinct_sql_vars.count;

    for(i = 0; i < schema.run.distinct_sql_vars.count; i++)
    {
        int bind_pos = schema.run.distinct_sql_vars.var_bind[i] - 1;
        if(schema.run.distinct_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->range_distinct.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->range_distinct.vars[bind_pos].data_length;
            bufs->range_distinct.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->range_distinct.vars[bind_pos];
        binds[i].is_null = 0;
    }

    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->range_distinct.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->range_distinct.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->range_distinct, binds, bufs->range_distinct.count))
        return 1;

    /* junyue start */

    /* Prepare the join range statement */
    //snprintf(query, MAX_QUERY_LEN, "SELECT j1.c from %s as j1,%s as j2 where j1.id=j2.id and j1.id between ? and ?",
    //         args.table_name, args.table_name);
    strcpy(query, schema.run.range_join_sql);
    set->join_range = db_prepare(conn, query);
    if (set->join_range == NULL)
        return 1;

    bufs->join_range.count = schema.run.range_join_sql_vars.count;

    for(i = 0; i < schema.run.range_join_sql_vars.count; i++)
    {
        int bind_pos = schema.run.range_join_sql_vars.var_bind[i] - 1;
        if(schema.run.range_join_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->join_range.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->join_range.vars[bind_pos].data_length;
            bufs->join_range.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->join_range.vars[bind_pos];
        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->join_range.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->join_range.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->join_range, binds, bufs->join_range.count))
        return 1;

    /* Prepare the join range_sum statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT SUM(j1.k) from %s as j1, %s as j2 where j1.id=j2.id and j1.id between ? and ?", args.table_name,args.table_name);
    strcpy(query, schema.run.sum_join_sql);
    set->join_range_sum = db_prepare(conn, query);
    if (set->join_range_sum == NULL)
        return 1;

    bufs->join_range_sum.count = schema.run.sum_join_sql_vars.count;

    for(i = 0; i < schema.run.sum_join_sql_vars.count; i++)
    {
        int bind_pos = schema.run.sum_join_sql_vars.var_bind[i] - 1;
        if(schema.run.sum_join_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->join_range_sum.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->join_range_sum.vars[bind_pos].data_length;
            bufs->join_range_sum.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->join_range_sum.vars[i];
        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->join_range_sum.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->join_range_sum.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->join_range_sum, binds, bufs->join_range_sum.count))
        return 1;

    /* Prepare the join range_order statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT j1.c from %s as j1, %s as j2 where j1.id=j2.id and j1.id between ? and ? order by j1.c",
    //         args.table_name, args.table_name);
    strcpy(query, schema.run.orderby_join_sql);
    set->join_range_order = db_prepare(conn, query);
    if (set->join_range_order == NULL)
        return 1;
    bufs->join_range_order.count = schema.run.orderby_join_sql_vars.count;

    for(i = 0; i < schema.run.orderby_join_sql_vars.count; i++)
    {
        int bind_pos = schema.run.orderby_join_sql_vars.var_bind[i] - 1;
        if(schema.run.orderby_join_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->join_range_order.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->join_range_order.vars[bind_pos].data_length;
            bufs->join_range_order.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->join_range_order.vars[bind_pos];

        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->join_range_order.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->join_range_order.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->join_range_order, binds, bufs->join_range_order.count))
        return 1;

    /* Prepare the join range_distinct statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "SELECT DISTINCT j1.c from %s as j1, %s as j2 where j1.id = j2.id and j1.id between ? and ? order by j1.c",
    //         args.table_name, args.table_name);
    strcpy(query, schema.run.distinct_join_sql);
    set->join_range_distinct = db_prepare(conn, query);
    if (set->join_range_distinct == NULL)
        return 1;

    bufs->join_range_distinct.count = schema.run.distinct_join_sql_vars.count;

    for(i = 0; i < schema.run.distinct_join_sql_vars.count; i++)
    {
        int bind_pos = schema.run.distinct_join_sql_vars.var_bind[i] - 1;
        if(schema.run.distinct_join_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->join_range_distinct.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->join_range_distinct.vars[bind_pos].data_length;
            bufs->join_range_distinct.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->join_range_distinct.vars[bind_pos];

        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->join_range_distinct.from;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
       binds[1].type = DB_TYPE_INT;
       binds[1].buffer = &bufs->join_range_distinct.to;
       binds[1].is_null = 0;
       binds[1].data_len = 0;
     */
    if (db_bind_param(set->join_range_distinct, binds, bufs->join_range_distinct.count))
        return 1;
    /* junyue end */

    /* Prepare the update_index statement */
    //snprintf(query, MAX_QUERY_LEN, "UPDATE %s set k=k+1 where id=?",
    //         args.table_name);
    strcpy(query, schema.run.update_key_sql);
    set->update_index = db_prepare(conn, query);
    if (set->update_index == NULL)
        return 1;

    bufs->update_index.count = schema.run.update_key_sql_vars.count;

    for(i = 0; i < schema.run.update_key_sql_vars.count; i++)
    {
        int bind_pos = schema.run.update_key_sql_vars.var_bind[i] - 1;
        if(schema.run.update_key_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->update_index.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->update_index.vars[bind_pos].data_length;
            bufs->update_index.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->update_index.vars[bind_pos];

        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->update_index.id;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
     */
    if (db_bind_param(set->update_index, binds, bufs->update_index.count))
        return 1;

    /* Prepare the update_non_index statement */
    //snprintf(query, MAX_QUERY_LEN,
    //         "UPDATE %s set c=? where id=?",
    //         args.table_name);
    strcpy(query, schema.run.update_nonkey_sql);
    set->update_non_index = db_prepare(conn, query);
    if (set->update_non_index == NULL)
        return 1;
    bufs->update_non_index.count = schema.run.update_nonkey_sql_vars.count;
    for(i = 0; i < schema.run.update_nonkey_sql_vars.count; i++)
    {
        int bind_pos = schema.run.update_nonkey_sql_vars.var_bind[i] - 1;
        if(schema.run.update_nonkey_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->update_non_index.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->update_non_index.vars[bind_pos].data_length;
            bufs->update_non_index.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->update_non_index.vars[bind_pos];

        binds[i].is_null = 0;
    }
    if (db_bind_param(set->update_non_index, binds, bufs->update_non_index.count))
        return 1;

    /*
       Prepare replace statement
     */
    strcpy(query, schema.run.replace_sql);
    set->replace = db_prepare(conn, query);
    if (set->replace == NULL)
        return 1;
    bufs->replace.count = schema.run.replace_sql_vars.count;
    for(i = 0; i < schema.run.replace_sql_vars.count; i++)
    {
        int bind_pos = schema.run.replace_sql_vars.var_bind[i] - 1;
        if(schema.run.replace_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->replace.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->replace.vars[bind_pos].data_length;
            bufs->replace.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->replace.vars[bind_pos];

        binds[i].is_null = 0;
    }

    if (db_bind_param(set->replace, binds, bufs->replace.count))
        return 1;

    /* Prepare the delete statement */
    //snprintf(query, MAX_QUERY_LEN, "DELETE from %s where id=?",
    //         args.table_name);
    strcpy(query, schema.run.delete_sql);
    set->delete_row = db_prepare(conn, query);
    if (set->delete_row == NULL)
        return 1;

    bufs->delete_row.count = schema.run.delete_sql_vars.count;

    for(i = 0; i < schema.run.delete_sql_vars.count; i++)
    {
        int bind_pos = schema.run.delete_sql_vars.var_bind[i] - 1;
        if(schema.run.delete_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->delete_row.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->delete_row.vars[bind_pos].data_length;
            bufs->delete_row.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->delete_row.vars[bind_pos];
        binds[i].is_null = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->delete_row.id;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
     */
    if (db_bind_param(set->delete_row, binds, bufs->delete_row.count))
        return 1;

    /* Prepare the insert statement */
    //snprintf(query, MAX_QUERY_LEN, "INSERT INTO %s values(?,0,' ',"
    //         "'aaaaaaaaaaffffffffffrrrrrrrrrreeeeeeeeeeyyyyyyyyyy')",
    //         args.table_name);
    strcpy(query, schema.run.insert_sql);
    set->insert = db_prepare(conn, query);
    if (set->insert == NULL)
        return 1;

    bufs->insert.count = schema.run.insert_sql_vars.count;

    for(i = 0; i < schema.run.insert_sql_vars.count; i++)
    { 
        int bind_pos = schema.run.insert_sql_vars.var_bind[i] - 1;
        if(schema.run.insert_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->insert.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->insert.vars[bind_pos].data_length;
            bufs->insert.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->insert.vars[bind_pos];

        binds[i].is_null = 0;
        binds[i].data_len = 0;
    }
    /*
       binds[0].type = DB_TYPE_INT;
       binds[0].buffer = &bufs->insert.id;
       binds[0].is_null = 0;
       binds[0].data_len = 0;
     */
    if (db_bind_param(set->insert, binds, bufs->insert.count))
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


/* Prepare a set of statements for non-transactional test */


int prepare_stmt_set_nontrx(oltp_stmt_set_t *set, oltp_bind_set_t *bufs, db_conn_t *conn)
{
    db_bind_t binds[11]; // at most 11 ??, fuck!
    char      query[MAX_QUERY_LEN];
    int       i = 0;

    /* Prepare the replace statement */
    strcpy(query, schema.run.replace_sql);
    set->replace = db_prepare(conn, query);
    if (set->replace == NULL)
        return 1;

    bufs->replace.count = schema.run.replace_sql_vars.count;
    for(i = 0; i < schema.run.replace_sql_vars.count; i++)
    {
        int bind_pos = schema.run.replace_sql_vars.var_bind[i] - 1;
        if(schema.run.replace_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->replace.vars[i].type = INT;
        }
        else { 
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->replace.vars[bind_pos].data_length;
            bufs->replace.vars[i].type = STRING;
        }
        binds[i].is_null = 0;
        binds[i].buffer = &bufs->replace.vars[bind_pos];
    }

    if (db_bind_param(set->replace, binds, bufs->replace.count))
        return 1;


    /* Prepare the point statement */
    strcpy(query, schema.run.point_sql);
    set->point = db_prepare(conn, query);
    if (set->point == NULL)
        return 1;

    bufs->point.count = schema.run.point_sql_vars.count;
    for(i = 0; i < schema.run.point_sql_vars.count; i++)
    {
        int bind_pos = schema.run.point_sql_vars.var_bind[i] - 1;
        if(schema.run.point_sql_vars.sql_vars[i] == INT){
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->point.vars[i].type = INT;
        }
        else {
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->point.vars[bind_pos].data_length;
            bufs->point.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->point.vars[bind_pos];

        binds[i].is_null = 0;
    }

    if (db_bind_param(set->point, binds, bufs->point.count))
        return 1;

    /* Prepare the update_index statement */
    strcpy(query, schema.run.update_key_sql);
    set->update_index = db_prepare(conn, query);
    if (set->update_index == NULL)
        return 1;
    bufs->update_index.count = schema.run.update_key_sql_vars.count;
    for(i = 0; i < schema.run.update_key_sql_vars.count; i++)
    {
        int bind_pos = schema.run.update_key_sql_vars.var_bind[i] - 1;
        if(schema.run.update_key_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->update_index.vars[i].type = INT;
        }
        else {
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->update_index.vars[bind_pos].data_length;
            bufs->update_index.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->update_index.vars[bind_pos];
        binds[i].is_null = 0;
    }

    if (db_bind_param(set->update_index, binds, bufs->update_index.count))
        return 1;

    /* Prepare the update_non_index statement */
    strcpy(query, schema.run.update_nonkey_sql);
    set->update_non_index = db_prepare(conn, query);
    if (set->update_non_index == NULL)
        return 1;
    bufs->update_non_index.count = schema.run.update_nonkey_sql_vars.count;
    for(i = 0; i < schema.run.update_nonkey_sql_vars.count; i++)
    {
        int bind_pos = schema.run.update_nonkey_sql_vars.var_bind[i] - 1;
        if(schema.run.update_nonkey_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->update_non_index.vars[i].type = INT;
        }
        else {
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->update_non_index.vars[bind_pos].data_length;
            bufs->update_non_index.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->update_non_index.vars[bind_pos];
        binds[i].is_null = 0;
    }
    if (db_bind_param(set->update_non_index, binds, bufs->update_non_index.count))
        return 1;
    /*
       Non-index update statement is re-bound each time because of the string
       parameter
     */

    /* Prepare the delete statement */
    strcpy(query, schema.run.delete_sql);
    set->delete_row = db_prepare(conn, query);
    if (set->delete_row == NULL)
        return 1;

    bufs->delete_row.count = schema.run.delete_sql_vars.count;
    for(i = 0; i < schema.run.delete_sql_vars.count; i++)
    {
        int bind_pos = schema.run.delete_sql_vars.var_bind[i] - 1;
        if(schema.run.delete_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->delete_row.vars[i].type = INT;
        }
        else {
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->delete_row.vars[bind_pos].data_length;
            bufs->delete_row.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->delete_row.vars[bind_pos];
        binds[i].is_null = 0;
    }

    if (db_bind_param(set->delete_row, binds, bufs->delete_row.count))
        return 1;

    /* Prepare the insert statement */
    strcpy(query, schema.run.insert_sql);
    set->insert = db_prepare(conn, query);
    /*
       Insert statement is re-bound each time because of the string
       parameters
     */
    if (set->insert == NULL)
        return 1;

    bufs->insert.count = schema.run.insert_sql_vars.count;
    for(i = 0; i < schema.run.insert_sql_vars.count; i++)
    {
        int bind_pos = schema.run.insert_sql_vars.var_bind[i] - 1;
        if(schema.run.insert_sql_vars.sql_vars[i] == INT) {
            binds[i].type = DB_TYPE_INT;
            binds[i].data_len = 0;
            bufs->insert.vars[i].type = INT;
        }
        else {
            binds[i].type = DB_TYPE_CHAR;
            binds[i].data_len = &bufs->insert.vars[bind_pos].data_length;
            bufs->insert.vars[i].type = STRING;
        }
        binds[i].buffer = &bufs->insert.vars[bind_pos];
        binds[i].is_null = 0;
    }

    if (db_bind_param(set->insert, binds, bufs->insert.count))
        return 1;


    return 0;
}


/* Generate SQL statement from query for SP test */


db_stmt_t *get_sql_statement_sp(sb_sql_query_t *query, int thread_id)
{
    db_stmt_t       *stmt;
    oltp_bind_set_t  *buf = bind_bufs + thread_id;

    (void) query; /* unused */

    stmt = statements[thread_id].call;
    buf->call.thread_id = thread_id;
    buf->call.nthreads = sb_globals.num_threads;

    return stmt;
}

/* junyue start */
db_stmt_t *get_sql_statement_join(sb_sql_query_t *query, int thread_id)
{
    return get_sql_statement_trx(query, thread_id);
}
/* junyue end */

/* Generate SQL statement from query for transactional test */


db_stmt_t *get_sql_statement_trx(sb_sql_query_t *query, int thread_id)
{
    db_stmt_t       *stmt = NULL;
    db_bind_t       binds[2];
    oltp_bind_set_t *buf = bind_bufs + thread_id;

    switch (query->type) {
        case SB_SQL_QUERY_LOCK:
            stmt = statements[thread_id].lock;
            break;

        case SB_SQL_QUERY_UNLOCK:
            stmt = statements[thread_id].unlock;
            break;

        case SB_SQL_QUERY_POINT:
            stmt = statements[thread_id].point;
            break;
            /* junyue start */ 
        case SB_SQL_QUERY_JOIN_POINT:
            stmt = statements[thread_id].join_point;
            break;
        case SB_SQL_QUERY_JOIN_RANGE:
            stmt = statements[thread_id].join_range;
            break;

        case SB_SQL_QUERY_JOIN_RANGE_SUM:
            stmt = statements[thread_id].join_range_sum;
            break;

        case SB_SQL_QUERY_JOIN_RANGE_ORDER:
            stmt = statements[thread_id].join_range_order;
            break;

        case SB_SQL_QUERY_JOIN_RANGE_DISTINCT:
            stmt = statements[thread_id].join_range_distinct;
            break;


            /* junyue end */ 

        case SB_SQL_QUERY_RANGE:
            stmt = statements[thread_id].range;
            break;

        case SB_SQL_QUERY_RANGE_SUM:
            stmt = statements[thread_id].range_sum;
            break;

        case SB_SQL_QUERY_RANGE_ORDER:
            stmt = statements[thread_id].range_order;
            break;

        case SB_SQL_QUERY_RANGE_DISTINCT:
            stmt = statements[thread_id].range_distinct;
            break;

        case SB_SQL_QUERY_UPDATE_INDEX:
            stmt = statements[thread_id].update_index;
            break;

        case SB_SQL_QUERY_UPDATE_NON_INDEX:
            stmt = statements[thread_id].update_non_index;
            break;

        case SB_SQL_QUERY_DELETE:
            stmt = statements[thread_id].delete_row;
            break;

        case SB_SQL_QUERY_INSERT:
            stmt = statements[thread_id].insert;
            break;
        case SB_SQL_QUERY_REPLACE:
            stmt = statements[thread_id].replace;
            break;

        default:
            return NULL;
    }

    return stmt;
}


/* Generate SQL statement from query for non-transactional test */


db_stmt_t *get_sql_statement_nontrx(sb_sql_query_t *query, int thread_id)
{
    db_stmt_t       *stmt = NULL;
    db_bind_t       binds[4];
    oltp_bind_set_t *buf = bind_bufs + thread_id;

    switch (query->type) {
        case SB_SQL_QUERY_POINT:
            stmt = statements[thread_id].point;
            break;

        case SB_SQL_QUERY_UPDATE_INDEX:
            stmt = statements[thread_id].update_index;
            break;

        case SB_SQL_QUERY_UPDATE_NON_INDEX:
            stmt = statements[thread_id].update_non_index;
            break;

        case SB_SQL_QUERY_DELETE:
            stmt = statements[thread_id].delete_row;
            break;
        case SB_SQL_QUERY_INSERT:
            stmt = statements[thread_id].insert;
            break;


        case SB_SQL_QUERY_REPLACE:
            stmt = statements[thread_id].replace;

            break;

        default:
            return NULL;
    }

    return stmt;
}


/* uniform distribution */


uint32_t rnd_func_uniform(void)
{
    uint32_t ret = 1 + (sb_rnd() % args.table_size) + args.row_offset;
    return ret;
}


/* gaussian distribution */


uint32_t rnd_func_gaussian(void)
{
    int          sum;
    uint32_t i;

    for(i=0, sum=0; i < args.dist_iter; i++)
        sum += (1 + sb_rnd() % args.table_size + args.row_offset);

    uint32_t ret= sum / args.dist_iter;
    return ret;
}


/* 'special' distribution */


uint32_t rnd_func_special(void)
{
    //log_text(LOG_NOTICE, "rnd_func_special does not support offset!!!");
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


/* Generate unique random id */


uint32_t get_unique_random_id(void)
{
    uint32_t res;

    pthread_mutex_lock(&rnd_mutex);
    res = (uint32_t) (rnd_seed % args.table_size) + 1 + args.row_offset;
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

bool check_data(db_result_set_t* result, sb_sql_query_t* query)
{
    int nrows = db_num_rows(result);
    if (nrows != query->nrows) {
        log_text(LOG_WARNING,
                "Number of received rows mismatch, expected: %ld, actual: %ld",
                (long )query->nrows, (long)nrows);
        return false;
    }
    db_row_t *row; 
    while(row  = db_fetch_row(result))
    {

        int val=-100000000;
        for(int i = 0; i < row->field_count; i++ ) {
            if(row->values[i] != NULL) {
                if(strchr(row->values[i],'-') != NULL) continue;

                if(val == -100000000) val =  atoi(row->values[i]);
                else {
                    int rval = atoi(row->values[i]);
                    if(rval != val) {
                        log_text(LOG_WARNING, "column values mismatch");
                        return false;
                    }
                }
            } else {
                log_text(LOG_FATAL, "Return NULL values!!!");
            }

        }
        /*
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

           else {
           log_text(LOG_DEBUG, "row = %p; i1 = %d; i2 = %d; s1 = %s; s2 = %s\n", row, i1, i2, row->values[2], row->values[3]);
           }
         */
    }
    return true;
}

/**
  using ps protocal
 **/  
static void* prepare_rows_using_ps(void* arg)
{
    struct timeval st,ed;
    db_conn_t      *con;
    int              t           = *(int *)(arg);
    int col_num = 0;
    uint32_t nrows = args.rows_per_insert;
    //uint32_t nrows = 10;
    uint64_t local_latency  = 0;
    uint32_t row = args.table_size + args.row_offset;
    uint32_t min_row = row;
    uint32_t step = args.range_size;
    char str[1000];

    db_result_set_t     *rs;

    sb_sql_bind_variables_t* vars = (sb_sql_bind_variables_t*)malloc(sizeof(sb_sql_bind_variables_t) * MAX_VAR_COUNT * nrows);
    db_bind_t* params = (db_bind_t*)malloc(MAX_VAR_COUNT * nrows * sizeof(db_bind_t));

    int query_len = MAX_QUERY_LEN + nrows * (MAX_QUERY_LEN + 1);
    char* query = (char *)malloc(query_len);
    memset(query, 0, query_len);

    snprintf(query, MAX_QUERY_LEN, "REPLACE INTO %s(", schema.prepare.table_names[t]);

    for(int c_idx = 0; c_idx < MAX_COLUMN_COUNT; c_idx++) {
        if(col_rules[t][c_idx].name[0] == 0) break;
        col_num++;
        query_len =  strlen(query);
        sprintf(query + query_len, "%s, ", col_rules[t][c_idx].name);
    }
    query_len =  strlen(query);
    strcpy(query + query_len-2, ") VALUES ");
    for(int i =0 ;i < nrows ;i++) {
        query_len =  strlen(query);
        query[query_len] = '(';
        for(int c_idx = 0; c_idx < col_num; c_idx++) {
            query_len =  strlen(query);
            sprintf(query + query_len, "?, ");
        }
        query_len =  strlen(query);
        query[query_len-2] = ')';
        query[query_len-1] = ',';
    }
    query_len =  strlen(query);
    query[query_len-1] = 0;
    log_text(LOG_DEBUG, "prepare stmt => `%s` col_num => %d", query, col_num);
    //sleep(1000000);
    if(args.simon_host) {
        char machine[512] = {0}; 
        size_t len;
        gethostname(machine, 512);
        prepare_blurbs[t] = new simon::SysbenchPerHostBlurbC();
        prepare_blurbs[t]->setHostname(machine);
    }

scale:    
    con = oltp_connect();
    if (con == NULL) {
        log_text(LOG_ALERT, "oltp_connect failed");
        pthread_exit(NULL);
    }
    db_stmt_t *stmt = db_prepare(con, query);
    if (stmt == NULL) {
        log_text(LOG_ALERT, "db_prepare failed");
        pthread_exit(NULL);
    }

    for(int i = 0; i < nrows; i++) {

        for(int c_idx = 0; c_idx < col_num; c_idx ++) {
            if(col_rules[t][c_idx].type == INT) 
            {
                params[c_idx + col_num * i].type = DB_TYPE_INT;
                params[c_idx + col_num * i].is_null = 0;
                params[c_idx + col_num * i].data_len = 0;
            }
            else if(col_rules[t][c_idx].type == STRING) 
            {
                params[c_idx + col_num * i].type = DB_TYPE_CHAR;
                params[c_idx + col_num * i].data_len = &(vars[c_idx + col_num * i].data_length);
            }
            params[c_idx + col_num * i].buffer = &vars[c_idx + col_num * i];
        }

    }

    if (db_bind_param(stmt, params, col_num * nrows)) {
        log_text(LOG_ALERT, "db_bind_param failed");
    }

    do {

        log_text(LOG_DEBUG, "prepare row(%d) %d",schema.prepare.table_names[t], row);
        for(int i =0 ; i < nrows; i ++) {
            for(int c_idx = 0; c_idx < col_num; c_idx++) {

                memset(str,0,1000);

                vars[c_idx + i * col_num].data_length  = 0;
                vars[c_idx + i * col_num].value.i_var = 0;
                memset(vars[c_idx + i * col_num].value.s_var, 0, MAX_COLUMN_SIZE);

                if(col_rules[t][c_idx].type == INT) {
                    //vars[c_idx + i * col_num].value.i_var = col_rules[t][c_idx].start.i_start + col_rules[t][c_idx].step * row + i * (col_rules[t][c_idx].step);
                    uint32_t var = hash(col_rules[t][c_idx].start.i_start + col_rules[t][c_idx].step * row + i * (col_rules[t][c_idx].step));
                    vars[c_idx + i * col_num].value.i_var =  var;
                }
                else if(col_rules[t][c_idx].type == STRING) {
                    uint32_t var = hash(col_rules[t][c_idx].step * row + 1 + i * col_rules[t][c_idx].step);
                    //sprintf(str,"%d_%s_mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm", col_rules[t][c_idx].step * row + 1 + i * col_rules[t][c_idx].step, col_rules[t][c_idx].start.s_start);
                    sprintf(str,"%u_%s_mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm", var, col_rules[t][c_idx].start.s_start);
                    strcpy(vars[c_idx + i * col_num].value.s_var,  str);
                    vars[c_idx + i * col_num].data_length = strlen(str);
                }
            }
        }

        /* Execute query */
        while(1) {
            gettimeofday(&st, NULL);
            rs = db_execute(stmt);
            if (rs == NULL)
            {
                log_text(LOG_FATAL, "replace failed, reconnect and retry");
                // need to reconnect
                int rc = db_errno(con);
                
                if(rc == 2006 || rc == 2013) {
                    if(oltp_disconnect(con)) {
                        log_text(LOG_FATAL, "close old connection failed");
                    }
                    goto scale;
                }
            }
            else {
                db_free_results(rs);
                break;
            }
        }
        gettimeofday(&ed, NULL);
 
        local_latency = (ed.tv_sec * 1000000 - st.tv_sec * 1000000 + ed.tv_usec - st.tv_usec)/nrows;

        if(args.simon_host) {
            prepare_blurbs[t]->incrPrepareCount(nrows);
            prepare_blurbs[t]->setPrepareLatency(local_latency);
            prepare_blurbs[t]->update();
        }
        //usleep(2000000);

        row+=nrows;
        log_text(LOG_NOTICE, "table =>%s reach row => %u",schema.prepare.table_names[t],row);
        args.table_size = row - args.row_offset;
        if(row%100000000 == 0) {
            log_text(LOG_NOTICE, "table =>%s reach row => %u, need sleep 1 hour",schema.prepare.table_names[t],row);
            sleep(3600);
        }
    } while(1);

    if(args.simon_host) delete prepare_blurbs[t];
    oltp_disconnect(con);
    free(query);
    pthread_exit(NULL);
}


static void* prepare_rows(void* arg)
{

    /* Create database connection */
    db_conn_t      *con;
    char           insert_str[MAX_QUERY_LEN];
    uint32_t   i;
    uint32_t   j;
    uint32_t   n;
    unsigned long  commit_cntr = 0;
    char           *pos;
    uint32_t nrows = args.rows_per_insert;
    int query_len = MAX_QUERY_LEN + nrows * (MAX_QUERY_LEN + 1);
    char* query = (char *)malloc(query_len);
    if (query == NULL)
    {
        log_text(LOG_FATAL, "memory allocation failure!");
        pthread_exit(NULL);
    }

    uint64_t local_latency  = 0;
    int t = *(int *)(arg);
    int loop = 0;

    uint32_t min_row = args.row_offset;
    uint32_t step = 0;
    uint32_t max_row = args.table_size;

    if(t >= 1000) {
        loop = 1;
        min_row = args.table_size + args.row_offset;
        step = args.range_size;
        max_row = min_row + step;
    }
    t = t % 1000;

    if(args.simon_host) {
        char machine[512] = {0}; 
        size_t len;
        gethostname(machine, 512);
        prepare_blurbs[t] = new simon::SysbenchPerHostBlurbC();
        prepare_blurbs[t]->setHostname(machine);
    }
    con = oltp_connect();
    if (con == NULL) {
        free(query);
        pthread_exit(NULL);

    }

    do {

        if(loop) max_row = min_row + step;
        log_text(LOG_DEBUG, "table:%s %d -> %d",schema.prepare.table_names[t], min_row, max_row);
        for (i = min_row; i < max_row; i += nrows)
        {
            sprintf(query, "REPLACE INTO %s(", schema.prepare.table_names[t]);
            for(int c_idx = 0; c_idx < MAX_COLUMN_COUNT; c_idx++) {
                if(col_rules[t][c_idx].name[0] == 0) break;
                sprintf(query + strlen(query), "%s, ", col_rules[t][c_idx].name);
            }
            strcpy(query + strlen(query)-2, ") VALUES ");
            n = strlen(query);

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

                memset(insert_str, 0, MAX_QUERY_LEN);
                insert_str[0] = '('; 
                for(int c_idx = 0; c_idx < MAX_COLUMN_COUNT; c_idx++) {
                    if(col_rules[t][c_idx].name[0] == 0) break;

                    if(col_rules[t][c_idx].type == INT) 
                    {
                        uint32_t var = hash(col_rules[t][c_idx].start.i_start + col_rules[t][c_idx].step * i +j * (col_rules[t][c_idx].step));
                        sprintf(insert_str + strlen(insert_str), "%u,",var);
                    }
                    else if(col_rules[t][c_idx].type == STRING) 
                    {
                        //sprintf(insert_str + strlen(insert_str), "'%d_%s_mmmmmmmmmmmmmmmmmmnnnnnnnnnnnnnnnnnnnnnnoooooooooopppppppppppqqqqqq',", (col_rules[t][c_idx].step) * i + 1 + j * (col_rules[t][c_idx].step), col_rules[t][c_idx].start.s_start);
                        uint32_t var = hash((col_rules[t][c_idx].step) * i + 1 + j * (col_rules[t][c_idx].step));
                        sprintf(insert_str + strlen(insert_str), "'%u_%s_mmmmmmmmmmmmmmmmmmnnnnnnnnnnnnnnnnnnnnnnoooooooooopppppppppppqqqqqq',", var, col_rules[t][c_idx].start.s_start);
                    }
                }

                strcpy(insert_str + strlen(insert_str) - 1, ")");

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

            struct timeval st,ed;
            /* Execute query */
            while(1) {
                gettimeofday(&st, NULL);
                if (db_query(con, query) == NULL)
                {
                    log_text(LOG_FATAL, "replace failed, retry !");
                    //goto error;
                }
                else {
                    break;
                }
            }
            gettimeofday(&ed, NULL);

            local_latency = (nrows > 0)?(ed.tv_sec * 1000000 - st.tv_sec * 1000000 + ed.tv_usec - st.tv_usec)/nrows:0;
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
            if(args.simon_host) {
                prepare_blurbs[t]->incrPrepareCount(nrows);
                prepare_blurbs[t]->setPrepareLatency(local_latency);
                prepare_blurbs[t]->update();
            }

        }
        if(loop) { 
            min_row = max_row;
            args.table_size = min_row - args.row_offset;
        }

    } while(loop);

    if(args.simon_host) delete prepare_blurbs[t];
    oltp_disconnect(con);
    free(query);
    pthread_exit(NULL);
error:
    oltp_disconnect(con);
    if(query) free(query);

    pthread_exit(NULL);
}

int oltp_scale(void)
{
    if(!args.range_size) return 0;

    pthread_t *pth  = (pthread_t*) malloc(sizeof(pthread_t) * schema.prepare.table_num);
    int *tid = (int*)malloc(sizeof(int) * schema.prepare.table_num);       
    for(int t = 0; t < schema.prepare.table_num; t++)
    {
        *(tid+t) = t;
        log_text(LOG_NOTICE, "Creating records from %d in table '%s'...", args.row_offset + args.table_size,
                schema.prepare.table_names[t]);
        pthread_create(&pth[t], NULL, prepare_rows_using_ps, (void*)(tid+t));
        //pthread_create(&pth[t], NULL, prepare_rows, (void*)(tid+t));
    }
    return 0;
}


uint32_t hash(uint32_t n) 
{
    /*revert, instead of hash */
    int r = 0;
    while(n) {
        r = r*10 + n%10;
        n/=10;
    }
    return r;
}
