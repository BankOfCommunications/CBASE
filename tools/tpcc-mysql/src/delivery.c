/*
 * -*-C-*-
 * delivery.pc
 * corresponds to A.4 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

#define NNULL ((void *)0)

int delivery( int t_num,
	      int w_id_arg,
	      int o_carrier_id_arg
)
{
	int64_t            w_id = w_id_arg;
	int            o_carrier_id = o_carrier_id_arg;
	int64_t            d_id;
	int            c_id;
	int64_t           no_o_id;
	float           ol_total;
	char            datetime[81];

	int proceed = 0;

	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[4];
        MYSQL_BIND    column[1];

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* For each district in warehouse */
	/* printf("W: %d\n", w_id); */

	for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {

        /* EXEC_SQL BEGIN */
        mysql_stmt = stmt[t_num][40];
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
        //printf("begin stmt id => %lu\n", mysql_stmt->stmt_id);
        //fflush(stdout);
  

	        proceed = 1;
		/*EXEC_SQL SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
		                FROM new_orders
		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][25];
        
        //printf("stmt[%d][25] stmt_id => %lu addr => %p\n",t_num, mysql_stmt->stmt_id, mysql_stmt);
        //fflush(stdout);

		memset(param, 0, sizeof(MYSQL_BIND) * 2); /* initialize */
		param[0].buffer_type = MYSQL_TYPE_LONGLONG;
		param[0].buffer = &d_id;
		param[1].buffer_type = MYSQL_TYPE_LONGLONG;
		param[1].buffer = &w_id;
        //printf("d_id => %d w_id => %d\n", d_id, w_id);
        //fflush(stdout);

		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        //printf("bind ==== stmt_id => %lu\n", mysql_stmt->stmt_id);
        //fflush(stdout);
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
        //printf("execute ==== stmt id => %lu\n", mysql_stmt->stmt_id);
        //fflush(stdout);

		memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize */
		column[0].buffer_type = MYSQL_TYPE_LONGLONG;
        column[0].buffer = &no_o_id;
		if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
        
        //printf("bind result==== stmt_id => %lu\n", mysql_stmt->stmt_id);
        //fflush(stdout);


		//if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
        //printf("store ==== stmt_id => %lu\n", mysql_stmt->stmt_id);
        //fflush(stdout);
        int ret = mysql_stmt_fetch(mysql_stmt);
        switch( ret ) {
            case 0: //SUCCESS
            break;
            case 1: //ERROR
            //printf("mysql_stmt_fetch => ERROR\n");
            //fflush(stdout);
            case MYSQL_NO_DATA: //NO MORE DATA
            //printf("mysql_stmt_fetch => NO_DATA\n");
            //fflush(stdout);
            default:
            //printf("stmt_id => %lu mysql_stmt_fetch => %d,  %p\n", mysql_stmt->stmt_id, ret, column[0].error);
            //fflush(stdout);
            mysql_stmt_free_result(mysql_stmt);
            //sleep(10000);
            goto sqlerr;
        }
        mysql_stmt_free_result(mysql_stmt);

        //sleep(600000);

        if(no_o_id == 0) continue;
        proceed = 2;
        /*EXEC_SQL DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
          AND no_w_id = :w_id;*/
        mysql_stmt = stmt[t_num][26];

        memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &no_o_id;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &d_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;


        proceed = 3;
        /*EXEC_SQL SELECT o_c_id INTO :c_id FROM orders
          WHERE o_id = :no_o_id AND o_d_id = :d_id
          AND o_w_id = :w_id;*/
        mysql_stmt = stmt[t_num][27];

        memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &no_o_id;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &d_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
        memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize */
        column[0].buffer_type = MYSQL_TYPE_LONG;
        column[0].buffer = &c_id;
        if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
        switch( mysql_stmt_fetch(mysql_stmt) ) {
            case 0: //SUCCESS
                break;
            case 1: //ERROR
            case MYSQL_NO_DATA: //NO MORE DATA
            default:
                mysql_stmt_free_result(mysql_stmt);
                goto sqlerr;
        }
        mysql_stmt_free_result(mysql_stmt);


        proceed = 4;
        /*EXEC_SQL UPDATE orders SET o_carrier_id = :o_carrier_id
          WHERE o_id = :no_o_id AND o_d_id = :d_id AND
          o_w_id = :w_id;*/
        mysql_stmt = stmt[t_num][28];

        memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &o_carrier_id;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &no_o_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &d_id;
        param[3].buffer_type = MYSQL_TYPE_LONG;
        param[3].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        proceed = 5;
        /*EXEC_SQL UPDATE order_line
          SET ol_delivery_d = :datetime
          WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
          ol_w_id = :w_id;*/ /* add "AND ol_number = ? "*/
        mysql_stmt = stmt[t_num][29];

        memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_STRING;
        param[0].buffer = datetime;
        param[0].buffer_length = strlen(datetime);
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &no_o_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &d_id;
        param[3].buffer_type = MYSQL_TYPE_LONG;
        param[3].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        proceed = 6;
        /*EXEC_SQL SELECT SUM(ol_amount) INTO :ol_total
          FROM order_line
          WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
          AND ol_w_id = :w_id;*/
        mysql_stmt = stmt[t_num][30];

        memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &no_o_id;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &d_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize */
        column[0].buffer_type = MYSQL_TYPE_FLOAT;
        column[0].buffer = &ol_total;
        if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
        if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
        switch( mysql_stmt_fetch(mysql_stmt) ) {
            case 0: //SUCCESS
            case MYSQL_DATA_TRUNCATED:
                break;
            case 1: //ERROR
            case MYSQL_NO_DATA: //NO MORE DATA
            default:
                mysql_stmt_free_result(mysql_stmt);
                goto sqlerr;
        }
        mysql_stmt_free_result(mysql_stmt);


        proceed = 7;
        /*EXEC_SQL UPDATE customer SET c_balance = c_balance + :ol_total ,
          c_delivery_cnt = c_delivery_cnt + 1
          WHERE c_id = :c_id AND c_d_id = :d_id AND
          c_w_id = :w_id;*/
        mysql_stmt = stmt[t_num][31];

        memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize */
        param[0].buffer_type = MYSQL_TYPE_FLOAT;
        param[0].buffer = &ol_total;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &c_id;
        param[2].buffer_type = MYSQL_TYPE_LONG;
        param[2].buffer = &d_id;
        param[3].buffer_type = MYSQL_TYPE_LONG;
        param[3].buffer = &w_id;
        if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        /*EXEC_SQL COMMIT WORK;*/
        //if( mysql_commit(ctx[t_num]) ) goto sqlerr;

        /* EXEC_SQL BEGIN */
        mysql_stmt = stmt[t_num][41];
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

        /* printf("D: %d, O: %d, time: %d\n", d_id, o_id, tad); */

    }
    /*EXEC_SQL COMMIT WORK;*/
    return (1);

sqlerr:
    fprintf(stderr, "ALERT: delivery %d:%d stmt_id %d\n",t_num,proceed, mysql_stmt->stmt_id);
    fflush(stderr);
    error(ctx[t_num],mysql_stmt);
    /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
    /*EXEC_SQL ROLLBACK WORK;*/
    //mysql_rollback(ctx[t_num]);
    /* EXEC_SQL ROLLBACK */
    mysql_stmt = stmt[t_num][42];
    //printf("rollback stmt_id is %d\n", mysql_stmt->stmt_id);
    //fflush(stdout);
    mysql_stmt_execute(mysql_stmt);
sqlerrerr:
    return (0);
}
