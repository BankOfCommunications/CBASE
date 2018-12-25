#ifdef RCSID
static char *RCSid =
   "$Header: copydata.c 2005.05.19 Lou Fangxin, http://www.anysql.net $ ";
#endif /* RCSID */

/*
   NAME
     copydata.c - Using OCI to rewrite the unload script.

   MODIFIED   (MM/DD/YY)
    Lou Fangxin    2005.05.19 -  Initial write.
    Lou Fangxin    2005.05.22 -  Add File Option to command
    Lou Fangxin    2005.05.25 -  Enable login as sysdba
*/

#if defined(_WIN32)
#include <winsock2.h>
#include <windows.h>
#include <process.h>
#else
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <mysql.h>

#include "anysqldes3.h"

/* Constants used in this program. */
#define MAX_SELECT_LIST_SIZE    1023
#define MAX_ITEM_BUFFER_SIZE      33
#define PARSE_NO_DEFER             0
#define PARSE_V7_LNG               2
#define MAX_BINDS                 12
#define MAX_SQL_IDENTIFIER        31
#define ROW_BATCH_SIZE        100000
#define UNLOAD_BUFFER_SIZE   4194304
#define IO_BUFFER_BITS            22
#define LOB_FILENAME_LENGTH       30
#define LONG_PIECE_SIZE       131072
#define MAX_BIND_ARRAY          4096

#if defined(_WIN32)
#define STRNCASECMP memicmp
#else
#define STRNCASECMP strncasecmp
#endif

#if !defined(MIN)
#define  MIN(a,b) ((a) > (b) ? (b) : (a))
#endif

#if !defined(MAX)
#define  MAX(a,b) ((a) < (b) ? (b) : (a))
#endif

#define OCI_SUCCESS 0
#define OCI_ERROR   1

#define DATA_COPY_INSERT 0
#define DATA_COPY_UPDATE 1
#define DATA_COPY_DELETE 2
#define DATA_COPY_INSUPD 3
#define DATA_COPY_ARRINS 4
#define DATA_COPY_ARRUPD 5
#define DATA_COPY_ARRREP 6

typedef struct _CMDLINE
{
  unsigned int  spos;
  unsigned int  epos;
  unsigned int  length;
  unsigned char buf[8192];
} CMDLINE;

struct COLUMN
{  
    /* Column Name */
    unsigned char            buf[64];
    unsigned short           buflen;

    unsigned int             dbtype;
    unsigned int             dsize;

    /*+ Fetch */
    unsigned char            *colbuf;
    long                     length[2000];
    my_bool                  is_null[2000];

    /*+ Point to next column */
    unsigned char            ispk;
    unsigned char 	     filler;
};

typedef struct _TABLEDATA
{
    int    rows;
    int    colcount;
    unsigned int    DEFAULT_ARRAY_SIZE;
    struct COLUMN *cols[MAX_SELECT_LIST_SIZE];
} TABLEDATA;

typedef struct _MYDATABASE
{
   MYSQL      *conn;
   unsigned int        errcode;
   unsigned char       errmsg[512];  
   unsigned char       charset[256];

   FILE       *fp_log;
   void      *context;
   #if defined(_WIN32)
   void (__stdcall *msgfunc) (void *p, unsigned char *buf);
   #else
   void  (*msgfunc) (void *p, unsigned char *buf);
   #endif
} MYDATABASE;

typedef struct _MYBINDLIST
{
    int size;
    int rows[MAX_BIND_ARRAY];
    int indx[MAX_BIND_ARRAY];
    long len[MAX_BIND_ARRAY];

    unsigned int    dbtype[MAX_BIND_ARRAY];
    unsigned char   *buf[MAX_BIND_ARRAY];
    long  buf_len[MAX_BIND_ARRAY];
    my_bool is_null[MAX_BIND_ARRAY];
} MYBINDLIST;

typedef struct _SQLPARAM
{
    unsigned int dbtype;
    char    pname[31];
    int     nlen;
    char    pval[256];
    int     vlen;
} SQLPARAM;

typedef struct _CMDOPTIONS
{
    unsigned char user1[132];
    unsigned char user2[132];
    unsigned char owner1[132];
    unsigned char table1[132];
    unsigned char table2[132];
    unsigned char query1[16384];
    unsigned char query2[16384];
    unsigned char delete[16384];
    unsigned char tables[8192];
    unsigned char pkcols[512];
    unsigned char cfcols[512];
    unsigned char filler[512];
    unsigned char logname[512];
    unsigned char charset[256];
    unsigned char tabsplit[132];
    unsigned char splitkey[132];
    
    int  crypt ;
    int  rows  ;
    int  commit;
    int  waittime;
    unsigned char  syncmode;
    int  rtncode;
    int  verbose;
    int  degree;
    int  slaveid;
    int  pretry;

    unsigned int DEFAULT_ARRAY_SIZE;
    unsigned int DEFAULT_LONG_SIZE;

    int  sqlargc;
    SQLPARAM  sqlargv[20];

    int  oerrcount;
    int  oerrorno[32];

   void *context;
   #if defined(_WIN32)
   void (__stdcall *msgfunc) (void *p, unsigned char *buf);
   #else
   void  (*msgfunc) (void *p, unsigned char *buf);
   #endif
} CMDOPTIONS;

typedef struct _OPTIONPOS
{
  unsigned char *name;
  int   nlen;
  unsigned char *val;
  int   vlen;
} OPTIONPOS;

typedef struct _ROWIDRANGE
{
  int id;
  unsigned char minrid[256];
  unsigned char maxrid[256];
} ROWIDRANGE;

typedef struct _TABLESPLIT
{
  int count;
  unsigned int dbtype;
  unsigned int dsize;
  ROWIDRANGE range[128];
  #if defined(_WIN32)
  CMDOPTIONS *opts[128];
  HANDLE hThread[128];
  unsigned threadID[128];
  #else
  pid_t thid[128];
  int   status[128];
  #endif

} TABLESPLIT;

void  initMYDB(MYDATABASE *db)
{
  db->conn=NULL;
  memset(db->charset,0,256);
  db->fp_log = stdout;
  db->context = NULL;
  db->msgfunc = NULL;
}


#if defined(_WIN32)
void __stdcall defaultMyMessageFunc(void *p, unsigned char *buf)
#else
void defaultMyMessageFunc(void *p, unsigned char *buf)
#endif
{
    MYDATABASE *db = (MYDATABASE *) p;
    if (db != NULL && db->fp_log != NULL) 
    {
        fprintf(db->fp_log, "%s\n", buf);
        fflush(db->fp_log);
    }
}

int getBestBatchSize(int batchsize, int maxlimit)
{
    int i, temp=0;
    int mincnt=maxlimit, bestbatch=maxlimit;

    if (batchsize > maxlimit) return maxlimit;
    for(i=batchsize;i>1;i--)
    {
        temp = maxlimit/i + maxlimit%i;
        if (temp < mincnt) { mincnt = temp; bestbatch = i; }
    }
    return bestbatch;
}

int scanNextString(CMDLINE *cmdline, char c)
{
  unsigned int i=cmdline->epos;
  if (cmdline->epos > 0)
  {
    i = cmdline->epos + 1;
  }
  cmdline->spos = i;
  while(i<cmdline->length && cmdline->buf[i]!=c) i++;
  cmdline->epos = i;
  return cmdline->epos - cmdline->spos;
}

int  myError8(MYDATABASE *db)
{
   const char *myerr=NULL;
   db->errcode=0;
   memset((void *) (db->errmsg), (int)'\0', (size_t)512);
   db->errcode = mysql_errno(db->conn);
   if (db->errcode)
   {
       myerr = mysql_error(db->conn);
       strncat(db->errmsg, myerr, MIN(strlen(myerr),511));
   }
   if (db->errcode)
   {
      if (db->msgfunc != NULL)
      {
          (*(db->msgfunc))(db->context, db->errmsg);
      }
   }
   return db->errcode;
}

MYSQL_STMT *myStmtPrepare(MYDATABASE *db, const char *sql, int sqllen)
{
   MYSQL_STMT *my_stmt1=NULL;

   my_stmt1 = mysql_stmt_init(db->conn);
   if (my_stmt1 != NULL)
   {
       if (mysql_stmt_prepare(my_stmt1, sql, sqllen) == 0)
       {
	   return my_stmt1;
	}
	else
	{
	   mysql_stmt_close(my_stmt1);
	   my_stmt1 = NULL;
	   myError8(db);
	   return NULL;
	}
   }
   return NULL;
}

int  myStmtError8(MYDATABASE *db, MYSQL_STMT *stmt)
{
   const char *myerr=NULL;
   db->errcode=0;
   memset((void *) (db->errmsg), (int)'\0', (size_t)512);
   db->errcode = mysql_stmt_errno(stmt);
   if (db->errcode)
   {
        myerr = mysql_stmt_error(stmt);
       	strncat(db->errmsg, myerr, MIN(strlen(myerr),511));
   } 
   if (db->errcode)
   {
      if (db->msgfunc != NULL)
      {
          (*(db->msgfunc))(db->context, db->errmsg);
      }
   }
   return db->errcode;
}

int disconnectMYDB(MYDATABASE *db)
{
    if(db->conn != NULL) mysql_close(db->conn);
    db->conn = NULL;
    return OCI_SUCCESS;
}


int LogonMYDB(MYDATABASE *db, char *o_connstr)
{
    int  i=0;
    int  pos=0;
    int  inquote=0;
    char o_user[132];
    char o_pass[132];
    char o_host[132];
    char o_port[132];
    char o_dbid[132];
    char tempbuf[256];
    memset(o_user,0,132);
    memset(o_pass,0,132);
    memset(o_host,0,132);
    memset(o_port,0,132);
    memset(o_dbid,0,132);
    memset(tempbuf,0,256);
   
    if (i < strlen(o_connstr))
    {
       while(i<strlen(o_connstr) && o_connstr[i] != '/' && (o_connstr[i] != '@' || inquote == 1))
       {
	   if (o_connstr[i] == '"')
           {
              if (inquote) inquote = 0  ;
              else inquote = 1;
           }
           i++; 
       }
       if (o_connstr[i]='/')
       {
          memcpy(o_user, o_connstr+pos, i-pos);
          pos = i + 1;
          i++;
          while(i<strlen(o_connstr) && (o_connstr[i] != '@' || inquote == 1))
          {
	     if (o_connstr[i] == '"')
             {
                if (inquote) inquote = 0  ;
                else inquote = 1;
             }
             i++; 
          }
          if (o_connstr[i]='@')
          {
             if (i>pos) memcpy(o_pass, o_connstr+pos, i-pos);
             pos = i + 1;
             i++;
             strcat(tempbuf, o_connstr + pos);
          }
          else  
          {
              strcat(o_pass, o_connstr + pos);
          }
       }
       else
       {
      	  if (o_connstr[i]='@')
      	  {
              strcat(o_user, o_connstr + pos);
              pos = i+ 1;
              strcat(tempbuf, o_connstr + pos);     	  	
      	  }
       }
    }
    i=0; 
    pos=0;
    while(tempbuf[i] && tempbuf[i] != ':') 
    {
       o_host[pos++] = tempbuf[i++];
    }
    if (tempbuf[i]) i++;
    pos=0;
    while(tempbuf[i] && tempbuf[i] != ':') 
    {
       o_port[pos++] = tempbuf[i++];
    }
    if (tempbuf[i]) i++;
    pos=0;
    while(tempbuf[i] && tempbuf[i] != ':') 
    {
       o_dbid[pos++] = tempbuf[i++];
    }

    db->conn = mysql_init(NULL);
    if (db->conn == NULL) return OCI_ERROR;
    if (db->charset[0])
	mysql_options(db->conn, MYSQL_SET_CHARSET_NAME, db->charset);
    if (!mysql_real_connect(db->conn, o_host,
        o_user, o_pass, o_dbid, atoi(o_port), NULL, CLIENT_MULTI_STATEMENTS)) 
    {
	myError8(db); 
        return OCI_ERROR;
    }
    memset(tempbuf,0,256);
    sprintf(tempbuf, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    mysql_query(db->conn, tempbuf);
    if (db->charset[0])
    {
       memset(tempbuf,0,256);
       sprintf(tempbuf, "set names %s", db->charset);
       mysql_query(db->conn, tempbuf);
    }

    /* mysql_autocommit(db->conn, 0);  */

    return OCI_SUCCESS;
}

int getAsciiHexValue(unsigned char c)
{
    if (c >= '0' && c <= '9') return c-'0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    return 0;
}

FILE *openFile(const unsigned char *fname)
{
   FILE *fp=NULL;
   int i, j, len;
   time_t now = time(0);
   struct tm *ptm = localtime(&now);   
   char tempbuf[512];
  
   len = strlen(fname);
   j = 0;
   for(i=0;i<len;i++)
   {
      if (*(fname+i) == '%')
      {
          i++;
	  if (i < len)
	  {
            switch(*(fname+i))
            {
              case 'Y':
              case 'y':
                j += sprintf(tempbuf+j, "%04d", ptm->tm_year + 1900);
		break;
              case 'M':
              case 'm':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mon + 1);
		break;
              case 'D':
              case 'd':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mday);
		break;
              case 'W':
              case 'w':
                j += sprintf(tempbuf+j, "%d", ptm->tm_wday);
		break;
              case 'T':
              case 't':
                j += sprintf(tempbuf+j, "%d", now);
		break;
              default:
                tempbuf[j++] = *(fname+i);
		break;
            }
          }
      }
      else
      {
         tempbuf[j++] = *(fname+i);
      }
   }
   tempbuf[j]=0;
   if (tempbuf[0] == '+')
       fp = fopen(tempbuf+1, "ab+");
   else
       fp = fopen(tempbuf, "wb+");
   return fp;
}

void readSQLFile(char *sqlfname, char *querybuf)
{
    FILE *fp;
    char tempbuf[1024];
    
    fp = fopen(sqlfname,"r+");
    if (fp != NULL)
    {
            while(!feof(fp))
            {
               memset(tempbuf,0,1024);
               fgets(tempbuf,1023,fp);
               strcat(querybuf,tempbuf);
            }
            fclose(fp);
    }
}

void  printRowInfo(MYDATABASE *db, unsigned int row)
{
        unsigned char tempbuf[512];
	time_t now = time(0);
	struct tm *ptm = localtime(&now);
	sprintf(tempbuf,"%10u rows processed at %04d-%02d-%02d %02d:%02d:%02d.",
                row,
		ptm->tm_year + 1900,
		ptm->tm_mon + 1,
		ptm->tm_mday,
		ptm->tm_hour,
		ptm->tm_min,
		ptm->tm_sec);
        if (db->msgfunc != NULL)
        {
          (*(db->msgfunc))(db->context, tempbuf);
        } 
}

int isSkipedError(CMDOPTIONS *cmdopts, int oerrn)
{
   int i=0;
   for(i=0;i<cmdopts->oerrcount;i++)
   {
       if (cmdopts->oerrorno[i] == oerrn) return 1;
   }
   return 0;
}

void sleepInterval(int interval)
{
    int i;
    for(i=0;i<interval;i++)
    {
        #if defined(_WIN32)
        _sleep(100);
        #else
        usleep(100000);
        #endif
    }
}

void freeTableData(TABLEDATA *data)
{
   int i;
   struct COLUMN *p;

   for(i=0;i<data->colcount;i++)
   {
     p=data->cols[i];
     if (p->colbuf != NULL) free(p->colbuf);
     free(p);
   }
}

int getMYColumnsNative(MYDATABASE *db, MYSQL_STMT *stmt, TABLEDATA *data, int maxrows, int maxlong)
{
    int totallen=0,i;
    CMDLINE cmdline;
    int col, colcount;
    struct COLUMN *tempcol;
    MYSQL_RES *my_meta=NULL;
    MYSQL_FIELD *my_col=NULL;

    data->colcount = 0;
    data->DEFAULT_ARRAY_SIZE = maxrows;

    my_meta = mysql_stmt_result_metadata(stmt);
    if (my_meta != NULL)
   {
	colcount = mysql_num_fields(my_meta);
	if (colcount == 0)
	{
	    return OCI_ERROR;
	}
    
        /* Describe the select-list items. */
                          
        for (col = 0; col < MIN(colcount,MAX_SELECT_LIST_SIZE); col++)
        {
	   my_col =  mysql_fetch_field(my_meta);

           tempcol = (struct COLUMN *) malloc(sizeof(struct COLUMN));
           tempcol->colbuf = NULL;
	   tempcol->ispk  = (unsigned char) 0;
	   tempcol->filler= (unsigned char) 0;
  	   data->cols[col] = tempcol;
	   data->colcount= col+1;

	   memset(tempcol->buf,0,64);
	   memcpy(tempcol->buf,my_col->name,MIN(63,strlen(my_col->name)));
           tempcol->buflen = MIN(63,strlen(my_col->name));

	   switch(my_col->type)
	   {
	      case MYSQL_TYPE_TINY:
	      case MYSQL_TYPE_SHORT:
	      case MYSQL_TYPE_INT24:
	      case MYSQL_TYPE_LONG:
		   tempcol->dbtype = MYSQL_TYPE_LONG;
		   tempcol->dsize = 4;
		   break;
	      case MYSQL_TYPE_LONGLONG:
		   tempcol->dbtype = MYSQL_TYPE_LONGLONG;
		   tempcol->dsize = 8;
		   break;
	      case MYSQL_TYPE_FLOAT:
		   tempcol->dbtype = MYSQL_TYPE_FLOAT;
		   tempcol->dsize = 4;
		   break;
	      case MYSQL_TYPE_DOUBLE:
	      case MYSQL_TYPE_DECIMAL:
	      case MYSQL_TYPE_NEWDECIMAL:
		   tempcol->dbtype = MYSQL_TYPE_DOUBLE;
		   tempcol->dsize = 8;
		   break;
	      case MYSQL_TYPE_TIME:
	      case MYSQL_TYPE_DATE:
	      case MYSQL_TYPE_DATETIME:
	      case MYSQL_TYPE_TIMESTAMP:
		   tempcol->dbtype = my_col->type;
		   tempcol->dsize = sizeof(MYSQL_TIME);
		   break;
	      case MYSQL_TYPE_YEAR:
	      case MYSQL_TYPE_STRING:
	      case MYSQL_TYPE_VAR_STRING:
		   tempcol->dbtype = MYSQL_TYPE_STRING;
		   tempcol->dsize = my_col->length;
		   if (tempcol->dsize > maxlong)
			tempcol->dsize = maxlong;
		   break;
	      case MYSQL_TYPE_BLOB:
		   tempcol->dbtype = MYSQL_TYPE_STRING;
		   tempcol->dsize  = my_col->length;
		   if (tempcol->dsize > maxlong)
			tempcol->dsize = maxlong;
		   break;
	      default:
		   tempcol->dbtype = my_col->type;
		   tempcol->dsize = my_col->length;
		   break;
	   }
	   totallen = totallen + tempcol->dsize;
	}
    }
    
    if (data->DEFAULT_ARRAY_SIZE > (1048576 * 2) / totallen)
    {
        data->DEFAULT_ARRAY_SIZE = (1048576 * 2) / totallen;
    }
    if (data->DEFAULT_ARRAY_SIZE > 2000)
    {
        data->DEFAULT_ARRAY_SIZE = 2000;
    }
    if (data->DEFAULT_ARRAY_SIZE < 2)
    {
        data->DEFAULT_ARRAY_SIZE = 2;
    }

    for(col=0; col < colcount; col++)
    {
        tempcol = data->cols[col];
	if (tempcol->colbuf == NULL)
	{
            tempcol->colbuf = malloc(data->DEFAULT_ARRAY_SIZE * (tempcol->dsize));
	    if (tempcol->colbuf == NULL)  return -1;
            memset(tempcol->colbuf,0,data->DEFAULT_ARRAY_SIZE * (tempcol->dsize));
	}
    }

    return OCI_SUCCESS;
}

TABLEDATA *queryData(MYDATABASE *db, unsigned char *query, int maxrows, int maxlong)
{
    int rtncode = 0, i;
    TABLEDATA    *data = NULL;
    MYSQL_STMT   *stmt=NULL;
    MYSQL_BIND   *pb=NULL, *rowbind=NULL;

    stmt = myStmtPrepare(db, query, strlen(query));
    if (stmt != NULL)
    {
        data = (TABLEDATA *) malloc(sizeof(TABLEDATA));
        if (data != NULL)
        {
           data->rows = 0;
           if (getMYColumnsNative(db,stmt,data,maxrows, maxlong) == 0) 
           {
               rowbind = (MYSQL_BIND *) malloc(data->colcount * sizeof(MYSQL_BIND));
               if (rowbind != NULL)
               {
             	   memset(rowbind,0, data->colcount * sizeof(MYSQL_BIND));
       	           for(i=0; i< data->colcount; i++)
	           {
		       pb = (rowbind + i);
 		       pb->buffer = (void *)(data->cols[i]->colbuf + (data->DEFAULT_ARRAY_SIZE - 1) * data->cols[i]->dsize);
		       pb->length = (long *)(&(data->cols[i]->length[data->DEFAULT_ARRAY_SIZE - 1]));
		       pb->is_null = (my_bool *)(&(data->cols[i]->is_null[data->DEFAULT_ARRAY_SIZE - 1])); 
	               pb->buffer_length = data->cols[i]->dsize;
		       pb->buffer_type   = data->cols[i]->dbtype;	
        	   }
                   rtncode = mysql_stmt_bind_result(stmt, rowbind);
                   if (rtncode  == 0) mysql_stmt_execute(stmt);
	           if (rtncode != 0) myStmtError8(db, stmt);                   
                   if (rtncode == 0) 
                   {
			data->rows = 0;
			while((rtncode=mysql_stmt_fetch(stmt))==0 && data->rows < data->DEFAULT_ARRAY_SIZE)		
			{
			     if (data->rows != data->DEFAULT_ARRAY_SIZE - 1)
			     {
				for(i=0;i<data->colcount;i++)
				{
				   pb = (rowbind + i);
				   data->cols[i]->is_null[data->rows] = data->cols[i]->is_null[data->DEFAULT_ARRAY_SIZE - 1];
				   data->cols[i]->length[data->rows]  = data->cols[i]->length[data->DEFAULT_ARRAY_SIZE - 1];
				   memcpy(data->cols[i]->colbuf + data->rows * data->cols[i]->dsize, 
					  data->cols[i]->colbuf + (data->DEFAULT_ARRAY_SIZE - 1) * data->cols[i]->dsize,
					  data->cols[i]->dsize);
				}
			    }
			    data->rows ++;
			    if(data->rows == data->DEFAULT_ARRAY_SIZE) break;
			 }
			 if (rtncode != 0 && rtncode != MYSQL_NO_DATA)	
			 {
			     myStmtError8(db,stmt);
			     myError8(db);
			 }
			 else
			 {
			     rtncode = 0;
			 }
                   }
	       }
               else
               {
                  free(data);
                  data = NULL;
               }
	   }
           else
           {
               free(data);
               data = NULL;
           }
        }
        else
        {
           free(data);
           data = NULL;
        }
    }
    if (rtncode != 0 && rtncode != MYSQL_NO_DATA)
    {
	if (data != NULL)
        {
           freeTableData(data);
           free(data);
        }
    }
    if (rowbind != NULL) free(rowbind);
    if (stmt != NULL) mysql_stmt_close(stmt);
    return data;
}

time_t  getMySQLTime(MYSQL_TIME *mtm)
{
   time_t now = time(0);
   struct tm *ptm = localtime(&now);
   ptm->tm_year = mtm->year - 1900;
   ptm->tm_mon = mtm->month - 1;
   ptm->tm_mday = mtm->day;
   ptm->tm_hour = mtm->hour;
   ptm->tm_min = mtm->minute;
   ptm->tm_sec = mtm->second;
   return mktime(ptm);
}

void  setMySQLTime(MYSQL_TIME *mtm, time_t utm)
{
   time_t now = utm;
   struct tm *ptm = localtime(&now);
   mtm->year = ptm->tm_year + 1900;
   mtm->month = ptm->tm_mon + 1;
   mtm->day = ptm->tm_mday;
   mtm->hour = ptm->tm_hour;
   mtm->minute = ptm->tm_min;
   mtm->second = ptm->tm_sec;
}

long mypower(long x, int y)
{
   int i;
   long val=1;
   for(i=0;i<y;i++) val = val * x;
   return val;
}

void splitTableRange(TABLESPLIT *tabsplit, MYDATABASE *db, unsigned char *tname, unsigned char *keycol, int splitcnt)
{
    int i,j, minlen=0, bitcnt=0, pos=0;
    char minsql[256], maxsql[256];
    unsigned char tempbuf[256], tempbuf2[256];
    TABLEDATA *data1=NULL, *data2=NULL;

    long  *l_minval, *l_maxval, l_temp, l_temp2;
    long long  *ll_minval, *ll_maxval, ll_temp, ll_temp2;    
    float *f_minval, *f_maxval, f_temp, f_temp2;
    double *d_minval, *d_maxval, d_temp, d_temp2;
    MYSQL_TIME *mt_minval, *mt_maxval, mt_temp;
    time_t     tm_minval, tm_maxval;

    tabsplit->count = 0;
    memset(minsql,0,256);
    memset(maxsql,0,256);
    sprintf(minsql, "SELECT %s FROM %s ORDER BY %s ASC LIMIT 1", keycol, tname, keycol);
    sprintf(maxsql, "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", keycol, tname, keycol);

    data1 = queryData(db, minsql, 2, 1024);
    data2 = queryData(db, maxsql, 2, 1024);

    if (data1 != NULL && data2 != NULL)
    {
        if (data1->rows == 1 && data2->rows == 1)
        {
            /* start to split it */
            switch(data1->cols[0]->dbtype)
            {
	      case MYSQL_TYPE_TINY:
	      case MYSQL_TYPE_SHORT:
	      case MYSQL_TYPE_INT24:
	      case MYSQL_TYPE_LONG:
                   tabsplit->dbtype = MYSQL_TYPE_LONG;
                   tabsplit->dsize = data1->cols[0]->dsize;
                   l_minval = (long *)(data1->cols[0]->colbuf);
                   l_maxval = (long *)(data2->cols[0]->colbuf);
                   l_temp = *l_maxval - *l_minval;
                   l_temp = l_temp / splitcnt;
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;
                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           l_temp2 = *l_minval + i * l_temp;
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, l_minval, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, &l_temp2, tabsplit->dsize);
                           l_temp2 = *l_minval + (i + 1) * l_temp;
                           memcpy(tabsplit->range[i].maxrid, &l_temp2, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           l_temp2 = *l_minval + i * l_temp;
                           memcpy(tabsplit->range[i].minrid, &l_temp2, tabsplit->dsize);
                           l_temp2 = *l_maxval + 1;
                           memcpy(tabsplit->range[i].maxrid, &l_temp2, tabsplit->dsize);
                       }
                   }
		   break;
	      case MYSQL_TYPE_LONGLONG:
                   tabsplit->dbtype = MYSQL_TYPE_LONGLONG;
                   tabsplit->dsize = data1->cols[0]->dsize;
                   ll_minval = (long long *)(data1->cols[0]->colbuf);
                   ll_maxval = (long long *)(data2->cols[0]->colbuf);
                   ll_temp = *ll_maxval - *ll_minval;
                   ll_temp = ll_temp / splitcnt;
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;
                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           ll_temp2 = *ll_minval + i * ll_temp;
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, ll_minval, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, &ll_temp2, tabsplit->dsize);
                           ll_temp2 = *ll_minval + (i + 1) * ll_temp;
                           memcpy(tabsplit->range[i].maxrid, &ll_temp2, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           ll_temp2 = *ll_minval + i * ll_temp;
                           memcpy(tabsplit->range[i].minrid, &ll_temp2, tabsplit->dsize);
                           ll_temp2 = *ll_maxval + 1;
                           memcpy(tabsplit->range[i].maxrid, &ll_temp2, tabsplit->dsize);
                       }
                   }
		   break;
	      case MYSQL_TYPE_FLOAT:
                   tabsplit->dbtype = MYSQL_TYPE_FLOAT;
                   tabsplit->dsize = data1->cols[0]->dsize;
                   f_minval = (float *)(data1->cols[0]->colbuf);
                   f_maxval = (float *)(data2->cols[0]->colbuf);
                   f_temp = *f_maxval - *f_minval;
                   f_temp = f_temp / splitcnt;
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;
                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           f_temp2 = *f_minval + i * f_temp;
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, f_minval, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, &f_temp2, tabsplit->dsize);
                           f_temp2 = *f_minval + (i + 1) * f_temp;
                           memcpy(tabsplit->range[i].maxrid, &f_temp2, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           f_temp2 = *f_minval + i * f_temp;
                           memcpy(tabsplit->range[i].minrid, &f_temp2, tabsplit->dsize);
                           f_temp2 = *f_maxval + 1.0f;
                           memcpy(tabsplit->range[i].maxrid, &f_temp2, tabsplit->dsize);
                       }
                   }
		   break;
	      case MYSQL_TYPE_DOUBLE:
	      case MYSQL_TYPE_DECIMAL:
	      case MYSQL_TYPE_NEWDECIMAL:
                   tabsplit->dbtype = MYSQL_TYPE_DOUBLE;
                   tabsplit->dsize = data1->cols[0]->dsize;
                   d_minval = (double *)(data1->cols[0]->colbuf);
                   d_maxval = (double *)(data2->cols[0]->colbuf);
                   d_temp = *d_maxval - *d_minval;
                   d_temp = d_temp / splitcnt;
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;
                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           d_temp2 = *d_minval + i * d_temp;
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, d_minval, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, &d_temp2, tabsplit->dsize);
                           d_temp2 = *d_minval + (i + 1) * d_temp;
                           memcpy(tabsplit->range[i].maxrid, &d_temp2, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           d_temp2 = *d_minval + i * d_temp;
                           memcpy(tabsplit->range[i].minrid, &d_temp2, tabsplit->dsize);
                           d_temp2 = *d_maxval + 1.0f;
                           memcpy(tabsplit->range[i].maxrid, &d_temp2, tabsplit->dsize);
                       }
                   }
		   break;
	      case MYSQL_TYPE_TIME:
	      case MYSQL_TYPE_DATE:
	      case MYSQL_TYPE_DATETIME:
	      case MYSQL_TYPE_TIMESTAMP:
                   tabsplit->dbtype = data1->cols[0]->dbtype;
                   tabsplit->dsize = data1->cols[0]->dsize;
                   mt_minval = (MYSQL_TIME *)(data1->cols[0]->colbuf);
                   mt_maxval = (MYSQL_TIME *)(data2->cols[0]->colbuf);
                   tm_minval = getMySQLTime(mt_minval);
                   tm_maxval = getMySQLTime(mt_maxval);
                   l_temp = tm_maxval - tm_minval;
                   l_temp = l_temp / splitcnt;
                   memcpy(&mt_temp, mt_minval, sizeof(MYSQL_TIME));
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;

                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           setMySQLTime(&mt_temp, tm_minval + i * l_temp);
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, mt_minval, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, &d_temp2, tabsplit->dsize);
                           setMySQLTime(&mt_temp, tm_minval + (i+1) * l_temp);
                           memcpy(tabsplit->range[i].maxrid, &mt_temp, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           setMySQLTime(&mt_temp, tm_minval + i * l_temp);
                           memcpy(tabsplit->range[i].minrid, &mt_temp, tabsplit->dsize);
                           setMySQLTime(&mt_temp, tm_maxval + 1000);
                           memcpy(tabsplit->range[i].maxrid, &mt_temp, tabsplit->dsize);
                       }
                   }
		   break;
	      case MYSQL_TYPE_STRING:
		   tabsplit->dbtype = data1->cols[0]->dbtype;
                   minlen = (data1->cols[0]->length[0] > data2->cols[0]->length[0] ? data2->cols[0]->length[0] : data1->cols[0]->length[0]);
                   memset(tempbuf,0,256);
                   l_temp = 0;
                   /* copy the same character */
                   for(i=0;i<minlen && i < 100;i++)
                   {
                       if (data1->cols[0]->colbuf[i] == data2->cols[0]->colbuf[i])
                           tempbuf[i] = data1->cols[0]->colbuf[i];
                       else
                           break;
                   }
                   if (data1->cols[0]->colbuf[i] == data2->cols[0]->colbuf[i]) i++;
                   while(i < minlen && i < 100 && l_temp < splitcnt)
                   {
                         l_temp = l_temp * splitcnt + (data2->cols[0]->colbuf[i] - data1->cols[0]->colbuf[i]);
                         bitcnt ++;
                   }
                   l_temp = l_temp / splitcnt;
                   memcpy(tempbuf2, tempbuf, 256);
                   tabsplit->dsize = strlen(tempbuf2) + bitcnt;
                   for(i=0; i<splitcnt;i++)
                   {
                       tabsplit->count = splitcnt;
                       tabsplit->range[i].id = i;
                       if (i < splitcnt - 1) 
                       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);

                           memcpy(tempbuf, tempbuf2, 256);
                           pos = strlen(tempbuf);
                           l_temp2 = i * l_temp;
                           for(j=bitcnt;j>0;j--)
                           {
                               tempbuf[pos] =  data1->cols[0]->colbuf[pos] + (unsigned char)(l_temp2 / mypower(splitcnt,(j - 1)));
                               pos ++;
                           }
                           if (i==0)
                              memcpy(tabsplit->range[i].minrid, data1->cols[0]->colbuf, tabsplit->dsize);
                           else
                              memcpy(tabsplit->range[i].minrid, tempbuf, tabsplit->dsize);

                           memcpy(tempbuf, tempbuf2, 256);
                           pos = strlen(tempbuf);
                           l_temp2 = (i+1) * l_temp;
                           for(j=bitcnt;j>0;j--)
                           {
                               tempbuf[pos] =  data1->cols[0]->colbuf[pos] + (unsigned char)(l_temp2 / mypower(splitcnt,(j - 1)));
                               pos ++;
                           }
                           memcpy(tabsplit->range[i].maxrid, tempbuf, tabsplit->dsize);
                       }
                       else
		       {
                           memset(tabsplit->range[i].minrid,0,256);
                           memset(tabsplit->range[i].maxrid,0,256);
                           memcpy(tempbuf, tempbuf2, 256);
                           pos = strlen(tempbuf);
                           l_temp2 = i * l_temp;
                           for(j=bitcnt;j>0;j--)
                           {
                               tempbuf[pos] =  data1->cols[0]->colbuf[pos] + (unsigned char)(l_temp2 / mypower(splitcnt,(j - 1)));
                               pos ++;
                           }
                           memcpy(tabsplit->range[i].minrid, tempbuf, tabsplit->dsize);
                           memcpy(tempbuf, tempbuf2, 256);
                           memcpy(tempbuf, data2->cols[0]->colbuf, strlen(tempbuf2));
                           pos = strlen(tempbuf);
                           l_temp2 = 1;
                           for(j=bitcnt;j>0;j--)
                           {
                               tempbuf[pos] =  data2->cols[0]->colbuf[pos] + (unsigned char)(l_temp2 / mypower(splitcnt,(j - 1)));
                               pos ++;
                           }
                           memcpy(tabsplit->range[i].maxrid, tempbuf, tabsplit->dsize);
                       }
                   }
		   break;
            }
        }
    }
    if (data1 != NULL) { freeTableData(data1); free(data1); }
    if (data2 != NULL) { freeTableData(data2); free(data2); }
}

int getMYColumns(MYDATABASE *db, MYSQL_STMT *stmt, TABLEDATA *data, CMDOPTIONS *cmdopts, int (*fsetcol) (struct COLUMN *col, unsigned long arrsize))
{
    int totallen=0,i;
    CMDLINE cmdline;
    int col, colcount;
    struct COLUMN *tempcol;
    MYSQL_RES *my_meta=NULL;
    MYSQL_FIELD *my_col=NULL;

    data->colcount = 0;
    data->DEFAULT_ARRAY_SIZE = cmdopts->DEFAULT_ARRAY_SIZE;

    my_meta = mysql_stmt_result_metadata(stmt);
    if (my_meta != NULL)
   {
	colcount = mysql_num_fields(my_meta);
	if (colcount == 0)
	{
	    return OCI_ERROR;
	}
    
        /* Describe the select-list items. */
                          
        for (col = 0; col < MIN(colcount,MAX_SELECT_LIST_SIZE); col++)
        {
	   my_col =  mysql_fetch_field(my_meta);

           tempcol = (struct COLUMN *) malloc(sizeof(struct COLUMN));
           tempcol->colbuf = NULL;
	   tempcol->ispk  = (unsigned char) 0;
	   tempcol->filler= (unsigned char) 0;
  	   data->cols[col] = tempcol;
	   data->colcount= col+1;

	   memset(tempcol->buf,0,64);
	   memcpy(tempcol->buf,my_col->name,MIN(63,strlen(my_col->name)));
           tempcol->buflen = MIN(63,strlen(my_col->name));

	   switch(my_col->type)
	   {
	      case MYSQL_TYPE_TINY:
		   tempcol->dbtype = MYSQL_TYPE_TINY;
		   tempcol->dsize = 1;
		   break;
	      case MYSQL_TYPE_SHORT:
		   tempcol->dbtype = MYSQL_TYPE_SHORT;
		   tempcol->dsize = 2;
		   break;
	      case MYSQL_TYPE_INT24:
	      case MYSQL_TYPE_LONG:
		   tempcol->dbtype = MYSQL_TYPE_LONG;
		   tempcol->dsize = 4;
		   break;
	      case MYSQL_TYPE_LONGLONG:
		   tempcol->dbtype = MYSQL_TYPE_LONGLONG;
		   tempcol->dsize = 8;
		   break;
	      case MYSQL_TYPE_FLOAT:
		   tempcol->dbtype = MYSQL_TYPE_FLOAT;
		   tempcol->dsize = 4;
		   break;
	      case MYSQL_TYPE_DOUBLE:
	      case MYSQL_TYPE_DECIMAL:
	      case MYSQL_TYPE_NEWDECIMAL:
		   tempcol->dbtype = MYSQL_TYPE_DOUBLE;
		   tempcol->dsize = 8;
		   break;
	      case MYSQL_TYPE_TIME:
	      case MYSQL_TYPE_DATE:
	      case MYSQL_TYPE_DATETIME:
	      case MYSQL_TYPE_TIMESTAMP:
		   tempcol->dbtype = my_col->type;
		   tempcol->dsize = sizeof(MYSQL_TIME);
		   break;
	      case MYSQL_TYPE_YEAR:
		   tempcol->dbtype = MYSQL_TYPE_YEAR;
		   tempcol->dsize = 4;
		   break;
	      case MYSQL_TYPE_STRING:
	      case MYSQL_TYPE_VAR_STRING:
		   tempcol->dbtype = MYSQL_TYPE_STRING;
		   tempcol->dsize = my_col->length;
		   if (tempcol->dsize > cmdopts->DEFAULT_LONG_SIZE)
			tempcol->dsize = cmdopts->DEFAULT_LONG_SIZE;
		   break;
	      case MYSQL_TYPE_BLOB:
		   tempcol->dbtype = MYSQL_TYPE_STRING;
		   tempcol->dsize  = my_col->length;
		   if (tempcol->dsize > cmdopts->DEFAULT_LONG_SIZE)
			tempcol->dsize = cmdopts->DEFAULT_LONG_SIZE;
		   break;
	      default:
		   tempcol->dbtype = my_col->type;
		   tempcol->dsize = my_col->length;
		   break;
	   }
	   totallen = totallen + tempcol->dsize;
	}
    }
    
    if (data->DEFAULT_ARRAY_SIZE > (20971520 * 2) / totallen)
    {
        data->DEFAULT_ARRAY_SIZE = (20971520 * 2) / totallen;
    }
    if (data->DEFAULT_ARRAY_SIZE > 2000)
    {
        data->DEFAULT_ARRAY_SIZE = 2000;
    }
    if (data->DEFAULT_ARRAY_SIZE < 2)
    {
        data->DEFAULT_ARRAY_SIZE = 2;
    }

    /*
    col = MAX_BIND_ARRAY/data->colcount;
    col = data->DEFAULT_ARRAY_SIZE/col;
    if (col > 0) data->DEFAULT_ARRAY_SIZE = col * col;
    */

    for(col=0; col < colcount; col++)
    {
        tempcol = data->cols[col];
	if (fsetcol != NULL)
	{
	    (*fsetcol)(tempcol, data->DEFAULT_ARRAY_SIZE);
	}

	if (tempcol->colbuf == NULL)
	{
            tempcol->colbuf = malloc(data->DEFAULT_ARRAY_SIZE * (tempcol->dsize));
	    if (tempcol->colbuf == NULL)  return -1;
            memset(tempcol->colbuf,0,data->DEFAULT_ARRAY_SIZE * (tempcol->dsize));
	}
    }

    if (strlen(cmdopts->pkcols))
    {
      memset(cmdline.buf, 0 ,8192);
      cmdline.length = strlen(cmdopts->pkcols);
      memcpy(cmdline.buf, cmdopts->pkcols, cmdline.length);
      cmdline.spos=cmdline.epos=0;

      while(scanNextString(&cmdline,',') > 0)
      {
	for(i=0;i<data->colcount;i++)
	{
	   if(STRNCASECMP(data->cols[i]->buf,cmdline.buf + cmdline.spos, 
                cmdline.epos - cmdline.spos) == 0 &&
	        data->cols[i]->buflen == cmdline.epos - cmdline.spos)
	   {
		data->cols[i]->ispk = (unsigned char)1;
	   }
	}
      }
    }   

    if (strlen(cmdopts->filler))
    {
      memset(cmdline.buf, 0 ,8192);
      cmdline.length = strlen(cmdopts->filler);
      memcpy(cmdline.buf, cmdopts->filler, cmdline.length);
      cmdline.spos=cmdline.epos=0;

      while(scanNextString(&cmdline,',') > 0)
      {
	for(i=0;i<data->colcount;i++)
	{
	   if(STRNCASECMP(data->cols[i]->buf,cmdline.buf + cmdline.spos, 
                cmdline.epos - cmdline.spos) == 0 &&
	        data->cols[i]->buflen == cmdline.epos - cmdline.spos)
	   {
		data->cols[i]->filler = (unsigned char)1;
	   }
	}
      }
    }  

    return OCI_SUCCESS;
}

int getInsertSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0, col=0, pos=0, cpos=0;

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  INSERT INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    cpos = 0;
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
	    bind->rows[bind->size] = 0;
	    bind->size ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");
    return pos;
}

int getArrayInsertSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0,j=0, col=0, pos=0, cpos=0, arrp=0;

    arrp=MAX_BIND_ARRAY/data->colcount;
    if (arrp <= 0) arrp=1;
    arrp = getBestBatchSize(arrp, data->DEFAULT_ARRAY_SIZE);

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  INSERT INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    for(j=0;j<arrp;j++)
    {
       if (j) pos = pos + sprintf(sqlbuf+pos, "), (");
       cpos = 0;
       for(col=0;col<data->colcount;col++)
       {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
	    bind->rows[bind->size] = j;
	    bind->size ++;
	 }
       } 
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");

    return pos;
}

int getReplaceSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0, col=0, pos=0, cpos=0;

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  REPLACE INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    cpos = 0;
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
	    bind->rows[bind->size] = 0;
	    bind->size ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");
    return pos;
}

int getArrayReplaceSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0,j=0, col=0, pos=0, cpos=0, arrp=0;

    arrp=MAX_BIND_ARRAY/data->colcount;
    if (arrp <= 0) arrp=1;
    arrp = getBestBatchSize(arrp, data->DEFAULT_ARRAY_SIZE);

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  REPLACE INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    for(j=0;j<arrp;j++)
    {
       if (j) pos = pos + sprintf(sqlbuf+pos, "), (");
       cpos = 0;
       for(col=0;col<data->colcount;col++)
       {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
	    bind->rows[bind->size] = j;
	    bind->size ++;
	 }
       } 
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");

    return pos;
}


int getUpdateSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, char *cfcols, MYBINDLIST *bind)
{
    char tempbuf[32];
    CMDLINE cmdline;
    struct COLUMN *tempcol;
    int i, col=0, pos=0;

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  UPDATE %s SET ", tname);
    i = 0;
    for(col=0;col<data->colcount;col++)
    {
  	 tempcol = data->cols[col];
	 if (tempcol->ispk == 0)
	 {
	    if (tempcol->filler == 0)
	    {
	        if (i) pos = pos + sprintf(sqlbuf + pos, ",");
	        if (i%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
                pos = pos + sprintf(sqlbuf + pos, "%s=?",  tempcol->buf);
	        i ++;
  	        bind->indx[bind->size] = col;
 	        bind->rows[bind->size] = 0;
	        bind->size ++;
	    }
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, "\n  WHERE ");
    i = 0;
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->ispk == 1)
	 {
	    if (tempcol->filler == 0)
	    {
	       if (i) pos = pos + sprintf(sqlbuf + pos, " AND ");
	       if (i%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
               pos = pos + sprintf(sqlbuf + pos, "%s=?",  tempcol->buf);
	       i++;
	       bind->indx[bind->size] = col;
 	       bind->rows[bind->size] = 0;
	       bind->size ++;
	    }
	 }
    } 

    if (strlen(cfcols))
    {
       memset(cmdline.buf, 0 ,8192);
       cmdline.length = strlen(cfcols);
       memcpy(cmdline.buf, cfcols, cmdline.length);
       cmdline.spos=cmdline.epos=0;
       while((col=scanNextString(&cmdline,',')) > 0)
       {
	    memset(tempbuf,0,32);
	    memcpy(tempbuf,cmdline.buf + cmdline.spos, MIN(cmdline.epos - cmdline.spos, 30));
	    pos = pos + sprintf(sqlbuf + pos, " AND ");
	    if (i%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s <= ?", tempbuf);
	    i++;
	    bind->indx[bind->size] = col;
 	    bind->rows[bind->size] = 0;
	    bind->size ++;
       }
    }   
    return pos;
}

int getDeleteSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, char *cfcols, MYBINDLIST *bind)
{
    char tempbuf[32];
    CMDLINE cmdline;
    struct COLUMN *tempcol;
    int i, col=0, pos=0;

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  DELETE FROM %s WHERE ", tname);
    i = 0;
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->ispk)
	 {
	    if (i) pos = pos + sprintf(sqlbuf + pos, " AND ");
	    if (i%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s=?",  tempcol->buf);
	    i++;
	    bind->indx[bind->size] = col;
 	    bind->rows[bind->size] = 0;
	    bind->size ++;
	 }
    } 

    if (strlen(cfcols))
    {
       memset(cmdline.buf, 0 ,8192);
       cmdline.length = strlen(cfcols);
       memcpy(cmdline.buf, cfcols, cmdline.length);
       cmdline.spos=cmdline.epos=0;
       while((col=scanNextString(&cmdline,',')) > 0)
       {
	    memset(tempbuf,0,32);
	    memcpy(tempbuf,cmdline.buf + cmdline.spos, MIN(cmdline.epos - cmdline.spos, 30));
	    pos = pos + sprintf(sqlbuf + pos, " AND ");
	    if (i%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s <= ?", tempbuf);
	    i++;
	    bind->indx[bind->size] = col;
 	    bind->rows[bind->size] = 0;
	    bind->size ++;
       }
    }   
    return pos;
}

int getInsertFirstSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, char *cfcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0, col=0, pos=0, cpos=0;

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  INSERT INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    cpos = 0;
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ", ");
            if (cpos%10 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
 	    bind->rows[bind->size] = 0;
	    bind->size ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");
    pos = pos + sprintf(sqlbuf+pos, " ON DUPLICATE KEY UPDATE ");

    i = 0;
    for(col=0;col<data->colcount;col++)
    {
  	 tempcol = data->cols[col];
	 if (tempcol->ispk == 0)
	 {
	    if (tempcol->filler == 0)
	    {
	        if (i) pos = pos + sprintf(sqlbuf + pos, ", ");
	        if (i%3 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
                pos = pos + sprintf(sqlbuf + pos, "%s=VALUES(%s)",  tempcol->buf, tempcol->buf);
	        i ++;
	    }
	 }
    } 
	
    return pos;
}

int getArrayInsertFirstSQL(TABLEDATA *data, char *sqlbuf, char *tname, char *pkcols, char *cfcols, MYBINDLIST *bind)
{
    struct COLUMN *tempcol;
    int i=0,j=0, col=0, pos=0, cpos=0, arrp=0;

    arrp=MAX_BIND_ARRAY/data->colcount;
    if (arrp <= 0) arrp=1;
    arrp = getBestBatchSize(arrp, data->DEFAULT_ARRAY_SIZE);

    bind->size = 0;
    pos = pos + sprintf(sqlbuf+pos, "  INSERT INTO %s (", tname);
    for(col=0;col<data->colcount;col++)
    {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "%s",  tempcol->buf);
	    cpos ++;
	 }
    } 
    pos = pos + sprintf(sqlbuf+pos, ") VALUES (");
    for(j=0;j<arrp;j++)
    {
       if (j) pos = pos + sprintf(sqlbuf+pos, "), (");
       cpos = 0;
       for(col=0;col<data->colcount;col++)
       {
	 tempcol = data->cols[col];
	 if (tempcol->filler == 0)
	 {
	    if (cpos) pos = pos + sprintf(sqlbuf + pos, ",");
            if (cpos%5 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
            pos = pos + sprintf(sqlbuf + pos, "?");
	    cpos ++;
	    bind->indx[bind->size] = col;
	    bind->rows[bind->size] = j;
	    bind->size ++;
	 }
       } 
    } 
    pos = pos + sprintf(sqlbuf+pos, ")");
    pos = pos + sprintf(sqlbuf+pos, " ON DUPLICATE KEY UPDATE ");

    i = 0;
    for(col=0;col<data->colcount;col++)
    {
  	 tempcol = data->cols[col];
	 if (tempcol->ispk == 0)
	 {
	    if (tempcol->filler == 0)
	    {
	        if (i) pos = pos + sprintf(sqlbuf + pos, ", ");
	        if (i%3 == 0) pos = pos + sprintf(sqlbuf + pos, "\n    ");
                pos = pos + sprintf(sqlbuf + pos, "%s=VALUES(%s)",  tempcol->buf, tempcol->buf);
	        i ++;
	    }
	 }
    } 
	
    return pos;
}

void getMySQLQueryParams2(CMDOPTIONS *data, char *query, MYBINDLIST *bind)
{
    int i;
    struct COLUMN *tempcol;
    char *ptr, *ptr2, *p;
    unsigned char inquote;
    char tempbuf[256];
    char sqlquery[16384];

    memset(sqlquery,0,16384);
    inquote = 0;
    ptr = query;
    p=sqlquery;
    bind->size = 0;

    while(*ptr)
    {
        if (inquote == 1) /* In Single Quote */
        {
	    if (*ptr == '\'') inquote=0;
	    *p++=*ptr;
	    ptr ++;
        }
        else if (inquote == 2) /* In Double Quote */
        {
	    if (*ptr == '"') inquote=0;
	    *p++=*ptr;
	    ptr ++;
        }
        else
        {
             if (*ptr == '\'') 
             {
		inquote = 1;
   	        *p++=*ptr;
		ptr++;
             }
             else if (*ptr == '"') 
             {
		inquote = 2;
	        *p++=*ptr;
		ptr++;
             }
             else if (*ptr == ':') 
             {
		ptr++;
		ptr2 = ptr;
		/* Get Whole Variable Name */
		if (*ptr2=='_' || isalpha(*ptr2))
		{
		    while(*ptr2=='_' || isdigit(*ptr2) || isalpha(*ptr2)) ptr2++;
	    	    *p++='?';
		    memset(tempbuf,0,256);
		    memcpy(tempbuf,ptr,ptr2-ptr);
		    for(i=0;i<data->sqlargc;i++)
		    {
		        if(data->sqlargv[i].nlen == strlen(tempbuf) && STRNCASECMP(data->sqlargv[i].pname,tempbuf,data->sqlargv[i].nlen) == 0)
			{
			    bind->indx[bind->size] = i;
			    bind->rows[bind->size] = 0;
			    bind->size ++;
			}			
		    }
		}
		ptr = ptr2;
             }
	     else
	     {
		*p++=*ptr;
                ptr++;
	     }
        }
    }
    memcpy(query, sqlquery, 16384);
}

void getMySQLQueryParams(TABLEDATA *data, unsigned char *query, MYBINDLIST *bind)
{
    int i;
    struct COLUMN *tempcol;
    char *ptr, *ptr2, *p;
    unsigned char inquote;
    char tempbuf[256];
    char sqlquery[16384];

    memset(sqlquery,0,16384);
    inquote = 0;
    ptr = query;
    p=sqlquery;
    bind->size = 0;

    while(*ptr)
    {
        if (inquote == 1) /* In Single Quote */
        {
	    if (*ptr == '\'') inquote=0;
	    *p++=*ptr;
	    ptr ++;
        }
        else if (inquote == 2) /* In Double Quote */
        {
	    if (*ptr == '"') inquote=0;
	    *p++=*ptr;
	    ptr ++;
        }
        else
        {
             if (*ptr == '\'') 
             {
		inquote = 1;
   	        *p++=*ptr;
		ptr++;
             }
             else if (*ptr == '"') 
             {
		inquote = 2;
	        *p++=*ptr;
		ptr++;
             }
             else if (*ptr == ':') 
             {
		ptr++;
		ptr2 = ptr;
		/* Get Whole Variable Name */
		if (*ptr2=='_' || isalpha(*ptr2))
		{
		    while(*ptr2=='_' || isdigit(*ptr2) || isalpha(*ptr2)) ptr2++;
	    	    *p++='?';
		    memset(tempbuf,0,256);
		    memcpy(tempbuf,ptr,ptr2-ptr);
		    for(i=0;i<data->colcount;i++)
		    {
			tempcol = data->cols[i];
		        if(strlen(tempcol->buf) == strlen(tempbuf) && STRNCASECMP(tempcol->buf,tempbuf,strlen(tempbuf)) == 0)
			{
			    bind->indx[bind->size] = i;
			    bind->rows[bind->size] = 0;
			    bind->size ++;
			}			
		    }
		}
		ptr = ptr2;
             }
	     else
	     {
		*p++=*ptr;
                ptr++;
	     }
        }
    }
    memcpy(query, sqlquery, 16384);
}

int copyDataByQuery(MYDATABASE *db1, MYDATABASE *db2, CMDOPTIONS *cmdopts)
{
   int i=0,j=0,colcount=0, rowsize=0, pos=0, rtncode=0, trybatchrow=0;
   MYSQL_STMT    *my_stmt1=NULL;
   MYSQL_RES     *my_row=NULL, *my_meta=NULL;
   MYSQL_FIELD   *my_col=NULL;
   TABLEDATA     *data=NULL;

   unsigned long      totalrows = 0, commitrows=0, row=0;
   unsigned long      batchcnt = 0;

   MYBINDLIST    parambind, binddel, mybind, arrmybind;
   MYSQL_STMT    *delstmt=NULL, *stmt=NULL, *arrstmt=NULL;
   MYSQL_BIND    *delbind=NULL, *rowbind=NULL, *bindarr=NULL, *paramarr=NULL, *pb=NULL, *arrbindarr=NULL;
   my_bool  is_null=0;
   unsigned char     arrquery2[65536], msgbuf[512];
   int      exitdump = 1;

   unsigned long st_type;
   unsigned long st_prefetch_rows = 2000;


   st_type = (unsigned long) CURSOR_TYPE_NO_CURSOR;

   mybind.size = 0;
   arrmybind.size = 0;
   binddel.size = 0;
   parambind.size = 0;
   memset(arrquery2,0,65536);

   data = (TABLEDATA *) malloc (sizeof(TABLEDATA)); 
   if (data == NULL)
   {
   	return OCI_ERROR;	
   }
   data->colcount = 0;

   if (strlen(cmdopts->query1) == 0)
   {
	sprintf(cmdopts->query1, "select * from %s", cmdopts->table1);
   }   
   getMySQLQueryParams2(cmdopts, cmdopts->query1, &parambind);

   my_stmt1 = myStmtPrepare(db1, cmdopts->query1, strlen(cmdopts->query1));
   if (my_stmt1 != NULL)
   {
	/*
        memset(msgbuf,0,512);
        sprintf(msgbuf,"SET GLOBAL innodb_old_blocks_time=1000");
        mysql_query(db1->conn, msgbuf);
	*/

        if (parambind.size > 0)
        {
	      for(i=0;i<parambind.size;i++)
	      {
        	   parambind.buf_len[i] = 256;
		   parambind.len[i] = cmdopts->sqlargv[parambind.indx[i]].vlen;
        	   parambind.is_null[i] = 0;
		   parambind.buf[i] = cmdopts->sqlargv[parambind.indx[i]].pval;
                   parambind.dbtype[i] = cmdopts->sqlargv[parambind.indx[i]].dbtype;
	      }
              paramarr = (MYSQL_BIND *) malloc( parambind.size * sizeof(MYSQL_BIND));
              if (paramarr != NULL)
              {
	         memset(paramarr,0, parambind.size * sizeof(MYSQL_BIND));
	         for(i=0;i<parambind.size;i++)
	         {
		    pb = (paramarr + i);
		    pb->buffer = (void *)(parambind.buf[i]);
		    pb->length = (long *)(&(parambind.len[i]));
		    pb->is_null = &(parambind.is_null[i]); 
        	    pb->buffer_length = parambind.buf_len[i];
		    pb->buffer_type   = parambind.dbtype[i];
	         }
                 mysql_stmt_bind_param(my_stmt1, paramarr);
              }
        }

        if (getMYColumns(db1,my_stmt1,data, cmdopts, NULL) == 0)
        {
             if (strlen(cmdopts->query2) == 0)
             {
		if (cmdopts->syncmode == (unsigned char)DATA_COPY_ARRINS)
		{
		   getInsertSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, &mybind);
		   getArrayInsertSQL(data, arrquery2, cmdopts->table2, cmdopts->pkcols, &arrmybind);
		}
		else if (cmdopts->syncmode == (unsigned char)DATA_COPY_ARRREP)
		{
		   getReplaceSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, &mybind);
		   getArrayReplaceSQL(data, arrquery2, cmdopts->table2, cmdopts->pkcols, &arrmybind);
		}
		else if (cmdopts->syncmode == (unsigned char)DATA_COPY_ARRUPD)
		{
		   getInsertFirstSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, cmdopts->cfcols, &mybind);
		   getArrayInsertFirstSQL(data, arrquery2, cmdopts->table2, cmdopts->pkcols, cmdopts->cfcols, &arrmybind);
		}
		else if (cmdopts->syncmode == (unsigned char)DATA_COPY_INSUPD)
		{
		   getInsertFirstSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, cmdopts->cfcols, &mybind);
		}
		else if (cmdopts->syncmode == (unsigned char)DATA_COPY_UPDATE)
		{
		   getUpdateSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, cmdopts->cfcols, &mybind);
		}
		else if (cmdopts->syncmode == (unsigned char)DATA_COPY_DELETE)
		{
		   getDeleteSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, cmdopts->cfcols, &mybind);
		}
	        else
        	{
		   getInsertSQL(data, cmdopts->query2, cmdopts->table2, cmdopts->pkcols, &mybind);
		}
	    }
	    else
	    {
	       getMySQLQueryParams(data, cmdopts->query2, &mybind);
   	    }

	    if (cmdopts->delete[0])
	    {
		getMySQLQueryParams(data, cmdopts->delete, &binddel);
            }

	    batchcnt= cmdopts->rows / data->DEFAULT_ARRAY_SIZE;
            printRowInfo(db1,totalrows);
	    if (st_prefetch_rows > data->DEFAULT_ARRAY_SIZE) 
		st_prefetch_rows = data->DEFAULT_ARRAY_SIZE;
   	    mysql_stmt_attr_set(my_stmt1, STMT_ATTR_CURSOR_TYPE,   (void*) &st_type);
	    mysql_stmt_attr_set(my_stmt1, STMT_ATTR_PREFETCH_ROWS, (void*) &st_prefetch_rows);

	    if (cmdopts->verbose)
	    {
	        printf("%s\n", cmdopts->query2);
	        if (arrquery2[0]) printf("%s\n", arrquery2);
	    }

	    stmt = myStmtPrepare(db2, cmdopts->query2, strlen(cmdopts->query2));
	    if (stmt == NULL) 	   rtncode = OCI_ERROR;
	    if (rtncode == OCI_SUCCESS && arrquery2[0])
	    {
		arrstmt = myStmtPrepare(db2, arrquery2, strlen(arrquery2));
  	        if (arrstmt == NULL) 	   rtncode = OCI_ERROR;
	    }
	    if (cmdopts->delete[0])
	    {
		delstmt = myStmtPrepare(db1, cmdopts->delete, strlen(cmdopts->delete));
            }

	    if (rtncode == OCI_SUCCESS && stmt != NULL && (arrquery2[0] == (char)0 || (arrquery2[0] && arrstmt != NULL)))
	    {
	       for(i=0;i<mybind.size;i++)
	       {
        	   mybind.buf_len[i] = data->cols[mybind.indx[i]]->dsize;
		   mybind.len[i] = 0;
        	   mybind.is_null[i] = 0;
		   mybind.buf[i] = data->cols[mybind.indx[i]]->colbuf;
	       }
	       if (arrquery2[0])
	       {
		   cmdopts->rows = (cmdopts->rows/(arrmybind.size/data->colcount)) * (arrmybind.size/data->colcount);
	           for(i=0;i<arrmybind.size;i++)
        	   {
	               arrmybind.buf_len[i] = data->cols[arrmybind.indx[i]]->dsize;
		       arrmybind.len[i] = 0;
	               arrmybind.is_null[i] = 0;
        	       arrmybind.buf[i] = data->cols[arrmybind.indx[i]]->colbuf + arrmybind.rows[i] * data->cols[arrmybind.indx[i]]->dsize;    
	           }
	       }
	       if (cmdopts->delete[0])
	       {
		   for(i=0;i<binddel.size;i++)
		   {
        	      binddel.buf_len[i] = data->cols[binddel.indx[i]]->dsize;
		      binddel.len[i] = 0;
        	      binddel.is_null[i] = 0;
		      binddel.buf[i] = data->cols[binddel.indx[i]]->colbuf;
                   }
		   delbind = (MYSQL_BIND *) malloc(binddel.size * sizeof(MYSQL_BIND));
		   if (delbind != NULL) memset(delbind,0, binddel.size * sizeof(MYSQL_BIND));
               }

	      bindarr = (MYSQL_BIND *) malloc( mybind.size * sizeof(MYSQL_BIND));
	      if (bindarr != NULL)
	      {
	          memset(bindarr,0, mybind.size * sizeof(MYSQL_BIND));
	          if (arrquery2[0] && arrstmt != NULL)
	          {
	             arrbindarr = (MYSQL_BIND *) malloc( arrmybind.size * sizeof(MYSQL_BIND));
		     if (arrbindarr != NULL)
	                memset(arrbindarr,0, arrmybind.size * sizeof(MYSQL_BIND));
                     else
                        rtncode = OCI_ERROR;
	          }
              }
	      else
	      {
                  rtncode = OCI_ERROR;
              }

	      if (rtncode == OCI_SUCCESS)
              {
	         for(i=0;i<mybind.size;i++)
	         {
		    pb = (bindarr + i);
		    pb->buffer = (void *)(mybind.buf[i]);
		    pb->length = (long *)(&(mybind.len[i]));
		    pb->is_null = &(mybind.is_null[i]); 
        	    pb->buffer_length = mybind.buf_len[i];
		    pb->buffer_type   = data->cols[mybind.indx[i]]->dbtype;
	         }
	         rtncode = mysql_stmt_bind_param(stmt, bindarr); 
              }

	      if (rtncode == OCI_SUCCESS && arrquery2[0])      
      	      {
	          for(i=0;i<arrmybind.size;i++)
        	  {
		      pb = (arrbindarr + i);
		      pb->buffer = (void *)(arrmybind.buf[i]);
		      pb->length = (long *)(&(arrmybind.len[i]));
		      pb->is_null = &(arrmybind.is_null[i]); 
	              pb->buffer_length = arrmybind.buf_len[i];
		      pb->buffer_type   = data->cols[arrmybind.indx[i]]->dbtype;
	          }
        	  rtncode = mysql_stmt_bind_param(arrstmt, arrbindarr); 
	      }

	      if (cmdopts->delete[0] && delstmt != NULL && delbind != NULL)
	      {
	         for(i=0;i<binddel.size;i++)
	         {
		   pb = (delbind + i);
		   pb->buffer = (void *)(binddel.buf[i]);
		   pb->length = (long *)(&(binddel.len[i]));
		   pb->is_null = &(binddel.is_null[i]); 
        	   pb->buffer_length = binddel.buf_len[i];
		   pb->buffer_type   = data->cols[binddel.indx[i]]->dbtype;
	         }
	         mysql_stmt_bind_param(delstmt, delbind); 		  
              }

	      rowbind = (MYSQL_BIND *) malloc(data->colcount * sizeof(MYSQL_BIND));
	      if (rowbind != NULL)
	      {
	          memset(rowbind,0, data->colcount * sizeof(MYSQL_BIND));
	          for(i=0; i< data->colcount; i++)
	          {
		     pb = (rowbind + i);
 		     pb->buffer = (void *)(data->cols[i]->colbuf + (data->DEFAULT_ARRAY_SIZE - 1) * data->cols[i]->dsize);
		     pb->length = (long *)(&(data->cols[i]->length[data->DEFAULT_ARRAY_SIZE - 1]));
		     pb->is_null = (my_bool *)(&(data->cols[i]->is_null[data->DEFAULT_ARRAY_SIZE - 1])); 
        	     pb->buffer_length = data->cols[i]->dsize;
		     pb->buffer_type   = data->cols[i]->dbtype;	
                  }
              }
	      else
	      {
	          rtncode = OCI_ERROR;
              }

	      if (rtncode == OCI_SUCCESS && (rtncode = mysql_stmt_bind_result(my_stmt1, rowbind)) == 0)
	      {
		      if (rtncode == OCI_SUCCESS && (rtncode = mysql_stmt_execute(my_stmt1)) == 0)
		      {
			      for (;;)
			      {
			        exitdump = 0;
				data->rows = 0;
				/* Fetch mysql into data array */
				while((i=mysql_stmt_fetch(my_stmt1))==0)		
				{
				    if (data->rows != data->DEFAULT_ARRAY_SIZE - 1)
				    {
					for(i=0;i<data->colcount;i++)
					{
					   pb = (rowbind + i);
					   data->cols[i]->is_null[data->rows] = data->cols[i]->is_null[data->DEFAULT_ARRAY_SIZE - 1];
					   data->cols[i]->length[data->rows]  = data->cols[i]->length[data->DEFAULT_ARRAY_SIZE - 1];
					   memcpy(data->cols[i]->colbuf + data->rows * data->cols[i]->dsize, 
						  data->cols[i]->colbuf + (data->DEFAULT_ARRAY_SIZE - 1) * data->cols[i]->dsize,
						  data->cols[i]->dsize);
					}
				    }
				    data->rows ++;
				    if(data->rows == data->DEFAULT_ARRAY_SIZE) break;
				}

				if (i != 0 && i != MYSQL_NO_DATA)	
				{
				    myError8(db1);
				    break;
				}

				if (cmdopts->verbose)
	        		{
			          printf("Start data batch (%d)...\n", data->rows);
			        }
				/* Process Row Insert on target database */
				if (data->rows)
				{
				   for(row = 0; row < data->rows; )
				   {
				     /*
			               Insert MySQL Bind Code Here 
			             */
				     trybatchrow = 1;
			             if (arrquery2[0] > 0 && arrmybind.size && (row + (arrmybind.size/data->colcount)) <= data->rows)
			             {
				        for(i=0;i<arrmybind.size;i++)
				        {
					  pb = (arrbindarr + i);
					  arrmybind.is_null[i] = (data->cols[arrmybind.indx[i]]->is_null[row + arrmybind.rows[i]]);
					  arrmybind.len[i] = (long)(data->cols[arrmybind.indx[i]]->length[row + arrmybind.rows[i]]);
					  if (row>0) memcpy(data->cols[arrmybind.indx[i]]->colbuf + arrmybind.rows[i] * data->cols[arrmybind.indx[i]]->dsize,
						(data->cols[arrmybind.indx[i]]->colbuf + (row  + arrmybind.rows[i]) * data->cols[arrmybind.indx[i]]->dsize),
						data->cols[arrmybind.indx[i]]->dsize);
					}
				        if (cmdopts->verbose)
			                {
			                   printf("Start batch bind data on MySQL...\n");
			                }
					for(i=0;i<arrmybind.size;i++)
					{
					     pb = (arrbindarr + i);
					     if (*(pb->is_null) == 0 && (*(pb->length)) > 16384)
					     {
				        	 if (cmdopts->verbose)
			                	 {
				                     printf("Start batch send long data (%d) on MySQL...\n", *(pb->length));
			         	         }
						 for(j=0; j * 16384 < *(pb->length); j++)
						     mysql_stmt_send_long_data(arrstmt, i, (char *)(pb->buffer) + j * 16384, MIN(16384, *(pb->length) - j * 16384));
					     }
					}
				        if (cmdopts->verbose)
			                {
			                   printf("Start batch execute on MySQL...\n");
			                }
			  	        if (mysql_stmt_execute(arrstmt) == 0)
					{
			  	            if (rtncode != OCI_SUCCESS)
				            {
					       myStmtError8(db2,arrstmt);
				            }
					    else
					    {  
					      /* start delete rows in source database */
					      if (cmdopts->delete[0] && delstmt != NULL && delbind != NULL)
					      {
					         for (j=row;j<row + arrmybind.size/data->colcount; j++)
					         {
					   	    for(i=0;i<binddel.size;i++)
						    {
							  binddel.is_null[i] = (data->cols[binddel.indx[i]]->is_null[j]);
						          binddel.len[i] = (int)(data->cols[binddel.indx[i]]->length[j]);
							  if (j>0) memcpy(data->cols[binddel.indx[i]]->colbuf, 
								 (data->cols[binddel.indx[i]]->colbuf + j * data->cols[binddel.indx[i]]->dsize), 
								 data->cols[binddel.indx[i]]->dsize);						
						    }
						    mysql_stmt_execute(delstmt);
					         }
					      }
					      trybatchrow=0;
				              totalrows = totalrows + arrmybind.size/data->colcount;
					      commitrows = commitrows + arrmybind.size/data->colcount;
					      row = row + arrmybind.size/data->colcount;
                                              if (totalrows % cmdopts->rows == 0) printRowInfo(db1, totalrows);
			                    }
                                        }
			             }
				     if (trybatchrow == 1)
			             {
				        for(i=0;i<mybind.size;i++)
				        {
					   mybind.is_null[i] = (data->cols[mybind.indx[i]]->is_null[row]);
				           mybind.len[i] = (int)(data->cols[mybind.indx[i]]->length[row]);
					   if (row>0) memcpy(data->cols[mybind.indx[i]]->colbuf, 
						(data->cols[mybind.indx[i]]->colbuf + row * data->cols[mybind.indx[i]]->dsize), 
						data->cols[mybind.indx[i]]->dsize);
					}
					if (cmdopts->delete[0] && delstmt != NULL && delbind != NULL)
					{
					   for(i=0;i<binddel.size;i++)
					   {
					      binddel.is_null[i] = (data->cols[binddel.indx[i]]->is_null[row]);
				              binddel.len[i] = (int)(data->cols[binddel.indx[i]]->length[row]);
					      if (row>0) memcpy(data->cols[binddel.indx[i]]->colbuf, 
						 (data->cols[binddel.indx[i]]->colbuf + row * data->cols[binddel.indx[i]]->dsize), 
						 data->cols[binddel.indx[i]]->dsize);						
					   }
					}
				        if (cmdopts->verbose)
			                {
		        	           printf("Start send long data on MySQL...\n");
	        		        }
					for(i=0;i<mybind.size;i++)
					{
					     if (mybind.is_null[i] == 0 && mybind.len[i] > 16384)
					     {
				        	 if (cmdopts->verbose)
			                	 {
				                     printf("Start send long data (%d) on MySQL...\n", *(pb->length));
			         	         }
						 for(j=0; j * 16384 < mybind.len[i]; j++)
						     mysql_stmt_send_long_data(stmt, i, (char *)(mybind.buf[i] + j * 16384), MIN(16384, mybind.len[i] - j * 16384));
					     }
					}
				        if (cmdopts->verbose)
			                {
			                   printf("Start execute on MySQL (row=%d)...\n", row);
			                }
				        if (cmdopts->verbose)
			                {
			                      printf("Finish execute on MySQL (return=%d)...\n", rtncode);
			                }
			  	        if (mysql_stmt_execute(stmt))
					{
					      myStmtError8(db2,stmt);
					}
					else
					{
					     if (cmdopts->delete[0] && delstmt != NULL) 
						mysql_stmt_execute(delstmt);
					}
				        totalrows ++;
					commitrows ++;
					row++;
	     		                if (totalrows % cmdopts->rows == 0) printRowInfo(db1, totalrows);
				     }
			           }
				}
				if (commitrows > cmdopts->commit)
				{
				   /*
				   if (mysql_commit(db2->conn))
				   {
				      myError8(db2);
				      exitdump = 1;
				   }
				   else
				   {
				      if (cmdopts->delete[0] && delstmt != NULL)  mysql_commit(db1->conn);
				   }
				   */
				   commitrows = 0;
				}
				if (data->rows < data->DEFAULT_ARRAY_SIZE) break;
				if (cmdopts->waittime) 
				{
				   sleepInterval(cmdopts->waittime);
				}
			      }
			      mysql_stmt_free_result(my_stmt1);
			      /*
			      if (mysql_commit(db2->conn))
			      {
				  myError8(db2);
				  rtncode = OCI_ERROR;
			      }
			      else
			      {
				  if (cmdopts->delete[0] && delstmt != NULL) mysql_commit(db1->conn);
			          if (totalrows % cmdopts->rows) printRowInfo(db1, totalrows);
			      }
			      */
		              if (totalrows % cmdopts->rows) printRowInfo(db1, totalrows);
		      }
              }
	      else
	      {
	           myError8(db1);
		   rtncode = OCI_ERROR;
              }
	      pb = NULL;
	    }
       }
       else
       {
           myError8(db1);
	   rtncode = OCI_ERROR;
       }
   }
   else
   {
       myError8(db1);
       rtncode = OCI_ERROR;
   }

   /*
   memset(msgbuf,0,512);
   sprintf(msgbuf,"SET GLOBAL innodb_old_blocks_time=0");
   mysql_query(db1->conn, msgbuf);
   */

   if (rowbind != NULL) free(rowbind);
   if (bindarr != NULL) free(bindarr);
   if (arrquery2[0] && arrbindarr != NULL) free(arrbindarr);
   if (cmdopts->delete[0] && delbind != NULL) free(delbind);

   if (stmt != NULL) mysql_stmt_close(stmt);
   if (arrstmt != NULL) mysql_stmt_close(arrstmt);
   if (my_stmt1 != NULL) mysql_stmt_close(my_stmt1);
   if (delstmt != NULL)  mysql_stmt_close(delstmt);

   freeTableData(data);
   free(data);

   return rtncode;
}

void autoDetectTableOwner(CMDOPTIONS *cmdopts)
{
    int i,pos=0;
    unsigned char tempbuf[132];
    
    for(i=0;i<strlen(cmdopts->table1);i++)
    {
         if (cmdopts->table1[i] == '.')
	 {
	     pos = i;
	     break;
	 }
    }

    if (pos && pos < strlen(cmdopts->table1) -1)
    {
	memset(cmdopts->owner1,0,132);
	memcpy(cmdopts->owner1, cmdopts->table1, pos);
	memset(tempbuf,0,132);
	memcpy(tempbuf,cmdopts->table1+pos+1, 132 - pos - 1);
	memset(cmdopts->table1,0,132);
	memcpy(cmdopts->table1,tempbuf,132);
    }
}

unsigned char  getHexIndex(char c)
{
   if ( c >='0' && c <='9') return c - '0';
   if ( c >='a' && c <='f') return 10 + c - 'a';
   if ( c >='A' && c <='F') return 10 + c - 'A';
   return 0;
}

int convertOption(const unsigned char *src, unsigned char *dst, int mlen)
{
   int i,len,pos;
   unsigned char c,c1,c2;

   i=pos=0;
   len = strlen(src);
   
   
   while(i<MIN(mlen,len))
   {
      if ( *(src+i) == '0')
      {
          if (i < len - 1)
          {
             c = *(src+i + 1);
             switch(c)
             {
                 case 'x':
                 case 'X':
                   if (i < len - 3)
                   {
                       c1 = getHexIndex(*(src+i + 2));
                       c2 = getHexIndex(*(src+i + 3));
                       *(dst + pos) = (unsigned char)((c1 << 4) + c2);
                       i=i+2;
                   }
                   else if (i < len - 2)
                   {
                       c1 = *(src+i + 2);
                       *(dst + pos) = c1;
                       i=i+1;
                   }
                   break;
                 default:
                   *(dst + pos) = c;
                   break;
             }
             i = i + 2;
          }
          else
          {
            i ++;
          }
      }
      else
      {
          *(dst + pos) = *(src+i);
          i ++;
      }
      pos ++;
   }
   *(dst+pos) = '\0';
   return pos;
}

void getCommandValue(unsigned char *buf, OPTIONPOS *optpos)
{
    int i=0,j=0,k=0;
    while(isspace(*(buf+i))) i++;
    j = i;
    while(*(buf+j) != '\0' && *(buf+j) != '=') j++;
    k = j + 1;
    while(isspace(*(buf+k))) k++;
    j --;
    while(isspace(*(buf+j))) j--;
    optpos->name = buf + i;
    optpos->nlen = j - i + 1;
    optpos->val  = buf + k;
    optpos->vlen = strlen(buf) - k;
}

void autoDecryptPassword(char *buf)
{
    int crypt = 1, i = 0;
    char tempbuf[132];
    memcpy(tempbuf,buf,132);
    
    for(i=0;i<strlen(tempbuf);i++)
    {
	if (!((tempbuf[i] >='0' && tempbuf[i] <='9') ||
	      (tempbuf[i] >='a' && tempbuf[i] <='f') ||
	      (tempbuf[i] >='A' && tempbuf[i] <='F') ))
	{
		crypt=0;
		break;
	}
    }
    
    if (crypt)
    {
        memset(tempbuf,0,132);
        for(i=0;i<=strlen(buf);i=i+2)
        {
            tempbuf[i/2] = 16 * getAsciiHexValue(buf[i]) + getAsciiHexValue(buf[i+1]);
        }
        anysql_decrypt_password(tempbuf);
	tempbuf[strlen(buf)/2]=0;    		
	memcpy(buf, tempbuf, 132);
    }
}
void encryptPassword(char *prompt, char *buf)
{
    int n,i;
    unsigned char real_username[132];
    n = strlen(buf);
    n = (n/8) + (n%8?1:0);
    memcpy(real_username, buf, 132);
    anysql_encrypt_password(real_username);
    printf("%s",prompt);
    for(i=0;i<n * 8;i++)
    {
       printf("%02x", real_username[i]);
       if (i==31) printf("\n");
    }
    printf("\n");
}

int parseCommandOption(CMDOPTIONS *cmdopt, char *line)
{
      unsigned char tempbuf[512];
      OPTIONPOS optpos;
      int i,j;

      getCommandValue(line, &optpos);
           
      if (optpos.nlen == 5 && STRNCASECMP("user1",optpos.name,5)==0)
      {
          memset(cmdopt->user1,0,132);
          memcpy(cmdopt->user1,optpos.val,MIN(optpos.vlen,128));
      }
      else if (optpos.nlen == 5 && STRNCASECMP("user2",optpos.name,5)==0)
      {
          memset(cmdopt->user2,0,132);
          memcpy(cmdopt->user2,optpos.val,MIN(optpos.vlen,128));
      }
      else if (optpos.nlen == 3 && STRNCASECMP("log",optpos.name,3)==0)
      {
          memset(cmdopt->logname,0,512);
          memcpy(cmdopt->logname,optpos.val,MIN(optpos.vlen,511));
      }
      else if (optpos.nlen == 5 && STRNCASECMP("table",optpos.name,5)==0)
      {
          memset(cmdopt->table1,0,132);
          memcpy(cmdopt->table1,optpos.val,MIN(optpos.vlen,128));
          memset(cmdopt->table2,0,132);
          memcpy(cmdopt->table2,optpos.val,MIN(optpos.vlen,128));
      }
      else if (optpos.nlen == 6 && STRNCASECMP("tables",optpos.name,6)==0)
      {
          memset(cmdopt->tables,0,8192);
          memcpy(cmdopt->tables,optpos.val,MIN(optpos.vlen,8191));
      }
      else if (optpos.nlen == 6 && STRNCASECMP("table1",optpos.name,6)==0)
      {
          memset(cmdopt->table1,0,132);
          memcpy(cmdopt->table1,optpos.val,MIN(optpos.vlen,128));
      }
      else if (optpos.nlen == 6 && STRNCASECMP("table2",optpos.name,6)==0)
      {
          memset(cmdopt->table2,0,132);
          memcpy(cmdopt->table2,optpos.val,MIN(optpos.vlen,128));
      }
      else if (optpos.nlen == 5 && STRNCASECMP("crypt",optpos.name,5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
	  cmdopt->crypt = 0;
	  if (STRNCASECMP(tempbuf,"YES",3) == 0 || STRNCASECMP(tempbuf,"ON",2) == 0)
	      cmdopt->crypt = 1;
      }      
      else if (optpos.nlen == 4 && STRNCASECMP("sync",optpos.name,4)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
	  cmdopt->syncmode = DATA_COPY_INSERT;
	  if (STRNCASECMP(tempbuf,"INSERT",6) == 0)
	      cmdopt->syncmode = DATA_COPY_INSERT;
	  else if (STRNCASECMP(tempbuf,"DELETE",6) == 0)
	      cmdopt->syncmode = DATA_COPY_DELETE;
	  else if (STRNCASECMP(tempbuf,"UPDATE",6) == 0)
	      cmdopt->syncmode = DATA_COPY_UPDATE;
	  else if (STRNCASECMP(tempbuf,"INSUPD",6) == 0)
	      cmdopt->syncmode = DATA_COPY_INSUPD;
	  else if (STRNCASECMP(tempbuf,"ARRINS",6) == 0)
	      cmdopt->syncmode = DATA_COPY_ARRINS;
	  else if (STRNCASECMP(tempbuf,"ARRUPD",6) == 0)
	      cmdopt->syncmode = DATA_COPY_ARRUPD;
	  else if (STRNCASECMP(tempbuf,"REPLACE",7) == 0)
	      cmdopt->syncmode = DATA_COPY_ARRREP;
      }  
      else if (optpos.nlen == 6 && STRNCASECMP("unique",optpos.name,6)==0)
      {
          memset(cmdopt->pkcols,0,512);
          memcpy(cmdopt->pkcols,optpos.val,MIN(optpos.vlen,511));
      }      
      else if (optpos.nlen == 8 && STRNCASECMP("conflict",optpos.name,8)==0)
      {
          memset(cmdopt->cfcols,0,512);
          memcpy(cmdopt->cfcols,optpos.val,MIN(optpos.vlen,511));
      }      
      else if (optpos.nlen == 6 && STRNCASECMP("filler",optpos.name,6)==0)
      {
          memset(cmdopt->filler,0,512);
          memcpy(cmdopt->filler,optpos.val,MIN(optpos.vlen,511));
      }      
      else if (optpos.nlen == 6 && STRNCASECMP("query1",optpos.name,6)==0)
      {
          memset(cmdopt->query1,0,16384);
          memcpy(cmdopt->query1,optpos.val,MIN(optpos.vlen,16383));
      }
      else if (optpos.nlen == 6 && STRNCASECMP("query2",optpos.name,6)==0)
      {
          memset(cmdopt->query2,0,16384);
          memcpy(cmdopt->query2,optpos.val,MIN(optpos.vlen,16383));
      }      
      else if (optpos.nlen == 6 && STRNCASECMP("delete",optpos.name,6)==0)
      {
          memset(cmdopt->delete,0,16384);
          memcpy(cmdopt->delete,optpos.val,MIN(optpos.vlen,16383));
      }      
      else if (optpos.nlen == 5 && STRNCASECMP("split",optpos.name,5)==0)
      {
          memset(cmdopt->table1,0,132);
          memcpy(cmdopt->table1,optpos.val,MIN(optpos.vlen,132));
      }
      else if (optpos.nlen == 8 && STRNCASECMP("splitkey",optpos.name,8)==0)
      {
          memset(cmdopt->splitkey,0,132);
          memcpy(cmdopt->splitkey,optpos.val,MIN(optpos.vlen,132));
      }
      else if (optpos.nlen == 6 && STRNCASECMP("degree",optpos.name,6)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->degree = atoi(tempbuf);
          if (cmdopt->degree < 2) cmdopt->degree = 2;
          if (cmdopt->degree > 128) cmdopt->degree = 128;
      }   
      else if (optpos.nlen == 7 && STRNCASECMP("charset",optpos.name,7)==0)
      {
          memset(cmdopt->charset,0,256);
          memcpy(cmdopt->charset,optpos.val,MIN(optpos.vlen,255));
      }
      else if (optpos.nlen == 4 && STRNCASECMP("rows",optpos.name,4)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->rows = atoi(tempbuf);
          if (cmdopt->rows < 2000) cmdopt->rows = 2000;
      }     
      else if (optpos.nlen == 6 && STRNCASECMP("commit",optpos.name,6)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->commit = atoi(tempbuf);
          if (cmdopt->commit < 1000) cmdopt->commit = 1000;
      }     
      else if (optpos.nlen == 4 && STRNCASECMP("wait",optpos.name,4)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->waittime = atoi(tempbuf);
          if (cmdopt->waittime < 0) cmdopt->waittime = 0;
          if (cmdopt->waittime > 10000) cmdopt->waittime = 10000;
      }     
      else if (optpos.nlen == 4 && STRNCASECMP("long",optpos.name,4)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->DEFAULT_LONG_SIZE = atoi(tempbuf) * 1024;
          if (cmdopt->DEFAULT_LONG_SIZE < 16384) cmdopt->DEFAULT_LONG_SIZE = 16384;
          if (cmdopt->DEFAULT_LONG_SIZE > 104857600) cmdopt->DEFAULT_LONG_SIZE = 104857600;
      }
      else if (optpos.nlen == 5 && STRNCASECMP("array",optpos.name,5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->DEFAULT_ARRAY_SIZE = atoi(tempbuf);
          if (cmdopt->DEFAULT_ARRAY_SIZE < 2) cmdopt->DEFAULT_ARRAY_SIZE = 2;
          if (cmdopt->DEFAULT_ARRAY_SIZE > 2000) cmdopt->DEFAULT_ARRAY_SIZE = 2000;
      }
      else if (optpos.nlen == 7 && STRNCASECMP("verbose",optpos.name,7)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          cmdopt->verbose= atoi(tempbuf);
          if (cmdopt->verbose < 0) cmdopt->verbose = 0;
      }     
      else if (optpos.nlen == 5 && STRNCASECMP("error",optpos.name,5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,optpos.val,MIN(optpos.vlen,254));
          i = atoi(tempbuf);
	  for(j=0;j<cmdopt->oerrcount;j++)
	  {
	      if (cmdopt->oerrorno[j] == i) return 1;
	  }
	  cmdopt->oerrorno[cmdopt->oerrcount++] = i;
      } 
      else
      {
      	  return 0;
      }
      return 1;
}

void resetCommandOptions(CMDOPTIONS  *cmdopts)
{
    memset(cmdopts->user1,0,132);
    memset(cmdopts->user2,0,132);
    memset(cmdopts->table1,0,132);
    memset(cmdopts->table2,0,132);
    memset(cmdopts->query1,0,16384);
    memset(cmdopts->query2,0,16384);
    memset(cmdopts->delete,0,16384);
    memset(cmdopts->tables,0,8192);
    memset(cmdopts->pkcols,0,512);
    memset(cmdopts->cfcols,0,512);
    memset(cmdopts->logname,0,512);
    memset(cmdopts->charset,0,256);
    memset(cmdopts->tabsplit,0,132);
    memset(cmdopts->splitkey,0,132);
    
    cmdopts->charset[0]='g';
    cmdopts->charset[1]='b';
    cmdopts->charset[2]='k';
    cmdopts->crypt = 0;
    cmdopts->rows   = 1000000;
    cmdopts->commit = 50000;
    cmdopts->waittime = 0;
    cmdopts->syncmode = (unsigned char)DATA_COPY_ARRREP;
    cmdopts->rtncode  = 1;
    cmdopts->oerrcount= 0;
    cmdopts->verbose  = 0;
    cmdopts->degree   = 0;

    cmdopts->DEFAULT_ARRAY_SIZE=    2000;
    cmdopts->DEFAULT_LONG_SIZE =   65536*4;

    cmdopts->context = NULL;
    cmdopts->msgfunc = NULL;

    cmdopts->sqlargc = 0;
}

int runDataCopy(CMDOPTIONS *cmdopts)
{
    int rtncode=0;
    MYDATABASE db1;
    MYDATABASE db2; 
    FILE *fp_log=NULL;

    unsigned char tempbuf[512];
    CMDLINE    cmdline;
    int i;

    initMYDB(&db1);
    initMYDB(&db2);

    autoDecryptPassword(cmdopts->user1);
    autoDecryptPassword(cmdopts->user2);

    if (strlen(cmdopts->logname))
    {
        if ((fp_log = openFile(cmdopts->logname)) == NULL) fp_log = stdout;
    }
    else
    {
        fp_log = stdout;
    }
    
    db1.fp_log=fp_log;
    db2.fp_log=fp_log;

    db1.context = cmdopts->context;
    db1.msgfunc = cmdopts->msgfunc;
    if (db1.context == NULL) db1.context = &db1;
    if (db1.msgfunc == NULL) db1.msgfunc = defaultMyMessageFunc;

    db2.context = cmdopts->context;
    db2.msgfunc = cmdopts->msgfunc;
    if (db2.context == NULL) db2.context = &db2;
    if (db2.msgfunc == NULL) db2.msgfunc = defaultMyMessageFunc;

    memcpy(db1.charset, cmdopts->charset, 256);
       
    if((LogonMYDB(&db1,cmdopts->user1)) != OCI_SUCCESS)
    {
       if (db1.msgfunc != NULL)
       {
          (*(db1.msgfunc))(db1.context, "cannot connect to source database\n");
       }
       if (db1.fp_log != stdout) fclose(db1.fp_log);
       cmdopts->rtncode = OCI_ERROR;
       return OCI_ERROR;
    }

    memcpy(db2.charset, cmdopts->charset, 256);

    if(LogonMYDB(&db2,cmdopts->user2) != OCI_SUCCESS)
    {
       disconnectMYDB(&db1);
       if (db2.msgfunc != NULL)
       {
          (*(db2.msgfunc))(db2.context, "cannot connect to target database\n");
       }
       if (db2.fp_log != stdout) fclose(db2.fp_log);
       cmdopts->rtncode = OCI_ERROR;
       return OCI_ERROR;
    }

    if (strlen(cmdopts->pkcols) == 0 || strlen(cmdopts->table2) == 0)
    {
	if (cmdopts->syncmode != DATA_COPY_ARRINS &&
            cmdopts->syncmode != DATA_COPY_ARRREP) cmdopts->syncmode = 0;
    }

    rtncode = copyDataByQuery(&db1, &db2, cmdopts);

    disconnectMYDB(&db1);
    disconnectMYDB(&db2);
    if (fp_log != stdout) fclose(fp_log);
    cmdopts->rtncode = rtncode;
    return rtncode;
}

#if defined(_WIN32)
unsigned __stdcall threadDataCopy(void *p)
{
    CMDOPTIONS *cmdopts = (CMDOPTIONS *) p;
    runDataCopy(cmdopts);
    _endthreadex( 0 );
    return 0;
}
#endif

int main(int argc, char *argv[])
{
    int i;
    int n, rtncode=0;
    FILE *fp;
    CMDOPTIONS cmdopts;
    MYDATABASE db1, db2; 
    TABLESPLIT splitdata;

    unsigned char tempbuf[512];
    unsigned char linebuf[16384];
    unsigned char ctlfname[256]="";

    char version1[] = {0x6c,0x2b,0xfd,0xa7,0x1e,0x04,0x35,0xb7,
            0xb5,0xc8,0x12,0xcc,0xb7,0xe6,0xe4,0x4f,
            0x3a,0x52,0xec,0x4b,0x85,0xcb,0x99,0xce,
            0xcf,0xa6,0xb2,0x4e,0xce,0x17,0x68,0xb9,
            0x8b,0x74,0x46,0xda,0x25,0x8a,0x11,0x13,
            0x99,0xb8,0x6c,0xf9,0x96,0x9d,0x65,0x02,
            0x28,0x5c,0xe2,0xc6,0x9e,0xad,0xad,0x94,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce};

    char version2[] = {0x62,0x3b,0x4d,0xbd,0x26,0x92,0x07,0x72,
            0x69,0x28,0x2a,0x90,0x13,0x3b,0x87,0x1b,
            0x56,0x15,0xd9,0xd3,0xff,0x11,0xd7,0x08,
            0x55,0x41,0x74,0xed,0xa9,0x4c,0x48,0x06,
            0xb5,0xb9,0x25,0xa5,0x5f,0xe9,0x24,0x87,
            0x69,0xc0,0xa3,0x02,0x2b,0x62,0xca,0x4e,
            0x0e,0xc0,0xe9,0x2b,0x8d,0xec,0x33,0xfd,
            0x83,0x4b,0x2a,0x8d,0x9e,0x4d,0x11,0x46,
            0x77,0x85,0x29,0x08,0xb2,0x7a,0x9a,0xbc,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce,
            0x7f,0x04,0x3e,0xea,0xe1,0x9f,0xfa,0x54,
            0x45,0xac,0x5e,0x9d,0x89,0x92,0x1d,0xce};

    splitdata.count = 0;
    initMYDB(&db1);
    initMYDB(&db2);

    anysql_decrypt_sqlstmt(version1);
    anysql_decrypt_sqlstmt(version2);

    resetCommandOptions(&cmdopts);
  
    for(i=1;i<argc;i++)
    {
      if (STRNCASECMP("parfile=",argv[i],8)==0)
      {
          memset(ctlfname,0,256);
          memcpy(ctlfname,argv[i]+8,MIN(strlen(argv[i]) - 8,256));
	  if (ctlfname[0] == '-')
              fp = stdin;
	  else
	      fp = fopen(ctlfname,"rb+");
          if (fp != NULL)
          {
        	memset(linebuf,0,16384);
          	while(!feof(fp))
          	{
          	   memset(tempbuf,0,512);
          	   fgets(tempbuf,511,fp);
          	   if (tempbuf==NULL) break;
          	   n = strlen(tempbuf);
          	   while(tempbuf[n-1] == '\r' || tempbuf[n-1] == '\n' || tempbuf[n-1] == '\t' || tempbuf[n-1] == ' ') n--;
          	   if (isspace(tempbuf[0]))
          	   {
          	   	memcpy(linebuf+strlen(linebuf), tempbuf, n);
          	   	continue;
          	   }
          	   else
          	   {
			if (strlen(linebuf))
			{
			    parseCommandOption(&cmdopts, linebuf);
			}
			memset(linebuf,0,16384);
          	   	memcpy(linebuf+strlen(linebuf), tempbuf, n);
          	   }
          	}
		if (strlen(linebuf))
		{
	          	parseCommandOption(&cmdopts, linebuf);
		}
		if (ctlfname[0] != '-')
             	    fclose(fp);
          }
	  memset(ctlfname,0,256);
      }
      else 
      {
      	  parseCommandOption(&cmdopts, argv[i]);
      }
    }

    for(i=1;i<argc;i++)
    {
        if (STRNCASECMP(":",argv[i],1) == 0)
        {        
            memset(tempbuf,0,512);
            memcpy(tempbuf,argv[i]+1,MIN(strlen(argv[i]) - 1,256));
            for(n=0;n<strlen(tempbuf);n++) if (tempbuf[n]=='=') break;
            if (n)
            {
                cmdopts.sqlargv[cmdopts.sqlargc].dbtype = MYSQL_TYPE_STRING;
                memset(cmdopts.sqlargv[cmdopts.sqlargc].pname,0,31);
                memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname, tempbuf, MIN(n,30));
                cmdopts.sqlargv[cmdopts.sqlargc].nlen = MIN(n,30);
                memset(cmdopts.sqlargv[cmdopts.sqlargc].pval,0,128);
                cmdopts.sqlargv[cmdopts.sqlargc].vlen=0;
                if(n+1<strlen(tempbuf))
                {
                    memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval, tempbuf+n+1, MIN(strlen(tempbuf) - n - 1,127));
                    cmdopts.sqlargv[cmdopts.sqlargc].vlen = MIN(strlen(tempbuf) - n - 1,127);
                }
                cmdopts.sqlargc ++;
		if (cmdopts.sqlargc >= 18) break;
             }
        }
    }
  
    if (strlen(cmdopts.user1)==0) cmdopts.user1[0]='/';
    if (strlen(cmdopts.user2)==0) cmdopts.user2[0]='/';

    if (cmdopts.crypt == 1)
    {
       encryptPassword("user1=", cmdopts.user1);
       encryptPassword("user2=", cmdopts.user2);
       exit(1);
    }

    if (strlen(cmdopts.table2) == 0)
    {
	memcpy(cmdopts.table2, cmdopts.table1, 132);
    }
    
    if (strlen(cmdopts.tables) == 0 &&
        ((strlen(cmdopts.query1)==0 && strlen(cmdopts.table1)==0) || 
        (strlen(cmdopts.query2)==0 && strlen(cmdopts.table2)==0)) )
    {
       printf("\n");
       printf("%s\n", version1);
       printf("%s\n", version2);

       printf("\n");
       printf("Usage: obdatacopy keyword=value [,keyword=value,...]\n");
       printf("\n");
       printf("Valid Keywords:\n");
       printf("   user1   = username/password@host:port:sid for source database.\n");
       printf("   user2   = username/password@host:port:database for target database.\n");
       printf("   table   = table name for both source and target.\n");
       printf("   tables  = table name list for both source and target.\n");
       printf("   table1  = source table name to query data from.\n");
       printf("   table2  = target table name to insert data into.\n");
       printf("   query1  = select SQL for source database.\n");
       printf("   query2  = insert SQL for target database.\n");
       printf("   array   = array fetch size \n");
       printf("   rows    = print log information for every given rows.\n");
       printf("   long    = maximum size for long, long raw, CLOB, BLOB columns.\n");
       printf("   crypt   = encrypt the connection info only, no data copy (YES/NO).\n");
       printf("   parfile = read command option from parameter file \n");
       printf(" * wait    = wait time in microsecond after each array.\n");
       printf(" * sync    = sync mode (INSERT,UPDATE,DELETE,INSUPD,ARRINS,ARRUPD,REPLACE).\n");
       printf(" * unique  = primary key or unique key columns of target table.\n");
       printf(" * conflict= conflict columns for update on target table.\n");
       printf(" * filler  = filler columns (exclude columns) for target table.\n");
       printf(" * safe    = double column buffer for character set conversion.\n");
       printf(" * split   = primary key column name used for automatic parallel.\n");
       printf(" * degree  = parallelize data copy degree (2-128).\n");
       printf("   log     = log file name for screen messages.\n");
       
       printf("\n");
       printf("Notes:\n");
       printf("   obdatacopy user1=scott/tiger user2=scott/tiger table=emp\n");
       printf("   obdatacopy user1=scott/tiger user2=scott/tiger table1=emp table2=emp_his\n");
       printf("\n");
       exit(1);
    }

    if(LogonMYDB(&db1,cmdopts.user1) != OCI_SUCCESS)
    {
       fprintf(db1.fp_log,"cannot connect to source database\n");
       myError8(&db1);
       if (db1.fp_log != stdout) fclose(db1.fp_log);
       return OCI_ERROR;
    }
    else
    {
       if (cmdopts.degree > 1 && strlen(cmdopts.table1) > 0 && strlen(cmdopts.splitkey) > 0)
       {
           splitTableRange(&splitdata, &db1, cmdopts.table1, cmdopts.splitkey, cmdopts.degree);
           if (splitdata.count > 0)
           {
		   #if defined(_WIN32)
	           for(i=0;i<splitdata.count;i++)
		   {
	               splitdata.opts[i] = (CMDOPTIONS *)malloc(sizeof(CMDOPTIONS));
		       memcpy(splitdata.opts[i], &cmdopts, sizeof(CMDOPTIONS));

		       if (strlen(cmdopts.query1) == 0)
		       {
	        	   sprintf(splitdata.opts[i]->query1,"SELECT * FROM %s WHERE %s >= :minrid AND %s < :maxrid", 
				cmdopts.table1, cmdopts.splitkey, cmdopts.splitkey);
		       }

		       memcpy(splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].pname,"minrid", 6);
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].nlen = 6;
		       memcpy(splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].pval,splitdata.range[i].minrid, splitdata.dsize);
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].vlen =splitdata.dsize;
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].dbtype =splitdata.dbtype;
		       splitdata.opts[i]->sqlargc ++;


		       memcpy(splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].pname,"maxrid", 6);
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].nlen = 6;
		       memcpy(splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].pval,splitdata.range[i].maxrid, splitdata.dsize);
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].vlen =splitdata.dsize;
		       splitdata.opts[i]->sqlargv[splitdata.opts[i]->sqlargc].dbtype =splitdata.dbtype;
		       splitdata.opts[i]->sqlargc ++;

		       splitdata.opts[i]->slaveid = splitdata.range[i].id + 1;
		       splitdata.hThread[i] = (HANDLE)_beginthreadex( NULL, 0, &threadDataCopy, splitdata.opts[i], 0, &(splitdata.threadID[i]) ); 
		   }

		   if (splitdata.count > 1)
	              WaitForMultipleObjects(splitdata.count, splitdata.hThread, TRUE, INFINITE );
		   else
	              WaitForSingleObject(splitdata.hThread[0], INFINITE );

		   for(i=0;i<splitdata.count;i++);
		   {
		       CloseHandle( splitdata.hThread[i] );
        	   }
		   rtncode = 0;
                   /*
                   allfail = 1;
                   for(i=0;i<splitdata.count;i++)
                   {
                        if (splitdata.opts[i]->rtncode == 0) { allfail = 0; break; }
                   }
                   if (allfail == 0)
                   {
                       allfail = 0;
                       for(i=0;i<splitdata.count;i++)
                       {
                           if (splitdata.opts[i]->rtncode != 0) { allfail = 1; break; }
                       }
                       while(allfail && retrycnt < cmdopts.pretry)
                       {
                           for(i=0;i<splitdata.count;i++)
                           {
                               if (splitdata.opts[i]->rtncode != 0)
                               {
                                  splitdata.hThread[i] = (HANDLE)_beginthreadex( NULL, 0, &threadTextUnloader, splitdata.opts[i], 0, &(splitdata.threadID[i]) ); 
                                  WaitForSingleObject(splitdata.hThread[i], INFINITE );
                                  retrycnt ++;
                               }
                           }
                           allfail = 0;
                           for(i=0;i<splitdata.count;i++)
                           {
                               if (splitdata.opts[i]->rtncode != 0) { allfail = 1; break; }
                           }
                       }
                   }
                   */
		   for(i=0;i<splitdata.count;i++)
		   {
		       if (splitdata.opts[i] != NULL)
		       {
	                  rtncode = rtncode + splitdata.opts[i]->rtncode;
			  free(splitdata.opts[i]);
		       }
        	   }
		   #else
		   for(i=0;i<splitdata.count;i++)
		   {
			splitdata.thid[i] = fork();
			if (splitdata.thid[i] == 0)
			{
	        	    if (strlen(cmdopts.query1)==0)
			    {
		        	   sprintf(cmdopts.query1,"SELECT * FROM %s WHERE %s >= :minrid AND %s < :maxrid", 
					   cmdopts.table1, cmdopts.splitkey, cmdopts.splitkey);
			    }
  		            memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"minrid", 6);
		            cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
		            memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].minrid, splitdata.dsize);
	        	    cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
	        	    cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
		            cmdopts.sqlargc ++;

	  	            memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"maxrid", 6);
		            cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
	        	    memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].maxrid, splitdata.dsize);
		            cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
	        	    cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
		            cmdopts.sqlargc ++;

		            cmdopts.slaveid = splitdata.range[i].id;
			    rtncode = runDataCopy(&cmdopts);
			    return rtncode;
			}
		   }
	   
		   for(i=0;i<splitdata.count;i++)
		   {
		      rtncode=waitpid(splitdata.thid[i],&(splitdata.status[i]),0);
		   }

                   /*
                   allfail = 1;
                   for(i=0;i<splitdata.count;i++)
                   {
                        if (WIFEXITED(splitdata.status[i]))
                        {
                            if (WEXITSTATUS(splitdata.status[i]) == 0)
                            {
                                allfail = 0; 
                                break;
                            }
                        }
                   }
                   if (allfail == 0)
                   {
                       allfail = 0;
                       for(i=0;i<splitdata.count;i++)
                       {
                            if (WIFEXITED(splitdata.status[i]))
                            {
                                if (WEXITSTATUS(splitdata.status[i]))
                                {
                                    allfail = 1; 
                                    break;
                                }
                            }
                            else
                            {
                                allfail = 1; 
                                break;
                            }
                       }
                       while(allfail && retrycnt < cmdopts.pretry)
                       {
                           for(i=0;i<splitdata.count;i++)
                           {
                                if (WIFEXITED(splitdata.status[i]))
                                {
                                    if (WEXITSTATUS(splitdata.status[i]))
                                    {
			                 splitdata.thid[i] = fork();
			                 if (splitdata.thid[i] == 0)
			                 {
	        	                     if (strlen(cmdopts.query1)==0)
                 			     {
		  		       	         sprintf(cmdopts.query1,"SELECT * FROM %s WHERE %s >= :minrid AND %s < :maxrid", 
					                 cmdopts.tabname, cmdopts.splitkey, cmdopts.splitkey);
			                     }
		  		             memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"minrid", 6);
				             cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
				             memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].minrid, splitdata.dsize);
			        	     cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
			        	     cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
				             cmdopts.sqlargc ++;

			  	             memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"maxrid", 6);
				             cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
			        	     memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].maxrid, splitdata.dsize);
				             cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
			        	     cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
				             cmdopts.sqlargc ++;

				             cmdopts.slaveid = splitdata.range[i].id;
					     rtncode = runDataCopy(&cmdopts);
					     return rtncode;
                                         }
                                         rtncode=waitpid(splitdata.thid[i],&(splitdata.status[i]),0);
                                         retrycnt ++;
                                     }
                                }
                                else
                                {
			             splitdata.thid[i] = fork();
			             if (splitdata.thid[i] == 0)
			             {
	        	                 if (strlen(cmdopts.query1)==0)
                 			 {
		  		       	     sprintf(cmdopts.query1,"SELECT * FROM %s WHERE %s >= :minrid AND %s < :maxrid", 
					             cmdopts.tabname, cmdopts.splitkey, cmdopts.splitkey);
			                 }
		  		         memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"minrid", 6);
				         cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
				         memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].minrid, splitdata.dsize);
			        	 cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
			        	 cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
				         cmdopts.sqlargc ++;

			  	         memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pname,"maxrid", 6);
				         cmdopts.sqlargv[cmdopts.sqlargc].nlen = 6;
			        	 memcpy(cmdopts.sqlargv[cmdopts.sqlargc].pval,splitdata.range[i].maxrid, splitdata.dsize);
				         cmdopts.sqlargv[cmdopts.sqlargc].vlen =splitdata.dsize;
			        	 cmdopts.sqlargv[cmdopts.sqlargc].dbtype =splitdata.dbtype;
				         cmdopts.sqlargc ++;

				         cmdopts.slaveid = splitdata.range[i].id;
					 rtncode = runDataCopy(&cmdopts);
					 return rtncode;
                                     }
                                     rtncode=waitpid(splitdata.thid[i],&(splitdata.status[i]),0);
                                     retrycnt++;
                                }
                           }
                           allfail = 0;
                           for(i=0;i<splitdata.count;i++)
                           {
                               if (WIFEXITED(splitdata.status[i]))
                               {
                                  if (WEXITSTATUS(splitdata.status[i]))
                                  {
                                     allfail = 1; 
                                     break;
                                  }
                               }
                               else
                               {
                                  allfail = 1; 
                                  break;
                               }
                           }
                       }
                   }
                   */

		   rtncode = 0;
		   for(i=0;i<splitdata.count;i++)
		   {
		      if (WIFEXITED(splitdata.status[i]))
		      {
			 if (WEXITSTATUS(splitdata.status[i])) rtncode ++;
		      }
		      else
		      {
			 rtncode ++;
		      }
		   }
		   #endif
           }
           else
           {
                   rtncode = 11;
           }
       }
       else
       {
          rtncode = runDataCopy(&cmdopts);
       }
       disconnectMYDB(&db1);
    }
    return rtncode;
}

