#include <getopt.h>
#include "ob_import_v2.h"
#include "ob_import_producer_v2.h"
#include "ob_import_comsumer_v2.h"
#include "ob_import_param_v2.h"
#include "common/ob_version.h"
#include "common/file_utils.h"
#include <string> //add by zhuxh:20150724

using namespace std; //add by zhuxh:20150727

//mod by zhuxh:20151201
//All declaration are moved to ob_import_param_v2.h
#if 0
//global param
//define => declare, 20151201
extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;
extern bool trim_end_blank;

//add by pangtz //modify by zhuxh:20150720
extern bool error_arr[IMPORT_ERROR_TYPE_NUM];
//add e
//modify e
#endif

//add by liyongfeng
void print_version()
{
	fprintf(stderr, "ob_import (%s %s)\n", PACKAGE_STRING, RELEASEID);
	fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
	fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
    fprintf(stderr, "Copyright (c) 2015 BOCOM. All Rights Reserved.\n");
}
//add:e

void usage(const char *prog)
{
  UNUSED(prog);
  fprintf(stderr, //"Usage:%s \n"
          "===== Examples =====\n"
          "\t(1)\n"
          "\t./ob_import_v2 -h 182.119.80.67 -p 2500 --dbname database_name -t table_name -l t.log --badfile t.bad -f t.del --rep --progress\n"
          "\t(2)\n"
          "\t~/oceanbase/bin/ob_import_v2 -h 182.119.80.59 -p 5500 --dbname TANG -t students -l t.log --badfile t.bad -f t.del --case-sensitive --progress --concurrency 100 --record-persql 50 --trim 0 --column-mapping \"number,seq();first_name,3;last_name,1;hobby,2\"\n"
          //"\t-c [config file] \n"  //delete by zhuxh:20150723
          "===== Basic Arguments =====\n"
          "\t-h host (rootserver ip)\n"
          "\t-p port (rootserver port, always x500)\n"
          //add by zhangcd:20150814:b
          "\t--dbname database_name (specify the database name of the table)\n"
          //add:e
          "\t-t table_name\n"
          "\t[-l log_file]\n"
          "\t[--badfile badfile_name]\n"
          "\t-f data_file\n"
          "\t[--ins|--rep|--del] (default --ins)\n"
          "\t[--progress] (to show how many rows processed succesfully, unsuccessfully and totally every 10 seconds )\n"
          "===== Advanced Arguments =====\n"
          "\t[--g2u] (gbk converted to utf-8. Attention: No u2g for the moment.)\n"
          "\t[--rowbyrow] (producer read the file rowbyrow)\n"
          "\t[--concurrency] (default is 50)\n"
          "\t[--record-persql] (default is 50)\n"
          //add by pangtz:20141126 添加trim命令行帮助信息
          "\t[--trim trim_type] (trim_type: 0-remove both spaces; 1-remove left spaces; 2-remove right spaces)\n"
          //add:e
          //add by pangtz:20141126 [append 会计日期]
          //"\t[--append] (only append accdate, for hdfs. sample format[1997-01-01])\n"
          //add:e


          //add by zhangcd:20150814:b
          "\t[--username username] (specify the username of oceanbase)\n"
          "\t[--passwd passwd] (specify the password of the username)\n"

          //add:e
          "\t[--varchar-not-null] (Nothing in datafile for varchar will be treated as empty string instead of NULL.)\n"
          //add by zhuxh:20150729 :b
          "\t[--delima] (ASCII of column separator)\n"
          "\t[--rec-delima] (ASCII of row separator)\n"
          //"\t[--null-flag] (ASCII of character which means NULL)\n"
          /*
          "\t[--substr substr_list] If column sub0 is the substring of column src0, starting from 2nd character, with 5 characters for all,\n"
          "\t  and the same for sub1 and src1, starting from 4th and with length 3,\n"
          "\t  the param should be:\n"
          "\t  --substr \"sub0,src0,2,5;sub1,src1,4,3\"\n"
          "\t  (The quotation marks \"\" are essential because of semicolon \";\".)\n"
          "\t[--decimal-to-varchar colname_list] if type of column d2v0 and d2v1 is varchar, but in fact they store decimals,\n"
          "\t  the param should be:\n"
          "\t  --decimal-to-varchar d2v0,d2v1\n"
          */
          //add:e
          //add by zhuxh:20150811 :b

          "\t[--case-sensitive] (which works when a name of database or table is case sensitive. For instance, when you are to import data into default database TANG.)\n"
          "\t[--keep-double-quotation-marks] (with this option 2 sequential double quotes will be both kept between 2 quotes. E.G.)\n"
          "\t[--column-mapping]\n"
          "\t  --column-mapping \"5:col3,5,[\'Hello,world\'];col1,2,[321];col4,3\"\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |table col name|datafile col|default token  |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |col3          |5           |\'Hello,world\'  |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |col1          |2           |321            |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |col4          |3           |               |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  (The followings are default maps:)\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |col2          |1           |               |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  |col5          |4           |               |\n"
          "\t  +--------------+------------+---------------+\n"
          "\t  And other forms for short:\n"
          "\t  \"5\" means default maps 1:1...5:5\n"
          "\t  \"col0,['ABC']\" means default value instead of datafile token for col0.\n"
          "\t  \"idx,seq(10000,1)\" means the column counts from 10000 increased by 1. seq(5) means to change start to 5; seq() means to keep both start and increment. --rep CANNOT be used with seq().\n"
          /*
          "\t[--char-delima] ASCII of char delima of data column, quotation mark \" as default.\n"
          "\t  EX. In data 123^!xyz!^!abc! \"!\" is the char delima.\n"
          "\t[--columns-with-char-delima] names of columns which have char delimas\n"
          "\t  If --char-delima is set but --columns-with-char-delima isn't, or you write it as\n"
          "\t  --columns-with-char-delima ALL\n"
          "\t  All VARCHAR columns are thought to have char delimas.\n"
          */
        //add:e
          "\t[-V] (version)\n"
          "\t[-H] (help)\n"
          //add by pangtz
          "===== Return Value =====\n"
          "\t0 normal\n"
          "\t1 non-data error, such as no data file, no config file, schema error...,  bad file may exist\n"
          "\t2 data error or unknown error, bad file exist\n"

          "\n***** Developer Arguments *****\n"
          "\t[-q queue_size] (default 1000)\n"
          "\t[-g log_level]\n"
          "\t[--buffersize buffer_size] (KBRELEASEID default is %dKB)\n"
          "\t[--timeout timeout] (the maxmium time we could wait while connecting or querying)\n"
          "\t[--ignore-merge] (ignore the merging status of the oceanbase while importing data to database)\n"
          //add e
          /*,prog*/, kReadBufferSize / 1024);
}

/*
 * add by pangtz:20141126 判断append的会计日期是否是规定格式
 *
 */
int is_date_valid(const char * date)
{
	int ret = OB_SUCCESS;
	if(strlen(date) != 10)
		ret = OB_ERROR;
	if(ret == OB_SUCCESS){
		int num, year, month, day;
		num = sscanf(date, "%4d-%2d-%2d", &year, &month, &day);
		if(num ==3){
			ret = OB_SUCCESS;
		}else{
            error_arr[DATE_FORMAT_ERROR] = false; //add by zhuxh: 20150721
			ret = OB_ERROR;
		}
	}
	return ret;
}
//add:end


// mod by zhangcd 20150814
// add a parameter: logInfo
int run_comsumer_queue(FileReader &reader, TableParam &param, ObRowBuilder *builder,
                       OceanbaseDb *db, size_t queue_size, ObImportLogInfo &logInfo)
{
  int ret = OB_SUCCESS;
  AppendableFile *bad_file = NULL;
  if (param.bad_file_ != NULL)
  {
    TBSYS_LOG(INFO, "using bad file name = %s", param.bad_file_);
    ret = AppendableFile::NewAppendableFile(param.bad_file_, bad_file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can't create appendable file %s", param.bad_file_);
      //return ret; //delete by zhuxh: 20150721
      error_arr[CREATE_BAD_FILE_ERROR] = false; //add by zhuxh: 20150721
    }
  }

  AppendableFile *correct_file = NULL;
  if (param.correctfile_ != NULL)
  {
    TBSYS_LOG(INFO, "using correct file name = %s", param.correctfile_);
    ret = AppendableFile::NewAppendableFile(param.correctfile_, correct_file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can't create appendable file %s", param.correctfile_);
      return ret;
    }
  }

  reader.set_bad_file(bad_file);
  ImportProducer producer(reader, param.delima, param.rec_delima, param.is_rowbyrow, param.record_persql_);
  ImportComsumer comsumer(db, builder, bad_file, correct_file, param);

    TBSYS_LOG(INFO, "[delima]: type = %d, part1 = %d, part2 = %d", param.delima.delima_type(),
              param.delima.part1_, param.delima.part2_);
    TBSYS_LOG(INFO, "[rec_delima]: type = %d, part1 = %d, part2 = %d", param.rec_delima.delima_type(),
              param.rec_delima.part1_, param.rec_delima.part2_);

    ComsumerQueue<RecordBlock> queue(db, &producer, &comsumer, queue_size);
    if (queue.produce_and_comsume(1, param.ignore_merge_ ? 0 : 1, param.concurrency) != 0)
    {
      ret = OB_ERROR;
      error_arr[PRODUCE_AND_COMSUME_ERROR] = false; //add by zhuxh: 20150721
    }
    else
    {
      queue.dispose();
    }

    if (bad_file != NULL)
    {
      delete bad_file;
      bad_file = NULL;
    }
  if (correct_file != NULL)
  {
    delete correct_file;
    correct_file = NULL;
  }

  logInfo.set_wait_time_sec(queue.get_waittime_sec()); // add by zhangcd 20150804

  return ret;
}

int parse_table_title(Slice &slice, const ObSchemaManagerV2 &schema, TableParam &table_param)
{
  int ret = OB_SUCCESS;
  int token_nr = ObRowBuilder::kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];

  Tokenizer::tokenize(slice, table_param.delima, token_nr, tokens);
  int rowkey_count = 0;
  table_param.col_descs.clear();

//delete by zhuxh:20150727
#if 0
  if (0 != table_param.input_column_nr)
  {
    ret = OB_ERROR;
    error_arr[PARSE_TABLE_TITLE_ERROR] = false; //add by zhuxh: 20150721
    TBSYS_LOG(ERROR, "input_column_nr should not be set[%d]", table_param.input_column_nr);
    return ret;
  }
  table_param.input_column_nr = token_nr;
#endif

  const ObTableSchema *table_schema = schema.get_table_schema(table_param.table_name.c_str());

  if (NULL == table_schema)
  {
    ret = OB_ERROR;
    error_arr[PARSE_TABLE_TITLE_ERROR] = false; //add by zhuxh: 20150721
    TBSYS_LOG(ERROR, "cannot find table named [%s]", table_param.table_name.c_str());
  }

  if (OB_SUCCESS == ret)
  {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();

    for (int i = 0; i < token_nr; i ++)
    {
      std::string column_name(tokens[i].token, 0, tokens[i].len);
      const ObColumnSchemaV2* column_schema = schema.get_column_schema(table_param.table_name.c_str(), column_name.c_str());
      if (NULL == column_schema)
      {
        ret = OB_ERROR;
        error_arr[PARSE_TABLE_TITLE_ERROR] = false; //add by zhuxh: 20150721
        TBSYS_LOG(ERROR, "can't find column[%s] in table[%s]", column_name.c_str(), table_param.table_name.c_str());
        break;
      }
      else
      {
        ColumnDesc col_desc;
        col_desc.name = column_name;
        col_desc.offset = i;
        col_desc.len = static_cast<int>(tokens[i].len);
        table_param.col_descs.push_back(col_desc);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (rowkey_count != rowkey_info.get_size())
      {
        ret = OB_ERROR;
        error_arr[PARSE_TABLE_TITLE_ERROR] = false; //add by zhuxh: 20150721
        TBSYS_LOG(ERROR, "don't contain all rowkey column, please check data file. list rowkey count[%d]. actual rowkeycount[%ld]", 
          rowkey_count, rowkey_info.get_size());
      }
    }
  }
  return ret;
}
//modify by pangtz:20141206 添加ObImportLogInfo logInfo参数
int do_work(/*const char *config_file, const char *table_name,*/ //delete by zhuxh:20150724
            const char *host, unsigned short port, size_t queue_size,
            TableParam &cmd_table_param, ObImportLogInfo &logInfo)
//add e
{
  ImportParam param;
  TableParam table_param;

  int ret = OB_SUCCESS;

//delete by zhuxh:20150723
#if 0
  int ret = param.load(config_file);
  if (ret != OB_SUCCESS)
  {
    error_arr[TABLE_CONF_NOT_EXIST] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
    TBSYS_LOG(ERROR, "can't load config file, please check file path");
    return ret;
  }

  ret = param.get_table_param(table_name, table_param);
  if (ret != OB_SUCCESS)
  {
    error_arr[CONF_FILE_ERROR] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
    TBSYS_LOG(ERROR, "no table=%s in config file, please check it", table_name);
    return ret;
  }
#endif

  if( OB_SUCCESS == ret ) //add by zhuxh:20150805
  {
    //add by zhuxh:20150724 :b
    //from config to argv
    //zxh #1
    //delima
    table_param.delima_str = cmd_table_param.delima_str;
    string_2_delima(table_param.delima_str, table_param.delima);
    //zxh #2
    //rec_delima
    table_param.rec_delima_str = cmd_table_param.rec_delima_str;
    string_2_delima(table_param.rec_delima_str, table_param.rec_delima);
    //zxh #3
    //null flag
    table_param.has_null_flag = cmd_table_param.has_null_flag;
    table_param.null_flag = cmd_table_param.null_flag;
    //zxh #4
    //substr
    table_param.has_substr = true; //TODO
#if 0
    table_param.substr_grammar = cmd_table_param.substr_grammar;
    if( table_param.has_substr )
      if( OB_SUCCESS != (ret = parse_substr_grammar(table_param.substr_grammar,table_param.substr_map)) )
      {
        error_arr[SUBSTR_GRAMMAR_ERROR] = false;
        TBSYS_LOG(ERROR, "%s : substr grammar error.", table_param.substr_grammar);
      }
#endif
  }
  if( OB_SUCCESS == ret ) //add by zhuxh:20150805
  {
    //zxh #5
    //table_name
    //mod by zhuxh:20160617 [sensitive case of database and table] :b
    table_param.table_name = cmd_table_param.table_name;
    if( ! cmd_table_param.case_sensitive )
    {
      string & s = table_param.table_name;
      for(int i=0;s[i];i++)
        s[i] = static_cast<char>(tolower(s[i]));
    }
    //mod :e
    //zxh #6
    //decimal to varchar
    table_param.has_decimal_to_varchar = cmd_table_param.has_decimal_to_varchar;
    table_param.decimal_to_varchar_grammar = cmd_table_param.decimal_to_varchar_grammar;
    //add :e

    //add by zhuxh:20150811 :b
    //zxh #7
    //valid columns
    table_param.has_column_map = cmd_table_param.has_column_map;
    table_param.column_map_grammar = cmd_table_param.column_map_grammar;
    //mod by zhuxh:20160907 [sensitive case of column map] :b
    if( table_param.has_column_map && ! cmd_table_param.case_sensitive && NULL != table_param.column_map_grammar )
    {
      char * & s = table_param.column_map_grammar;
      for(int i=0;s[i];i++)
        s[i] = static_cast<char>(tolower(s[i]));
    }
    //mod by zhuxh:20160907 [sensitive case of column map] :e
    //zxh #8
    //char delima
    table_param.has_char_delima = cmd_table_param.has_char_delima;
    table_param.char_delima = cmd_table_param.char_delima;
    table_param.columns_with_char_delima_grammar = cmd_table_param.columns_with_char_delima_grammar;

    //zxh #9
    //char delima
    //No grammar || grammar == "ALL"
    //=>
    //all columns are with char delimas
    table_param.all_columns_have_char_delima =
    table_param.has_char_delima
    &&
    (
      ! table_param.columns_with_char_delima_grammar
      ||
      strcmp( table_param.columns_with_char_delima_grammar, "ALL" )==0
    );
    //add :e

    //zxh #10
    //varchar is not null
    table_param.varchar_not_null = cmd_table_param.varchar_not_null;

    //trim_end_blank = table_param.is_trim_; //delete by pangtz:20141127
    if (cmd_table_param.data_file.size() != 0)
    {
      /* if cmd line specifies data file, use it */
      table_param.data_file = cmd_table_param.data_file;
    }

    //add by pangtz:20141126 将trim参数从命令行赋值到table_param
    table_param.trim_flag = cmd_table_param.trim_flag;
    //add:end
    table_param.is_delete = cmd_table_param.is_delete;
    table_param.is_rowbyrow = cmd_table_param.is_rowbyrow;

    //add by zhuxh:20160303 :b
    //zxh #11
    table_param.progress = cmd_table_param.progress;
    if(table_param.progress)
      fprintf(stdout,"processed good bad\n");
    //add :e

    table_param.keep_double_quotation_marks = cmd_table_param.keep_double_quotation_marks; //mod by zhuxh:20160804 [special quotation mark] //mod by zhuxh:20160912 [quotation mark]
  }
//add by pangtz:20141126 获取append参数赋值到table_param中
  if( OB_SUCCESS == ret && cmd_table_param.is_append_date_) //add by zhuxh:20150805
  {
	  table_param.is_append_date_ = cmd_table_param.is_append_date_;
    if(cmd_table_param.appended_date.size() != 0)
    {
      if(is_date_valid(cmd_table_param.appended_date.c_str()) == OB_SUCCESS)
      {
        table_param.appended_date = cmd_table_param.appended_date;
      }
      else
      {
        error_arr[PARAM_APPEND_ACCDATE_ERROR] = false; //add by zhuxh:20150722
        TBSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
        return OB_ERROR;
      }
    }
    else
    {
      error_arr[PARAM_APPEND_ACCDATE_ERROR] = false; //add by zhuxh:20150722
      TBSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
      return OB_ERROR;
    }
  }
//add:end

  if( OB_SUCCESS == ret ) //add by zhuxh:20150805
  {
    if (cmd_table_param.bad_file_ != NULL)
    {
      table_param.bad_file_ = cmd_table_param.bad_file_;
    }

    if(cmd_table_param.correctfile_ != NULL)
    {
      table_param.correctfile_ = cmd_table_param.correctfile_;
    }

    if (cmd_table_param.concurrency != 0)
    {
      table_param.concurrency = cmd_table_param.concurrency;
    }

    if (table_param.data_file.empty())
    {
      error_arr[DATAFLIE_NOT_EXIST] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
      TBSYS_LOG(ERROR, "no datafile is specified, no work to do, quiting");
      return OB_ERROR;
    }

    table_param.is_insert = cmd_table_param.is_insert;
    table_param.is_replace = cmd_table_param.is_replace;
    table_param.is_delete = cmd_table_param.is_delete;
    table_param.record_persql_ = cmd_table_param.record_persql_;
    table_param.ignore_merge_ = cmd_table_param.ignore_merge_;
    //add by zhangcd 20150814:b
    table_param.user_name_ = cmd_table_param.user_name_;
    table_param.passwd_ = cmd_table_param.passwd_;

    //mod by zhuxh:20160617 [sensitive case of database and table] :b
    table_param.db_name_ = cmd_table_param.db_name_;
    if( ! cmd_table_param.case_sensitive )
    {
      char * & s = table_param.db_name_;
      for(int i=0;s[i];i++)
        s[i] = static_cast<char>(tolower(s[i]));
    }
    //mod :e

    table_param.timeout_ = cmd_table_param.timeout_;
    //add:e
  }

  if( OB_SUCCESS == ret ) //add by zhuxh:20150805
  {
    //mod by zhuxh:20161018 [add class Charset] :b
    FileReader reader(table_param.data_file.c_str(),g_gbk_encoding);
    //reader.set_g2u(g_gbk_encoding); //add by zhuxh:20150825
    if( reader.fail() )
    {
      ret = OB_INIT_FAIL;
      TBSYS_LOG(ERROR, "can't init file reader");
    }
    OceanbaseDb db(host, port, 8 * kDefaultTimeout);
    if ( OB_SUCCESS == ret )
    {
      ret = db.init();
      if (ret != OB_SUCCESS)
      {
        error_arr[SYSTEM_ERROR] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
        TBSYS_LOG(ERROR, "can't init database,%s:%d", host, port);
      }
    //mod by zhuxh:20161018 [add class Charset] :e
    }
    ObSchemaManagerV2 *schema = NULL;
    if (ret == OB_SUCCESS)
    {
      schema = new(std::nothrow) ObSchemaManagerV2;
      if (schema == NULL)
      {
        error_arr[MEMORY_SHORTAGE] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
        TBSYS_LOG(ERROR, "no enough memory for schema");
        ret = OB_ERROR;
      }
    }

    if (ret == OB_SUCCESS)
    {
      RPC_WITH_RETIRES(db.fetch_schema(*schema), 5, ret);
      if (ret != OB_SUCCESS)
      {
        error_arr[SYSTEM_ERROR] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
        TBSYS_LOG(ERROR, "can't fetch schema from root server [%s:%d]", host, port);
      }
    }

    if (ret == OB_SUCCESS)
    {
      ret = reader.open();
      if (OB_SUCCESS != ret)
      {
        error_arr[DATAFLIE_NOT_EXIST] = false;//add by pangtz:20141205 //modify by zhuxh:20150720
        TBSYS_LOG(ERROR, "can't open reader: ret[%d]", ret);
      }
    }

  #if 0 //delete by pangtz:20141205
    if (ret == OB_SUCCESS)
    {
      if (table_param.has_table_title)
      {
        TBSYS_LOG(INFO, "parse table title from data file");
        ret = reader.get_records(rec_block, table_param.rec_delima, table_param.delima, 1);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't get record ret[%d]", ret);
        }

        if (ret == OB_SUCCESS)
        {
          if (!rec_block.next_record(slice))
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "can't get title row");
          }
          else if (OB_SUCCESS != (ret = parse_table_title(slice, *schema, table_param)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "can't parse table title ret[%d]", ret);
          }
        }
      }
    }
  #endif

    if (ret == OB_SUCCESS)
    {
      /* setup ObRowbuilder */
      ObRowBuilder builder(schema, table_param);
      //mod by zhuxh:20160603 [Sequence] :b
      std::vector<SequenceInfo> vseq;
      if( OB_SUCCESS != ( ret = builder.set_column_desc( vseq ) ) )
      {
        error_arr[COLUMN_DESC_SET_ERROR] = false;//add by pangtz:20141205 //modify by zhuxh:20150805
        TBSYS_LOG(ERROR, "Can't setup column descriptions.");
      }
      //mod by zhuxh:20160907 [cb sequence]
      else if( OB_SUCCESS != ( ret = cb_sequence(db,table_param.db_name_,table_param.table_name.c_str(),vseq,true) ) )
      {
        error_arr[COLUMN_DESC_SET_ERROR] = false;
        TBSYS_LOG(ERROR, "Creating Sequence failed.");
      }
      //mod :e
      if(ret == OB_SUCCESS)
      {
        //add by zhangcd 20150814:b
        ObString complete_table_name;
        char buffer[OB_MAX_COMPLETE_TABLE_NAME_LENGTH];
        complete_table_name.assign_buffer(buffer, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        if(table_param.db_name_ != NULL && strlen(table_param.db_name_) > 0)
        {
          complete_table_name.write(table_param.db_name_, (int)strlen(table_param.db_name_));
          complete_table_name.write(".", 1);
        }
        complete_table_name.write(table_param.table_name.c_str(), (int)table_param.table_name.length());
        //add:e

        const ColumnInfo *column_desc = NULL;
        int column_desc_nr = 0;
        if(OB_SUCCESS != builder.get_column_desc(column_desc, column_desc_nr))
        {
          error_arr[COLUMN_DESC_SET_ERROR] = false; //add by zhuxh:20150722
          TBSYS_LOG(ERROR, "column desc not set!");
          ret = OB_ERROR;
        }
        //mod by zhangcd 20150814:b
        else if(!builder.has_all_rowkey(column_desc, column_desc_nr, complete_table_name))
        //mod:e
        {
          error_arr[ROWKEY_SET_ERROR] = false; //add by zhuxh:20150722
          TBSYS_LOG(ERROR, "rowkey not set correct!");
          ret = OB_ERROR;
        }
      }

      if (ret == OB_SUCCESS)
      {
        // mod by zhangcd 20150814
        // add a parameter to the function calling
        ret = run_comsumer_queue(reader, table_param, &builder, &db, queue_size, logInfo);
      }
      //add by pangtz:20141206 获取处理记录和失败记录信息
      logInfo.set_processed_lineno(builder.get_lineno());
      logInfo.set_bad_lineno(builder.get_bad_lineno());
      //add e

      //add by liyongfeng:20141020 判断monitor监控的状态
      if (OB_SUCCESS == ret)
      {
        if(!g_cluster_state)
        {
          //add by pangtz:20141204
          error_arr[RS_ERROR] = false; //modify by zhuxh:20150720
          //add e
          TBSYS_LOG(ERROR, "root server has switched or no root server, please re-run ob_import");
          ret = OB_ERROR;
        }
      }
      //add:end
      //add by zhuxh:20160603 [Sequence] :b
      /*
      //del by zhuxh:20160907 [cb sequence]
      if( OB_SUCCESS != ( ret = cb_sequence(db,table_param.db_name_,table_param.table_name.c_str(),vseq,false) ) )
      {
        error_arr[COLUMN_DESC_SET_ERROR] = false;
        TBSYS_LOG(ERROR, "Dropping Sequence failed.");
      }
      */
      //add :e
    }

    if (schema != NULL)
    {
      delete schema;
    }
  }
  return ret;
}

void handle_signal(int signo)
{
  switch (signo)
  {
    case 40:
      g_print_lineno_taggle = true;
      break;
    default:
      break;
  }
}

int main(int argc, char *argv[])
{
  srand( static_cast<unsigned int>(time(NULL)) ); //add by zhuxh:20160914 [retry mysql connection]

  //const char *config_file = NULL; //delete by zhuxh:20150728
  const char *table_name = NULL;
  const char *host = NULL;
  const char *log_file = NULL;
  const char *log_level = "INFO";
  TableParam cmd_table_param;
  cmd_table_param.concurrency = 0;
  cmd_table_param.is_replace = false;
  cmd_table_param.is_delete = false;
  cmd_table_param.is_insert = false;
  cmd_table_param.record_persql_ = 50;
  cmd_table_param.ignore_merge_ = false;
  //add by zhangcd 20150814:b
  cmd_table_param.user_name_ = "admin";
  cmd_table_param.passwd_ = "admin";
  cmd_table_param.db_name_ = NULL;
  cmd_table_param.timeout_ = 10;
  //add:e

  unsigned short port = 0;
  size_t queue_size = 1000;
  int ret = 0;
  signal(40, handle_signal);

  enum ParamKey{
    e_table_name = 't',
    e_delima = 2002,
    e_rec_delima = 2003,
    e_null_flag = 2004,
    e_substr = 2005,
    e_decimal_to_varchar = 2006,

    e_column_map = 2007,
    e_char_delima = 2008,
    e_columns_with_char_delima = 2009,

    e_varchar_not_null = 2010, //add by zhuxh:20151123

    e_progress = 2011, //add by zhuxh:20160303
    e_case_sensitive = 2012, //add by zhuxh:20160617 [sensitive case of database and table]

    //mod by zhuxh:20160912 [quotation mark] :b
    //e_special_quotation_mark = 2013 //mod by zhuxh:20160804 [special quotation mark]
    e_keep_double_quotation_marks = 2013,
    //mod by zhuxh:20160912 [quotation mark] :e
    e_del = 2014 //add by zhuxh:20161018 [add class Charset]
  }; //add by zhuxh:20150723

  static struct option long_options[] =
  {
    {"g2u", 0, 0, 1000}, //mod by zhuxh:20161018 [add class Charset]
    {"buffersize", 1, 0, 1001},
    {"badfile", 1, 0, 1002},
    {"concurrency", 1, 0, 1003},
//    {"del", 0, 0, 1004},
    {"rowbyrow", 0, 0, 1005},
    {"append",1, 0, 1006},//add by pangtz:20141126 添加append命令行选项
    {"trim", 1, 0, 1007},//add by pangtz:20141126 添加trim命令行选项
    {"rep", 0, 0, 1008},
    {"ins", 0, 0, 1009},
    {"record-persql", 1, 0, 1010},
    {"ignore-merge", 0, 0, 1011},
    {"correctfile", 1, 0, 1012},
    //add by zhangcd 20150814:b
    {"username", 1, 0, 1013},
    {"passwd", 1, 0, 1014},
    {"dbname", 1, 0, 1015},
    {"timeout", 1, 0, 1016},
    //add:e

    //add by zhuxh:20150723 :b
    {"table-name", 1, 0, e_table_name},
    {"delima", 1, 0, e_delima},
    {"rec-delima", 1, 0, e_rec_delima},
    {"null-flag", 1, 0, e_null_flag},
    {"substr", 1, 0, e_substr},
    {"decimal-to-varchar", 1, 0, e_decimal_to_varchar},
    //add :e

    //add by zhuxh:20150811 :b
    {"column-mapping", 1, 0, e_column_map},
    {"char-delima", 1, 0, e_char_delima},
    {"columns-with-char-delima", 1, 0, e_columns_with_char_delima},
    //add :e
    {"varchar-not-null", 0, 0, e_varchar_not_null},

    {"progress", 0, 0, e_progress}, //add by zhuxh:20160303
    {"case-sensitive", 0, 0, e_case_sensitive}, //add by zhuxh:20160617 [sensitive case of database and table]

    //mod by zhuxh:20160912 [quotation mark] :b
    //{"special-quotation-mark", 0, 0, e_special_quotation_mark}, //mod by zhuxh:20160804 [special quotation mark]
    {"keep-double-quotation-marks", 0, 0, e_keep_double_quotation_marks},
    //mod by zhuxh:20160912 [quotation mark] :e
    {"del",0,0,e_del}, //add by zhuxh:20161018 [add class Charset]
    {0, 0, 0, 0}
  };
  int option_index = 0;

  while ((ret = getopt_long(argc, argv, "h:p:t:c:l:g:q:f:HV", long_options, &option_index)) != -1)
  {
    switch (ret)
    {
//delete by zhuxh:20150723
#if 0
    case 'c':
      config_file = optarg;
      break;
#endif
    case 't':
      //del by zhuxh:20160617 [sensitive case of database and table]
      /*
      for(int i=0;optarg[i];i++) //add by zhuxh 20151111
        optarg[i] = static_cast<char>(tolower(optarg[i]));
      */
      table_name = optarg;
      cmd_table_param.table_name = optarg;
      break;
    case 'h':
      host = optarg;
      break;
    case 'l':
      log_file = optarg;
      break;
    case 'g':
      log_level = optarg;
      break;
    case 'p':
      port = static_cast<unsigned short>(atoi(optarg));
      break;
    case 'q':
      queue_size = static_cast<size_t>(atol(optarg));
      break;
    case 'f':
      cmd_table_param.data_file = optarg;
      break;
    case 1000:
      g_gbk_encoding = true;
      break;
    case 1001:
      kReadBufferSize = static_cast<int>(atol(optarg)) * 1024;
      break;
    case 1002:
      cmd_table_param.bad_file_ = optarg;
      break;
    case 1003:
      cmd_table_param.concurrency = static_cast<int>(atol(optarg));
      break;
//    case 1004:
//      cmd_table_param.is_delete = true;
//      break;
    case 1005:
      cmd_table_param.is_rowbyrow = true;
      break;
    case 1006:
     cmd_table_param.is_append_date_ = true;
     cmd_table_param.appended_date = optarg;
     break;
    case 1007:
     cmd_table_param.trim_flag = static_cast<int>(atol(optarg));
     break;
    case 'V':
     print_version();
     exit(0);
     break;
    //case 'H':
    case 1008:
      cmd_table_param.is_replace = true;
      break;
    case 1009:
      cmd_table_param.is_insert = true;
      break;
    case 1010:
      cmd_table_param.record_persql_ = static_cast<int64_t>(atoll(optarg));
      break;
    case 1011:
      cmd_table_param.ignore_merge_ = true;
      break;
    case 1012:
      cmd_table_param.correctfile_ = optarg;
      break;
    //add by zhangcd 20150814:b
    case 1013:
      cmd_table_param.user_name_ = optarg;
      break;
    case 1014:
      cmd_table_param.passwd_ = optarg;
      break;
    case 1015:
      //for(int i=0;optarg[i];i++) //add by zhuxh 20151111
        //optarg[i] = static_cast<char>(tolower(optarg[i]));
      cmd_table_param.db_name_ = optarg;
      break;
    case 1016:
      cmd_table_param.timeout_ = atoi(optarg);
      break;
    //add:e

    //add by zhuxh:20150723 :b
    case e_delima:
      cmd_table_param.delima_str = optarg;
      break;
    case e_rec_delima:
      cmd_table_param.rec_delima_str = optarg;
      break;
    case e_null_flag:
      cmd_table_param.has_null_flag = true;
      cmd_table_param.null_flag = static_cast<char>(atoi(optarg));
      break;
    case e_substr:
      cmd_table_param.has_substr = true;
      cmd_table_param.substr_grammar = optarg;
      break;
    case e_decimal_to_varchar:
      cmd_table_param.has_decimal_to_varchar = true;
      cmd_table_param.decimal_to_varchar_grammar = optarg;
      break;
    //add :e

    //add by zhuxh:20150811 :b
    case e_column_map:
      cmd_table_param.has_column_map = true;
      cmd_table_param.column_map_grammar = optarg;
      break;
    case e_char_delima:
      cmd_table_param.has_char_delima = true;
      cmd_table_param.char_delima = static_cast<char>(atoi(optarg));
      break;
    case e_columns_with_char_delima:
      cmd_table_param.has_char_delima = true;
      cmd_table_param.columns_with_char_delima_grammar = optarg;
      break;
    //add :e

    case e_varchar_not_null:
      cmd_table_param.varchar_not_null = true;
      break;

    case e_progress:
      cmd_table_param.progress = true;
      break;

    case e_case_sensitive:
      cmd_table_param.case_sensitive = true;
      break;

    //mod by zhuxh:20160912 [quotation mark] :b
    /*
    case e_special_quotation_mark:
      cmd_table_param.special_quotation_mark = true;
    */
    case e_keep_double_quotation_marks:
      cmd_table_param.keep_double_quotation_marks = true;
    //mod by zhuxh:20160912 [quotation mark] :e
      break;

    //add by zhuxh:20161018 [add class Charset] :b
    case e_del:
      cmd_table_param.is_delete = true;
      break;
    //add by zhuxh:20161018 [add class Charset] :e

    default:
      usage(argv[0]);
      exit(0);
      break;
    }
  }

  if(cmd_table_param.record_persql_ <= 0)
  {
    usage(argv[0]);
    exit(0);
  }

  //add by zhangcd [add a parameter to if-condition]20150814
  if (/*!config_file ||*/ !table_name || !cmd_table_param.db_name_ || !host || !port) //delete by zhuxh:20150728
  {
    usage(argv[0]);
    exit(0);
  }

  //判断三者只能取其一
  int tmp_value = (0x1 & (cmd_table_param.is_delete ? 0xf:0x0)) |
                  (0x2 & (cmd_table_param.is_replace ? 0xf:0x0)) |
                  (0x4 & (cmd_table_param.is_insert ? 0xf:0x0));
  if(tmp_value == 0)
  {
    //cmd_table_param.is_replace = true; //mod by zhuxh:20150917
    cmd_table_param.is_insert = true;
  }
  else if(!(tmp_value == 1 || tmp_value == 2 || tmp_value == 4))
  {
    usage(argv[0]);
    exit(0);
  }

  //add by pangtz:20141206
  ret = 0;
  ObImportLogInfo logInfo;
  logInfo.set_begin_time();
  //add e
  if (log_file != NULL)
    TBSYS_LOGGER.setFileName(log_file,true);
  OceanbaseDb::global_init(log_file, log_level);
  //modify by pangtz:20141206 添加ObImportLogInfo logInfo参数
  //return do_work(config_file, table_name, host, port, queue_size, cmd_table_param);
  ret = do_work(/*config_file, table_name, */host, port, queue_size, cmd_table_param, logInfo);
  //mod e
  //add by pangtz:20141206
  logInfo.set_table_name(table_name);
  logInfo.set_datafile_name(cmd_table_param.data_file);
  logInfo.set_final_ret(ret);
  logInfo.set_end_time();
  sleep(1);
  logInfo.print_error_log();
  return logInfo.get_final_ret();
  //add e
}
