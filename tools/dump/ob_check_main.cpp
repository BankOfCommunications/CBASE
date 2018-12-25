#include <getopt.h>
#include "ob_check.h"
#include "ob_check_producer.h"
#include "ob_check_comsumer.h"
#include "ob_check_param.h"
#include "common/ob_version.h"

//global param
bool g_gbk_encoding = false;
bool g_print_lineno_taggle = false;
bool trim_end_blank = false;
//add by pangtz
	bool error_arr[15] = {true, true, true,true, true, true,true, true, true,true, true, true,true, true,true};
//add e
//add by liyongfeng
void print_version()
{
	fprintf(stderr, "ob_check (%s %s)\n", PACKAGE_STRING, RELEASEID);
	fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
	fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
	fprintf(stderr, "Copyrigth (c) 2013-2014 Taobao Inc.\n");
}
//add:e

void usage(const char *prog)
{
  fprintf(stderr, "Usage:%s \n"
  		  "\t-c [config file] \n"
          "\t-t [table name]\n"
          "\t-h host\n"
		  "\t-p port\n"
          "\t-l [log file]\n"
          "\t-q [queue size, default 1000]\n"
          "\t[-f datafile]\n"
          "\t-g [log level]\n"
          "\t[--del]\n"
          "\t[--gbk]\n"
          "\t[--badfile]\n"
          "\t[--rowbyrow]\n"
          "\t[--concurrency] default is 10\n"
          "\t[--buffersize] KBRELEASEID default is %dKB\n"
		 //add by pangtz:20141126 添加trim命令行帮助信息
//		 "\t[--trim] (0-remove both spaces; 1-remove left spaces; 2-remove right spaces)\n"
		 //add:e
		 //add by pangtz:20141126 [append 会计日期]
//		  "\t[--append] only append accdate, for hdfs. sample format[1997-01-01]\n" 
		 //add:e
	  "\t-V [version]\n"
	  "\t-H [help]\n"
  //add by pangtz
	  "return value: \n"
	  				"\t0 normal\n"
	  				"\t1 non-data error, such as no data file, no config file, schema error...,  bad file may exist\n"
					"\t2 data error or unknow error, bad file exist\n"
  //add e
		,prog, kReadBufferSize / 1024);
}

/*
 * add by pangtz:20141126 判断append的会计日期是否是规定格式
 *
 */
int is_date_valid(const char * date){
	int ret = OB_SUCCESS;
	if(strlen(date) != 10)
		ret = OB_ERROR;
	if(ret == OB_SUCCESS){
		int num, year, month, day;
		num = sscanf(date, "%4d-%2d-%2d", &year, &month, &day);
		if(num ==3){
			ret = OB_SUCCESS;
		}else{
			ret = OB_ERROR;
		}
	}
	return ret;
}
//add:end


int run_comsumer_queue(FileReader &reader, TableParam &param, ObRowBuilder *builder, 
                       OceanbaseDb *db, size_t queue_size)
{
  int ret = OB_SUCCESS;
  AppendableFile *bad_file = NULL;
  if (param.bad_file_ != NULL) {
    TBSYS_LOG(INFO, "using bad file name = %s", param.bad_file_);
    ret = AppendableFile::NewAppendableFile(param.bad_file_, bad_file);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't create appendable file %s", param.bad_file_);
      return ret;
    }
  }

  reader.set_bad_file(bad_file);
  ImportProducer producer(reader, param.delima, param.rec_delima, param.is_rowbyrow);
  ImportComsumer comsumer(db, builder, bad_file, param);

  TBSYS_LOG(INFO, "[delima]: type = %d, part1 = %d, part2 = %d", param.delima.delima_type(), 
            param.delima.part1_, param.delima.part2_);
  TBSYS_LOG(INFO, "[rec_delima]: type = %d, part1 = %d, part2 = %d", param.rec_delima.delima_type(), 
            param.rec_delima.part1_, param.rec_delima.part2_);

  ComsumerQueue<RecordBlock> queue(db, &producer, &comsumer, queue_size);
  if (queue.produce_and_comsume(1, 0, param.concurrency) != 0) {
    ret = OB_ERROR;
  } else {
    queue.dispose();
  }

  if (bad_file != NULL)
  {
    delete bad_file;
    bad_file = NULL;
  }

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

  if (0 != table_param.input_column_nr) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "input_column_nr should not be set[%d]", table_param.input_column_nr);
    return ret;
  }
  table_param.input_column_nr = token_nr;

  const ObTableSchema *table_schema = schema.get_table_schema(table_param.table_name.c_str());

  if (NULL == table_schema) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "cannot find table named [%s]", table_param.table_name.c_str());
  }

  if (OB_SUCCESS == ret) {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();

    for (int i = 0; i < token_nr; i ++) {
      std::string column_name(tokens[i].token, 0, tokens[i].len);
      const ObColumnSchemaV2* column_schema = schema.get_column_schema(table_param.table_name.c_str(), column_name.c_str());
      if (NULL == column_schema) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't find column[%s] in table[%s]", column_name.c_str(), table_param.table_name.c_str());
        break;
      }
      else {
        ColumnDesc col_desc;
        col_desc.name = column_name;
        col_desc.offset = i;
        col_desc.len = static_cast<int>(tokens[i].len);
        table_param.col_descs.push_back(col_desc);
      }
    }

    if (OB_SUCCESS == ret) {
      if (rowkey_count != rowkey_info.get_size()) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "don't contain all rowkey column, please check data file. list rowkey count[%d]. actual rowkeycount[%ld]", 
          rowkey_count, rowkey_info.get_size());
      }
    }
  }
  return ret;
}
//modify by pangtz:20141206 添加ObImportLogInfo logInfo参数
int do_work(const char *config_file, const char *table_name, 
            const char *host, unsigned short port, size_t queue_size,
            TableParam &cmd_table_param, ObImportLogInfo &logInfo)
//add e
{
  ImportParam param;
  TableParam table_param;
  RecordBlock rec_block;
  Slice slice;

  int ret = param.load(config_file);
  if (ret != OB_SUCCESS) {
	error_arr[13] = false;//add by pangtz:20141205
    TBSYS_LOG(ERROR, "can't load config file, please check file path");
    return ret;
  }
  ret = param.get_table_param(table_name, table_param);
  if (ret != OB_SUCCESS) {
	  error_arr[11] = false;//add by pangtz:20141205
    TBSYS_LOG(ERROR, "no table=%s in config file, please check it", table_name);
    return ret;
  }
  
  //trim_end_blank = table_param.is_trim_; //delete by pangtz:20141127
  if (cmd_table_param.data_file.size() != 0) {              /* if cmd line specifies data file, use it */
    table_param.data_file = cmd_table_param.data_file;
  }
	
 //add by pangtz:20141126 将trim参数从命令行赋值到table_param
 table_param.trim_flag = cmd_table_param.trim_flag;
 //add:end
  table_param.is_delete = cmd_table_param.is_delete;
  table_param.is_rowbyrow = cmd_table_param.is_rowbyrow;

//add by pangtz:20141126 获取append参数赋值到table_param中
  if(cmd_table_param.is_append_date_){
	  table_param.is_append_date_ = cmd_table_param.is_append_date_;
  if(cmd_table_param.appended_date.size() != 0){
	  if(is_date_valid(cmd_table_param.appended_date.c_str()) == OB_SUCCESS){
		  table_param.appended_date = cmd_table_param.appended_date;
	  }else{
		  TBSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
		  return OB_ERROR;
	  }
  }else{
	  TBSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
	  return OB_ERROR;
  }
  }
//add:end

  if (cmd_table_param.bad_file_ != NULL) {
    table_param.bad_file_ = cmd_table_param.bad_file_;
  }

  if (cmd_table_param.concurrency != 0) {
    table_param.concurrency = cmd_table_param.concurrency;
  }

  if (table_param.data_file.empty()) {
	error_arr[1] = false;//add by pangtz:20141205 
    TBSYS_LOG(ERROR, "no datafile is specified, no work to do, quiting");
    return OB_ERROR;
  }

  FileReader reader(table_param.data_file.c_str());
  OceanbaseDb db(host, port, 8 * kDefaultTimeout);
  ret = db.init();
  if (ret != OB_SUCCESS) {
	error_arr[5] = false;//add by pangtz:20141205
    TBSYS_LOG(ERROR, "can't init database,%s:%d", host, port);
  }
  ObSchemaManagerV2 *schema = NULL;
  if (ret == OB_SUCCESS) {
    schema = new(std::nothrow) ObSchemaManagerV2;

    if (schema == NULL) {
		error_arr[14] = false;//add by pangtz:20141205
      TBSYS_LOG(ERROR, "no enough memory for schema");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    RPC_WITH_RETIRES(db.fetch_schema(*schema), 5, ret);
    if (ret != OB_SUCCESS) {
	   error_arr[5] = false;//add by pangtz:20141205
      TBSYS_LOG(ERROR, "can't fetch schema from root server [%s:%d]", host, port);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = reader.open();
    if (OB_SUCCESS != ret) {
		error_arr[1] = false;//add by pangtz:20141205
      TBSYS_LOG(ERROR, "can't open reader: ret[%d]", ret);
    }
  }

#if 0 //delete by pangtz:20141205
  if (ret == OB_SUCCESS) {
    if (table_param.has_table_title) {
      TBSYS_LOG(INFO, "parse table title from data file");
      ret = reader.get_records(rec_block, table_param.rec_delima, table_param.delima, 1);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't get record ret[%d]", ret);
      }

      if (ret == OB_SUCCESS) {
        if (!rec_block.next_record(slice)) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "can't get title row");
        }
        else if (OB_SUCCESS != (ret = parse_table_title(slice, *schema, table_param))) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "can't parse table title ret[%d]", ret);
        }
      }
    }
  }
#endif

  if (ret == OB_SUCCESS) {
    /* setup ObRowbuilder */
    ObRowBuilder builder(schema, table_param);
	
    ret = builder.set_column_desc(table_param.col_descs);
    if (ret != OB_SUCCESS) {
	error_arr[11] = false;//add by pangtz:20141205
      TBSYS_LOG(ERROR, "can't setup column descripes");
    }

    if (ret == OB_SUCCESS) {
      ret = run_comsumer_queue(reader, table_param, &builder, &db, queue_size);
    }
//add by pangtz:20141206 获取处理记录和失败记录信息
	logInfo.set_processed_lineno(builder.get_lineno());
	logInfo.set_bad_lineno(builder.get_bad_lineno());
//add e
	
	//add by liyongfeng:20141020 判断monitor监控的状态
	/*
	if (OB_SUCCESS == ret) {
		if(!g_cluster_state) {
			//add by pangtz:20141204
			error_arr[10] = false;
			//add e
			TBSYS_LOG(ERROR, "root server has swithced or no root server, please re-run ob_import");
			ret = OB_ERROR;
		}
	}
	*/
	//add:end

	//add by liyongfeng:20141020 判断是否发生主UPS切换
	/*
	if (OB_SUCCESS == ret) {
		//RPC_WITH_RETRIES_SLEEP(db.get_latest_update_server(), 3, ret);
		ret = db.get_latest_update_server();
		if (ret != OB_SUCCESS) {
			TBSYS_LOG(ERROR, "ob_import maybe lost some record, please re-run ob_import");
		}
	}
	*/
	//add:end
  }


  if (schema != NULL) {
    delete schema;
  }

  return ret;
}


void handle_signal(int signo)
{
  switch (signo) {
    case 40:
      g_print_lineno_taggle = true;
      break;
    default:
      break;
  }
}


int main(int argc, char *argv[])
{
  const char *config_file = NULL;
  const char *table_name = NULL;
  const char *host = NULL;
  const char *log_file = NULL;
  const char *log_level = "INFO";
  TableParam cmd_table_param;
  cmd_table_param.concurrency = 0;
  unsigned short port = 0;
  size_t queue_size = 1000;
  int ret = 0;
  signal(40, handle_signal);

  static struct option long_options[] = {
    {"gbk", 0, 0, 1000},
    {"buffersize", 1, 0, 1001},
    {"badfile", 1, 0, 1002},
    {"concurrency", 1, 0, 1003},
    {"del", 0, 0, 1004},
    {"rowbyrow", 0, 0, 1005},
	{"append",1, 0, 1006},//add by pangtz:20141126 添加append命令行选项
	{"trim", 1, 0, 1007},//add by pangtz:20141126 添加trim命令行选项
    {0, 0, 0, 0}
  };
  int option_index = 0;

  while ((ret = getopt_long(argc, argv, "h:p:t:c:l:g:q:f:HV", long_options, &option_index)) != -1) {
    switch (ret) {
     case 'c':
       config_file = optarg;
       break;
     case 't':
       table_name = optarg;
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
     case 1004:
       cmd_table_param.is_delete = true;
       break;
     case 1005:
       cmd_table_param.is_rowbyrow = true;
	   break;
	 //add by pangtz:20141119 添加append命令行选项
	 case 1006:
	 	 cmd_table_param.is_append_date_ = true;
		 cmd_table_param.appended_date = optarg;
       break;
	 //add:end
	 //add by pangtz:20141126 添加trim命令行选项
	 case 1007:
	 	cmd_table_param.trim_flag = static_cast<int>(atol(optarg));
		break;
	//add:end
     //add by liyongfeg
     case 'V':
	print_version();
	exit(0);
	break;
     case 'H':
     //add:e
     default:
       usage(argv[0]);
       exit(0);
       break;
    }
  }
  if (!config_file || !table_name || !host || !port) {
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
  ret = do_work(config_file, table_name, host, port, queue_size, cmd_table_param, logInfo);
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
