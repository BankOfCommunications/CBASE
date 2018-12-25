#include <string>
#include <vector>
#include <getopt.h>
#include "oceanbase_db.h"
#include "common/ob_string.h"
#include "common/ob_version.h"
#include "file_writer.h"
#include "ob_export_param.h"
#include "ob_export_extra_param.h"
#include "ob_export_producer.h"
#include "ob_export_consumer.h"
#include "ob_export_monitor.h"
#include "ob_export_queue.h"

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace std;

bool trim_end_blank = false;
static bool flag = true;

void print_version()
{
  fprintf(stderr, "ob_export (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
  fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
  fprintf(stderr, "Copyrigth (c) 2013-2014 bankcommon Inc.\n");
}

void usage(const char *prog) //mod by zhuxh [modified for bug fix] 20170518
{
  fprintf(stderr,
          "\n"
          "#======================= Example =======================\n"
          "~/cb2/tools/dump/ob_export -h 182.119.80.102 -p 2500 -t epccdb.epcc_tran_flw -l e.log -f epcc_tran_flw.dat --delima 64 --select-statement \"SELECT EPF_BATCHID,EPF_PAYERACCTYPE,EPF_PAYEEACCTYPE,EPF_SERIALNO,EPF_CURRENCY,EPF_AMOUNT,EPF_TRANTYPE,EPF_TRANSTT,EPF_INSTGID,EPF_PAYERINSTGID,CASE WHEN EPF_RPFLAG = '1' THEN EPF_BANKSERIALNO ELSE '999' END EPF_BANKSERIALNO,EPF_PAYERINSTGID,EPF_PAYEEINSTGID,CASE WHEN EPF_RPFLAG = '2' THEN EPF_BANKSERIALNO ELSE '999' END EPF_BANKSERIALNO,EPF_PAYEEINSTGID,EPF_RESERVEACCNO FROM epccdb.epcc_tran_flw WHERE EPF_BATCHID BETWEEN 'B201703010001' AND 'B201703019999' \"\n"
          "\n"
          "#======================= Usage =========================\n"
          "# Commonly used params:\n"
          "\t-t database_name.table_name\n"
          "\t-h rs_ip\n"
          "\t-p rs_port\n"
          "\t-l log_file\n"
          "\t-f data_file_name\n"
          "\t[--delima column_delima], denary column separator, 15 as default\n"
          "\t[--select-statement SQL]\n"
          "\t[-V] [version]\n"
          "\t[-H] [help]\n"

          "\n"
          "# Use carefully if you truly understand:\n"
          "\t[--multi-tables] \n"
          "\t\tUsed only for NOT TOO LARGE result set. You need this param in any case as follows:\n"
          "\t\t(1) All rowkeys with equal sign as verb in 'where' condition. E.G. select * from t where c0 is null and c1 = 'hello'. c0 and c1 are all of rowkeys\n"
          "\t\t(2) Multiple tables mentioned in sql. E.G. join sql.\n"
          "\t\t(3) You think the sql executes swifter when indexes is used.\n"
          "\t\tThe param of [-t] is useless, but you still need to write a following existing table]\n"
          "\t[--masterpercent] how much percent of ms in master cluster can be used, 100%% as default\n"
          "\t[--slavepercent]  how much percent of ms in slave  cluster can be used, 0%%   as default\n"
          "\t[--rec-delima record_delima], denary row separator, 10 as default\n"
          "\t[--g2u] no param, charset GBK convert to UTF-8\n"
          "\t[--u2g] no param, charset UTF-8 convert to GBK\n"
          "\t[--add-quotes] add quotes(\") to columns values\n" //add dinggh [add_quotes] 20160908
          "\t[-c config_file]\n"
          "\t[--case-sensitive] When you has uppercase name of database or table. E.G. when you try to export a table from default database TANG.\n"

          "\n"
          "# Only for developer\n"
          "\t[--badfile] write bad records, if provided, default is not storing bad records\n"
          "\t[--init-limittime] the limit time for each sql,    20 min as default\n"
          "\t[--max-limittime]  the limit time for each tablet, 30 min as default\n"
          "\t[--consumer-concurrency] concurrency of consumers, between 1 and 30, 3 as default\n"
          "\t[--multi-files] Different tablets writen into different files\n"
          "\t[-q queue_size] 1000 as default\n"
          "\t[-g log_level], 5 options, DEBUG|WARN|INFO|TRACE|ERROR, INFO as default\n"
          "\t[--maxfilesize] data file max file size, %ld MB as default\n"
          "\t[--maxrecordnum] data file max reocrd num, %ld Million as default\n"
          , /*basename(prog),*/ kDefaultMaxFileSize / (1024*1024), kDefaultMaxRecordNum);
  UNUSED(prog);
}

#if 1
struct ExportCmdParam
{
  ExportCmdParam() : config_file(NULL),
		     table_name(NULL),
		     host(NULL),
		     port(0),
		     log_file(NULL),
		     queue_size(1000),
		     data_file(NULL),
		     log_level(NULL),
             export_sql(NULL),
             delima('\017'),
             rec_delima('\n'),
             bad_file(NULL),
             master_percent(100),//mod qianzm 20160705
             slave_percent(50),
             init_limittime(20*60*1000*1000),//20分钟
             max_limittime(MAX_TIMEOUT_US),//25分钟
             consumer_concurrency(3),
             producer_concurrency(0),  //add by zhuxh [producer concurrency set] 20170518
             is_multi_tables(0), //add qianzm [multi-tables] 20151228
             is_multi_files(false),//add qianzm [export_by_tablets] 20160524
             case_sensitive(false),//add qianzm [db_name_sensitive] 20160617
             //add qianzm [charset] 20160629:b
             g2u(false),
             u2g(false),
             //add 20160629:e
             add_quotes(NULL)//add dinggh [add_quotes] 20160908
		{ }
		     
  void printDebug()
  {
    TBSYS_LOG(TRACE, "config file=%s", config_file);
    TBSYS_LOG(TRACE, "table name=%s", table_name);
    TBSYS_LOG(TRACE, "host=%s, port=%u", host, port);
    TBSYS_LOG(TRACE, "log file=%s", log_file);
    TBSYS_LOG(TRACE, "queue size=%u", queue_size);
    TBSYS_LOG(TRACE, "data file=%s", data_file);
    TBSYS_LOG(TRACE, "log level=%s", log_level);
    TBSYS_LOG(TRACE, "master percent=%d%%", master_percent);
    TBSYS_LOG(TRACE, "slave percent=%d%%", slave_percent);
    TBSYS_LOG(TRACE, "init_limittime=%ldus", init_limittime);
    TBSYS_LOG(TRACE, "max_limittime=%ldus", max_limittime);
    TBSYS_LOG(TRACE, "consumer_concurrency=%d", consumer_concurrency);
    TBSYS_LOG(TRACE, "is_multi_tables=%d", is_multi_tables);//add qianzm [multi-tables] 20151228
  }

  const char *config_file;
  const char *table_name;
  const char *host;
  unsigned short port;
  const char *log_file;
  unsigned int queue_size;
  const char *data_file;
  const char *log_level;
  const char *export_sql;
  RecordDelima delima;
  RecordDelima rec_delima;
  const char *bad_file;
  int master_percent;
  int slave_percent;
  int64_t init_limittime;
  int64_t max_limittime;
  int consumer_concurrency;
  int producer_concurrency; //add by zhuxh [producer concurrency set] 20170518
  int is_multi_tables;//add qianzm [multi-tables] 20151228
  bool is_multi_files;//add qianzm [export_by_tablets] 20160524
  bool case_sensitive;//add qianzm [db_name_sensitive] 20160617
  //add qianzm [charset] 20160629:b
  bool g2u;
  bool u2g;
  //add 20160629:e
  const char *add_quotes;//add dinggh [add_quotes] 20160908
  string real_table_name;//add qianzm [db_name_sensitive] 20160803
};
#endif

int format_time_str(const int64_t time_us, char *buffer, int64_t capacity, const char *format = "%Y-%m-%d %H:%M:%S")
{
  struct tm time_struct;//broken-down time分解时间
  int64_t time_s = time_us / 1000000;//微秒转为秒
  int ret = OB_SUCCESS;
  if (NULL != localtime_r(&time_s, &time_struct))
  {
    if (0 == strftime(buffer, capacity, format, &time_struct))
    {
      ret = OB_ERROR;
    }
  }
  return ret;
}

void parse_delima(const char *delima_str, RecordDelima &delima)
{
  const char *end_pos = delima_str + strlen(delima_str);
  if (find(delima_str, end_pos, ',') == end_pos)
  {
     delima = RecordDelima(static_cast<char>(atoi(delima_str)));
  }
  else
  {
     int part1, part2;
     sscanf(delima_str, "%d,%d", &part1, &part2);
     delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
  }
}

int parse_options(int argc, char *argv[], ExportCmdParam &cmd_param)
{
  int ret = OB_SUCCESS;
  int res = 0;

  int option_index = 0;

  static struct option long_options[] = {
      {"delima", 1, 0, 1000},
      {"rec-delima", 1, 0, 1001},//mod by zhuxh:20151230
      {"select-statement", 1, 0, 1002},//mod by zhuxh:20151230
      {"maxfilesize", 1, 0, 1003},
      {"maxrecordnum", 1, 0, 1004},
      {"badfile", 1, 0, 1005},
      {"masterpercent", 1, 0, 1006},
      {"slavepercent", 1, 0, 1007},
      {"init-limittime", 1, 0, 1008},
      {"max-limittime", 1, 0, 1009},
      {"consumer-concurrency", 1, 0, 1010},
      {"multi-tables", 0, 0, 1011},//mod by zhuxh:20151230
      {"multi-files",0, 0, 1012},//add qianzm [export_by_tablets] 20160524
      {"case-sensitive", 0, 0, 1013},//add qianzm [db_name_sensitive] 20160617
      //add qianzm [charset] 20160629:b
      {"g2u", 0, 0, 1014},
      {"u2g", 0, 0, 1015},
      //add 20160629:e
      {"add-quotes", 1, 0, 1016},//add dinggh [add-quotes] 20160908

      {"producer-concurrency", 1, 0, 1017},  //add by zhuxh [producer concurrency set] 20170518
      {0, 0, 0, 0}
  };

  while ((OB_SUCCESS == ret) && ((res = getopt_long(argc, argv, "c:t:h:p:l:q:f:g:HV", long_options, &option_index)) != -1))
  {
    switch (res) 
    {
      case 'c':
        cmd_param.config_file = optarg;
        break;
      case 't':
        //del by qianzm [db_name_sensitive] 20160617
		cmd_param.table_name = optarg;
		//del 20160617:e
        break;
      case 'h':
        cmd_param.host = optarg;
        break;
      case 'p':
        cmd_param.port = static_cast<unsigned short>(atoi(optarg));
        break;
      case 'l':
        cmd_param.log_file = optarg;
        break;
      case 'q':
        cmd_param.queue_size = static_cast<unsigned int>(atoi(optarg));
        break;
      case 'f':
        cmd_param.data_file = optarg;
        break;
      case 'g':
        cmd_param.log_level = optarg;
        break;
      case 1000:
        parse_delima(optarg, cmd_param.delima);
        break;      
      case 1001:
        parse_delima(optarg, cmd_param.rec_delima);
        break;
      case 1002:
        cmd_param.export_sql = optarg;
        break;
      case 1003:
        kDefaultMaxFileSize = static_cast<int64_t>(atol(optarg)) * 1024 * 1024;
        flag = true;
        break;
      case 1004:
        kDefaultMaxRecordNum = static_cast<int64_t>(atol(optarg)) * 100 * 100 * 100;
        flag = false;
        break;
      case 1005:
        cmd_param.bad_file = optarg;
        break;
      case 1006:
        {
          errno = 0;
          int percent = 0;
          int n = sscanf(optarg, "%d%%", &percent);
          if(1 == n)
          {
            cmd_param.master_percent = percent;
          }
          else
          {
            cmd_param.master_percent = -1;
          }
        }
        break;
      case 1007:
        {
          errno = 0;
          int percent = 0;
          int n = sscanf(optarg, "%d%%", &percent);
          if(1 == n)
          {
            cmd_param.slave_percent = percent;
          }
          else
          {
            cmd_param.slave_percent = -1;
          }
        }
        break;
      case 1008:
        {
          cmd_param.init_limittime = static_cast<int64_t>(atol(optarg)) * 1000 * 1000;
        }
        break;
      case 1009:
        {
          cmd_param.max_limittime = static_cast<int64_t>(atol(optarg)) * 1000 * 1000;
        }
        break;
      case 1010:
        cmd_param.consumer_concurrency = static_cast<int>(atoi(optarg));
        break;
        //add qianzm [multi-tables] 20151229
      case 1011:
        cmd_param.is_multi_tables = 1;
        break;
        //add qianzm [export_by_tablets] 20160524
      case 1012:
        cmd_param.is_multi_files = true;
        break;
        //add 20160524:e
        //add qianzm [db_name_sensitive] 20160617:b
      case 1013:
        cmd_param.case_sensitive = true;
        break;
        //add 20160615:e
      //add qianzm [charset] 20160629:b
      case 1014:
        cmd_param.g2u = true;
        break;
      case 1015:
        cmd_param.u2g = true;
        break;
      //add 20160629:e
      //add dinggh [add_quotes] 20160908:b
      case 1016:
        cmd_param.add_quotes = optarg;
        break;
      //add 20160908:e

      //add by zhuxh [producer concurrency set] 20170518 :b
      case 1017:
        cmd_param.producer_concurrency = static_cast<int>(atoi(optarg));
        break;
      //add by zhuxh [producer concurrency set] 20170518 :e

      case 'V':
        print_version();
        exit(OB_SUCCESS);
        break;
      case 'H':
        usage(argv[0]);
        exit(OB_SUCCESS);
        break;
      default:
        usage(argv[0]);
        exit(OB_ERROR);
        break;
    }
  }
  if (//(NULL == cmd_param.config_file) //mod by zhuxh:20151230
       /*||*/ (NULL == cmd_param.table_name)
       || (NULL == cmd_param.host) 
       || (0 == cmd_param.port) 
       || (NULL == cmd_param.data_file)
       || (0 > kDefaultMaxFileSize)
       || (0 > kDefaultMaxRecordNum)
       || (0 == cmd_param.queue_size)
       || (0 > cmd_param.slave_percent)
       || (100 < cmd_param.slave_percent)
       || (0 > cmd_param.master_percent)
       || (100 < cmd_param.master_percent)
       || (0 > cmd_param.init_limittime)
       || (0 > cmd_param.max_limittime)
       || (0 >= cmd_param.consumer_concurrency)
       || (30 < cmd_param.consumer_concurrency)
     )
  {
    usage(argv[0]);
    ret = OB_ERROR;
  }

  return ret;
}

int run_dump_table(const ObExportTableParam &table_param, const ExportCmdParam &cmd_param)
{
  int ret = OB_SUCCESS;

  int64_t start_timestamp = tbsys::CTimeUtil::getTime();

  OceanbaseDb db(cmd_param.host, cmd_param.port, cmd_param.init_limittime);
  if (OB_SUCCESS != (ret = db.init()))
  {
    TBSYS_LOG(ERROR, "OceanbaseDb init failed, ret=[%d]", ret);
    return ret;
  }
  /* 获取schema */
  ObSchemaManagerV2 *schema = NULL;
  if (OB_SUCCESS == ret)
  {
    schema = new(std::nothrow) ObSchemaManagerV2;
    if (NULL == schema)
    {
      TBSYS_LOG(ERROR, "no enough memory for schema");
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret)
  {
    RPC_WITH_RETIRES(db.fetch_schema(*schema), 5, ret);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "failed to fetch schema from root server, ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "fetch schema failed, ret=[%d]", ret);
    return ret;
  }

  /* 获取MS list,进而获取并发度 */
  std::vector<ObServer> ms_vec;
  int concurrency = 0;
  if (OB_SUCCESS != (ret = db.fetch_ms_list(cmd_param.master_percent, cmd_param.slave_percent, ms_vec)))
  {
    TBSYS_LOG(ERROR, "fetch_ms_list failed! ret=[%d]", ret);
  }

  if (ms_vec.size() == 0)
  {
    TBSYS_LOG(ERROR, "there is no available ms!");
    ret = OB_ERROR;
  }
  //add by zhuxh [producer concurrency set] 20170518 :b
  else if( cmd_param.producer_concurrency > 0 ) //When you set producer number by command param
  {
    concurrency = cmd_param.producer_concurrency;
  }
  //add by zhuxh [producer concurrency set] 20170518 :e
  else
  {
    concurrency = static_cast<unsigned int>(ms_vec.size());
  }

#if 1
  /* 初始化bad file */
  AppendableFile *bad_file = NULL;
  if (OB_SUCCESS == ret)
  {
    if (cmd_param.bad_file != NULL)
    {
      TBSYS_LOG(DEBUG, "using bad file name = %s", cmd_param.bad_file);
      ret = AppendableFile::NewAppendableFile(cmd_param.bad_file, bad_file);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "can't create appendable bad file [%s]", cmd_param.bad_file);
        ret = OB_SUCCESS;
      }
    }
    else
    {
      TBSYS_LOG(WARN, "user not specify bad file");
    }
  }
#endif
  /* 初始化数据文件 */
  FileWriter fw;
  if (OB_SUCCESS == ret)
  {
    if ((ret = fw.init_file_name(cmd_param.data_file)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "FileWrite init failed, ret=[%d]", ret);
    }
    else
    {
      if (flag)//根据命令行中指定的先后顺序,以最后一个为准;如果不指定,使用kDefaultMaxFileSize
      {
        fw.set_max_file_size(kDefaultMaxFileSize);
      }
      else
      {
        fw.set_max_record_count(kDefaultMaxRecordNum);
      }
    }
  }
  
  DbRowFormator formator(schema, table_param);
  if (OB_SUCCESS == ret)/* 初始化formator */
  {
    if (OB_SUCCESS != (ret = formator.init()))
    {
      TBSYS_LOG(ERROR, "db row formator init failed, ret=[%d]", ret);
    }
    //del by zhuxh:20161018 [add class Charset]
    /*
    //add qianzm [charset] 20160629:b
    else if (cmd_param.u2g)
    {
      formator.set_u2g();
    }
    else if (cmd_param.g2u)
    {
      formator.set_g2u();
    }
    //add 20160629:e
    */
  }

  //多线程情况下作为线程异常退出判断使用
  ErrorCode err_code;
  if (OB_SUCCESS == ret)
  {
    //监控线程，负责监控当前进程中其他线程的状态和用户的输入，如果发现问题则会做一些收尾处理
    ExportMonitor monitor(&db, &err_code);
    monitor.start_monitor();

    //ExportProducer producer(&db, schema, cmd_param.table_name, &err_code, table_param, &formator, ms_vec,  cmd_param.max_limittime,
    //                        /*add qianzm [export_by_tablets] 20160524*/cmd_param.is_multi_files);
    ExportProducer producer(&db, schema, cmd_param.real_table_name.c_str(), &err_code, table_param, &formator, ms_vec,  cmd_param.max_limittime, cmd_param.is_multi_files, concurrency);//mod qianzm [db_name_sensitive] 20160803 //mod by zhuxh [producer concurrency set] 20170518
    ExportConsumer consumer(&fw, &formator, &err_code, table_param, bad_file,
                            /*add qianzm [export_by_tablets] 20160524*/cmd_param.is_multi_files);
    ComsumerQueue<TabletBlock*> queue(&producer, &consumer, cmd_param.queue_size);

    //add qianzm [multi-tables] 20151228
    if (table_param.is_multi_tables_ != 0)
    {
      queue.set_is_multi_tables();
      if (queue.produce_and_comsume(1, cmd_param.consumer_concurrency) != 0)/* 这里的并发度已经去除,需要获取MS list同时获取并发度 */
      {
        TBSYS_LOG(ERROR, "queue.produce_and_comsume failed!");
        ret = OB_ERROR;
      }
      else
      {
        queue.dispose();
      }
    }
    //add e
    else
    {
      //add qianzm [export_by_tablets] 20160415:b
      string data_file_name(cmd_param.data_file);
      queue.set_data_file_name(data_file_name);
      //add 20160415:e
      if (queue.produce_and_comsume(concurrency, cmd_param.consumer_concurrency) != 0)/* 这里的并发度已经去除,需要获取MS list同时获取并发度 */
      {
        TBSYS_LOG(ERROR, "queue.produce_and_comsume failed!");
        ret = OB_ERROR;
      }
      else
      {
        queue.dispose();
      }
    }
    //通知监控线程退出
    monitor.set_exit_signal(true);
    monitor.wait();

    //判断工作线程退出的状态，如果是异常退出，需要返回异常返回码给用户
    if (OB_SUCCESS != err_code.get_err_code())
    {
      ret = err_code.get_err_code();
      TBSYS_LOG(ERROR, "consumer producer exit with fail code, ret=[%d]", ret);    
    }
	
    //如果是正常执行结束，则将执行时间、记录数等信息输出到屏幕
    if (OB_SUCCESS == ret)
    {
      int64_t end_timestamp = tbsys::CTimeUtil::getTime();
      char buffer_start[1024];
      char buffer_end[1024];

      format_time_str(start_timestamp, buffer_start, 1024);
      format_time_str(end_timestamp, buffer_end, 1024);
      fprintf(stdout, "total record count=%d, success record count=%d, failed record count=%d\n", 
                       consumer.get_total_succ_rec_count() + consumer.get_total_failed_rec_count(),
                       consumer.get_total_succ_rec_count(), consumer.get_total_failed_rec_count());

      TBSYS_LOG(INFO, "total record count=%d, success record count=%d, failed record count=%d",
                       consumer.get_total_succ_rec_count() + consumer.get_total_failed_rec_count(),
                       consumer.get_total_succ_rec_count(), consumer.get_total_failed_rec_count());

      fprintf(stdout, "start time=%s, end time=%s, total time=%lds\n", 
                       buffer_start, buffer_end, 
                       (end_timestamp - start_timestamp)/(1000*1000));

      TBSYS_LOG(INFO, "start time=%s, end time=%s, total time=%lds", 
                       buffer_start, buffer_end, 
                       (end_timestamp - start_timestamp)/(1000*1000));
    }
  }

  
  if (bad_file != NULL)
  {
    delete bad_file;
    bad_file = NULL;
  }
  if (schema != NULL)
  {
    delete schema;
    schema = NULL;
  }

  return ret;
}

int do_work(const ExportCmdParam& cmd_param)
{
  int ret = OB_SUCCESS;
  
  ExportParam param;
  ObExportTableParam table_param;

  table_param.is_multi_tables_ = cmd_param.is_multi_tables;//add qianzm [multi-tables] 20151228
  //新建一个用日志文件名加相应的后缀的文件，把执行失败的sql写入该文件中
  table_param.tablet_info_file = cmd_param.log_file;// add qianzm [export_by_tablets] 20160415
  //加载和解析配置文件
  //mod qianzm [no config file] 20151109:b
  if (!cmd_param.config_file || 0 == strncmp(cmd_param.config_file,"null",4))//mod by zhuxh:20151230
  {
    //table_param.table_name = cmd_param.table_name;
	table_param.table_name = cmd_param.real_table_name;//mod qianzm [db_name_sensitive] 20160803
  }
  else
  {
    if (OB_SUCCESS != (ret = param.load(cmd_param.config_file))) {
      TBSYS_LOG(ERROR, "can't load config file, please check file path[%s]", cmd_param.config_file);
    } 
	//else if (OB_SUCCESS != (ret = param.get_table_param(cmd_param.table_name, table_param))) {
	else if (OB_SUCCESS != (ret = param.get_table_param(cmd_param.real_table_name.c_str(), table_param))) {//mod qianzm [db_name_sensitive] 20160803
      TBSYS_LOG(ERROR, "no table=%s in config file, please check it", cmd_param.table_name);
    }
  }
  //mod :e
  
  //获取到table_param后,需要将其与命令行中一些参数进行融合,以命令行参数为主
  if (OB_SUCCESS == ret)
  {
    //add dinggh [add_quotes] 20160908:b
    if (cmd_param.add_quotes != NULL)
    {
      string value = cmd_param.add_quotes;
      Tokenizer::tokenize(value, table_param.add_quotes, ',');
      if (table_param.add_quotes.size() <= 0)
      {
        TBSYS_LOG(WARN, "add_quotes has no values");
      }
    }
    //add dinggh 20160908:e
    //对分割符进行整合
    if(cmd_param.delima.type_ == RecordDelima::CHAR_DELIMA)
    {
      table_param.delima.set_char_delima(cmd_param.delima.part1_);
    }
    else
    {
      table_param.delima.set_short_delima(cmd_param.delima.part1_, cmd_param.delima.part2_);
    }

//  cmd_param.delima.type_ == RecordDelima::CHAR_DELIMA ? table_param.delima.set_char_delima(cmd_param.delima.part1_) : table_param.delima.set_short_delima(cmd_param.delima.part1_, cmd_param.delima.part2_);

    if(cmd_param.rec_delima.type_ == RecordDelima::CHAR_DELIMA)
    {
      table_param.rec_delima.set_char_delima(cmd_param.rec_delima.part1_);
    }
    else
    {
      table_param.rec_delima.set_short_delima(cmd_param.rec_delima.part1_, cmd_param.rec_delima.part2_);
    }
//  cmd_param.rec_delima.type_ == RecordDelima::CHAR_DELIMA ? table_param.rec_delima.set_char_delima(cmd_param.rec_delima.part1_) : table_param.rec_delima.set_short_delima(cmd_param.rec_delima.part1_, cmd_param.rec_delima.part2_);

    //对查询SQL进行整合
    if (NULL != cmd_param.export_sql)
    {
      //add by zhuxh [no index for range] 20170518 :b
      const char * s = cmd_param.export_sql;
      int i=0;

      for( ; s[i] && isspace(s[i]); i++ );
      if( strncasecmp(s+i,"select",6) == 0 )
      {
        if( ! cmd_param.is_multi_tables )
        {
          table_param.export_sql += "select /*+not_use_index()*/ ";
          i+=6;
        }

        table_param.export_sql += s+i;
      }
      else
        ret = OB_INIT_SQL_CONTEXT_ERROR;
      //add by zhuxh [no index for range] 20170518 :e
    }
    else if (table_param.export_sql.empty())
    {
      //TBSYS_LOG(ERROR, "no select statement specified, please check");
      //ret = OB_ERROR;
      //如果不报错的话,可使用默认SQL,即查询全表数据的SQL
	  //mod qianzm [db_name_sensitive] 20160617:b
	  //table_param.export_sql = "select * from " + table_param.table_name;
	  if (!cmd_param.case_sensitive)
	  {
	      /*for(int i=0;table_param.table_name[i];i++) //add by zhuxh 20151230
              table_param.table_name[i] = static_cast<char>(tolower(table_param.table_name[i]));*/
        table_param.table_name = cmd_param.real_table_name;//mod qianzm [db_name_sensitive] 20160803
        table_param.export_sql = "select /*+not_use_index()*/ * from " + table_param.table_name; //mod by zhuxh [no index for range] 20170518
	  }
	  else
	  {
        string t_name = "\"";
        for (int i=0; i<(int)table_param.table_name.length(); i++)
        {
          if (table_param.table_name[i] != '.')
            t_name = t_name + table_param.table_name[i];
          else
            t_name = t_name + "\"" + table_param.table_name[i] + "\"";//库名和表名两边要单独加双引号
        }
        t_name += "\"";
        table_param.export_sql = "select /*+not_use_index()*/ * from " + t_name; //mod by zhuxh [no index for range] 20170518
	  }
	  //mod 20160617:e
    }
    char buff[OB_MAX_SQL_LENGTH];
    if (OB_SUCCESS != (ret = remove_last_comma(table_param.export_sql.c_str(), buff, OB_MAX_SQL_LENGTH)))
    {
      TBSYS_LOG(ERROR, "remove the last comma failed!");
    }
    table_param.export_sql = buff;
  }

  //add by zhuxh:20161018 [add class Charset] :b
  if ( cmd_param.u2g && cmd_param.g2u )
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "Parameter u2g & g2u cannot be both used, ret=[%d]", ret);
  }
  else if (cmd_param.g2u)
  {
    table_param.code_conversion_flag = e_g2u;
  }
  else if (cmd_param.u2g)
  {
    table_param.code_conversion_flag = e_u2g;
  }
  //add by zhuxh:20161018 [add class Charset] :e

  if (OB_SUCCESS == ret)
  {
    ret = run_dump_table(table_param, cmd_param);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "run dump table failed! ret=[%d]", ret);
    }
  }
  return ret;
}

int main(int argc, char *argv[])
{
  /*
 * 命令行参数的结构体，存储所有的命令行参数值
 */
  ExportCmdParam cmd_param;
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = parse_options(argc, argv, cmd_param))) {
    return ret;
  }
  //add qianzm [db_name_sensitive] 20160803:b
  cmd_param.real_table_name = cmd_param.table_name;
  if (!cmd_param.case_sensitive)
  {
    for(int i=0; cmd_param.table_name[i]; i++)
      cmd_param.real_table_name[i] = static_cast<char>(tolower(cmd_param.table_name[i]));
  }
  //add 20160803:e

  if (NULL != cmd_param.log_file)
    TBSYS_LOGGER.setFileName(cmd_param.log_file, true);
  if (NULL != cmd_param.log_level) {
    OceanbaseDb::global_init(NULL, cmd_param.log_level);
  } else {
    OceanbaseDb::global_init(NULL, "INFO");
  }

  cmd_param.printDebug();

  /*
 * 开始启动工作流程
 */
 
  if (OB_SUCCESS != (ret = do_work(cmd_param)))
  {
    TBSYS_LOG(ERROR, "do work error! ret=[%d]", ret);
    fprintf(stderr, "some error occur, please check the logfile!\n");
  }
  else
  {
    fprintf(stdout, "all work done!\n");
  }

  return ret;
}
