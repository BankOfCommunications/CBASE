/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include <getopt.h>
#include <locale.h>

#include "libgen.h"
#include "base_main.h"
#include "ob_define.h"
#include "ob_trace_log.h"
#include "easy_log.h"
#include "common/ob_easy_log.h"
#include "common/ob_profile_fill_log.h"
#include "common/ob_profile_log.h"
#include "common/ob_preload.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_phy_operator_type.h"

namespace
{
  const char* DEFAULT_PID_DIR = "./run";
  const char* DEFAULT_LOG_DIR = "./log";
}
namespace oceanbase
{
  namespace common
  {
    BaseMain* BaseMain::instance_ = NULL;
    bool BaseMain::restart_ = false;
    bool PACKET_RECORDER_FLAG = false;

    BaseMain::BaseMain(const bool daemon)
      : cmd_cluster_id_(0), cmd_rs_port_(0), cmd_master_rs_port_(0), cmd_port_(0),
        //add pangtianze [Paxos rs_election] 20150721:b
        cmd_max_rs_count_(0),
        //add:e
        //add pangtianze [Paxos rs_election] 201501020:b
        cmd_is_use_paxos_(true),
        //add:e
        cmd_inner_port_(0), cmd_obmysql_port_(0), pid_dir_(DEFAULT_PID_DIR),
        log_dir_(DEFAULT_LOG_DIR), server_name_(NULL), use_daemon_(daemon)
    {
      setlocale(LC_ALL, "");
      memset(config_, 0, sizeof (config_));
      memset(cmd_rs_ip_, 0, sizeof (cmd_rs_ip_));
      memset(cmd_data_dir_, 0, sizeof (cmd_data_dir_));
      memset(cmd_prefix_dir_, 0, sizeof (cmd_prefix_dir_));
      memset(cmd_master_rs_ip_, 0, sizeof (cmd_master_rs_ip_));
      memset(cmd_datadir_, 0, sizeof (cmd_datadir_));
      memset(cmd_appname_, 0, sizeof (cmd_appname_));
      memset(cmd_devname_, 0, sizeof (cmd_devname_));
      memset(cmd_extra_config_, 0, sizeof (cmd_extra_config_));
      memset(cmd_ms_type_, 0, sizeof(cmd_ms_type_));
    }
    BaseMain::~BaseMain()
    {
    }
    void BaseMain::destroy()
    {
      signal(SIGTERM, SIG_IGN);
      signal(SIGINT, SIG_IGN);
      if (instance_ != NULL)
      {
        delete instance_;
        instance_ = NULL;
      }
    }
    void BaseMain::sign_handler(const int sig)
    {
      TBSYS_LOG(WARN, "receive signal sig=%d", sig);
      switch (sig) {
        case SIGTERM:
        case SIGINT:
          break;
        case 40:
          TBSYS_LOGGER.checkFile();
          break;
        case 41:
        case 42:
          if(sig == 41) {
            TBSYS_LOGGER._level++;
          }
          else {
            TBSYS_LOGGER._level--;
          }
          TBSYS_LOG(INFO, "TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
          break;
        case 43:
        case 44:
          if (43 == sig)
          {
            INC_TRACE_LOG_LEVEL();
          }
          else
          {
            DEC_TRACE_LOG_LEVEL();
          }
          break;
        case 45:
        case 46:
          if (46 == sig)
          {
            ObProfileLogger::getInstance()->setLogLevel("DEBUG");
          }
          else
          {
            ObProfileLogger::getInstance()->setLogLevel("INFO");
          }
          break;
        case 47:
        case 48:
          if (47 == sig)
          {
            easy_log_level = static_cast<easy_log_level_t>(static_cast<int>(easy_log_level) + 1);
          }
          else
          {
            easy_log_level = static_cast<easy_log_level_t>(static_cast<int>(easy_log_level) - 1);
          }
          TBSYS_LOG(INFO, "easy_log_level: %d", easy_log_level);
          break;
        case 49:
          ob_print_mod_memory_usage();
          sql::ob_print_phy_operator_stat();
          sql::ObPhyPlanTCFactory::get_instance()->stat();
          sql::ObPhyPlanGFactory::get_instance()->stat();
          break;
        case 50:
          ObProfileFillLog::open();
          break;
        case 51:
          ObProfileFillLog::close();
          break;
        case 52:
          PACKET_RECORDER_FLAG = !PACKET_RECORDER_FLAG;
          TBSYS_LOG(INFO, "toggle packet_record=%s", PACKET_RECORDER_FLAG ? "ON":"OFF");
          break;
      }
      if (instance_ != NULL) instance_->do_signal(sig);
    }
    void BaseMain::do_signal(const int sig)
    {
      UNUSED(sig);
      return;
    }
    void BaseMain::add_signal_catched(const int sig)
    {
      signal(sig, BaseMain::sign_handler);
    }
    void BaseMain::parse_cmd_line(const int argc,  char *const argv[])
    {
      int opt = 0;
      //mod pangtianze [Paxos rs_election] 20150721:b
      //const char* opt_string = "r:R:p:i:C:c:n:m:o:z:D:P:hNVt:f:";
      const char* opt_string = "r:R:p:i:C:c:n:m:o:z:D:P:hNVt:f:u:U:";
      //mod:e
      struct option longopts[] =
        {
          {"rootserver", 1, NULL, 'r'},
          {"port", 1, NULL, 'p'},
          {"data_dir", 1, NULL, 'D'},
          {"prefix_dir", 1, NULL, 'P'},
          {"interface", 1, NULL, 'i'},
          {"config", 1, NULL, 'c'},
          {"cluster_id", 1, NULL, 'C'},
          {"appname", 1, NULL, 'n'},
          {"inner_port", 1, NULL, 'm'},
          {"master_rootserver", 1, NULL, 'R'},
          {"mysql_port", 1, NULL, 'z'},
          {"help", 0, NULL, 'h'},
          {"version", 0, NULL, 'V'},
          {"no_daemon", 0, NULL, 'N'},
          {"extra_config", 0, NULL, 'o'},
          {"ms_type", 0, NULL, 't'},
          {"proxy_config_file", 0, NULL, 'f'},
          //add wangjiahao [Paxos ups_replication] 20150604 :b
          {"quorum_scale", 1, NULL, 'u'},
          //add :e
          //add pangtianze [Paxos rs_election] 20150721:b
          {"max_rs_count", 1, NULL, 'U'},
          //add:e
          {0, 0, 0, 0}
        };

      while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
        switch (opt) {
          case 'r':
            snprintf(cmd_rs_ip_, OB_IP_STR_BUFF, "%s", optarg);
            break;
          case 'f':
            snprintf(proxy_config_file_, sizeof (proxy_config_file_), "%s", optarg);
            break;
          case 'R':
            snprintf(cmd_master_rs_ip_, OB_IP_STR_BUFF, "%s", optarg);
            break;
          case 'c':
            snprintf(config_, sizeof (config_), "%s", optarg);
            break;
          case 'C':
            cmd_cluster_id_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            break;
          case 'o':
            snprintf(cmd_extra_config_, sizeof (cmd_extra_config_), "%s", optarg);
            break;
          case 'p':
            cmd_port_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            break;
          case 'D':
            snprintf(cmd_data_dir_, sizeof (cmd_data_dir_), "%s", optarg);
            break;
          case 'P':
            snprintf(cmd_prefix_dir_, sizeof (cmd_prefix_dir_), "%s", optarg);
            break;
          case 'i':
            snprintf(cmd_devname_, sizeof (cmd_devname_), "%s", optarg);
            break;
          case 'n':
            snprintf(cmd_appname_, sizeof (cmd_appname_), "%s", optarg);
            break;
          case 'm':
            cmd_inner_port_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            break;
          case 'z':
            cmd_obmysql_port_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            break;
          case 'V':
            print_version();
            exit(1);
          case 'h':
            print_usage(basename(argv[0]));
            exit(1);
          case 'N':
            use_daemon_ = false;
            break;
          case 't':
            snprintf(cmd_ms_type_, sizeof(cmd_ms_type_), "%s", optarg);
            break;
          //add wangjiahao [Paxos ups_replication] 20150601 :b
          case 'u':
            cmd_ups_quorum_scale_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            break;
          //add pangtianze [Paxos rs_election] 20150721:b
          case 'U':
            cmd_max_rs_count_ = static_cast<int32_t>(strtol(optarg, NULL, 0));
            cmd_is_use_paxos_ = true;
            break;
          //add:e
          default:
            break;
        }
      }

      for (int64_t del = 0; del < OB_IP_STR_BUFF; del++)
      {
        if (':' == cmd_rs_ip_[del])
        {
          cmd_rs_ip_[del++] = '\0';
          char *endptr = NULL;
          cmd_rs_port_ = static_cast<int32_t>(
            strtol(cmd_rs_ip_ + del, &endptr, 10));
          if (endptr != cmd_rs_ip_ + del + strlen(cmd_rs_ip_ + del))
          {
            TBSYS_LOG(WARN, "rs address truncated!");
          }
          if (cmd_rs_port_ >= 65536 || cmd_rs_port_ <= 0)
          {
            TBSYS_LOG(ERROR, "rs port invalid: [%d]", cmd_rs_port_);
          }
          break;
        }
      }
      for (int64_t del = 0; del < OB_IP_STR_BUFF; del++)
      {
        if (':' == cmd_master_rs_ip_[del])
        {
          cmd_master_rs_ip_[del++] = '\0';
          char *endptr = NULL;
          cmd_master_rs_port_ = static_cast<int32_t>(
            strtol(cmd_master_rs_ip_ + del, &endptr, 10));
          if (endptr != cmd_master_rs_ip_ + del + strlen(cmd_master_rs_ip_ + del))
          {
            TBSYS_LOG(WARN, "Master obi rs address truncated!");
          }
          else if (cmd_master_rs_port_ >= 65536 || cmd_master_rs_port_ <= 0)
          {
            TBSYS_LOG(ERROR, "Master Obi rs port invalid: [%d]", cmd_master_rs_port_);
          }
          break;
        }
      }
    }
    void BaseMain::print_usage(const char *prog_name)
    {
      if (0 == strcmp("rootserver", prog_name))
      {
        fprintf(stderr, "rootserver\n"
                "\t-c|--config BIN_CONFIG_FILE\n"
                "\t-h|--help\n"
                "\t-i|--interface DEV\n"
                "\t-o|--extra_config name=value[,...]\n"
                "\t-r|--rootserver IP:PORT\n"
                "\t-C|--cluster_id CLUSTER_ID\n"
                "\t-N|--no_daemon\n"
                "\t-R|--master_rootserver IP:PORT\n"
                //add pangtianze [Paxos rs_election] 20150820:b
                "\t-U|--max_rs_count RS_PAXOS_NUM\n"
                "\t-u|--quorum_scale UPS_QUORUM_NUM\n"
                //add:e
                "\t-V|--version\n");
      }
      else if (0 == strcmp("chunkserver", prog_name))
      {
        fprintf(stderr, "chunkserver\n"
                "\t-c|--config BIN_CONFIG_FILE\n"
                "\t-h|--help\n"
                "\t-i|--interface DEV\n"
                "\t-n|--appname APPLICATION_NAME\n"
                "\t-o|--extra_config name=value[,...]\n"
                "\t-p|--port PORT\n"
                "\t-r|--rootserver IP:PORT\n"
                "\t-D|--data_dir CS_DATA_PATH\n"
                "\t-N|--no_daemon\n"
                "\t-V|--version\n");
      }
      else if (0 == strcmp("updateserver", prog_name))
      {
        fprintf(stderr, "updateserver\n"
                "\t-c|--config BIN_CONFIG_FILE\n"
                "\t-h|--help\n"
                "\t-i|--interface DEV\n"
                "\t-m|--inner_port PORT\n"
                "\t-n|--appname APPLICATION_NAME\n"
                "\t-o|--extra_config name=value[,...]\n"
                "\t-p|--port PORT\n"
                "\t-r|--rootserver IP:PORT\n"
                "\t-N|--no_daemon\n"
                "\t-V|--version\n");
      }
      else if (0 == strcmp("mergeserver", prog_name))
      {
        fprintf(stderr, "mergeserver\n"
                "\t-c|--config BIN_CONFIG_FILE\n"
                "\t-h|--help\n"
                "\t-i|--interface DEV\n"
                "\t-o|--extra_config name=value[,...]\n"
                "\t-p|--port PORT\n"
                "\t-r|--rootserver IP:PORT\n"
                "\t-z|--mysql_port PORT\n"
                "\t-N|--no_daemon\n"
                "\t-V|--version\n");
      }
      else if (0 == strcmp("proxyserver", prog_name))
      {
        fprintf(stderr, "proxyserver\n"
                "\t-h|--help\n"
                "\t-i|--interface DEV\n"
                "\t-p|--port PORT\n"
                "\t-f|--proxy config\n"
                "\t-o|--extra_config name=value[,...]\n"
                "\t-N|--no_daemon\n"
                "\t-V|--version\n");
      }
      else
      {
        fprintf(stderr, "what SERVER are you run?? [%s]\n", prog_name);
      }
    }
    void BaseMain::print_version()
    {
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
    }

    int BaseMain::start(const int argc, char *argv[])
    {
      int ret = EXIT_SUCCESS;
      easy_log_format = ob_easy_log_format;
      parse_cmd_line(argc, argv);
      server_name_ = basename(argv[0]);
      char pid_file[OB_MAX_FILE_NAME_LENGTH];
      char log_file[OB_MAX_FILE_NAME_LENGTH];
      if (EXIT_SUCCESS == ret)
      {
        int pid = 0;

        snprintf(pid_file, sizeof (pid_file), "%s", pid_dir_);
        if(!tbsys::CFileUtil::mkdirs(pid_file)) {
          fprintf(stderr, "create dir %s error\n", pid_file);
          ret = EXIT_FAILURE;
        }

        snprintf(pid_file, sizeof (pid_file),
                 "%s/%s.pid", pid_dir_, server_name_);
        if((pid = tbsys::CProcess::existPid(pid_file))) {
          fprintf(stderr, "program has been exist: pid=%d\n", pid);
          ret = EXIT_FAILURE;
        }
      }
      if (EXIT_SUCCESS == ret)
      {
        snprintf(log_file, sizeof (log_file), "%s", log_dir_);
        if(!tbsys::CFileUtil::mkdirs(log_file)) {
          fprintf(stderr, "create dir %s error\n", log_file);
          ret = EXIT_FAILURE;
        }
        else
        {
          snprintf(log_file, sizeof (log_file),
                   "%s/%s.log", log_dir_, server_name_);

          /* @TODO */
          /* const char * sz_log_level = */
          /*   TBSYS_CONFIG.getString(section_name, LOG_LEVEL, "info"); */
          TBSYS_LOGGER.setLogLevel("info");
          easy_log_level = EASY_LOG_INFO;
          /* const char * trace_log_level = */
          /*   TBSYS_CONFIG.getString(section_name, TRACE_LOG_LEVEL, "debug"); */
          SET_TRACE_LOG_LEVEL("trace");
          /* int max_file_size= TBSYS_CONFIG.getInt(section_name, MAX_LOG_FILE_SIZE, 1024); */
          TBSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L); /* 256M */
        }
      }
      /*
      if (EXIT_SUCCESS == ret)
      {
        ObProfileLogger *logger = ObProfileLogger::getInstance();
          logger->setLogLevel("INFO");
          char profile_dir_path[512];
          snprintf(profile_dir_path, 512,
                   "%s/%s.profile", log_dir_, server_name_);
          logger->setLogDir(profile_dir_path);

          if (OB_SUCCESS != (ret = logger->init()))
          {
            TBSYS_LOG(ERROR, "init ObProfileLogger error, ret: [%d]", ret);
          }
      }
      */
      if (EXIT_SUCCESS == ret)
      {
        char profile_dir_path[512];
        snprintf(profile_dir_path, 512,
            "%s/%s.profile", log_dir_, server_name_);
        ObProfileFillLog::setLogDir(profile_dir_path);
        ObProfileFillLog::init();
      }
      if (EXIT_SUCCESS == ret)
      {
        bool start_ok = true;

        if (use_daemon_)
        {
          start_ok = (tbsys::CProcess::startDaemon(pid_file, log_file) == 0);
        }
        if(start_ok)
        {
          if (use_daemon_)
          {
            bt("enable_preload_bt") = 1;
          }
          signal(SIGPIPE, SIG_IGN);
          signal(SIGHUP, SIG_IGN);
          add_signal_catched(SIGINT);
          add_signal_catched(SIGTERM);
          add_signal_catched(40);
          add_signal_catched(41);
          add_signal_catched(42);
          add_signal_catched(43);
          add_signal_catched(44);
          add_signal_catched(45);
          add_signal_catched(46);
          add_signal_catched(47);
          add_signal_catched(48);
          add_signal_catched(49);
          add_signal_catched(50);
          add_signal_catched(51);
          add_signal_catched(52);
          ret = do_work();
          TBSYS_LOG(INFO, "exit program.");
        }
      }

      return ret;
    }


    void BaseMain::set_restart_flag(bool restart)
    {
      restart_ = restart;
    }

    bool BaseMain::restart_server(int argc, char* argv[])
    {
      pid_t pid;
      bool ret;

      for (int i = 0; ret && i < argc; i++)
      {
        if (argv[i] == NULL)
        {
          TBSYS_LOG(WARN, "invalid arguments, restart has cancled");
          ret = false;
        }
      }

      if (ret &&restart_)
      {
        if (0 == (pid = vfork())) // child
        {
          execv(argv[0], argv);
        }
        else if (-1 == pid)
        {
          // fork fails.
          TBSYS_LOG(ERROR, "fork process error! errno = [%d]", errno);
          ret = false;
        }
        else                      // father
        {
          ret = true;
        }
      }
      return ret;
    }

  }
}
