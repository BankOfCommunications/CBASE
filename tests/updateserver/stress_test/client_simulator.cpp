
#include "client_simulator.h"

using namespace oceanbase::common;

const char* Param::TR_TYPE_WRITE_STR = "WRITE";
const char* Param::TR_TYPE_USSCAN_STR = "USSCAN";
const char* Param::TR_TYPE_USGET_STR = "USGET";
const char* Param::LOG_PTYPE_NONE_STR = "NONE";
const char* Param::LOG_PTYPE_TR_STR = "TR";

const char* Param::TrTypeStr[TR_TYPE_MAX];
const char* Param::LogPtypeStr[LOG_PTYPE_MAX];

const char* Param::DEFAULT_LOG_LEVEL = "INFO";

Param::Param() : ip(NULL), port(INVALID_VAL),
                 schema_file(), thread_no(INVALID_VAL),
                 tps_limit(INVALID_VAL), total_tr_limit(INVALID_VAL),
                 timeout_us(INVALID_VAL),
                 tr_type(INVALID_VAL), size_p_tr(INVALID_VAL),
                 log_name(), log_level(NULL), log_ptype(INVALID_VAL)
{
  memset(TrTypeStr, 0x00, sizeof(TrTypeStr));
  memset(LogPtypeStr, 0x00, sizeof(LogPtypeStr));
  TrTypeStr[TR_TYPE_WRITE] = TR_TYPE_WRITE_STR;
  TrTypeStr[TR_TYPE_USSCAN] = TR_TYPE_USSCAN_STR;
  TrTypeStr[TR_TYPE_USGET] = TR_TYPE_USGET_STR;
  LogPtypeStr[LOG_PTYPE_NONE] = LOG_PTYPE_NONE_STR;
  LogPtypeStr[LOG_PTYPE_TR] = LOG_PTYPE_TR_STR;
}

void Param::print_usage(const char *prog_name)
{
  fprintf(stderr, "\nUsage: %s <-i Host_Address> <-p Host_Port> [other optional arguments]\n"
      "    -i, --ip           Rootserver Address\n"
      "    -p, --port         Rootserver Port\n"
      "    -c, --schema       Schema File\n"
      "    -t, --thread       Thread Number (default %d)\n"
      "    -a, --tps          TPS Limit (default %ld, 0 represent no limit)\n"
      "    -A, --total_tr     Total Transcation Limit (default %ld, 0 represent no limit)\n"
      "    -m, --timeout      Request Tiemout in us (default %ldus)\n"
      "    -o, --type         Transcation Type: %s or %s or %s (deafult %s)\n"
      "    -s, --size         Transcation Size Limit (default %ld, 0 represent no limit)\n"
      "    -l, --log_name     Log Name (default %s)\n"
      "    -e, --log_level    Log Level (ERROR|WARN|INFO|DEBUG, default %s)\n"
      "    -r, --log_ptype    Whether printing log per transaction: NONE or TR (default %s)\n"
      "    -h, --help         this help\n\n",
      prog_name, DEFAULT_THREAD_NO, DEFAULT_TPS_LIMIT, DEFAULT_TOTAL_TR_LIMIT,
      DEFAULT_TIMEOUT_US, 
      TR_TYPE_WRITE_STR, TR_TYPE_USSCAN_STR, TR_TYPE_USGET_STR, str_tr_type(DEFAULT_TR_TYPE),
      DEFAULT_SIZE_P_TR, buffer().assign(prog_name).append(".log").ptr(),
      DEFAULT_LOG_LEVEL, str_log_ptype(DEFAULT_LOG_PTYPE));
}

int Param::parse_cmd_line(const int argc,  char *const argv[])
{
  int ret = 0;

  int opt = 0;
  const char* opt_string = "i:p:c:t:a:A:m:o:s:l:e:r:h";
  struct option longopts[] = 
  {
    {"ip", 1, NULL, 'i'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'c'},
    {"thread", 1, NULL, 't'},
    {"tps", 1, NULL, 'a'},
    {"total_tr", 1, NULL, 'A'},
    {"timeout", 1, NULL, 'm'},
    {"type", 1, NULL, 'o'},
    {"size", 1, NULL, 's'},
    {"log_name", 1, NULL, 'l'},
    {"log_level", 1, NULL, 'e'},
    {"log_ptype", 1, NULL, 'r'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'i': ip = optarg; break;
      case 'p': port = atoi(optarg); break;
      case 'c': schema_file.assign(optarg); break;
      case 't': thread_no = atoi(optarg); break;
      case 'a': tps_limit = atol(optarg); break;
      case 'A': total_tr_limit = atol(optarg); break;
      case 'm': timeout_us = atol(optarg); break;
      case 'o':
        if (0 == strcasecmp(optarg, TR_TYPE_USSCAN_STR)) tr_type = TR_TYPE_USSCAN;
        else if (0 == strcasecmp(optarg, TR_TYPE_USGET_STR)) tr_type = TR_TYPE_USGET;
        else if (0 == strcasecmp(optarg, TR_TYPE_WRITE_STR)) tr_type = TR_TYPE_WRITE;
        else {TBSYS_LOG(ERROR, "Unknown argument: %s", optarg); ret = 1;}
        break;
      case 's': size_p_tr = atol(optarg); break;
      case 'l': log_name.assign(optarg); break;
      case 'e': log_level = optarg; break;
      case 'r':
        if (0 == strcasecmp(optarg, LOG_PTYPE_NONE_STR)) log_ptype = LOG_PTYPE_NONE;
        else if (0 == strcasecmp(optarg, LOG_PTYPE_TR_STR)) log_ptype = LOG_PTYPE_TR;
        else {TBSYS_LOG(ERROR, "Unknown argument: %s", optarg); ret = 1;}
        break;
      case 'h': print_usage(argv[0]); ret = -1; break;
      case '?': ret = 1; break;
      default: ret = 1; break;
    }
  }

  if (0 == ret)
  {
    ret = fill_default_param(argc, argv);
  }

  return ret;
}

int Param::fill_default_param(const int argc,  char *const argv[])
{
  int ret = 0;

  ////////// Host Address //////////
  if (NULL == ip)
  {
    TBSYS_LOG(ERROR, "Host Address needed");
    goto error_h;
  }
  else TBSYS_LOG(INFO, "Host Address: %s", ip);
  
  ////////// Host Port //////////
  if (INVALID_VAL == port)
  {
    TBSYS_LOG(ERROR, "Host Address needed");
    goto error_h;
  }
  else TBSYS_LOG(INFO, "Host Port: %d", port);

  ////////// Schema File //////////
  if (0 == schema_file.length())
  {
    TBSYS_LOG(ERROR, "Schema File needed");
    goto error_h;
  }
  else TBSYS_LOG(INFO, "Schema File: %s", schema_file.ptr());

  ////////// Thread Number //////////
  if (INVALID_VAL == thread_no)
  {
    thread_no = DEFAULT_THREAD_NO;
    TBSYS_LOG(INFO, "Thread Number using DEFAULT: %d", thread_no);
  }
  else TBSYS_LOG(INFO, "Thread Number: %d", thread_no);

  ////////// TPS Limit //////////
  if (INVALID_VAL == tps_limit)
  {
    tps_limit = DEFAULT_TPS_LIMIT;
    TBSYS_LOG(INFO, "TPS Limit using DEFAULT: %ld", tps_limit);
  }
  else TBSYS_LOG(INFO, "TPS Limit: %ld", tps_limit);

  ////////// Total Transaction Limit //////////
  if (INVALID_VAL == total_tr_limit)
  {
    total_tr_limit = DEFAULT_TOTAL_TR_LIMIT;
    TBSYS_LOG(INFO, "Total Transaction Limit using DEFAULT: %ld", total_tr_limit);
  }
  else TBSYS_LOG(INFO, "Total Transaction Limit: %ld", total_tr_limit);

  ////////// Request Timeout in us //////////
  if (INVALID_VAL == timeout_us)
  {
    timeout_us = DEFAULT_TIMEOUT_US;
    TBSYS_LOG(INFO, "Request Timeout using DEFAULT: %ldus", timeout_us);
  }
  else TBSYS_LOG(INFO, "Request Timeout: %ldus", timeout_us);

  ////////// Transaction Type //////////
  if (INVALID_VAL == tr_type)
  {
    tr_type = DEFAULT_TR_TYPE;
    TBSYS_LOG(INFO, "Transaction Type using DEFAULT: %s", str_tr_type(tr_type));
  }
  else TBSYS_LOG(INFO, "Transaction Type: %s", str_tr_type(tr_type));

  ////////// Transaction Size Limit //////////
  if (INVALID_VAL == size_p_tr)
  {
    size_p_tr = DEFAULT_SIZE_P_TR;
    TBSYS_LOG(INFO, "Transcation Size Limit using DEFAULT: %ld", size_p_tr);
  }
  else TBSYS_LOG(INFO, "Transcation Size Limit: %ld", size_p_tr);

  ////////// Log Name //////////
  if (0 == log_name.length())
  {
    log_name.assign(argv[0]).append(".log");
    TBSYS_LOG(INFO, "Log Name using DEFAULT: %s", log_name.ptr());
  }
  else TBSYS_LOG(INFO, "Log Name: %s", log_name.ptr());

  ////////// Log Level //////////
  if (NULL == log_level)
  {
    log_level = DEFAULT_LOG_LEVEL;
    TBSYS_LOG(INFO, "Log Level using DEFAULT: %s", log_level);
  }
  else TBSYS_LOG(INFO, "Log Level: %s", log_level);

  ////////// Log Ptype //////////
  if (INVALID_VAL == log_ptype)
  {
    log_ptype = DEFAULT_LOG_PTYPE;
    TBSYS_LOG(INFO, "Whether printing log per transaction using DEFAULT: %s", str_log_ptype(log_ptype));
  }
  else TBSYS_LOG(INFO, "Whether printing log per transaction: %s", str_log_ptype(log_ptype));

  goto normal_r;

error_h:
  print_usage(argv[0]);
  ret = 1;

normal_r:

  return ret;
}

OBAPI::OBAPI() : buff_(OB_MAX_PACKET_LENGTH)
{
}

OBAPI::~OBAPI()
{
  transport_.stop();
  transport_.wait();
}

int OBAPI::init(const ObServer &server)
{
  int ret = OB_SUCCESS;

  rs_ = server;
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  transport_.start();

  ret = init_us_addr_();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "init_us_addr_ error, ret=%d", ret);
  }
  else
  {
//    ret = init_ms_addr_();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init_ms_addr_ error, ret=%d", ret);
    }
  }

  return ret;
}

int OBAPI::apply(const common::ObMutator &mutator, const int64_t timeout)
{
  return apply_(us_, mutator, timeout);
}

int OBAPI::msscan(const common::ObScanParam& scan_param, const int64_t timeout, common::ObScanner& scanner)
{
  if (ms_arr_.size() > 0)
  {
    return scan_(random_ms_(), scan_param, timeout, scanner);
  }
  else
  {
    TBSYS_LOG(ERROR, "No Mergeserver available");
    return OB_ERROR;
  }
}

int OBAPI::msget(const common::ObGetParam& get_param, const int64_t timeout, common::ObScanner& scanner)
{
  if (ms_arr_.size() > 0)
  {
    return get_(random_ms_(), get_param, timeout, scanner);
  }
  else
  {
    TBSYS_LOG(ERROR, "No Mergeserver available");
    return OB_ERROR;
  }
}

int OBAPI::usscan(const common::ObScanParam& scan_param, const int64_t timeout, common::ObScanner& scanner)
{
  return scan_(us_, scan_param, timeout, scanner);
}

int OBAPI::usget(const common::ObGetParam& get_param, const int64_t timeout, common::ObScanner& scanner)
{
  return get_(us_, get_param, timeout, scanner);
}

int OBAPI::init_us_addr_()
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;

  ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
  ObResultCode res;

  if (OB_SUCCESS == ret)
  {
    ret = client_.send_request(rs_, OB_GET_UPDATE_SERVER_INFO, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    data_buff.get_position() = 0;
    ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
    }
    else
    {
      ret = us_.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObServer deserialize error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int OBAPI::init_ms_addr_()
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;

  ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
  ObResultCode res;
  rootserver::ObChunkServerManager ser_mgr;

  if (OB_SUCCESS == ret)
  {
    ret = client_.send_request(rs_, OB_DUMP_CS_INFO, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
    }
    else
    {
      ret = ser_mgr.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObServer deserialize error, ret=%d", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObServer ms;
    const rootserver::ObServerStatus *ss = NULL;
    for (ss = ser_mgr.begin(); ss != ser_mgr.end(); ++ss)
    {
      if (NULL == ss)
      {
        TBSYS_LOG(WARN, "Server in ObChunkServerManager is NULL, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ms.set_ipv4_addr(ss->server_.get_ipv4(), ss->port_ms_);
        ms_arr_.push_back(ms);
      }
    }
  }

  return ret;
}

int OBAPI::apply_(const common::ObServer &server, const common::ObMutator &mutator, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;

  common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

  ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS == ret)
  {
    ret = client_.send_request(server, OB_WRITE, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    // deserialize the response code
    int64_t pos = 0;
    if (OB_SUCCESS == ret)
    {
      ObResultCode result_code;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }
    }
  }
  return ret;
}

int OBAPI::scan_(const common::ObServer &server, const common::ObScanParam& scan_param,
    const int64_t timeout, common::ObScanner& scanner)
{
  int err = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

  err = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "ObScanParam serialize error, err=%d", err);
  }

  if (OB_SUCCESS == err)
  {
    err = client_.send_request(server, OB_SCAN_REQUEST, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to send request, err=%d", err);
    }
  }

  if (OB_SUCCESS == err)
  {
    // deserialize the response code
    int64_t pos = 0;
    if (OB_SUCCESS == err)
    {
      ObResultCode result_code;
      err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
      }
      else
      {
        err = result_code.result_code_;
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "scan result_code_ is %d", err);
        }
      }
    }
    // deserialize scanner
    if (OB_SUCCESS == err)
    {
      err = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize scanner from buff failed, "
            "pos[%ld], err[%d]", pos, err);
      }
    }
  }

  return err;
}

int OBAPI::get_(const common::ObServer &server, const ObGetParam& get_param, const int64_t timeout, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

  ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObGetParam serialize error, err=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_.send_request(server, OB_GET_REQUEST, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      char buf[BUFSIZ];
      server.to_string(buf, BUFSIZ);
      TBSYS_LOG(WARN, "server=%s", buf);
    }
  }

  if (OB_SUCCESS == ret)
  {
    // deserialize the response code
    int64_t pos = 0;
    if (OB_SUCCESS == ret)
    {
      ObResultCode result_code;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get result_code_ is %d", ret);
        }
      }
    }
    // deserialize scanner
    if (OB_SUCCESS == ret)
    {
      ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize scanner from buff failed, "
            "pos[%ld], ret[%d]", pos, ret);
      }
    }
  }

  return ret;
}

const common::ObServer& OBAPI::random_ms_() const
{
  if (ms_arr_.size() > 0)
  {
    return ms_arr_[GI::instance().random().rand64(ms_arr_.size())];
  }
  else
  {
    return us_;
  }
}

int64_t Random::rand64(int64_t min, int64_t max)
{
  if (min == max)
  {
    return min;
  }
  else
  {
    double r = erand48(xsubi_arr());
    int64_t range = max - min + 1;
    double v = trunc(r * range);
    int64_t ret = lrint(v) + min;
    return ret;
  }
}

void Random::init()
{
  int64_t time = tbsys::CTimeUtil::getTime();
  xsubi_arr()[0] = time & 0xFFFF;
  xsubi_arr()[1] = (time >> 2) & 0xFFFF;
  xsubi_arr()[2] = (time >> 4) & 0xFFFF;
}

void Random::randbuffer(buffer &buff)
{
  for (buffer::iterator bi = buff.begin(); bi != buff.capacity_end(); bi++)
  {
    *bi = randchar();
  }
  buff.length() = buff.capacity();
}

int SchemaProxy::init(const char* schema_file)
{
  int ret = 0;

  tbsys::CConfig config;
  if (!schema_.parse_from_file(schema_file, config))
  {
    TBSYS_LOG(ERROR, "ObSchemaManager parse_from_file error");
    ret = 1;
  }
  else
  {
    table_num_ = 0;
    const ObSchema* si = schema_.begin();
    for (; si != schema_.end(); ++si) ++table_num_;
  }

  return ret;
}

int SchemaProxy::random_table(const common::ObSchema* &table) const
{
  table = random_schema_();
  return 0;
}

int SchemaProxy::random_column(const common::ObSchema* table, ColumnArr &columns) const
{
  const ObColumnSchema* column_schema = table->column_begin();
  for (; column_schema != table->column_end(); ++column_schema)
  {
    if ((ObIntType == column_schema->get_type()
//        || ObVarcharType == column_schema->get_type()
        || ObDateTimeType == column_schema->get_type()
        || ObPreciseDateTimeType == column_schema->get_type())
        && NULL == table->find_join_info(column_schema->get_id()))
      columns.push_back(column_schema);
  }
  return 0;
}

const ObSchema* SchemaProxy::random_schema_() const
{
  int64_t table_num = GI::instance().random().rand64(table_num_);
  const ObSchema* si = schema_.begin();
  while (table_num > 0)
  {
    ++si;
    --table_num;
  }
  return si;
}

int ReqFiller::fill_mutator(const common::ObSchema* table,
    const common::ObVector<const common::ObColumnSchema*> &columns,
    common::ObMutator &mutator) const
{
  int ret = 0;

  buffer rowkey(table->get_rowkey_max_length());

  GI::instance().random().randbuffer(rowkey);

  if (OB_SUCCESS == ret)
  {
    ret = mutator.use_ob_sem();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObMutator use_ob_sem error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (ColumnArr::iterator ci = columns.begin(); OB_SUCCESS == ret && ci != columns.end(); ++ci)
    {
      ObObj val;
      ColumnType ct = (*ci)->get_type();
      if (ObIntType == ct)
        val.set_int(GI::instance().random().rand64(UINT64_MAX));
      else if (ObVarcharType == ct)
      {
        buffer str((*ci)->get_size() / 4);
        GI::instance().random().randbuffer(str);
        val.set_varchar(ObString(str.length(), str.length(), str.ptr()));
      }
      else if (ObDateTimeType == ct)
        val.set_datetime(GI::instance().random().rand64(UINT64_MAX));
      else if (ObPreciseDateTimeType == ct)
        val.set_precise_datetime(GI::instance().random().rand64(UINT64_MAX));
      else
      {
        TBSYS_LOG(ERROR, "unknown column type: %d", ct);
        ret = 1;
      }

      int table_name_len = strlen(table->get_table_name());
      int column_name_len = strlen((*ci)->get_name());
      ret = mutator.update(
          ObString(table_name_len, table_name_len, const_cast<char*>(table->get_table_name())),
          ObString(rowkey.length(), rowkey.length(), rowkey.ptr()),
          ObString(column_name_len, column_name_len, const_cast<char*>((*ci)->get_name())),
          val);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObMutator update error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ReqFiller::fill_usscan_param(const common::ObSchema* table,
    const common::ObVector<const common::ObColumnSchema*> &columns,
    common::ObScanParam &scan_param) const
{
  int ret = 0;

  buffer rowkey(table->get_rowkey_max_length());

  GI::instance().random().randbuffer(rowkey);

  ObString rowkey_string(rowkey.length(), rowkey.length(), rowkey.ptr());

  ObRange range;
  ObVersionRange vrange;

  if (OB_SUCCESS == ret)
  {
    range.table_id_ = table->get_table_id();
    range.start_key_ = rowkey_string;
    range.end_key_ = rowkey_string;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();

    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_max_value();
    vrange.start_version_ = 2;
    vrange.end_version_ = 0;

    scan_param.set(table->get_table_id(), ObString(), range);
    scan_param.set_version_range(vrange);
    for (ColumnArr::iterator ci = columns.begin(); OB_SUCCESS == ret && ci != columns.end(); ++ci)
    {
      ret = scan_param.add_column((*ci)->get_id());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObScanParam add_column error, ret=%d column_id=%lu",
            ret, (*ci)->get_id());
      }
    }
  }


  return ret;
}

int IdentityReqFiller::fill_mutator(const common::ObSchema* table,
    const common::ObVector<const common::ObColumnSchema*> &columns,
    common::ObMutator &mutator) const
{
  int ret = 0;

  buffer rowkey(table->get_rowkey_max_length());

  GI::instance().random().randbuffer(rowkey);

  buffer::iterator bi = rowkey.begin();
  for (++bi; bi != rowkey.end(); ++bi) *bi = '\0';

  if (OB_SUCCESS == ret)
  {
    ret = mutator.use_ob_sem();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObMutator use_ob_sem error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    const int64_t sum = 0;
    int64_t line_sum = 0;
    int rem = columns.size();
    for (ColumnArr::iterator ci = columns.begin(); OB_SUCCESS == ret && ci != columns.end(); ++ci)
    {
      --rem;
      int64_t range_begin = std::max(+BOMB_INT_MIN, sum + rem * +BOMB_INT_MIN - line_sum);
      int64_t range_end = std::min(+BOMB_INT_MAX, sum + rem * +BOMB_INT_MAX - line_sum);
      int64_t value = GI::instance().random().rand64(range_begin, range_end);
      line_sum += value;

      ObObj val;
      ColumnType ct = (*ci)->get_type();
      if (ObIntType == ct)
        val.set_int(value, true);
      else if (ObDateTimeType == ct)
        val.set_datetime(value, true);
      else if (ObPreciseDateTimeType == ct)
        val.set_precise_datetime(value, true);
      else
      {
        TBSYS_LOG(ERROR, "unknown column type: %d", ct);
        ret = 1;
      }

      if (OB_SUCCESS == ret)
      {
        int table_name_len = strlen(table->get_table_name());
        int column_name_len = strlen((*ci)->get_name());
        ret = mutator.update(
            ObString(table_name_len, table_name_len, const_cast<char*>(table->get_table_name())),
            ObString(rowkey.length(), rowkey.length(), rowkey.ptr()),
            ObString(column_name_len, column_name_len, const_cast<char*>((*ci)->get_name())),
            val);
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObMutator update error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int IdentityReqFiller::fill_usscan_param(const common::ObSchema* table,
    const common::ObVector<const common::ObColumnSchema*> &columns,
    common::ObScanParam &scan_param) const
{
  int ret = 0;

  buffer rowkey(table->get_rowkey_max_length());

  GI::instance().random().randbuffer(rowkey);

  buffer::iterator bi = rowkey.begin();
  for (++bi; bi != rowkey.end(); bi++) *bi = '\0';

  ObString rowkey_string(rowkey.length(), rowkey.length(), rowkey.ptr());

  ObRange range;
  ObVersionRange vrange;

  if (OB_SUCCESS == ret)
  {
    range.table_id_ = table->get_table_id();
    range.start_key_ = rowkey_string;
    range.end_key_ = rowkey_string;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();

    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_max_value();
    vrange.start_version_ = 2;
    vrange.end_version_ = 0;

    scan_param.set(table->get_table_id(), ObString(), range);
    scan_param.set_version_range(vrange);
    for (ColumnArr::iterator ci = columns.begin(); OB_SUCCESS == ret && ci != columns.end(); ++ci)
    {
      ret = scan_param.add_column((*ci)->get_id());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObScanParam add_column error, ret=%d column_id=%lu",
            ret, (*ci)->get_id());
      }
    }
  }


  return ret;
}

int IdentityReqFiller::fill_usget_param(const common::ObSchema* table,
    const common::ObVector<const common::ObColumnSchema*> &columns,
    common::ObGetParam &get_param) const
{
  int ret = 0;

  buffer rowkey(table->get_rowkey_max_length());

  GI::instance().random().randbuffer(rowkey);

  buffer::iterator bi = rowkey.begin();
  for (++bi; bi != rowkey.end(); bi++) *bi = '\0';

  ObString rowkey_string(rowkey.length(), rowkey.length(), rowkey.ptr());

  if (OB_SUCCESS == ret)
  {
    ObVersionRange vrange;
    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_max_value();
    vrange.start_version_ = 2;
    vrange.end_version_ = 0;
    get_param.set_version_range(vrange);

    ObCellInfo cell;
    cell.table_id_ = table->get_table_id();
    cell.row_key_ = rowkey_string;
    for (ColumnArr::iterator ci = columns.begin(); OB_SUCCESS == ret && ci != columns.end(); ++ci)
    {
      cell.column_id_ = (*ci)->get_id();
      ret = get_param.add_cell(cell);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObGetParam add_cell error, ret=%d column_id=%lu",
            ret, (*ci)->get_id());
      }
    }
  }


  return ret;
}

int IdentityReqFiller::check_scanner(const common::ObScanParam &scan_param,
        const common::ObScanner &scanner) const
{
  int ret = OB_SUCCESS;

  ObCellInfo *cell = NULL;
  bool row_changed = false;
  bool row_not_exist = false;
  int64_t line_sum = 0;
  ObScanner::Iterator iter = scanner.begin();
  for (; OB_SUCCESS == ret && iter != scanner.end(); ++iter)
  {
    int64_t value = 0;
    ret = iter.get_cell(&cell, &row_changed);
    if (ObExtendType == cell->value_.get_type())
      if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != cell->value_.get_ext())
      {
        TBSYS_LOG(ERROR, "Scanner find an illegal ext type: %ld", cell->value_.get_ext());
        ret = OB_ERROR;
      }
      else
        row_not_exist = true;
    else if (ObIntType == cell->value_.get_type())
      cell->value_.get_int(value);
    else if (ObDateTimeType == cell->value_.get_type())
      cell->value_.get_datetime(value);
    else if (ObPreciseDateTimeType == cell->value_.get_type())
      cell->value_.get_precise_datetime(value);
    else
    {
      TBSYS_LOG(ERROR, "Scanner find an illegal cell type: %ld", cell->value_.get_type());
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret)
    {
      line_sum += value;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (!row_not_exist && 0 != line_sum)
    {
      TBSYS_LOG(ERROR, "Sum of line is not zero: line_sum=%ld", line_sum);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != cell)
    {
      buffer row_key_str;
      ObString row_key = cell->row_key_;
      for (int64_t i = 0; i < row_key.length(); i++)
      {
        row_key_str.length() += snprintf(row_key_str.ptr() + row_key_str.length(), row_key_str.capacity(), "%02hhX", row_key.ptr()[i]);
      }

      buffer column_value;
      ObScanner::Iterator iter = scanner.begin();
      for (; iter != scanner.end(); ++iter)
      {
        int64_t value = 0;
        iter.get_cell(&cell);
        if (ObExtendType == cell->value_.get_type())
        {
          if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != cell->value_.get_ext())
          {
            column_value.append(cell->column_id_).append(":Unknow Cell Ext Type(").append(cell->value_.get_ext()).append(") ");
          }
          else
          {
            column_value.append(cell->column_id_).append("ROW DOES NOT EXIST");
          }
        }
        else if (ObIntType == cell->value_.get_type())
        {
          cell->value_.get_int(value);
          column_value.append(cell->column_id_).append(":Int(").append(value).append(") ");
        }
        else if (ObDateTimeType == cell->value_.get_type())
        {
          cell->value_.get_datetime(value);
          column_value.append(cell->column_id_).append(":DateTime(").append(value).append(") ");
        }
        else if (ObPreciseDateTimeType == cell->value_.get_type())
        {
          cell->value_.get_precise_datetime(value);
          column_value.append(cell->column_id_).append(":PreciseDateTime(").append(value).append(") ");
        }
        else if (ObVarcharType == cell->value_.get_type())
        {
          ObString str;
          cell->value_.get_varchar(str);
          int64_t pos = 0;
          serialization::decode_i64(str.ptr(), str.length(), pos, &value);
          column_value.append(cell->column_id_).append(":Varchar(").append(value).append(") ");
        }
        else
        {
          column_value.append(cell->column_id_).append(":Unknow Cell Type(").append(cell->value_.get_type()).append(") ");
        }
        TBSYS_LOG(ERROR, "value=%ld", value);
      }

      TBSYS_LOG(ERROR, "scanner_size=%ld rowkey=%.*s Columns={%.*s}",
          scanner.get_size(),
          row_key_str.length(), row_key_str.ptr(),
          column_value.length(), column_value.ptr());
    }
  }

  return ret;
}

int IdentityReqFiller::check_scanner(const common::ObGetParam &get_param,
    const common::ObScanner &scanner) const
{
  ObScanParam scan_param;
  return check_scanner(scan_param, scanner);
}

FlowLimiter::FlowLimiter()
  : total_tr_limit_(0), tr_unit_limit_(0)
{
}

FlowLimiter::Stat::Stat()
  : last_timestamp_(0), total_tr_(0), tr_flow_(0)
{
}

void FlowLimiter::init(int64_t total_tr_limit, int64_t tps_limit)
{
  total_tr_limit_ = total_tr_limit;
  tr_unit_limit_ = tps_limit / 10;
}

int FlowLimiter::wait()
{
  int ret = 0;
  if (0 != tr_unit_limit_)
  {
    int64_t time = tbsys::CTimeUtil::getTime();
    int64_t time_elapsed = time - stat_.get()->last_timestamp_;
    if (time_elapsed > TIME_UNIT)
    {
      stat_.get()->tr_flow_ = 0;
      stat_.get()->last_timestamp_ = tbsys::CTimeUtil::getTime();
    }
    else
    {
      if (++stat_.get()->tr_flow_ >= tr_unit_limit_)
      {
        ::usleep(TIME_UNIT - time_elapsed);
        stat_.get()->tr_flow_ = 0;
        stat_.get()->last_timestamp_ = tbsys::CTimeUtil::getTime();
      }
    }
  }
  ++stat_.get()->total_tr_;

  if (0 != total_tr_limit_ && stat_.get()->total_tr_ > total_tr_limit_) ret = 1;

  return ret;
}

StatCollector::StatCollector()
  : sum_num_(0), sum_succ_num_(0), sum_failed_num_(0), sum_retry_num_(0),
    sum_total_time_(0), max_rt_(0), min_rt_(0)
{
}

StatCollector::Stat::Stat()
  : num_(0), succ_num_(0), failed_num_(0), retry_num_(0),
    start_timestamp_(0), total_time_(0), max_rt_(0), min_rt_(0)
{
}

void StatCollector::start()
{
  ++(stat_.get()->num_);
  stat_.get()->start_timestamp_ = tbsys::CTimeUtil::getTime();
}

void StatCollector::succ()
{
  if (0 != stat_.get()->start_timestamp_)
  {
    ++(stat_.get()->succ_num_);
    int64_t et = tbsys::CTimeUtil::getTime() - stat_.get()->start_timestamp_;
    if (0 == stat_.get()->max_rt_ || et > stat_.get()->max_rt_) stat_.get()->max_rt_ = et;
    if (0 == stat_.get()->min_rt_ || et < stat_.get()->min_rt_) stat_.get()->min_rt_ = et;
    stat_.get()->total_time_ += et;
  }
}

void StatCollector::fail(int err_code)
{
  UNUSED(err_code);
  ++(stat_.get()->failed_num_);
}

void StatCollector::retry()
{
  ++(stat_.get()->retry_num_);
  stat_.get()->start_timestamp_ = tbsys::CTimeUtil::getTime();
}

void StatCollector::add_to_sum()
{
  tbutil::Mutex mutex;
  mutex.lock();
  sum_num_ += stat_.get()->num_;
  sum_succ_num_ += stat_.get()->succ_num_;
  sum_failed_num_ += stat_.get()->failed_num_;
  sum_retry_num_ += stat_.get()->retry_num_;
  sum_total_time_ += stat_.get()->total_time_;
  if (0 == max_rt_ || stat_.get()->max_rt_ > max_rt_) max_rt_ = stat_.get()->max_rt_;
  if (0 == min_rt_ || stat_.get()->min_rt_ < min_rt_) min_rt_ = stat_.get()->min_rt_;
  mutex.unlock();
}

void ApplyRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);

  int ret = 0;

  GI::instance().random().init();

  StatCollector &stat = GI::instance().stat();

  while (!_stop)
  {
    ret = gen_mutator_();
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "gen_mutator_ error, ret=%d", ret);
      break;
    }
    else
    {
      do
      {
        int err = GI::instance().flow().wait();
        if (0 != err)
        {
          ret = 1;
        }
        else
        {
          if (OB_SUCCESS == ret) stat.start();
          else stat.retry();
          ret = GI::instance().obapi().apply(*(mutator_.get()), GI::instance().param().timeout_us);
          if (OB_SUCCESS != ret)
          {
            stat.fail(ret);
            TBSYS_LOG(ERROR, "apply error, ret=%d", ret);
          }
          else stat.succ();
        }
      }
      while (OB_RESPONSE_TIME_OUT == ret && !_stop);

      if (0 != ret) break;
    }
  }

  stat.add_to_sum();
}

int ApplyRunnable::gen_mutator_()
{
  int ret = 0;

  const ObSchema* table = 0;
  ColumnArr columns;

  SchemaProxy &schema = GI::instance().schema();

  mutator_.get()->reset();
  
  ret = schema.random_table(table);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "random_table error, ret=%d", ret);
  }
  else
  {
    ret = schema.random_column(table, columns);
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "random_column error, ret=%d", ret);
    }
  }

  if (0 == ret)
  {
    ret = GI::instance().filler().fill_mutator(table, columns, *(mutator_.get()));
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "fill_mutator error, ret=%d", ret);
    }
  }

  return ret;
}

void USScanRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);

  int ret = 0;

  GI::instance().random().init();

  StatCollector &stat = GI::instance().stat();

  while (!_stop)
  {
    ret = gen_scan_param_();
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "gen_scan_param_ error, ret=%d", ret);
      break;
    }
    else
    {
      do
      {
        int err = GI::instance().flow().wait();
        if (0 != err)
        {
          ret = 1;
        }
        else
        {
          ObScanner scanner;
          if (OB_SUCCESS == ret) stat.start();
          else stat.retry();
          ret = GI::instance().obapi().usscan(*(scan_param_.get()), GI::instance().param().timeout_us, scanner);
          if (OB_SUCCESS != ret)
          {
            stat.fail(ret);
            TBSYS_LOG(ERROR, "scan error, ret=%d", ret);
          }
          else
          {
            ret = GI::instance().filler().check_scanner(*(scan_param_.get()), scanner);
            if (OB_SUCCESS != ret)
            {
              stat.fail(ret);
              TBSYS_LOG(ERROR, "scanner check error, ret=%d", ret);
            }
            else
            {
              stat.succ();
            }
          }
        }
      }
      while (OB_RESPONSE_TIME_OUT == ret && !_stop);

      if (0 != ret) break;
    }
  }

  stat.add_to_sum();
}

int USScanRunnable::gen_scan_param_()
{
  int ret = 0;

  const ObSchema* table = 0;
  ColumnArr columns;

  SchemaProxy &schema = GI::instance().schema();

  scan_param_.get()->reset();
  
  ret = schema.random_table(table);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "random_table error, ret=%d", ret);
  }
  else
  {
    ret = schema.random_column(table, columns);
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "random_column error, ret=%d", ret);
    }
  }

  if (0 == ret)
  {
    ret = GI::instance().filler().fill_usscan_param(table, columns, *(scan_param_.get()));
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "fill_mutator error, ret=%d", ret);
    }
  }

  return ret;
}

void USGetRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);

  int ret = 0;

  GI::instance().random().init();

  StatCollector &stat = GI::instance().stat();

  while (!_stop)
  {
    ret = gen_get_param_();
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "gen_scan_param_ error, ret=%d", ret);
      break;
    }
    else
    {
      do
      {
        int err = GI::instance().flow().wait();
        if (0 != err)
        {
          ret = 1;
        }
        else
        {
          ObScanner scanner;
          if (OB_SUCCESS == ret) stat.start();
          else stat.retry();
          ret = GI::instance().obapi().usget(*(get_param_.get()), GI::instance().param().timeout_us, scanner);
          if (OB_SUCCESS != ret)
          {
            stat.fail(ret);
            TBSYS_LOG(ERROR, "get error, ret=%d", ret);
          }
          else
          {
            ret = GI::instance().filler().check_scanner(*(get_param_.get()), scanner);
            if (OB_SUCCESS != ret)
            {
              stat.fail(ret);
              TBSYS_LOG(ERROR, "scanner check error, ret=%d", ret);
            }
            else
            {
              stat.succ();
            }
          }
        }
      }
      while (OB_RESPONSE_TIME_OUT == ret && !_stop);

      if (0 != ret) break;
    }
  }

  stat.add_to_sum();
}

int USGetRunnable::gen_get_param_()
{
  int ret = 0;

  const ObSchema* table = 0;
  ColumnArr columns;

  SchemaProxy &schema = GI::instance().schema();

  get_param_.get()->reset();
  
  ret = schema.random_table(table);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "random_table error, ret=%d", ret);
  }
  else
  {
    ret = schema.random_column(table, columns);
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "random_column error, ret=%d", ret);
    }
  }

  if (0 == ret)
  {
    ret = GI::instance().filler().fill_usget_param(table, columns, *(get_param_.get()));
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "fill_mutator error, ret=%d", ret);
    }
  }

  return ret;
}

int GI::init()
{
  int ret = 0;

  ret = obapi_.init(ObServer(ObServer::IPV4, param_.ip, param_.port));
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "ObServer init error, ret=%d", ret);
  }
  else
  {
    ret = schema_.init(param_.schema_file.ptr());
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "SchemaProxy init error, ret=%d", ret);
    }
    else
    {
      flow_.init(param_.total_tr_limit, param_.tps_limit);
      if (Param::TR_TYPE_USSCAN == param_.tr_type)
        run_ = new USScanRunnable(param_.thread_no);
      else if (Param::TR_TYPE_USGET == param_.tr_type)
        run_ = new USGetRunnable(param_.thread_no);
      else
        run_ = new ApplyRunnable(param_.thread_no);
    }
  }

  return ret;
}

GI& GI::instance()
{
  static GI instance_object;
  return instance_object;
}

