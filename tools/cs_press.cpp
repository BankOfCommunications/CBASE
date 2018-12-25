/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_press.cpp is for what ...
 *
 *  Version: $Id: cs_press.cpp 2010年11月16日 11时01分16秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include <unistd.h>
#include <getopt.h>
#include <math.h>
#include "cs_press.h"
#include "common/utility.h"
#include "common/page_arena.h"
#include "common/serialization.h"
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common_func.h"

using namespace oceanbase;
using namespace oceanbase::common;

GFactory GFactory::instance_;

GFactory::GFactory()
{
  memset(params_, 0, sizeof(Param) * MAX_THREAD_COUNT);
}

GFactory::~GFactory()
{
}

int GFactory::initialize(const ObServer& remote_server, 
    const char* schema_file, const int32_t tc)
{
  common::ob_init_memory_pool();
  common::ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  thread_count_ = tc;

  int ret = client_.initialize();
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize client object failed, ret:%d", ret);
    return ret;
  }

  tbsys::CConfig config;
  bool parse_ok = schema_manager_.parse_from_file(schema_file, config);
  if (!parse_ok) 
  {
    TBSYS_LOG(ERROR, "open schema_file failed, ret:%d", ret);
    return OB_ERROR;
  }

  ret = rpc_stub_.initialize(remote_server, &client_.get_client_manager());
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize rpc stub failed, ret:%d", ret);
  }
  return ret;
}

int GFactory::start()
{
  client_.start();
  worker_.setThreadCount(thread_count_);
  worker_.get_metrics().start();
  worker_.start();
  return OB_SUCCESS;
}

int GFactory::stop()
{
  worker_.stop();
  return OB_SUCCESS;
}

int GFactory::wait()
{
  worker_.wait();
  worker_.get_metrics().stop();
  worker_.dump_metrics();
  client_.stop();
  client_.wait();
  return OB_SUCCESS;
}

void Metrics::dump() const 
{
  TBSYS_LOG(INFO, "worker done, total consume:%ld, total case:%ld,  "
      "average consume:%ld, succeed:(%ld, %ld, %ld), failed:(%ld, %ld, %ld), qps(%ld)",
      get_total_consume(), get_total_count(), average(),
      get_total_succeed(), get_succeed_consume(), 
      succeed_average(),
      get_total_failed(), get_failed_consume(),
      failed_average(),
      get_total_count() * 1000 * 1000 / duration()
      );
}

//--------------------------------------------------------------------
// class Worker
//--------------------------------------------------------------------
void Worker::run(tbsys::CThread *thread, void *arg)
{
  long index = (long) arg;
  const Param &param = GFactory::get_instance().get_param(index);
  if (param.qtype_ == 0)
    seq_scan_case(param);
  else if (param.qtype_ == 1)
    random_scan_case(param);
  else if (param.qtype_ == 2)
    seq_get_case(param);
  else if (param.qtype_ == 3)
    random_get_case(param);
}

void Worker::dump_metrics() const
{
  metrics_.dump();
}

int Worker::seq_scan_case(const Param &param)
{
  int64_t start_seed = param.seed_;

  int64_t column_id_array[OB_MAX_COLUMN_NUMBER] ;
  int64_t column_id_size = OB_MAX_COLUMN_NUMBER;
  bool is_whole_row = false;
  int ret = get_column_array(param, column_id_array, column_id_size, is_whole_row);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get_column_array failed=%d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get_column_array size=%ld, is_whole_row=%d", column_id_size, is_whole_row);
  }

  while (!_stop)
  {
    if (start_seed > param.max_seed_) 
    {
      TBSYS_LOG(DEBUG, "start_seed:%ld > max_seed_:%ld, thread exit.", 
          start_seed, param.max_seed_);
      break;
    }
    int64_t end_seed = start_seed + param.step_ * (param.row_count_ - 1);
    if (end_seed > param.max_seed_) end_seed = param.max_seed_;
    TBSYS_LOG(DEBUG, "sequence generator, start_seed=%ld, end_seed=%ld ",
        start_seed, end_seed);

    unit_scan_case(param, start_seed, end_seed, 
        column_id_array, column_id_size, is_whole_row);

    start_seed = end_seed + param.step_;
  }

  return OB_SUCCESS;
}

int Worker::random_scan_case(const Param &param)
{
  int64_t start_seed = param.seed_;

  int64_t column_id_array[OB_MAX_COLUMN_NUMBER] ;
  int64_t column_id_size = OB_MAX_COLUMN_NUMBER;
  bool is_whole_row = false;
  int ret = get_column_array(param, column_id_array, column_id_size, is_whole_row);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get_column_array failed=%d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get_column_array size=%ld", column_id_size);
  }

  while (!_stop)
  {
    start_seed = random_number(param.seed_, param.max_seed_);
    int64_t end_seed = start_seed + param.step_ * (param.row_count_ - 1);
    if (end_seed > param.max_seed_) end_seed = param.max_seed_;

    TBSYS_LOG(DEBUG, "random generator, start_seed=%ld, end_seed=%ld ",
        start_seed, end_seed);

    unit_scan_case(param, start_seed, end_seed, 
        column_id_array, column_id_size, is_whole_row);
  }

  return OB_SUCCESS;
}

int Worker::unit_scan_case(const Param &param,
    const int64_t start_seed,
    const int64_t end_seed,
    const int64_t *column_id_array, 
    const int64_t column_id_size, 
    const bool is_whole_row)
{
  const int32_t MAX_KEY_SIZE = 50;
  char start_key_buf[MAX_KEY_SIZE]; 
  char end_key_buf[MAX_KEY_SIZE];
  ObString start_key(MAX_KEY_SIZE, get_rowkey_length(start_seed), start_key_buf);
  ObString end_key(MAX_KEY_SIZE, get_rowkey_length(end_seed), end_key_buf);
  get_rowkey(start_seed, start_key);
  get_rowkey(end_seed, end_key);

  ObScanParam input;
  ObString ob_table_name;
  ObRange range;
  range.table_id_= param.table_id_;
  range.start_key_.assign_ptr((char*)start_key.ptr(), start_key.length());
  range.end_key_.assign_ptr((char*)end_key.ptr(), end_key.length());

  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  input.set(param.table_id_, ob_table_name, range);
  input.set_scan_size(2*1024*1024);
  input.set_is_result_cached(param.use_cache_);
  input.set_scan_direction(ObScanParam::FORWARD);
  input.set_read_mode((ObScanParam::ReadMode)param.read_mode_);

  for (int i = 0; i < column_id_size; ++i)
  {
    input.add_column(column_id_array[i]);
  }

  TBSYS_LOG(DEBUG, "unit_scan_case dump range start");
  range.hex_dump();
  TBSYS_LOG(DEBUG, "unit_scan_case dump range end");

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = GFactory::get_instance().get_rpc_stub().cs_scan(input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  TBSYS_LOG(DEBUG, "cs_scan time consume:%ld", end - start);
  metrics_.tick(end - start, 1, OB_SUCCESS == ret ? true : false);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "cs_scan error ret:%d, start seed=%ld, end seed=%ld", ret, start_seed, end_seed);
  }
  else
  {
    int64_t *seed_array = new int64_t[param.row_count_];
    int64_t seed_array_size = param.row_count_;
    int64_t i = 0;
    int64_t seed = start_seed;
    while (i < seed_array_size && seed <= end_seed)
    {
      seed_array[i++] = seed;
      seed += param.step_;
    }
    seed_array_size = i;
    ret = check_query_result(param, scanner, seed_array, 
        seed_array_size, column_id_array, column_id_size, is_whole_row); 
    if (seed_array) delete []seed_array;
  }
  return ret;
}

int32_t parse_string_to_int_array_2(const char* line, 
    const char del, int64_t *array, int64_t& size) 
{
  int ret = 0;
  const char* start = line;
  const char* p = NULL;
  char buffer[OB_MAX_ROW_KEY_LENGTH];

  if (NULL == line || NULL == array || size <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
  }

  int32_t idx = 0;
  if (OB_SUCCESS == ret)
  {
    while (NULL != start) 
    {
      p = strchr(start, del);
      if (NULL != p) 
      {
        memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
        strncpy(buffer, start, p - start);
        if (strlen(buffer) > 0) 
        {
          if (idx >= size) 
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          else
          {
            array[idx++] = strtoll(buffer, NULL, 10);
          }
        }
        start = p + 1;
      } 
      else 
      {
        memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
        strcpy(buffer, start);
        if (strlen(buffer) > 0) 
        {
          if (idx >= size) 
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          else
          {
            array[idx++] = strtoll(buffer, NULL, 10);
          }
        }
        break;
      }
    }

    if (OB_SUCCESS == ret) size = idx;
  }
  return ret;
}

int Worker::get_column_array(const Param &param, 
    int64_t *column_id_array, int64_t &column_id_size, bool &is_whole_row)
{
  int64_t size = column_id_size;
  is_whole_row = false;
  int ret  = parse_string_to_int_array_2(
      param.columns_, ',', column_id_array, column_id_size);
  if (OB_SUCCESS != ret) return ret;
  // full columns
  if (column_id_size == 1 && column_id_array[0] == 0)
  {
    column_id_size = 0;
    const ObSchema* schema = GFactory::get_instance().
      get_schema_manager().get_table_schema(param.table_id_);
    if (NULL == schema) return OB_ERROR;

    const ObColumnSchema* col_begin = schema->column_begin();
    for (; col_begin < schema->column_end(); ++col_begin)
    {
      if (column_id_size >= size) return OB_ERROR;
      column_id_array[column_id_size++] = col_begin->get_id();
    }
    is_whole_row = true;
    TBSYS_LOG(DEBUG, "query all coluumns, param.columns_=%s,  column_id_size=%ld" 
        ",is_whole_row=%d, , ret = %d",
        param.columns_,  column_id_size, is_whole_row, ret);
  }
  return ret;
}

int Worker::get_rowkey_length(const int64_t seed)
{
  int64_t postfix = (seed * 12911) % 15485863;  
  char postfix_buf[50];
  memset(postfix_buf, 0, 50);
  snprintf(postfix_buf, 50, "%ld", postfix);
  return  8 + strlen(postfix_buf);
}

int Worker::get_rowkey(const int64_t seed, oceanbase::common::ObString& rowkey)
{
  //snprintf(rowkey.ptr(), rowkey.length(), "%08ld", seed);

  int64_t pos = 0;
  serialization::encode_i64(rowkey.ptr(), rowkey.length(), pos, seed);
  int64_t postfix = (seed * 12911) % 15485863;  
  snprintf(rowkey.ptr() + pos, rowkey.length() - pos + 1, "%ld", postfix);

  return OB_SUCCESS;
}

int Worker::check_cell(
    const Param &param,
    const ObCellInfo &cell_info,
    const uint64_t column_id, 
    uint32_t & hash_value)
{
  if (cell_info.column_id_ != column_id) 
  {
    TBSYS_LOG(ERROR, "check cell failed,  "
        "base column id=%lu, actual column_id=%lu", column_id,
        cell_info.column_id_);
    return OB_ERROR;
  }
  if (cell_info.table_id_ != param.table_id_) 
  {
    TBSYS_LOG(ERROR, "check cell failed, column id=%lu, "
        "base table_id=%lu, actual table_id=%lu", column_id,
        param.table_id_, cell_info.table_id_);
    return OB_ERROR;
  }
  // check value type
  const ObSchema* schema = GFactory::get_instance().
    get_schema_manager().get_table_schema(param.table_id_);
  const ObColumnSchema* col_schema = schema->find_column_info(column_id);
  if (cell_info.value_.get_type() != col_schema->get_type())
  {
    TBSYS_LOG(ERROR, "check cell failed, column id=%lu, "
        "base type=%d, actual type=%d", column_id,
        col_schema->get_type(), cell_info.value_.get_type());
    return OB_ERROR;
  }

  int ret = OB_SUCCESS;
  int64_t prefix = 0;
  int64_t pos = 0;
  ret = serialization::decode_i64(cell_info.row_key_.ptr(), 
      schema->get_split_pos(), pos, &prefix);
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "decode prefix error=%d", ret);
    return ret;
  }

  int64_t base_value = prefix * column_id * cell_info.value_.get_type() * 991;
  TBSYS_LOG(DEBUG, "column id(%ld) prefix(%ld), base value(%ld), type(%d)", 
      column_id, prefix, base_value, cell_info.value_.get_type());

  int64_t intv = 0;
  //add lijianqiang [INT_32] 20151008:b
  int32_t intv32 = 0;
  //add 20151008:e
  int64_t trs = 0;
  float  floatv = 0.0;
  double doublev = 0.0;
  //add peiouya [DATE_TIME] 20150906:b
  ObDate date = 0;
  ObTime time = 0;
  //add 20150906:e
  ObDateTime datetimev = 0;
  ObPreciseDateTime precisev = 0;
  ObString varcharv;
  char cstringv[128];
  memset(cstringv, 0, 128);


  switch (cell_info.value_.get_type())
  {
    case ObNullType:
      ret = OB_ERROR;
      break;
    case ObIntType:
      cell_info.value_.get_int(intv);
      if (intv != base_value) 
      {
        TBSYS_LOG(ERROR, "check int value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, intv);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&intv, sizeof(intv), 0);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      cell_info.value_.get_int32(intv32);
      if (intv32 != base_value)
      {
        TBSYS_LOG(ERROR, "check int value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, intv32);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&intv32, sizeof(intv32), 0);
      break;
    //add 20151008:e
    case ObFloatType:
      cell_info.value_.get_float(floatv);
      if (fabs(floatv - (float)base_value) > 0.0001)
      {
        TBSYS_LOG(ERROR, "check float value failed, column id=%ld, "
            "base value=%ld, actual value=%10.2f", column_id,
            base_value, floatv);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&floatv, sizeof(floatv), 0);
      break;
    case ObDoubleType:
      cell_info.value_.get_double(doublev);
      if (fabs(doublev - (double)base_value) > 0.0001)
      {
        TBSYS_LOG(ERROR, "check double value failed, column id=%ld, "
            "base value=%ld, actual value=%10.2f", column_id,
            base_value, doublev);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&doublev, sizeof(doublev), 0);
      break;
    case ObDateTimeType:
      cell_info.value_.get_datetime(datetimev);
      if (datetimev != base_value) 
      {
        TBSYS_LOG(ERROR, "check datetime value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, datetimev);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&datetimev, sizeof(datetimev), 0);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
      cell_info.value_.get_date(date);
      if (date != base_value)
      {
        TBSYS_LOG(ERROR, "check  date value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, date);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&date, sizeof(date), 0);
      break;
    case ObTimeType:
      cell_info.value_.get_time(time);
      if (time != base_value)
      {
        TBSYS_LOG(ERROR, "check time value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, time);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&precisev, sizeof(precisev), 0);
      break;
    //add 20150906:e
    case ObPreciseDateTimeType:
      cell_info.value_.get_precise_datetime(precisev);
      if (precisev != base_value) 
      {
        TBSYS_LOG(ERROR, "check precise datetime value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, precisev);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&precisev, sizeof(precisev), 0);
      break;
    case ObVarcharType:
      cell_info.value_.get_varchar(varcharv);
      memcpy(cstringv, varcharv.ptr(), varcharv.length());
      trs = strtoll(cstringv, NULL, 10);
      if (trs != base_value) 
      {
        TBSYS_LOG(ERROR, "check varchar value failed, column id=%ld, "
            "base value=%ld, actual value=%.*s", column_id,
            base_value, varcharv.length(), varcharv.ptr());
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)varcharv.ptr(), varcharv.length(), 0);
      break;
    case ObCreateTimeType:
      cell_info.value_.get_createtime(precisev);
      if (precisev != base_value) 
      {
        TBSYS_LOG(ERROR, "check create datetime value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, precisev);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&precisev, sizeof(precisev), 0);
      break;
    case ObModifyTimeType:
      cell_info.value_.get_modifytime(precisev);
      if (precisev != base_value) 
      {
        TBSYS_LOG(ERROR, "check modify datetime value failed, column id=%ld, "
            "base value=%ld, actual value=%ld", column_id,
            base_value, precisev);
        ret = OB_ERROR;
      }
      hash_value = murmurhash2((void*)&precisev, sizeof(precisev), 0);
      break;
    case ObSeqType:
    case ObExtendType:
      break;
    default:
      ret = OB_ERROR;
      break;
  }
  return ret;
}


int Worker::seq_get_case(const Param &param)
{
  int64_t start_seed = param.seed_;

  int64_t column_id_array[OB_MAX_COLUMN_NUMBER] ;
  int64_t column_id_size = OB_MAX_COLUMN_NUMBER;
  bool is_whole_row = false;
  int ret = get_column_array(param, column_id_array, column_id_size, is_whole_row);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get_column_array failed=%d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get_column_array size=%ld", column_id_size);
  }

  int64_t seed_array_size = 0;
  int64_t * seed_array = new int64_t[param.row_count_];
  if (NULL == seed_array) return OB_ERROR;

  while (!_stop)
  {
    if (start_seed > param.max_seed_) 
    {
      TBSYS_LOG(DEBUG, "start_seed:%ld > max_seed_:%ld, thread exit.", 
          start_seed, param.max_seed_);
      break;
    }

    int64_t i = 0;
    for (; i < param.row_count_; ++i)
    {
      int64_t current_seed = start_seed + param.step_ * i;
      if (current_seed > param.max_seed_) break;
      seed_array[i] = current_seed;
    }
    seed_array_size = i;
    TBSYS_LOG(DEBUG, "sequence generator, start_seed=%ld, end_seed=%ld ",
        start_seed, seed_array[i - 1]);

    unit_get_case(param, seed_array, seed_array_size, 
        column_id_array, column_id_size, is_whole_row);

    start_seed = seed_array[i - 1] + param.step_;
  }

  if (seed_array) delete []seed_array;

  return OB_SUCCESS;
}

int Worker::random_get_case(const Param &param)
{

  int64_t column_id_array[OB_MAX_COLUMN_NUMBER] ;
  int64_t column_id_size = OB_MAX_COLUMN_NUMBER;
  bool is_whole_row = false;
  int ret = get_column_array(param, column_id_array, column_id_size, is_whole_row);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get_column_array failed=%d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "get_column_array size=%ld", column_id_size);
  }

  int64_t seed_array_size = 0;
  int64_t * seed_array = new int64_t[param.row_count_];
  if (NULL == seed_array) return OB_ERROR;
  int64_t last_seed = 0 ;

  while (!_stop)
  {

    int64_t i = 0;
    int64_t count = 0;
    last_seed = 0;
    // don't generate same and continuous seed
    while (count < param.row_count_)
    {
      int64_t current_seed = random_number(param.seed_, param.max_seed_);
      ++count;
      if (current_seed == last_seed) 
      {
        continue;
      }
      else 
      {
        last_seed = current_seed;
      }
      TBSYS_LOG(DEBUG, "random generator, current_seed=%ld ", current_seed);
      seed_array[i++] = current_seed;
    }
    seed_array_size = i;

    unit_get_case(param, seed_array, seed_array_size, 
        column_id_array, column_id_size, is_whole_row);
  }


  if (seed_array) delete []seed_array;

  return OB_SUCCESS;
}


int Worker::unit_get_case(const Param &param,
    const int64_t *seed_array,
    const int64_t seed_array_size,
    const int64_t *column_id_array, 
    const int64_t column_id_size,
    const bool is_whole_row)
{
  ObGetParam input;
  CharArena allocator;

  //printf("seed_array_size=%ld\n", seed_array_size);
  for (int i = 0; i < seed_array_size; ++i)
  {
    int64_t seed = seed_array[i];
    //printf("seed=%ld\n", seed);
    int64_t rowkey_length = get_rowkey_length(seed);
    //printf("rowkey_length =%ld\n", rowkey_length);
    char* rowkey_buf = allocator.alloc(rowkey_length);
    //char* rowkey_buf = (char*) malloc(rowkey_length);
    //printf("rowkey_buf=%p\n", rowkey_buf);
    ObString rowkey(rowkey_length, rowkey_length, rowkey_buf);
    get_rowkey(seed, rowkey);
    char* cell_info_buf = allocator.alloc(sizeof(ObCellInfo));
    ObCellInfo* cell_info = new (cell_info_buf) ObCellInfo();
    //char* cell_info_buf = (char*)malloc(sizeof(ObCellInfo));
    //printf("cell_info_buf=%p\n", cell_info_buf);
    //ObCellInfo* cell_info = new (cell_info_buf) ObCellInfo();

    cell_info->table_id_ = param.table_id_;
    cell_info->row_key_ = rowkey;
    //cell_info->column_id_ = 0;
    //input.add_cell(*cell_info);
    for (int j = 0 ; j < column_id_size; ++j)
    {
      int64_t column_id = column_id_array[j];
      cell_info->column_id_ = column_id;
      input.add_cell(*cell_info);
    }
  }
  input.set_is_result_cached(param.use_cache_);

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = GFactory::get_instance().get_rpc_stub().cs_get(input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  TBSYS_LOG(DEBUG, "cs_get time consume:%ld", end - start);
  metrics_.tick(end - start, 1, OB_SUCCESS == ret ? true : false);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "cs_get error ret:%d", ret);
  }
  else
  {
    ret = check_query_result(param, scanner, seed_array, 
        seed_array_size, column_id_array, column_id_size, is_whole_row); 
  }

  return ret;
}

int Worker::check_query_result(
    const Param &param,
    const oceanbase::common::ObScanner& scanner,
    const int64_t *seed_array,
    const int64_t seed_array_size,
    const int64_t *column_id_array, 
    const int64_t column_id_size,
    const bool is_whole_row)
{
  ObScannerIterator iter;
  int row_count = 0;
  int column_index = 0;
  int ret = OB_SUCCESS;

  const int32_t MAX_KEY_SIZE = 50;
  char base_key_buf[MAX_KEY_SIZE];

  int64_t seed = 0;
  int64_t total_hash_value = 0;
  int64_t base_hash_value = 0;
  uint32_t item_hash_value = 0;

  //int cmp = 0;
  bool is_row_changed = false;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info = NULL;
    iter.get_cell(&cell_info, &is_row_changed);
    if (NULL == cell_info)
    {
      TBSYS_LOG(ERROR, "get null cell_info");
      ret = OB_ERROR;
      break;
    }

    if (is_row_changed)
    {
      ++row_count;
      column_index = 0;

      if (row_count > seed_array_size)
      {
        TBSYS_LOG(ERROR, "get row count=%d > seed_array_size=%ld ", row_count, seed_array_size);
        ret = OB_ERROR;
        break;
      }

      seed = seed_array[row_count - 1];
      ObString base_key(MAX_KEY_SIZE, get_rowkey_length(seed), base_key_buf);
      get_rowkey(seed, base_key);
      int cmplen = base_key.length() <= cell_info->row_key_.length() ? 
        base_key.length() : cell_info->row_key_.length();
      int cmp = memcmp(base_key.ptr(), cell_info->row_key_.ptr(), cmplen);
      if (cmp != 0)
      {
        TBSYS_LOG(ERROR, "check_scan_result error, cell_info key not equal base_key");
        hex_dump(base_key.ptr(), base_key.length(), TBSYS_LOG_LEVEL_ERROR);
        hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length(),
            TBSYS_LOG_LEVEL_ERROR);
        ret = OB_ERROR;
      }
      else
      {
        // row changed, a new row start, reset the hash value
        total_hash_value = 0;
        base_hash_value  = murmurhash2(base_key.ptr(), base_key.length(), 0);
      }
    }
    else
    {
      ++column_index;
    }

    TBSYS_LOG(DEBUG, "---------------------%d----------------------\n", row_count);
    TBSYS_LOG(DEBUG,"table_id:%lu, column_id:%ld, is_row_changed:%d\n", 
        cell_info->table_id_, cell_info->column_id_, is_row_changed);
    hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());

    if (column_index >= column_id_size) 
    {
      TBSYS_LOG(ERROR, "get column_index:%d > column_id_size:%ld",
          column_index, column_id_size);
      ret = OB_ERROR;
    }

    if (!is_whole_row || column_index != column_id_size - 1)
    {
      item_hash_value = 0;
      ret = check_cell(param, *cell_info, column_id_array[column_index], item_hash_value);
      total_hash_value += item_hash_value;
    }
    else
    {
      //if (cell_info->value_.get_type() == ObIntType)
      int64_t check_sum = 0;
      cell_info->value_.get_int(check_sum);
      TBSYS_LOG(DEBUG, "column_index=%d, check_sum=%ld", column_index, check_sum);
      total_hash_value += check_sum;

      if (base_hash_value != total_hash_value)
      {
        TBSYS_LOG(ERROR, "check cell mismatch , base_hash_value=%ld not equal total_hash_value=%ld",
            base_hash_value, total_hash_value);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "check cell succeed, base_hash_value=%ld equal total_hash_value=%ld",
            base_hash_value, total_hash_value);
      }
    }
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "check cell mismatch, index=%d, id=%ld", column_index, 
          column_id_array[column_index]);
    }
    TBSYS_LOG(DEBUG,"---------------------%d----------------------, check ret=%d\n", row_count, ret);

    if (OB_SUCCESS != ret) break;
  }

  return ret;
}

//--------------------------------------------------------------------
// utility functions
//--------------------------------------------------------------------

void print_usage(const char* exename)
{
  fprintf(stderr, "%s "
      "-f[--config_file] \n"
      "-h[--help] \n"
      "-V[--version] \n"
      "-N[--no_deamon] \n", 
      exename);
}

static bool use_deamon_ = true;
const char* PID_FILE = "pid_file";
const char* LOG_FILE = "log_file";
const char* DATA_DIR = "data_dir";
const char* LOG_LEVEL = "log_level";
const char* MAX_LOG_FILE_SIZE = "max_log_file_size";
const char* section_name = "public";

const char* parse_cmd_line(const int argc,  char *const argv[])
{
  int opt = 0;
  const char* opt_string = "hNVf:";
  static char conf_name[256];
  struct option longopts[] = 
  {
    {"config_file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {"version", 0, NULL, 'V'},
    {"no_deamon", 0, NULL, 'N'},
    {0, 0, 0, 0}
  };

  const char* config_file = NULL;
  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'f':
        config_file = optarg;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit(1);
      case 'h':
        print_usage(argv[0]);
        exit(1);
      case 'N':
        use_deamon_ = false;
        break;
      default:
        break;
    }
  }
  if (config_file == NULL) 
  {
    snprintf(conf_name, 256, "%s.conf", argv[0]);
    config_file = conf_name;
  }
  return config_file;
}

int initialize(const char* section_name)
{
  int thread_count = TBSYS_CONFIG.getInt(section_name, "thread_count", 1);
  int query_type = TBSYS_CONFIG.getInt(section_name, "query_type", 0);
  const char* rowkey_seed_str = TBSYS_CONFIG.getString(section_name, "rowkey_seed");
  int64_t rowkey_seed = strtoll(rowkey_seed_str, NULL, 10);
  const char* rowkey_max_seed_str = TBSYS_CONFIG.getString(section_name, "rowkey_max_seed");
  int64_t rowkey_max_seed = strtoll(rowkey_max_seed_str, NULL, 10);
  int32_t rowkey_step =  TBSYS_CONFIG.getInt(section_name, "rowkey_step", 1);
  int32_t row_count = TBSYS_CONFIG.getInt(section_name, "row_count", 1);
  const char* column_id_array = TBSYS_CONFIG.getString(section_name, "column_id_array");
  int64_t table_id =  TBSYS_CONFIG.getInt(section_name, "table_id", 0);
  int32_t use_cache = TBSYS_CONFIG.getInt(section_name, "use_cache", 1);
  int32_t read_mode = TBSYS_CONFIG.getInt(section_name, "read_mode", 1);

  const char* remote_server_addr = TBSYS_CONFIG.getString(section_name, "remote_server_addr");
  int32_t remote_port = TBSYS_CONFIG.getInt(section_name, "remote_port", 0);
  const char* schema_file = TBSYS_CONFIG.getString(section_name, "schema_file");

  // check parameters
  if (NULL == schema_file) 
  {
    fprintf(stderr, "schema_file cannot be null (%s)\n", schema_file);
    return OB_INVALID_ARGUMENT;
  }
  if (NULL == column_id_array) 
  {
    fprintf(stderr, "column_id_array cannot be null (%s)\n", column_id_array);
    return OB_INVALID_ARGUMENT;
  }
  if (thread_count < 1 || thread_count >= GFactory::MAX_THREAD_COUNT) 
  {
    fprintf(stderr, "thread_count(%d) must between 1 to %d\n", 
        thread_count, GFactory::MAX_THREAD_COUNT);
    return OB_INVALID_ARGUMENT;
  } 
  if (rowkey_step < 1) 
  {
    fprintf(stderr, "rowkey_step(%d) must >= 1 \n", rowkey_step);
    return OB_INVALID_ARGUMENT;
  }
  if (rowkey_max_seed < rowkey_seed) 
  {
    fprintf(stderr, "rowkey_max_seed(%ld) must > rowkey_seed(%ld) \n", 
        rowkey_max_seed, rowkey_seed);
    return OB_INVALID_ARGUMENT;
  }
  if (table_id <= 0)
  {
    fprintf(stderr, "table_id(%ld) must >= 1 \n", table_id);
    return OB_INVALID_ARGUMENT;
  }

  ObServer remote_server;
  if (NULL == remote_server_addr && remote_port <= 0)
  {
    fprintf(stderr, "remote_server_addr(%s), remote_port(%d) invalid\n", 
        remote_server_addr, remote_port);
    return OB_INVALID_ARGUMENT;
  }
  else
  {
    remote_server.set_ipv4_addr(remote_server_addr, remote_port);
  }

  Param* params = GFactory::get_instance().get_params();
  for (int i = 0; i < thread_count; ++i)
  {
    Param& param = params[i];
    param.index_ = i;
    param.table_id_ = table_id;
    param.qtype_ = query_type;
    param.use_cache_ = use_cache;
    param.read_mode_ = read_mode;
    param.row_count_ = row_count;
    param.step_ = rowkey_step;
    param.seed_ = rowkey_seed;
    param.max_seed_ = rowkey_max_seed;
    strncpy(param.columns_, column_id_array, strlen(column_id_array));
  }

  return GFactory::get_instance().initialize(remote_server, schema_file, thread_count);
}

void sign_handler(const int sig)
{
  switch (sig) {
    case SIGTERM:
    case SIGINT:
      GFactory::get_instance().stop();
      break;
  }
}

int main(const int argc, char *argv[])
{
  const char* config_file = parse_cmd_line(argc, argv);

  if (NULL == config_file) 
  {
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }

  if (static_cast<int>(strlen(config_file)) >= OB_MAX_FILE_NAME_LENGTH)
  {
    fprintf(stderr, "file name too long %s error\n", config_file);
    return EXIT_FAILURE;
  }

  if(TBSYS_CONFIG.load(config_file)) 
  {
    fprintf(stderr, "load file %s error\n", config_file);
    return EXIT_FAILURE;
  }

  const char* sz_pid_file =
    TBSYS_CONFIG.getString(section_name, PID_FILE, "server.pid");
  const char* sz_log_file =
    TBSYS_CONFIG.getString(section_name, LOG_FILE, "server.log");

  int pid = 0;
  if((pid = tbsys::CProcess::existPid(sz_pid_file))) {
    fprintf(stderr, "program has been exist: pid=%d\n", pid);
    return EXIT_FAILURE;
  }

  const char * sz_log_level =
    TBSYS_CONFIG.getString(section_name, LOG_LEVEL, "info");
  TBSYS_LOGGER.setLogLevel(sz_log_level);
  int max_file_size = 
    TBSYS_CONFIG.getInt(section_name, MAX_LOG_FILE_SIZE, 1024); //1G
  TBSYS_LOGGER.setMaxFileSize(max_file_size * 1024L * 1024L);

  int ret = initialize("client");
  if (OB_SUCCESS != ret) 
  { 
    TBSYS_LOG(ERROR, "initialize failed, ret=%d", ret);
    return ret; 
  }

  bool start_ok = true;
  if (use_deamon_) 
  {
    start_ok = (tbsys::CProcess::startDaemon(sz_pid_file, sz_log_file) == 0);
  }
  if(start_ok) 
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    //signal(40, BaseMain::sign_handler);
    //signal(41, BaseMain::sign_handler);
    //signal(42, BaseMain::sign_handler);
    //
    GFactory::get_instance().start();
    GFactory::get_instance().wait();
    TBSYS_LOG(INFO, "exit program.");
  }

  return ret;
}
