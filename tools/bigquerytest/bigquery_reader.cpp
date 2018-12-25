/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery_reader.cpp,v 0.1 2012/04/01 11:12:56 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "bigquery_reader.h"
#include "util.h"

static const char* BIGQUERY_TABLE = "bigquery_table";

BigqueryReader::BigqueryReader()
{
  ob_client_ = NULL;
}

BigqueryReader::~BigqueryReader()
{
}

int BigqueryReader::init(MysqlClient& ob_client, PrefixInfo& prefix_info)
{
  int err = 0;
  ob_client_ = &ob_client;
  prefix_info_ = &prefix_info;

  return err;
}

int BigqueryReader::read_data(uint64_t prefix)
{
  int err = 0;
  assert(NULL != ob_client_);
  char rule_data[256];
  memset(rule_data, 0x00, sizeof(rule_data));
  ValueRule rule;

  err = prefix_info_->set_read_write_status(prefix, BIGQUERY_READING);
  if (0 != err && READ_WRITE_CONFLICT != err)
  {
    TBSYS_LOG(WARN, "failed to set read flag, prefix=%lu, err=%d", prefix, err);
  }

  if (0 == err)
  {
    err = prefix_info_->get_rule(prefix, rule_data, sizeof(rule_data));
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to get rule, prefix=%lu, err=%d", prefix, err);
    }
    else
    {
      uint64_t row_num = 0;
      err = prefix_info_->get_row_num(prefix, row_num);
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to get row num, err=%d", err);
      }
      else
      {
        err = rule.deserialize(rule_data, strlen(rule_data) + 1);
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to deserialize rule, err=%d", err);
        }
        else
        {
          rule.set_row_num(row_num);
        }
      }
    }
  }

  Bigquery query;
  Array expected_result;
  if (0 == err)
  {
    err = get_bigquery(prefix, rule, query, expected_result);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to get big query, rule_data=%s, err=%d", rule_data, err);
    }
  }

  Array res;
  if (0 == err)
  {
    TBSYS_LOG(INFO, "row_num=%ld, rule_data=%s, bigquery sql=%s", rule.get_row_num(), rule_data, query.to_sql());
    err = ob_client_->exec_query(query.to_sql(), res);
    if (0 != err)
    {
      TBSYS_LOG(ERROR, "failed to exec query, sql=%s, err=%d", query.to_sql(), err);
    }
  }

  if (0 == err)
  {
    err = verify_result(expected_result, res);
  }

  if (0 == err)
  {
    err = prefix_info_->set_read_write_status(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to reset flag, prefix=%lu, err=%d", prefix, err);
    }
  }
  else if (READ_WRITE_CONFLICT == err)
  {
    err = OB_SUCCESS;
  }

  return err;
}

int BigqueryReader::get_bigquery(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  int64_t rule_num = rule.get_rule_num();
  switch (rule_num)
  {
    case 1:
      err = get_bigquery_one_col(prefix, rule, query, result);
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to get_bigquery_one_col, rule_num=%ld, err=%d", rule_num, err);
      }
      break;
    case 2:
      err = get_bigquery_two_col(prefix, rule, query, result);
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to get_bigquery_two_col, rule_num=%ld, err=%d", rule_num, err);
      }
      break;
    default:
      TBSYS_LOG(WARN, "invalid rule_num, rule_num=%ld", rule_num);
      break;
  }

  return err;
}

int BigqueryReader::get_bigquery_one_equal_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  int64_t equal_val = 0;
  char value[32];
  int64_t row_num = rule.get_row_num();

  // select sum(col1), count(col1), sum(col1) / count(col1) from
  // bigquery_test group by col1;
  rule.get_equal_rule(0, equal_val);
  query.set_table_name(BIGQUERY_TABLE);
  query.add_select_column("sum(col1)");
  query.add_select_column("count(col1)");
  query.add_select_column("min(col1)");
  query.add_select_column("max(col1)");
  query.add_select_column("sum(col1) / count(col1)");
  query.add_groupby_column("col1");
  query.add_filter("prefix", Bigquery::FILTER_EQ, prefix);
  query.add_filter("suffix", Bigquery::FILTER_LT, row_num);

  err = result.init(1, 5);
  if (0 == err)
  {
    sprintf(value, "%ld", equal_val * row_num);
    result.set_value(0, 0, value, Array::INT_TYPE);
    sprintf(value, "%ld", row_num);
    result.set_value(0, 1, value, Array::INT_TYPE);
    sprintf(value, "%ld", equal_val);
    result.set_value(0, 2, value, Array::INT_TYPE);
    sprintf(value, "%ld", equal_val);
    result.set_value(0, 3, value, Array::INT_TYPE);
    sprintf(value, "%lf", equal_val * 1.0);
    result.set_value(0, 4, value, Array::FLOAT_TYPE);
  }

  return err;
}

int BigqueryReader::get_bigquery_one_equal_col2(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  int64_t equal_val = 0;
  char value[32];
  int64_t row_num = rule.get_row_num();

  // select distinct col1 from bigquery_test;
  rule.get_equal_rule(0, equal_val);
  query.set_table_name(BIGQUERY_TABLE);
  query.add_select_column("col1");
  query.set_distinct(true);
  query.add_filter("prefix", Bigquery::FILTER_EQ, prefix);
  query.add_filter("suffix", Bigquery::FILTER_LT, row_num);

  err = result.init(1, 1);
  if (0 == err)
  {
    sprintf(value, "%ld", equal_val);
    result.set_value(0, 0, value, Array::INT_TYPE);
  }
  return err;
}

// seq col + equal col
int BigqueryReader::get_bigquery_two_equal_seq_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  char value[32];
  int64_t row_num = rule.get_row_num();
  int64_t start_val = 0;
  int64_t step = 0;

  // select sum(col1), count(col1), sum(col1) / count(col1) from
  // bigquery_test group by col2;
  rule.get_seq_rule(0, start_val, step);
  query.set_table_name(BIGQUERY_TABLE);
  query.add_select_column("sum(col1)");
  query.add_select_column("count(col1)");
  query.add_select_column("min(col1)");
  query.add_select_column("max(col1)");
  query.add_select_column("sum(col1) / count(col1)");
  query.add_groupby_column("col2");
  query.add_filter("prefix", Bigquery::FILTER_EQ, prefix);
  query.add_filter("suffix", Bigquery::FILTER_LT, row_num);

  err = result.init(1, 5);
  if (0 == err)
  {
    sprintf(value, "%ld", start_val * row_num + row_num * (row_num - 1) * step / 2);
    result.set_value(0, 0, value, Array::INT_TYPE);
    sprintf(value, "%ld", row_num);
    result.set_value(0, 1, value, Array::INT_TYPE);
    sprintf(value, "%ld", start_val);
    result.set_value(0, 2, value, Array::INT_TYPE);
    sprintf(value, "%ld", start_val + (row_num - 1) * step);
    result.set_value(0, 3, value, Array::INT_TYPE);
    sprintf(value, "%lf", start_val + (double) ((row_num - 1) * step) / 2.0);
    result.set_value(0, 4, value, Array::FLOAT_TYPE);
  }

  return err;
}

int BigqueryReader::get_bigquery_two_equal_cycle_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  char value[32];
  int64_t row_num = rule.get_row_num();
  int64_t start_val = 0;
  int64_t step = 0;
  int64_t size_cycle = 0;
  int64_t num_cycles = 0;

  // select sum(col1), count(col1), sum(col1) / count(col1) from
  // bigquery_test group by col2;
  rule.get_cycle_rule(0, start_val, step, size_cycle, num_cycles);
  assert(size_cycle * num_cycles == row_num);

  query.set_table_name(BIGQUERY_TABLE);
  query.add_select_column("sum(col1)");
  query.add_select_column("count(col1)");
  query.add_select_column("min(col1)");
  query.add_select_column("max(col1)");
  query.add_select_column("sum(col1) / count(col1)");
  query.add_groupby_column("col2");
  query.add_filter("prefix", Bigquery::FILTER_EQ, prefix);
  query.add_filter("suffix", Bigquery::FILTER_LT, row_num);

  err = result.init(1, 5);
  if (0 == err)
  {
    sprintf(value, "%ld", (start_val * size_cycle + size_cycle * (size_cycle - 1) * step / 2) * num_cycles);
    result.set_value(0, 0, value, Array::INT_TYPE);
    sprintf(value, "%ld", row_num);
    result.set_value(0, 1, value, Array::INT_TYPE);
    sprintf(value, "%ld", start_val);
    result.set_value(0, 2, value, Array::INT_TYPE);
    sprintf(value, "%ld", start_val + (size_cycle - 1) * step);
    result.set_value(0, 3, value, Array::INT_TYPE);
    sprintf(value, "%lf", start_val + (double) ((size_cycle - 1) * step) / 2.0);
    result.set_value(0, 4, value, Array::FLOAT_TYPE);
  }

  return err;
}

int BigqueryReader::get_bigquery_two_equal_cycle_col2(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  char value[32];
  int64_t row_num = rule.get_row_num();
  int64_t start_val = 0;
  int64_t step = 0;
  int64_t size_cycle = 0;
  int64_t num_cycles = 0;
  int64_t equal_val = 0;

  // select sum(col1), count(col1), sum(col1) / count(col1), col2 from
  // bigquery_test group by col1 order by col1;
  rule.get_cycle_rule(0, start_val, step, size_cycle, num_cycles);
  rule.get_equal_rule(1, equal_val);
  assert(size_cycle * num_cycles == row_num);

  query.set_table_name(BIGQUERY_TABLE);
  query.add_select_column("sum(col1)");
  query.add_select_column("count(col1)");
  query.add_select_column("min(col1)");
  query.add_select_column("max(col1)");
  query.add_select_column("sum(col1) / count(col1)");
  query.add_groupby_column("col1");
  query.add_orderby_column("col1");
  query.add_filter("prefix", Bigquery::FILTER_EQ, prefix);
  query.add_filter("suffix", Bigquery::FILTER_LT, row_num);

  err = result.init(size_cycle, 5);
  if (0 == err)
  {
    for (int64_t i = 0; i < size_cycle; ++i)
    {
      sprintf(value, "%ld", (start_val + i * step) * num_cycles);
      result.set_value(i, 0, value, Array::INT_TYPE);
      sprintf(value, "%ld", num_cycles);
      result.set_value(i, 1, value, Array::INT_TYPE);
      sprintf(value, "%ld", start_val + i * step);
      result.set_value(i, 2, value, Array::INT_TYPE);
      sprintf(value, "%ld", start_val + i * step);
      result.set_value(i, 3, value, Array::INT_TYPE);
      sprintf(value, "%lf", (double) (start_val + i * step));
      result.set_value(i, 4, value, Array::FLOAT_TYPE);
    }
  }
  return err;
}

int BigqueryReader::get_bigquery_one_col(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  assert(rule.get_rule_num() == 1);
  int64_t rule_id = rule.get_rule_id(0);
  const static int64_t EQUAL_GEN_NUM = 1;
  gen_fun equal_fun[EQUAL_GEN_NUM] = {
    get_bigquery_one_equal_col1,
    //get_bigquery_one_equal_col2 //distinct is not supported
  };


  switch (rule_id)
  {
    case ValueRule::EQUAL_RULE:
      {
        err = (*equal_fun[rand() % EQUAL_GEN_NUM])(prefix, rule, query, result);
        break;
      }
    default:
      TBSYS_LOG(WARN, "invalid rule_id, rule_id=%ld", rule_id);
      err = OB_ERROR;
      break;
  }

  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to call get_bigquery_one_col, err=%d", err);
  }
  return err;
}

int BigqueryReader::get_bigquery_two_col(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result)
{
  int err = 0;
  assert(rule.get_rule_num() == 2);
  int64_t first_rule_id = rule.get_rule_id(0);
  int64_t second_rule_id = rule.get_rule_id(1);
  const static int64_t SEQ_GEN_NUM = 1;
  gen_fun seq_fun[SEQ_GEN_NUM] = {
    get_bigquery_two_equal_seq_col1,
  };

  const static int64_t CYCLE_GEN_NUM = 2;
  gen_fun cycle_fun[CYCLE_GEN_NUM] = {
    get_bigquery_two_equal_cycle_col1,
    get_bigquery_two_equal_cycle_col2,
  };

  // seq_rule + equal_rule
  if (first_rule_id == ValueRule::SEQ_RULE && second_rule_id == ValueRule::EQUAL_RULE)
  {
    err = (*seq_fun[rand() % SEQ_GEN_NUM])(prefix, rule, query, result);
  }
  // cycle_rule + equal_rule
  else if (first_rule_id == ValueRule::CYCLE_RULE && second_rule_id == ValueRule::EQUAL_RULE)
  {
    err = (*cycle_fun[rand() % CYCLE_GEN_NUM])(prefix, rule, query, result);
  }
  else
  {
    TBSYS_LOG(WARN, "invalid rule_id, first_rule_id=%ld, second_rule_id=%ld",
        first_rule_id, second_rule_id);
    err = OB_ERROR;
  }

  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to call get bigquery_two_col, err=%d", err); 
  }
  return err;
}

int BigqueryReader::verify_result(Array& expected_result, Array& result)
{
  int err = 0;

  err = expected_result.compare(result);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to compare result, first=expected, second=real, err=%d", err);
  }
  return err;
}



