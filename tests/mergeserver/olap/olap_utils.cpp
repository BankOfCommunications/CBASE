#include <iostream>
#include <cstdlib>
#include <getopt.h>
#include <cstdio>
#include <arpa/inet.h>
#include "olap.h"
#include "common/ob_define.h"
#include "common/ob_scan_param.h"
#include "common/ob_range2.h"
#include "../../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;


const char * msolap::target_table_name_cstr = "olap";
ObString msolap::target_table_name(static_cast<int32_t>(strlen(msolap::target_table_name_cstr)),
    static_cast<int32_t>(strlen(msolap::target_table_name_cstr)),
    const_cast<char*>(msolap::target_table_name_cstr));
static CharArena allocator_;

int msolap::init_scan_param(ObScanParam &param, const uint32_t min_key_include, const uint32_t max_key_include)
{
  int err = OB_SUCCESS;
  ObVersionRange version_range;
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  uint32_t start_key_val = htonl(min_key_include);
  uint32_t end_key_val = htonl(max_key_include);
  const int64_t KEYSZ = 32;
  char start_key_buf[KEYSZ];
  char end_key_buf[KEYSZ];
  snprintf(start_key_buf, 32, "%d", start_key_val);
  snprintf(end_key_buf, 32, "%d", end_key_val);
  ObNewRange range;
  range.start_key_ = make_rowkey(start_key_buf, &allocator_);
  range.end_key_ = make_rowkey(end_key_buf, &allocator_);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  param.reset();
  param.set_version_range(version_range);
  param.set_is_result_cached(true);
  param.set_read_mode(ObScanParam::PREREAD);
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = param.set(OB_INVALID_ID, msolap::target_table_name,range,true))))
  {
    TBSYS_LOG(WARN,"fail to set scan param [err:%d]", err);
  }
  return err;
}


void msolap::gen_key_range(OlapConf &conf, uint32_t &start_key, uint32_t &end_key)
{
  static uint32_t key_range = conf.get_max_scan_count() - conf.get_min_scan_count() + 1;
  static uint32_t total_key_count = conf.get_end_key() - conf.get_start_key() + 1;
  uint32_t scan_count = static_cast<uint32_t>(conf.get_min_scan_count() + random()%key_range);
  start_key = static_cast<uint32_t>(conf.get_start_key() + random()%(total_key_count - scan_count + 1));
  end_key = start_key + scan_count - 1;
}

char msolap::olap_get_column_name(const uint64_t column_id)
{
  oceanbase::common::ObString cname;
  static __thread char c ;
  c = static_cast<char>('a' + (column_id - min_column_id));
  return c;
}
