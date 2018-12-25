#ifndef OLAP_OLAP_H_ 
#define OLAP_OLAP_H_
#include <arpa/inet.h>
#include "sstable/ob_sstable_trailer.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "olap_conf.h"
#include <assert.h>
namespace msolap
{
  static const uint64_t target_table_id = 1001;
  extern const char * target_table_name_cstr;
  extern oceanbase::common::ObString target_table_name;
  static const uint64_t ups_sst_block_size = 64*1024;
  static const char * ups_sst_compressor_name = "snappy_1.0";
  static const int ups_sst_store_type = oceanbase::sstable::OB_SSTABLE_STORE_DENSE;
  static const int64_t ups_sst_version = 2;
  static const int64_t ups_sst_column_group_id = 0;

  static const uint64_t min_column_id = 11;
  static const uint64_t max_column_id = 36;
  static const char  min_column_name = 'a';
  static const char  max_column_name = 'z';

  inline void UNUSED_DEF()
  {
    UNUSED(target_table_name);
    UNUSED(ups_sst_compressor_name);
  }



  char olap_get_column_name(const uint64_t column_id);

  inline uint32_t olap_get_column_val(const uint64_t column_id, const uint32_t big_endian_row_key)
  {
    uint32_t val = 0;
    assert((column_id >= min_column_id) && (column_id <= max_column_id));
    switch (column_id)
    {
    case 11:
    case 12:
    case 13:
    case 14:
      val = ((unsigned char*)&(big_endian_row_key))[column_id - min_column_id];
      break;
    default:
      val = ntohl(big_endian_row_key);
      break;
    }
    return val;
  }



  inline uint32_t olap_get_column_val(const oceanbase::common::ObString column_name, const uint32_t big_endian_row_key)
  {
    uint32_t val = 0;
    assert((column_name.length() == 1) 
      && (column_name.ptr()[0] <= max_column_name)
      && (column_name.ptr()[0] >= min_column_name));
    int32_t idx = column_name.ptr()[0] - min_column_name;
    switch (idx)
    {
    case 0:
    case 1:
    case 2:
    case 3:
      val = ((unsigned char*)&(big_endian_row_key))[idx];
      break;
    default:
      val = ntohl(big_endian_row_key);
      break;
    }
    return val;
  }

  inline bool olap_check_cell(const oceanbase::common::ObCellInfo & cell)
  {
    bool res = true;
    if (res && (cell.row_key_.length() != sizeof(uint32_t)))
    {
      TBSYS_LOG(WARN,"rowkey size error [size:%ld]",  cell.row_key_.length());
      res = false;
    }
    uint32_t big_endian_row_key = 0;
    int64_t val = 0;
    if (res)
    {
      big_endian_row_key = *(uint32_t*)cell.row_key_.ptr();
    }
    if (res && (oceanbase::common::OB_SUCCESS != cell.value_.get_int(val)))
    {
      res = false;
      TBSYS_LOG(WARN,"fail to get int val from cell");
    }
    if (res && (val != static_cast<int64_t>(olap_get_column_val(cell.column_name_,big_endian_row_key))))
    {
      TBSYS_LOG(WARN,"value not correct [expect:%u,got:%ld]", olap_get_column_val(cell.column_name_,big_endian_row_key), val);
      res = false;
    }
    return res;
  }

  int init_scan_param(oceanbase::common::ObScanParam &param, const uint32_t min_key_include, const uint32_t max_key_include);

  void gen_key_range(OlapConf &conf, uint32_t &start_key, uint32_t &end_key);
}

#endif /* OLAP_OLAP_H_ */
