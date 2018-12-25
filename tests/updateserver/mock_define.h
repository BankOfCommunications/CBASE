#ifndef MERGESERVER_MOCK_DEFINE_H_ 
#define MERGESERVER_MOCK_DEFINE_H_
#include "common/ob_define.h"
namespace mock
{
/// left column is test_table.test_column2, right column is test_join_table.test_join_column1
/// join rowkey is "test_join" prefix
  static const char* join_table_name  = "test_join_table";
  static const char* join_column1     =  "test_join_column1"; 
  static const char* join_column2     =  "test_join_column2";
  static const char* join_rowkey      =  "test_join";
  static const char* table_name       =  "test_table";
  static const char* column1          =  "test_column1";
  static const char* column2          =  "test_column2";
  static const char* rowkey           =  "test_join_rowkey";
  static const uint64_t schema_timestamp = 1025;
  static const uint64_t join_column1_id = 22;
  static const uint64_t join_column2_id = 23;
  static const uint64_t column1_id = 12;
  static const uint64_t column2_id = 13;
  static const int32_t MOCK_SERVER_LISTEN_PORT = 7070;
  static const int64_t NETWORK_TIMEOUT = 100 * 1000L;

  static const int64_t join_column1_cs_value = 5;
  static const int64_t join_column1_ups_value_1 = +10;
  static const int64_t join_column1_ups_value_2 = -100;
  static const int64_t join_column1_final_value = join_column1_cs_value + join_column1_ups_value_1 
                                                  + join_column1_ups_value_2;

  static const int64_t join_column2_cs_value = 10;
  static const int64_t join_column2_ups_value_1 = +100;
  static const int64_t join_column2_ups_value_2 = -1000;
  static const int64_t join_column2_final_value = join_column2_cs_value + join_column2_ups_value_1 
                                                  + join_column2_ups_value_2;

  static const int64_t column1_cs_value = 5;
  static const int64_t column1_ups_value_1 = +876;
  static const int64_t column1_ups_value_2 = -958;
  static const int64_t column1_final_value = column1_cs_value + column1_ups_value_1 
                                             + column1_ups_value_2;

  static const int64_t column2_cs_value = 587;
  static const int64_t column2_ups_value_1 = +682;
  static const int64_t column2_ups_value_2 = -777;
  static const int64_t column2_final_value = column2_cs_value + column2_ups_value_1 
                                             + column2_ups_value_2 + join_column1_ups_value_1
                                             + join_column1_ups_value_2;


  static const uint64_t join_table_id = 1002;
  static const uint64_t table_id = 1001;

  inline void unused()
  {
    UNUSED(join_table_name);
    UNUSED(join_column1);
    UNUSED(join_column2);
    UNUSED(join_rowkey);
    UNUSED(table_name);
    UNUSED(column1);
    UNUSED(column2);
    UNUSED(rowkey);
  }
}
#endif /* MERGESERVER_MOCK_DEFINE_H_ */
