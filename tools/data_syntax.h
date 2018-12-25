#ifndef  OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_
#define OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_
#include "common/ob_object.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    const int MAX_COLUMS = 50;
    const int MAX_LINE_LEN = 4096;

    struct data_format
    {
      common::ObObjType type;
      int32_t len;
      int32_t column_id;
      int32_t index; //index in raw data, -1 means the item is new ,no data in raw data
    };

    enum RowKeyType 
    {
      INT8 = 0,
      INT16,
      INT32,
      INT64,
      VARCHAR,
      DATETIME
    };

    struct row_key_format
    {
      RowKeyType type;
      int32_t len;
      int32_t index;  //index in raw data
      int32_t flag;
    };

    //static struct row_key_format INFO_ROW_KEY_ENTRY[] = 
    //{
    //  {INT64,8,0},
    //  {INT8,1,2},
    //  {INT64,8,1}
    //};
    //
    //static const int32_t INFO_ROW_KEY_ENTRIES_NUM = sizeof(INFO_ROW_KEY_ENTRY) / sizeof(INFO_ROW_KEY_ENTRY[0]);

    //static struct row_key_format ITEM_ROW_KEY_ENTRY[] = 
    //{
    //  {INT8,1,1},
    //  {INT64,8,0}
    //};
    //static const int32_t ITEM_ROW_KEY_ENTRIES_NUM = sizeof(ITEM_ROW_KEY_ENTRY) / sizeof(ITEM_ROW_KEY_ENTRY[0]);

    //static struct data_format INFO_ENTRY[] = 
    //{
    //  {common::ObIntType,               8,      0,0},
    //  {common::ObIntType,               4,      0,1},
    //  {common::ObIntType,               8,      0,2},
    //  {common::ObVarcharType,           32,     2,3},    //user_nick
    //  {common::ObIntType,               4,      3,4},    //is_shared
    //  {common::ObDateTimeType,          8,      4,5},    //collect_time
    //  {common::ObIntType,               4,      5,6},    //status
    //  {common::ObCreateTimeType,        8,      6,7},    //gm_create
    //  {common::ObModifyTimeType,        8,      7,8},    //gm_modified
    //  {common::ObVarcharType,           105,    8,9},    //tag
    //  {common::ObIntType,               4,      9,12},    //category
    //  {common::ObVarcharType,           256,    10,13},   //title
    //  {common::ObVarcharType,           256,    11,14},   //picurl
    //  {common::ObIntType,               8,      12,15},   //owner id
    //  {common::ObVarcharType,           32,     13,16},   //owner nick
    //  {common::ObIntType  ,             4,      14,17},   //price
    //  {common::ObDateTimeType,          8,      15,18},   //ends
    //  {common::ObVarcharType,           2048,   16,19},   //proper_xml
    //  {common::ObIntType,               8,      17,20},   //collector count
    //  {common::ObIntType,               8,      18,21},   //collect_count
    //  {common::ObPreciseDateTimeType,   8,      19,22},   //item_create_time
    //  {common::ObPreciseDateTimeType,   8,      20,23},   //item_modify_time
    //  {common::ObIntType,               4,      21,24},   //item_status
    //  {common::ObIntType,               4,      22,10},   //item_status
    //  {common::ObIntType,               4,      23,11},   //item_status
    //  {common::ObIntType,               4,      24,-1},   //invalid
    //};

    //static const int32_t INFO_DATA_ENTRIES_NUM = sizeof(INFO_ENTRY) / sizeof(INFO_ENTRY[0]);

    //static struct data_format ITEM_ENTRY[] = 
    //{
    //  {common::ObIntType,               8,      0,0},
    //  {common::ObIntType,               4,      0,1},
    //  {common::ObIntType,               4,      2,2},    //category
    //  {common::ObVarcharType,           32,     3,3},    //title
    //  {common::ObVarcharType,           256,    4,4},    //picurl
    //  {common::ObIntType,               8,      5,5},    //owner id
    //  {common::ObVarcharType,           32,     6,6},    //owner nick
    //  {common::ObIntType,               4,      7,7},    //price
    //  {common::ObDateTimeType,          8,      8,8},    //ends
    //  {common::ObVarcharType,           2048,   9,9},    //proper_xml
    //  {common::ObIntType,               8,      10,10},  //collector count
    //  {common::ObIntType,               8,      11,11},  //collect_count
    //  {common::ObCreateTimeType,        8,      12,12},  //gm_create
    //  {common::ObModifyTimeType,        8,      13,13},  //gm_modified
    //  {common::ObIntType,               4,      14,14},  //item_status
    //  {common::ObIntType,               4,      15,-1},  //invalid
    //};

    //static const int32_t ITEM_DATA_ENTRIES_NUM = sizeof(ITEM_ENTRY) / sizeof(ITEM_ENTRY[0]);

  } /* chunkserver */
} /* oceanbase */
#endif /*OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_*/

