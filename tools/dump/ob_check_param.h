#ifndef __OB_CHECK_PARAM_H__
#define  __OB_CHECK_PARAM_H__

#include <vector>
#include <string>
#include <map>
#include "tokenizer.h"
#include "common/ob_schema.h"

using namespace oceanbase::common;

struct ColumnDesc {
  std::string name;
  int offset;
  int len;
};

struct RowkeyDesc {
  int offset;
  int type;
  int pos;
};

struct ColumnInfo {
  const ObColumnSchemaV2 *schema;
  int offset;
};

//add by pangtz:20141217 存储substr信息的结构体
struct SubstrInfo {
	SubstrInfo()
	: beg_pos(-1),
	  len(-1)
	  { }
	std::string column_name;
	std::string source_column_name;
	int beg_pos;
	int len;
};
//add end

struct TableParam {
  TableParam() 
    : input_column_nr(0), 
      delima('\01'), 
      rec_delima('\n'), 
      has_nop_flag(false), 
      has_null_flag(false), 
      concurrency(10), 
      has_table_title(false),
      bad_file_(NULL),
      is_delete(false),
      is_rowbyrow(false),
      trim_flag(-1), //add by pangtz:20141126 添加trim参数
	  is_append_date_(false),//add by pangtz:20141126 添加append参数
	  has_substr(false) //add by pangtz:20141217 添加substr参数
      { }

  std::vector<ColumnDesc> col_descs;
  std::string table_name;
  std::string data_file;
  std::string appended_date;//add by pangtz:20141126 添加append参数
  int input_column_nr;
  RecordDelima delima;
  RecordDelima rec_delima;
  bool has_nop_flag;
  char nop_flag;
  bool has_null_flag;
  char null_flag;
  int concurrency;                              /* default 5 threads */
  bool has_table_title;
  const char *bad_file_;
  bool is_delete;
  bool is_rowbyrow;
  int trim_flag; //add by pangtz:20141115 [添加trim参数]
  bool is_append_date_; //add by pangtz:20141126 添加append参数
  std::vector<int> decimal_to_varchar_fields_;	//add by liyongfeng for decimal into varchar
  bool has_substr;//add by pangtz:20141217 for substr
  std::map<std::string, SubstrInfo> substr_map; //add by pangtz:20141217 for substr
};

class ImportParam {
  public:
    ImportParam();
	
    int load(const char *file);

    int get_table_param(const char *table_name, TableParam &param);

    void PrintDebug();
  private:
    std::vector<TableParam> params_;
};

#endif
