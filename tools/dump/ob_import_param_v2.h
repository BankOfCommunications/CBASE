#ifndef __OB_IMPORT_PARAM_H__
#define  __OB_IMPORT_PARAM_H__

#include <vector>
#include <string>
#include <map>
#include <set> // add by zhuxh:20150803
#include <limits.h> //add by zhuxh:20160907 [cb Sequence]
#include "tokenizer_v2.h" //modify by zhuxh:20150728
#include "common/ob_schema.h"
#include "db_record.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::api;

//add by zhuxh:20151201 :b
#ifndef SC
#define SC(X) strcat(str,X)
#endif

//512k
#ifndef MAX_CONSTRUCT_SQL_LENGTH
#define MAX_CONSTRUCT_SQL_LENGTH (1<<19)
#endif

//512k
#ifndef MAX_TOKEN_LENGTH
#define MAX_TOKEN_LENGTH (1<<19)
#endif

//512k
#define IMPORT_DEFAULT_READ_BUFFER_SIZE (1<<19)

#define IMPORT_ERROR_TYPE_NUM 42

//add by zhuxh:20160603 [Sequence] :b
//mod by zhuxh:20161121 [Sequence] //Too short length leads to memory overflow.
#define IMPORT_SEQUENCE_MAX_LENGTH 200
//add :e

enum ImportErrorType{
  NORMAL = 0, //正常
  TABLE_NOT_EXIST = 1, //表不存在
  DATAFLIE_NOT_EXIST = 2, //数据文件不存在
  DATAFLIE_COLUMN_NUM_ERROR = 3, //字段数错误
  DATA_ERROR = 4, //脏数据
  NOT_NULL_CONSTRAIN_ERROR = 5, //非空约束问题
  SYSTEM_ERROR = 6, //系统问题
  LOCK_CONFLICT = 7, //锁冲突
  VARCHAR_OVER_FLOW = 8, //字符串超长
  OVER_MAX_ROWKEY_NUM = 9, //主键数超出最大限制
  LINE_TOO_LONG = 10, //一行数据过长
  RS_ERROR = 11, //rs异常
  CONF_FILE_ERROR = 12, //配置文件错误
  DELETE_ERROR = 13, //数据删除错误
  TABLE_CONF_NOT_EXIST = 14, //配置文件不存在
  MEMORY_SHORTAGE = 15, //服务器内存不足
  WRONG_TIME_FORMAT = 16, //错误的时间格式
  READ_FILE_ERROR = 17, //读文件错误
  CREATE_BAD_FILE_ERROR = 18,
  PARSE_TABLE_TITLE_ERROR = 19,
  DATE_FORMAT_ERROR = 20,
  PRODUCE_AND_COMSUME_ERROR = 21,
  PARAM_APPEND_ACCDATE_ERROR = 22,
  COLUMN_DESC_SET_ERROR = 23,
  ROWKEY_SET_ERROR = 24,
  GET_TABLE_PARAM_ERROR = 25,
  ALLOCATE_MEMORY_ERROR = 26,
  MS_MANAGER_INIT_ERROR = 27,
  BAD_FILE_ERROR = 28,
  MEMORY_OVERFLOW = 29,
  SQL_BUILDING_ERROR = 30,
  GET_ONE_MS_ERROR = 31,
  MS_REF_COUNT_ERROR = 32,
  FETCH_MS_LIST_ERROR = 33,
  SUBSTR_GRAMMAR_ERROR = 34,
  SUBSTR_COLUMN_NOT_EXIST = 35,
  DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST = 36,
  MAPPING_COLUMN_NOT_EXIST = 37,
  COLUMN_WITH_CHAR_DELIMA_NOT_EXIST = 38,
  INCOMPLETE_DATA = 39,
  G2U_ERROR = 40,
  BUFFER_OVER_FLOW = 41
};

extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;
extern bool trim_end_blank;
extern bool error_arr[IMPORT_ERROR_TYPE_NUM];
extern int kReadBufferSize;
//add :e

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
  const static int DEFAULT_TOKEN_COLUMN; //add by zhuxh:20150825
  const static int SEQUENCE;//add by zhuxh:20160603 [Sequence]
  std::string default_token; //add by zhuxh:20150825
  ColumnInfo():schema(NULL),offset(DEFAULT_TOKEN_COLUMN)
  {}
/*
There are 2 conditions that token is valuated by default:
(1) offset == DEFAULT_TOKEN_COLUMN, which means the column isn't from any datafile column.
(2) Datafile token is NULL
 */
};

//add by pangtz:20141217 存储substr信息的结构体
struct SubstrInfo {
    const static int UNDEFINED;
    std::string column_name;
    int datafile_column_id; //modify by zhuxh:20150906
    int beg_pos;
    int len;
    SubstrInfo()
    : datafile_column_id(UNDEFINED),
      beg_pos(0),
      len(INT_MAX>>1)
      { }
};
//add end

//structure for --columns-map
//add by zhuxh:20150820 :b
//mod by zhuxh:20160506 [sequence] :b
struct ColumnMapInfo
{
  const static int UNDEFINED;
  const static int SEQUENCE;
  std::string column_name;
  int datafile_column_id;

  std::string default_token;
  int seq_index;

  ColumnMapInfo()
    :datafile_column_id(UNDEFINED),seq_index(UNDEFINED)
  {}
  void clear()
  {
    column_name.clear();
    datafile_column_id = UNDEFINED;
    default_token.clear();
    seq_index = UNDEFINED; //add by zhuxh:20160603 [Sequence]
  }
};
//mod:e
//add:e

//add by zhuxh:20160504 [sequence]:b
struct SequenceInfo
{
  //add by zhuxh:20160907 [cb Sequence] :b
  const static int64_t UNDEFINED;
  string column_name;
  //add by zhuxh:20160907 [cb Sequence] :e
  int64_t start;
  int64_t increment;
};
//add :e

struct TableParam {
  TableParam() 
    : //input_column_nr(0), ////delete by zhuxh:20150727
      delima(15),      //mod by zhangcd [modify the default field_delima] 20150814
      delima_str(NULL), //add by zhuxh:20150724
      rec_delima('\n'),
      rec_delima_str(NULL), //add by zhuxh:20150724
      has_nop_flag(false), 
      has_null_flag(false), 
      null_flag('\0'),//add by zhuxh:20150914
      concurrency(100), //mod by zhuxh:20170411 [performance optimization]
      has_table_title(false),
      bad_file_(NULL),
      is_delete(false),
      is_rowbyrow(false),
      trim_flag(-1), //add by pangtz:20141126 添加trim参数
      is_append_date_(false),//add by pangtz:20141126 添加append参数
      has_decimal_to_varchar(false), //add by zhuxh:20150731
      decimal_to_varchar_grammar(NULL), //add by zhuxh:20150731
      has_substr(true), //add by pangtz:20141217 添加substr参数
      substr_grammar(NULL), //add by zhuxh:20150724
      record_persql_(100),//add by zhuxh:20150914 //mod by zhuxh:20170411 [performance optimization]
      correctfile_(NULL),
      //add by zhangcd 20150814:b

      user_name_(NULL),
      passwd_(NULL),
      db_name_(NULL),
      timeout_(0),
      //add:e
      //add by zhuxh:20150811 :b
      has_column_map(false),
      column_map_grammar(NULL),
      has_char_delima(false),
      char_delima('\"'),
      columns_with_char_delima_grammar(NULL),
      all_columns_have_char_delima(false),

      //add :e
      varchar_not_null(false),//add by zhuxh:20151123
      progress(false),//add by zhuxh:20150303
      case_sensitive(false), //add by zhuxh:20160617 [sensitive case of database and table]
      //special_quotation_mark(false) //add by zhuxh:20160804 [special quotation mark]
      keep_double_quotation_marks(false) //add by zhuxh:20160912 [quotation mark]
      { }

  std::vector<ColumnDesc> col_descs;
  std::string table_name;
  std::string data_file;
  std::string appended_date;//add by pangtz:20141126 添加append参数
  //int input_column_nr; //delete by zhuxh:20150727
  RecordDelima delima;
  char * delima_str; //add by zhuxh:20150724
  RecordDelima rec_delima;
  char * rec_delima_str; ////add by zhuxh:20150724
  bool has_nop_flag;
  char nop_flag;
  bool has_null_flag;
  char null_flag;
  int concurrency;                              /* default 5 threads */
  bool has_table_title;
  const char *bad_file_;
  bool is_delete;
  bool is_replace;
  bool is_insert;
  bool is_rowbyrow;
  int trim_flag; //add by pangtz:20141115 [添加trim参数]
  bool is_append_date_; //add by pangtz:20141126 添加append参数
  bool has_decimal_to_varchar; //add by zhuxh:20150731
  char * decimal_to_varchar_grammar; //add by zhuxh:20150731
  mutable std::set<int> decimal_to_varchar_set;	//add by liyongfeng for decimal into varchar //mod by zhuxh:20150803
  bool has_substr;//add by pangtz:20141217 for substr
  char * substr_grammar; //add by zhuxh:20150724 for substr
  mutable std::map<int, SubstrInfo> substr_map; //add by pangtz:20141217 for substr
  int64_t record_persql_;
  bool ignore_merge_;
  const char *correctfile_;
  //add by zhangcd 20150814:b
  const char *user_name_;
  const char *passwd_;
  /*const*/ char *db_name_; //mod by zhuxh:20160617 [sensitive case of database and table]
  int timeout_;
  //add:e

  //add by zhuxh:20150811 :b
  bool has_column_map;
  char * column_map_grammar;
  bool has_char_delima;
  char char_delima;
  char * columns_with_char_delima_grammar;
  bool all_columns_have_char_delima;
  mutable std::set<int> columns_with_char_delima_set;

  bool varchar_not_null;//add by zhuxh:20151123 //true means NULL will be treated as empty varchar ''
  bool progress;//add by zhuxh:20160303 //true means showing progress while importing data
  bool case_sensitive; //add by zhuxh:20160617 [sensitive case of database and table]
  //bool special_quotation_mark; //add by zhuxh:20160804 [special quotation mark]
  bool keep_double_quotation_marks; //add by zhuxh:20160912 [quotation mark]
  //add :e
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
