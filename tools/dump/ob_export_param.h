// =====================================================================================
//
//       Filename:  ob_export_param.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年10月30日 14时48分43秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================

#ifndef __OB_EXPORT_PARAM_H__
#define __OB_EXPORT_PARAM_H__

#include <vector>
#include <string>
#include "tokenizer.h"
#include "common/ob_schema.h"
#include "charset.h" //add by zhuxh:20161018 [add class Charset]

using namespace oceanbase::common;
using namespace std;

static const int64_t kDefaultDatafileSize = 1 * 1024 * 1024 * 1024; //数据文件默认大小1GB
static const int64_t kDefaultConcurrency = 10;//默认并发度
static const int64_t KDefaultReocrdNum = 1000 * 100 * 100;//1000万

// 去除sql末尾的分号，如果存在则去掉，不存在则不做任何操作
int remove_last_comma(const char *str, char* buff, size_t buff_len);

struct ColumnFormat {
	string name;
	string format;
};

struct ObExportColumnInfo {
	ObExportColumnInfo():col_schema(NULL),
				is_add_quotes(false),
				is_del_point(false),
				is_format(false)
				{ }
	
	const ObColumnSchemaV2 *col_schema;

	bool is_add_quotes;
	bool is_del_point;
	bool is_format;

	int idx;
};

struct ObExportTableParam {
  ObExportTableParam() : delima('\017'), rec_delima('\n'),is_multi_tables_(0),code_conversion_flag(e_non) //add by zhuxh:20161018 [add class Charset]
  { }

  string table_name;

  RecordDelima delima;
  RecordDelima rec_delima;

  int is_multi_tables_;//add qianzm [multi-tables] 20151228
  string tablet_info_file;//add qianzm [export_by_tablets] 20160415

  string export_sql;
  //format
  vector<ColumnFormat> col_formats;
  vector<string> add_quotes;
  vector<string> del_point;
  int max_file_size;
  int max_record_num;

  CodeConversionFlag code_conversion_flag; //add by zhuxh:20161018 [add class Charset]
};

class ExportParam {
public:
	ExportParam();
	virtual ~ExportParam();

	int load(const char *file);

	int get_table_param(const char *table_name, ObExportTableParam &param);

	void PrintDebug();

private:
	vector<ObExportTableParam> params_;
};
#endif
