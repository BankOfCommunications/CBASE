// =====================================================================================
//
//       Filename:  ob_export_param.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年10月31日 13时20分44秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================

#include "ob_export_param.h"
#include "common/utility.h"

//static const char *kColumnDesc = "column_desc";
//static const char *kDataFile = "datafile";
//static const char *kMaxFileSize = "max_file_size";
//static const char *kMaxRecordNum = "max_record_num";
//static const char *kConcurrency = "concurrency";
//static const char *kDelima = "delima";
//static const char *kRecDelima = "rec_delima";
static const char *kColumnFormat = "column_format";
static const char *kAddQuotes = "add_quotes";
static const char *kDelPoint = "del_point";
static const char *kExportSql = "export_sql";

// 去除sql末尾的分号，如果存在则去掉，不存在则不做任何操作
int remove_last_comma(const char *str, char* buff, size_t buff_len)
{
  int ret = OB_SUCCESS;
  strncpy(buff, str, buff_len);
  size_t str_len = strlen(buff);
  int i = (int)(str_len - 1);
  for(; i >= 0; i--)
  {
    if(buff[i] == '\n'
       || buff[i] == '\t'
       || buff[i] == ' '
    )
    {
      continue;
    }
    else if(buff[i] == ';')
    {
      buff[i] = '\0';
      break;
    }
    else
    {
      break;
    }
  }
  return ret;
}

ExportParam::ExportParam()
{

}

ExportParam::~ExportParam()
{

}

int ExportParam::load(const char *file)
{
	int ret = OB_SUCCESS;
	if (NULL == file) {
		TBSYS_LOG(ERROR, "check conf file failed, file[%s]", file);
		ret = OB_ERROR;
	} else {
		ret = TBSYS_CONFIG.load(file);
		if (ret != OB_SUCCESS) {
			TBSYS_LOG(ERROR, "load conf failed, file[%s], ret[%d]", file, ret);
		}
	}

	if (OB_SUCCESS == ret) {
		vector<string> all_sections;
		TBSYS_CONFIG.getSectionName(all_sections);

		for(size_t i = 0; i < all_sections.size(); i++) {
			ret = OB_SUCCESS;
			ObExportTableParam param;
			param.table_name = all_sections[i];
			
			//init export query
			const char *str_sql = TBSYS_CONFIG.getString(param.table_name.c_str(), kExportSql);
                        if (str_sql != NULL) {
                                param.export_sql = str_sql;
                        } else {
                                TBSYS_LOG(WARN, "table[%s] export query is not specified, please check config file[%s]", param.table_name.c_str() ,file);
                                //break;
                        }
#if 0
			//init data_file
			const char *data_file = TBSYS_CONFIG.getString(param.table_name.c_str(),kDataFile);
			if (data_file != NULL) {
				param.data_file = data_file;
			}
#endif
			
#if 0 //分隔符也移到命令行指定
			//init delima and rec_delima
			const char *str_delima = TBSYS_CONFIG.getString(param.table_name.c_str(), kDelima);
			if (str_delima != NULL) {
				const char *end_pos = str_delima + strlen(str_delima);
				if (find(str_delima, end_pos, ',') == end_pos) {
					param.delima = RecordDelima(static_cast<char>(atoi(str_delima)));
				} else {
					int part1, part2;
					sscanf(str_delima, "%d,%d", &part1, &part2);
					param.delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
				}
			}

			const char *str_rec_delima = TBSYS_CONFIG.getString(param.table_name.c_str(), kRecDelima);
			if (str_rec_delima != NULL) {
				const char *end_pos = str_rec_delima + strlen(str_rec_delima);
				if (find(str_rec_delima, end_pos, ',') == end_pos) {
					param.rec_delima = RecordDelima(static_cast<char>(atoi(str_rec_delima)));
				} else {
					int part1, part2;
					sscanf(str_rec_delima, "%d,%d", &part1, &part2);
					param.rec_delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
				}
			}
			//这个条件可能不需要约束
			if (param.delima.delima_type() != param.rec_delima.delima_type()) {
				TBSYS_LOG(WARN, "%s:delima and rec_delima length is not same, skiping config", param.table_name.c_str());
				continue;
			}
#endif
#if 0 //这部分参数已经移除,全部放置于命令行参数
			//init concurrency
			const char *str_concurr = TBSYS_CONFIG.getString(param.table_name.c_str(), kConcurrency);
			if (str_concurr != NULL) {
				param.concurrency = atoi(str_concurr);
			}

			//init max file size
			const char *str_filesize = TBSYS_CONFIG.getString(param.table_name.c_str(), kMaxFileSize);
			if (str_filesize != NULL) {
				param.max_file_size = atoi(str_filesize) * 1024 * 1024;
			}
			//init max reocrd num
			const char *str_recordnum = TBSYS_CONFIG.getString(param.table_name.c_str(), kMaxRecordNum);
			if (str_recordnum != NULL) {
				param.max_record_num = atol(str_recordnum);
			}
#endif
#if 1
			//init add quote column
			const char *add_quotes_columns_str = TBSYS_CONFIG.getString(param.table_name.c_str(), kAddQuotes);
			if (add_quotes_columns_str != NULL) {
				string value = add_quotes_columns_str;
				Tokenizer::tokenize(value, param.add_quotes, ',');
				if (param.add_quotes.size() <= 0) {
					TBSYS_LOG(WARN, "error %s config, has no values", kAddQuotes);
				}
			}

			//init del point column
			const char *del_point_columns_str = TBSYS_CONFIG.getString(param.table_name.c_str(), kDelPoint);
			if (del_point_columns_str != NULL) {
				string value = del_point_columns_str;
				Tokenizer::tokenize(value, param.del_point, ',');
				if (param.add_quotes.size() <= 0) {
					TBSYS_LOG(WARN, "error %s config, has no values", kDelPoint);
				}
			}

			//init date/time/timestamp format column
			vector<const char *> format_columns_desc = TBSYS_CONFIG.getStringList(param.table_name.c_str(), kColumnFormat);
			for (size_t idx = 0; idx < format_columns_desc.size(); idx++) {
				string value = format_columns_desc[idx];
				vector<string> res;

				Tokenizer::tokenize(value, res, ',');
				if (res.size() != 2) {
					ret = OB_ERROR;
					TBSYS_LOG(ERROR, "error column config, %s=%s", kColumnFormat, format_columns_desc[idx]);
					break;
				}

				ColumnFormat col_format;
				col_format.name = res[0];
				col_format.format = res[1];

				param.col_formats.push_back(col_format);
			}

			if (ret != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "parse table[%s] config failed", param.table_name.c_str());
				continue;
			}
#endif
			params_.push_back(param);
		}	
	}

	return ret;
}

int ExportParam::get_table_param(const char *table_name, ObExportTableParam &param)
{
	for(size_t i = 0; i < params_.size(); i++) {
		if (params_[i].table_name == table_name) {
			param = params_[i];
			return OB_SUCCESS;
		}
	}

	return OB_ERROR;
}

void ExportParam::PrintDebug()
{
	for(size_t i = 0; i < params_.size(); i++) {
		ObExportTableParam param = params_[i];

		TBSYS_LOG(DEBUG, "#######################################################################");
		TBSYS_LOG(DEBUG, "table name=[%s]", param.table_name.c_str());
		TBSYS_LOG(DEBUG, "delima type = %d, part1 = %d, part2 = %d", param.delima.type_, param.delima.part1_, param.delima.part2_);
		TBSYS_LOG(DEBUG, "rec_delima type = %d, part1 = %d, part2 = %d", param.rec_delima.type_, param.rec_delima.part1_, param.rec_delima.part2_);
		//TBSYS_LOG(DEBUG, "max file size = %ld", param.max_file_size);
		//TBSYS_LOG(DEBUG, "max record num = %ld", param.max_record_num);
		//TBSYS_LOG(DEBUG, "concurrency = %d", param.concurrency);
		TBSYS_LOG(DEBUG, "export sql = %s", param.export_sql.c_str());
		size_t idx;
		for (idx = 0; idx < param.col_formats.size(); idx++) {
			TBSYS_LOG(DEBUG, "column format = [%s,%s]", param.col_formats[idx].name.c_str(), param.col_formats[idx].format.c_str());
		}
		for (idx = 0; idx < param.add_quotes.size(); idx++) {
			TBSYS_LOG(DEBUG, "add quotes = [%s]", param.add_quotes[idx].c_str());
		}
		for (idx = 0; idx < param.del_point.size(); idx++) {
			TBSYS_LOG(DEBUG, "del point = [%s]", param.del_point[idx].c_str());
		}
	}
}

