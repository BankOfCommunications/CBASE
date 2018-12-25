#include "ob_check_param.h"
#include "common/utility.h"
#include <vector>
#include "ob_check.h" //add by pangtz:20141218

using namespace std;

static const char *kColumnDesc = "column_desc";
static const char *kDataFile = "datafile";
static const char *kInputColumnNr = "input_column_nr";
static const char *kDelima = "delima";
static const char *kRecDelima = "rec_delima";
static const char *kNopFlag = "nop_flag";
static const char *kNullFlag = "null_flag";
static const char *kConcurrency = "concurrency";
static const char *kHasTableTitle = "has_table_title";
static const char *kBadFile = "bad_file";
//static const char *kTrim = "trim"; //delete by pangtz:20141127
static const char *kDecimalToVarchar = "decimal_to_varchar";		//add by liyongfeng 20140807
static const char *kSubstr = "substr";		//add by pangtz:20141217

ImportParam::ImportParam()
{
}


int ImportParam::load(const char *file)
{
  int ret = OB_SUCCESS;
  if (NULL == file)
  {
    TBSYS_LOG(ERROR, "check conf file failed:file[%s]", file);
    ret = OB_ERROR;
  }
  else
  {
    ret = TBSYS_CONFIG.load(file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load conf failed:file[%s], ret[%d]", file, ret);
    }
  }

  if (ret == OB_SUCCESS) {
    std::vector<std::string> all_sections;
    TBSYS_CONFIG.getSectionName(all_sections);

    for(size_t i = 0;i < all_sections.size() && ret == OB_SUCCESS; i++) {
      TableParam param;
      param.table_name = all_sections[i];

      //init data_file
      const char *data_file  = TBSYS_CONFIG.getString(param.table_name.c_str(), kDataFile);
      if (data_file != NULL) {
        param.data_file = data_file;
      } 

      param.bad_file_ = TBSYS_CONFIG.getString(param.table_name.c_str(), kBadFile);

      const char *str_input_column_nr = 
        TBSYS_CONFIG.getString(param.table_name.c_str(), kInputColumnNr);
      if (str_input_column_nr == NULL) {
        TBSYS_LOG(WARN, "input columns nr is not specified for table=%s, skiping it", 
                  param.table_name.c_str());
        continue;
      }
      param.input_column_nr = atoi(str_input_column_nr);

      const char *str_delima = 
        TBSYS_CONFIG.getString(param.table_name.c_str(), kDelima);
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

      const char *str_rec_delima = 
        TBSYS_CONFIG.getString(param.table_name.c_str(), kRecDelima);
      if (str_rec_delima != NULL) {
        const char *end_pos = str_rec_delima + strlen(str_delima);

        if (find(str_rec_delima, end_pos, ',') == end_pos) {
          param.rec_delima = RecordDelima(static_cast<char>(atoi(str_rec_delima)));
        } else {
          int part1, part2;

          sscanf(str_rec_delima, "%d,%d", &part1, &part2);
          param.rec_delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
        }
      }

      if (param.delima.delima_type() != param.rec_delima.delima_type()) {
        TBSYS_LOG(WARN, "%s:delima and rec_delima is not same, skiping config", param.table_name.c_str());
        continue;
      }

      const char *str_concurr = TBSYS_CONFIG.getString(param.table_name.c_str(), kConcurrency);
      if (str_concurr != NULL) {
        param.concurrency = atoi(str_concurr);
      }

      const char *str_has_table_title = TBSYS_CONFIG.getString(param.table_name.c_str(), kHasTableTitle);
      if (str_has_table_title != NULL) {
        param.has_table_title = (atoi(str_has_table_title) != 0);
      }
      else {
        param.has_table_title = false;
      }

      const char *str_nop_flag = TBSYS_CONFIG.getString(param.table_name.c_str(), kNopFlag);
      if (str_nop_flag != NULL) {
        param.has_nop_flag = true;
        param.nop_flag = static_cast<char>(atoi(str_nop_flag));
      } 
      else {
        param.has_nop_flag = false;
      }

      const char *str_null_flag = TBSYS_CONFIG.getString(param.table_name.c_str(), kNullFlag);
      if (str_null_flag != NULL) {
        param.has_null_flag = true;
        param.null_flag = static_cast<char>(atoi(str_null_flag));
      }
      else {
        param.has_null_flag = false;
      }

	  //add by pangtz:20141217 加载substr配置信息
	  const char *str_substr = TBSYS_CONFIG.getString(param.table_name.c_str(), kSubstr);
	  if(str_substr != NULL) {
		  param.has_substr = true;
		  vector<string> substr_info;
		  Tokenizer::tokenize(str_substr,substr_info, ',');

		  for(size_t index = 0; index < substr_info.size(); index++){
			    SubstrInfo substr_struct;
				vector<string> substr_info_original;
				Tokenizer::tokenize(substr_info[index],substr_info_original, ':');
				if(substr_info_original.size() != 2){
					ret = OB_ERROR;
					error_arr[11] = false;
					TBSYS_LOG(ERROR, "error substr config, %s", str_substr);
					break;
				}
			  std::string one_substr = substr_info_original[1];
			  vector<string> one_substr_info;
			  Tokenizer::tokenize(one_substr, one_substr_info, ';');
			  if(one_substr_info.size() != 3){
				  ret = OB_ERROR;
				  error_arr[11] = false;
				  TBSYS_LOG(ERROR, "error substr config, %s", str_substr);
				  break;
			  }
			  	  substr_struct.source_column_name = one_substr_info[0];
				  substr_struct.column_name = substr_info_original[0];
				  substr_struct.beg_pos = atoi(one_substr_info[1].c_str());
				  substr_struct.len = atoi(one_substr_info[2].c_str());
				  param.substr_map[substr_struct.column_name] = substr_struct;
		  }

	  }
	  //add e

#if 0 
//delete by pangtz:20141127
     const char *trim_str = TBSYS_CONFIG.getString(param.table_name.c_str(), kTrim);
      if(trim_str != NULL) {
        param.is_trim_ = (atoi(trim_str) != 0);
      }
#endif

      const char *decimal_to_varchar_str = TBSYS_CONFIG.getString(param.table_name.c_str(), kDecimalToVarchar);	//add by liyongfeng 20140807:e
      if(decimal_to_varchar_str != NULL) {
          vector<string> decimal_to_varchar;
          Tokenizer::tokenize(decimal_to_varchar_str, decimal_to_varchar, ',');
          for(size_t index = 0; index < decimal_to_varchar.size(); index++) {
              int column_offset = atoi(decimal_to_varchar[index].c_str());
              param.decimal_to_varchar_fields_.push_back(column_offset);
          }
      }//add:e

      std::vector<const char *> columns_desc = 
        TBSYS_CONFIG.getStringList(param.table_name.c_str(), kColumnDesc);

      size_t idx = 0;
      //setup columns desc
      for (;idx < columns_desc.size();idx++) {
        string str = columns_desc[idx];
        vector<string> res;

        Tokenizer::tokenize(str, res, ',');
        if (res.size() != 2) {                  /* name,[int]--offset */
          ret = OB_ERROR;
		  error_arr[11] = false;//add by pangtz:20141218
          TBSYS_LOG(ERROR, "error column config, %s", str.c_str());
          break;
        }

        ColumnDesc col_desc;
        col_desc.name = res[0];
        col_desc.offset = atoi(res[1].c_str());

        param.col_descs.push_back(col_desc);
      }
      params_.push_back(param);
    }
  }

  return ret;
}

int ImportParam::get_table_param(const char *table_name, TableParam &param)
{
  for(size_t i = 0;i < params_.size(); i++) {
    if (params_[i].table_name == table_name) {
      param = params_[i];
      return OB_SUCCESS;
    }
  }

  return OB_ERROR;
}

void ImportParam::PrintDebug()
{
  for(size_t i = 0;i < params_.size(); i++) {
    TableParam param = params_[i];

    fprintf(stderr, "#######################################################################");

    fprintf(stderr, "table name=[%s]\n", param.table_name.c_str());
    fprintf(stderr, "data file=[%s]\n", param.data_file.c_str());
    fprintf(stderr, "input column nr=[%d]\n", param.input_column_nr);
    fprintf(stderr, "delima type = %d, part1 = %d, part2 = %d\n", param.delima.type_, param.delima.part1_, param.delima.part2_ );
    fprintf(stderr, "rec_delima type = %d, part1 = %d, part2 = %d\n", param.rec_delima.type_, 
            param.rec_delima.part1_, param.rec_delima.part2_ );

    size_t idx = 0;

    for (;idx < param.col_descs.size();idx++) {
      ColumnDesc desc = param.col_descs[idx];
      fprintf(stderr, "column desc=[%s, %d]\n", desc.name.c_str(), desc.offset);
    }
  }
}

