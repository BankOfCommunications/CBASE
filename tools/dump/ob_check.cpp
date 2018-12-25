#include "ob_check.h"
#include "db_utils.h"
#include <stdio.h> //add by pangtz:20141211
#include <stdlib.h> //add by pangtz:20141211
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include <string>
using namespace std;

using namespace oceanbase::common;

extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;


//add by pangtz:20141206

ObImportLogInfo::ObImportLogInfo()
{
	import_begin_time_ = 0;
	import_end_time_ = 0;
	during_time_ = 0;
	processed_lineno_ = 0;
	bad_lineno_ = 0;
	succ_lineno_ = 0;
}

void ObImportLogInfo::print_error_log()
{
	 fprintf(stdout, "\n###########################################\n");
	 fprintf(stdout, "#【IMPORT INFO】\n");
	 fprintf(stdout, "# table name: %s\n",get_table_name().data());
	 fprintf(stdout, "# data file: %s\n",get_datafile_name().data());
	 fprintf(stdout, "# begin time: %s\n", get_begin_time());
	 fprintf(stdout, "# end time: %s\n", get_end_time());
	 fprintf(stdout, "# during time: %lds\n", get_during_time());
	 fprintf(stdout, "# processed lines: %ld\n", get_processed_lineno());
	 fprintf(stdout, "# bad lines: %ld\n", get_bad_lineno());
	 std::string succ_str;
	 if(processed_lineno_ == 0){
		succ_str="0";
	// 	fprintf(stdout, "# successed lines: 0\n");
	 }else if(bad_lineno_ > processed_lineno_){
		 succ_str="unknow";
	//	 fprintf(stdout, "# successed line: unknow\n");
	 }else{
		 sprintf(const_cast<char *>(succ_str.c_str()),"%ld",get_succ_lineno());
	//	 fprintf(stdout, "# successed: %ld\n", get_succ_lineno());
	 }
	 fprintf(stdout, "# successed: %s\n", succ_str.data());
	 fprintf(stdout, "###########################################\n\n");
	if(final_ret_ != 0){
	 fprintf(stdout, "###########################################\n");
	 fprintf(stderr, "#【ERROR MESSAGE】\n");
	 if(!error_arr[11]){
		 error_arr[13] = true;
	 }
    int error_no = 0;
	bool error_flg = false;
	for(;error_no < 15;error_no++){
		if(!error_arr[error_no]){
			error_flg = true;
		}
		switch(error_no){
			case TABLE_NOT_EXIST:
				if(!error_arr[0]){
					fprintf(stderr, "# ERROR:%d  message: the table is not exist in db\n", error_no);
				}
				break;
			case COLUMN_NUM_ERROR:
				if(!error_arr[2]){
					fprintf(stderr, "# ERROR:%d  message: the real column number in data file is not equal with that in table schema\n", error_no);
				}
				break;
			case TABLE_CONF_NOT_EXIST:
				if(!error_arr[13]){
					fprintf(stderr, "# ERROR:%d  message: the config file is not exist\n", error_no);
				}
				break;
			case DATAFLIE_NOT_EXIST:
				if(!error_arr[1]){
					fprintf(stderr, "# ERROR:%d  message: the data file is not exist\n", error_no);
				}
				break;
			case DATA_ERROR:
				if(!error_arr[3]){
					fprintf(stderr, "# ERROR:%d  message: there are dirty records in data file，please look for details in import log\n", error_no);
				}
				break;
			case SYSTEM_ERROR:
				if(!error_arr[5]){
					fprintf(stderr, "# ERROR:%d  message: import error occurred, possibly due to oceanbase system exceptions，please look for details in import log\n", error_no);
				}
				break;
			case CONF_FILE_ERROR:
				if(!error_arr[11]){
					fprintf(stderr, "# ERROR:%d  message: the config file info does not accord with that in table schema, or config file info is not correct, please look for details in import log\n", error_no);
				}
				break;
			case NOT_NULL_CONSTRAIN_ERROR:
				if(!error_arr[4]){
					fprintf(stderr, "# ERROR:%d  message: the column data is not satisfied with not-null constrain\n", error_no);
				}
				break;
			case VARCHAR_OVER_FLOW:
				if(!error_arr[7]){
					fprintf(stderr, "# ERROR:%d  message: the length varchar column overflow, please look for details in import log\n", error_no);
				}
				break;
			case MEMORY_ERROR_NOT_ENOUGH:
				if(!error_arr[14]){
					fprintf(stderr, "# ERROR:%d  message: there is no enough memory to store schema\n", error_no);
				}
				break;
			case RS_ERROR:
				if(!error_arr[10]){
					fprintf(stderr, "# ERROR:%d  message: root server has swithced or no root server, please re-run ob_import\n", error_no);
				}
				break;

			default:				
				break;

		}
	}
		if(!error_flg && final_ret_ != 0){
			
			fprintf(stderr, "# import error occurred, unknow error no, please check the import log\n");
		}
		fprintf(stdout, "###########################################\n");
	}
		
}
//add by pangtz










ObRowBuilder::ObRowBuilder(ObSchemaManagerV2 *schema,
                           const TableParam &table_param
                           ) : table_param_(table_param)
{
  schema_ = schema;
  memset(columns_desc_, 0, sizeof(columns_desc_));
  //memset(rowkey_desc_, 0, sizeof(rowkey_desc_));
  atomic_set(&lineno_, 0);
  rowkey_max_size_ = OB_MAX_ROW_KEY_LENGTH;
  columns_desc_nr_ = 0;
  line_buffer_ = new char[LINE_BUFFER_SIZE];
  //add by pangtz:20141204 
  atomic_set(&bad_lineno_, 0);
  //add e
  
}

ObRowBuilder::~ObRowBuilder()
{
  if (line_buffer_ != NULL)
  {
    delete line_buffer_;
  }
  TBSYS_LOG(INFO, "Processed lines = %d", atomic_read(&lineno_));
 //add by pangtz:20141204
  TBSYS_LOG(INFO, "bad lines = %d", atomic_read(&bad_lineno_));
  //add e
}

int ObRowBuilder::set_column_desc(const std::vector<ColumnDesc> &columns)
{
  int ret = OB_SUCCESS;

  //columns_desc_nr_ = static_cast<int32_t>(columns.size());
  for (size_t i = 0; i < columns.size(); i++) {
    const ObColumnSchemaV2 *col_schema =
      schema_->get_column_schema(table_param_.table_name.c_str(), columns[i].name.c_str());
	  int offset = columns[i].offset;
	  //add by pangtz:20141127
	  if(table_param_.is_append_date_){
		  if((offset - 1) >= table_param_.input_column_nr) {
			  TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s, offset=[%d]", table_param_.table_name.c_str(), columns[i].name.c_str(), offset);
			  ret = OB_ERROR;
			  break;
		  }
	  }else{
			  if(offset >= table_param_.input_column_nr) {
				  TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s, offset=[%d]", table_param_.table_name.c_str(), columns[i].name.c_str(), offset);
				  ret = OB_ERROR;
				  break;
			  }
		    }
#if 0
//delete by pangtz:20141127
    int offset = columns[i].offset;
		if(offset >= table_param_.input_column_nr) {
			 TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s, offset=[%d]", table_param_.table_name.c_str(), columns[i].name.c_str(), offset);
			 ret = OB_ERROR;
			 break;
		}
#endif
    if (col_schema) {
      //ObModifyTimeType, ObCreateTimeType update automaticly, skip
      if (col_schema->get_type() != ObModifyTimeType &&
          col_schema->get_type() != ObCreateTimeType) {
        columns_desc_[columns_desc_nr_].schema = col_schema;
        columns_desc_[columns_desc_nr_].offset = offset;
        columns_desc_nr_++;
      }
    } else {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "column:%s is not a legal column in table %s",
                columns[i].name.c_str(), table_param_.table_name.c_str());
      break;
    }
  }

  //schema_->print_info();
  const ObTableSchema *table_schema = schema_->get_table_schema(table_param_.table_name.c_str());
  if (table_schema != NULL) {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    rowkey_desc_nr_ = rowkey_info.get_size();
    assert(rowkey_desc_nr_ < (int64_t)kMaxRowkeyDesc);

    for(int64_t i = 0;i < rowkey_info.get_size(); i++) {
      ObRowkeyColumn rowkey_column;
      rowkey_info.get_column(i, rowkey_column);

      TBSYS_LOG(INFO, "rowkey_info idx=%ld, column_id = %ld", i, rowkey_column.column_id_);
      int64_t idx = 0;
      for(;idx < columns_desc_nr_; idx++) {
        const ObColumnSchemaV2 *schema = columns_desc_[idx].schema;
        if (NULL == schema) {
          continue;
        }
        if (rowkey_column.column_id_ == schema->get_id()) {
          break;
        }
      }
      if (idx >= columns_desc_nr_) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "row_desc in config file is not correct");
      }
      else {
        rowkey_offset_[i] = idx;
        if (idx == OB_MAX_COLUMN_NUMBER) {
          TBSYS_LOG(ERROR, "%ldth rowkey column is not specified", i);
          ret = OB_ERROR;
          break;
        }
      }
    }
  } else {
    ret = OB_ERROR;
	error_arr[0] = false;//add by pangtz:20141208
    TBSYS_LOG(ERROR, "no such table %s in schema", table_param_.table_name.c_str());
  }

  return ret;
}


/*
int ObRowBuilder::set_rowkey_desc(const std::vector<RowkeyDesc> &rowkeys)
{
  int ret = OB_SUCCESS;

  const ObTableSchema *tab_schema = schema_->get_table_schema(table_param_.table_name.c_str());
  if (tab_schema == NULL) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "no such table in schema");
  } else {
    rowkey_max_size_  = tab_schema->get_rowkey_max_length();
  }

  if (ret == OB_SUCCESS) {
    rowkey_desc_nr_ = rowkeys.size();

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      rowkey_desc_[i].pos = -1;
    }

    for(size_t i = 0;i < rowkeys.size(); i++) {
      int pos = rowkeys[i].pos;
      int offset = rowkeys[i].offset;

      if (offset >= table_param_.input_column_nr) {
        TBSYS_LOG(ERROR, "wrong config, table=%s, offset=%d", table_param_.table_name.c_str(), offset);
        ret = OB_ERROR;
        break;
      }
      rowkey_desc_[pos] = rowkeys[i];
    }

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      if (rowkey_desc_[i].pos == -1) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "rowkey config error, intervals in it, pos=%ld, please check it", i);
        break;
      }
    }
  }

  return ret;
}
*/

bool ObRowBuilder::check_valid()
{
  bool valid = true;

  if (table_param_.input_column_nr <= 0) {
    TBSYS_LOG(ERROR, "input data file has 0 column");
    valid = false;
  }

  return valid;
}


int ObRowBuilder::build_tnx(RecordBlock &block, DbTranscation *tnx, AppendableFile *bad_file) const
{
  int ret = OB_SUCCESS;
  int token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];

  Slice slice;
  block.reset();

//add by pangtz:20141229 在此处计算lineno_准确
  atomic_add(int(block.get_rec_num()), &lineno_);
//add e

  while (block.next_record(slice)) {
    token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
    if (g_gbk_encoding) {
      if (RecordDelima::CHAR_DELIMA == table_param_.delima.delima_type() && table_param_.delima.get_char_delima() < 128u) {
        Tokenizer::tokenize_gbk(slice, static_cast<char>(table_param_.delima.get_char_delima()), token_nr, tokens);
      }
      else {
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(ERROR, "short delima or delima great than 128 is not support in gbk encoding");
        break;
      }
    }
    else {
	//modify by pangtz:20141127
      //Tokenizer::tokenize(slice, table_param_.delima, token_nr, tokens);
      Tokenizer::tokenize(slice, table_param_, token_nr, tokens);
	 //modify by pangtz:20141127
    }

    if (token_nr != table_param_.input_column_nr) {
		//add by pangtz:20141229
		error_arr[2] = false;
		//add e
      TBSYS_LOG(ERROR, "corrupted data files, please check it, input column number=%d, real nr=%d, slice[%.*s]",
        table_param_.input_column_nr, token_nr, static_cast<int>(slice.size()), slice.data());
      ret = OB_ERROR;
      break;
    }
//delete by pangtz:20141229 一旦出现脏数据，在这里统计lineno_就会有问题，故删除
    //atomic_add(1, &lineno_);
//delete:e
    if (g_print_lineno_taggle) {
      g_print_lineno_taggle = false;
      TBSYS_LOG(INFO, "proccessed line no [%d]", atomic_read(&lineno_));
    }

    ObRowkey rowkey;
	//modify by pangtz:20141127 
    //ret = create_rowkey(rowkey, tokens);
    ret = create_rowkey(rowkey, tokens,token_nr);
	//modify:end
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't rowkey, quiting");
    }

    RowMutator mutator;
    if (ret == OB_SUCCESS) {
      ret = tnx->insert_mutator(table_param_.table_name.c_str(), rowkey, &mutator);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't create insert mutator , table=%s", table_param_.table_name.c_str());
      }
    }

    if (table_param_.is_delete && ret == OB_SUCCESS) {
      if (OB_SUCCESS != (ret = mutator.delete_row())) {
        TBSYS_LOG(WARN, "fail to delete row:ret[%d]", ret);
      }
    }
    else {
      if (ret == OB_SUCCESS) {
		//modify by pangtz:20141127 给函数添加token_nr参数
        ret = setup_content(&mutator, tokens, token_nr); 
		//modify:end
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't transcation content, table=%s", table_param_.table_name.c_str());
          for (int i = 0; i < token_nr; i++) {
            TBSYS_LOG(ERROR, "token seq[%d] [%.*s]", i, static_cast<int>(tokens[i].len), tokens[i].token);
          }
        }
      }
    }

    if (OB_INVALID_ARGUMENT == ret) {
      ret = OB_SUCCESS;
      int64_t pos = slice.size() - table_param_.delima.length();
      memcpy(line_buffer_, slice.data(), pos);
      table_param_.rec_delima.append_delima(line_buffer_, pos, LINE_BUFFER_SIZE);
      pos += table_param_.rec_delima.length();
      if(bad_file->Append(line_buffer_, pos) != OB_SUCCESS) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't write to bad_file name = %s", table_param_.bad_file_);
      }
    }
    else if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "failed slice[%.*s]", static_cast<int>(slice.size()), slice.data());
      break;
    }
  }

  return ret;
}
//modify by pangtz:20141127 给函数添加token_nr参数
//int ObRowBuilder::create_rowkey(ObRowkey &rowkey, TokenInfo *tokens) const
int ObRowBuilder::create_rowkey(ObRowkey &rowkey, TokenInfo *tokens, int token_nr) const
//modify:end
{
  int ret = OB_SUCCESS;
  //static __thread ObObj buffer[kMaxRowkeyDesc];
  ObMemBuf *mbuf = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);

  if (mbuf == NULL || mbuf->ensure_space(rowkey_desc_nr_ * sizeof(ObObj))) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "can't create object from TSI, or ObMemBuff can't alloc memory");
  }

  if (ret == OB_SUCCESS) {
    ObObj *buffer = (ObObj *)mbuf->get_buffer();
    for(int64_t i = 0;i < rowkey_desc_nr_; i++) {
      ObObj obj;
      int token_idx = columns_desc_[rowkey_offset_[i]].offset;
      if (table_param_.has_nop_flag) {
        if (table_param_.nop_flag == tokens[token_idx].token[0]) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "There is nop flag in rowkey. rowkey seq[%ld]", i);
          return ret;
        }
      }
	//add by pangtz:20141126 如果需要append参数，则多构造一个主键obj
	if(table_param_.is_append_date_ && (columns_desc_[rowkey_offset_[i]].offset > token_nr - 1)){
		if((ret = make_obobj(columns_desc_[rowkey_offset_[i]], obj, table_param_.appended_date.c_str())) != OB_SUCCESS){
			error_arr[3] = false; //add by pangtz:20141205
			TBSYS_LOG(WARN, "create hds_lupt rowkey failed, pos = %ld", i);
			 break;
		}
		 buffer[i] = obj;
		continue;
	}
	//add:end
      if ((ret = make_obobj(columns_desc_[rowkey_offset_[i]], obj, tokens)) != OB_SUCCESS) {
		error_arr[3] = false; //add by pangtz:20141205
        TBSYS_LOG(WARN, "create rowkey failed, pos = %ld", i);
        break;
      }
      buffer[i] = obj;
    }
    rowkey.assign(buffer, rowkey_desc_nr_);
  }

  return ret;
}

/*
 * add by pangtz:20141126 可以将一个char*串进行make_obj
 */
int ObRowBuilder::make_obobj(const ColumnInfo &column_info, ObObj &obj,  const char *accdate) const
{
  int ret = OB_SUCCESS;
  if (NULL == column_info.schema) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "column_info schema is null");
    return ret;
  }

  int token_idx = column_info.offset;
  int type = column_info.schema->get_type();

  const char *token = accdate; //add by pangtz:20141126
  int token_len = static_cast<int32_t>(strlen(accdate));//add by pangtz:20141126

  bool is_null = false;
  bool is_decimal_to_varchar = false;//add by liyongfeng 20140818 for processing decimal stored into varchar:b
  for(size_t idx = 0; idx < table_param_.decimal_to_varchar_fields_.size(); idx++)
  {
	  if(token_idx == table_param_.decimal_to_varchar_fields_[idx])
	  {
		  is_decimal_to_varchar = true;
		  break;
	  }
  }//add:e
  if (1 == token_len && NULL != token) {
    if (table_param_.null_flag == token[0]) {
      is_null = true;
    }
  }
  //mod by liyongfeng
  //if (((token_len == 0) && (type != ObVarcharType)/*&& !is_rowkey_col*/) || (table_param_.has_null_flag && is_null )) {
    //obj.set_null();
  //}
  if((token_len == 0) && !column_info.schema->is_nullable() && ((type != ObVarcharType) || is_decimal_to_varchar)) {
	//add by pangtz:20141206 添加非空约束错误信息
	error_arr[4] = false;
	TBSYS_LOG(ERROR, "according to schema, the column is not allowed to be set null, column=%s, column offset[%d]", column_info.schema->get_name(), token_idx);
	//add e
	ret = OB_ERROR;
  }
  else if((token_len == 0) && ((type != ObVarcharType) || is_decimal_to_varchar)) {//mod by liyongfeng 20140818 for processing NULL decimal stored into NULL varchar
	obj.set_null();
  }
  //mod:e
  else {
    switch(type) {
     case ObIntType:
       {
	 //mod by liyongfeng
         int64_t lv = 0;
         if (token_len != 0)
         {
	    //lv = atol(token);
	    ret = transform_str_to_int(token, token_len, lv);
	    if(ret == OB_SUCCESS)
	    {
		obj.set_int(lv);
	    }
	    else
	    {
		TBSYS_LOG(ERROR, "failed to transform_str_to_int, actual value[%.*s], column offset[%d]", token_len, token, token_idx);
	    }
         }
       }
       break;
     case ObFloatType:
       if (token_len != 0)
         obj.set_float(strtof(token, NULL));
       else
         obj.set_float(0);
       break;

     case ObDoubleType:
       if (token_len != 0)
         obj.set_double(strtod(token, NULL));
       else
         obj.set_double(0);
       break;
     case ObDateTimeType:
       {
         ObPreciseDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"failed to transform_date_to_time");
         }
         else
         {
           obj.set_datetime(t / 1000 / 1000);
         }
       }
       break;
     case ObVarcharType:
       {
		   if(is_decimal_to_varchar)
		   {
			   ret = is_decimal(token, token_len);
			   if(ret == OB_ERROR) {
				   TBSYS_LOG(ERROR, "decimal field, but numeric string[%.*s] is invalid decimal string", token_len, token);
				   break;
			   }
		   }
         ObString bstring;
         bstring.assign_ptr(const_cast<char *>(token),token_len);
         obj.set_varchar(bstring);

         if (token_len > column_info.schema->get_size())
         {
		//add by pangtz:20141206
		error_arr[7] = false;
		//add e
           TBSYS_LOG(ERROR, "varchar overflow max_len[%ld], actual size[%d], column offset[%d]",
            column_info.schema->get_size(), bstring.length(), token_idx);
	   		ret = OB_ERROR;
         }
       }
       break;
     case ObDecimalType://mod by fanqiushi&liyongfeng 20140818 for new decimal:b
        {
			if (token_len != 0)
	   		{
		   		ObString bstring;
		   		uint32_t p = column_info.schema->get_precision();
		   		uint32_t s = column_info.schema->get_scale();
				ObDecimal od;
				od.from(token, token_len);
				uint32_t data_p = od.get_precision();
				uint32_t data_s = od.get_scale();
				if((data_p - data_s) > (p - s)) {
					TBSYS_LOG(ERROR, "decimal value[%.*s] not satisfies decimal precision[%u, %u]", token_len, token, p, s);
					ret = OB_ERROR;
				}else {
		   			bstring.assign_ptr(const_cast<char *>(token), token_len);
		   			if(OB_SUCCESS != (ret = obj.set_decimal(bstring))){
			   			TBSYS_LOG(ERROR, "failed to covert ObString to Decimal,string=%.*s", bstring.length(),bstring.ptr());
		   			}else{
			   			obj.set_precision(p);
			   			obj.set_scale(s);
		   			}
				}
           	}
		}
		break;//mod:e
     case ObPreciseDateTimeType:
       {
         ObPreciseDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"failed to transform_date_to_time, actual value[%s], column offset[%d]", token, token_idx);
         }
         else
         {
           obj.set_precise_datetime(t);
         }
       }
       break;
     default:
       TBSYS_LOG(ERROR,"unexpect type index: %d", type);
       ret = OB_ERROR;
       break;
    }
  }

  return ret;
}
//add:end












int ObRowBuilder::make_obobj(const ColumnInfo &column_info, ObObj &obj, TokenInfo *tokens) const
{
  int ret = OB_SUCCESS;
  if (NULL == column_info.schema) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "column_info schema is null");
    return ret;
  }

  int token_idx = column_info.offset;
  int type = column_info.schema->get_type();
  const char *token = tokens[token_idx].token;
  int token_len = static_cast<int32_t>(tokens[token_idx].len);
  bool is_null = false;
  bool is_decimal_to_varchar = false;//add by liyongfeng 20140818 for processing decimal stored into varchar:b
  for(size_t idx = 0; idx < table_param_.decimal_to_varchar_fields_.size(); idx++)
  {
	  if(token_idx == table_param_.decimal_to_varchar_fields_[idx])
	  {
		  is_decimal_to_varchar = true;
		  break;
	  }
  }//add:e
  if (1 == token_len && NULL != token) {
    if (table_param_.null_flag == token[0]) {
      is_null = true;
    }
  }
  //mod by liyongfeng
  //if (((token_len == 0) && (type != ObVarcharType)/*&& !is_rowkey_col*/) || (table_param_.has_null_flag && is_null )) {
    //obj.set_null();
  //}
  if((token_len == 0) && !column_info.schema->is_nullable() && ((type != ObVarcharType) || is_decimal_to_varchar)) {
	//add by pangtz:20141206 添加非空约束错误信息
	error_arr[4] = false;
	TBSYS_LOG(ERROR, "according to schema, the column is not allowed to be set null, column=%s, column offset[%d]", column_info.schema->get_name(), token_idx);
	//add e
	
	ret = OB_ERROR;
  }
  else if((token_len == 0) && ((type != ObVarcharType) || is_decimal_to_varchar)) {//mod by liyongfeng 20140818 for processing NULL decimal stored into NULL varchar
	obj.set_null();
  }
  //mod:e
  else {
    switch(type) {
     case ObIntType:
       {
	 //mod by liyongfeng
         int64_t lv = 0;
         if (token_len != 0)
         {
	    //lv = atol(token);
	    ret = transform_str_to_int(token, token_len, lv);
	    if(ret == OB_SUCCESS)
	    {
		obj.set_int(lv);
	    }
	    else
	    {
		TBSYS_LOG(ERROR, "failed to transform_str_to_int, actual value[%.*s], column offset[%d]", token_len, token, token_idx);
	    }
         }
       }
       break;
     case ObFloatType:
       if (token_len != 0)
         obj.set_float(strtof(token, NULL));
       else
         obj.set_float(0);
       break;

     case ObDoubleType:
       if (token_len != 0)
         obj.set_double(strtod(token, NULL));
       else
         obj.set_double(0);
       break;
     case ObDateTimeType:
       {
         ObPreciseDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"failed to transform_date_to_time");
         }
         else
         {
           obj.set_datetime(t / 1000 / 1000);
         }
       }
       break;
     case ObVarcharType:
       {
		     if(is_decimal_to_varchar)
		     {
			     ret = is_decimal(token, token_len);
			     if(ret == OB_ERROR) {
			       TBSYS_LOG(ERROR, "decimal field, but numeric string[%.*s] is invalid decimal string", token_len, token);
			       break;
			     }
		     }
         //add pangtz:20141217 判断并处理substr
         //std::string column_name_str = column_info.schema->get_name();
         //fprintf(stdout,"column_name_str[%s]\n",column_name_str.data());
		if(table_param_.has_substr){
			SubstrInfo substrInfo;
			std::string column_name_str = column_info.schema->get_name();
			std::map<std::string, SubstrInfo>::const_iterator it = table_param_.substr_map.find(column_name_str);
			if(it != table_param_.substr_map.end()){
				//fprintf(stdout,"column_name_str[%s]\n",column_name_str.data());
				substrInfo = it->second;
				if((substrInfo.beg_pos <= token_len) && (substrInfo.beg_pos - 1 + substrInfo.len) <= token_len){
					token_len = substrInfo.len;
				}else if((substrInfo.beg_pos <= token_len) && (substrInfo.beg_pos - 1 + substrInfo.len) > token_len){
					token_len = token_len - substrInfo.beg_pos + 1;
				}else{
					token_len = 0;
				}
				token = token + substrInfo.beg_pos - 1;
			}
		}
	 //fprintf(stdout,"token_len[%d]\n",token_len);
	 //fprintf(stdout,"token_new[%s]\n\n",token);
//add end
         ObString bstring;
         bstring.assign_ptr(const_cast<char *>(token),token_len);
         obj.set_varchar(bstring);
         if (token_len > column_info.schema->get_size())
         {
			 //add by pangtz:20141206
			error_arr[7] = false;
			//add e
           TBSYS_LOG(ERROR, "varchar overflow max_len[%ld], actual size[%d], column offset[%d]",
            column_info.schema->get_size(), bstring.length(), token_idx);
	   		ret = OB_ERROR;
         }
       }
       break;
     case ObDecimalType://mod by fanqiushi&liyongfeng 20140818 for new decimal:b
        {
			if (token_len != 0)
	   		{
		   		ObString bstring;
		   		uint32_t p = column_info.schema->get_precision();
		   		uint32_t s = column_info.schema->get_scale();

			//add by pangtz:20141207 将decimal转为标准格式
			/*
			ObMemBuf *mbuf = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);
			 if (mbuf == NULL || mbuf->ensure_space(sizeof(ObDecimal))) {
			     ret = OB_ERROR;
			     TBSYS_LOG(ERROR, "can't create object from TSI, or ObMemBuff can't alloc memory");
			}
			if (ret == OB_SUCCESS){
				char *buffer = (char *)mbuf->get_buffer();
				ObDecimal deci;
				deci.from(token, token_len);
				deci.modify_value(p, s);
				deci.to_string(buffer, sizeof(buffer));
			*/
			//add e
			
				ObDecimal od;
				//modify by pangtz:20141207
					od.from(token, token_len);
					//od.from(buffer, strlen(buffer));
				//mod e
				uint32_t data_p = od.get_precision();
				uint32_t data_s = od.get_scale();
				if((data_p - data_s) > (p - s)) {
					TBSYS_LOG(ERROR, "decimal value[%.*s] not satisfies decimal precision[%u, %u]", token_len, token, p, s);
					ret = OB_ERROR;
				}else {
		   			bstring.assign_ptr(const_cast<char *>(token), token_len);
		   			if(OB_SUCCESS != (ret = obj.set_decimal(bstring))){
			   			TBSYS_LOG(ERROR, "failed to covert ObString to Decimal,string=%.*s", bstring.length(),bstring.ptr());
		   			}else{
			   			obj.set_precision(p);
			   			obj.set_scale(s);
		   			}
				}
			
           	}
		}
		break;//mod:e
     case ObPreciseDateTimeType:
       {
         ObPreciseDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"failed to transform_date_to_time, actual value[%s], column offset[%d]", token, token_idx);
         }
         else
         {
           obj.set_precise_datetime(t);
         }
       }
       break;
     default:
       TBSYS_LOG(ERROR,"unexpect type index: %d", type);
       ret = OB_ERROR;
       break;
    }
  }

  return ret;
}

//modify by pangtz:20141127 给函数添加token_nr参数
//int ObRowBuilder::setup_content(RowMutator *mutator, TokenInfo *tokens) const
int ObRowBuilder::setup_content(RowMutator *mutator, TokenInfo *tokens, int token_nr) const
{
  int ret = OB_SUCCESS;

  const ObTableSchema *table_schema = schema_->get_table_schema(table_param_.table_name.c_str());
  const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
  for(int i = 0;i < columns_desc_nr_ && ret == OB_SUCCESS; i++) {
    const ObColumnSchemaV2 *schema = columns_desc_[i].schema;
    if (NULL == schema) {
      continue;
    }

    if (rowkey_info.is_rowkey_column(schema->get_id())) {
      continue;
    }

    ObObj obj;

    const char *token = tokens[columns_desc_[i].offset].token;
    int token_len = static_cast<int32_t>(tokens[columns_desc_[i].offset].len);
    bool is_nop = false;

    if (token_len == 1 && NULL != token) {
      if (token[0] == table_param_.nop_flag) {
        is_nop = true;
      }
    }

    if (!table_param_.has_nop_flag || !is_nop) {
	
	//add by pangtz:20141126 append参数
	if(table_param_.is_append_date_ && (columns_desc_[i].offset > token_nr - 1)){
		//error_arr[3] = false; //add by pangtz:20141205
		ret = make_obobj(columns_desc_[i], obj, table_param_.appended_date.c_str());
	}else{
	//add:end
	 // error_arr[3] = false; //add by pangtz:20141205
      ret = make_obobj(columns_desc_[i], obj, tokens);
	}
      if (ret == OB_SUCCESS) {
        ret = mutator->add(schema->get_name(), obj);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't add column to row mutator");
          break;
        } else {
          TBSYS_LOG(DEBUG, "obj idx=%d, name=%s", columns_desc_[i].offset, schema->get_name());
          obj.dump();
        }
      } else {
		  error_arr[3] = false; //add by pangtz:20141219
        TBSYS_LOG(ERROR, "can't make obobj, column=%s", schema->get_name());
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "nop token, offset[%d]", columns_desc_[i].offset);
    }
  }

  return ret;
}

