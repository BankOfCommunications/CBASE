#include <string>
#include <stdio.h> //add by pangtz:20141211
#include <stdlib.h> //add by pangtz:20141211
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include "db_utils.h"
#include "ob_import_v2.h"
using namespace std;

using namespace oceanbase::common;

#if 0
extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;
#endif

//add by pangtz:20141206

ObImportLogInfo::ObImportLogInfo()
{
	import_begin_time_ = 0;
	import_end_time_ = 0;
	during_time_ = 0;
	processed_lineno_ = 0;
	bad_lineno_ = 0;
	succ_lineno_ = 0;
  wait_time_sec_ = 0;
}

void ObImportLogInfo::print_error_log()
{
    //mod by zhuxh:20151113
    //change error output from stderr to stdout
    FILE * _stderr = stdout;
    fprintf(stdout, "\n###########################################\n");
    fprintf(stdout, "# [ IMPORT INFO ]\n");
    fprintf(stdout, "# table name: %s\n",get_table_name().data());
    fprintf(stdout, "# data file: %s\n",get_datafile_name().data());
    fprintf(stdout, "# begin time: %s\n", get_begin_time());
    fprintf(stdout, "# end time: %s\n", get_end_time());
    fprintf(stdout, "# during time(s): %ld\n", get_during_time());
    fprintf(stdout, "# wait for merge done time(s): %ld\n", get_wait_time_sec()); // add by zhangcd 20150814
    fprintf(stdout, "# processed lines: %ld\n", get_processed_lineno()); // add by zhangcd 20150814
    fprintf(stdout, "# bad lines: %ld\n", get_bad_lineno());
    std::string succ_str;
    if(processed_lineno_ == 0){
      succ_str="0";
    // 	fprintf(stdout, "# successed lines: 0\n");
    }else if(bad_lineno_ > processed_lineno_){
    succ_str="unknown"; //modify by zhuxh:20150720
    //	 fprintf(stdout, "# successed line: unknow\n");
    }else{
       sprintf(const_cast<char *>(succ_str.c_str()),"%ld",get_succ_lineno());
    //	 fprintf(stdout, "# successed: %ld\n", get_succ_lineno());
    }
    //mod by zhuxh:20160313 :b
    fprintf(stdout, "# good lines: %s\n", succ_str.data());
    fprintf(stdout, "# velocity(row/s): %.2lf\n", static_cast<double>(get_processed_lineno()) / static_cast<double>(get_during_time()-get_wait_time_sec()));
    //mod :e
    fprintf(stdout, "###########################################\n\n");

    //add by zhuxh:20151201 :b
    int error_no = 0;
    for(;error_no<IMPORT_ERROR_TYPE_NUM; error_no++)
      if( !error_arr[error_no] )
        final_ret_ = OB_ERROR;
    //add :e

    if(final_ret_ != 0){
    fprintf(_stderr, "###########################################\n");
    fprintf(_stderr, "# [ ERROR MESSAGE ]\n");
    //modify by zhuxh:20150720 b
    fprintf(stdout, "# error table name: %s\n",get_table_name().data());
    if(!error_arr[CONF_FILE_ERROR]){
      error_arr[TABLE_CONF_NOT_EXIST] = true;
    }
    //int error_no = TABLE_NOT_EXIST;
    bool error_flg = false;
    for(error_no=0;error_no < IMPORT_ERROR_TYPE_NUM;error_no++){
      if(error_arr[error_no]) continue;
      error_flg = true;
      switch(error_no){
      case TABLE_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: the table doesn't exist in db\n", error_no);
        break;
      case DATAFLIE_COLUMN_NUM_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: the column number in data file is not equal to that in table schema\n", error_no);
        break;
      case TABLE_CONF_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: the config file doesn't exist\n", error_no);
        break;
      case DATAFLIE_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: the data file doesn't exist\n", error_no);
        break;
      case DATA_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: there are dirty records in data file，please look for details in import log\n", error_no);
        break;
      case SYSTEM_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: import error occurred, possibly due to oceanbase system exceptions，please look for details in import log\n", error_no);
        break;
      case CONF_FILE_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: the config file info does not accord with that in table schema, or config file info is not correct, please look for details in import log\n", error_no);
        break;
      case NOT_NULL_CONSTRAIN_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: the column data is not satisfied with not-null constrain\n", error_no);
        break;
      case VARCHAR_OVER_FLOW:
        fprintf(_stderr, "# ERROR:%d  message: the length varchar column overflow, please look for details in import log\n", error_no);
        break;
      case MEMORY_SHORTAGE:
        fprintf(_stderr, "# ERROR:%d  message: there is no enough memory to store schema\n", error_no);
        break;
      case RS_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: root server has swithced or no root server, please re-run ob_import\n", error_no);
        break;
      case CREATE_BAD_FILE_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: CREATE_BAD_FILE_ERROR\n", error_no);
        break;
      case PARSE_TABLE_TITLE_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: PARSE_TABLE_TITLE_ERROR\n", error_no);
        break;
      case DATE_FORMAT_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: DATE_FORMAT_ERROR\n", error_no);
        break;
      case PRODUCE_AND_COMSUME_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: PRODUCE_AND_COMSUME_ERROR\n", error_no);
        break;
      case PARAM_APPEND_ACCDATE_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: PARAM_APPEND_ACCDATE_ERROR\n", error_no);
        break;
      case COLUMN_DESC_SET_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: COLUMN_DESC_SET_ERROR\n", error_no);
        break;
      case ROWKEY_SET_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: ROWKEY_SET_ERROR\n", error_no);
        break;
      case GET_TABLE_PARAM_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: GET_TABLE_PARAM_ERROR\n", error_no);
        break;
      case ALLOCATE_MEMORY_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: ALLOCATE_MEMORY_ERROR\n", error_no);
        break;
      case MS_MANAGER_INIT_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: MS_MANAGER_INIT_ERROR\n", error_no);
        break;
      case BAD_FILE_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: BAD_FILE_ERROR\n", error_no);
        break;
      case MEMORY_OVERFLOW:
        fprintf(_stderr, "# ERROR:%d  message: MEMORY_OVERFLOW\n", error_no);
        break;
      case SQL_BUILDING_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: SQL_BUILDING_ERROR\n", error_no);
        break;
      case GET_ONE_MS_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: GET_ONE_MS_ERROR\n", error_no);
        break;
      case MS_REF_COUNT_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: MS_REF_COUNT_ERROR\n", error_no);
        break;
      case FETCH_MS_LIST_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: FETCH_MS_LIST_ERROR\n", error_no);
        break;
      case SUBSTR_GRAMMAR_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: SUBSTR_GRAMMAR_ERROR\n", error_no);
        break;
      case SUBSTR_COLUMN_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: SUBSTR_COLUMN_NOT_EXIST\n", error_no);
        break;
      case DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST\n", error_no);
        break;
      case MAPPING_COLUMN_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: VALID_COLUMN_NOT_EXIST\n", error_no);
        break;
      case COLUMN_WITH_CHAR_DELIMA_NOT_EXIST:
        fprintf(_stderr, "# ERROR:%d  message: COLUMN_WITH_CHAR_DELIMA_NOT_EXIST\n", error_no);
        break;
      case INCOMPLETE_DATA:
        fprintf(_stderr, "# ERROR:%d  message: Incomplete data\nThe corrupted data isn't written into the bad file, and its first 30 characters can be found in log.\n", error_no); //mod by zhuxh:20160313
        break;
      case G2U_ERROR:
        fprintf(_stderr, "# ERROR:%d  message: Transformation from GBK to UTF-8 error\n", error_no);
        break;
      case BUFFER_OVER_FLOW:
        fprintf(_stderr, "# ERROR:%d  message: %dKB buffer over flows. You may try the paramter --buffersize xxx for a bigger buffer with xxx KB.\n", error_no, kReadBufferSize>>10);
        break;
      default:
        break;
      }
    }
    //modify e
        if(!error_flg && final_ret_ != 0){
			
            fprintf(stderr, "# Unknown error, please check the log.\n");
		}
        fprintf(_stderr, "###########################################\n");
	}
		
}
//add by pangtz

ObRowBuilder::ObRowBuilder(ObSchemaManagerV2 *schema,
                           TableParam &table_param //TODO //temporarily delete const
                           ) : table_param_(table_param)
{
  schema_ = schema;
  //memset(columns_desc_, 0, sizeof(columns_desc_)); //del by zhuxh:20150831 //can't init like this
  atomic_set(&lineno_, 0);
  rowkey_max_size_ = OB_MAX_ROW_KEY_LENGTH;
  columns_desc_nr_ = 0;
  //add by pangtz:20141204 
  atomic_set(&bad_lineno_, 0);
  //add e
}

ObRowBuilder::~ObRowBuilder()
{
  TBSYS_LOG(INFO, "Processed lines = %d", atomic_read(&lineno_));
 //add by pangtz:20141204
  TBSYS_LOG(INFO, "bad lines = %d", atomic_read(&bad_lineno_));
  //add e
}

//delete by zhuxh:20150724
//column_desc will only rely on table columns' name
#if 0
int ObRowBuilder::set_column_desc(const std::vector<ColumnDesc> &columns)
{
  int ret = OB_SUCCESS;

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

/*
//delete by pangtz:20141127
    int offset = columns[i].offset;
		if(offset >= table_param_.input_column_nr) {
			 TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s, offset=[%d]", table_param_.table_name.c_str(), columns[i].name.c_str(), offset);
			 ret = OB_ERROR;
			 break;
        }
*/

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
    error_arr[TABLE_NOT_EXIST] = false;//add by pangtz:20141208 //modify by zhuxh:20150720
    TBSYS_LOG(ERROR, "no such table %s in schema", table_param_.table_name.c_str());
  }

  return ret;
}
#endif

bool str_eq_schema( const std::string & str, const ObColumnSchemaV2 & sch )
{
  return str==sch.get_name();
}

bool str_eq_column_info( const std::string & str, const ColumnInfo & ci )
{
  return str==ci.schema->get_name();
}

//add by zhuxh:20150731
//decimal_to_varchar_grammar => set<int> decimal_to_varchar_set
#if 0
int ObRowBuilder::parse_decimal_2_varchar_grammar()
{
  int ret = OB_SUCCESS;
  std::vector<string> vs;
  const static char COMMA = ',';
  Tokenizer::tokenize(table_param_.decimal_to_varchar_grammar,vs,COMMA);
  std::vector<string>::iterator it = vs.begin();
  for( ; it != vs.end(); it++ )
  {
    int i=0;
    for(; i<columns_desc_nr_ && *it != columns_desc_[i].schema->get_name(); i++ );
    if( i>=columns_desc_nr_ )
    {
      ret = OB_ERROR;
      error_arr[DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST] = false;
      TBSYS_LOG(ERROR, "Substring error! Fail to find column[%s].",it->c_str());
      break;
    }
    else
      table_param_.decimal_to_varchar_set.insert(i);
  }
  return ret;
}
#endif
//add by zhuxh:20150811 :b
//"col5,col3" => set<int> {3,5}
int ObRowBuilder::parse_decimal_2_varchar_grammar()
{
  vector<int> vi;
  const static char COMMA = ',';
  int ret = strs_2_idx_vector(table_param_.decimal_to_varchar_grammar, COMMA, columns_desc_, columns_desc_nr_, str_eq_column_info, vi );
  if( OB_SUCCESS != ret )
  {
    error_arr[DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST] = false;
    TBSYS_LOG(ERROR, "DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST!");
  }
  else
  {
    std::vector<int>::iterator it = vi.begin();
    for( ; it!=vi.end(); it++ )
      table_param_.decimal_to_varchar_set.insert(*it);
  }
  return ret;
}

//about columns_desc_[]
//columns_desc_[] is the map between column of table schema and datafile
//EX.
//datafile column 0   1   2
//table column    c0  c1  c2  c3
//If we want 1=>c2,2=>c3
//Namely we wish a sql "INSERT INTO mytable (c2,c3) VALUES(data[1],data[2])"
//We just make columns_desc_ like
/*
+-------+--+--+
|schema |c2|c3|
+-------+--+--+
|offset |1 |2 |
+-------+--+--+
|default|  |  |
+-------+--+--+
 */
//mod by zhuxh:20160603 [Sequence]
int ObRowBuilder::fill_columns_desc_( const ObColumnSchemaV2 * column_schema, int column_schema_nr, std::vector<SequenceInfo> & vseq )
{
  int ret = OB_SUCCESS;
  int cdi = 0; //columns_desc_i
  int left_nr = column_schema_nr;
  bool selected_table_column_id[OB_MAX_COLUMN_NUMBER] = {0};
  bool selected_datafile_column_id[OB_MAX_COLUMN_NUMBER] = {0};
  if( table_param_.has_column_map )
  {
    vector<ColumnMapInfo> v;
    ret = parse_column_map_grammar(table_param_.column_map_grammar,columns_desc_nr_,v,table_param_.substr_map,vseq); //mod by zhuxh:20160603 [Sequence]
    if( OB_SUCCESS == ret && columns_desc_nr_ > column_schema_nr )
    {
      ret = OB_ERROR;
    }
    else
      left_nr = columns_desc_nr_;
    if( OB_SUCCESS == ret )
    {
      //i is index of vector v
      //j is index of schema of v[i]
      //Namely v[i]~schema[j]
      int vl= static_cast<int>(v.size());
      int i=0;
      for(; i<vl; i++)
      {
        int j=0;
        //mod by zhuxh:20160101
        //ignore case
        for(; j<column_schema_nr && strcasecmp(v[i].column_name.c_str(),column_schema[j].get_name())!=0; j++);
        if( j >= column_schema_nr )
        {
          ret = OB_ERROR;
          error_arr[MAPPING_COLUMN_NOT_EXIST] = false;
          TBSYS_LOG(ERROR, "Column \"%s\" doesn't exist in column mapping!", v[i].column_name.c_str());
        }
        else
        {
          std::string dft(v[i].default_token);
          int len = static_cast<int>(dft.length());
          bool var = type_has_quotation_marks(column_schema[j].get_type());
          if( ! dft.empty() )
          {
            bool la = ( dft[0]=='\'' ); //with left apostrophe
            bool ra = ( dft[len-1]=='\'' ); //with right apostrophe
            //If a varchar/timestamp/... has no apostrophe or another has apostrophes,
            //it'll be wrong
            if
            (
                (var && (!la ||!ra ))
                ||
                (!var && (la || ra) )
            )
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "Default token format error in column mapping!\n(Do you forget single quotation marks '' for a varchar default token?)");
            }
          }
          //mod by zhuxh:20160603 [Sequence] :b
          if( OB_SUCCESS == ret && v[i].datafile_column_id == ColumnMapInfo::SEQUENCE && ! type_supports_sequence(column_schema[j].get_type()) )
          {
            TBSYS_LOG(ERROR, "Type id of Column \"%s\", %d, doesn't support sequence.", v[i].column_name.c_str(), column_schema[j].get_type());
            ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
          }
          if( OB_SUCCESS == ret )
          {
            bool column_id_defined = (v[i].datafile_column_id > ColumnMapInfo::UNDEFINED);
            columns_desc_[i].schema = column_schema+j;
            columns_desc_[i].offset = v[i].datafile_column_id;
            if( v[i].datafile_column_id == ColumnMapInfo::SEQUENCE )
            {
              char seq_prefix [IMPORT_SEQUENCE_MAX_LENGTH] = {0};
              //mod by zhuxh:20160907 [cb sequence] :b
              //char number [IMPORT_SEQUENCE_MAX_LENGTH] = {0};
              get_sequence_prefix( table_param_.db_name_, table_param_.table_name.c_str(), seq_prefix);
              //sprintf(number, "%d", v[i].seq_index);
              columns_desc_[i].default_token = "nextval for \""; //mod by zhuxh:20161018 [sequence case bug]
              columns_desc_[i].default_token += seq_prefix;
              //columns_desc_[i].default_token += number;
              columns_desc_[i].default_token += v[i].column_name;
              columns_desc_[i].default_token += "\""; //mod by zhuxh:20161018 [sequence case bug]
              //mod by zhuxh:20160907 [cb sequence] :e
            }
            else if( ! dft.empty() )
            {
          //mod :e
              if(var)
                dft=dft.substr(1,len-2);//delete single quotation
              columns_desc_[i].default_token = dft;
            }
            selected_table_column_id[j] = true;
            if( column_id_defined )
              selected_datafile_column_id[v[i].datafile_column_id] = true;
          }
        }
      }//for
      left_nr -= vl;
      cdi = vl;
    }
  }
  else
    columns_desc_nr_ = column_schema_nr;
  //Orderly and over selected table column and datafile id,
  //column schemas => columns_desc_
  int i=0;
  int j=0;
  for(; left_nr>0; i++,j++,cdi++,left_nr--)
  {
    while(selected_table_column_id[i])i++;
    while(selected_datafile_column_id[j])j++;
    columns_desc_[cdi].schema = column_schema+i;
    columns_desc_[cdi].offset = j;
  }
  return ret;
}

//"col6,col4" => set<int> {4,6}
int ObRowBuilder::fill_columns_with_char_delima_set()
{
  if( table_param_.all_columns_have_char_delima )
    return OB_SUCCESS;
  vector<int> vi;
  const static char COMMA = ',';
  int ret = strs_2_idx_vector(table_param_.columns_with_char_delima_grammar, COMMA, columns_desc_, columns_desc_nr_, str_eq_column_info, vi );
  if( OB_SUCCESS != ret )
  {
    error_arr[COLUMN_WITH_CHAR_DELIMA_NOT_EXIST] = false;
    TBSYS_LOG(ERROR, "A column with char delima doen't exist!");
  }
  else
  {
    std::vector<int>::iterator it = vi.begin();
    for( ; it!=vi.end(); it++ )
      table_param_.columns_with_char_delima_set.insert(*it);
  }
  return ret;
}

//if the column has char delima
bool ObRowBuilder::has_char_delima(int offset) const
{
  const std::set<int> & st = table_param_.columns_with_char_delima_set;
  return st.find(offset) != st.end();
}
//add :e

//if the varchar column represents decimal
bool ObRowBuilder::is_decimal_to_varchar(int offset) const
{
  const std::set<int> & st = table_param_.decimal_to_varchar_set;
  return st.find(offset) != st.end();
}

#if 0
//add by zhuxh:20150803
//Generally, offset of column[i] is i.
//But offset of a substr is same to its source.
//EX.
//--substr sub,src,4,5
//columns_desc_[src]=100;
//so
//columns_desc_[sub]=100;
int ObRowBuilder::adjust_substr_offset()
{
  int ret = OB_SUCCESS;
  std::map<int, SubstrInfo>::const_iterator it = table_param_.substr_map.begin();
  for(; it != table_param_.substr_map.end(); it++)
  {
    int i=0;
    for(; i<columns_desc_nr_ && (it->second).column_name != columns_desc_[i].schema->get_name(); i++ );
    if( i>=columns_desc_nr_ )
    {
      ret = OB_ERROR;
      error_arr[SUBSTR_COLUMN_NOT_EXIST] = false;
      TBSYS_LOG(ERROR, "Substring error! Fail to find column[%s].",(it->second).column_name.c_str());
      break;
    }
  }
  return ret;
}
#endif

//add by zhuxh:20150731
//mod by zhuxh:20160603 [Sequence]
int ObRowBuilder::set_column_desc( std::vector<SequenceInfo> & vseq )
{
  int ret = OB_SUCCESS;

  //table_param => table_schema
  // add by zhangcd 20150814:b
  ObString complete_table_name;
  char *buffer  = (char *)allocator_.alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
  complete_table_name.assign_buffer(buffer, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
  if(table_param_.db_name_ != NULL && strlen(table_param_.db_name_) > 0)
  {
    complete_table_name.write(table_param_.db_name_, (int)strlen(table_param_.db_name_));
    complete_table_name.write(".", 1);
  }
  complete_table_name.write(table_param_.table_name.c_str(), (int)table_param_.table_name.length());
  const ObTableSchema *table_schema = schema_->get_table_schema(complete_table_name);
  //add:e
  if (table_schema == NULL)
  {
    ret = OB_ERROR;
    error_arr[TABLE_NOT_EXIST] = false;//add by pangtz:20141208 //modify by zhuxh:20150720
    TBSYS_LOG(ERROR, "no such table %.*s in schema", complete_table_name.length(), complete_table_name.ptr());
  }
  else
  {
    //table_schema => column_schema
    int column_schema_nr = 0;
    const ObColumnSchemaV2 * column_schema = schema_->get_table_schema(table_schema->get_table_id(),column_schema_nr);
    //table_param_.input_column_nr = columns_desc_nr_; //read-only
    if (column_schema == NULL)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "Fail to get column schema!");
    }
    else
    {
      ret = fill_columns_desc_(column_schema, column_schema_nr, vseq);  //mod by zhuxh:20160603 [Sequence]
    }
    if( OB_SUCCESS == ret && table_param_.has_decimal_to_varchar )
      ret = parse_decimal_2_varchar_grammar();
    //if( OB_SUCCESS == ret && table_param_.has_substr)
    //  ret = adjust_substr_offset();
    if( OB_SUCCESS == ret && table_param_.has_char_delima )
      ret = fill_columns_with_char_delima_set();
    if( OB_SUCCESS == ret )
    {

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
          TBSYS_LOG(ERROR, "row_desc error\n(Do datas cover all row keys?)"); //modify by zhuxh:20150805
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
    }
  }

  return ret;
}

bool ObRowBuilder::check_valid()
{
  bool valid = true;

//delete by zhuxh:20150727
#if 0
  if (table_param_.input_column_nr <= 0) {
    TBSYS_LOG(ERROR, "input data file has 0 column");
    valid = false;
  }
#endif

  return valid;
}

bool has_char_delima(const char * str, int len, char delima)
{
  return str[0]==delima && str[len-1]==delima;
}

//mod by zhuxh:20151125
//add by zhuxh:20160603 [Sequence]
bool is_token_null( const TokenInfo & token_info, const ColumnInfo & columns_desc, bool has_null_flag, char null_flag, bool varchar_not_null )
{
  //mod by zhuxh:20160603 [Sequence] :b
  if( ( columns_desc.schema->get_type() == ObVarcharType && varchar_not_null )
      ||
      columns_desc.offset == ColumnInfo::SEQUENCE
  )
  //mod :e
    return false;
  //add by zhuxh:20151216 :b
  //If all characters are spaces
  int i=0;
  for(;i<token_info.len && token_info.token[i]==' ';i++);
  if( i >= token_info.len )
    return true;
  //add :e
  return
  token_info.len == 0
  ||
  (token_info.len == 4 && strncmp(token_info.token,"NULL",4)==0)
  ||
  (token_info.len == 4 && strncmp(token_info.token,"null",4)==0)
  ||
  (
    has_null_flag
    &&
    token_info.len == 1
    &&
    token_info.token[0]==null_flag
  );
}

int ObRowBuilder::check_format(const TokenInfo *tokens,
                                     const int token_nr,
                                     const ColumnInfo *columns_desc,
                                     const int columns_desc_nr, char token_buf[]) const //mod by zhuxh:20170411 [performance optimization]
{
  //UNUSED(token_nr); //del by zhuxh:20151104
  int ret = OB_SUCCESS;

  //mod by zhuxh:20160105
  //Without a column map,
  //Column number of datafile must be equal to that of table.
  if( !table_param_.has_column_map && token_nr != columns_desc_nr_ )
  {
    TBSYS_LOG(ERROR, "The column number of datafile is %d while that of table is %d", token_nr, columns_desc_nr);
    ret = OB_ERROR;
  }

  for(int i=0; i<columns_desc_nr; i++)
    if( columns_desc[i].offset != ColumnInfo::DEFAULT_TOKEN_COLUMN && columns_desc[i].offset >= token_nr )//mod by zhuxh:20160307
    {
      //TODO
      error_arr[DATAFLIE_COLUMN_NUM_ERROR] = false;
      TBSYS_LOG(ERROR,"The column number of datafile is only %d but there is a pair [table column %s,datafile column %d] in column map.", token_nr, columns_desc[i].schema->get_name(),columns_desc[i].offset+1);
      ret = OB_ERROR;
    }

  if(OB_SUCCESS == ret)
  {
    for(int i = 0; i < columns_desc_nr && OB_SUCCESS == ret; i++)
    {
      //del by zhuxh:20160307
      #if 0
      //modify by zhuxh:20150827
      /*const */TokenInfo token_info = tokens[columns_desc[i].offset];//delete by zhuxh:20150827
      #endif
      //char token_buf[MAX_TOKEN_LENGTH]={0}; //del by zhuxh:20170411 [performance optimization]
      bool column_uses_default_token = ( columns_desc[i].offset == ColumnInfo::DEFAULT_TOKEN_COLUMN );
      bool column_has_default_token = !columns_desc[i].default_token.empty();

      //add by zhuxh:20160307 [consider the situation of default column value] :b
      TokenInfo token_info;
      if( column_uses_default_token )
      {
        token_info.token = columns_desc[i].default_token.c_str();
        token_info.len = columns_desc[i].default_token.length();
      }
      else
      {
        token_info = tokens[columns_desc[i].offset];
      }
      //add :e

      if( token_info.len > MAX_TOKEN_LENGTH )
      {
        ret = OB_ERROR;
        //mod by zhuxh:20160313 :b
        TBSYS_LOG(ERROR, "Token overflows. MAX_TOKEN_LENGTH is %d but Length of token [%.*s](1st 100 characters) is %d." ,MAX_TOKEN_LENGTH, static_cast<int>(min(token_info.len,100)),token_info.token, static_cast<int>(token_info.len));
        //mod :e
        break;
      }

      //add by zhuxh:20151023 :b
      //In case of unexpected \r from windows text files
      if( token_info.len>0 && token_info.token[token_info.len-1]=='\r' )
        token_info.len--;
      //add :e

      //mod by zhuxh:20161128 :b
      //This part should be here 
      bool token_is_null = is_token_null(token_info,columns_desc[i] ,table_param_.has_null_flag,table_param_.null_flag,table_param_.varchar_not_null);  //mod by zhuxh:20160603 [Sequence]
      if( ! columns_desc[i].schema->is_nullable() && token_is_null )
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR,"Column %s can't be null!",columns_desc[i].schema->get_name());
        break;
      }
      //mod by zhuxh:20161128 :e

      //add by zhuxh:20150323 :b
      if( token_info.len>=2 && token_info.token[0]=='\"' && token_info.token[token_info.len-1]==token_info.token[0] )
      {
        token_info.token++;
        token_info.len-=2;
      }
      //add :e

      //mod by zhuxh:20150831

      //del by zhuxh:20161128 :b
      //This part has been moved upword.
      /*
      bool token_is_null = is_token_null(token_info,columns_desc[i] ,table_param_.has_null_flag,table_param_.null_flag,table_param_.varchar_not_null);  //mod by zhuxh:20160603 [Sequence]
      if( ! columns_desc[i].schema->is_nullable() && token_is_null )
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR,"Column %s can't be null!",columns_desc[i].schema->get_name());
        break;
      }
      */
      //del by zhuxh:20161128 :e

      if( column_has_default_token )
      {
        if( column_uses_default_token || token_is_null )
        {
          strcpy(token_buf,columns_desc[i].default_token.c_str());
          token_info.token = token_buf;
          token_info.len = columns_desc[i].default_token.length();
        }
      }
      else
      {
        if( token_is_null )continue;
      }

      switch((int)columns_desc[i].schema->get_type())
      {
        case ObIntType:
        case ObInt32Type://add by zhuxh: 20151023
        {
          int64_t value = 0;
          if( columns_desc[i].offset != ColumnInfo::SEQUENCE
              && OB_SUCCESS != transform_str_to_int(token_info.token, (int)token_info.len, value))  //mod by zhuxh:20160603 [Sequence]
          {
            ret = OB_ERROR;
          }
          break;
        }
        case ObFloatType:
        {
          char tmp_buf[token_info.len + 1];
          char *end_ptr = NULL;
          strncpy(tmp_buf, token_info.token, token_info.len);
          float value = strtof(tmp_buf, &end_ptr);
          if(value == 0 && end_ptr == tmp_buf)
          {
            ret = OB_ERROR;
          }
          break;
        }
        case ObDoubleType:
        {
          char tmp_buf[token_info.len + 1];
          char *end_ptr = NULL;
          strncpy(tmp_buf, token_info.token, token_info.len);
          double value = strtod(tmp_buf, &end_ptr);
          if(value == 0 && end_ptr == tmp_buf)
          {
            ret = OB_ERROR;
          }
          break;
        }
#if 0
        {
          char timestamp[256] = {0};
          int ret_val = transform_date_to_time(token_info.token, (int)token_info.len, timestamp);
          if (OB_SUCCESS != ret_val)
          {
            ret = false;
          }
          break;
        }
#endif
        case ObVarcharType:
        {
          //add by zhuxh:20150811 :b
          //If the column is thought to have char delima, it must have 2.
          if( table_param_.all_columns_have_char_delima || has_char_delima(columns_desc[i].offset) )
            if( ! ::has_char_delima(token_info.token, (int)token_info.len, table_param_.char_delima) )
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR,"%.*s doesn't have char delimas!",static_cast<int>(token_info.len),token_info.token);
            }
          //add:e

          //add by zhuxh:20150804 :b
          if( is_decimal_to_varchar(i) && OB_SUCCESS != is_decimal(token_info.token,static_cast<int>(token_info.len)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR,"%.*s isn't a decimal!",static_cast<int>(token_info.len),token_info.token);
          }
          break;
          //add:e
        }
        case ObDecimalType:
        {
          char tmp_buf[token_info.len + 1];
          char *end_ptr = NULL;
          strncpy(tmp_buf, token_info.token, token_info.len);
          double value = strtod(tmp_buf, &end_ptr);
          if(value == 0 && end_ptr == tmp_buf)
          {
            ret = OB_ERROR;
          }
          break;
        }
        case ObDateType:
        case ObTimeType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        {
        char timestamp[100]="";
        int ret_val = transform_date_to_time(columns_desc[i].schema->get_type(), token_info.token, (int)token_info.len, timestamp);
        if (OB_SUCCESS != ret_val)
        {
          ret = OB_ERROR;
        }
        break;
      }
      default:
        TBSYS_LOG(ERROR,"unexpect type index: %d", columns_desc[i].schema->get_type());
        ret = OB_ERROR;
        break;
      }
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "check format error! idx[%d], type[%s] content[%.*s]", i, ob_obj_type_str(columns_desc[i].schema->get_type()), (int)tokens[i].len, tokens[i].token);
      }
    }
  }

  return ret;
}

//mod by zhuxh:20160603 [Sequence]
int ObRowBuilder::tokens_format(const TokenInfo *tokens_in,
                                const int tokens_in_nr,
                                char* tokens_buffer,
                                int64_t tokens_buffer_size,
                                TokenInfo *tokens_out,
                                int &tokens_out_nr,
                                const TableParam& table_param,
                                const ColumnInfo *columns_desc,
                                int columns_desc_nr,
                                char token_buf[]) const //mod by zhuxh:20170411 [performance optimization]
{
  UNUSED(tokens_in_nr);
  int ret = OB_SUCCESS;
  char *pos = tokens_buffer;
  tokens_out_nr = columns_desc_nr; //add by zhuxh:20150831

  for(int i = 0; i < columns_desc_nr && OB_SUCCESS == ret; i++)
  {
    //add by zhuxh:20160307 [consider the situation of default column value] :b
    //char token_buf[MAX_TOKEN_LENGTH]={0}; //del by zhuxh:20170411 [performance optimization]
    bool column_uses_default_token = ( columns_desc[i].offset == ColumnInfo::DEFAULT_TOKEN_COLUMN );
    bool column_has_default_token = !columns_desc[i].default_token.empty();
    bool column_uses_sequence = ( columns_desc[i].offset == ColumnInfo::SEQUENCE ); //mod by zhuxh:20160603 [Sequence]
    bool between_quotation_marks = false; //the cell is contained by " //add by zhuxh:20160912 [quotation mark]

    //mod by zhuxh:20150916 :b
    //int offset_in = columns_desc[i].offset;
    int offset = i; //offset_out
    //add :e

    TokenInfo token_info_obj;
    if( column_uses_default_token )
    {
      token_info_obj.token = columns_desc[i].default_token.c_str();
      token_info_obj.len = columns_desc[i].default_token.length();
    }
    else
    {
      token_info_obj = tokens_in[columns_desc[i].offset];
    }
    //add by zhuxh:20160307 :e

    TokenInfo * token_info = & token_info_obj;
    const ColumnInfo *column_info = columns_desc + i;

    //mod by zhuxh:20160307 [moved into the following situation of no default]
#if 0
    //add by zhuxh:20151023 :b
    //In case of unexpected \r from windows text files
    if( token_info->len>0 && token_info->token[token_info->len-1]=='\r' )
      token_info->len--;
    //add :e
#endif

    if(column_info->offset == columns_desc_nr-1) //modify by zhuxh:20150811
    {
      if(table_param.is_append_date_)
      {
        memcpy(pos, table_param.appended_date.c_str(), table_param.appended_date.length());
        tokens_out[offset].token = pos;
        tokens_out[offset].len = table_param.appended_date.length();
        pos += table_param.appended_date.length();
        tokens_out_nr++;
        continue;
      }
      //delete by zhuxh:20150811
      //Column number has been checked in check_format()
#if 0
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "config file may not be correct!");
        break;
      }
#endif
    }

    //mod by zhuxh:20160307 [moved to top]
#if 0
    char token_buf[MAX_TOKEN_LENGTH]={0};
    bool column_uses_default_token = ( columns_desc[i].offset == ColumnInfo::DEFAULT_TOKEN_COLUMN );
    bool column_has_default_token = !columns_desc[i].default_token.empty();
#endif
    bool token_is_null = is_token_null(*token_info,columns_desc[i],table_param_.has_null_flag,table_param_.null_flag,table_param_.varchar_not_null); //mod by zhuxh:20160603 [Sequence]
    if( column_has_default_token )
    {
      if( column_uses_default_token || column_uses_sequence || token_is_null ) //mod by zhuxh:20160603 [Sequence]
      {
        strcpy(token_buf,columns_desc[i].default_token.c_str());
        token_info->token = token_buf;
        token_info->len = columns_desc[i].default_token.length();
      }
    }
    else
    {
      //add by zhuxh:20160307 :b
      //In case of unexpected \r from windows text files
      if( token_info->len>0 && token_info->token[token_info->len-1]=='\r' )
        token_info->len--;
      //add :e

      //add by zhuxh:20150323 :b
      if( token_info->len>=2 && token_info->token[0]=='\"' && token_info->token[token_info->len-1]==token_info->token[0] )
      {
        token_info->token++;
        token_info->len-=2;
        between_quotation_marks = true; //add by zhuxh:20160912 [quotation mark]
      }
      //add :e

      //add by zhuxh:20161018 [--trim] :b
      //Module of trim is moved here and able to remove blanks between quotes.
      //E.G. "    Hello    " => "HELLO"
      if( table_param.trim_flag >= 0 )
      {
        const char * blanks=" \t\n\r";
        const char * & p = token_info->token;
        int64_t & l = token_info->len;
        if( table_param.trim_flag == 0 || table_param.trim_flag == 1 )
        {
          while(l > 0){
            if( strchr(blanks,*p) ){ // if *p is a kind of blank char
              p++;
              l--;
            }else{
              break;
            }
          }
        }
        if( table_param.trim_flag == 0 || table_param.trim_flag == 2 )
        {
          while(l > 0){
            if( strchr(blanks,*(p + l - 1)) ){
              l--;
            }else{
              break;
            }
          }
        }
      }
      //add by zhuxh:20161018 [--trim] :e

//del by zhuxh:20161018 [varchar not null]
//It has been moved into switch varchar case.
#if 0
      //mod by zhuxh:20151126
      //when --varchar-not-null
      //Empty is ""
      //NULL is "NULL"
      if( column_info->schema->get_type()==ObVarcharType && table_param_.varchar_not_null )
      {
        if( token_info->len==0 ||
          ( token_info->len==4 && ( strncmp(token_info->token,"NULL",4)==0 || strncmp(token_info->token,"null",4)==0 ) )
        )
        {
          pos[0]='\"';
          int i=0;
          for(;i<token_info->len;i++)
            pos[i+1]=token_info->token[i];
          pos[i+1]='\"';
          tokens_out[offset].token = pos;
          pos += (tokens_out[offset].len = token_info->len+2);
          continue;
        }
      }
      else if( token_is_null )
#endif
      if( token_is_null )
      {
        //mod by zhuxh: 20151216
        //memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = 0;//token_info->len;
        //pos += token_info->len;
        continue;
      }
    }

    //add by zhuxh:20150731 :b
    //about null flag
    //add:e
    switch((int)column_info->schema->get_type())
    {
      case ObIntType:
      case ObInt32Type: //add by zxh: 20151023
      {
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
        break;
      }
      case ObFloatType:
      {
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
        break;
      }
      case ObDoubleType:
      {
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
        break;
      }
#if 0
      {
        /*
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
        */
        //add by zhuxh
        if(OB_SUCCESS != (ret = transform_date_to_time(token_info->token, (int)token_info->len, pos)))
        {
        }
        else
        {
          int len = (int)strlen(pos);
          tokens_out[offset].token = pos;
          tokens_out[offset].len = len;
          pos += len;
        }
        break;
      }
#endif
      case ObVarcharType: //modify by zhuxh:20150730 [add START_NUM]
      {
        //add by zhuxh:20161018 [varchar not null] :b
        //It doesn't mean NULL even if it's empty in any special case:
        //#1 --varchar-not-null
        //#2 the cell is contained by quotes
        if(
          ( table_param_.varchar_not_null || between_quotation_marks )
          &&
          ( token_info->len==0 || ( token_info->len==4 && strncmp(token_info->token,"NULL",4)==0 ) )
        )
        {
          //mod by zhuxh:20161128 :b
          /*
          strcpy( pos, "\'\'" );
          tokens_out[offset].token = pos;
          tokens_out[offset].len = 2;
          pos+=2;
          */
          pos[0]='\0';
          tokens_out[offset].token = pos;
          tokens_out[offset].len = 1;
          pos++;
          break;
          //mod by zhuxh:20161128 :e
        }
        //add by zhuxh:20161018 [varchar not null] :e

        TokenInfo tmp_token;
        int64_t out_buffer_len = 0;
        //const static int START_NUM = 0; //modify by zhuxh:20150804 [begin from 0] //del by zhuxh:20160307

        //Attention
        //To a substr column
        //i refers to itself
        //token_info and offset refer to its source

        //add by zhuxh:20150811 :b
        TokenInfo token_info_obj(*token_info);
        if( table_param_.all_columns_have_char_delima || has_char_delima(offset) )
        {
          token_info_obj.token++;
          token_info_obj.len-=2;
        }
        const TokenInfo * token_info_ = & token_info_obj;
        //add :e

        //mod by zhuxh:20160307 :b
        //Function of substr has been moved to column mapping.
        tmp_token = *token_info_;
#if 0
        /*
        i:[0,columns_desc_nr)
        Offset of non-substr is i, but of substr is its source string.
        The following if-else means:
        if non-substr, tmp_token = token_info_;
        if substr, tmp_token = token_info_.substr.
        <To a substr column, token_info is its source instead of itself.>
        */
        if(table_param.has_substr)
        {
          SubstrInfo substrInfo;
          std::map<int, SubstrInfo>::const_iterator it = table_param.substr_map.find(offset_in);
          if(it != table_param_.substr_map.end())
          {
            substrInfo = it->second;
            //add by zhuxh:20150804 :b
            if( substrInfo.beg_pos<START_NUM )
            {
              ret = OB_ARRAY_OUT_OF_RANGE;
              TBSYS_LOG(ERROR, "Beginning of substring < %d !",START_NUM);
              break;
            }
            if( substrInfo.len<0 )
            {
              //mod by zhuxh:20151030
              /*
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "Length of substring < 0 !");
              break;
              */
              substrInfo.len += static_cast<int>(token_info_->len)-substrInfo.beg_pos;
            }
            //add:e
            if((substrInfo.beg_pos - START_NUM < token_info_->len) && (substrInfo.beg_pos - START_NUM + substrInfo.len) <= token_info_->len)
            {
              tmp_token.len = substrInfo.len;
            }
            else if((substrInfo.beg_pos <= token_info_->len) && (substrInfo.beg_pos - START_NUM + substrInfo.len) > token_info_->len)
            {
              tmp_token.len = token_info_->len - substrInfo.beg_pos + START_NUM;
            }
            else
            {
              tmp_token.len = 0;
            }
            tmp_token.token = token_info_->token + substrInfo.beg_pos - START_NUM;
          }
          else
          {
            tmp_token = *token_info_;
          }
        }
        else
        {
          tmp_token = *token_info_;
        }
#endif
        //mod :e

        if(OB_SUCCESS != (ret = add_escape_char(tmp_token.token, tmp_token.len, pos, tokens_buffer_size - (pos - tokens_buffer), out_buffer_len, between_quotation_marks))) //mod by zhuxh:20160912 [quotation mark]
        {
          TBSYS_LOG(ERROR, "add escape char error! because string is too long!");
        }
        else
        {
          tokens_out[offset].token = pos;
          tokens_out[offset].len = out_buffer_len;
          pos += out_buffer_len;
        }
        break;
      }
      case ObDecimalType:
      {
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
        if( decimal_needs_point( token_info ) )
        {
          *pos='.';
          pos++;
          tokens_out[offset].len++;
        }
        break;
      }
      case ObDateType:
      case ObTimeType:
      case ObDateTimeType:
      case ObPreciseDateTimeType:
      {
        /*
        memcpy(pos, token_info->token, token_info->len);
        tokens_out[offset].token = pos;
        tokens_out[offset].len = token_info->len;
        pos += token_info->len;
         */
        //add by zhuxh
        ObObjType tp = columns_desc[i].schema->get_type();
        if(OB_SUCCESS != (ret = transform_date_to_time(tp, token_info->token, (int)token_info->len, pos)))
        {
          TBSYS_LOG(ERROR, "Time format error!");
          ret = OB_ERROR;
          break;
        }
        else
        {
          int len = (int)strlen(pos);
          tokens_out[offset].token = pos;
          tokens_out[offset].len = len;
          pos += len;
        }
        break;
      }
      default:
        TBSYS_LOG(ERROR, "unexpect type index: %d", column_info->schema->get_type());
        ret = OB_ERROR;
        break;
    }
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "token format failed! columnname[%s] offset[%d] type[%s]", column_info->schema->get_name(), column_info->offset, ob_obj_type_str(column_info->schema->get_type()));
    }
  }
  if(ret && pos > tokens_buffer + tokens_buffer_size)
  {
    TBSYS_LOG(ERROR, "token_buffer overflow, because the line is too long!");
    ret = OB_ERROR;
  }
  return ret;
}

//add or delete escape character
//mod by zhuxh:20151112
int ObRowBuilder::add_escape_char(const char *str, const int64_t str_len, char *out_buffer, int64_t capacity, int64_t& out_buffer_len, bool between_quotation_marks) const
{
  char *dest_str = out_buffer;
  int i = 0;
  int j = 0;
  int ret = OB_SUCCESS;
  char q='\"';
  bool last_is_q=false;
  for(; i < str_len; i++)
  {
    if(str[i] == '\\')
    {
      dest_str[j++] = '\\';
      dest_str[j++] = str[i];
    }
    //del by zhuxh:20151104
    //single quotaion mark may not need escape character
    /*
    else if(str[i] == '\'')
    {
      dest_str[j++] = '\\';
      dest_str[j++] = '\'';
    }*/
    else
    {
      //mod by zhuxh:20160912 [quotation mark] :b
      //Normally we keep the char except the followings happen simultaneously
      //#1 I'm contained by ""
      //#2 The second of connected " isn't kept which is indicated by param --keep-double-quotation-marks
      //#3 My left char and I are both "
      if( ! ( between_quotation_marks && ! table_param_.keep_double_quotation_marks && str[i] == q && last_is_q ) )
        dest_str[j++] = str[i];
      //mod by zhuxh:20160912 [quotation mark] :e

      //We only deal with quotation marks not at the beginning or end
      //which are marks, not the contents of a string

      //del by zhuxh:20160310 [logic error]
#if 0
      if(str[i] == q && i!=0 && i!=str_len-1)
        last_is_q=!last_is_q;
#endif

      //If I am ", I'll tell next it's the opposite to me.
      //For an example, the last sequence of """" is 0101
      //mod by zhuxh:20160912 [quotation mark]
      //if(i!=0 && i!=str_len-1)
      if(i<str_len-1)
         last_is_q = ( str[i] == q ? !last_is_q : false );
      //mod :e
    }
    if(j > capacity)
    {
      ret = OB_ERROR;
    }
    else
    {
      out_buffer_len = j;
    }
  }
  return ret;
}


//int ObRowBuilder::construct_replace_sql(const TokenInfo *tokens,
//                                        const int token_nr,
//                                        const ColumnInfo *columns_desc,
//                                        const int columns_desc_nr,
//                                        const ObString table_name,
//                                        ObString &sql) const
//{
//  UNUSED(tokens);
//  UNUSED(token_nr);
//  UNUSED(columns_desc);
//  UNUSED(columns_desc_nr);
//  UNUSED(table_name);
//  UNUSED(sql);
//  const char *str = "replace into db.test_import_t1(c1,c2,c3,c4,c5) values(1,2,3,4,5),(2,2,3,4,5),(3,2,3,4,5),(4,2,3,4,5);";
//  sql = ObString::make_string(str);
//  return OB_SUCCESS;
//}

//int ObRowBuilder::construct_insert_sql(const TokenInfo *tokens,
//                                        const int token_nr,
//                                        const ColumnInfo *columns_desc,
//                                        const int columns_desc_nr,
//                                        const ObString table_name,
//                                        ObString &sql) const
//{
//  UNUSED(tokens);
//  UNUSED(token_nr);
//  UNUSED(columns_desc);
//  UNUSED(columns_desc_nr);
//  UNUSED(table_name);
//  UNUSED(sql);
//  return OB_SUCCESS;
//}

//int ObRowBuilder::construct_delete_sql(const TokenInfo *tokens,
//                                        const int token_nr,
//                                        const ColumnInfo *columns_desc,
//                                        const int columns_desc_nr,
//                                        const int64_t *rowkey_offset,
//                                        int64_t rowkey_offset_nr,
//                                        const ObString table_name,
//                                        ObString &sql) const
//{
//  UNUSED(tokens);
//  UNUSED(token_nr);
//  UNUSED(columns_desc);
//  UNUSED(columns_desc_nr);
//  UNUSED(rowkey_offset);
//  UNUSED(rowkey_offset_nr);
//  UNUSED(table_name);
//  UNUSED(sql);
//  return OB_SUCCESS;
//}

int ObRowBuilder::build_sql(RecordBlock &block, ObString &sql)
{
  int ret = OB_SUCCESS;
  int token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  int tokens_out_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];
  TokenInfo tokens_out[token_nr];
  Slice slice;
  block.reset();

  //add by zhuxh:20170411 [performance optimization] :b
  char * token_buf = new (std::nothrow) char[MAX_TOKEN_LENGTH];
  if( !token_buf )
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    return ret;
  }
  //add by zhuxh:20170411 [performance optimization] :e
  static __thread char tokens_buffer[1024*1024];
  if(NULL == tokens_buffer)
  {
    TBSYS_LOG(ERROR, "allocate memory failed!");
    ret = OB_ERROR;
  }

  atomic_add(int(block.get_rec_num()), &lineno_);
  while (block.next_record(slice))
  {
    token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;

    //del by zhuxh:20151126
    //Both gbk and utf-8 should use the same way to cut.

    //add by zhuxh:20150814 :b
    /*
    if (g_gbk_encoding) {
      if (RecordDelima::CHAR_DELIMA == table_param_.delima.delima_type() && table_param_.delima.get_char_delima() < 128u) {
        Tokenizer::tokenize_gbk(slice, static_cast<char>(table_param_.delima.get_char_delima()), token_nr, tokens);
      }
      else {
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(ERROR, "Short delima or delima greater than 128 are not supported in gbk encoding.");
        break;
      }
    }
    else {
    */
      //modify by pangtz:20141127
      //Tokenizer::tokenize(slice, table_param_.delima, token_nr, tokens);
      Tokenizer::tokenize(slice, table_param_, token_nr, tokens);
    //}
    //add:e

//delete by zhuxh:20150727
#if 0
    if (token_nr != table_param_.input_column_nr)
    {
      error_arr[COLUMN_NUM_ERROR] = false;
      TBSYS_LOG(ERROR, "corrupted data files, please check it, input column number=%d, real nr=%d, slice[%.*s]",
                table_param_.input_column_nr, token_nr, static_cast<int>(slice.size()), slice.data());
      ret = OB_ERROR;
      break;
    }
#endif
    if (g_print_lineno_taggle)
    {
      g_print_lineno_taggle = false;
      TBSYS_LOG(INFO, "proccessed line no [%d]", atomic_read(&lineno_));
    }
    // 检查格式
    if(OB_SUCCESS != (ret = check_format(tokens, token_nr, columns_desc_, columns_desc_nr_,token_buf)) ) //add by zhuxh:20170411 [performance optimization]
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "check_format error! row_content[%.*s]", (int)slice.size(), slice.data());
    }
    // 处理数据
    else if(OB_SUCCESS != (ret = tokens_format(tokens, token_nr, tokens_buffer, 1024*1024, tokens_out, tokens_out_nr, table_param_, columns_desc_, columns_desc_nr_,token_buf))) //add by zhuxh:20170411 [performance optimization]
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "tokens_format error! row_content[%.*s]", (int)slice.size(), slice.data());
    }

    //mod by zhangcd 20150814:b
    if(OB_SUCCESS == ret)
    {
      if(tokens_out_nr != columns_desc_nr_)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "the real value is not mattching with the column_desc! input_values count[%d], column_desc count[%d]", tokens_out_nr, columns_desc_nr_);
      }
    }
    //mod:e
    // 拼接sql语句
    if(OB_SUCCESS == ret && table_param_.is_insert)
    {
      if(OB_SUCCESS != (ret = construct_insert_sql(tokens_out, tokens_out_nr, columns_desc_, columns_desc_nr_, ObString::make_string(table_param_.table_name.c_str()), sql)))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "construct insert sql error! row_content[%.*s]", (int)slice.size(), slice.data());
      }
    }
    else if(OB_SUCCESS == ret && table_param_.is_replace)
    {
      if(OB_SUCCESS != (ret = construct_replace_sql(tokens_out, tokens_out_nr, columns_desc_, columns_desc_nr_, ObString::make_string(table_param_.table_name.c_str()), sql)))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "construct replace sql error! row_content[%.*s]", (int)slice.size(), slice.data());
      }
    }
    else if(OB_SUCCESS == ret && table_param_.is_delete)
    {
      if(OB_SUCCESS != (ret = construct_delete_sql(tokens_out, tokens_out_nr, columns_desc_, columns_desc_nr_, rowkey_offset_, rowkey_desc_nr_, ObString::make_string(table_param_.table_name.c_str()), sql))) //mod by zhuxh:20161018 [--del]
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "construct delete sql error! row_content[%.*s]", (int)slice.size(), slice.data());
      }
    }

    //TBSYS_LOG(DEBUG, "sql: %.*s", static_cast<int>(sql.length()), sql.ptr());

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed slice[%.*s]", static_cast<int>(slice.size()), slice.data());
      break;
    }
  }
  TBSYS_LOG(DEBUG, "sql: %.*s", static_cast<int>(sql.length()), sql.ptr());
  return ret;
}

int ObRowBuilder::get_column_desc(const ColumnInfo *& columns_desc, int &columns_desc_nr)
{
  int ret = OB_SUCCESS;
  if(NULL != columns_desc_)
  {
    columns_desc = columns_desc_;
    columns_desc_nr = columns_desc_nr_;
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

bool ObRowBuilder::has_all_rowkey(const ColumnInfo *columns_desc,
                                  const int columns_desc_nr,
                                  const ObString table_name)
{
  bool ret = true;
  const ObTableSchema *table_schema = schema_->get_table_schema(table_name);
  if(NULL == table_schema)
  {
    ret = false;
    TBSYS_LOG(ERROR, "table[%.*s] not exist!", table_name.length(), table_name.ptr());
  }
  if(ret)
  {
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    for(int i = 0; i < rowkey_info.get_size() && ret; i++)
    {
      uint64_t column_id = 0;
      if(OB_SUCCESS != rowkey_info.get_column_id(i, column_id))
      {
        ret = false;
        TBSYS_LOG(ERROR, "get rowkey failed!");
      }
      bool key_find = false;
      for(int j = 0; j < columns_desc_nr && ret; j++)
      {
        if(columns_desc[j].schema->get_id() == column_id)
        {
          key_find = true;
          break;
        }
      }
      if(!key_find)
      {
        ret = false;
        TBSYS_LOG(ERROR, "find key name[%s] column_id[%ld] failed!", columns_desc[i].schema->get_name(), columns_desc[i].schema->get_id());
        break;
      }
    }
  }
  return ret;
}

//===============================================================
//zhuxh
//construct replace, insert and delete

//#1
//main
int ObRowBuilder::construct_replace_sql(
    const TokenInfo *tokens,
    const int token_nr,
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    const ObString table_name,
    ObString &sql)
{
  return construct_replace_or_insert_sql(
        "REPLACE",
        tokens,token_nr,columns_desc,columns_desc_nr,table_name,sql
        );
}

int ObRowBuilder::construct_insert_sql(
    const TokenInfo *tokens,
    const int token_nr,
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    const ObString table_name,
    ObString &sql)
{
  return construct_replace_or_insert_sql(
        "INSERT",
        tokens,token_nr,columns_desc,columns_desc_nr,table_name,sql
        );
}

//mod by zhuxh:20161018 [--del]
//=>
//delete from d where (c0,c1) in (('0','0'),('1','1'))
int ObRowBuilder::construct_delete_sql(
    const TokenInfo *tokens,
    const int token_nr, //mod by zhuxh:20161018 [--del]
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    const int64_t *rowkey_offset,
    int64_t rowkey_offset_nr,
    const ObString table_name,
    ObString &sql)
{
  int ret = OB_SUCCESS;
  //UNUSED(ret);
  int columns_desc_fail_i = -1;
  if( NULL == tokens )
  {
    TBSYS_LOG(ERROR, "NULL == tokens");
    ret = OB_ERROR;
  }
  else if( NULL == columns_desc )
  {
    TBSYS_LOG(ERROR, "NULL == columns_desc");
    ret = OB_ERROR;
  }
  else if(
      (columns_desc_fail_i =
      columns_desc_fail_at(/*token_nr, */columns_desc, columns_desc_nr))
      >= 0
      )
  {
    TBSYS_LOG(ERROR, "columns_desc[%d] wrong.", columns_desc_fail_i);
  }
  else if( NULL == rowkey_offset )
  {
    TBSYS_LOG(ERROR, "NULL == rowkey_offset");
    ret = OB_ERROR;
  }
  else
  {
  char str[MAX_CONSTRUCT_SQL_LENGTH]={0};
  char tokens_str[MAX_CONSTRUCT_SQL_LENGTH]={0};
  char columns_str[MAX_CONSTRUCT_SQL_LENGTH]={0};

  if( OB_SUCCESS != (ret = tokens_2_str(
      tokens, token_nr, columns_desc,
      rowkey_offset, rowkey_offset_nr, tokens_str))
      )
  {
    TBSYS_LOG(ERROR, "tokens conversion error");
    ret = OB_ERROR;
  }
  else if(
      OB_SUCCESS != (ret = columns_2_str(
      columns_desc, columns_desc_nr,
      rowkey_offset, rowkey_offset_nr, columns_str) )
          )
  {
    TBSYS_LOG(ERROR, "column descriptions conversion error");
    ret = OB_ERROR;
  }
  else
  {
    bool sql_not_empty = sql.compare("");

    if(
       static_cast<int64_t>(
         strlen("DELETE FROM \"")+table_name.length()
         +strlen("\" WHERE ")+strlen(columns_str)+strlen(" IN (")+strlen(tokens_str)+strlen(")")
         )
       >=MAX_CONSTRUCT_SQL_LENGTH
       ||
       static_cast<int64_t>(
         sql.length()+strlen(",")+strlen(tokens_str)+strlen(")")
         )
       >=MAX_CONSTRUCT_SQL_LENGTH
       )
    {
      TBSYS_LOG(ERROR, "SQL overflows");
      ret = OB_ARRAY_OUT_OF_RANGE;
    }
    else
    {
      if( sql_not_empty ) // str != ""
        SC(",");
      else
      {
        SC("DELETE FROM \"");
        strncat(str,table_name.ptr(),table_name.length());
        SC("\" WHERE ");  //mod by zhuxh:20160617 [sensitive case of database and table]
        SC(columns_str);
        SC(" IN (");
      }

      SC(tokens_str);
      SC(")");
    }
    ObString::obstr_size_t val_len = static_cast<ObString::obstr_size_t>(strlen(str));
    if( sql_not_empty )
    {
      sql.set_length( sql.length()-1 ); //remove ')' at the end
    }
    if(val_len != sql.write(str, val_len))
    {
      TBSYS_LOG(ERROR, "SQL overflows");
      ret = OB_MEM_OVERFLOW;
    }
    else
      ret = OB_SUCCESS;
  }}
  return ret;
}

//===============================================================
//#2
//components

int ObRowBuilder::columns_desc_fail_at(
    //const int token_nr,
    const ColumnInfo *columns_desc,
    const int columns_desc_nr
    )
{
  int ret = -1;
  int i=0;
  for(i=0; i<columns_desc_nr; i++)
    if( NULL == columns_desc[i].schema /*|| columns_desc[i].offset >= columns_desc_nr*/ ) //modify by zhuxh:20150811
      break;
  if(i<columns_desc_nr)
    ret = i;
  else
    ret = -1;
  return ret;
}

//del by zhuxh:20150917
#if 0
const char * ObRowBuilder::get_quotation_mark(const ColumnInfo * p_column_info)
{
  const static int LEN = 4;
  static ColumnType column_types_with_quotation_marks[20]=
  {
    ObVarcharType,ObPreciseDateTimeType,ObDateType,ObTimeType
  };
  const ColumnType & column_type = p_column_info->schema->get_type();
  for(int i=0; i<LEN; i++)
    if( column_types_with_quotation_marks[i] == column_type )
      return "\'";
  return "";
}
#endif

//modify by zhuxh:20150727 :b
/*
+--------+-----------------+----------------+
|datafile|instruction      |treated as      |
+--------+-----------------+----------------+
|        |nothing          |NULL            |
+--------+-----------------+----------------+
|NULL    |4 letters        |NULL            |
+--------+-----------------+----------------+
|''      |2 quotation marks|''(empty string)|
+--------+-----------------+----------------+
*/
//Attention!
//token and column_desc is the pointer of current object instead of its array address
//Namely when use it you should write it like
//add_token_2_str(tokens+i,columns_desc+i,i,str)
//instead of
//add_token_2_str(tokens,columns_desc,i,str)
int ObRowBuilder::add_token_2_str(
    const TokenInfo *token,
    const ColumnInfo *column_desc,
    int offset,
    char str[]
    )
{
  int ret = OB_SUCCESS;

//del by zhuxh:20151102
#if 0
  bool A0 = (column_desc->schema->get_type()==ObVarcharType) && !is_decimal_to_varchar(offset);
  bool A1 = !A0;
  bool B0 = (token->len == 0 || token->token == NULL);
  bool B1 = !B0 && table_param_.has_null_flag && token->token[0]==table_param_.null_flag && token->len == 1;
  bool C0 = table_param_.has_null_flag;
  bool C1 = !C0;

  if( false )//A1 && B0 && C0 //mod by zhuxh:20150917
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Token[%.*s] is an unknown data.", static_cast<int>(token->len), token->token);
    //TBSYS_LOG(ERROR, "\nA0:%d A1:%d\nB0:%d B1:%d\nC0:%d C1:%d", A0, A1, B0, B1, C0, C1);
    //TBSYS_LOG(ERROR, "Token[%.*s]", static_cast<int>(token->len), token->token);
  }
  else if( (A0 && B0 && C0) || (A0 && B0 && C1) )
    SC("\'\'");
  else if( (A0 && B1 && C0) || (A1 && B1 && C0) || (A1 && B0 && C1) || (A1 && B0 && C0) )
    SC("NULL");
  else
  {
    char quotation_mark[2]={0};
    if( type_has_quotation_marks(column_desc->schema->get_type()) )
      quotation_mark[0]='\'';
    SC( quotation_mark );
    strncat(str,token->token,token->len);
    SC( quotation_mark );
  }
#endif

  UNUSED(offset);
  if(!str)
  {
    TBSYS_LOG(ERROR,"Parameter string is NULL");
    ret = OB_ERROR;
  }
  else
  {
    if(!token->token)
    {
      TBSYS_LOG(ERROR,"token->token is NULL");
      ret = OB_ERROR;
    }
    else if( token->len == 0 || (token->len==4 && strncasecmp(token->token,"NULL",4)==0 ) ) //mod by zhuxh:20161018 [varchar not null]
    {
      SC("NULL");
    }
    //add by zhuxh:20161128 :b
    else if( token->len == 1 && token->token[0]=='\0' ) //empty string
    {
      SC("\'\'");
    }
    //add by zhuxh:20161128 :e
    else
    {
      //mod by zhuxh:20151109 :b
      //if the original token includes ' or " at the beginning and end

      //mod by zhuxh:20160912 [quotation mark] :b
      //mod by zhuxh:20161018 [quotation mark]

//del by zhuxh:20161128 //The superficial " have been removed in tokens_format()
#if 0
      int with_quotation_mark=static_cast<int>(
            (token->token[0]=='\''/* || token->token[0]=='\"'*/) //mod by zhuxh:20161018 [varchar not null]
          && token->len>=2
          && token->token[token->len-1]==token->token[0]
          && column_desc->offset != ColumnInfo::SEQUENCE
      ); //mod by zhuxh:20160603 [Sequence]
#endif

      int with_quotation_mark=0; //The superficial " have been removed in tokens_format()
      //mod by zhuxh:20160912 [quotation mark] :e

      //si, index of str[]
      int si=static_cast<int>(strlen(str));
      //if the column should have ' at start and end in sql, like varchar and so on
      int add_quotation_mark=static_cast<int>(type_has_quotation_marks(column_desc->schema->get_type()));
      //ti, index of token->token[]
      int ti=with_quotation_mark;
      if(add_quotation_mark)str[si++]='\'';
      for(int tl=static_cast<int>(token->len)-with_quotation_mark;ti<tl;ti++,si++)
      {
        if(token->token[ti]=='\'') //escape char
          str[si++]='\\';
        str[si]=token->token[ti];
      }
      if(add_quotation_mark)str[si++]='\'';
      //mod :e
    }
  }
  return ret;
}
//modify :e

//modify by zhuxh:20150727
//TokenInfo[]
//{"1", "abc", "3.14"}
//=>
//('1','abc','3.14')
int ObRowBuilder::tokens_2_str(
    const TokenInfo *tokens,
    const int token_nr,
    const ColumnInfo *columns_desc,
    char str[]
    )
{
  int ret = OB_SUCCESS;
  str[0]=0;
  int i=0;
  SC("(");
  for(i=0;i<token_nr;i++)
  {
    if(i>0) SC(",");
    if( OB_SUCCESS != add_token_2_str(tokens+i,columns_desc+i,i,str) )
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "Fail when a token is transformed to string.");
      break;
    }
  }
  SC(")");
  return ret;
}

//add by zhuxh:20161018 [--del] :b
int ObRowBuilder::tokens_2_str(
    const TokenInfo *tokens,
    const int token_nr,
    const ColumnInfo *columns_desc,
    const int64_t * offsets,
    int64_t offset_nr,
    char str[])
{
  int ret = OB_SUCCESS;
  str[0]=0;
  int64_t i=0;
  int j=0;
  SC("(");
  for(j=0;j<offset_nr;j++)
  {
    i=offsets[j];
    if( i>=token_nr )
    {
      ret = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(ERROR, "offset %ld is beyond the number of tokens, %d",i,token_nr);
      break;
    }

    if(/*i*/j>0) SC(","); //mod by zhuxh:20161128
    if( OB_SUCCESS != add_token_2_str(tokens+i,columns_desc+i,static_cast<int>(i),str) )
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "Fail when a token is transformed to string.");
      break;
    }
  }
  SC(")");
  return ret;
}
//add by zhuxh:20161018 [--del] :e

//ColumnInfo[]
//=>
//("c1","c2","c3")
//Quotation marks in case that column name is a key word
//mod by zhuxh:20161018 [--del] //Return value modified
int ObRowBuilder::columns_2_str(
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    char str[]
    )
{
  int ret = OB_SUCCESS;
  str[0]=0;
  int i=0;
  SC("(");
  for(i=0;i<columns_desc_nr;i++)
  {
    if(i>0) SC(",");
    SC("\"");
    SC(columns_desc[i].schema->get_name());
    SC("\"");
  }
  SC(")");
  return ret;
}
//add by zhuxh:20161018 [--del] :b
int ObRowBuilder::columns_2_str(
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    const int64_t * offsets,
    int64_t offset_nr,
    char str[]
    )
{
  int ret = OB_SUCCESS;
  str[0]=0;
  int64_t i=0;
  int j=0;
  SC("(");
  for(j=0;j<offset_nr;j++)
  {
    i=offsets[j];
    if( i>=columns_desc_nr )
    {
      ret = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(ERROR, "offset %ld is beyond the number of column descriptions, %d",i,columns_desc_nr);
      break;
    }

    if(/*i*/j>0) SC(","); //mod by zhuxh:20161128
    SC("\"");
    SC(columns_desc[i].schema->get_name());
    SC("\"");
  }
  SC(")");
  return ret;
}
//add by zhuxh:20161018 [--del] :e

//=>
//c1='abc' AND c9='DGSG'
int ObRowBuilder::tokens_and_columns_2_str(
    const TokenInfo *tokens,
    const ColumnInfo *columns_desc,
    const int64_t *rowkey_offset,
    int64_t rowkey_offset_nr,
    char str[])
{
  int ret = OB_SUCCESS;
  str[0]=0;
  int i=0;
  int64_t j=0;
  for(i=0;i<rowkey_offset_nr;i++)
  {
    j=rowkey_offset[i];
    if(i>0) SC(" AND ");
    SC(columns_desc[j].schema->get_name());
    SC("=");
    if( OB_SUCCESS != add_token_2_str(tokens+j,columns_desc+j,(int)j,str) )
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "Fail when a token is transformed to string.");
      break;
    }
  }
  return ret;
}

//=>
//REPLACE INTO my_table(c1,c2) VALUES('1','abc')
int ObRowBuilder::construct_replace_or_insert_sql(
    const char * function_name, //"REPLACE" or "INSERT"
    const TokenInfo *tokens,
    const int token_nr,
    const ColumnInfo *columns_desc,
    const int columns_desc_nr,
    const ObString table_name,
    ObString &sql)
{
  int ret = OB_ERROR;
  int columns_desc_fail_i = 0;
  //UNUSED(ret);
  if( NULL == tokens )
  {
    TBSYS_LOG(ERROR, "NULL == tokens");
    ret = OB_ERROR;
  }
  else if( NULL == columns_desc )
  {
    TBSYS_LOG(ERROR, "NULL == columns_desc");
    ret = OB_ERROR;
  }
  else if(
      (columns_desc_fail_i =
      columns_desc_fail_at(/*token_nr, */columns_desc, columns_desc_nr))
      >= 0
      )
  {
    TBSYS_LOG(ERROR, "columns_desc[%d] wrong.", columns_desc_fail_i);
  }
  else
  {
  char str[MAX_CONSTRUCT_SQL_LENGTH]="",
      tokens_str[MAX_CONSTRUCT_SQL_LENGTH]="",
      columns_str[MAX_CONSTRUCT_SQL_LENGTH]="";
  if( OB_SUCCESS !=
     (ret = tokens_2_str(tokens,token_nr,columns_desc,tokens_str)) )
  {
  }
  else
  {
    columns_2_str(columns_desc,columns_desc_nr,columns_str);

    if(
       static_cast<int64_t>(
         strlen(str)+strlen(",")+strlen(tokens_str)
         )>=MAX_CONSTRUCT_SQL_LENGTH
       ||
       static_cast<int64_t>(
         strlen(function_name)+strlen(" INTO ")+table_name.length()
         +strlen(columns_str)+strlen(" VALUES")+strlen(tokens_str)
         )>=MAX_CONSTRUCT_SQL_LENGTH
       )
    {
      TBSYS_LOG(ERROR, "SQL overflows");
      ret = OB_ERROR;
    }
    else
    {
      if( sql.compare("") ) // str != ""
        SC(",");
      else
      {
        SC(function_name);
        SC(" INTO \"");  //mod by zhuxh:20160617 [sensitive case of database and table]
        strncat(str,table_name.ptr(),table_name.length());
        SC("\"");  //mod by zhuxh:20160617 [sensitive case of database and table]
        SC(columns_str);
        SC(" VALUES");
      }
      SC(tokens_str);
      ret = OB_SUCCESS;
    }
    //TBSYS_LOG(INFO, "%s", str);
    ObString::obstr_size_t val_len = static_cast<ObString::obstr_size_t>(strlen(str));
    if(val_len != sql.write(str, val_len))
    {
      TBSYS_LOG(ERROR, "SQL overflows");
      ret = OB_ERROR;
    }
    else
      ret = OB_SUCCESS;
  }}
  return ret;
}

//add by zhuxh:20150724
//--delima 15
//=>
//(ASCII)'\15'
//    OR
//--delima 2,8
//=>
//(ASCII)'\2','\8'
void string_2_delima( const char * str, RecordDelima & delima )
{
  if ( str ) {
    const char *end_pos = str + strlen(str);
    if (find(str, end_pos, ',') == end_pos) {
      delima.set_char_delima(static_cast<char>(atoi(str)));
    } else {
      int part1, part2;
      sscanf(str, "%d,%d", &part1, &part2);
      delima.set_short_delima(static_cast<char>(part1), static_cast<char>(part2));
    }
  }
}

//del by zhuxh:20150906
#if 0
//add by zhuxh:20150724
//parse a string like
//"sub0,src0,2,1;sub1,src1,4,2"
//which means column sub0 is column src0.substr(2,2+1)...
int parse_substr_grammar(const char * grammar, std::map<std::string, SubstrInfo> & m)
{
  const char COMMA = ',';
  const char SEMICOLON = ';';
  const int PARAM_NR = 4;
  SubstrInfo si;
  si.beg_pos = 0;
  si.len = 0;
  int i=0;
  int j=1;
  for(; ; i++)
  {
    if( !grammar[i] || grammar[i]==SEMICOLON )
    {
      if(j!=PARAM_NR)
        return OB_ERROR;
      m[si.column_name] = si;
      j=1;
      si.column_name.clear();
      si.source_column_name.clear();
      si.beg_pos = 0;
      si.len = 0;
      if( !grammar[i] )
        break;
    } else if( grammar[i]==COMMA )
      j++;
    else switch(j)
    {
    case 1:
      si.column_name += grammar[i];
      break;
    case 2:
      si.source_column_name += grammar[i];
      break;
    case 3:
      if( !isdigit(grammar[i]) ) return OB_ERROR;
      si.beg_pos = si.beg_pos * 10 + grammar[i]-'0';
      break;
    case 4:
      if( !isdigit(grammar[i]) ) return OB_ERROR;
      si.len = si.len * 10 + grammar[i]-'0';
      break;
    }
  }
  return OB_SUCCESS;
}
#endif

//add by zhuxh:20150820 :b
//parse a column map grammar like
//"9:col9,5,[\'Hello,world\'];col3,[321];col4,2;col6,9,substr(5,3)"
int parse_column_map_grammar(const char * grammar, int & columns_sum, vector<ColumnMapInfo> & v, std::map<int, SubstrInfo> & m, std::vector<SequenceInfo> & vseq)
{
    const static char COLON = ':';
    const static char SEMICOLON = ';';
    const static char COMMA = ',';
    const static char LEFT_BRACKET = '[';
    const static char RIGHT_BRACKET = ']';
    const static char LEFT_PARENTHESIS = '(';
    const static char RIGHT_PARENTHESIS = ')';
    const static char SINGLE_QUOTE = '\'';
    const static int INIT_INT = ColumnMapInfo::UNDEFINED; //mod by zhuxh:20160603 [Sequence]
    int i=0;
    int _i=0;
    int cnt = 0;
    bool is_digit = true;
    bool in_brackets = false;
    bool in_parentheses = false;
    ColumnMapInfo cmi;

    columns_sum = INIT_INT;
    for(; ; i++)
    {
        //#1
        //syntex analysis and loop control
        switch( grammar[i] )
        {
        case '\0':
            if( is_digit && _i == 0 )
            {
                columns_sum = atoi(grammar);
                return OB_SUCCESS;
            }
        }

        if( in_brackets )
        {
            if(!grammar[i])
            {
                TBSYS_LOG(ERROR, "Incomplete bracket in column map syntex!");
                return OB_ERROR;
            }
            if( grammar[i]!=SINGLE_QUOTE && grammar[i]!=LEFT_BRACKET && grammar[i]!=RIGHT_BRACKET )
              is_digit = is_digit && (isdigit(grammar[i]) || grammar[i]=='-' || grammar[i]=='.'); // mod by zhuxh:20150917
            if( grammar[i]!=RIGHT_BRACKET ) continue;
        }

        if( in_parentheses )
        {
            if(!grammar[i])
            {
                TBSYS_LOG(ERROR, "Incomplete parenthesis in column map syntex!");
                return OB_ERROR;
            }
            if( grammar[i]!=RIGHT_PARENTHESIS ) continue;
        }

        switch( grammar[i] )
        {
        case SEMICOLON:
        case '\0':
            if( cnt<1 )
            {
                TBSYS_LOG(ERROR, "Wrong number of parameters for a column name in column map syntex!");
                return OB_ERROR;
            }
        case COMMA:
            if( cnt>=3 )
            {
                TBSYS_LOG(ERROR, "Wrong number of parameters for a column name in column map syntex!");
                return OB_ERROR;
            }
        case COLON:
            if( i==_i )
            {
                TBSYS_LOG(ERROR, "Redundant punctuations in column map syntex!");
                return OB_ERROR;
            }
            break;
        }

        switch( grammar[i] )
        {
        case '\0':
        case COMMA:
        case SEMICOLON:
            if( cnt==0 ) //column name
            {
                if( cmi.column_name.empty() )
                {
                    //mod by zhuxh:20160105
                    //trim
                    int j=i;
                    int _j=_i;
                    while(_j<j && grammar[_j]==' ')_j++;
                    while(_j<j && grammar[j-1]==' ')j--;
                    cmi.column_name = string(grammar+_j,j-_j);
                }
                else
                {
                    TBSYS_LOG(ERROR, "Repeatedly set column name in column map syntex!");
                    return OB_ERROR;
                }
            }
            else if( grammar[_i]==LEFT_BRACKET && grammar[i-1]==RIGHT_BRACKET ) //default token
            {
                if( ! cmi.default_token.empty() )
                {
                    TBSYS_LOG(ERROR, "Repeatedly set default token in column map syntex!");
                    return OB_ERROR;
                }
                else if( ! is_digit && ( grammar[_i+1]!=SINGLE_QUOTE || grammar[i-2]!=SINGLE_QUOTE ) )
                {
                    TBSYS_LOG(ERROR, "A string as default token must be contained by single quote like \'apple\' in column map syntex!");
                    return OB_ERROR;
                }
                else
                    cmi.default_token = string(grammar+_i+1, i-_i-2);
            }
            else if( is_digit ) //datafile column id
            {
                if( cmi.datafile_column_id == INIT_INT )
                    cmi.datafile_column_id = atoi(grammar+_i)-1;//mod by zhuxh:20160101 [starting from 1, not 0]
                //mod by zhuxh:20160603 [Sequence] :b
                else if( cmi.datafile_column_id == ColumnMapInfo::SEQUENCE )
                {
                    TBSYS_LOG(ERROR, "Datafile column can't not be set when sequence function is used!");
                    return OB_ERROR;
                }
                //mod :e
                else
                {
                    TBSYS_LOG(ERROR, "Repeatedly set datafile column id in column map syntex!");
                    return OB_ERROR;
                }
            }
            else if( i-_i>=3 && strncasecmp(grammar+_i,"sub",3)==0 && grammar[i-1]==RIGHT_PARENTHESIS ) //sub(0,5) || substr(0,5)  //mod by zhuxh:20160603 [Sequence]
            {
              std::vector<int> v;
              int l=_i;
              for(; l<i && grammar[l]!=LEFT_PARENTHESIS; l++);
              l++;
              int r=i-1;
              size_t vl=0;//vector length
              int ret = OB_SUCCESS;
              if(l<r)
              {
                ret = numbers_2_idx_vector(v,grammar+l,r-l,',');
                vl = v.size();
              }
              if( OB_SUCCESS != ret || l>=r || vl>=3 || (vl==2 && v[0]<0) )  //mod by zhuxh:20160603 [Sequence]
              {
                TBSYS_LOG(ERROR, "Substr syntex error!");
                return OB_ERROR;
              }
              else
              {
                SubstrInfo si;
                si.column_name = cmi.column_name;
                si.datafile_column_id = cmi.datafile_column_id;
                //si.beg_pos = v[0];
                if(vl==1)
                {
                    if(v[0]>=0)
                        si.beg_pos=v[0];
                    else
                        si.len=v[0];
                }
                else if(vl==2)
                {
                    si.beg_pos=v[0];
                    si.len = v[1];
                }
                m[cmi.datafile_column_id] = si;
              }
            }
            //mod by zhuxh:20160603 [Sequence] :b
            else if( i-_i>=3 && strncasecmp(grammar+_i,"seq",3)==0 && grammar[i-1]==RIGHT_PARENTHESIS ) //seq(0,5)
            {
              if( cmi.datafile_column_id != INIT_INT )
              {
                TBSYS_LOG(ERROR, "Datafile column can't not be set when sequence function is used!");
                return OB_ERROR;
              }
              std::vector<int64_t> v;
              int l=_i;
              for(; l<i && grammar[l]!=LEFT_PARENTHESIS; l++);
              l++;
              int r=i-1;
              size_t vl=0;//vector length
              int ret = OB_SUCCESS;
              if(l<r)
              {
                ret = numbers_2_idx_vector(v,grammar+l,r-l,',');
                vl = v.size();
              }
              if( OB_SUCCESS != ret || /*l>=r*/ l>r || vl>=3 ) //mod by zhuxh:20160907 [cb sequence] //l==r when the function has no parameter like seq()
              {
                TBSYS_LOG(ERROR, "Sequence syntex error!");
                return OB_ERROR;
              }
              else
              {
                SequenceInfo sqi;
                //mod by zhuxh:20160907 [cb sequence] :b
                //seq() means to keep the start and increment
                if(vl==0)
                {
                    sqi.start=SequenceInfo::UNDEFINED;
                    sqi.increment=SequenceInfo::UNDEFINED;
                }
                //seq(5) means to keep increment and change start //mod by zhuxh:20161018
                else if(vl==1)
                {
                    sqi.start=v[0];
                    sqi.increment=SequenceInfo::UNDEFINED;
                }
                else if(vl==2)
                {
                    sqi.start=v[0];
                    sqi.increment = v[1];
                }
                sqi.column_name = cmi.column_name;
                //mod by zhuxh:20160907 [cb sequence] :e
                vseq.push_back(sqi);
                cmi.datafile_column_id = ColumnMapInfo::SEQUENCE;
                cmi.seq_index = static_cast<int>(vseq.size())-1;
              }
            }
            //mod :e
            else
            {
                TBSYS_LOG(ERROR, "Wrong column map syntex!");
                return OB_ERROR;
            }
            break;
        case COLON:
            if( is_digit && _i == 0 )
                columns_sum = atoi(grammar);
            else
                return OB_ERROR;
            break;
        case LEFT_BRACKET:
            in_brackets = true;
            break;
        case LEFT_PARENTHESIS:
            in_parentheses = true;
            break;
        case RIGHT_BRACKET:
            in_brackets = false;
            break;
        case RIGHT_PARENTHESIS:
            in_parentheses = false;
            break;
        default:
            is_digit = is_digit && isdigit(grammar[i]);
            break;
        }

        switch( grammar[i] )
        {
        case SEMICOLON:
        case '\0':
            //column name and datafile column id are unable to have existed
            vector<ColumnMapInfo>::iterator it=v.begin();
            for(; it!=v.end(); it++)
                if( it->column_name == cmi.column_name
                    ||
                    (
                      //mod by zhuxh:20160603 [Sequence] :b
                      it->datafile_column_id > ColumnMapInfo::UNDEFINED
                      && cmi.datafile_column_id > ColumnMapInfo::UNDEFINED
                      //mod :e
                      && it->datafile_column_id == cmi.datafile_column_id
                    )
                  )
                  break;
            if( it!=v.end() )
            {
                TBSYS_LOG(ERROR, "Repeated column name or datafile column id in column map syntex!");
                return OB_ERROR;
            }
            v.push_back(cmi);
            cmi.clear();
            break;
        }

        if( ! grammar[i] ) break;

        //#2
        //loop variables

        switch( grammar[i] )
        {
        case COMMA:
            cnt++;
            break;
        case SEMICOLON:
            cnt=0;
            break;
        }

        switch( grammar[i] )
        {
        case COMMA:
        case SEMICOLON:
        case COLON:
            is_digit = true;
            _i = i+1;
            break;
        }
    }
    if( columns_sum == INIT_INT )
        columns_sum = static_cast<int>(v.size());
    else if( columns_sum < static_cast<int>(v.size()) )
    {
        TBSYS_LOG(ERROR, "Wrong sum of columns in column map syntex!");
        return OB_ERROR;
    }
    return OB_SUCCESS;
}
//add:e

//del by zhuxh:20160603 [Sequence]
//numbers_2_idx_vector() has been transformed into a template function, definition of which certainly locates in ob_import_v2.h
//In an INSERT sql
//value of int type should be written like 123
//but of varchar type should be '123'
bool type_has_quotation_marks(ObObjType tp)
{
  //return true;

  const static int LEN = 5;
  static ObObjType a[5]=
  {
    ObVarcharType,ObDateTimeType,ObPreciseDateTimeType,ObDateType,ObTimeType
  };
  for(int i=0;i<LEN;i++)
    if(tp == a[i])
      return true;
  return false;
}

//add by zhuxh:20160603 [Sequence] :b

bool type_supports_sequence(ObObjType tp)
{
  const static int LEN = 2;
  static ObObjType a[2]=
  {
    ObIntType, ObInt32Type
  };
  for(int i=0;i<LEN;i++)
    if(tp == a[i])
      return true;
  return false;
}

//mod by zhuxh:20160907 [cb sequence]
//returns "import_mydbname_mytablename_" into str
//Every column has its own sequence which won't be deleted after the end of import.
void get_sequence_prefix( const char * db_name, const char * table_name, char str[IMPORT_SEQUENCE_MAX_LENGTH] )
{
  memset(str,0,IMPORT_SEQUENCE_MAX_LENGTH);
  /*
  const static char * prefix = "import_pid";
  int prefix_len = static_cast<int>( strlen(prefix) );
  SC(prefix);
  sprintf(str+prefix_len,"%d",getpid());
  SC("_");
  */
  SC("import_");
  SC(db_name);
  SC("_");
  SC(table_name);
  SC("_");
}

int cb_sequence( OceanbaseDb & db,
                 const char * db_name,
                 const char * table_name,
                 const std::vector<SequenceInfo> & vseq,
                 bool create )
{
  int vl=static_cast<int>(vseq.size());
  if( vl == 0 ) return OB_SUCCESS;

  int ret = OB_SUCCESS;

  char seq_prefix [IMPORT_SEQUENCE_MAX_LENGTH] = {0};
  //char number [IMPORT_SEQUENCE_MAX_LENGTH] = {0};
  std::string seq_name;

  get_sequence_prefix(db_name,table_name,seq_prefix);
  for(int i=0; i<vl; i++)
  {
    seq_name  = seq_prefix;
    seq_name += vseq[i].column_name;
    /*
    sprintf(number, "%d", i);
    seq_name += number;
    */

    int64_t start = vseq[i].start;
    int64_t increment = vseq[i].increment;
    bool default_start = ( start == SequenceInfo::UNDEFINED );
    bool default_increment = ( increment == SequenceInfo::UNDEFINED );
    if( OB_SUCCESS != (ret = db.sequence(create, seq_name, start, increment, default_start, default_increment ) ) )
    {
      TBSYS_LOG(ERROR, "Manipulating sequence failed, ret=%d", ret);
      break;
    }
  }

  return ret;
}
//add :e

bool decimal_needs_point( const TokenInfo * token_info )
{
  int i=0;
  const char * chars="Ee.";

  if( token_info->len < 19 )
  {
    return false;
  }
  for(; i<token_info->len ; i++)
  {
    if( strchr(chars,token_info->token[i]) )
    {
      return false;
    }
  }
  return true;
}
