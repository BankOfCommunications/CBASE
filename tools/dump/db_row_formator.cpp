/*
 * =====================================================================================
 *
 *       Filename:  db_row_formator.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月27日 13时28分00秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  liyongfeng
 *   Organization:  
 *
 * =====================================================================================
 */

#include "db_row_formator.h"


const char *const DbRowFormator::user_format_style_[] = {"YYYYMMDD", "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS"};
const char *const DbRowFormator::sys_format_style1_[] = {"%.*s%.*s%.*s", "%.*s-%.*s-%.*s", "%.*s-%.*s-%.*s %.*s:%.*s:%.*s"};
const char *const DbRowFormator::sys_format_style2_[] = {"%04d%02d%02d", "%04d-%02d-%2d", "%04d-%02d-%2d %02d:%02d:%02d"};
static const char *kDefualtTimestamp = "%04d-%02d-%02d %02d:%02d:%02d";//mod dinggh [bug_fix_timestamp] 20160918
//add qianzm 20151231
static const char *kDefualtDate = "%04d%02d%02d";
static const char *kDefualtTime = "%02d:%02d:%02d";
//add e

static const int64_t kLineBufferSize = 2 * 1024 * 1024;//2M

using namespace oceanbase::sql;

DbRowFormator::DbRowFormator(ObSchemaManagerV2 *schema, const ObExportTableParam &param)
{
  schema_ = schema;
  param_ = param;
  assert(NULL != schema_);

//del zcd multi_consumers 20150114:b
//  line_buffer = NULL;
//  buff_pos = 0;
//  buff_size_ = 0;
//del:e

  inited_ = false;
}

DbRowFormator::~DbRowFormator()
{
//del zcd multi_consumers 20150114:b
//  if (line_buffer_ != NULL)
//  {
//     free(line_buffer_);
//     line_buffer_ = NULL;
//  }
//  buff_pos_ = 0;
//  buff_size_ = 0;
//del:e
}

int DbRowFormator::init()
{
  int ret = OB_SUCCESS;
//del zcd multi_consumers 20150114:b  
//  line_buffer_ = (char *)malloc(kLineBufferSize);
//  if (NULL == line_buffer_)
//  {
//     TBSYS_LOG(ERROR, "failed to allocate line buffer");
//     ret = OB_ERROR;
//  }
//  buff_pos_ = 0;
//  buff_size_ = kLineBufferSize;
//del:e
  
  size_t idx = 0;
  /* 初始化需要去除末尾小数点的字段,该字段类型仅限DECIMAL和VARCHAR(其实,DECIMAL类型可以无需指定,也可自动去掉) */
  for (; idx < param_.del_point.size() && OB_SUCCESS == ret; idx++)
  {
      const ObColumnSchemaV2 *col_schema = schema_->get_column_schema(param_.table_name.c_str(), param_.del_point[idx].c_str());
      if (NULL == col_schema)
      {
         TBSYS_LOG(ERROR, "can't find column[%s] in table[%s], please check config file", param_.del_point[idx].c_str(), param_.table_name.c_str());
         ret = OB_ERROR;
      }
      else if (col_schema->get_type() != ObVarcharType && col_schema->get_type() != ObDecimalType)
      {
         TBSYS_LOG(ERROR, "column[%s] is not VARCHAR or DECIMAL in table[%s], should not delete last point", param_.del_point[idx].c_str(), param_.table_name.c_str());
         ret = OB_ERROR;
      }
      else
      {
         TBSYS_LOG(DEBUG, "find column[%s] in table[%s]", param_.del_point[idx].c_str(), param_.table_name.c_str());
         //uint64_t cid = col_schema->get_id();
         ColumnCache::iterator itr = format_columns_.find(col_schema->get_name());
         if (itr != format_columns_.end())/* 某一列的格式化模式只能为: 删除最后一个小数点, 添加双引号分隔, 日期格式化中的一个*/
         {
            TBSYS_LOG(ERROR, "column[%s] mustn't delete last point, maybe add quotes or formating, please check config file", param_.del_point[idx].c_str());
            ret = OB_ERROR;
         }
         else
         {
            ObExportColumnInfo col_info;
            col_info.col_schema = col_schema;
            col_info.is_del_point = true;
            format_columns_.insert(ColumnCache::value_type(col_schema->get_name(), col_info));
         }
      }
  }
  /* 初始化日期格式化字段,该字段只能为VARCHAR和TIMESTAMP */
  for (idx = 0; idx < param_.col_formats.size() && OB_SUCCESS == ret; idx++)
  {
     const ObColumnSchemaV2 *col_schema = schema_->get_column_schema(param_.table_name.c_str(), param_.col_formats[idx].name.c_str());
     if (NULL == col_schema)
     {
        TBSYS_LOG(ERROR, "can't find column[%s] in table[%s], please check config file", param_.col_formats[idx].name.c_str(), param_.table_name.c_str());
        ret = OB_ERROR;
     }
     else if (col_schema->get_type() != ObVarcharType && col_schema->get_type() != ObPreciseDateTimeType && col_schema->get_type() != ObDateTimeType)
     {
        TBSYS_LOG(ERROR, "column[%s] is not VARCHAR or TIMESTAMP in table[%s], should not format", param_.col_formats[idx].name.c_str(), param_.table_name.c_str());
        ret = OB_ERROR;
     }
     else
     {
        TBSYS_LOG(DEBUG, "find column[%s] in table[%s]", param_.col_formats[idx].name.c_str(), param_.table_name.c_str());
        //uint64_t cid = col_schema->get_id();
        ColumnCache::iterator itr = format_columns_.find(col_schema->get_name());
        if (itr != format_columns_.end())
        {
           TBSYS_LOG(ERROR, "column[%s] mustn't format, maybe add quotes or delete last point, please check config file", param_.col_formats[idx].name.c_str());
           ret =OB_ERROR;
        }
        else
        {
           ObExportColumnInfo col_info;
           col_info.col_schema = col_schema;
           col_info.is_format = true;
           /* 进行格式化格式匹配与转换 */
           int l = sizeof(user_format_style_) / sizeof(char *);
           int i = 0;
           for (; i < l; i++)
           {
              if (0 == strcasecmp(param_.col_formats[idx].format.c_str(), user_format_style_[i]))
                 break;
           }
           if (i == l)
           {
              TBSYS_LOG(ERROR, "user specify error format, not support format[%s], column[%s], please check config file",
                                param_.col_formats[idx].format.c_str(), param_.col_formats[idx].name.c_str());
              ret = OB_ERROR;
              break;
           }
           col_info.idx = i;
           format_columns_.insert(ColumnCache::value_type(col_schema->get_name(), col_info));
        }
     }
  }
  /* 初始化添加双引号分隔字段,该字段只能为VARCHAR类型 */
  for (idx = 0; idx < param_.add_quotes.size() && OB_SUCCESS == ret; idx++)
  {
     const ObColumnSchemaV2 *col_schema = schema_->get_column_schema(param_.table_name.c_str(), param_.add_quotes[idx].c_str());
     if (NULL == col_schema)
     {
        TBSYS_LOG(ERROR, "can't find column[%s] in table[%s], please check config file", param_.add_quotes[idx].c_str(), param_.table_name.c_str());
        ret = OB_ERROR;
     }
     else if (col_schema->get_type() != ObVarcharType)
     {
        TBSYS_LOG(ERROR, "column[%s] is not VARCHAR in table[%s], should not add quotes", param_.add_quotes[idx].c_str(), param_.table_name.c_str());
        ret = OB_ERROR;
     }
     else
     {
        TBSYS_LOG(DEBUG, "find column[%s] in table[%s]", param_.add_quotes[idx].c_str(), param_.table_name.c_str());
        //uint64_t cid = col_schema->get_id();
        ColumnCache::iterator itr = format_columns_.find(col_schema->get_name());
        if (itr != format_columns_.end())
        {
           TBSYS_LOG(ERROR, "column[%s] mustn't add quotes, maybe formating or delete last point, please check config file", param_.add_quotes[idx].c_str());
           ret = OB_ERROR;
        }
        else
        {
           ObExportColumnInfo col_info;
           col_info.col_schema = col_schema;
           col_info.is_add_quotes = true;
           format_columns_.insert(ColumnCache::value_type(col_schema->get_name(), col_info));
        }
     }
  }
  
  if (OB_SUCCESS == ret) inited_ = true;
  //add zcd multi_consumers 20150114:b
  memset(format_flags_, 0, OB_ROW_MAX_COLUMNS_COUNT * sizeof(int));
  //add:e

  return ret;
}

//add zcd multi_consumers 20150114:b
DbRowFormator::TSIBuffer *DbRowFormator::get_tsi_buffer()
{
  TSIBuffer *tsi_buffer = GET_TSI_MULT(TSIBuffer, 0);
  if(NULL == tsi_buffer)
  {
    TBSYS_LOG(ERROR, "failed to allocate memory for thread tsi_buffer!");
  }
  else
  {
    if(NULL == tsi_buffer->get_line_buffer())
    {
      if(OB_SUCCESS != tsi_buffer->init(kLineBufferSize))
      {
        TBSYS_LOG(ERROR, "tsi_buffer init failed!");
        tsi_buffer = NULL;
      }
    }
  }
  return tsi_buffer;
}
//add:e

//add zcd multi_consumers 20150114:b
const char* DbRowFormator::get_line_buffer()
{
  char *buffer = NULL;
  TSIBuffer *tsi_buffer = get_tsi_buffer();
  if(NULL != tsi_buffer)
  {
    buffer = tsi_buffer->get_line_buffer();
  }
  else
  {
    TBSYS_LOG(ERROR, "get tsi_buffer failed!");
  }
  return buffer;
}
//add:e

//add zcd multi_consumers 20150114:b
int64_t DbRowFormator::length()
{
  int64_t pos = 0;
  TSIBuffer *tsi_buffer = get_tsi_buffer();
  if(NULL != tsi_buffer)
  {
    pos = tsi_buffer->get_pos();
  }
  else
  {
    TBSYS_LOG(ERROR, "get tsi_buffer failed!");
  }

  return pos;
}
//add:e

/* 重置DbRowFormator,用以格式化下一行 */
void DbRowFormator::reset()
{
  get_tsi_buffer()->get_pos() = 0;
  //del zcd multi_consumers 20150114:b
//  buff_pos_ = 0;
  //del:e
}

/*
 * @author liyongfeng 20141127
 * @brief DbRowFormator::format 对一行记录ObRow进行格式化
 * @param row 输入格式化的一行记录ObRow
 * @param bad 标志本次格式化是否正确,不正确bad=1,反应给上层调用,会将其写入badfile
 * @return 格式化失败,返回非OB_SUCCESS;否则,返回OB_SUCCESS,但是需要考虑格式化是否正确 
 */
int DbRowFormator::format(const ObRow &row, int32_t &bad, Charset & charset) //mod by zhuxh:20161018 [add class Charset]
{
  int ret = OB_SUCCESS;

  const ObObj *cell = NULL;
  uint64_t tid = 0;
  uint64_t cid = 0;
  int64_t cell_idx = 0;

  bad = 0;
  int32_t bad_format = 0;
  std::string cname;

 //add zcd multi_consumers 20150114:b
  TSIBuffer *tsi_buffer = get_tsi_buffer();

  char* line_buffer = tsi_buffer->get_line_buffer();
  int64_t& buff_pos = tsi_buffer->get_pos();

  if(NULL == line_buffer)
  {
    TBSYS_LOG(ERROR, "failed to allocate memory for thread tsi_buffer!");
    ret = OB_ERROR;
    return ret;
  }
  //add:e
  for (; cell_idx < row.get_column_num() && OB_SUCCESS == ret; cell_idx++)
  {
     if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx, cell, tid, cid)))
     {
        TBSYS_LOG(ERROR, "fialed to get one cell, tid=[%ld], cid=[%ld], ret=[%d]", tid, cid, ret);
        break;
     }
     else if (OB_SUCCESS != (ret = format_column(cell, (int)cell_idx, bad_format, charset))) //mod by zhuxh:20161018 [add class Charset]
     {
        TBSYS_LOG(ERROR, "failed to format one column into buffer, tid=[%ld], cid=[%ld], ret=[%d]", tid, cid, ret);
        break;
     }
     else
     {
        if (1 == bad_format)//将字段格式化成指定格式失败,记录该line_buffer_需要写入badfile
        {
           TBSYS_LOG(WARN, "failed to transform column in specified format");
           bad = 1;
        }
        if (cell_idx == row.get_column_num() - 1)//最后一个字段后面追加记录分隔符
        {
           param_.rec_delima.append_delima(line_buffer, buff_pos, kLineBufferSize);
           param_.rec_delima.skip_delima(buff_pos);
        }
        else
        {
           param_.delima.append_delima(line_buffer, buff_pos, kLineBufferSize);
           param_.delima.skip_delima(buff_pos);
        }
     }
  }
  return ret;
}
/*
 * @author liyongfeng 20141127
 * @brief DbRowFormator::format_column 对一个字段进行format格式化
 * @param cell 需要格式化字段ObObj
 * @param cid column id 列id
 * @param bad 返回参数,标志是否格式化成功,如果该字段格式化成指定格式失败,记录bad=1
 * @return 格式化不支持数据类型时,返回OB_ERROR;其余,全部返回OB_SUCCESS,但是需要查看格式化是否成功(bad)
 */
int DbRowFormator::format_column(const ObObj *cell, int cell_idx, int32_t &bad, Charset & charset) //mod by zhuxh:20161018 [add class Charset]
{
  int ret = OB_SUCCESS;
  bad = 0;//正常格式化
  
  bool is_format = false;//是否需要格式化
  const ObExportColumnInfo *export_col_info = NULL;
  if(format_flags_[cell_idx] == 1)
  {
    export_col_info = &(export_column_infos_[cell_idx]);
    is_format = true;
  }
  else
  {
    is_format = false;
  }

  TSIBuffer *tsi_buffer = get_tsi_buffer();

  char* line_buffer = tsi_buffer->get_line_buffer();
  int64_t& buff_pos = tsi_buffer->get_pos();

  if(NULL == line_buffer)
  {
    TBSYS_LOG(ERROR, "failed to allocate memory for thread tsi_buffer!");
    ret = OB_ERROR;
    return ret;
  }

  int64_t int_val = 0;
  int32_t int32_val = 0;
  float float_val = 0.0;
  double double_val = 0.0;//add dinggh 20161130 [add double type]
  ObString char_val;
  ObString tmp_char_val; //mod by zhuxh:20161018 [fix bug of varchar code conversion]
  ObString decimal_val;

  int type = cell->get_type();
  switch (type)
  {
    case ObIntType:
    {
      cell->get_int(int_val);
      databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%ld", int_val);
      break;
    }
      //add qianzm
    case ObInt32Type:
    {
      cell->get_int32(int32_val);
      databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%d", int32_val);
      break;
    }
    case ObFloatType:
    {
      cell->get_float(float_val);
      databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%f", float_val);
      break;
    }
      //add e
    //add dinggh 20161130 [add double type] b:
#if 1
    case ObDoubleType:
    {
      cell->get_double(double_val);
      databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%lf", double_val);
      break;
    }
#endif
    //add dinggh e
    case ObVarcharType:
    {
      //mod by zhuxh:20161018 [fix bug of varchar code conversion] :b
      char * to_buff = NULL; //where to put code converted string
      /*
      charset not initialized means no conversion.
      I do know it failing or not indicating whether to convert isn't a very good idea.
      But I don't want to introduce an extra parameter like "bool flag_conversion" to do the thing.
      */
      if ( charset.fail() )
      {
        cell->get_varchar(char_val);
      }
      else
      {
        cell->get_varchar(tmp_char_val);
        size_t to_buff_size = tmp_char_val.length() * 3 / 2 + 5;
        to_buff = new(std::nothrow) char[to_buff_size];
        if( ! to_buff )
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "failed to allocate memory for code conversion buffer!");
        }
        else
        {
          size_t tmp_char_val_size = static_cast<size_t>(tmp_char_val.length());
          ret = charset.code_convert( tmp_char_val.ptr(), tmp_char_val_size, to_buff, to_buff_size );
          if( OB_SUCCESS != ret )
          {
            TBSYS_LOG(ERROR, "Code conversion error.");
          }
          else
          {
            char_val.assign_ptr( to_buff, static_cast<oceanbase::common::ObString::obstr_size_t>(to_buff_size) );
            //TBSYS_LOG(INFO, "zxh: len=%d, str=[%.*s]",char_val.length(),char_val.length(),char_val.ptr());
          }
        }
      }

      if( OB_SUCCESS == ret )
      {
      //mod by zhuxh:20161018 [fix bug of varchar code conversion] :e
      if (is_format)
      {
        if (export_col_info->is_format)//进行日期形式格式化,只能格式化满足要求的字段(尽管不能全部排出异常输入)
        {
          if (char_val.length() >= 19 && char_val.length() <= 26) {
            if (0 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[0], 4, char_val.ptr(), 2, char_val.ptr() + 5, 2, char_val.ptr() + 8);
            else if (1 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[1], 4, char_val.ptr(), 2, char_val.ptr() + 5, 2, char_val.ptr() + 8);
            else if (2 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[2], 4, char_val.ptr(), 2, char_val.ptr() + 5, 2, char_val.ptr() + 8, 2, char_val.ptr() + 11, 2, char_val.ptr() + 14, 2, char_val.ptr() + 17);
          }
          else if (10 == char_val.length())
          {
            if (0 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[0], 4, char_val.ptr(), 2, char_val.ptr() + 5, 2, char_val.ptr() + 8);
            else if (1 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[1], 4, char_val.ptr(), 2, char_val.ptr() + 5, 2, char_val.ptr() + 8);
            else if (2 == export_col_info->idx)//不能格式化成YYYY-MM-DD HH:MM:SS
            {
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
              bad = 1;
            }
          }
          else if (8 == char_val.length())//不能格式化成YYYY-MM-DD HH:MM:SS
          {
            if (0 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
            else if (1 == export_col_info->idx)
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style1_[1], 4, char_val.ptr(), 2, char_val.ptr() + 4, 2, char_val.ptr() + 6);
            else if (2 == export_col_info->idx)
            {
              databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
              bad = 1;
            }
          }
          else//非法字段,不能进行日期格式化
          {
            databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
            bad = 1;
          }
        }
        else if (export_col_info->is_del_point)//去除最后一个小数点(DECIMAL存成VARCHAR导致)
        {
          if ('.' == *(char_val.ptr() + char_val.length() - 1))
          {
            databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length() - 1, char_val.ptr());
          }
          else
          {
            databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
          }
        }
        else//添加双引号分隔
        {
          //add dinggh [fix add_quotes bug] 20160909:b
          //If a varchar column is exported with double quotation marks, ones inside originally will double to escape.
          //E.G.
          //abc"""def"""""123" exports as
          //"abc""""""def""""""""""123"""
          const char * a = char_val.ptr();

          if (buff_pos < kLineBufferSize)
          {
            line_buffer[buff_pos++] = '\"';
          }
          else
          {
            ret = OB_MEM_OVERFLOW;
          }

          for (int64_t i = 0; (OB_SUCCESS == ret) && i < char_val.length(); i++)
          {
            if (buff_pos >= kLineBufferSize)
            {
              ret = OB_MEM_OVERFLOW;
              break;
            }
            //TBSYS_LOG(INFO, "zxh: sum=%d,i=%ld",char_val.length(),i);
            line_buffer[buff_pos++] = a[i];
            if (a[i] == '\"')
            {
              if (buff_pos >= kLineBufferSize)
              {
                ret = OB_MEM_OVERFLOW;
                break;
              }
              line_buffer[buff_pos++] = '\"';
            }
          }
          if (OB_SUCCESS == ret && buff_pos < kLineBufferSize)
          {
            line_buffer[buff_pos++] = '\"';
          }
          else
          {
            ret = OB_MEM_OVERFLOW;
          }

          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "Buff over flows.");
            break;
          }
          //databuff_printf(line_buffer, kLineBufferSize, buff_pos, "\"%.*s\"", char_val.length(), char_val.ptr());
          //add 20160909:e
        }
      }
      //del by zhuxh:20161018 [fix bug of varchar code conversion] :b
      /*
      //add qianzm [charset] 20160629:b
      else if (g2u_)
      {
        char outbuf[1024];
        size_t outlen=(size_t)char_val.length()*2;
        int cd = -1;
        cd = ::g2u(char_val.ptr(), size_t(char_val.length()), outbuf, outlen);
        if (cd == 0){
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", (int32_t)outlen, outbuf);
        }
        else{
          TBSYS_LOG(WARN, "charset:GBK convert to UTF-8 failed, do nothing!");
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
        }
      }
      else if (u2g_)
      {
        char outbuf[1024];
        size_t outlen=(size_t)char_val.length()*2;
        int cd = -1;
        cd = ::u2g(char_val.ptr(), size_t(char_val.length()), outbuf, outlen);
        if (cd == 0){
            databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", (int32_t)outlen, outbuf);
        }
        else{
          TBSYS_LOG(WARN, "charset:UTF-8 convert to GBK failed, do nothing!");
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
        }
      }
      //add 20160629:e
      */
      //del by zhuxh:20161018 [fix bug of varchar code conversion] :e
      else//不需要进行日期格式化
      {
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", char_val.length(), char_val.ptr());
      }
      //add by zhuxh:20161018 [fix bug of varchar code conversion] :b
      }//if( OB_SUCCESS == ret )

      if( to_buff )
      {
        delete []to_buff;
      }
      //add by zhuxh:20161018 [fix bug of varchar code conversion] :e
      break;
    }
    case ObDecimalType:
    {
      cell->get_decimal(decimal_val);
      static const int32_t BUFFER_SIZE = 64;//不清楚此处使用是否
      static __thread char res[BUFFER_SIZE];
      memset(res, 0, BUFFER_SIZE);

      ObDecimal od;
      if (OB_SUCCESS != (ret = od.from(decimal_val.ptr(), decimal_val.length())))
      {
        TBSYS_LOG(ERROR, "failed to convert decimal from buf, ret=[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = od.modify_value(cell->get_precision(), cell->get_scale())))
      {
        TBSYS_LOG(ERROR, "fialed to modify decimal, ret=[%d]", ret);
      }
      else
      {
        od.to_string(res, BUFFER_SIZE);
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", static_cast<int>(strlen(res)), res);
      }
      if (OB_SUCCESS != ret)
      {
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, "%.*s", decimal_val.length(), decimal_val.ptr());
        bad = 1;
        ret = OB_SUCCESS;
      }

      break;
    }
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    {
      cell->get_timestamp(int_val);
      struct tm time_struct;
      int64_t time_s = int_val / 1000000;
      int64_t time_us = int_val % 1000000;
      localtime_r(&time_s, &time_struct);
      if (is_format)//用户指定输出格式
      {
        if (0 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[0], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (1 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[1], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (2 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[2],
              time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday,
              time_struct.tm_hour, time_struct.tm_min, time_struct.tm_sec);

      }
      else//不格式化,选择默认格式输出
      {
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, kDefualtTimestamp,
                        time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday,
                        time_struct.tm_hour, time_struct.tm_min, time_struct.tm_sec);
        //if (time_us != 0) //delete dinggh 20160918
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, ".%06ld", time_us);
      }
      break;
    }
    case ObNullType:
    {
      break;
    }
    //add qianzm 20151231
    case ObDateType:
    {
      ObDate tmp_date=0;
      cell->get_date(tmp_date);
      struct tm time_struct;
      int64_t time_s = tmp_date / 1000000;
      localtime_r(&time_s, &time_struct);
      /*if (is_format)//用户指定输出格式
      {
        if (0 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[0], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (1 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[1], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (2 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[2],
              time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday,
              time_struct.tm_hour, time_struct.tm_min, time_struct.tm_sec);
      }
      else//不格式化,选择默认格式输出*/

      {
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, kDefualtDate,
                        time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
      }
      break;
    }
    case ObTimeType:
    {
      ObTime tmp_time;
      cell->get_time(tmp_time);
      int64_t hour = tmp_time /1000000 / 3600;
      int64_t min = tmp_time /1000000 % 3600 / 60;
      int64_t sec = tmp_time /1000000 %60;

      /*if (is_format)//用户指定输出格式
      {
        if (0 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[0], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (1 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[1], time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday);
        else if (2 == export_col_info->idx)
          databuff_printf(line_buffer, kLineBufferSize, buff_pos, sys_format_style2_[2],
              time_struct.tm_year + 1900, time_struct.tm_mon + 1, time_struct.tm_mday,
              time_struct.tm_hour, time_struct.tm_min, time_struct.tm_sec);
      }
      else//不格式化,选择默认格式输出*/

      {
        databuff_printf(line_buffer, kLineBufferSize, buff_pos, kDefualtTime,
                        hour, min, sec);
      }
      break;
    }
    //add e
    default:
    {
      TBSYS_LOG(ERROR, "ob_export not support data type[%d]", type);
      ret = OB_ERROR;
      break;
    }

  }

  return ret;
}

//del zcd export :20150113
//int DbRowFormator::find_cname_by_idx(int cell_idx, std::string &cname)
//{
//  std::map<int, std::string>::iterator iter = result_fields_.find(cell_idx);
//  if(iter == result_fields_.end())
//  {
//    TBSYS_LOG(ERROR, "find column name by index error!");
//    return OB_ERROR;
//  }
//  cname = iter->second;
//  return OB_SUCCESS;
//}
//del:e

int DbRowFormator::init_result_fields_map(const ObArray<ObResultSet::Field>& fields)
{
  //初始化result_fields_，result_fields_中存储结果集中每一列的列名和列的顺序的对应关系
  if(fields.count() > OB_ROW_MAX_COLUMNS_COUNT)
  {
    return OB_ERROR;
  }
  result_fields_.clear();
  for(int i = 0; i < fields.count(); i++)
  {
    result_fields_.insert(make_pair(i, fields.at(i).cname_.ptr()));
  }

  //初始化format_flags_数组，这个需要在format_columns_已经被初始化的情况下才能做
  //format_flags_数组与format_columns_一样也是只能初始化一次
  ColumnCache::iterator it = format_columns_.begin();
  while(it != format_columns_.end())
  {
    for(int i = 0; i < fields.count(); i++)
    {
      if(strcmp(it->first.c_str(), fields.at(i).cname_.ptr()) == 0)
      {
        format_flags_[i] = 1;
        export_column_infos_[i] = it->second;
        break;
      }
    }
    it++;
  }

  return OB_SUCCESS;
}
