#include <iostream>
#include <stdio.h>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include "tblog.h"
#include "data_syntax.h"
#include "common/utility.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "common/ob_range2.h"
#include "ob_databuilder.h"
#include "common/ob_compact_store_type.h"
#include "compactsstablev2/ob_sstable_store_struct.h"

#include <vector>
#include <string>

const char *g_sstable_directory = "./";

namespace oceanbase 
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::compactsstablev2;

    char DELIMETER = '\0';
    int32_t RAW_DATA_FILED = 0;
  
    static struct row_key_format *ROW_KEY_ENTRY;
    static struct data_format *ENTRY;

    static int32_t ROW_KEY_ENTRIES_NUM = 0;
    static int32_t DATA_ENTRIES_NUM = 0;

    static const char *COLUMN_INFO="column_info";

    static struct data_format DATA_ENTRY[MAX_COLUMS];

    int drop_esc_char(char *buf,int32_t& len)
    {
      int ret = OB_SUCCESS;
      int32_t orig_len = len;
      char *dest = NULL;
      char *ptr = NULL;
      int32_t final_len = len;
      if (NULL == buf)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        dest = buf;
        ptr = buf;
        for(int32_t i=0;i<orig_len-1;++i)
        {
          if ('\\' == *ptr)
          {
            switch(*(ptr+1))
            {
              case '\"':
                *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\'':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\\':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              default:
              {
                if (dest != ptr)
                  *dest = *ptr;
                ++dest;
                ++ptr;
              }
              break;
            }
          }
          else
          {
            if (dest != ptr)
              *dest = *ptr;
            ++dest;
            ++ptr;
          }
        }
        len = final_len;
      }
      return ret;
    }
    
    int fill_sstable_schema(const ObSchemaManagerV2* schema,
                            uint64_t table_id,
                            ObSSTableSchema* sstable_schema,
                            ObRowDesc* row_desc)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;
      int cols = 0;
      int32_t size = 0;
      sstable_schema->reset();

      const ObColumnSchemaV2 *col = schema->get_table_schema(table_id,size);
      const ObTableSchema *table_schema = schema->get_table_schema(table_id);

      if (NULL == col || NULL == table_schema || size <= 0)
      {
        TBSYS_LOG(ERROR,"can't find this table:%lu",table_id);
        ret = OB_ERROR;
      }
      else
      {
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        row_desc->reset();

        const ObRowkeyColumn* column = NULL;
        for (int col_index = 0; col_index < rowkey_info.get_size() && OB_SUCCESS == ret; ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));          
          column = rowkey_info.get_column(col_index);
          if (NULL == column) 
          {
            TBSYS_LOG(ERROR, "schema internal error, rowkey column %d not exist.", col_index);
            ret = OB_ERROR;
            break;
          }

          column_def.table_id_ = static_cast<uint32_t>(table_id);
          column_def.column_id_ = static_cast<uint16_t>(column->column_id_);
          column_def.column_value_type_ = column->type_;
          column_def.rowkey_seq_ = static_cast<uint16_t>(col_index + 1);

          if ( OB_SUCCESS != (ret = sstable_schema->add_column_def(column_def)) )
          {
            TBSYS_LOG(ERROR,"can not add column_def to sstable_schema [%d],table_id[%lu],column_id[%lu]",ret,column_def.table_id_,column_def.column_id_);            
          }
          else
          {
            row_desc->add_column_desc(table_id,column_def.column_id_);            
          }
        }

        for(int col_index = 0; (col_index < size) && (OB_SUCCESS == ret); ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));
          column_def.table_id_ = static_cast<uint32_t>(table_id);
          column_def.column_id_ = static_cast<uint32_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
          column_def.offset_ = 0;
          column_def.rowkey_seq_ = 0;
          TBSYS_LOG(INFO,"add column table_id[%lu],column_id[%lu]",column_def.table_id_,column_def.column_id_);
          if (rowkey_info.is_rowkey_column(column_def.column_id_))
          {
            //do nothing
            continue;
          }

          if ((OB_SUCCESS == ret) && ((ret = sstable_schema->add_column_def(column_def)) != OB_SUCCESS))
          {
            TBSYS_LOG(ERROR,"can not add column_def to sstable_schema [%d],table_id[%lu],column_id[%lu]",ret,column_def.table_id_,column_def.column_id_);
          }
          else
          {
            row_desc->add_column_desc(table_id,column_def.column_id_);
          }
          ++cols;
        }
        
        if (OB_SUCCESS == ret)
        {
          row_desc->set_rowkey_cell_count(rowkey_info.get_size());
        }
      }

      if ( 0 == cols ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      return ret;
    }

    int parse_data_syntax(const char *syntax_file,uint64_t table_id,const ObSchemaManagerV2* schema)
    {
      int ret = OB_SUCCESS;
      tbsys::CConfig c1;
      char table_section[20];

      snprintf(table_section,sizeof(table_section),"%ld",table_id);

      if (NULL == syntax_file || '\0' == syntax_file || NULL == schema)
      {
        TBSYS_LOG(ERROR,"syntax_file is null");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && c1.load(syntax_file) != 0)
      {
        TBSYS_LOG(ERROR,"load syntax file [%s],falied",syntax_file);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        vector<const char *> column_info = c1.getStringList(table_section,COLUMN_INFO);
        if (column_info.empty())
        {
          TBSYS_LOG(ERROR,"load column info failed");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          int i = 0;
          int d[2];
          int l = 2;
          //const ObColumnSchemaV2 *column_schema = NULL;
          int32_t column_index[OB_MAX_COLUMN_GROUP_NUMBER];
          int32_t size = OB_MAX_COLUMN_GROUP_NUMBER;
          for(vector<const char *>::iterator it = column_info.begin(); it != column_info.end();++it)
          {
            if ((ret = parse_string_to_int_array(*it,',',d,l)) != OB_SUCCESS || l != 2)
            {
              TBSYS_LOG(ERROR,"deal column info failed [%s]",*it);
              break;
            }
            DATA_ENTRY[i].column_id= d[0];
            DATA_ENTRY[i].index = d[1];
            if (0 != DATA_ENTRY[i].column_id)
            {
              //column_schema = table_schema->find_column_info(DATA_ENTRY[i].column_id);
              ret = schema->get_column_index(table_id,static_cast<uint64_t>(DATA_ENTRY[i].column_id),column_index,size);
              if (ret != OB_SUCCESS || size <= 0)
              {
                TBSYS_LOG(ERROR,"find column info from schema failed : %d,ret:%d,size,%d",d[0],ret,size);
                ret = OB_ERROR;
                break;
              }
              DATA_ENTRY[i].type = schema->get_column_schema(column_index[0])->get_type();
              DATA_ENTRY[i].len = static_cast<int32_t>(schema->get_column_schema(column_index[0])->get_size());
            }
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"data entry [%d], id:%2d,index:%2d,type:%2d,len:%2d",i,DATA_ENTRY[i].column_id,
                DATA_ENTRY[i].index,DATA_ENTRY[i].type, DATA_ENTRY[i].len);
#endif
            ++i;
          }

          if (OB_SUCCESS == ret)
          {
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"data entry num: [%d]",i);
#endif
            DATA_ENTRIES_NUM = i;
            ENTRY = DATA_ENTRY;
          }
        }
      }
      return ret;
    }

    ObDataBuilder::ObDataBuilder():fp_(NULL),
                                   file_no_(-1),
                                   first_key_(true),
                                   reserve_ids_(-1),
                                   current_sstable_id_(SSTABLE_ID_BASE),
                                   row_key_buf_index_(0),
                                   wrote_keys_(0),
                                   read_lines_(0),
                                   table_id_(0),
                                   total_rows_(0),
                                   table_schema_(NULL),
                                   sstable_schema_(NULL)
    {
    }
        
    ObDataBuilder::~ObDataBuilder()
    {
      if (NULL != sstable_schema_)
      {
        delete sstable_schema_;
        sstable_schema_ = NULL;
      }

      if (NULL != row_desc_)
      {
        delete row_desc_;
        row_desc_ = NULL;
      }
    }

    int ObDataBuilder::init(ObDataBuilderArgs& args)
    {
      int ret = OB_SUCCESS;
      fp_ = NULL;
      file_no_ = args.file_no_;
      reserve_ids_ = args.reserve_ids_;
      max_sstable_size_ = args.max_sstable_size_;

      current_sstable_id_ = SSTABLE_ID_BASE;
      row_key_buf_index_ = 0;
      wrote_keys_ = 0;
      read_lines_ = 0;
      
      table_id_ = args.table_id_;
      schema_ = args.schema_;
      table_schema_ = args.schema_->get_table_schema(table_id_);

      sstable_schema_ = new ObSSTableSchema();
      row_desc_ = new ObRowDesc();

      if (NULL == sstable_schema_)
      {
        TBSYS_LOG(ERROR,"alloc sstable schema failed");
        ret = OB_ERROR;
      }
      else if ((ret = fill_sstable_schema(schema_,table_id_,sstable_schema_,row_desc_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"convert schema failed [%d]",ret);
      }
      else
      {
        split_pos_ = table_schema_->get_split_pos();

        disk_no_ = args.disk_no_;

        strncpy(filename_,args.filename_,strlen(args.filename_));
        filename_[strlen(args.filename_)] = '\0';

        strncpy(dest_dir_,args.dest_dir_,strlen(args.dest_dir_));
        dest_dir_[strlen(args.dest_dir_)] = '\0';

        snprintf(index_file_path_,sizeof(index_file_path_),"%s/%d.idx_%d",dest_dir_,file_no_,disk_no_);

        compressor_string_.assign((char*)args.compressor_string_,static_cast<int32_t>(strlen(args.compressor_string_)));

        memset(row_key_buf_[0],0,sizeof(row_key_buf_[0]));
        memset(row_key_buf_[1],0,sizeof(row_key_buf_[1]));
        memset(last_end_key_buf_,0,sizeof(last_end_key_buf_));

        if ((ret = parse_data_syntax(args.syntax_file_,table_id_,schema_)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"parse_data_syntax failed : [%d]",ret);
        }

        image_.set_data_version(1);
      }

      return ret;
    }

    int ObDataBuilder::start_builder()
    {
      return build_with_no_cache();
    }

    int ObDataBuilder::build_with_no_cache()
    {
      int fields = 0;
      int ret = 0;
      if ((NULL == fp_) && ( NULL == (fp_ = fopen(filename_,"r"))))
      {
        TBSYS_LOG(ERROR,"open file error:%s\n",strerror(errno));
        ret = OB_ERROR;
      }
      else 
      {
        ObFrozenMinorVersionRange version_range;
        ObNewRange table_range_;

        version_range.major_version_ = 1;
        version_range.is_final_minor_version_ = 1;
        table_range_.set_whole_range();
        prepare_new_sstable_name();

        if ( (OB_SUCCESS != (ret = writer_.set_sstable_param(version_range,
                                                           DENSE_DENSE,
                                                           1L,
                                                           65536L,
                                                           compressor_string_,
                                                           max_sstable_size_,
                                                           1L))) ||
             (OB_SUCCESS != (ret = writer_.set_table_info(table_id_,*sstable_schema_,table_range_))) ||
             (OB_SUCCESS != (ret = writer_.set_sstable_filepath(dest_file_string_))))
        {
          TBSYS_LOG(ERROR,"create sstable failed [%d]\n",ret);
        }
        else
        {
          bool is_split = false;
          while((OB_SUCCESS == ret) && read_line(fields) != NULL)
          {
            process_line(fields);

            if (writer_.append_row(sstable_row_,is_split) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"append_row failed:%ld\n",read_lines_);
            }
            else
            {
              ++wrote_keys_;
              ++total_rows_;

              if (is_split)
              {
                record_sstable_range();
                prepare_new_sstable_name(); //get new sstable name
                if ((OB_SUCCESS != (ret = writer_.set_sstable_filepath(dest_file_string_))))
                {
                  TBSYS_LOG(ERROR,"set sstable filepath failed");
                }
                wrote_keys_ = 0;
              }
            }
          }
        }

        fclose(fp_);

        if (OB_SUCCESS == ret)
        {
          if (wrote_keys_ > 0)
          {
            writer_.finish();          
            record_sstable_range();
          }
          else
          {
            //don't write index
            writer_.finish();
          }
          //ok,now we write meta info to the index file
          ret = write_idx_info();
        }
      }
      TBSYS_LOG(INFO,"total_rows:%ld\n",total_rows_);
      return ret;
    }

    char *ObDataBuilder::read_line(int &fields)
    {
      char *line = NULL;
      static int buf_idx = 0;
      while(true)
      {
        if ( (line = fgets(buf_[buf_idx%2],sizeof(buf_[buf_idx%2]),fp_)) != NULL)
        {
          //++read_lines_;
          char *phead = buf_[buf_idx%2];
          char *ptail = phead;
          int i = 0;
          while (*ptail != '\n')
          {
            while( (*ptail != DELIMETER) && (*ptail != '\n'))
              ++ptail;
            colums_[i].column_ = phead;
            colums_[i++].len_ = static_cast<int32_t>(ptail - phead);
            if ('\n' == *ptail)
            {
              *ptail= '\0';
              break;
            }
            else
            {
              *ptail++ = '\0';
              //++ptail;
            }
            phead = ptail;
          }

          if ( '\n' == *ptail && '\0' == *(ptail - 1))
          {
            colums_[i++].len_ = 0;
          }
          
          //colums_[i] = NULL;
          fields = i;

          //check 
          if (RAW_DATA_FILED != 0 && fields != RAW_DATA_FILED)
          {
            TBSYS_LOG(ERROR,"%ld : raw data expect %d fields,but %d",read_lines_,RAW_DATA_FILED,fields);
            continue;
          }

          ++buf_idx;

          // int64_t row_key_len = 0;

          // for(int j=0;j<i;++j)
          // {
          //   if ( 0 == ENTRY[j].column_id)
          //   {
          //     row_key_len += strlen(colums_[j].column_) + 1;
          //   }
          //   else
          //   {
          //     break; //row_key always at the begin
          //   }
          // }

          // if (0 == memcmp(buf_[0],buf_[1],row_key_len)) 
          // {
          //   TBSYS_LOG(DEBUG,"dont deal the same line,%ld,%d,%ld\n",read_lines_,buf_idx,row_key_len);
          //   continue;
          // }
        }

#ifdef BUILDER_DEBUG
        if (line != NULL)
        {
          TBSYS_LOG(DEBUG,"fields : %d",fields);
          for(int i=0;i<fields;++i)
          {
            TBSYS_LOG(DEBUG,"%d : type:%d,val: %s",i,ENTRY[i].type,colums_[i].column_);
          }
        }
#endif
        break;
      }
      return line;
    }
    
    int ObDataBuilder::process_line(int fields)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      int j = 0;

      if (fields <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
//     create_rowkey(row_key_buf_[row_key_buf_index_++ % 2],ROW_KEY_BUF_LEN,pos);

        for(;i < DATA_ENTRIES_NUM; ++i)
        {
          if (0 == ENTRY[i].column_id) //row key
          {
            continue;
          }

          if (-1 == ENTRY[i].index) //new data ,add a null obj
          {
            row_value_[j++].set_null();
          }
          else if ( ENTRY[i].index >= fields)
          {
            TBSYS_LOG(ERROR,"data format error?");
          }
          else
          {
            switch(ENTRY[i].type)
            {
              case ObIntType:
                {
                  char *p = colums_[ENTRY[i].index].column_;
                  int64_t v = 0;
                  if (p != NULL)
                  {
                    if (strchr(p,'.') != NULL) //float/double to int
                    {
                      double a = atof(p);
                      a *= 100.0;
                      v = static_cast<int64_t>(a);
                    }
                    else
                    {
                      v = atol(colums_[ENTRY[i].index].column_);
                    }
                  }
                  row_value_[j++].set_int(v);
                }
                break;
              case ObFloatType:
                row_value_[j++].set_float(strtof(colums_[ENTRY[i].index].column_,NULL));
                break;
              case ObDoubleType:
                row_value_[j++].set_double(atof(colums_[ENTRY[i].index].column_));
                break;
              case ObDateTimeType:
                row_value_[j++].set_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_));
                break;
              case ObModifyTimeType:
                row_value_[j++].set_modifytime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
                break;
              case ObCreateTimeType:
                row_value_[j++].set_createtime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L);
                break;
              case ObVarcharType:
                {
                  ObString bstring;
                  if ( colums_[ENTRY[i].index].len_ > 0)
                  {
                    int32_t len = colums_[ENTRY[i].index].len_;
                    char *obuf = colums_[ENTRY[i].index].column_;
                    drop_esc_char(obuf,len);
                    bstring.assign(obuf,len);
                  }
                  row_value_[j++].set_varchar(bstring);
                }
                break;
              case ObPreciseDateTimeType:
                row_value_[j++].set_precise_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
                break;
              default:
                TBSYS_LOG(DEBUG,"unexpect type index: %d,type:%d",i,ENTRY[i].type);
                continue;
                break;
            }
          }
        }
      }
#ifdef BUILDER_DEBUG
      common::hex_dump(row_key_.ptr(),row_key_.length(),true);
      for(int k=0;k<j;++k)
      {
        TBSYS_LOG(DEBUG,"%d,type:%d",k,row_value_[k].get_type());
        row_value_[k].dump(TBSYS_LOG_LEVEL_INFO);
      }
#endif
      sstable_row_.reset(false,ObRow::DEFAULT_NULL);
      sstable_row_.set_row_desc(*row_desc_);
      
      for(int k=0;k<j;++k)
      {
        if ( (ret = sstable_row_.set_cell(table_id_,ENTRY[k].column_id,row_value_[k])) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"add_obj failed:%d\n",ret);
        }
      }
      return ret;
    }

    int ObDataBuilder::prepare_new_sstable_name()
    {
      if (SSTABLE_ID_BASE == current_sstable_id_)
      {
        current_sstable_id_ += (file_no_ * reserve_ids_) + 1;
      }
      else 
      {
        ++current_sstable_id_;
      }

      uint64_t real_id = (current_sstable_id_ << 8)|((disk_no_ & 0xff));
      id_.sstable_file_id_ = real_id;
      id_.sstable_file_offset_ = 0;
      
      snprintf(dest_file_,sizeof(dest_file_),"%s/%ld",dest_dir_,real_id);
      dest_file_string_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));

      return OB_SUCCESS;
    }

    int ObDataBuilder::record_sstable_range()
    {
      int ret = OB_SUCCESS;

      ObTablet *tablet = NULL;
      const ObNewRange *range = NULL;

      if ((ret = writer_.get_table_range(range,0)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"get table_range failed [%d]",ret);
      }
      else if (((ret = image_.alloc_tablet_object(*range,tablet)) != OB_SUCCESS) || NULL == tablet)
      {
        TBSYS_LOG(ERROR,"alloc_tablet_object failed [%d]",ret);
      }

      if (OB_SUCCESS == ret && tablet != NULL)
      {
        tablet->set_range(*range);
        tablet->set_data_version(1);
        tablet->add_sstable_by_id(id_);
        tablet->set_disk_no(disk_no_);
      }

      if (OB_SUCCESS == ret && ((ret = image_.add_tablet(tablet)) != OB_SUCCESS))
      {
        TBSYS_LOG(ERROR,"add tablet failed [%d]",ret);
      }

      return ret;
    }

    int ObDataBuilder::write_idx_info()
    {
      int ret = OB_SUCCESS;

      if ((ret = image_.write(index_file_path_,0)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"write meta info failed");
      }
      return ret;
    }

    ObDateTime ObDataBuilder::transform_date_to_time(const char *str)
    {
      int err = OB_SUCCESS;
      struct tm time;
      ObDateTime t = 0;
      time_t tmp_time = 0;
      if (NULL != str && *str != '\0')
      {
        if (strchr(str, '-') != NULL)
        {
          if (strchr(str, ':') != NULL)
          {
            if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else
          {
            if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        else
        {
          if (strchr(str, ':') != NULL)
          {
            if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else if (strlen(str) > 8)
          {
            if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else
          {
            if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR,"sscanf failed : [%s] ",str);
          TBSYS_LOG(ERROR,"line,%ld\n",read_lines_);
          t = atol(str);
        }
        else
        {
          time.tm_year -= 1900;
          time.tm_mon -= 1;
          time.tm_isdst = -1;

          if ((tmp_time = mktime(&time)) != -1)
          {
            t = tmp_time;
          }
        }
      }
      return t;
    }

    int ObDataBuilder::create_rowkey(char *buf,int64_t buf_len,int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf || buf_len < 0 || pos < 0)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for(int32_t i=0; i < ROW_KEY_ENTRIES_NUM; ++i)
        {
          switch(ROW_KEY_ENTRY[i].type)
          {
            case INT8:
              {
                int val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i8(buf,buf_len,pos,static_cast<int8_t>(val));
                serialize_size_ += 1;
              }
              break;
            case INT16:
              {
                int val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i16(buf,buf_len,pos,static_cast<int16_t>(val));
                serialize_size_ += 2;
              }
              break;
            case INT32:
              {
                int32_t val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i32(buf,buf_len,pos,val);
                serialize_size_ += 4;
              }
              break;
            case INT64:
              {
                int64_t val = atol(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i64(buf,buf_len,pos,val);
                serialize_size_ += 8;
              }
              break;
            case VARCHAR:
              {
                memcpy(buf + pos,colums_[ROW_KEY_ENTRY[i].index].column_,colums_[ROW_KEY_ENTRY[i].index].len_);
                pos += colums_[ROW_KEY_ENTRY[i].index].len_;
                if (ROW_KEY_ENTRY[i].flag != 0)
                {
                  *(buf + pos) = '\0';
                  ++pos;
                }
                serialize_size_ += colums_[ROW_KEY_ENTRY[i].index].len_;
              }
              break;
            case DATETIME:
              {
                int64_t val = 1000L * 1000L * transform_date_to_time(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i64(buf,buf_len,pos,val);
                serialize_size_ += 8;
              }
              break;
            default:
              break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_key_.assign(buf,static_cast<int32_t>(pos));
        if (first_key_)
        {
          memcpy(last_end_key_buf_,buf,pos);
          if (split_pos_ > 0 && pos > split_pos_)
            memset(last_end_key_buf_ + split_pos_,0xff,pos - split_pos_);
          last_end_key_.assign_ptr(last_end_key_buf_,static_cast<int32_t>(pos));
          first_key_ = false;
        }
      }
      return ret;
    }

    int ObDataBuilder::create_and_append_rowkey(const char *fields,int index,char *buf,int64_t buf_len,int64_t& pos)
    {
      int ret = 0;
      switch(ROW_KEY_ENTRY[index].type)
      {
        case INT8:
          {
            int val = atoi(fields);
            ret = serialization::encode_i8(buf,buf_len,pos,static_cast<int8_t>(val));
          }
          break;
        case INT16:
          {
            int val = atoi(fields);
            ret = serialization::encode_i16(buf,buf_len,pos,static_cast<int16_t>(val));
          }
          break;
        case INT32:
          {
            int32_t val = atoi(fields);
            ret = serialization::encode_i32(buf,buf_len,pos,val);
          }
          break;
        case INT64:
          {
            int64_t val = atol(fields);
            ret = serialization::encode_i64(buf,buf_len,pos,val);
          }
          break;
        case VARCHAR:
          //TODO
          break;
        default:
          break;
      }
      return ret;
    }
  }
}


using namespace oceanbase;
using namespace oceanbase::chunkserver;

void usage(const char *program_name)
{
  printf("Usage: %s  -s schema_file\n"
                    "\t\t-t table_id\n"
                    "\t\t-f data_file\n"
                    "\t\t-d dest_path\n"
                    "\t\t-i disk_no (default is 1)\n"
                    "\t\t-b sstable id will start from this id(default is 1)\n"
                    "\t\t-l delimeter(ascii value)\n"
                    "\t\t-x syntax file\n"
                    "\t\t-r number of fields in raw data\n"
                    "\t\t-z name of compressor library\n"
                    "\t\t-S maximum sstable size(in Bytes, default is %ld)\n"
                    "\t\t-n maximum sstable number for one data file(default is %ld)\n"
                    ,program_name,
                    ObDataBuilder::DEFAULT_MAX_SSTABLE_SIZE,
                    ObDataBuilder::DEFAULT_MAX_SSTABLE_NUMBER);
  exit(1);
}

int main(int argc, char *argv[])
{
  const char *schema_file = NULL;
  int ret = OB_SUCCESS;
  ObDataBuilder::ObDataBuilderArgs args;

  while((ret = getopt(argc,argv,"s:t:f:d:i:b:x:l:r:z:S:n:")) != -1)
  {
    switch(ret)
    {
      case 's':
        schema_file = optarg;
        break;
      case 't':
        args.table_id_ = atoi(optarg);
        break;
      case 'f':
        args.filename_ = optarg;
        break;
      case 'd':
        args.dest_dir_ = optarg;
        break;
      case 'i':
        args.disk_no_ = atoi(optarg);
        break;
      case 'b':
        args.file_no_ = atoi(optarg);
        break;
      case 'x':
        args.syntax_file_ = optarg;
        break;
      case 'l':
        DELIMETER  = static_cast<char>(atoi(optarg));
        break;
      case 'r':
        RAW_DATA_FILED = atoi(optarg);
        break;
      case 'z':
        args.compressor_string_ = optarg;
        break;
      case 'S':
        args.max_sstable_size_ = atol(optarg);
        break;
      case 'n':
        args.reserve_ids_ = static_cast<int32_t>(atol(optarg));
        break;
      default:
        fprintf(stderr,"%s is not identified",optarg);
        break;
    }
  }

  if (NULL == schema_file
      || args.table_id_ <= 1000 
      || NULL == args.filename_ 
      || NULL == args.dest_dir_ 
      || args.disk_no_ < 0 
      || args.file_no_ < 0
      || NULL == args.syntax_file_
      || NULL == args.compressor_string_)
  {
    usage(argv[0]);
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  tbsys::CConfig c1;
  ObSchemaManagerV2  *mm = new (std::nothrow)ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  if ( !mm->parse_from_file(schema_file, c1) )
  {
    TBSYS_LOG(ERROR,"parse schema failed.\n");
    exit(1);
  }

  TBSYS_LOGGER.setLogLevel("INFO");

  const ObTableSchema *table_schema = mm->get_table_schema(args.table_id_);

  if (NULL == table_schema)
  {
    TBSYS_LOG(ERROR,"table schema is null");
    exit(1);
  }
  args.schema_ = mm;

  ObDataBuilder *data_builder = new ObDataBuilder();

  fprintf(stderr,"sizeof databuilder : %lu\n",sizeof(ObDataBuilder));
 
  if (NULL == data_builder)
  {
    TBSYS_LOG(ERROR,"new databulder failed");
    exit(1);
  }
  
  if (data_builder->init(args) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR,"init failed");
  }
  else
  {
    int ret = data_builder->start_builder();
    if (OB_SUCCESS != ret)
    {
      exit(ret);
    }
  }

  if (mm != NULL)
  {
    delete mm;
    mm = NULL;
  }

  if (data_builder != NULL)
  {
    delete data_builder;
    data_builder = NULL;
  }
  return 0;
}
