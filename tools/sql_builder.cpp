#include "sql_builder.h"
#include "common/ob_obj_cast.h"
const char* IDS::LogCommandStr[1024];
const char* IDS::ActionFlagStr[1024];
const char* IDS::ObjTypeStr[ObMaxType + 1];
static int varchar_printf(ObExprObj &out, const char* format, ...)
{
  int ret = OB_SUCCESS;
  ObString varchar = out.get_varchar();
  if (NULL == varchar.ptr() || varchar.length() <= 0)
  {
    TBSYS_LOG(WARN, "output buffer for varchar not enough, buf_len=%u", varchar.length());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    va_list args;
    va_start(args, format);
    int length = vsnprintf(varchar.ptr(), varchar.length(), format, args);
    va_end(args);
    ObString varchar2(varchar.length(), length, varchar.ptr());
    out.set_varchar(varchar2);
  }
  return ret;
}
SqlBuilder::SqlBuilder()
{
    IDS::initialize();
}

const string SqlBuilder::obj_tostring(const ObObj &obj) const
{
    string sql="";
    if(obj.get_type() == ObVarcharType && obj.get_data_length() > 100)
    {
        ObString ob_temp_str;
        obj.get_varchar(ob_temp_str);
        int64_t buff_len = ob_temp_str.length() + 2 + 1;
        char buffer[buff_len];
        int64_t pos = 0;
        databuff_printf(buffer, buff_len, pos, "%.*s", ob_temp_str.length(), ob_temp_str.ptr());
        buffer[buff_len - 1] = '\0';
        sql += "varchar:" + string(buffer);
    }
    else if(obj.get_type() == ObDateTimeType ||
            obj.get_type() == ObDateType ||
            obj.get_type() == ObTimeType ||
            obj.get_type() == ObPreciseDateTimeType ||
            obj.get_type() == ObCreateTimeType ||
            obj.get_type() == ObModifyTimeType)
    {

        int64_t timestamp = 0;
        obj.get_timestamp(timestamp);

        time_t t = static_cast<time_t>(timestamp/1000000L);
        //mod liuzy [datetime bug] 20150909:b
        //Exp: usec before 1970-01-01 08:00:00 is negative, change it to positive, when we output it
        //int64_t usec = static_cast<int32_t>(timestamp % 1000000L);
        int32_t usec = abs(static_cast<int32_t>(timestamp % 1000000L));
        if (0 > timestamp && 0 != usec)
        {
            t = t - 1;
            usec = 1000000 - usec;
        }
        //add peiouya [USEC_BUG_FIX] 20161031:e
        ObExprObj out;
        char buf[MAX_PRINTABLE_SIZE];
        memset(buf, 0, MAX_PRINTABLE_SIZE);
        ObString os;
        os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
        ObObj tmp_obj;
        tmp_obj.set_varchar(os);
        out.assign(tmp_obj);

        struct tm gtm;
        localtime_r(&t, &gtm);
        if(obj.get_type() == ObDateType)
        {
            varchar_printf(out, "%04d-%02d-%02d", gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday);
        }
        else if(obj.get_type() == ObTimeType)
        {
            int hour = 0;
            int min = 0;
            int sec = 0;
            //localtime_r(&t, &gtm);
            hour = static_cast<int32_t>(t / 3600);
            min  = static_cast<int32_t>(t % 3600 / 60);
            sec  = static_cast<int32_t>(t % 60);
            varchar_printf(out, "%02d:%02d:%02d", hour, min, sec);
        }
        else
        {
            varchar_printf(out, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                                 gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday,
                                 gtm.tm_hour, gtm.tm_min, gtm.tm_sec, usec);
        }
        ObString val = out.get_varchar();
        string str(val.ptr(), val.length());

        if(obj.get_type() == ObDateTimeType)
            sql += "datetime:'" + str + "'";
        else if(obj.get_type() == ObDateType)
            sql += "date:'" + str + "'";
        else if(obj.get_type() == ObTimeType)
            sql += "time:'" + str + "'";
        else if(obj.get_type() == ObPreciseDateTimeType)
            sql += "precisedatetime:'" + str + "'";
        else if(obj.get_type() == ObCreateTimeType)
            sql += "precisedatetime:'" + str + "'";
        else if(obj.get_type() == ObModifyTimeType)
            sql += "precisedatetime:'" + str + "'";
    }
    else
    {
        sql += string(to_cstring(obj));
    }
    return sql;
}

const string SqlBuilder::rowkey_tostring(const ObRowkey &obj) const
{
  string buffer="";
  int64_t pos = 0;

  if (obj.is_min_row())
  {
    if (pos < obj.length())
    {
      buffer+="MIN";
    }
  }
  else if (obj.is_max_row())
  {
    if (pos < obj.length())
    {
      buffer+="MAX";
    }
  }
  else
  {
    const ObObj* obj_ptr_=obj.get_obj_ptr();
    int64_t obj_cnt_=obj.get_obj_cnt();
    for (int i = 0; i < obj_cnt_ ; ++i)
    {
        buffer += obj_tostring(obj_ptr_[i]);
        if (i < obj_cnt_ - 1)
        {
            buffer += char(15);
        }

    }
  }
  return buffer;
}
int SqlBuilder::ups_mutator_to_sql(uint64_t seq, ObUpsMutator &mut, vector<string> &vector, CommonSchemaManagerWrapper *schema_manager)
{
    int ret = OB_SUCCESS;
    ObMutatorCellInfo* cell = NULL;
    int cell_idx = 0;

    string mutator_op = "OB Operation";

    //遍历所有的MutatorCell
    while (OB_SUCCESS == (ret = mut.next_cell()))
    {
      ret = mut.get_cell(&cell);
      string sql;
      if (OB_SUCCESS != ret || NULL == cell)
      {
        TBSYS_LOG(ERROR, "get cell error\n");
        continue;
        //fprintf(stderr, "get cell error\n");
      }
      else
      {
        // figure out freeze/drop/normal operation of UpsMutator
        if (0 == cell_idx)
        {
          if (mut.is_freeze_memtable())
          {
            //Freeze Operation
            TBSYS_LOG(WARN, "Freeze Operation\n");
            continue;
          }
          else if (mut.is_drop_memtable())
          {
            //Drop Operation
            TBSYS_LOG(WARN, "Drop Operation\n");
            continue;
          }
          else if (ObExtendType == cell->cell_info.value_.get_type())
          {
            if (ObActionFlag::OP_USE_OB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "OB Operation";
              TBSYS_LOG(WARN, "OB Operation\n");
              continue;
            }
            else if (ObActionFlag::OP_USE_DB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "DB Operation";
              TBSYS_LOG(WARN, "DB Operation\n");
              continue;
            }
          }
        }
        if (schema_manager != NULL && schema_manager->get_version() > 0)
        {
            string iu_op = "";
            if(ObExtendType == cell->cell_info.value_.get_type())
            {
                iu_op=IDS::str_action_flag(cell->cell_info.value_.get_ext());
            }
            else
            {
                iu_op=IDS::str_action_flag(cell->op_type.get_ext());
            }
            const ObTableSchema* ts = schema_manager->get_impl()->get_table_schema(cell->cell_info.table_id_);
            const ObColumnSchemaV2* cs = schema_manager->get_impl()->get_column_schema(cell->cell_info.table_id_,cell->cell_info.column_id_);
            //[2016.8.4] add:Yukun schema not match,table or column not find
            //DEL_ROW and other has no column info, update/insert/replace must has ts and cs
            if( NULL == ts )
            {
                ret = OB_SCHEMA_ERROR;
                TBSYS_LOG(WARN,"ObTableSchema null error[table null],SEQ:%lu op:%s table_id=%lu table_column=%lu\n",seq,iu_op.c_str(), cell->cell_info.table_id_,cell->cell_info.column_id_);
                //exit(0);
                return ret;
            }
            //filter system table, only accept UPDATE/INSERT/DEL_ROW
            else if(IS_SYS_TABLE_ID(cell->cell_info.table_id_))
            {
                continue;
            }
            else if (iu_op == "UNKNOWN")
            {
                continue;
            }
            //filter index_table
            else if(OB_INVALID_ID != ts->get_index_helper().tbl_tid)
            {
                continue;
            }
            else if (NULL==cs && iu_op=="UPDATE")
            {
                    ret = OB_SCHEMA_ERROR;
                    TBSYS_LOG(WARN,"ObTableSchema null error[column null],SEQ:%lu op:%s table_id=%lu table_column=%lu\n",seq,iu_op.c_str(), cell->cell_info.table_id_,cell->cell_info.column_id_);
                    //exit(0);
                    return ret;
            }

            //[2016.8.4] end:
            const char* table_name  = NULL==ts ? "NoTable":ts->get_table_name();
            const char* column_name = NULL==cs ? "NoColumn":cs->get_name();

            sql = "op_type->" + iu_op + " table->" + string(table_name) + " column->" + string(column_name) + " row_key->" + rowkey_tostring(cell->cell_info.row_key_);

            sql += " row_key_info->";
            for (int64_t i = 0; i < ts->get_rowkey_info().get_size(); i++)
            {
               const ObRowkeyColumn* rowKey = ts->get_rowkey_info().get_column(i);

               const ObColumnSchemaV2* temp_cs = schema_manager->get_impl()->get_column_schema(cell->cell_info.table_id_, rowKey->column_id_);

               if(temp_cs == NULL)
               {
                   sql += "error_rowkey";
                   ret = OB_SCHEMA_ERROR;
                   TBSYS_LOG(WARN,"ObTableSchema error rowkey not found,SEQ:%lu op:%s table_id=%lu table_rowkey=%lu\n",seq,iu_op.c_str(), cell->cell_info.table_id_,rowKey->column_id_);
                   //exit(0);
                   return ret;
               }
               else
               {
                   sql += to_cstring(ob_obj_type_str(rowKey->type_));
                   sql += ":";
                   sql += to_cstring(temp_cs->get_name());
               }

               if(i != ts->get_rowkey_info().get_size() -1)
               {
                   sql += char(15);
               }
            }
            sql += " value->" + obj_tostring(cell->cell_info.value_);
            vector.push_back(sql);
        }
        else
        {
           ret = OB_SCHEMA_ERROR;
           TBSYS_LOG(WARN,"Empty schema,seq=%lu table_id=%lu column_id=%lu\n", seq, cell->cell_info.table_id_,cell->cell_info.column_id_);
           return ret;
        }
      }
      ++cell_idx;
    }
    if (ret == OB_ITER_END)
    {
        ret = OB_SUCCESS;
    }
    return ret;
}


