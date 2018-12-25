/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * gen_data_test.cpp is for what ...
 *
 * Version: ***: gen_data_test.cpp  Thu Apr 14 16:20:53 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want
 *
 */
#include <getopt.h>
#include "gen_data_test.h"
#include "tbsys.h"
#include "sstable/ob_disk_path.h"

char* g_sstable_directory = NULL;

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;
    static char item_type_value[62] = {'0','1','2','3','4','5','6','7','8','9',
                                       'A','B','C','D','E','F','G','H','I','J','K','L','M','N',
                                       'O','P','Q','R','S','T','U','V','W','X','Y','Z',
                                       'a','b','c','d','e','f','g','h','i','j','k','l','m','n',
                                       'o','p','q','r','s','t','u','v','w','x','y','z'};

    GenDataTest::GenDataTest():user_id_(0), item_type_(0), item_id_(0), first_row_disk_(true),
                               first_row_sstable_(true), table_index_(0), curr_sstable_id_(SSTABLE_ID_BASE),
                               curr_sstable_size_(0), table_args_(NULL), table_num_(0),   schema_mgr_(NULL)
    {

    }

    int GenDataTest::init(TableArgs *args, int table_num)
    {
      int ret = OB_SUCCESS;
      if (NULL == args || table_num <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument args = %p table_num = %d", args, table_num);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        table_args_ = args;
        table_num_  = table_num;
      }

      tbsys::CConfig config;
      ObSchemaManagerV2 *mm = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
      if (!mm->parse_from_file(table_args_[0].schema_file_, config))
      {
        fprintf(stderr,"parse schema failed\n");
        exit(0);
      }
      schema_mgr_ = mm;

      func_table_[0] = &GenDataTest::fill_row_press_info;
      func_table_[1] = &GenDataTest::fill_row_press_item;
      func_table_[2] = &GenDataTest::fill_row_recovery_info;
      func_table_[3] = &GenDataTest::fill_row_recovery_item;
      strncpy(compressor_name_,"lzo_1.0",7); //TODO
      compressor_name_[7] = '\0';
      compressor_string_.assign(compressor_name_,strlen(compressor_name_));
      return ret;
    }

    const common::ObString& GenDataTest::get_compressor() const
    {
      return compressor_string_;
    }


    int GenDataTest::create_sstable(uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      first_row_sstable_ = true;
      uint64_t real_id = (++curr_sstable_id_ << 8)|((table_args_[table_index_].disk_no_ & 0xff));

      snprintf(dest_file_,sizeof(dest_file_),"%s/%d/%s/sstable/%ld",
          table_args_[table_index_].data_dest_, table_args_[table_index_].disk_no_,
          schema_mgr_->get_app_name(), real_id);
      sstable_file_name_.assign(dest_file_,strlen(dest_file_));

      sstable_id_.sstable_file_id_ = real_id;
      sstable_id_.sstable_file_offset_ = 0;

      range_.table_id_ = table_id;
      range_.start_key_ = last_sstable_end_key_;

      if ((ret = writer_.create_sstable(table_schema_, sstable_file_name_, get_compressor(),
                                        0, OB_SSTABLE_STORE_DENSE,
                                        table_args_[table_index_].block_size_)) != OB_SUCCESS)
      {
        fprintf(stderr,"create sstable failed\n");
      }
      return ret;
    }

    int GenDataTest::finish_sstable(bool set_max)
    {
      int ret = OB_SUCCESS;
      int64_t trailer_offset, sstable_size;
      if ((ret = writer_.close_sstable(trailer_offset, sstable_size)) != OB_SUCCESS
          || sstable_size < 0)
      {
        TBSYS_LOG(WARN, "close sstable error ret=%d sstable size=%ld", ret, sstable_size);
      }

      if (sstable_size >= 0)
      {
        //update end key
        range_.border_flag_.unset_inclusive_start();
        range_.border_flag_.set_inclusive_end();
        range_.end_key_ = curr_rowkey_;

        if (table_args_[table_index_].set_min_)
        {
          fprintf(stderr,"set_min\n");
          range_.start_key_.set_min_row();
          table_args_[table_index_].set_min_ = false;
        }

        if (/*table_args_[table_index_].*/set_max)
        {
          fprintf(stderr,"set_max\n");
          range_.end_key_.set_max_row();
          table_args_[table_index_].set_max_ = false;
        }

        //load the tablet
        ObTablet *tablet = NULL;
        if ((ret = tablet_image_.alloc_tablet_object(range_,tablet)) != OB_SUCCESS)
        {
          fprintf(stderr, "alloc_tablet_object failed.\n\n");
        }
        else
        {
          if (sstable_size > 0) tablet->add_sstable_by_id(sstable_id_);
          tablet->set_data_version(1);
          tablet->set_disk_no(table_args_[table_index_].disk_no_);
          TBSYS_LOG(DEBUG, "add tablet:%s, version=%ld", to_cstring(tablet->get_range()), tablet->get_data_version());
        }

        if (OB_SUCCESS == ret && (ret = tablet_image_.add_tablet(tablet)) != OB_SUCCESS)
        {
          fprintf(stderr,"add table failed,ret:[%d]\n",ret);
        }

        //prepare the start key for the next sstable
        range_.end_key_.deep_copy(last_sstable_end_key_, rowkey_allocator_);
      }
      return ret;
    }

    int GenDataTest::fill_sstable_schema(const ObSchemaManagerV2& schema,uint64_t table_id,ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;
      //memset(&sstable_schema,0,sizeof(sstable_schema));

      int cols = 0;
      int32_t size = 0;

      const ObColumnSchemaV2 *col = schema.get_table_schema(table_id,size);
      const ObRowkeyInfo& rowkey_info = schema.get_table_schema(table_id)->get_rowkey_info();
      int64_t rowkey_index = 0;
      ObRowkeyColumn split;

      if (NULL == col || size <= 0)
      {
        fprintf(stderr,"cann't find this table:%lu\n",table_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int col_index = 0; col_index < size; ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));
          column_def.table_id_ = table_id;
          column_def.column_group_id_ = (col + col_index)->get_column_group_id();
          column_def.column_name_id_ = (col + col_index)->get_id();
          column_def.column_value_type_ = (col + col_index)->get_type();

          rowkey_index = 0;
          ret = rowkey_info.get_index(column_def.column_name_id_, rowkey_index, split);
          int32_t rowkey_seq = OB_SUCCESS == ret ? rowkey_index + 1 : 0;
          column_def.rowkey_seq_ = rowkey_seq;

          ret = sstable_schema.add_column_def(column_def);

          ++cols;
        }
      }

      if ( 0 == cols ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      return ret;
    }

    int GenDataTest::generate()
    {
      int ret = OB_SUCCESS;
      if (0 == table_num_)
      {
        TBSYS_LOG(WARN, "table_num_=%d", table_num_);
        ret = OB_ERROR;
      }

      TableArgs arg;
      tablet_image_.set_data_version(1);

      if (OB_SUCCESS == ret)
      {
        //generate table one by one
        for (int index = 0; index < table_num_ ; ++index)
        {
          first_row_disk_ = true;
          arg = table_args_[index];
          table_index_ = index;
          if (OB_SUCCESS == ret)
          {
            table_schema_.reset();
            user_id_ = arg.start_uid_;
            item_id_ = arg.start_item_id_;
            item_type_ = (item_id_ >= (ITEM_SIZE /2)) ? 1 : 0;
            ret = fill_sstable_schema(*schema_mgr_, arg.table_id_, table_schema_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN,"convert schema error");
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = create_sstable(arg.table_id_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN,"create sstable error table_id=%d", arg.table_id_);
            }
          }

          uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
          int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

          if ((ret = schema_mgr_->get_column_groups(arg.table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
          {
            fprintf(stderr,"get column groups failed : [%d]\n\n",ret);
          }

          for (int32_t sstable_num = 0; sstable_num < arg.sstable_num_; ++sstable_num)
          {
            uint64_t user_id_ori = user_id_;
            uint64_t item_type_ori  = item_type_;
            uint64_t item_id_ori = item_id_;
            for (int32_t group = 0; group < column_group_num; ++group)
            {
              user_id_ = user_id_ori;
              item_type_ = item_type_ori;
              item_id_ = item_id_ori;

              if (arg.data_type_< 4)
              {
                for (int32_t row = 0; row < arg.row_count_ && OB_SUCCESS == ret; ++row)
                {
                  if ((ret = (this->*(func_table_[arg.data_type_]))(column_group_ids[group]))
                      != OB_SUCCESS)
                  {
                    TBSYS_LOG(WARN,"fill row error");
                  }

                  if (OB_SUCCESS == ret)
                  {
                    ret = writer_.append_row(sstable_row_, curr_sstable_size_);
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN,"append_row error ret=%d", ret);
                    }
                  }
                }
              }
              else if (arg.data_type_ == 4)
              {
                FILE * fp = fopen(arg.data_file_, "r");
                char* line = NULL;
                char* trimed_line = NULL;
                size_t len = 0;
                ssize_t read = 0;

                if (NULL == fp)
                {
                  fprintf(stderr, "cannot open input data file %s.\n", arg.data_file_);
                  ret = OB_ERROR;
                }

                while (OB_SUCCESS == ret && -1 != (read = getline(&line, &len, fp)) )
                {
                  trimed_line = str_trim(line);
                  if (NULL != trimed_line && trimed_line[0] == '#')
                  {
                    continue;
                  }
                  else
                  {
                    if (line[strlen(line)-1] == '\n') line[strlen(line)-1] = '\0';
                  }

                  ret = fill_row_with_data_file(column_group_ids[group], trimed_line);
                  if (OB_SUCCESS == ret)
                  {
                    ret = writer_.append_row(sstable_row_, curr_sstable_size_);
                  }
                }

                if (NULL != line) free(line);
                if (NULL != fp) fclose(fp);

              }

            }

            bool set_max = sstable_num == arg.sstable_num_ - 1 ? arg.set_max_ : false;

            if ((ret = finish_sstable(set_max)) != OB_SUCCESS)
            {
              fprintf(stderr,"finish_sstable failed: [%d]\n",ret);
            }
            else if (sstable_num < arg.sstable_num_ - 1)
            {
              ret = create_sstable(arg.table_id_);
            }
          }//end generate table for cycle
        }//end if


        char idx_path[MAX_PATH];
        char tmp_path[MAX_PATH];

        if (OB_SUCCESS == ret)
        {
          sprintf(idx_path, "%s/%d/%s/sstable/idx_1_%d",
              arg.data_dest_, arg.disk_no_, schema_mgr_->get_app_name(), arg.disk_no_);
          sprintf(tmp_path, "%s/%d/", arg.data_dest_, arg.disk_no_);
          g_sstable_directory = tmp_path;
        }
        if (OB_SUCCESS == ret && (ret = tablet_image_.write(idx_path, arg.disk_no_)) != OB_SUCCESS)
        {
          fprintf(stderr,"write meta file failed: [%d]\n",ret);
        }
      }
      return ret;
    }

    int GenDataTest::fill_row_with_data_file(uint64_t group_id, char* line)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t int_val = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      time_t time_val = 0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();

      if (OB_SUCCESS == ret)
      {
        sstable_row_.set_table_id(table_args_[table_index_].table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      vector<char*> column_value_list;
      tbsys::CStringUtil::split(line, ",", column_value_list);


      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_args_[table_index_].table_id_, group_id, size);

        if (size != (int32_t)column_value_list.size())
        {
          ret = OB_ERROR;
          fprintf(stderr, "column group :%ld, schema column count: %d, value size:%ld.\n",
              group_id, size, column_value_list.size());
        }

        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();

          int_val = strtoll(column_value_list[i], NULL, 10);
          time_val = tbsys::CTimeUtil::strToTime(column_value_list[i]);

          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(int_val);
              break;
            case ObFloatType:
              float_val = static_cast<float>(int_val);
              obj.set_float(float_val);
              break;
            case ObDoubleType:
              double_val = static_cast<double>(int_val);
              obj.set_double(double_val);
              break;
            case ObDateTimeType:
              int_val = static_cast<int32_t>(time_val);
              obj.set_datetime(int_val);
              break;
            case ObPreciseDateTimeType:
            case ObModifyTimeType:
            case ObCreateTimeType:
              int_val = static_cast<int64_t>(time_val) * (1000000LL);
              obj.set_precise_datetime(int_val);
              break;
            case ObVarcharType:
              {
                snprintf(varchar_buf_, sizeof(varchar_buf_), "%s", column_value_list[i]);
              }
              str.assign_ptr(varchar_buf_,strlen(varchar_buf_));
              obj.set_varchar(str);
              break;
            default:
              fprintf(stderr,"unexpect type : %d\n",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int GenDataTest::fill_row_press_info(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();
      const ObRowkeyInfo& rowkey_info = schema_mgr_->get_table_schema(table_args_[table_index_].table_id_)->get_rowkey_info();

      if ((ret = gen_rowkey_press_info()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      if (OB_SUCCESS == ret)
      {
        sstable_row_.set_rowkey(curr_rowkey_);
        sstable_row_.set_table_id(table_args_[table_index_].table_id_);
        sstable_row_.set_column_group_id(group_id);
      }


      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_args_[table_index_].table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = user_id_  + item_id_ + column->get_id();

          if (rowkey_info.is_rowkey_column(column->get_id()))
          {
            continue;
          }

          switch(column->get_type())
          {
          case ObIntType:
            if ( NULL != column->get_join_info() )
            {
              column_value = column_value - JOIN_DIFF;
            }
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObFloatType:
            float_val = static_cast<float>(column_value);
            if ( NULL != column->get_join_info())
            {
              float_val = static_cast<float>(float_val - JOIN_DIFF);
            }
            obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | ",float_val);
#endif
            break;
          case ObDoubleType:
            double_val = static_cast<double>(column_value);
            if (NULL != column->get_join_info())
            {
              double_val = static_cast<double>(double_val - JOIN_DIFF);
            }
            obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | ",double_val);
#endif
            break;
          case ObDateTimeType:
            if ( NULL != column->get_join_info())
            {
              column_value = column_value - JOIN_DIFF;
            }
            obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif

            break;
          case ObPreciseDateTimeType:
            if ( NULL != column->get_join_info())
            {
              column_value = (column_value - JOIN_DIFF);
            }
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObModifyTimeType:
            if ( NULL != column->get_join_info())
            {
              column_value = (column_value - JOIN_DIFF);
            }
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObCreateTimeType:
            if ( NULL != column->get_join_info())
            {
              column_value = (column_value - JOIN_DIFF);
            }
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObVarcharType:
            if (0 == memcmp("item_proper_xml", column->get_name(), 15))
            {
              snprintf(varchar_buf_, sizeof(varchar_buf_), "%s", "hello,world!Unix-like operating systems are built from a collection of applications, libraries, and developer tools plus a program to allocate resources and talk to the hardware, known as a kernel.The Hurd, GNU's kernel, is actively developed, but is still some way from being ready for daily use, so GNU is often used with a kernel called Linux.");
            }
            else if ( NULL != column->get_join_info())
            {
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%08ld-%ld",item_id_,column->get_id()-JOIN_DIFF);
            }
            else
            {
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column->get_id());
            }
            str.assign_ptr(varchar_buf_,strlen(varchar_buf_)); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | ",varchar_buf_);
#endif
            break;
          default:
            fprintf(stderr,"unexpect type : %d\n",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
        //change tid uid itype
        item_id_++;
        if ( 0 == item_id_ % ITEM_PER_USER )
        {
          user_id_++;
        }

        if (ITEM_SIZE == item_id_)
        {
          item_id_ = 0;
          item_type_ = 0;
        }
        else if ( 0 == item_id_ % (ITEM_SIZE / 2) )
        {
          item_type_ = (item_type_ + 1) % 2;
        }
      }
      return ret;
    }

    int GenDataTest::fill_row_press_item(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();
      if ((ret = gen_rowkey_press_item()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        fprintf(stderr,"set row key failed: [%d]\n",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_args_[table_index_].table_id_);
        sstable_row_.set_column_group_id(group_id);
      }


      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_args_[table_index_].table_id_,group_id,size);
        for(int32_t i = 0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = item_id_ + column->get_id();
          switch(column->get_type())
          {
          case ObIntType:
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObFloatType:
            float_val = static_cast<float>(column_value);
            obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | ",float_val);
#endif
            break;
          case ObDoubleType:
            double_val = static_cast<double>(column_value);
            obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | ",double_val);
#endif

            break;
          case ObDateTimeType:
            obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
            break;
          case ObVarcharType:
            if (0 == memcmp("item_proper_xml", column->get_name(), 15))
            {
              snprintf(varchar_buf_, sizeof(varchar_buf_), "%s", "hello,world!Unix-like operating systems are built from a collection of applications, libraries, and developer tools plus a program to allocate resources and talk to the hardware, known as a kernel.The Hurd, GNU's kernel, is actively developed, but is still some way from being ready for daily use, so GNU is often used with a kernel called Linux.");
            }
            else
            {
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%08ld-%ld",item_id_,column->get_id());
            }
            str.assign_ptr(varchar_buf_,strlen(varchar_buf_)); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | ",varchar_buf_);
#endif
            break;
          default:
            fprintf(stderr,"unexpect type : %d\n",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
        //change tid uid itype
        item_id_++;

        if ( 0 == item_id_ % (ITEM_SIZE / 2))
        {
          item_type_ = (item_type_ + 1) % 2;
        }
      }
      return ret;
    }

    int GenDataTest::fill_row_recovery_info(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();
      if ((ret = gen_rowkey_recovery_info()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        fprintf(stderr,"set row key failed: [%d]\n",ret);
      }
      else
      {

        sstable_row_.set_table_id(table_args_[table_index_].table_id_);
        sstable_row_.set_column_group_id(group_id);
      }


      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_args_[table_index_].table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = (user_id_ * FIR_MULTI + item_id_) * SEC_MULTI + column->get_id();
          switch(column->get_type())
          {
          case ObIntType:
            if ( NULL != column->get_join_info())
            {
              column_value = item_id_ * SEC_MULTI + (column->get_id()-JOIN_DIFF);
            }
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObFloatType:
            float_val = static_cast<float>(column_value);
            if ( NULL != column->get_join_info() )
            {
              float_val = static_cast<float>(item_id_ * SEC_MULTI + (column->get_id()-JOIN_DIFF));
            }
            obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | column_name:%s",float_val, column->get_name());
#endif
            break;
          case ObDoubleType:
            double_val = static_cast<double>(column_value);
            if ( NULL != column->get_join_info() )
            {
              double_val = static_cast<double>(item_id_ * SEC_MULTI + (column->get_id()-JOIN_DIFF));
            }
            obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | column_name:%s",double_val, column->get_name());
#endif

            break;
          case ObDateTimeType:
            column_value = crtime - FIR_MULTI*user_id_ - item_id_ - column->get_id();
            if ( NULL != column->get_join_info() )
            {
              column_value = crtime - item_id_ - (column->get_id()-JOIN_DIFF);
            }
            obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif

            break;
          case ObPreciseDateTimeType:
            column_value = (crtime - FIR_MULTI*user_id_ - item_id_ - column->get_id())*MICRO_PER_SEC;
            if ( NULL != column->get_join_info() )
            {
              column_value = (crtime - item_id_ - (column->get_id()-JOIN_DIFF))*MICRO_PER_SEC;
            }
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld |column_name:%s ",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            column_value = (crtime - FIR_MULTI*user_id_ - item_id_ - column->get_id())*MICRO_PER_SEC;
            if ( NULL != column->get_join_info() )
            {
              column_value = (crtime - item_id_ - (column->get_id()-JOIN_DIFF))*MICRO_PER_SEC;
            }
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            column_value = (crtime - FIR_MULTI*user_id_ - item_id_ - column->get_id())*MICRO_PER_SEC;
            if ( NULL != column->get_join_info() )
            {
              column_value = (crtime - item_id_ - (column->get_id()-JOIN_DIFF))*MICRO_PER_SEC;
            }
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:

            if ( NULL != column->get_join_info() )
            {
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%08ld-%ld",item_id_,column->get_id()-JOIN_DIFF);
            }
            else
            {
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column->get_id());
            }
            str.assign_ptr(varchar_buf_,strlen(varchar_buf_)); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | column_name:%s",varchar_buf_, column->get_name());
#endif
            break;
          default:
            fprintf(stderr,"unexpect type : %d\n",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }

        //change tid uid itype
        ++item_id_;
        ++item_type_;
        if (ARR_SIZE*SEC_MULTI == item_id_)
        {
          item_id_   = 0;
          item_type_ = 0;
          ++user_id_;
        }
      }
      return ret;
    }

    int GenDataTest::fill_row_recovery_item(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();
      if ((ret = gen_rowkey_recovery_item()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        fprintf(stderr,"set row key failed: [%d]\n",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_args_[table_index_].table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_args_[table_index_].table_id_, group_id, size);
        for(int32_t i = 0; i < size && (OB_SUCCESS == ret); ++i,++column)
        {
          obj.reset();
          column_value = item_id_ * SEC_MULTI + column->get_id();
          switch(column->get_type())
          {
          case ObIntType:
            column_value = item_id_ * SEC_MULTI + column->get_id();
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObFloatType:
            float_val = static_cast<float>(item_id_ * SEC_MULTI + column->get_id());
            obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | column_name:%s",float_val,  column->get_name());
#endif
            break;
          case ObDoubleType:
            double_val = static_cast<double>(item_id_ * SEC_MULTI + column->get_id());
            obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%f | column_name:%s",double_val, column->get_name());
#endif

            break;
          case ObDateTimeType:
            column_value = crtime - item_id_ - column->get_id();
            obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObPreciseDateTimeType:
            column_value = (crtime - item_id_ - column->get_id())*MICRO_PER_SEC;
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            column_value = (crtime - item_id_ - column->get_id())*MICRO_PER_SEC;
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            column_value = (crtime - item_id_ - column->get_id())*MICRO_PER_SEC;
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | column_name:%s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            snprintf(varchar_buf_,sizeof(varchar_buf_),"%08ld-%ld",item_id_,column->get_id());
            str.assign_ptr(varchar_buf_,strlen(varchar_buf_)); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | column_name:%s",varchar_buf_, column->get_name());
#endif
            break;
          default:
            fprintf(stderr,"unexpect type : %d\n",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
        //change tid uid itype
        item_id_++;
        item_type_++;
        if ( ARR_SIZE*SEC_MULTI == item_id_ )
        {
          item_id_ = 0;
          item_type_ = 0;
        }
      }
      return ret;
    }

    int GenDataTest::gen_rowkey_press_item()
    {
      int ret = OB_SUCCESS;
      rowkey_object_array_[0].set_int(item_type_);
      rowkey_object_array_[1].set_int(item_id_);
      curr_rowkey_.assign(rowkey_object_array_, 2);

#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"tid is %ld  itype is %d\n",item_id_,item_type_);
#endif
      if (first_row_disk_)
      {
        if (0 == item_id_)
        {
          //sprintf(start_key_buf_,"%c%08d",'0',0);
          start_key_object_array_[0].set_int(0);
          start_key_object_array_[1].set_int(0);
        }
        else
        {
          int64_t i_type = (item_id_ - ITEM_SIZE / 10) / (ITEM_SIZE/2);
          //sprintf(start_key_buf_,"%ld%08ld",i_type,item_id_ - 1);
          start_key_object_array_[0].set_int(i_type);
          start_key_object_array_[1].set_int(item_id_ - 1);
        }
        last_sstable_end_key_.assign(start_key_object_array_, 2);
        range_.start_key_  = last_sstable_end_key_;
        first_row_disk_    = false;
        first_row_sstable_ = false;
      }
      else if (first_row_sstable_)
      {
        range_.start_key_  = last_sstable_end_key_;
        first_row_sstable_ = false;
      }
      return ret;
    }

    int GenDataTest::gen_rowkey_press_info()
    {
      int ret = OB_SUCCESS;
      //sprintf(row_key_buf_,"%08ld%ld%08ld",user_id_,item_type_,item_id_);
      //curr_rowkey_.assign(row_key_buf_,strlen(row_key_buf_));

      rowkey_object_array_[0].set_int(user_id_);
      rowkey_object_array_[1].set_int(item_type_);
      rowkey_object_array_[2].set_int(item_id_);
      curr_rowkey_.assign(rowkey_object_array_, 3);

#ifdef GEN_SSTABLE_DEBUG
      char buf[OB_RANGE_STR_BUFSIZ];
      curr_rowkey_.to_string(buf, OB_RANGE_STR_BUFSIZ);
      fprintf(stderr, "gen_rowkey_press_info curr_rowkey=%s\n",buf);
#endif
      if (first_row_disk_)
      {
        if (user_id_ == 0)
        {
          //sprintf(start_key_buf_,"%08d%c%08d",0,'0',0);
          start_key_object_array_[0].set_int(0);
          start_key_object_array_[1].set_int(0);
          start_key_object_array_[2].set_int(0);
        }
        else
        {
          //sprintf(start_key_buf_,"%08ld199999999",user_id_-1);
          start_key_object_array_[0].set_int(user_id_-1);
          start_key_object_array_[1].set_int(1);
          start_key_object_array_[2].set_int(99999999);
        }
        last_sstable_end_key_.assign(start_key_object_array_, 3);
        range_.start_key_  = last_sstable_end_key_;
        first_row_disk_    = false;
        first_row_sstable_ = false;
      }
      else if (first_row_sstable_)
      {
        range_.start_key_  = last_sstable_end_key_;
        first_row_sstable_ = false;
      }
      return ret;
    }

    int GenDataTest::gen_rowkey_recovery_item()
    {
      int ret = OB_SUCCESS;
      //sprintf(row_key_buf_,"%c%08ld",item_type_value[(item_type_/SEC_MULTI)%ARR_SIZE],item_id_);
      rowkey_object_array_[0].set_int(item_type_value[(item_type_/SEC_MULTI)%ARR_SIZE]);
      rowkey_object_array_[1].set_int(item_id_);
      curr_rowkey_.assign(rowkey_object_array_, 2);

#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"tid is %ld  itype is %d\n",item_id_,item_type_);
#endif
      if (first_row_disk_)
      {
        //sprintf(start_key_buf_,"%c%08d",'0',0);
        start_key_object_array_[0].set_int(0);
        start_key_object_array_[1].set_int(0);
        last_sstable_end_key_.assign(start_key_object_array_, 2);
        range_.start_key_ = last_sstable_end_key_;
        first_row_disk_   = false;
      }
      return ret;
    }

    int GenDataTest::gen_rowkey_recovery_info()
    {
      int ret = OB_SUCCESS;
      //sprintf(row_key_buf_,"%08ld%c%08ld",user_id_,item_type_value[(item_type_/SEC_MULTI)%ARR_SIZE],item_id_);
      rowkey_object_array_[0].set_int(item_type_value[(item_type_/SEC_MULTI)%ARR_SIZE]);
      rowkey_object_array_[1].set_int(item_id_);
      curr_rowkey_.assign(rowkey_object_array_, 2);

#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"uid is %ld  tid is %ld  itype is %d\n",user_id_,item_id_,item_type_);
#endif
      if (first_row_disk_)
      {
        if (user_id_ == 0)
        {
          //sprintf(start_key_buf_,"%08d%c%08d",0,'0',0);
          start_key_object_array_[0].set_int(0);
          start_key_object_array_[1].set_int(0);
          start_key_object_array_[2].set_int(0);
        }
        else
        {
          //sprintf(start_key_buf_, "%08ldz00006199",user_id_-1);
          start_key_object_array_[0].set_int(user_id_-1);
          start_key_object_array_[1].set_int(0xf);
          start_key_object_array_[2].set_int(6199);
        }
        last_sstable_end_key_.assign(start_key_object_array_, 3);
        range_.start_key_  = last_sstable_end_key_;
        first_row_disk_    = false;
        first_row_sstable_ = false;
      }
      else if (first_row_sstable_)
      {
        range_.start_key_ = last_sstable_end_key_;
        first_row_sstable_ = false;
      }
      return ret;
    }
  }
}

using namespace std;
using namespace oceanbase;
using namespace oceanbase::chunkserver;

int main(int argc, char *argv[])
{
  int ret = 0;
  const char *config_file = NULL;
  int quiet = 0;

  while((ret = getopt(argc,argv,"qf:vV")) != -1)
  {
    switch(ret)
    {
      case 'f':
        config_file = optarg;
        break;
      case 'q':
        quiet = 1;
        break;
      case 'v':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit(1);
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit(1);
      default:
        fprintf(stderr,"%s is not identified",optarg);
        exit(1);
    }
  }

  if (quiet) TBSYS_LOGGER.setLogLevel("ERROR");

  if (NULL != config_file)
  {
    if (TBSYS_CONFIG.load(config_file))
    {
      fprintf(stderr, "load file %s error\n", config_file);
      return EXIT_FAILURE;
    }

    ret = OB_SUCCESS;
    int32_t table_num = TBSYS_CONFIG.getInt("config", "table_num", 1);
    TableArgs *args = reinterpret_cast<TableArgs *>(malloc(table_num * sizeof(TableArgs)));
    if (NULL == args)
    {
      fprintf(stderr, "malloc memory failed args=%p", args);
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret)
    {
      vector<int> tableids = TBSYS_CONFIG.getIntList("config","table");
      int i = 0;
      for (vector<int>::iterator it = tableids.begin(); it != tableids.end(); ++it)
      {
        args[i++].table_id_ = *it;
      }

      if (NULL == config_file || args[0].table_id_ < 1000)
      {
        //usage(argv[0]);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        //default config
        for (int i = 0; i < table_num; ++i)
        {
          args[i].schema_file_ = TBSYS_CONFIG.getString("config", "schema_file", "schema_file");
          args[i].row_count_   = TBSYS_CONFIG.getInt("config", "row_count", 100);
          args[i].disk_no_     = TBSYS_CONFIG.getInt("config", "disk_no",   1);
          args[i].data_type_   = TBSYS_CONFIG.getInt("config", "data_type", 0);
          args[i].block_size_  = TBSYS_CONFIG.getInt("config", "block_size", ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE);
          int smax             = TBSYS_CONFIG.getInt("config", "set_max",   0);
          int smin             = TBSYS_CONFIG.getInt("config", "set_min",   0);
          args[i].set_max_     = 1 == smax ? true : false;
          args[i].set_min_     = 1 == smin ? true : false;
          args[i].data_dest_   = TBSYS_CONFIG.getString("config", "data_dest",  "data");
        }

        //table config
        for (int i = 0; i < table_num; ++i)
        {
          char table_name[100];
          snprintf(table_name,100,"%d",args[i].table_id_);

          args[i].row_count_   = TBSYS_CONFIG.getInt(table_name, "row_count", 100);
          args[i].disk_no_     = TBSYS_CONFIG.getInt(table_name, "disk_no",   1);
          args[i].sstable_num_ = TBSYS_CONFIG.getInt(table_name, "sstable_num", 10);
          args[i].data_type_   = TBSYS_CONFIG.getInt(table_name, "data_type", 0);
          args[i].block_size_  = TBSYS_CONFIG.getInt("config", "block_size", ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE);
          args[i].start_uid_   = TBSYS_CONFIG.getInt(table_name, "start_uid",   0);
          args[i].start_item_id_   = TBSYS_CONFIG.getInt(table_name, "start_tid",   0);
          int smax             = TBSYS_CONFIG.getInt(table_name, "set_max",   0);
          int smin             = TBSYS_CONFIG.getInt(table_name, "set_min",   0);
          args[i].set_max_     = 1 == smax ? true : false;
          args[i].set_min_     = 1 == smin ? true : false;
          args[i].data_file_   = TBSYS_CONFIG.getString(table_name, "data_file");
          fprintf(stderr, "table=%d, row count=%d, disk no =%d, sstable num=%d, data type = %d,"
              "block size = %ld, start uid = %ld, start item id = %ld, smax=%d, smin=%d, data_file=%s\n",
              args[i].table_id_, args[i].row_count_, args[i].disk_no_ ,
              args[i].sstable_num_, args[i].data_type_, args[i].block_size_,
              args[i].start_uid_, args[i].start_item_id_, args[i].set_max_,
              args[i].set_min_, args[i].data_file_);
        }
      }

      ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
      ob_init_memory_pool();

      GenDataTest data_builder;
      data_builder.init(args, table_num);
      data_builder.generate();
    }
  }
}
