/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * gen_sstable.cpp is for what ...
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *   huating <huating.zmq@taobao.com>
 *
 */
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
#include <tbsys.h>
#include <map>
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "gen_sstable.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_disk_path.h"
#include "common/utility.h"
#include <getopt.h>
#include <vector>

const char* g_sstable_directory = NULL;
namespace oceanbase
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;
    using namespace oceanbase::compactsstablev2;
    const int64_t MICRO_PER_SEC = 1000000;
    const int64_t FIR_MULTI = 256;
    const int64_t SEC_MULTI = 100;
    const int32_t ARR_SIZE = 62;
    const int32_t JOIN_DIFF = 9;
    const int64_t crtime=1289915563;


    ObGenSSTable::ObGenSSTable():
      file_no_(-1),
      reserve_ids_(30),
      disk_no_(1),
      dest_dir_(NULL),
      gen_join_table_(false),
      block_size_(0),
      row_key_prefix_(0),
      row_key_suffix_(0),
      step_length_(1),
      max_rows_(0),
      max_sstable_num_(1),
      schema_mgr_(NULL),
      table_schema_(NULL),
      current_sstable_id_(SSTABLE_ID_BASE),
      curr_uid_(0),
      curr_tid_(0),
      curr_itype_(0),
      config_file_(NULL),
      comp_name_(NULL),
      sstable_version_(0)
    {
      memset(&table_id_list_,0,sizeof(table_id_list_));
    }

    ObGenSSTable::~ObGenSSTable()
    {
    }

    void ObGenSSTable::init(ObGenSSTableArgs& args)
    {
      config_file_ = args.config_file_;
      file_no_ = args.file_no_;
      reserve_ids_ = args.reserve_ids_;
      disk_no_ = args.disk_no_;

      memcpy(table_id_list_,args.table_id_list_,sizeof(args.table_id_list_));
      row_key_prefix_ = args.seed_ - 1;
      row_key_suffix_ = args.suffix_ - 1;
      step_length_ = args.step_length_;
      max_rows_ = args.max_rows_;
      max_sstable_num_ = args.max_sstable_num_;
      set_min_ = args.set_min_;
      set_max_ = args.set_max_;
      gen_join_table_ = args.gen_join_table_;
      binary_rowkey_format_ = args.binary_rowkey_format_;
      block_size_ = args.block_size_;
      curr_uid_ = args.c_uid_;
      current_sstable_id_ = SSTABLE_ID_BASE;

      schema_mgr_ = args.schema_mgr_;
      dest_dir_ = args.dest_dir_;

      comp_name_ = args.comp_name_;
      if (NULL == comp_name_)
      {
        comp_name_ = "lzo_1.0";
      }
      strncpy(compressor_name_,comp_name_,strlen(comp_name_));
      compressor_name_[strlen(comp_name_)] = '\0';
      compressor_string_.assign(compressor_name_,static_cast<int32_t>(strlen(compressor_name_)));
      sstable_version_ = args.sstable_version_;
    }

    int ObGenSSTable::start_builder()
    {
      int ret = OB_SUCCESS;
      bool is_join_table = false;

      tablet_image_.set_data_version(1);

      for(int table=0; OB_SUCCESS == ret && table_id_list_[table] != 0; ++table)
      {
        fprintf(stderr,"start build data of table[%d]\n",table_id_list_[table]);

        TableGen gen(*this);
        if (gen_join_table_ && 1 == table)
        {
          is_join_table = true;
        }

        if ((ret = gen.init(table_id_list_[table], is_join_table)) != OB_SUCCESS )
        {
          fprintf(stderr,"init tablegen failed:[%d]\n",ret);
          continue; //ignore this table
        }

        if ( (ret = gen.generate()) != OB_SUCCESS)
        {
          fprintf(stderr,"generate data failed:[%d]\n",ret);
          continue;
        }
      }

      snprintf(dest_file_,sizeof(dest_file_),"%s/idx_1_%d",dest_dir_,disk_no_);
      snprintf(dest_path_, sizeof(dest_path_), "%s", dest_dir_);
      g_sstable_directory = dest_path_;

      if (OB_SUCCESS == ret && (ret = tablet_image_.write(dest_file_, disk_no_)) != OB_SUCCESS)
      {
        fprintf(stderr,"write meta file failed: [%d]\n",ret);
      }
      return ret;
    }

    int ObGenSSTable::start_builder2()
    {
      int ret = OB_SUCCESS;
      bool is_join_table = false;

      tablet_image_.set_data_version(1);

      for(int table=0; OB_SUCCESS == ret && table_id_list_[table] != 0; ++table)
      {
        fprintf(stderr,"start build data of table[%d]\n",table_id_list_[table]);

        TableGen gen(*this);
        if (gen_join_table_ && 1 == table)
        {
          is_join_table = true;
        }

        if ((ret = gen.init(table_id_list_[table], is_join_table)) != OB_SUCCESS )
        {
          fprintf(stderr,"init tablegen failed:[%d]\n",ret);
          continue; //ignore this table
        }


        if ( (ret = gen.generate2()) != OB_SUCCESS)
        {
          fprintf(stderr,"generate data failed:[%d]\n",ret);
          continue;
        }
      }

      snprintf(dest_file_,sizeof(dest_file_),"%s/idx_1_%d",dest_dir_,disk_no_);
      snprintf(dest_path_, sizeof(dest_path_), "%s", dest_dir_);
      g_sstable_directory = dest_path_;

      if (OB_SUCCESS == ret && (ret = tablet_image_.write(dest_file_, disk_no_)) != OB_SUCCESS)
      {
        fprintf(stderr,"write meta file failed: [%d]\n",ret);
      }
      return ret;
    }

    int64_t ObGenSSTable::get_sstable_id()
    {
      if (SSTABLE_ID_BASE == current_sstable_id_)
      {
        current_sstable_id_ += (file_no_ * reserve_ids_) + 1;
      }
      else
      {
        ++current_sstable_id_;
      }
      return current_sstable_id_;
    }

    const common::ObString& ObGenSSTable::get_compressor() const
    {
      return compressor_string_;
    }

    /*-----------------------------------------------------------------------------
     *  ObGenSSTable::TableGen
     *-----------------------------------------------------------------------------*/

    ObGenSSTable::TableGen::TableGen(ObGenSSTable& gen_sstable) :
                                                    row_key_prefix_(-1),
                                                    row_key_suffix_(-1),
                                                    step_length_(0),
                                                    max_rows_(1),
                                                    max_sstable_num_(1),
                                                    set_min_(false),
                                                    set_max_(false),
                                                    is_join_table_(false),
                                                    gen_sstable_(gen_sstable),
                                                    table_schema_(NULL),
                                                    first_key_(true),
                                                    inited_(false),
                                                    table_id_(0),
                                                    total_rows_(0),
                                                    disk_no_(1),
                                                    current_sstable_size_(0),
                                                    tablet_image_(gen_sstable.tablet_image_),
                                                    curr_uid_(0),
                                                    curr_tid_(0),
                                                    curr_itype_(0),
                                                    config_file_(NULL),
                                                    comp_name_(NULL),
                                                    sstable_version_(gen_sstable.sstable_version_)
    {}

    int ObGenSSTable::TableGen::init(uint64_t table_id, const bool is_join_table)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        table_id_ = table_id;
        config_file_    = gen_sstable_.config_file_;
        dest_dir_       = gen_sstable_.dest_dir_;
        disk_no_        = gen_sstable_.disk_no_;
        comp_name_      = gen_sstable_.comp_name_;

        if (NULL == config_file_)
        {
          row_key_suffix_ = gen_sstable_.row_key_suffix_;
          step_length_    = gen_sstable_.step_length_;
          set_min_        = gen_sstable_.set_min_;
          set_max_        = gen_sstable_.set_max_;
          is_join_table_  = is_join_table;
          disk_no_        = gen_sstable_.disk_no_;
          max_sstable_num_ = gen_sstable_.max_sstable_num_;
          if (is_join_table_)
          {
            //join table only has 1/10 rows count of wide table
            max_rows_     = gen_sstable_.max_rows_ / ITEMS_PER_USER;
            row_key_prefix_ = (disk_no_ - 1) *
              (gen_sstable_.max_rows_ * max_sstable_num_ / ITEMS_PER_USER) - 1;
            if (max_rows_ <= 0)
            {
              TBSYS_LOG(ERROR, "max_rows_(=%ld/%ld) of join_table must larger than 0!",
                  gen_sstable_.max_rows_, ITEMS_PER_USER);
              ret = OB_INVALID_ARGUMENT;
            }
          }
          else
          {
            max_rows_     = gen_sstable_.max_rows_;
            row_key_prefix_ = (disk_no_ - 1) *
              (gen_sstable_.max_rows_ * max_sstable_num_ / ITEMS_PER_USER) - 1;
          }
        }
        else
        {
          char table_name[100];
          snprintf(table_name,100,"%ld",table_id);

          row_key_prefix_   = TBSYS_CONFIG.getInt(table_name,"rowkey_prefix",0) - 1;
          row_key_suffix_   = TBSYS_CONFIG.getInt(table_name,"rowkey_suffix",0) - 1;
          step_length_      = TBSYS_CONFIG.getInt(table_name,"step",1);

          set_min_       = false;
          set_max_       = false;
          int smax       = TBSYS_CONFIG.getInt(table_name,"set_max",1);
          int smin       = TBSYS_CONFIG.getInt(table_name,"set_min",1);
          set_max_       = 1 == smax ? true : false;
          set_min_       = 1 == smin ? true : false;

          curr_uid_       = TBSYS_CONFIG.getInt(table_name,"start_uid",0);
          curr_tid_       = TBSYS_CONFIG.getInt(table_name,"start_tid",0);
          curr_itype_     = TBSYS_CONFIG.getInt(table_name,"start_type",0);
          max_rows_       = TBSYS_CONFIG.getInt(table_name,"max_rows",1);
          max_sstable_num_ = TBSYS_CONFIG.getInt(table_name,"max_sstable",10);

        }

        schema_mgr_ = gen_sstable_.schema_mgr_;
        table_schema_ = gen_sstable_.schema_mgr_->get_table_schema(table_id);
        if (NULL == table_schema_)
        {
          fprintf(stderr,"can't find the schema of table [%lu]\n", table_id);
          ret = OB_ERROR;
        }

        if (3 == sstable_version_)
        {
          schema2_.reset();
          if (OB_SUCCESS == ret && compactsstablev2::build_sstable_schema(table_id_,
                *gen_sstable_.schema_mgr_, schema2_))
          {
            fprintf(stderr,"convert schema failed\n");
            ret = OB_ERROR;
          }
        }
        else
        {
          if ( OB_SUCCESS == ret && sstable::build_sstable_schema(table_id_,
                *gen_sstable_.schema_mgr_, schema_, gen_sstable_.binary_rowkey_format_) != OB_SUCCESS)
          {
            fprintf(stderr,"convert schema failed\n");
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          inited_  = true;
        }

        fprintf(stderr, "gen init, table_id:=%ld,prefix=%ld,suffix=%ld,"
            "max_rows=%ld,max_sstable_num_=%ld,disk_no_=%d,"
            "is_join_table_=%d, min=%d,max=%d",
            table_id_, row_key_prefix_, row_key_suffix_,
            max_rows_, max_sstable_num_, disk_no_,
            is_join_table_, set_min_, set_max_);

      }
      return ret;
    }


    int ObGenSSTable::TableGen::generate_range_by_prefix(ObNewRange& range, int64_t prefix, int64_t suffix)
    {
      UNUSED(suffix);
      range.table_id_ = table_id_;
      int64_t start_prefix = prefix;
      int64_t end_prefix = start_prefix + max_rows_ / ObGenSSTable::ITEMS_PER_USER;
      if (is_join_table_) end_prefix = start_prefix + max_rows_;
      if (start_prefix < 0) start_prefix = 0;

      ObObj obj_array[2];
      int32_t rowkey_column_count = is_join_table_ ? 1 : 2;
      ObRowkey row_key(obj_array, rowkey_column_count);

      obj_array[0].set_int(start_prefix);
      obj_array[1].set_int(INT64_MAX);

      row_key.deep_copy(range.start_key_, allocator_);

      obj_array[0].set_int(end_prefix);
      obj_array[1].set_int(INT64_MAX);
      row_key.deep_copy(range.end_key_, allocator_);

      return OB_SUCCESS;

    }

    int ObGenSSTable::TableGen::generate()
    {
      int ret = OB_SUCCESS;
      int64_t prev_row_key_prefix = 0;
      int64_t prev_row_key_suffix = 0;
      static int64_t total_time = 0;


      if ((ret = create_new_sstable(table_id_)) != OB_SUCCESS)
      {
        fprintf(stderr,"create sstable failed.\n");
      }

      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if ( (ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
      {
        fprintf(stderr,"get column groups failed : [%d]",ret);
      }

      for(int32_t sstable_num = 0; OB_SUCCESS == ret && sstable_num < max_sstable_num_; ++sstable_num)
      {
        generate_range_by_prefix(range_, row_key_prefix_, row_key_suffix_);
        prev_row_key_prefix = row_key_prefix_;
        prev_row_key_suffix = row_key_suffix_;

          struct timezone zone;
          struct timeval start;
          struct timeval end;

        for(int32_t group_index=0; OB_SUCCESS == ret && group_index < column_group_num; ++group_index)
        {
          srandom(static_cast<unsigned int>(prev_row_key_prefix));
          row_key_prefix_ = prev_row_key_prefix;
          row_key_suffix_ = prev_row_key_suffix;

          fprintf(stderr, "add column group %lu max_rows_ %ld\n", column_group_ids[group_index], max_rows_);


          for(int i=0;(i < max_rows_) && (OB_SUCCESS == ret); ++i)
          {
            if ((ret = fill_row_with_aux_column( column_group_ids[group_index] ) ) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "fill row error: [%d]",ret);
            }

            gettimeofday(&start, &zone);
            if (OB_SUCCESS == ret &&
                (ret = writer_.append_row(sstable_row_,current_sstable_size_)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"append row failed: [%d]",ret);
            }
            gettimeofday(&end, &zone);
            total_time = total_time + (end.tv_sec - start.tv_sec) * 1000000
              + (end.tv_usec - start.tv_usec);
          }
        }

        bool set_max = sstable_num == max_sstable_num_ - 1 ? set_max_ : false;
        int tmp_ret = finish_sstable(set_max);

        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(ERROR,"finish_sstable failed: [%d]",tmp_ret);
          if (OB_SUCCESS == ret)
          {
            ret = tmp_ret;
          }
        }
        if (OB_SUCCESS == ret && sstable_num < max_sstable_num_ - 1)
        {
          ret = create_new_sstable(table_id_);
        }
      }

      printf("time:::%ld\n", total_time);
      return ret;
    }

    int ObGenSSTable::TableGen::generate2()
    {
      int ret = OB_SUCCESS;
      int64_t prev_row_key_prefix = 0;
      int64_t prev_row_key_suffix = 0;
      static int64_t total_time = 0;

      const compactsstablev2::ObSSTableSchemaColumnDef* def_array = NULL;
      int64_t row_size = 0;
      row_desc_.reset();
      if (NULL == (def_array = schema2_.get_table_schema(
              table_id_, true, row_size)))
      {
        TBSYS_LOG(ERROR, "schem2_ get table schema error");
      }
      else
      {
        for (int64_t i = 0; i < row_size; i ++)
        {
          if (OB_SUCCESS != (ret = row_desc_.add_column_desc(
                  table_id_, def_array[i].column_id_)))
          {
            TBSYS_LOG(ERROR, "row_desc_ add column desc error:"
                "ret=%d", ret);
            break;
          }
        }
        row_desc_.set_rowkey_cell_count(row_size);
      }

      if (NULL == (def_array = schema2_.get_table_schema(
              table_id_, false, row_size)))
      {
        TBSYS_LOG(ERROR, "schema2_ get table schema error");
      }
      else
      {
        for (int64_t i = 0; i < row_size; i ++)
        {
          if (OB_SUCCESS != (ret = row_desc_.add_column_desc(
                  table_id_, def_array[i].column_id_)))
          {
            TBSYS_LOG(ERROR, "row_desc_ add column desc error:"
                "ret=%d", ret);
            break;
          }
        }
      }

      row_.set_row_desc(row_desc_);

      if ((ret = create_new_sstable2(table_id_)) != OB_SUCCESS)
      {
        fprintf(stderr,"create sstable failed.\n");
      }

      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if ( (ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
      {
        fprintf(stderr,"get column groups failed : [%d]",ret);
      }

      for(int32_t sstable_num = 0; OB_SUCCESS == ret && sstable_num < max_sstable_num_; ++sstable_num)
      {
        generate_range_by_prefix(range_, row_key_prefix_, row_key_suffix_);
        prev_row_key_prefix = row_key_prefix_;
        prev_row_key_suffix = row_key_suffix_;

          struct timezone zone;
          struct timeval start;
          struct timeval end;

        if (OB_SUCCESS != (ret = writer2_.set_table_info(table_id_,
                schema2_, range_)))
        {
          fprintf(stderr, "writer2 set table info eror");
        }
        else if (OB_SUCCESS != (ret = writer2_.set_sstable_filepath(
                dest_file_string_)))
        {
          fprintf(stderr, "writer2 set sstable filepath");
        }

        for(int32_t group_index=0; OB_SUCCESS == ret && group_index < column_group_num; ++group_index)
        {
          srandom(static_cast<unsigned int>(prev_row_key_prefix));
          row_key_prefix_ = prev_row_key_prefix;
          row_key_suffix_ = prev_row_key_suffix;

          fprintf(stderr, "add column group %lu max_rows_ %ld\n", column_group_ids[group_index], max_rows_);

          for(int i=0;(i < max_rows_) && (OB_SUCCESS == ret); ++i)
          {
            if ( (ret = fill_row_with_aux_column2( column_group_ids[group_index] ) ) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "fill row error: [%d]",ret);
            }

            bool is_split = false;

            gettimeofday(&start, &zone);
            if (OB_SUCCESS == ret &&
                (ret = writer2_.append_row(row_, is_split)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"append row failed: [%d]",ret);
            }
            gettimeofday(&end, &zone);
            total_time = total_time + (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
          }
        }

        int tmp_ret = writer2_.finish();

        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(ERROR,"finish_sstable failed: [%d]",tmp_ret);
          if (OB_SUCCESS == ret)
          {
            ret = tmp_ret;
          }
        }
        if (OB_SUCCESS == ret && sstable_num < max_sstable_num_ - 1)
        {
          ret = create_new_sstable2(table_id_);
        }
      }
      printf("time:::%ld\n", total_time);
      return ret;
    }

    int ObGenSSTable::TableGen::create_new_sstable(int64_t table_id)
    {
      UNUSED(table_id);
      int ret = OB_SUCCESS;
      sstable::ObSSTableId sst_id;

      uint64_t real_id = ( gen_sstable_.get_sstable_id() << 8)|((disk_no_ & 0xff));

      sst_id.sstable_file_id_ = real_id;
      sst_id.sstable_file_offset_ = 0;

      //ret = get_sstable_path(sst_id, dest_file_, sizeof(dest_file_));
      if (OB_SUCCESS != ret)
      {
        fprintf(stderr,"get sstable file path failed\n");
      }
      else
      {
        snprintf(dest_file_,sizeof(dest_file_),"%s/%ld",dest_dir_,real_id);
        dest_file_string_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));

        sstable_id_.sstable_file_id_ = real_id;
        sstable_id_.sstable_file_offset_ = 0;

        writer_.set_dio(true);

        if ((ret = writer_.create_sstable(schema_,dest_file_string_,gen_sstable_.get_compressor(),
                0, OB_SSTABLE_STORE_DENSE, gen_sstable_.block_size_, max_rows_)) != OB_SUCCESS)
        {
          fprintf(stderr,"create sstable failed\n");
        }
      }

      return ret;
    }

    int ObGenSSTable::TableGen::create_new_sstable2(int64_t table_id)
    {
      (void) table_id;
      int ret = OB_SUCCESS;
      sstable::ObSSTableId sst_id;

      uint64_t real_id = ( gen_sstable_.get_sstable_id() << 8)|((disk_no_ & 0xff));

      sst_id.sstable_file_id_ = real_id;
      sst_id.sstable_file_offset_ = 0;

      //ret = get_sstable_path(sst_id, dest_file_, sizeof(dest_file_));
      if (OB_SUCCESS != ret)
      {
        fprintf(stderr,"get sstable file path failed\n");
      }
      else
      {
        snprintf(dest_file_,sizeof(dest_file_),"%s/%ld",dest_dir_,real_id);
        dest_file_string_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));

        sstable_id_.sstable_file_id_ = real_id;
        sstable_id_.sstable_file_offset_ = 0;

        ObFrozenMinorVersionRange version_range;
        version_range.major_version_ = 2;
        writer2_.reset();
        if (OB_SUCCESS != (ret = writer2_.set_sstable_param(
                version_range, DENSE_DENSE, 1, gen_sstable_.block_size_,
                gen_sstable_.get_compressor(), 0, 0)))
        {
          fprintf(stderr,"create sstable failed\n");
        }
      }

      return ret;
    }

    int ObGenSSTable::TableGen::finish_sstable(bool set_max /*=false*/)
    {
      int64_t trailer_offset = 0;
      int64_t sstable_size = 0;
      int ret = OB_SUCCESS;
      //update end key
      range_.border_flag_.unset_inclusive_start();
      range_.border_flag_.set_inclusive_end();

      if (set_min_)
      {
        fprintf(stderr,"set_min\n");
        range_.start_key_.set_min_row();
        set_min_ = false;
      }

      if (set_max)
      {
        fprintf(stderr,"set_max\n");
        range_.end_key_.set_max_row();
      }

      if (OB_SUCCESS != (ret = writer_.set_tablet_range(range_)))
      {
        fprintf(stderr, "failed to set range of sstable, ret=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret =
            writer_.close_sstable(trailer_offset, sstable_size)) || sstable_size < 0 )
      {
        fprintf(stderr,"close_sstable failed,offset %ld,ret: [%d]\n\n",trailer_offset,ret);
      }

      //load the tablet
      ObTablet *tablet = NULL;
      const ObSSTableTrailer& trailer = writer_.get_trailer();
      ObTabletExtendInfo extend_info;
#ifdef GEN_SSTABLE_DEBUG
      range_.dump();
      range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
#endif
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = tablet_image_.alloc_tablet_object(range_,tablet)))
        {
          fprintf(stderr, "alloc_tablet_object failed.\n");
        }
        else
        {
          int64_t pos = 0;
          const int64_t checksum_len = sizeof(uint64_t);
          char checksum_buf[checksum_len];
          const int64_t frozen_version = 1;
          tablet->add_sstable_by_id(sstable_id_);
          tablet->set_data_version(frozen_version);
          tablet->set_disk_no(disk_no_);

          extend_info.last_do_expire_version_ = frozen_version;
          extend_info.occupy_size_ = sstable_size;
          extend_info.row_count_ = trailer.get_row_count();
          if (OB_SUCCESS == (ret = serialization::encode_i64(checksum_buf,
              checksum_len, pos, trailer.get_sstable_checksum())))
          {
            extend_info.check_sum_ = ob_crc64(checksum_buf, checksum_len);
          }
          else
          {
            TBSYS_LOG(ERROR, "failed to serialization chunksum, ret=%d", ret);
          }
          if (0 == extend_info.row_count_ || 0 == extend_info.check_sum_)
          {
            TBSYS_LOG(WARN, "extend_info.row_count_ %ld and extend_info.check_sum_ %ld"
                " should not be 0", extend_info.row_count_, extend_info.check_sum_);
          }

          if (OB_SUCCESS == ret)
          {
            tablet->set_extend_info(extend_info);
          }
        }
      }
      //  char range_buf[1024];
      //  range_.to_string(range_buf, 1024);
      //  fprintf(stderr, "range_: %s\n", range_buf);

      if (OB_SUCCESS == ret && (OB_SUCCESS != (ret = tablet_image_.add_tablet(tablet))))
      {
        fprintf(stderr,"add table failed,ret:[%d]\n",ret);
      }

      //prepare the start key for the next sstable
      range_.end_key_.deep_copy(last_end_key_, allocator_);

      return ret;
    }



    int ObGenSSTable::TableGen::fill_row_with_aux_column(uint64_t group_id)
    {
      int ret = OB_SUCCESS;

      const common::ObColumnSchemaV2* column = NULL;
      const common::ObColumnSchemaV2* column_end = NULL;
      int64_t column_value = 0;
      int64_t hash_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      bool aux_column = false;
      MurmurHash2 mur_hash;
      ObObj obj;
      ObString str;

      const ObRowkeyInfo& rowkey_info = schema_mgr_->get_table_schema(table_id_)->get_rowkey_info();

      std::map<uint64_t,int64_t> aux_value;
      std::map<uint64_t,float> aux_float_value;
      std::map<uint64_t,double> aux_double_value;
      sstable_row_.clear();

      bool special_key = false;
      int id = 1;

      if ((ret = create_rowkey_aux()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      if (!is_join_table_ && 0 == row_key_prefix_ && 0 == row_key_suffix_)
      {
        special_key = true;
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(row_key_)) != OB_SUCCESS)
      {
        fprintf(stderr,"set row key failed: [%d]\n",ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = sstable_row_.set_table_id(table_id_);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr,"set row table id failed: [%d]\n",ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = sstable_row_.set_column_group_id(group_id);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr,"set row column group id failed: [%d]\n",ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),
                                                            group_id, size);
        column_end = column + size;
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          if (rowkey_info.is_rowkey_column(column->get_id()))
          {
            continue;
          }

          if ( 0 == strncmp(column->get_name(),"aux_",4) )
          {
            aux_column = true;
          }
          else
          {
            aux_column = false;
          }

          obj.reset();
          if (special_key && id < 2)
          {
            column_value = max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM; //assume there are 10 disks
          }
          else if (!aux_column)
          {
            column_value = row_key_prefix_ * column->get_id() * column->get_type() * 991;
            if (column_value < 0)
            {
//              fprintf(stderr,"column_value[%ld] < 0\n", column_value);
              column_value = -column_value;
            }
          }

          if (id++ > 2) special_key = false;

          if (!aux_column || special_key)
          {
            switch(column->get_type())
            {
              case ObIntType:
                obj.set_int(column_value);
                aux_value.insert(make_pair(column->get_id(), 0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [int],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObFloatType:
                float_val = static_cast<float>(column_value);
                obj.set_float(float_val);
                aux_float_value.insert(make_pair(column->get_id(), float_val));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [float],value [%f]\n",column->get_id(),float_val);
#endif
                break;
              case ObDoubleType:
                double_val = static_cast<double>(column_value);
                obj.set_double(double_val);
                aux_double_value.insert(make_pair(column->get_id(),double_val));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [double],value [%f]\n",column->get_id(),double_val);
#endif
                break;
              case ObDateTimeType:
                obj.set_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [datetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObPreciseDateTimeType:
                obj.set_precise_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [pdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObModifyTimeType:
                obj.set_modifytime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [cdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObCreateTimeType:
                obj.set_createtime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [mdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObVarcharType:
                snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
                hash_value = mur_hash(varchar_buf_);
                str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
                obj.set_varchar(str);
                aux_value.insert(make_pair(column->get_id(),0 - hash_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [varchar:%d],value [%s]\n",column->get_id(),obj.get_type(),varchar_buf_);
#endif
                break;
              default:
                fprintf(stderr,"unexpect type : %d\n",column->get_type());
                ret = OB_ERROR;
                break;
            }

          }
          else
          {
            switch(column->get_type())
            {
              case ObIntType:
                {
                  map<uint64_t,int64_t>::iterator it = aux_value.find(column->get_id() - 1);
                  if (it != aux_value.end())
                  {
                    obj.set_int(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [int],value [%ld]\n",column->get_id(),it->second);
#endif
                }
                break;
              case ObFloatType:
                {
                  map<uint64_t,float>::iterator it = aux_float_value.find(column->get_id() - 1);
                  if (it != aux_float_value.end())
                  {
                    obj.set_float(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [float],value [%f]\n",column->get_id(),it->second);
#endif
                }
                break;
              case ObDoubleType:
                {
                  map<uint64_t,double>::iterator it = aux_double_value.find(column->get_id() - 1);
                  if (it != aux_double_value.end())
                  {
                    obj.set_double(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [double],value [%f]\n",column->get_id(),it->second);
#endif
                }
                break;
              default:
                TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
                ret = OB_ERROR;
                break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }

      if (OB_SUCCESS == ret && gen_sstable_.binary_rowkey_format_)
      {
        sstable_row_.set_rowkey_info(&rowkey_info);
      }

      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_with_aux_column2(uint64_t group_id)
    {
      int ret = OB_SUCCESS;

      const common::ObColumnSchemaV2* column = NULL;
      const common::ObColumnSchemaV2* column_end = NULL;

      int64_t column_value = 0;
      int64_t hash_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      bool aux_column = false;
      MurmurHash2 mur_hash;
      ObObj obj;
      ObString str;

      const ObRowkeyInfo& rowkey_info = schema_mgr_->get_table_schema(table_id_)->get_rowkey_info();

      std::map<uint64_t,int64_t> aux_value;
      std::map<uint64_t,float> aux_float_value;
      std::map<uint64_t,double> aux_double_value;

      bool special_key = false;
      int id = 1;
      int64_t obj_count = 0;

      row_.reset(false, ObRow::DEFAULT_NULL);

      if ((ret = create_rowkey_aux()) != OB_SUCCESS)
      {
        fprintf(stderr,"create rowkey failed: [%d]\n",ret);
      }

      const ObObj* ptr = row_key_.ptr();
      for (int64_t i = 0; i < row_key_.length(); i ++)
      {
        row_.raw_set_cell(i, ptr[i]);
        obj_count ++;
      }

      if (!is_join_table_ && 0 == row_key_prefix_ && 0 == row_key_suffix_)
      {
        special_key = true;
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),
                                                              group_id, size);
        column_end = column + size;

        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          if (rowkey_info.is_rowkey_column(column->get_id()))
          {
            continue;
          }

          if ( 0 == strncmp(column->get_name(),"aux_",4) )
          {
            aux_column = true;
          }
          else
          {
            aux_column = false;
          }

          obj.reset();
          if (special_key && id < 2)
          {
            column_value = max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM; //assume there are 10 disks
          }
          else if (!aux_column)
          {
            column_value = row_key_prefix_ * column->get_id() * column->get_type() * 991;
            if (column_value < 0)
            {
              column_value = -column_value;
            }
          }

          if (id++ > 2) special_key = false;

          if (!aux_column || special_key)
          {
            switch(column->get_type())
            {
              case ObIntType:
                obj.set_int(column_value);
                aux_value.insert(make_pair(column->get_id(), 0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [int],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObFloatType:
                float_val = static_cast<float>(column_value);
                obj.set_float(float_val);
                aux_float_value.insert(make_pair(column->get_id(), float_val));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [float],value [%f]\n",column->get_id(),float_val);
#endif
                break;
              case ObDoubleType:
                double_val = static_cast<double>(column_value);
                obj.set_double(double_val);
                aux_double_value.insert(make_pair(column->get_id(),double_val));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [double],value [%f]\n",column->get_id(),double_val);
#endif
                break;
              case ObDateTimeType:
                obj.set_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [datetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObPreciseDateTimeType:
                obj.set_precise_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [pdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObModifyTimeType:
                obj.set_modifytime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [cdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObCreateTimeType:
                obj.set_createtime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [mdatetime],value [%ld]\n",column->get_id(),column_value);
#endif
                break;
              case ObVarcharType:
                snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
                hash_value = mur_hash(varchar_buf_);
                str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
                obj.set_varchar(str);
                aux_value.insert(make_pair(column->get_id(),0 - hash_value));
#ifdef GEN_SSTABLE_DEBUG
              fprintf(stderr,"[%d]type [varchar:%d],value [%s]\n",column->get_id(),obj.get_type(),varchar_buf_);
#endif
                break;
              default:
                fprintf(stderr,"unexpect type : %d\n",column->get_type());
                ret = OB_ERROR;
                break;
            }

          }
          else
          {
            switch(column->get_type())
            {
              case ObIntType:
                {
                  map<uint64_t,int64_t>::iterator it = aux_value.find(column->get_id() - 1);
                  if (it != aux_value.end())
                  {
                    obj.set_int(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [int],value [%ld]\n",column->get_id(),it->second);
#endif
                }
                break;
              case ObFloatType:
                {
                  map<uint64_t,float>::iterator it = aux_float_value.find(column->get_id() - 1);
                  if (it != aux_float_value.end())
                  {
                    obj.set_float(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [float],value [%f]\n",column->get_id(),it->second);
#endif
                }
                break;
              case ObDoubleType:
                {
                  map<uint64_t,double>::iterator it = aux_double_value.find(column->get_id() - 1);
                  if (it != aux_double_value.end())
                  {
                    obj.set_double(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                fprintf(stderr,"[%d]type [double],value [%f]\n",column->get_id(),it->second);
#endif
                }
                break;
              default:
                TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
                ret = OB_ERROR;
                break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = row_.raw_set_cell(obj_count, obj);
            obj_count ++;
          }
        }
      }

      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_aux()
    {
      int ret = OB_SUCCESS;

      int64_t suffix = 0;
      static int64_t row_count = 0;

      // is_join_table_ of collect_item table is true
      // is_join_table_ of collect_info table is false
      if (is_join_table_ || (!is_join_table_ && row_count % ITEMS_PER_USER == 0))
      {
        ++row_key_prefix_;
      }

      rowkey_object_array_[0].set_int(row_key_prefix_);
      if (!is_join_table_)
      { // only for collect_info table
        ++row_key_suffix_;

        if (row_key_prefix_ == 0 && row_key_suffix_ == 0)
        {
          suffix = 0;
          row_count ++;
        }
        else
        {
          /**
           * each perfix combine with  10 suffix
           * item count = max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM
           * user count = max_rows_ * max_sstable_num_ * DISK_NUM
           */
          int64_t item_step = (max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM) / ITEMS_PER_USER;
          //fprintf(stderr, "item_step %ld\n", item_step);
          if (item_step <= 0)
          {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR, "item step[%ld] must larger than 0\n", item_step);
          }
          else
          {
            suffix = random() % item_step + (row_count++ % ITEMS_PER_USER) * item_step;
          }
        }
        rowkey_object_array_[1].set_int(suffix);
      }

      int32_t rowkey_column_count = is_join_table_ ? 1 : 2;
      row_key_.assign(rowkey_object_array_, rowkey_column_count);
      //char row_key_buf[64];
      //row_key_.to_string(row_key_buf, 64);
      //fprintf(stderr, "row key, prefix :%ld, row_key_suffix_:%ld suffix:%ld, rowkey_string_buf:%s row_count %ld\n",
      //    row_key_prefix_,row_key_suffix_, suffix, row_key_buf, row_count);
      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_aux2()
    {
      int ret = OB_SUCCESS;

      int64_t suffix = 0;
      static int64_t row_count = 0;

      // is_join_table_ of collect_item table is true
      // is_join_table_ of collect_info table is false
      if (is_join_table_ || (!is_join_table_ && row_count % ITEMS_PER_USER == 0))
      {
        ++row_key_prefix_;
      }

      rowkey_object_array_[0].set_int(row_key_prefix_);
      if (!is_join_table_)
      { // only for collect_info table
        ++row_key_suffix_;

        if (row_key_prefix_ == 0 && row_key_suffix_ == 0)
        {
          suffix = 0;
          row_count ++;
        }
        else
        {
          /**
           * each perfix combine with  10 suffix
           * item count = max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM
           * user count = max_rows_ * max_sstable_num_ * DISK_NUM
           */
          int64_t item_step = (max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM) / ITEMS_PER_USER;
          //fprintf(stderr, "item_step %ld\n", item_step);
          if (item_step <= 0)
          {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR, "item step[%ld] must larger than 0\n", item_step);
          }
          else
          {
            suffix = random() % item_step + (row_count++ % ITEMS_PER_USER) * item_step;
          }
        }
        rowkey_object_array_[1].set_int(suffix);
      }

      int32_t rowkey_column_count = is_join_table_ ? 1 : 2;
      row_key_.assign(rowkey_object_array_, rowkey_column_count);
      return ret;
    }
  }
}

void usage(const char *program_name)
{
  printf("Usage: %s\t-s schema_file\n"
                    "\t\t-t table_id\n"
                    "\t\t-l seed (default is 1)\n"
                    "\t\t-e step length (default is 1)\n"
                    "\t\t-m max rows(default is 1)\n"
                    "\t\t-N max sstable number(default is 1)\n"
                    "\t\t-d dest_path\n"
                    "\t\t-i disk_no (default is 1)\n"
                    "\t\t-b sstable id will start from this id(default is 1)\n"
                    "\t\t-n generate null tablet [meta from min to max]\n"
                    "\t\t-a set min range\n"
                    "\t\t-z set max range\n"
                    "\t\t-j whether create join table data\n"
                    "\t\t-r binary rowkey sstable, compatible with old fashion\n"
                    "\t\t-x suffix\n"
                    "\t\t-f config file\n"
                    "\t\t-c compress lib name\n"
                    "\t\t-V version\n"
                    "\t\t-v sstable version (3:compactsstablev2)\n",
         program_name);
  exit(0);
}

using namespace std;
using namespace oceanbase;
using namespace oceanbase::chunkserver;


int main(int argc, char *argv[])
{
  ObGenSSTable::ObGenSSTableArgs args;
  const char *schema_file = NULL;
  int ret = 0;
  int32_t table_num = 2;
  int quiet = 0;

  while(-1 != (ret = getopt(argc,argv,"qrs:t:l:e:d:i:b:m:f:nazjhVx:B:c:N:v:")))
  {
    switch(ret)
    {
      case 'q'://only printf ERROR
        quiet = 1;
        break;
      case 's'://schema file path
        schema_file = optarg;
        break;
      case 't'://table id
        parse_string_to_int_array(optarg, ',', args.table_id_list_, table_num);
        break;
      case 'l'://seed
        args.seed_ = strtoll(optarg, NULL, 10);
        break;
      case 'e'://step length
        args.step_length_ = atoi(optarg);
        break;
      case 'd'://dest dir
        args.dest_dir_ = optarg;
        break;
      case 'i'://disk num
        args.disk_no_ = atoi(optarg);
        break;
      case 'b'://file num
        args.file_no_ = atoi(optarg);
        break;
      case 'm'://max row count
        args.max_rows_ = atoi(optarg);
        break;
      case 'N'://max sstable num
        args.max_sstable_num_ = atoi(optarg);
        break;
      case 'n'://range
        args.set_min_ = true;
        args.set_max_ = true;
        break;
      case 'a':
        args.set_min_ = true;
        break;
      case 'z':
        args.set_max_ = true;
        break;
      case 'j':
        args.gen_join_table_ = true;
        break;
      case 'x':
        args.suffix_ = strtoll(optarg,NULL,10);
        break;
      case 'B':
        args.block_size_ = strtoll(optarg, NULL, 10);
        break;
      case 'f':
        args.config_file_ = optarg;
        break;
      case 'c':
        args.comp_name_ = optarg;
        break;
      case 'r':
        args.binary_rowkey_format_ = true;
        break;
      case 'h':
        usage(argv[0]);
        break;
      case 'v':
        args.sstable_version_ = atoi(optarg);
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit(1);
      default:
        fprintf(stderr,"%s is not identified\n",optarg);
        exit(1);
    };
  }

  if (quiet) TBSYS_LOGGER.setLogLevel("ERROR");

  if (NULL != args.config_file_)
  {
    if (TBSYS_CONFIG.load(args.config_file_))
    {
      fprintf(stderr, "load file %s error\n", args.config_file_);
      return EXIT_FAILURE;
    }

    schema_file = TBSYS_CONFIG.getString("config", "schema", "schema.ini");
    fprintf(stderr, "schema is %s\n", schema_file);

    vector<int> tableids = TBSYS_CONFIG.getIntList("config","table");
    int i = 0;
    for (vector<int>::iterator it = tableids.begin(); it != tableids.end(); ++it)
    {
      args.table_id_list_[i++] = *it;
    }

    if (NULL == schema_file || args.table_id_list_[0] < 1000)
    {
      usage(argv[0]);
    }

    args.dest_dir_    = TBSYS_CONFIG.getString("config","dest_dir","data");
    args.file_no_     = TBSYS_CONFIG.getInt("config","sstable_id_base",1);
    args.disk_no_     = TBSYS_CONFIG.getInt("config","disk_no",1);
    args.block_size_ = TBSYS_CONFIG.getInt("config","block_size", sstable::ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE);
    args.comp_name_ = TBSYS_CONFIG.getString("config", "comp_name", "lzo_1.0");
  }
  else
  {
    if (argc < 6)
    {
      usage(argv[0]);
    }

    if (NULL == schema_file || table_num <= 0 || args.table_id_list_[0] < 1000
        || NULL == args.dest_dir_ || args.disk_no_ < 0
        || args.file_no_ < 0 || args.seed_ < 0
        || args.step_length_ < 0 || args.max_rows_ < 0 || args.max_sstable_num_ <= 0)
    {
      usage(argv[0]);
    }
    srandom(static_cast<unsigned int>(args.seed_));
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  tbsys::CConfig c1;
  ObSchemaManagerV2* mm = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  if ( !mm->parse_from_file(schema_file, c1) )
  {
    fprintf(stderr, "parse schema failed\n");
    exit(0);
  }

  args.schema_mgr_  = mm;

  ObGenSSTable data_builder;
  data_builder.init(args);
  if (3 == args.sstable_version_)
  {
    return data_builder.start_builder2();
  }
  else
  {
    return data_builder.start_builder();
  }
}
