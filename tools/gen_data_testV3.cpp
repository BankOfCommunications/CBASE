#include <getopt.h>
#include "gen_data_testV3.h"
#include "sstable/ob_disk_path.h"

char* g_sstable_directory = NULL;

//TODO 每次切换column group的时候 需要记住所有构成rowkey变量
//TODO 把构造rowkow这个拿出来 range的start key设置一下
namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace oceanbase::common;
    using namespace oceanbase::chunkserver;
    using namespace oceanbase::sstable;

    static const int32_t TEN = 10;
    static const int32_t HUN = 100;
    static const int32_t STRMAXINDEX = 62;
    static const int32_t ROUND = 3;
    static const int32_t BIG = 10000;
    static const int32_t THU = 1000;
    //static const int64_t crtime = 1289915563000000;
    static const int64_t crtime = 0;
    static const char* demo = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static const char* demostr[62] = {"0123456789","123456789a","23456789ab","3456789abc",
                                      "456789abcd","56789abcde","6789abcdef","789abcdefg",
                                      "89abcdefgh","9abcdefghi","abcdefghij","bcdefghijk",
                                      "cdefghijkl","defghijklm","efghijklmn","fghijklmno",
                                      "ghijklmnop","hijklmnopq","ijklmnopqr","jklmnopqrs",
                                      "klmnopqrst","lmnopqrstu","mnopqrstuv","nopqrstuvw",
                                      "opqrstuvwx","pqrstuvwxy","qrstuvwxyz","rstuvwxyzA",
                                      "stuvwxyzAB","tuvwxyzABC","uvwxyzABCD","vwxyzABCDE",
                                      "wxyzABCDEF","xyzABCDEFG","yzABCDEFGH","zABCDEFGHI",
                                      "ABCDEFGHIJ","BCDEFGHIJK","CDEFGHIJKL","DEFGHIJKLM",
                                      "EFGHIJKLMN","FGHIJKLMNO","GHIJKLMNOP","HIJKLMNOPQ",
                                      "IJKLMNOPQR","JKLMNOPQRS","KLMNOPQRST","LMNOPQRSTU",
                                      "MNOPQRSTUV","NOPQRSTUVW","OPQRSTUVWX","PQRSTUVWXY",
                                      "QRSTUVWXYZ","RSTUVWXYZ0","STUVWXYZ01","TUVWXYZ012",
                                      "UVWXYZ0123","VWXYZ01234","WXYZ012345","XYZ0123456",
                                      "YZ01234567","Z012345678"};
    const static char *demostrreverse[62] = {"ZYXWVUTSRQ","YXWVUTSRQP","XWVUTSRQPO","WVUTSRQPON",
                                             "VUTSRQPONM","UTSRQPONML","TSRQPONMLK","SRQPONMLKJ",
                                             "RQPONMLKJI","QPONMLKJIH","PONMLKJIHG","ONMLKJIHGF",
                                             "NMLKJIHGFE","MLKJIHGFED","LKJIHGFEDC","KJIHGFEDCB",
                                             "JIHGFEDCBA","IHGFEDCBAz","HGFEDCBAzy","GFEDCBAzyx",
                                             "FEDCBAzyxw","EDCBAzyxwv","DCBAzyxwvu","CBAzyxwvut",
                                             "BAzyxwvuts","Azyxwvutsr","zyxwvutsrq","yxwvutsrqp",
                                             "xwvutsrqpo","wvutsrqpon","vutsrqponm","utsrqponml",
                                             "tsrqponmlk","srqponmlkj","rqponmlkji","qponmlkjih",
                                             "ponmlkjihg","onmlkjihgf","nmlkjihgfe","mlkjihgfed",
                                             "lkjihgfedc","kjihgfedcb","jihgfedcba","ihgfedcba9",
                                             "hgfedcba98","gfedcba987","fedcba9876","edcba98765",
                                             "dcba987654","cba9876543","ba98765432","a987654321",
                                             "9876543210","876543210Z","76543210ZY","6543210ZYX",
                                             "543210ZYXW","43210ZYXWV","3210ZYXWVU","210ZYXWVUT",
                                             "10ZYXWVUTS","0ZYXWVUTSR"};
    GenDataTestV3::GenDataTestV3():customid_(0),thedate_(0),campaignid_(0),adgroupid_(0),
                                   bidwordid_(0),network_(0),matchscope_('a'),searchtype_(demostr[0]),
                                   creativeid_(0), isshop_(0)
    {
    }

    int GenDataTestV3::init(Args* arg)
    {
      int ret = OB_SUCCESS;
      arg_ = arg;
      for(int index = 0; index < arg_->disk_num_; ++index)
      {
        tablet_image_[index] = new ObTabletImage();
      }

      disk_no_ = 0;
      generate_table_[0] = &GenDataTestV3::generate_table_cust;
      generate_table_[1] = &GenDataTestV3::generate_table_campaign;
      generate_table_[2] = &GenDataTestV3::generate_table_search;
      generate_table_[3] = &GenDataTestV3::generate_table_jdatetime;
      generate_table_[4] = &GenDataTestV3::generate_table_jvarchar;
      generate_table_[5] = &GenDataTestV3::generate_table_jint;

      tbsys::CConfig config;
      ObSchemaManagerV2 *mm = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
      if (!mm->parse_from_file(arg_->schema_file_, config))
      {
        TBSYS_LOG(ERROR,"parse schema failed");
        exit(0);
      }
      schema_mgr_ = mm;
      strncpy(compressor_buf_,"lzo_1.0",7);
      compressor_buf_[7] = '\0';
      compressor_name_.assign(compressor_buf_,static_cast<int32_t>(strlen(compressor_buf_)));
      return ret;
    }

    void GenDataTestV3::reset_rowkey_var()
    {
      char_index_ = 0;
      str_index_  = 0;
      customid_   = 0;
      thedate_    = 0;
      campaignid_ = 0;
      adgroupid_  = 1;
      bidwordid_  = 0;
      network_    = 0;
      matchscope_ = '0';
      searchtype_ = demostr[0];
      searchtypereverse_ = demostrreverse[0];
      creativeid_ = 0;
      isshop_     = 0;
    }

    int GenDataTestV3::generate_table_jdatetime()
    {
      int ret = OB_SUCCESS;
      ++disk_no_;
      if (disk_no_ > 10)
        disk_no_ = 1;
      ret = create_sstable(table_id_, disk_no_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
      }
      else
      {
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
        {
          for (int32_t row = 0; row < 150000 && OB_SUCCESS == ret; ++row)
          {
            set_rowkey_var_join(row);
            if ((ret = fill_row_jdatetime(column_group_ids[group],row))
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

        if ((ret = close_sstable(true, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
        }
      }
      return ret;
    }

    int GenDataTestV3::generate_table_jvarchar()
    {
      int ret = OB_SUCCESS;
      ++disk_no_;
      if (disk_no_ > 10)
        disk_no_ = 1;
      ret = create_sstable(table_id_, disk_no_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
      }
      else
      {
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
        {
          for (int32_t row = 0; row < 150000 && OB_SUCCESS == ret; ++row)
          {
            set_rowkey_var_join(row);
            if ((ret = fill_row_jvarchar(column_group_ids[group],row))
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

        if ((ret = close_sstable(true, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
        }
      }
      return ret;
    }

    int GenDataTestV3::generate_table_jint()
    {
      int ret = OB_SUCCESS;
      ++disk_no_;
      if (disk_no_ > 10)
        disk_no_ = 1;
      ret = create_sstable(table_id_, disk_no_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
      }
      else
      {
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
        {
          for (int32_t row = 0; row < 150000 && OB_SUCCESS == ret; ++row)
          {
            set_rowkey_var_join(row);
            if ((ret = fill_row_jint(column_group_ids[group],row))
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

        if ((ret = close_sstable(true, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
        }
      }
      return ret;
    }

    int GenDataTestV3::generate_table_search()
    {
      int ret = OB_SUCCESS;
      ++disk_no_;
      if (disk_no_ > 10)
        disk_no_ = 1;
      ret = create_sstable(table_id_, disk_no_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
      }
      else
      {
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
        {
          for (int32_t row = 0; row < 1000 && OB_SUCCESS == ret; ++row)
          {
            set_rowkey_var_search(row);
            if ((ret = fill_row_search(column_group_ids[group],row))
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

        if ((ret = close_sstable(true, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
        }
      }
      return ret;
    }

    int GenDataTestV3::generate_table_campaign()
    {
      int ret = OB_SUCCESS;
      ++disk_no_;
      if (disk_no_ > 10)
        disk_no_ = 1;
      ret = create_sstable(table_id_,disk_no_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
      }
      else
      {
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
        {
          for (int32_t row = 0; row < 10000/*change the number*/ && OB_SUCCESS == ret; ++row)
          {
            set_rowkey_var_campaign(row);
            if ((ret = fill_row_campaign(column_group_ids[group], row))
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

        if ((ret = close_sstable(true, true)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
        }
      }
      return ret;
    }

     int GenDataTestV3::generate_table_cust()
     {
       int ret = OB_SUCCESS;
       int32_t str_index = 0;
       int32_t char_index = 0;
       int64_t customid = 0;
       int64_t thedate = 0;
       int64_t campaignid = 0;
       int64_t adgroupid = 0;
       int64_t bidwordid = 0;
       int64_t network = 0;
       char     matchscope = 0;
       const char* searchtype = NULL;
       const char* searchtypereverse = NULL;
       int64_t creativeid = 0;
       int64_t isshop = 0;
       bool set_min = false;
       bool set_max = false;
       uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
       int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

       if ((ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
       {
         TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
       }

       for (int32_t sstable = 0; OB_SUCCESS == ret && sstable < 50; ++sstable)
       {
         set_min = false;
         set_max = false;
         if (sstable == 0)
         {
           set_min = true;
         }
         if (sstable == 49)
         {
           set_max = true;
         }

         //set sstable last sstable end key first table == first key
         if (sstable == 0)
         {
           if ((ret = gen_rowkey_cust()) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
             return ret;
           }
           else
           {
             curr_rowkey_.deep_copy(last_sstable_end_key_, rowkey_allocator_);
           }
         }
         range_.start_key_ = last_sstable_end_key_;

         ++disk_no_;
         if (disk_no_ > 10)
           disk_no_ =  1;
         ret = create_sstable(table_id_, disk_no_);

         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR, "create sstable failed table_id = %lu", table_id_);
         }
         //record var when new group
         //int64_t customid_last = customid_;
         //int64_t
         for (int32_t group = 0; OB_SUCCESS == ret && group < column_group_num; ++group)
         {

             if (group == 0)
             {
                 fprintf(stderr, "isshop = %ld", isshop_);
                 char_index = char_index_;
                 str_index = str_index_;
                 customid = customid_;
                 thedate = thedate_;
                 campaignid = campaignid_;
                 adgroupid = adgroupid_;
                 bidwordid = bidwordid_;
                 network = network_;
                 matchscope = matchscope_;
                 searchtype = searchtype_;
                 searchtypereverse = searchtypereverse_;
                 creativeid = creativeid_;
                 isshop = isshop_;
             }
             else
             {
                 char_index_ = char_index;
                 str_index_ = str_index;
                 customid_ = customid;
                 thedate_ = thedate;
                 campaignid_ = campaignid;
                 adgroupid_ = adgroupid;
                 bidwordid_ = bidwordid;
                 network_ = network;
                 matchscope_ = matchscope;
                 searchtype_ = searchtype;
                 searchtypereverse_ = searchtypereverse;
                 creativeid_ = creativeid;
                 isshop_ = isshop;
             }
           for (int32_t row = 0; row < 10000/*TODO change the number*/ && OB_SUCCESS == ret; ++row)
           {
             set_rowkey_var_cust(sstable*10000+row);
             if ((ret = fill_row_cust(column_group_ids[group], sstable*10000+row))
                 != OB_SUCCESS)
             {
               TBSYS_LOG(ERROR,"fill row error");
             }

             if (OB_SUCCESS == ret)
             {
               ret = writer_.append_row(sstable_row_, curr_sstable_size_);
               if (OB_SUCCESS != ret)
               {
                 TBSYS_LOG(ERROR,"append_row error disk_no_=%d, ret=%d", disk_no_, ret);
                 TBSYS_LOG(ERROR,"row number is %d", row);
               }
             }
           }
         }
         if ((ret = close_sstable(set_min, set_max)) != OB_SUCCESS)
         {
           TBSYS_LOG(ERROR,"close_sstable failed: [%d]",ret);
         }
       }
       return ret;
     }

    int GenDataTestV3::generate()
    {
      int ret = OB_SUCCESS;
      for(int index = 0; index < arg_->disk_num_; ++index)
      {
        tablet_image_[index]->set_data_version(1);
      }

      //set table_id here for test, read it from schema
      table_id_ = 1010;
      //generate data
      for(int index = 0; index < 6; ++index)
      {
        table_id_ ++;
        table_schema_.reset();
        reset_rowkey_var();
        //TODO where table_id_ comes from
        ret = fill_sstable_schema(*schema_mgr_, table_id_, table_schema_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "convert schema error, table_id is %lu", table_id_);
        }
        else
        {
          ret = (this->*(generate_table_[index]))();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "generate_table failed, table id = %lu", table_id_);
          }
        }
      }
      //write idx file
      char idx_path[MAX_PATH_LEN];
      for(int index = 1; index <= arg_->disk_num_; ++index)
      {
        sprintf(idx_path, "%s/%d/idx_1_%d", arg_->data_dest_, index, index);
        if (OB_SUCCESS == ret && (ret = tablet_image_[index - 1]->write(idx_path, index)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write meta file faied, ret=%d", ret);
        }
      }
      return ret;
    }

    int GenDataTestV3::create_sstable(uint64_t table_id, int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      uint64_t real_id = (++curr_sstable_id_ << 8)|(disk_no_ & 0xff);
      UNUSED(table_id);

      snprintf(data_file_buf_, sizeof(data_file_buf_), "%s/%d/%ld", arg_->data_dest_, disk_no, real_id);
      data_file_.assign(data_file_buf_, static_cast<int32_t>(strlen(data_file_buf_)));

      sstable_id_.sstable_file_id_ = real_id;
      sstable_id_.sstable_file_offset_ = 0;

      range_.table_id_  = table_id_;
      range_.start_key_ = last_sstable_end_key_;
      if ((ret = writer_.create_sstable(table_schema_, data_file_, get_compressor(),
                                        0, OB_SSTABLE_STORE_DENSE,
                                        arg_->block_size_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "create sstable failed");
      }

      return ret;
    }

    int GenDataTestV3::fill_sstable_schema(const ObSchemaManagerV2& schema,uint64_t table_id,ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;

      int cols = 0;
      int32_t size = 0;

      const ObColumnSchemaV2 *col = schema.get_table_schema(table_id,size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this table:%lu",table_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int col_index = 0; col_index < size; ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));
          column_def.table_id_ = static_cast<int32_t>(table_id);
          column_def.column_group_id_ = static_cast<int16_t>((col + col_index)->get_column_group_id());
          column_def.column_name_id_ = static_cast<int16_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
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

    int GenDataTestV3::close_sstable(bool set_min, bool set_max)
    {
      int ret = OB_SUCCESS;
      int64_t trailer_offset, sstable_size;
      if ((ret = writer_.close_sstable(trailer_offset, sstable_size)) != OB_SUCCESS
          || sstable_size < 0)
      {
        TBSYS_LOG(ERROR, "close sstable error ret%d sstable size=%ld", ret, sstable_size);
      }

      if (sstable_size > 0)
      {
        range_.border_flag_.unset_inclusive_start();
        range_.border_flag_.set_inclusive_end();
        range_.end_key_ = curr_rowkey_;

#ifdef GEN_SSTABLE_DEBUG
        fprintf(stderr, "dump range here\n");
        range_.dump();
        range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
#endif

        if (set_min)
        {
          range_.start_key_.set_min_row();
          fprintf(stderr, "set min here\n");
        }

        if (set_max)
        {
          range_.end_key_.set_max_row();
        }
        fprintf(stderr, "set_min=%s set_max=%s\n", set_min? "true": "false", set_max?"true":"false");
        ObTablet *tablet = NULL;
        tablet_image_[disk_no_ - 1]->dump();
        if ((ret = tablet_image_[disk_no_ - 1]->alloc_tablet_object(range_, tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "alloc tablet object failed.\n");
        }
        else
        {
          tablet->add_sstable_by_id(sstable_id_);
          tablet->set_data_version(1);
          tablet->set_disk_no(disk_no_);
        }

        if (OB_SUCCESS == ret &&
            (ret = tablet_image_[disk_no_ - 1]->add_tablet(tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add tablet failed, ret=%d", ret);
        }
        range_.end_key_.deep_copy(last_sstable_end_key_, rowkey_allocator_);
      }

      return ret;
    }

    const common::ObString& GenDataTestV3::get_compressor() const
    {
      return compressor_name_;
    }

    int GenDataTestV3::fill_row_search(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + (int64_t)index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_search()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          uint64_t columnid = column->get_id();
          column_value = (int64_t)index*1000000;
          switch(column->get_type())
          {
          case ObIntType:
            if (columnid == 1001)
            {
              column_value =  creativeid_;
            }
            else if (columnid == 1002)
            {
              column_value = isshop_;
            }
            else if (columnid > 1008)
            {
              column_value = creativeid_;
            }
            else
            {
              if (columnid % 3 == 0)
              {
                column_value = index;
              }
              else if (columnid % 3 == 1)
              {
                column_value = 100000 - index;
              }
              else if (columnid % 3 == 2)
              {
                column_value = index % 1000;
              }
            }
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s | %d",column_value, column->get_name(), column->get_type());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            str.assign_ptr(var, 10); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }

    int GenDataTestV3::fill_row_campaign(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + (int64_t)index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_campaign()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          uint64_t columnid = column->get_id();
          column_value = (int64_t)index*1000000;
          switch(column->get_type())
          {
          case ObIntType:
            if (columnid == 1001)
            {
              column_value = thedate_;
            }
            else if (columnid == 1002)
            {
              column_value = adgroupid_;
            }
            else if (columnid == 1003)
            {
              column_value = bidwordid_;
            }
            else if (columnid == 1004)
            {
              column_value = network_;
            }
            else if (columnid == 1007)
            {
              column_value = creativeid_;
            }
            else if (columnid == 1008)
            {
              column_value = isshop_;
            }
            else if (columnid % 3 == 0)
            {
              column_value = index;
            }
            else if (columnid % 3 == 1)
            {
              column_value = 100000 - index;
            }
            else if (columnid % 3 == 2)
            {
              column_value = index % 1000;
            }
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            if (columnid == 1005)
            {
              str.assign_ptr(&matchscope_, 1);
            }
            else
            {
              str.assign_ptr(var, 10); //todo len
            }
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }

    int GenDataTestV3::fill_row_cust(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_cust()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        fprintf(stderr, "column size is %d\n", size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          uint64_t columnid = column->get_id();
          column_value = index*(int64_t)1000000;

          if (column->get_id() == 1025)
          {
              column_value = (index%10) * (int64_t)1000000;
          }

          switch(column->get_type())
          {
          case ObIntType:
            if (columnid == 1001)
            {
              column_value = customid_;
            }
            else if (columnid == 1002)
            {
              column_value = campaignid_;
            }
            else if (columnid == 1003)
            {
              column_value = thedate_;
            }
            else if (columnid == 1004)
            {
              column_value = adgroupid_;
            }
            else if (columnid == 1005)
            {
              column_value = bidwordid_;
            }
            else if (columnid == 1006)
            {
              column_value = network_;
            }
            else if (columnid == 1009)
            {
              column_value = creativeid_;
            }
            else if (columnid == 1010)
            {
              column_value = isshop_;
            }
            else if (columnid % 3 == 0)
            {
              column_value = index;
            }
            else if (columnid % 3 == 1)
            {
              column_value = 100000 - index;
            }
            else if (columnid % 3 == 2)
            {
              column_value = index % 1000;
            }
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s | %d",column_value, column->get_name(), column->get_type());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            if (columnid == 1007)
            {
              str.assign_ptr(&matchscope_, 1);
            }
            else
            {
                if (columnid == 1026)
                {
                    memcpy(var, searchtypereverse_, 10);
                }
                str.assign_ptr(var, 10);
            }
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }


    int GenDataTestV3::fill_row_jdatetime(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + (int64_t)index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_join(index)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          //uint64_t columnid = column->get_id();
          column_value = (int64_t)index*1000000;
          switch(column->get_type())
          {
          case ObIntType:
            column_value = index % 100;
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s | %d",column_value, column->get_name(), column->get_type());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            str.assign_ptr(var, 10); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }

    int GenDataTestV3::fill_row_jvarchar(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + (int64_t)index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_join(index)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          //uint64_t columnid = column->get_id();
          column_value = (int64_t)index*1000000;
          switch(column->get_type())
          {
          case ObIntType:
            column_value = index;
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s | %d",column_value, column->get_name(), column->get_type());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            str.assign_ptr(var, 10); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }

    int GenDataTestV3::fill_row_jint(uint64_t group_id, int32_t index)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = crtime + (int64_t)index*1000000;
      ObObj obj;
      ObString str;
      char var[11];
      var[10] = '\0';
      memcpy(var, searchtype_, 10);
      sstable_row_.clear();
      if ((ret = gen_rowkey_joinint(index)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_rowkey(curr_rowkey_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = schema_mgr_->get_group_schema(table_id_,group_id,size);
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = (int64_t)index*1000000;
          switch(column->get_type())
          {
          case ObIntType:
            column_value = 100000 - index;
            obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s | %d",column_value, column->get_name(), column->get_type());
#endif
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObModifyTimeType:
            obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObCreateTimeType:
            obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%ld | %s",column_value, column->get_name());
#endif
            break;
          case ObVarcharType:
            str.assign_ptr(var, 10); //todo len
            obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"%s | %s",var, column->get_name());
#endif
            break;
          default:
            TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
            ret = OB_ERROR;
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"row end\n");
#endif
      }
      return ret;
    }

    int GenDataTestV3::gen_rowkey_search()
    {
      int ret = OB_SUCCESS;
      rowkey_object_array_[0].set_int(creativeid_);
      rowkey_object_array_[1].set_int(isshop_);
      if (OB_SUCCESS == ret)
      {
        curr_rowkey_.assign(rowkey_object_array_, 2);
      }
      return ret;
    }

    int GenDataTestV3::gen_rowkey_campaign()
    {
      int ret = OB_SUCCESS;

      ObString str4(0, 1, &matchscope_);
      ObString str5(0, 10, searchtype_);


      rowkey_object_array_[0].set_int(thedate_);
      rowkey_object_array_[1].set_int(adgroupid_);
      rowkey_object_array_[2].set_int(bidwordid_);
      rowkey_object_array_[3].set_int(network_);
      rowkey_object_array_[4].set_varchar(str4);
      rowkey_object_array_[5].set_varchar(str5);
      rowkey_object_array_[6].set_int(creativeid_);
      rowkey_object_array_[7].set_int(isshop_);


      if (OB_SUCCESS == ret)
      {
        curr_rowkey_.assign(rowkey_object_array_, 8);
      }

      return ret;
    }

    int GenDataTestV3::gen_rowkey_cust()
    {
      int ret = OB_SUCCESS;
      ret = gen_rowkey_campaign();
      return ret;
    }

    int GenDataTestV3::gen_rowkey_join(int32_t index)
    {
      int ret = OB_SUCCESS;
      int64_t num = (int64_t)index * 1000000;
      rowkey_object_array_[0].set_int(num);
      curr_rowkey_.assign(rowkey_object_array_, 1);

      return ret;
    }


    int GenDataTestV3::gen_rowkey_joinint(int32_t index)
    {
      int ret = OB_SUCCESS;
      rowkey_object_array_[0].set_int(index);
      curr_rowkey_.assign(rowkey_object_array_, 1);
      return ret;
    }


    void GenDataTestV3::set_rowkey_var_join(int32_t index)
    {
      isshop_++;
      if (isshop_ == HUN)
      {
        isshop_ = 0;
        creativeid_++;
      }
      matchscope_ = demo[index%STRMAXINDEX];
      searchtype_ = demostr[index%STRMAXINDEX];
      searchtypereverse_ = demostrreverse[index%STRMAXINDEX];
      thedate_ = 1000000 *(int64_t)index;
    }

    void GenDataTestV3::set_rowkey_var_search(int32_t index)
    {
      isshop_++;
      if (isshop_ == HUN)
      {
        isshop_ = 0;
        creativeid_++;
      }
      matchscope_ = demo[index%STRMAXINDEX];
      searchtype_ = demostr[index%STRMAXINDEX];
      searchtypereverse_ = demostrreverse[index%STRMAXINDEX];
      thedate_ = 1000000 *(int64_t)index;
    }

    void GenDataTestV3::set_rowkey_var_campaign(int32_t index)
    {
      isshop_++;
      if (isshop_ == HUN)
      {
        isshop_ = 0;
        creativeid_ ++;
        if (creativeid_ == TEN)
        {
          creativeid_ = 0;
          network_++;
        }
      }
      matchscope_ = demo[index%STRMAXINDEX];
      searchtype_ = demostr[index%STRMAXINDEX];
      searchtypereverse_ = demostrreverse[index%STRMAXINDEX];
      thedate_ = 1000000 *(int64_t)index;
    }

    void GenDataTestV3::set_rowkey_var_cust(int32_t index)
    {
        static int32_t i = 0;
        i++;
      isshop_++;
      if (isshop_ == HUN)
      {
        isshop_ = 0;
        creativeid_ ++;
        if (creativeid_ == TEN)
        {
          creativeid_ = 0;
          network_++;
          if (network_ == TEN)
          {
            network_ = 0;
            customid_ ++;
          }
        }
      }
      fprintf(stderr, "isshop=%ld, creativeid=%ld, network=%ld, custmoid=%ld, index=%d,i=%d\n",
              isshop_, creativeid_, network_, customid_, index, i);

      matchscope_ = demo[index%STRMAXINDEX];
      searchtype_ = demostr[index%STRMAXINDEX];
      searchtypereverse_ = demostrreverse[index%STRMAXINDEX];
      thedate_ = 1000000 *(int64_t)index;
    }
  }
}


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

int main(int argc, char *argv[])
{
  int ret = 0;
  Args arg;
  while((ret = getopt(argc, argv, ":t:d:b:s:vV")) != -1)
  {
    switch(ret)
    {
    case 'b':
      arg.block_size_ = strtoll(optarg, NULL, 10);
      break;

    case 's':
      arg.schema_file_ = optarg;
      break;

    case 'd':
      arg.disk_num_  = strtol(optarg, NULL, 10);
      break;

    case 't':
      arg.data_dest_ = optarg;
      break;

    case 'v':
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
      exit(1);

    case 'V':
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
      exit(1);
    }
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();

  GenDataTestV3 data_builder;
  ret = data_builder.init(&arg);
  if (OB_SUCCESS == ret)
  {
    ret = data_builder.generate();
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "data generate failed\n");
    }
  }
  else
  {
    fprintf(stderr, "init failed\n");
  }
}
