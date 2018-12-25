#include "dumpsst_v2.h"

using namespace oceanbase::common;
using namespace oceanbase::compactsstablev2;

const char* g_sstable_directory = NULL;
ObSSTableSchema g_schema;

bool safe_char_int32(char *src_str , int32_t &dst_num)
{
  bool ret = true;
  char *tmp_str = src_str;
  char *str = NULL;
  int str_len = 0;
  int dot_num = 0;
  int64_t src_num_64;
  int64_t src_num_32;
  while(*tmp_str != '\0')
  {
    if(*tmp_str > '9')
    {
      fprintf(stderr,"error int : invalid char\n");
      ret = false;
      break;
    }
    else if(*tmp_str < '0')
    {
      if(*tmp_str == ' ')
      {
        if(str_len == 0)
        {
          tmp_str++;
          continue;
        }else
        {
          fprintf(stderr,"error int : invalid space \n");
          ret = false;
          break;
        }
      }
      else if(*tmp_str == '+' || *tmp_str == '-')
      {
        dot_num++;
        if( 1 < dot_num )
        {
          fprintf(stderr,"error int : invalid + or -\n");
          ret = false;
          break;
        }
        else
        {
          str = tmp_str;
        }
      }else
      {
        fprintf(stderr,"error int : invalid char\n");
        ret = false;
        break;
      }
    }
    else
    {
      if (*tmp_str == '0' && str_len == 0)
      {
        if(*(tmp_str+1) != '\0')
        {
          tmp_str++;
          continue;
        }else
        {
          str = tmp_str;
        }
      }else if(dot_num == 0)
      {
        dot_num++;
        str = tmp_str;
      }
    }

    str_len++;
    tmp_str++;
  }//end of while

  if(str_len > 10 )
  {
    ret = false;
    fprintf(stderr,"error int : invalid length \n");
  }
  if( ret )
  {
    src_num_64 = atoll(str);
    src_num_32 = atoi(str);
    if(src_num_32 != src_num_64)
    {
      fprintf(stderr,"error int : overflow or underflow\n");
      ret = false;
    }
    /*
       if(src_num > 0xffffffff)
       {
       fprintf(stderr,"error int : overflow \n");
       return false;
       }else if (src_num < 0xffffff+1)
       {
       fprintf(stderr,"error int : underflow \n");
       return false;
       }
       dst_num = src_num & 0xffffffff;
       */
    dst_num = static_cast<int32_t>(src_num_32);
  }
  return ret;
}

void deal_cmd_line(char* optarg_tmp, CmdLineParam &clp)
{
  const char *split_ = ",[]()\0";
  char * tmp;
  int32_t tmp_num;
  int split_count = 0;
  bool left_paren = false;
  bool right_paren = false;
  char *b_opt_arg;
  int paren_match = 0;
  int  paren_exist = 0;//if exist paren
  char *ptr;
  char *ptr2;

  b_opt_arg = optarg_tmp;

  //find the dot
  ptr = strchr(b_opt_arg,',');
  ptr2 = strrchr(b_opt_arg,',');
  if ( NULL != ptr && NULL != ptr2 )
  {
    if ( ptr == b_opt_arg || *(ptr2+1) == '\0' )
    {
      fprintf(stderr,"invalid input of , \n");
      exit(1);
    }
    if ( ptr != ptr2 )
    {
      fprintf(stderr,"multi input of , \n");
      exit(1);
    }
  }

  //match the paren
  ptr = strchr(b_opt_arg,'(');
  if(ptr)
  {
    left_paren = true;
    paren_exist++;
    paren_match++;
  }

  ptr2 = strrchr(b_opt_arg,'(');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi left parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,')');
  if(ptr)
  {
    right_paren = true;
    paren_exist++;
    paren_match--;
  }
  ptr2 = strrchr(b_opt_arg,')');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi right parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,'[');
  if(ptr)
  {
    paren_match++;
    paren_exist++;
  }
  ptr2 = strrchr(b_opt_arg,'[');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi left parens \n");
    exit(1);
  }

  ptr = strchr(b_opt_arg,']');
  if(ptr)
  {
    paren_match--;
    paren_exist++;
  }
  ptr2 = strrchr(b_opt_arg,']');
  if(ptr2 != ptr)
  {
    fprintf(stderr,"multi right parens \n");
    exit(1);
  }

  //

  if ( paren_exist % 2 != 0 )
  {
    fprintf(stderr,"wrong num of parens ! \n");
    exit(1);
  }
  if ( 0 != paren_match )
  {
    fprintf(stderr,"wrong param , paren mismatch !\n");
    exit(1);
  }else if((0 == paren_match)
      && (2 != paren_exist && 0 != paren_exist))
  {
    fprintf(stderr,"wrong paren , only allow two parens !\n");
    exit(1);
  }

  tmp = strtok(b_opt_arg,split_);
  while( tmp!=NULL )
  {
    //check if all is num
    if( !safe_char_int32(tmp, tmp_num) )// !is_all_num(tmp)
    {
      fprintf(stderr,"wrong param , not all is number !\n");
      exit(1);
    }

    if( 0 == split_count )
    {
      clp.block_id_ = tmp_num ;//atoi(tmp);
      clp.block_n_ = clp.block_id_;
      split_count++;
    }
    else if( 1 == split_count )
    {
      if( clp.block_id_  <=  tmp_num )//( clp.block_id <= atoi(tmp) )
        clp.block_n_ = tmp_num; //clp.block_n = atoi(tmp);
      else
      {//swap that block_n should >= block_id
        clp.block_n_ = clp.block_id_;
        clp.block_id_ = tmp_num; //clp.block_id = atoi(tmp);
      }
      split_count++;
    }

    if( 2 < split_count )
    {
      //more than 2 param is wrong
      fprintf(stderr,"wrong param , more than 2 params !\n");
      exit(1);
    }

    tmp = strtok(NULL,split_);
    tmp_num = 0 ;
  }//end of while (tmp!=NULL)

  if(1 == split_count && paren_exist)
  {
    fprintf(stderr,"wrong param , miss one param in 2 parens!\n");
    // (id , ]
    exit(1);
  }

  if( 2 == split_count && 2 > paren_exist)
  {
    fprintf(stderr,"wrong param , miss parens !\n");
    exit(1);
  }
  //deal the params

  if( (-1 == clp.block_id_) && (-1 == clp.block_n_) )//get all blocks
    return ;

  if( (-1 >= clp.block_id_) || (-1 >= clp.block_n_) ){
    fprintf(stderr,"wrong param , input error !\n");
    exit(1);
  }

  if( (0 <= clp.block_id_) && (clp.block_n_ == clp.block_id_)
      && (!left_paren && !right_paren))
    return ;
  else
  {
    //deal the parens
    if(left_paren)
      clp.block_id_ = clp.block_id_ + 1;
    if(right_paren)
      clp.block_n_ = clp.block_n_ -1;
  }

  if( clp.block_id_ > clp.block_n_ )
  {
    fprintf(stderr,"wrong param , input error2 !\n");
    exit(1);
  }
}

void usage()
{
  printf("\n");
  printf("Usage: sstable_tools [OPTION]\n");
  printf("   -c| --cmd_type command type"
      "[dump_sstable|cmp_sstable|dump_meta|search_meta] \n");
  printf("   -D| --sstable_directory sstable directory \n");
  printf("   -I| --idx_file_name must be set while "
      "cmd_type is dump_meta\n");
  printf("   -f| --file_id sstable file id\n");
  printf("   -p| --dst_file_id must be set while cmp_type "
      "is cmp_sstable\n");
  printf("   -d| --dump_content [dump_index|dump_schema|dump_trailer] "
      "could be set while cmd_type is dump_sstable\n");
  printf("   -b| --block_id show rows number in block,could be set  "
      "while cmd_type is dump_sstable and dump_content "
      "is dump_trailer\n");
  printf("   -i| --tablet_version must be set while cmp_type "
      "is dump_meta \n");
  printf("   -r| --search_range must be set while cmp_type "
      "is search _meta\n");
  printf("   -t| --table_id must be set while cmp_type "
      "is search _meta\n");
  printf("   -a| --app_name must be set while cmp_type is search _meta\n");
  printf("   -x| --hex_format (0 plain string/1 hex string/2 number)\n");
  printf("   -h| --help print this help info\n");
  printf("   samples:\n");
  printf("   -c dump_meta -P idx_file_path "
      "[-i disk_no -v data_version -x]\n");
  printf("   -c dump_sstable -d dump_index -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_schema -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_trailer -P sstable_file_path\n");
  printf("   -c dump_sstable -d dump_block -P sstable_file_path "
      "-b block_id\n");
  printf("   -c search_meta -t table_id -r search_range -a app_name "
      "-x hex_format -D sstable_directory \n");
  printf("   -c change_meta -t table_id -r search_range -d action"
      "(remove_range/set_merged_flag/clear_merged_flag/"
      "find_tablet/change_range) "
      "-x hex_format -D sstable_directory "
      "[-i disk_no -v data_version -f dump_sstable -n new_range]\n");
  printf("\n");
  exit(0);
}

int parse_cmd_line(int argc, char** argv, CmdLineParam& clp)
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "c:f:p:d:b:i:r:t:a:I:D:hx:v:qo:s:n:P:S:";
  const char* file_path = NULL;
  struct option longopts[] =
  {
    {"help",0,NULL,'h'},
    {"cmd_type",1,NULL,'c'},
    {"idx_file_name",1,NULL,'I'},
    {"file_id",1,NULL,'f'},
    {"dst_file_id",1,NULL,'p'},
    {"sstable_directory",1,NULL,'D'},
    {"dump_content",1,NULL,'d'},
    {"block_id",1,NULL,'b'},
    {"tablet_version",1,NULL,'v'},
    {"disk_no",1,NULL,'i'},
    {"search_range",1,NULL,'r'},
    {"new_range",1,NULL,'n'},
    {"table_id",1,NULL,'t'},
    {"app_name",1,NULL,'a'},
    {"hex_format",0,NULL,'x'},
    {"quiet",0,NULL,'q'},
    {"output_sstable_directory",1,NULL,'o'},
    {"compressor_name",1,NULL,'s'},
    {"file_path",1,NULL,'P'},
    {"schema",1,NULL,'S'},
    {0,0,0,0}
  };

  while(-1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL)))
  {
    switch (opt)
    {
      case 'h':
        usage();
        break;
      case 'c':
        clp.cmd_type_ = optarg;
        break;
      case 'I':
        clp.idx_file_name_ = optarg;
        break;
      case 'f':
        clp.file_id_ = strtoll(optarg, NULL, 10);
        break;
      case 'p':
        clp.dst_file_id_ = strtoll(optarg, NULL, 10);
        break;
      case 'P':
        file_path = optarg;
        break;
      case 'D':
        g_sstable_directory = optarg;
        break;
      case 'd':
        clp.dump_content_ = optarg;
        break;
      case 'b':
        deal_cmd_line(optarg , clp);
        break;
      case 'v':
        clp.table_version_ = atoi(optarg);
        break;
      case 'i':
        clp.disk_no_ = atoi(optarg);
        break;
      case 'r':
        clp.search_range_ = optarg;
        break;
      case 'n':
        clp.new_range_ = optarg;
        break;
      case 't':
        clp.table_id_ = strtoll(optarg, NULL, 10);
        break;
      case 'a':
        clp.app_name_ = optarg;
        break;
      case 'x':
        clp.hex_format_ = static_cast<int32_t>(strtoll(optarg, NULL, 10));
        break;
      case 'q':
        clp.quiet_ = 1;
        break;
      case 'o':
        clp.output_sst_dir_ = optarg;
        fprintf(stderr,"out_put_sst_dir = %s\n",clp.output_sst_dir_);
        break;
      case 's':
        {
          clp.compressor_name_ = optarg;
          fprintf(stderr,"compressor_name = %s\n",clp.compressor_name_);
        }
        break;
      case 'S':
        clp.schema_file_ = optarg;
        break;
      default:
        usage();
        exit(1);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == clp.cmd_type_)
    {
      TBSYS_LOG(ERROR, "cmd_type is not set");
      usage();
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (0 == strcmp("dump_sstable", clp.cmd_type_))
    {
      if (NULL == clp.dump_content_)
      {
        TBSYS_LOG(ERROR, "-d must be appear when the cmd_type is"
            "dump_sstable");
        usage();
        ret = OB_ERROR;
      }
      else if (0 == clp.file_id_)
      {
        TBSYS_LOG(ERROR, "-f must be appear when the cmd_type is:"
            "dump_sstable");
        usage();
        ret = OB_ERROR;
      }
      else if (NULL == g_sstable_directory)
      {
        TBSYS_LOG(ERROR, "-D must be appear when the cmd_type is:"
            "dump_sstable");
        usage();
        ret = OB_ERROR;
      }
      else if (0 == strcmp("dump_block", clp.dump_content_))
      {
        if (0 == clp.table_id_)
        {
          TBSYS_LOG(ERROR, "-t must be appear when thc cmd_tye is"
              "dump_sstable and the dump_content is dump_block");
        }
        else
        {
          //TODO:block_id的判断
        }
      }

    }
  }

  return ret;
}

int DumpSSTableV2::open(const int64_t sstable_file_id)
{
  int ret = OB_SUCCESS;
  char filename[OB_MAX_FILE_NAME_LENGTH];

  if (sstable_file_id <= 0)
  {
    TBSYS_LOG(ERROR, "sstable_file_id <=0:sstable_file_id=%ld",
        sstable_file_id);
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_sstable_path(sstable_file_id,
          filename, OB_MAX_FILE_NAME_LENGTH)))
  {
    TBSYS_LOG(ERROR, "get sstable path error:ret=%d,sstable_file_id=%ld",
        ret, sstable_file_id);
  }
  else if (OB_SUCCESS != (ret = reader_.init(sstable_file_id)))
  {
    TBSYS_LOG(ERROR, "open sstable error:ret=%d,sstable_file_id=%ld",
        ret, sstable_file_id);
  }
  else if (util_.open(filename, O_RDONLY|O_EXCL) <= 0)
  {
    TBSYS_LOG(ERROR, "util open filename error:ret = %d,filename=%s",
        ret, filename);
  }
  else if (OB_SUCCESS != (ret = load_block_index()))
  {
    TBSYS_LOG(ERROR, "load block index error:ret=%d", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "dumpsstablev2 open success");
  }
  
  return ret;
}

int DumpSSTableV2::load_block_index()
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = reader_.get_table_cnt();
  const ObSSTableTableIndex* table_index = reader_.get_table_index();
  char* record_index_buf = NULL;
  char* record_endkey_buf = NULL;
  char* total_buf = NULL;
  int64_t record_index_offset = 0;
  int64_t record_endkey_offset = 0;
  int64_t record_index_size = 0;
  int64_t record_endkey_size = 0;
  int64_t record_size = 0;
  int64_t block_count = 0;

  if (NULL == (block_index_mgr_ = (ObSSTableBlockIndexMgr**)malloc(
          table_cnt * sizeof(ObSSTableBlockIndexMgr*))))
  {
    TBSYS_LOG(ERROR, "failed to malloc memory for block index:size=%ld",
        table_cnt * sizeof(ObSSTableBlockIndexMgr));
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < table_cnt; i ++)
    {
      record_index_offset = table_index[i].block_index_offset_
        + sizeof(ObRecordHeaderV2);
      record_index_size = table_index[i].block_index_size_
        - sizeof(ObRecordHeaderV2);
      record_endkey_offset = table_index[i].block_endkey_offset_
        + sizeof(ObRecordHeaderV2);
      record_endkey_size = table_index[i].block_endkey_size_
        - sizeof(ObRecordHeaderV2);
      block_count = table_index[i].block_count_;
      util_.lseek(record_index_offset, SEEK_SET);
      if (NULL == (record_index_buf = (char*)malloc(record_index_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for record index:"
            "record_index_size=%ld", record_index_size);
        ret = OB_ERROR;
      }
      else if (NULL == (record_endkey_buf = (char*)malloc(
              record_endkey_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for record endkey:"
            "record_endkey_size=%ld", record_endkey_size);
        ret = OB_ERROR;
      }
      else if (NULL == (total_buf = (char*)malloc(
              sizeof(ObSSTableBlockIndexMgr) + record_index_size
              + record_endkey_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for total:size=%ld",
            sizeof(ObSSTableBlockIndexMgr) + record_index_size
            + record_endkey_size);
        ret = OB_ERROR;
      }
      else if (record_index_size != (record_size = util_.read(
              record_index_buf, record_index_size)))
      {
        TBSYS_LOG(ERROR, "util read error:index_size=%ld,read_size=%ld",
            record_index_size, record_size);
        ret = OB_ERROR;
      }
      else if (record_endkey_size != (record_size = util_.read(
              record_endkey_buf, record_endkey_size)))
      {
        TBSYS_LOG(ERROR, "util read error:record_endkey_size=%ld,"
            "record_size=%ld", record_endkey_size, record_size);
        ret = OB_ERROR;
      }
      else
      {
        memcpy(total_buf + sizeof(ObSSTableBlockIndexMgr),
            record_index_buf, record_index_size);
        memcpy(total_buf + sizeof(ObSSTableBlockIndexMgr)
            + record_index_size, record_endkey_buf, record_endkey_size);
        if (NULL == (block_index_mgr_[i] = new(total_buf)
              ObSSTableBlockIndexMgr(record_index_size,
                record_endkey_size, block_count)))
        {
          TBSYS_LOG(WARN, "new block index mgr error");
          ret = OB_ERROR;
        }
      }
    }//end for
  }

  return ret;
}

/*
int DumpSSTableV2::load_block_index()
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = reader_.get_table_cnt();
  const ObSSTableTableIndex* table_index = reader_.get_table_index();
  if (NULL == (block_index_array_ = (ObSSTableBlockIndex**)malloc(
      table_cnt * sizeof(ObSSTableBlockIndex*))))
  {
    TBSYS_LOG(ERROR, "failed to malloc memory for block index:size=%ld",
        table_cnt * sizeof(ObSSTableBlockIndex*));
    ret = OB_ERROR;
  }
  else if (NULL == (block_endkey_array_ = (char**)malloc(
          table_cnt * sizeof(char*))))
  {
    TBSYS_LOG(ERROR, "failed to malloc memory for block endkey:size=%ld",
        table_cnt * sizeof(char*));
    ret = OB_ERROR;
  }
  
  if (OB_SUCCESS == ret)
  {
    int64_t index_offset = 0;
    int64_t index_size = 0;
    int64_t endkey_offset = 0;
    int64_t endkey_size = 0;
    int64_t read_size = 0;
    
    for (int64_t i = 0; i < table_cnt; i ++)
    {
      index_offset = table_index[i].block_index_offset_
        + sizeof(ObRecordHeaderV2);
      index_size = table_index[i].block_index_size_
        - sizeof(ObRecordHeaderV2);
      endkey_offset = table_index[i].block_endkey_offset_
        + sizeof(ObRecordHeaderV2);
      endkey_size = table_index[i].block_endkey_size_
        - sizeof(ObRecordHeaderV2);

      if (OB_SUCCESS == ret) 
      {
        util_.lseek(index_offset, SEEK_SET);
        if (NULL == (block_index_array_[i]
              = (ObSSTableBlockIndex*)malloc(index_size)))
        {
          TBSYS_LOG(ERROR, "failed to malloc memory for block index:"
              "index_size=%ld", index_size);
          ret = OB_ERROR;
        }
        else if (index_size != (read_size = util_.read(
                (char*)(block_index_array_[i]), index_size)))
        {
          TBSYS_LOG(ERROR, "util read error:index_size=%ld,read_size=%ld",
              index_size, read_size);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        util_.lseek(endkey_offset, SEEK_SET);
        if (NULL == (block_endkey_array_[i] = (char*)malloc(
                endkey_size)))
        {
          TBSYS_LOG(WARN, "failed to malloc memory for block endkey:"
              "endkey_size=%ld", endkey_size);
          ret = OB_ERROR;
        }
        else if (endkey_size != (read_size = util_.read(
                (char*)(block_endkey_array_[i]), endkey_size)))
        {
          TBSYS_LOG(ERROR, "util read error:endkey_size=%ld, read_size=%ld",
              endkey_size, read_size);
          ret = OB_ERROR;
        }
      }
    }
  }

  return ret;
}
*/

int DumpSSTableV2::load_display_block(
    const ObSSTableBlockIndexMgr& index, const int64_t block_id)
{
  int ret = OB_SUCCESS;
  const int64_t block_cnt = index.get_block_count();
  int64_t offset = 0;
  int64_t size = 0;
  int64_t record_size = 0;
  const ObSSTableBlockIndex* ptr = reinterpret_cast<
    const ObSSTableBlockIndex*>(index.get_block_index_base());
  char* record_buf = NULL;
  char* uncomp_buf = NULL;
  int64_t uncomp_size = 0;
  int64_t real_size = 0;
  ObRecordHeaderV2 record_header;
  const char* payload_ptr = NULL;
  int64_t payload_size = 0;
  ObSSTableBlockReader block_reader;
  ObSSTableBlockHeader* block_header = NULL;
  const ObCompactStoreType row_store_type = reader_.get_row_store_type();
  common::ObCompactCellIterator row;
  const ObObj* obj = NULL;
  bool is_row_finished = false;
  uint64_t column_id = 0;

  if (block_id >= block_cnt || block_id < 0)
  {
    TBSYS_LOG(ERROR, "block id error:block_id=%ld", block_id);
    ret = OB_ERROR;
  }
  else
  {
    offset = (ptr + block_id)->block_data_offset_;
    size = (ptr + block_id + 1)->block_data_offset_
      - (ptr + block_id)->block_data_offset_;
    util_.lseek(offset, SEEK_SET);
    if (NULL == (record_buf = (char*)malloc(size)))
    {
      TBSYS_LOG(ERROR, "failed to malloc memory:size=%ld", size);
      ret = OB_ERROR;
    }
    else if (size != (record_size = util_.read(record_buf, size)))
    {
      TBSYS_LOG(ERROR, "util read error:index_size=%ld,read_size=%ld",
          record_size, size);
      ret = OB_ERROR;
    }
    else if (OB_SUCCESS != (ret = ObRecordHeaderV2::check_record(
            record_buf, size, OB_SSTABLE_BLOCK_DATA_MAGIC,
            record_header, payload_ptr, payload_size)))
    {
      TBSYS_LOG(ERROR, "check record header error:ret=%d", ret);
    }
    else if (NULL == (uncomp_buf = (char*)malloc(
            record_header.data_length_)))
    {
      TBSYS_LOG(ERROR, "failed to alloc memeory for uncomp_buf:"
          "size=%ld", record_header.data_length_);
      ret = OB_ERROR;
    }
    else
    {
      if (record_header.data_length_ == record_header.data_zlength_)
      {
        memcpy(uncomp_buf, payload_ptr, record_header.data_length_);
        uncomp_size = record_header.data_length_;
        real_size = record_header.data_length_;
      }
      else
      {
        uncomp_size = record_header.data_length_;
        if (OB_SUCCESS != (ret = reader_.get_decompressor()
              ->decompress(payload_ptr, payload_size, uncomp_buf,
                uncomp_size, real_size)))
        {
          TBSYS_LOG(ERROR, "decompressor error:ret=%d", ret);
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      block_header = reinterpret_cast<ObSSTableBlockHeader*>(uncomp_buf);
      int64_t array_size = sizeof(ObSSTableBlockReader::RowIndexItemType)
        * (block_header->row_count_ + 1);
      char row_index_array[array_size];
      ObSSTableBlockReader::BlockData block_data(row_index_array, 
          array_size, uncomp_buf, real_size);   
      ObSSTableBlockReader::iterator tmp_index;
      ObSSTableBlockReader::const_iterator begin_index;
      ObSSTableBlockReader::const_iterator end_index;

      if (OB_SUCCESS != (ret = block_reader.init(block_data,
              row_store_type)))
      {
        TBSYS_LOG(ERROR, "block reader init error:ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        begin_index = block_reader.begin_index();
        end_index = block_reader.end_index();
        tmp_index = const_cast<ObSSTableBlockReader::iterator>(
            begin_index);
        printf("---block num=%ld---\n", block_id);
        printf("row_index_offset_=%d\n",
            block_header->row_index_offset_);
        printf("row_count_=%d\n", block_header->row_count_);
        printf("reserved64_=%ld\n", block_header->reserved64_);
        for (int i = 0; tmp_index  < end_index; tmp_index ++, i ++)
        {
          printf("--row num=%d--:", i);
          if (OB_SUCCESS != (ret = block_reader.get_row(tmp_index,
                  row)))
          {
            TBSYS_LOG(ERROR, "block reader get row error:ret=%d", ret);
            break;
          }
          else
          {
            printf("row key=");
            while (true)
            {
              if (OB_SUCCESS != (ret = row.next_cell()))
              {
                TBSYS_LOG(ERROR, "next cell error:ret=%d", ret);
                break;
              }
              else if (OB_SUCCESS != (ret = row.get_cell(obj,
                      &is_row_finished)))
              {
                TBSYS_LOG(ERROR, "row get cell error:ret=%d", ret);
                break;
              }
              else
              {
                if (is_row_finished)
                {
                  break;
                }
                else
                {
                  printf("%s,", to_cstring(*obj));
                }
              }
            }

            if (OB_SUCCESS == ret)
            {
              while (true)
              {
                if (OB_SUCCESS != (ret = row.next_cell()))
                {
                  TBSYS_LOG(ERROR, "next cell error:ret=%d", ret);
                  break;
                }
                else
                {
                  if (DENSE_DENSE == row_store_type)
                  {
                    if (OB_SUCCESS != (ret = row.get_cell(obj,
                            &is_row_finished)))
                    {
                      TBSYS_LOG(ERROR, "get cell error:ret=%d", ret);
                      break;
                    }
                    else
                    {
                      if (is_row_finished)
                      {
                        break;
                      }
                      else
                      {
                        printf("%s,", to_cstring(*obj));
                      }
                    }
                  }
                  else if (DENSE_SPARSE == row_store_type)
                  {
                    if (OB_SUCCESS != (ret = row.get_cell(column_id, obj,
                            &is_row_finished)))
                    {
                      TBSYS_LOG(ERROR, "get cell error:ret=%d", ret);
                      break;
                    }
                    else
                    {
                      if (is_row_finished)
                      {
                        break;
                      }
                      else
                      {
                        printf("(%lu,%s),", column_id, to_cstring(*obj));
                      }
                    }
                  }
                }
              }
            }

            if (OB_SUCCESS != ret)
            {
              break;
            }
          }

          printf("\n");
        }
      }
    }
  }

  return ret;
}

//start_block_id==-1-->min
//end_block_id==-1--->max
int DumpSSTableV2::load_display_blocks(const uint64_t table_id,
  const int64_t start_block_id,
  const int64_t end_block_id)
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = reader_.get_table_cnt();
  const ObSSTableTableIndex* table_index = reader_.get_table_index();
  ObSSTableBlockIndexMgr* block_index_mgr_ptr = NULL;
  int64_t real_start_id = 0;
  int64_t real_end_id = 0;
  int64_t cur_table_i = 0;

  for (int64_t i = 0; i < table_cnt; i ++, cur_table_i ++)
  {
    if (table_index[i].table_id_ == table_id)
    {
      block_index_mgr_ptr = block_index_mgr_[i];
      break;
    }
  }

  if (cur_table_i == table_cnt)
  {
    TBSYS_LOG(ERROR, "table is not exist:table_id=%lu", table_id);
    ret = OB_ERROR;
  }
  else if (start_block_id > end_block_id)
  {
    TBSYS_LOG(ERROR, "start_block_id > end_block_id:start_block_id=%ld,"
        "end_block_id=%ld", start_block_id, end_block_id);
    ret = OB_ERROR;
  }
  else if (end_block_id >= block_index_mgr_ptr->get_block_count())
  {
    TBSYS_LOG(ERROR, "end_block_id is overflow:end_block_id=%ld",
        end_block_id);
    ret = OB_ERROR;
  }
  else
  {
    if (-1 == start_block_id)
    {
      real_start_id = 0;
    }
    else
    {
      real_start_id = start_block_id;
    }

    if (-1 == end_block_id)
    {
      real_end_id = block_index_mgr_ptr->get_block_count();
    }
    else
    {
      real_end_id = end_block_id;
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = real_start_id; i <= real_end_id; i ++)
    {
      if (OB_SUCCESS != (ret = load_display_block(
              *block_index_mgr_ptr, i)))
      {
        TBSYS_LOG(ERROR, "load block error:ret=%d,i=%ld", ret, i);
        break;
      }
    }
  }

  return ret;
}

int DumpSSTableV2::display_sstable_size()
{
  int ret = OB_SUCCESS;
  const int64_t size = reader_.get_sstable_size();
  
  printf("sstable_size=%ld\n", size);

  return ret;
}

int DumpSSTableV2::display_sstable_trailer()
{
  int ret = OB_SUCCESS;
  const ObSSTableHeader* sstable_header = reader_.get_sstable_header();

  printf("SPARSE==0,DENSE==1,DENSE_SPARSE==2,DENSE_DENSE==3,INVALID_COMPACT_STORE_TYPE==4");
  printf("header_size_=%d\n"
      "header_version_=%d\n"
      "checksum_=%lu\n"
      "major_version_=%ld\n"
      "major_frozen_time_=%ld\n"
      "next_transaction_id_=%ld\n"
      "next_commit_log_id_=%ld\n"
      "start_minor_version_=%d\n"
      "end_minor_version_=%d\n"
      "is_final_minor_version_=%d\n"
      "row_stroe_type_=%d\n"
      "reserved16_=%d\n"
      "first_table_id_=%lu\n"
      "table_count_=%d\n"
      "table_index_unit_size_=%d\n"
      "table_index_offset_=%ld\n"
      "schema_array_unit_count_=%d\n"
      "schema_array_unit_size_=%d\n"
      "schema_record_offset_=%ld\n"
      "blocksize_=%ld\n"
      "compressor_name_=%s\n",
    sstable_header->header_size_,
    sstable_header->header_version_,
    sstable_header->checksum_,
    sstable_header->major_version_,
    sstable_header->major_frozen_time_,
    sstable_header->next_transaction_id_,
    sstable_header->next_commit_log_id_,
    sstable_header->start_minor_version_,
    sstable_header->end_minor_version_,
    sstable_header->is_final_minor_version_,
    sstable_header->row_store_type_,
    sstable_header->reserved16_,
    sstable_header->first_table_id_,
    sstable_header->table_count_,
    sstable_header->table_index_unit_size_,
    sstable_header->table_index_offset_,
    sstable_header->schema_array_unit_count_,
    sstable_header->schema_array_unit_size_,
    sstable_header->schema_record_offset_,
    sstable_header->block_size_,
    sstable_header->compressor_name_);
  printf("reserved64_=");
  for (int64_t i = 0; i < 64; i ++)
  {
    printf("%ld,", sstable_header->reserved64_[i]);
  }
  printf("\n");

  return ret;
}

int DumpSSTableV2::display_sstable_schema()
{
  int ret = OB_SUCCESS;
  const ObSSTableSchema* schema = reader_.get_schema();
  const int64_t column_cnt = schema->get_column_count();
  const ObSSTableSchemaColumnDef* def = NULL;

  printf("--table_id_--column_id_--column_value_type_--offset_-"
      "-rowkey_seq_--\n");
  for (int64_t i = 0; i < column_cnt; i ++)
  {
    if (NULL != (def = schema->get_column_def(i)))
    {
      printf("%lu,%lu,%d,%ld,%ld\n", def->table_id_, def->column_id_,
          def->column_value_type_, def->offset_, def->rowkey_seq_);
    }
    else
    {
      TBSYS_LOG(ERROR, "get column def error:def==NULL");
      ret = OB_ERROR;
      break;
    }
  }

  return ret;
}

int DumpSSTableV2::display_table_index()
{
  int ret = OB_SUCCESS;
  int64_t table_cnt = reader_.get_table_cnt();
  const ObSSTableTableIndex* table_index = reader_.get_table_index();

  for (int64_t i = 0; i < table_cnt; i ++)
  {
    printf("====================================\n");
    printf("size_=%d\nversion_=%d\ntable_id_=%lu\ncolumn_count_=%d\n"
        "columns_in_rowkey_=%d\nrow_count_=%ld\nblock_count_=%ld\n"
        "block_data_offset_=%ld\nblock_data_size_=%ld\n"
        "block_index_offset=%ld\nblock_index_size_=%ld\n"
        "block_endkey_offset_=%ld\nblock_endkey_size_=%ld\n"
        "bloom_filter_hash_count_=%ld\nbloom_filter_offset_=%ld\n"
        "bloom_filter_size_=%ld\nrange_keys_offset_=%ld\n"
        "range_start_key_length_=%d\nrange_end_key_length_=%d\n"
        "reserved_[]=%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld\n",
        table_index[i].size_,
        table_index[i].version_,
        table_index[i].table_id_,
        table_index[i].column_count_,
        table_index[i].columns_in_rowkey_,
        table_index[i].row_count_,
        table_index[i].block_count_,
        table_index[i].block_data_offset_,
        table_index[i].block_data_size_,
        table_index[i].block_index_offset_,
        table_index[i].block_index_size_,
        table_index[i].block_endkey_offset_,
        table_index[i].block_endkey_size_,
        table_index[i].bloom_filter_hash_count_,
        table_index[i].bloom_filter_offset_,
        table_index[i].bloom_filter_size_,
        table_index[i].range_keys_offset_,
        table_index[i].range_start_key_length_,
        table_index[i].range_end_key_length_,
        table_index[i].reserved_[0],
        table_index[i].reserved_[1],
        table_index[i].reserved_[2],
        table_index[i].reserved_[3],
        table_index[i].reserved_[4],
        table_index[i].reserved_[5],
        table_index[i].reserved_[6],
        table_index[i].reserved_[7]);
  }

  return ret;
}

int DumpSSTableV2::display_table_bloomfilter()
{
  int ret = OB_SUCCESS;
  return ret;
}

int DumpSSTableV2::display_table_range()
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = reader_.get_table_cnt();
  const ObNewRange* table_range = reader_.get_table_range();

  for (int64_t i = 0; i < table_cnt; i ++)
  {
    printf("=======================================\n");
    printf("table_id_=%lu\n", table_range[i].table_id_);
    printf("border_flag_.is_min_row=%d\n",
        table_range[i].border_flag_.is_min_value());
    printf("border_flag_.is_max_row=%d\n",
        table_range[i].border_flag_.is_max_value());
    printf("border_flag_.is_inclusive_start=%d\n",
        table_range[i].border_flag_.inclusive_start());
    printf("border_flag_.is_inclusive_end=%d\n",
        table_range[i].border_flag_.inclusive_end());
    printf("start_key_=%s\n", to_cstring(table_range[i].start_key_));
    printf("end_key_=%s\n", to_cstring(table_range[i].end_key_));
  }

  return ret;
}

int DumpSSTableV2::display_block_index()
{
  int ret = OB_SUCCESS;
  int64_t block_cnt = 0;
  const ObSSTableTableIndex* table_index = reader_.get_table_index();
  const int64_t table_cnt = reader_.get_table_cnt();
  //ObCompactCellIterator row;
  //const ObObj* obj = NULL;
  //bool is_row_finished = false;
  const char* block_index_base = NULL;
  const char* block_endkey_base = NULL;
  const ObSSTableBlockIndex* tmp_index_base = NULL;

  for (int64_t i = 0; i < table_cnt; i ++)
  {
    printf("=================table_id=%ld============\n",
        table_index[i].table_id_);
    block_cnt = table_index[i].block_count_;
    block_index_base = block_index_mgr_[i]->get_block_index_base();
    block_endkey_base = block_index_mgr_[i]->get_block_endkey_base();
    tmp_index_base = reinterpret_cast<const ObSSTableBlockIndex*>
      (block_index_base);
    for (int64_t j = 0; j < block_cnt; j ++)
    {
      printf("---block_num=%ld---\n", j);
      printf("offset=%ld\n", tmp_index_base[j].block_data_offset_);
      printf("size=%ld\n", tmp_index_base[j + 1].block_data_offset_
          - tmp_index_base[j].block_data_offset_);
    }
  }

  return ret;
}

int main(const int argc, char** argv)
{
  int ret = OB_SUCCESS;
  CmdLineParam clp;

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = parse_cmd_line(argc, argv, clp)))
    {
      TBSYS_LOG(ERROR, "parse cmd line error:argc=%d", argc);
    }
    else if (clp.quiet_)
    {
      TBSYS_LOGGER.setLogLevel("WARN");
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = ob_init_memory_pool()))
    {
      TBSYS_LOG(ERROR, "ob_init_memory_pool error:ret=%d", ret);
    }
  }

  if (0 == strcmp("dump_sstable", clp.cmd_type_))
  {
    DumpSSTableV2 dump;
    if (OB_SUCCESS != (ret = dump.open(clp.file_id_)))
    {
      TBSYS_LOG(ERROR, "failed to open sstable file:clp.file_id=%ld",
          clp.file_id_);
    }
    else if (0 == strcmp("dump_sstable_size", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_sstable_size()))
      {
        TBSYS_LOG(ERROR, "dump sstable size error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_sstable_trailer", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_sstable_trailer()))
      {
        TBSYS_LOG(ERROR, "dump trailer error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_sstable_schema", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_sstable_schema()))
      {
        TBSYS_LOG(ERROR, "dump sstable schema error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_table_index", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_table_index()))
      {
        TBSYS_LOG(ERROR, "dump table index error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_table_bloomfilter", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_table_bloomfilter()))
      {
        TBSYS_LOG(ERROR, "dump table bloomfilter error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_table_range", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_table_range()))
      {
        TBSYS_LOG(ERROR, "dump table range error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_block_index", clp.dump_content_))
    {
      if (OB_SUCCESS != (ret = dump.display_block_index()))
      {
        TBSYS_LOG(ERROR, "dump block index error:ret=%d", ret);
      }
    }
    else if (0 == strcmp("dump_block", clp.dump_content_))
    {
      printf("table_id_=%lu\n", clp.table_id_);
      printf("block_id_=%d\n", clp.block_id_);
      printf("block_n_=%d\n", clp.block_n_);
      if (OB_SUCCESS != (ret = dump.load_display_blocks(clp.table_id_,
              clp.block_id_, clp.block_n_)))
      {
        TBSYS_LOG(ERROR, "dump load display blocks error:ret=%d", ret);
      }
    }
  }

  return ret;
}
