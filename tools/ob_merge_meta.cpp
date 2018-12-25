#include <dirent.h>
#include <errno.h>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_range.h"
#include "common/ob_crc64.h"
#include "common/ob_malloc.h"
#include "ob_tablet_meta.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    using namespace common;

    using oceanbase::obsolete::chunkserver::ObTabletMeta;

    class ObMergeMeta 
    {
      public:
        ObMergeMeta ():is_first_key_(true),is_first_file_(true),timestamp_(0),entries_(0),dest_meta_(NULL){}
        ~ObMergeMeta() {arena_.free();}
        int scan_file(const char *path,const char *dest_meta);
        int parse(const char *idx_file);
      private:
        bool is_file_exists(const char *file);
        bool range_check(ObRange& cur);
        CharArena arena_;
        ObTabletMeta meta_;
        bool is_first_key_;
        bool is_first_file_;
        ObRange last_range_;
        int64_t timestamp_;
        int64_t entries_;
        const char *dest_meta_;
    };

    int ObMergeMeta::scan_file(const char *file_list,const char *meta_file)
    {
      int ret = OB_SUCCESS;
      FILE *fp = NULL;
      if (NULL == file_list || '\0' == *file_list || NULL == meta_file || '\0' == *meta_file)
      {
        ret = OB_ERROR;
      }
      else if (NULL == (fp = fopen(file_list,"r")))
      {
        printf("open file failed\n");
        ret = OB_ERROR;
      }
      else
      {
        dest_meta_ = meta_file;
        char buf[256];
        while( NULL != fgets(buf,sizeof(buf),fp) )
        {
          if ( '\n' == buf[strlen(buf) -1])
          {
            buf[strlen(buf)-1] = '\0';
          }
          parse(buf);
        }
        meta_.write_meta_info();
        fclose(fp);
      }
      return 0;
    }

    bool ObMergeMeta::is_file_exists(const char *file)
    {
      bool ret = false;
      if (NULL == file)
      {
        ret = false;
      }
      else
      {
        if (access(file,R_OK) != 0)
        {
          ret = false;
        }
        else
        {
          ret = true;
        }
      }
      return ret;
    }
        
    bool ObMergeMeta::range_check(ObRange& cur)
    {
      //current range must greater than last_range 
      bool ret = false;

      assert(cur.end_key_ > cur.start_key_);

      if (cur.table_id_ != last_range_.table_id_)
      {
        ret = true;
      }
      else
      {
        if ((cur.start_key_ < last_range_.end_key_) && !is_first_key_)
        {
          ret = false;
        }
        else
        {
          ret = true;
        }
      }
      return ret;
    }

    int ObMergeMeta::parse(const char *idx_file)
    {
      int ret = OB_ERROR;
      int fd = -1;
      if ( (NULL == idx_file) || ((fd = open(idx_file,O_RDONLY)) < 0))
      {
        perror("open indx_file failed");
        ret = OB_ERROR;
      }
      else
      {
        printf("deal %s\n",idx_file);
        struct stat st;
        if (fstat(fd,&st) != 0)
        {
          ret = OB_ERROR;
        }
        if (st.st_size <= 0)
        {
          ret = OB_ERROR;
        }
        else
        {
          char *buf = arena_.alloc(st.st_size);
          if (read(fd,buf,st.st_size) != st.st_size)
          {
            ret = OB_ERROR;
            arena_.free();
          }
          else
          {
            if ( is_first_file_)
            {
              meta_.init(dest_meta_);
            }
            ObTabletMeta::ObMetaHead meta_head_;
            int64_t pos = 0;
            if (OB_ERROR == meta_head_.deserialize(buf,st.st_size,pos))
            {
              ret = OB_ERROR;
            }
            else
            {
              timestamp_ = meta_head_.timestamp_;
              entries_ += meta_head_.entries_;

              ObTabletMeta::IndexEntry e;
              while( OB_SUCCESS == e.deserialize(buf,st.st_size,pos))
              {
                if (!is_file_exists(e.path_.ptr()))
                {
                  printf("file is not exists:%s\n",e.path_.ptr());
                  last_range_ = e.range_;
                  is_first_key_ = false;
                  memset(&e,0,sizeof(e));
                  continue;
                }
                if (!range_check(e.range_))
                {
                  printf("range is invalid\n");
                  assert(0);
                  memset(&e,0,sizeof(e));
                  continue;
                }
                if ((!is_first_file_) && is_first_key_ && e.range_.table_id_ == last_range_.table_id_)
                {
                  printf("chang start_key\n");
                  e.range_.start_key_ = last_range_.end_key_;
                  e.range_.border_flag_.unset_min_value();
                  is_first_key_ = false;
                }
                else if ( is_first_file_ && is_first_key_ )
                {
                  e.range_.border_flag_.set_min_value();
                  is_first_file_ = false;
                  is_first_key_ = false;
                }
                if (OB_ERROR ==  (ret = meta_.add_entry(e)))
                {
                  printf("add entry failed\n");
                  assert(0);
                }
                last_range_ = e.range_;
                memset(&e,0,sizeof(e));
              }
              printf("set is_first_key_ = true\n");
              is_first_key_ = true;
              is_first_file_ = false;
            }
          }
        }
      }
      return ret;
    }
  } /* chunkserver */
} /* oceanbase */


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

int main(int argc,char **argv)
{
  if (argc != 3)
  {
    printf("usage:%s file_list dest_meta_file\n",argv[0]);
    exit(-1);
  }
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM); 
  ob_init_memory_pool();
  ObMergeMeta merge;
  merge.scan_file(argv[1],argv[2]);

  return 0;
}
