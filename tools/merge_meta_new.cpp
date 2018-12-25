#include <dirent.h>
#include <errno.h>
#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/ob_malloc.h"
#include "common/ob_crc64.h"
#include "chunkserver/ob_tablet.h"
#include "chunkserver/ob_tablet_image.h"

const char *g_sstable_directory = "./";

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace sstable;

    class ObMergeMetaNew
    {
      public:
        ObMergeMetaNew (const char *app_name,
                        const char *data_dir,
                        bool set_ring = false,
                        bool drop_file = false,
                        bool just_merge = false,
                        int32_t version = 1) : app_name_(app_name),data_dir_(data_dir),
                                                  set_ring_(set_ring),drop_file_(drop_file),
                                                  just_merge_(just_merge),version_(version)
      {
        image_final_.set_data_version(version_);
        image_tmp_.set_data_version(version_);
        image_.set_data_version(version_);
      }

        ~ObMergeMetaNew() {}
        int read_file_list(const char *file_list);
        int parse(const char *idx_file);
      private:
        int process_ring_range();
        int drop_not_exist_file();
        bool is_file_exists(const char *file);
      private:
        const char *app_name_;
        const char *data_dir_;
        bool set_ring_;
        bool drop_file_;
        bool just_merge_;
        int32_t version_;
        ObTabletImage image_final_;
        ObTabletImage image_tmp_;
        ObTabletImage image_;
    };

    int ObMergeMetaNew::read_file_list(const char *file_list)
    {
      int ret = OB_SUCCESS;
      FILE *fp = NULL;
      if (NULL == file_list || '\0' == *file_list)
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
        char buf[256];
        while( NULL != fgets(buf,sizeof(buf),fp) )
        {
          if ( '\n' == buf[strlen(buf) - 1])
          {
            buf[strlen(buf)-1] = '\0';
          }

          if ( (ret = parse(buf)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"parse %s failed [%d]",buf,ret);
            break;
          }
        }

        if (set_ring_ && (OB_SUCCESS == ret) && ( (ret = process_ring_range()) != OB_SUCCESS))
        {
          TBSYS_LOG(ERROR,"parse ring failed: [%d]",ret);
        }

        if (just_merge_ && (OB_SUCCESS == ret) && ((ret = image_.write("/tmp/idx_all",0)) != OB_SUCCESS))
        {
          TBSYS_LOG(ERROR,"write meta filed");
        }

        if (drop_file_ && (OB_SUCCESS == ret) && ((ret = drop_not_exist_file()) != OB_SUCCESS))
        {
          TBSYS_LOG(ERROR,"drop not exist file failed: [%d]",ret);
        }

        if (!just_merge_ && OB_SUCCESS == ret)
        {
          char idx_path[common::OB_MAX_FILE_NAME_LENGTH];
          char gsd_str[common::OB_MAX_FILE_NAME_LENGTH];

          for(int32_t i=1;i<=10;++i)
          {
            snprintf(gsd_str, sizeof(gsd_str), "%s/%d/%s/sstable/", data_dir_, i, app_name_);
            g_sstable_directory = gsd_str;
            snprintf(idx_path,sizeof(idx_path),"%s/%d/%s/sstable/idx_%d",data_dir_,i,app_name_,i);
            if ((ret = image_final_.write(idx_path,i)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"write failed [%d]",ret);
            }
            image_final_.dump();
          }
        }
      }
      return ret;
    }

    int ObMergeMetaNew::parse(const char *idx_file)
    {
      int ret = OB_SUCCESS;
      int fd = -1;
      struct stat st;
      if ( (NULL == idx_file) || ((fd = open(idx_file,O_RDONLY)) < 0))
      {
        perror("open indx_file failed");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO,"deal %s\n",idx_file);

        if ( (fstat(fd,&st) != 0) || (st.st_size <= 0))
        {
          TBSYS_LOG(ERROR,"fstat failed");
          ret = OB_ERROR;
        }
      }

      // /data/xx/dfa/idx_[0-9]{1,2}.200xxxx
      char dbuf[3] = {'\0','\0','\0'};
      char *p = (char*)strrchr(idx_file,'_');

      dbuf[0] = *(p + 1);
      dbuf[1] = *(p + 2) == '.' ? '\0' : *(p + 2);
      int32_t disk_no = atoi(dbuf);
      ObSSTableId sstable_id;

      TBSYS_LOG(INFO,"disk_no is %d,ret is %d",disk_no,ret);

      image_tmp_.set_data_version(version_);

      if ((OB_SUCCESS == ret) &&  (ret = image_tmp_.read(idx_file,disk_no)) != OB_SUCCESS)
      {
        TBSYS_LOG(INFO,"deserialize image failed.");
      }

      if (OB_SUCCESS == ret)
      {
        ObTablet *tablet = NULL;
        ObTablet *tablet_new = NULL;
        if (OB_SUCCESS == (ret = image_tmp_.begin_scan_tablets()))
        {
          ret = image_tmp_.get_next_tablet(tablet);
          while(OB_SUCCESS == ret && tablet != NULL)
          {
            if ( OB_SUCCESS != (ret = image_.alloc_tablet_object(tablet->get_range(),tablet_new)))
            {
              TBSYS_LOG(ERROR,"alloc tablet object failed : [%d]",ret);
            }

            TBSYS_LOG(INFO,"id is %ld",tablet->get_sstable_id_list().at(0).sstable_file_id_);
            tablet_new->add_sstable_by_id((tablet->get_sstable_id_list().at(0)));
            tablet_new->set_disk_no( tablet->get_sstable_id_list().at(0).sstable_file_id_ & 0xff);

            tablet_new->set_data_version(tablet->get_data_version());

            if (OB_SUCCESS == ret && ((ret = image_.add_tablet(tablet_new)) != OB_SUCCESS))
            {
              TBSYS_LOG(ERROR,"add tablet failed : [%d]",ret);
            }

            image_tmp_.release_tablet(tablet);
            ret = image_tmp_.get_next_tablet(tablet);
          }
          image_tmp_.end_scan_tablets();
          TBSYS_LOG(INFO,"destroy image_tmp_");
          image_tmp_.destroy();
        }
      }

      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      TBSYS_LOG(INFO,"total tablet:%d",image_.get_tablets_num());
      return ret;
    }

    int ObMergeMetaNew::process_ring_range()
    {
      ObTablet *tablet = NULL;
      ObTablet *last_tablet = NULL;
      bool set_this_min = false;
      bool set_last_max = false;
      bool set_this_start_key = false;
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO,"process ring range");

      if (OB_SUCCESS == (ret = image_.begin_scan_tablets()))
      {
        ret = image_.get_next_tablet(tablet);
        while(OB_SUCCESS == ret && tablet != NULL)
        {
          if (NULL == last_tablet)
          {
            //set this tablet's min value
            set_this_min = true;
          }
          else
          {
            if (last_tablet->get_range().table_id_ != tablet->get_range().table_id_)
            {
              //set last tablet's max value
              //set this tablet's min value
              set_last_max = true;
              set_this_min = true;
            }
            else
            {
              //set this tablet's start_key to last tablet end_key
              set_this_start_key = true;
            }
          }

          if (set_this_min)
          {
            ObNewRange range = tablet->get_range();
            range.start_key_.set_min_row();
            tablet->set_range(range);
          }

          if (set_last_max)
          {
            ObNewRange range = last_tablet->get_range();
            range.end_key_.set_max_row();
            last_tablet->set_range(range);
          }

          if (set_this_start_key)
          {
            ObNewRange range = tablet->get_range();
            range.start_key_ = last_tablet->get_range().end_key_;
            tablet->set_range(range);
          }

          set_this_min = false;
          set_last_max = false;
          set_this_start_key = false;

          last_tablet = tablet;

          tablet->dump();
          image_.release_tablet(tablet);
          ret = image_.get_next_tablet(tablet);
        }

        image_.end_scan_tablets();

        if (last_tablet != NULL)
        {
          ObNewRange range = last_tablet->get_range();
          range.end_key_.set_max_row();

          last_tablet->set_range(range);
        }
      }
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObMergeMetaNew::drop_not_exist_file()
    {
      ObTablet *tablet = NULL;
      ObTablet *tablet_tmp = NULL;
      char path[256];
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO,"drop_not_exit_file");
      if (OB_SUCCESS == (ret = image_.begin_scan_tablets()))
      {
        ret = image_.get_next_tablet(tablet);
        while(OB_SUCCESS == ret && tablet != NULL)
        {
          const common::ObArray<ObSSTableId>&  ids = tablet->get_sstable_id_list();
          int64_t id = ids.at(0).sstable_file_id_;
          snprintf(path,sizeof(path),"%s/%d/%s/sstable/%ld",data_dir_,(int)(id & 0xff),app_name_,id);
          if (!is_file_exists(path))
          {
            TBSYS_LOG(INFO,"file %s is not exist",path);
          }
          else
          {
            TBSYS_LOG(INFO,"file %s is exist",path);
            //copy meta
            if ( OB_SUCCESS != (ret = image_final_.alloc_tablet_object(tablet->get_range(),tablet_tmp)))
            {
              TBSYS_LOG(ERROR,"alloc tablet object failed : [%d]",ret);
            }

            //TBSYS_LOG(INFO,"id is %ld",tablet->get_sstable_id_list().at(0).sstable_file_id_);
            tablet_tmp->add_sstable_by_id((tablet->get_sstable_id_list().at(0)));
            tablet_tmp->set_disk_no( tablet->get_sstable_id_list().at(0).sstable_file_id_ & 0xff);

            tablet_tmp->set_data_version(tablet->get_data_version());

            if (OB_SUCCESS == ret && ((ret = image_final_.add_tablet(tablet_tmp)) != OB_SUCCESS))
            {
              TBSYS_LOG(ERROR,"add tablet failed : [%d]",ret);
            }
          }
          image_.release_tablet(tablet);
          ret = image_.get_next_tablet(tablet);
        }
        image_.end_scan_tablets();
      }
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      TBSYS_LOG(INFO,"this host has %d tablets",image_.get_tablets_num());
      return ret;
    }

    bool ObMergeMetaNew::is_file_exists(const char *file)
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

  } /* chunkserver */
} /* oceanbase */


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

int main(int argc,char **argv)
{
  int i = 0;
  int32_t version = 0;
  const char *file_list = NULL;
  const char *data_dir = NULL;
  const char *app_name = NULL;
  bool set_ring = false;
  bool drop_file = false;
  bool just_merge = false;
  while ((i = getopt(argc, argv, "f:n:d:remv:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        file_list = optarg;
        break;
      case 'n':
        app_name = optarg;
        break;
      case 'd':
        data_dir = optarg;
        break;
      case 'r':
        set_ring = true;
        break;
      case 'e':
        drop_file = true;
        break;
      case 'm':
        just_merge = true;
        break;
      case 'v':
        version = atoi(optarg);
        break;
      default:
        fprintf(stderr, "Usage: %s "
            "-f file_list -n app_name -d data_dir -r(set ring) -e (drop not exists file) -v version \n", argv[0]);
        return OB_ERROR;
    }
  }

  if (NULL == app_name || NULL == data_dir || NULL == file_list)
  {
    fprintf(stderr,"app_name or data_dir is null\n");
    fprintf(stderr, "Usage: %s "
            "-f file_list -n app_name -d data_dir -r(set ring) -e (drop not exists file) \n", argv[0]);
    exit(1);
  }
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();

  TBSYS_LOGGER.setLogLevel("INFO");

  ObMergeMetaNew merge(app_name,data_dir,set_ring,drop_file,just_merge,version);
  merge.read_file_list(file_list);

  return 0;
}
