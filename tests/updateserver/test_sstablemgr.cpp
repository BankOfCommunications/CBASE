#include "common/ob_malloc.h"
#include "updateserver/ob_sstable_mgr.h"

#define CLOG_ID 1

#define SSTABLE_FNAME "10_1-1_1.sst"
#define SSTABLE_ID SSTableMgr::sstable_str2id(SSTABLE_FNAME, CLOG_ID)

#define SSTABLE_FNAME2 "10_4-4_7.sst"
#define SSTABLE_ID2 SSTableMgr::sstable_str2id(SSTABLE_FNAME2, CLOG_ID)

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    class OBS : public ISSTableObserver
    {
      public:
        OBS() {};
        virtual ~OBS() {};
      public:
        virtual int add_sstable(const uint64_t sstable_id)
        {
          TBSYS_LOG(INFO, "add sstable id=%lu", sstable_id);
          return OB_SUCCESS;
        };
        virtual int erase_sstable(const uint64_t sstable_id)
        {
          TBSYS_LOG(INFO, "erase sstable id=%lu", sstable_id);
          return OB_SUCCESS;
        };
        virtual int add_sstable(const common::ObList<uint64_t> &sstable_list)
        {
          TBSYS_LOG(INFO, "add sstable list size=%lu", sstable_list.size());
          return OB_SUCCESS;
        };
    };

    class FP// : public IFetchParam
    {
      public:
        FP() {};
        virtual ~FP() {};
      public:
        virtual int add_sst_file_info(const SSTFileInfo &sst_file_info)
        {
          TBSYS_LOG(INFO, "add sst path=[%s] name=[%s]", sst_file_info.path.ptr(), sst_file_info.name.ptr());
          return OB_SUCCESS;
        };
        virtual void set_max_clog_id(const uint64_t max_clog_id)
        {
          TBSYS_LOG(INFO, "set clog id=%lu", max_clog_id);
        };
    };

    class TestSSTableInfo
    {
      public:
        static int main(int argc, char **argv)
        {
          SSTableMgr sstable_mgr;
          OBS obs;
          UNUSED(argc);

          char *root = argv[1];
          char *raid = argv[2];
          char *store = argv[3];
          int ret = sstable_mgr.init(root, raid, store);
          fprintf(stderr, "store_mgr init ret=%d\n", ret);

          ret = sstable_mgr.reg_observer(&obs);
          fprintf(stderr, "reg observer ret=%d\n", ret);

          sstable_mgr.load_new();

          for (int64_t i = 0; i < 10; i++)
          {
            uint64_t sstable_id = OB_INVALID_ID;
            uint64_t clog_id = OB_INVALID_ID;
            SSTableMgr::sstable_str2id(SSTABLE_FNAME2, sstable_id, clog_id);
            const IFileInfo *file_info = sstable_mgr.get_fileinfo(sstable_id);
            fprintf(stderr, "get fileinfo ret=%p\n", file_info);

            if (NULL != file_info)
            {
              int fd = file_info->get_fd();
              fprintf(stderr, "get fd=%d\n", fd);

              ret = sstable_mgr.revert_fileinfo(file_info);
              fprintf(stderr, "revert fileinfo ret=%d\n", ret);
            }
          }

          sstable_mgr.umount_store("data/raid1/store1");
          sstable_mgr.umount_store("data/raid1/store2");

          sstable_mgr.check_broken();

          for (int64_t i = 0; i < 10; i++)
          {
            uint64_t sstable_id = OB_INVALID_ID;
            uint64_t clog_id = OB_INVALID_ID;
            SSTableMgr::sstable_str2id(SSTABLE_FNAME, sstable_id, clog_id);
            const IFileInfo *file_info = sstable_mgr.get_fileinfo(sstable_id);
            fprintf(stderr, "get fileinfo ret=%p\n", file_info);

            if (NULL != file_info)
            {
              int fd = file_info->get_fd();
              fprintf(stderr, "get fd=%d\n", fd);

              ret = sstable_mgr.revert_fileinfo(file_info);
              fprintf(stderr, "revert fileinfo ret=%d\n", ret);
            }
          }
  
          symlink("/tmp", "data/raid1/store1");
          symlink("/home/yubai", "data/raid1/store2");

          sstable_mgr.load_new();

          for (int64_t i = 0; i < 10; i++)
          {
            uint64_t sstable_id = OB_INVALID_ID;
            uint64_t clog_id = OB_INVALID_ID;
            SSTableMgr::sstable_str2id(SSTABLE_FNAME2, sstable_id, clog_id);
            const IFileInfo *file_info = sstable_mgr.get_fileinfo(sstable_id);
            fprintf(stderr, "get fileinfo ret=%p\n", file_info);

            if (NULL != file_info)
            {
              int fd = file_info->get_fd();
              fprintf(stderr, "get fd=%d\n", fd);

              ret = sstable_mgr.revert_fileinfo(file_info);
              fprintf(stderr, "revert fileinfo ret=%d\n", ret);
            }
          }

          uint64_t sstable_id = OB_INVALID_ID;
          uint64_t clog_id = OB_INVALID_ID;
          SSTableMgr::sstable_str2id(SSTABLE_FNAME2, sstable_id, clog_id);
          sstable_mgr.erase_sstable(sstable_id, true);
          system("cp data/raid1/store1/10_1-1_1.sst data/raid1/store1/" SSTABLE_FNAME2);
          system("cp data/raid1/store2/10_1-1_1.sst data/raid1/store2/" SSTABLE_FNAME2);
          //system("cp data/raid1/store1/10_1-1_1.sst data/raid1/store1/12_1-1_12.sst");
          //system("cp data/raid1/store2/10_1-1_1.sst data/raid1/store2/12_1-1_12.sst");
          //system("cp data/raid1/store1/10_1-1_1.sst data/raid1/store1/13_1-1_13.sst");
          //system("cp data/raid1/store2/10_1-1_1.sst data/raid1/store2/13_1-1_13.sst");

          sstable_mgr.get_store_mgr().log_store_info();
          sstable_mgr.log_sstable_info();

          sstable_mgr.reload_all();

          ObUpsFetchParam fp;
          SSTableID id1;
          SSTableID id2;
          id1.major_version = 10;
          id1.minor_version_start = 1;
          id1.minor_version_end = 1;
          id2.major_version = 10;
          id2.minor_version_start = 3;
          id2.minor_version_end = 3;
          sstable_mgr.fill_fetch_param(id1.id, id2.id, 1, fp);
          fprintf(stderr, "fill fetch param succ\n");
          sstable_mgr.fill_fetch_param(id1.id, id2.id, 4, fp);
          fprintf(stderr, "fill fetch param succ\n");

          id1.major_version = 0;
          id2.major_version = 0;
          sstable_mgr.fill_fetch_param(id1.id, id2.id, 3, fp);
          fprintf(stderr, "fill fetch param succ\n");

          id1.id = UINT64_MAX;
          id2.id = 0;
          sstable_mgr.fill_fetch_param(id1.id, id2.id, 3, fp);
          fprintf(stderr, "fill fetch param succ\n");

          return 0;
        };
    };
  }
}

using namespace oceanbase;
using namespace common;
using namespace updateserver;

int main(int argc, char **argv)
{
  if (4 != argc)
  {
    fprintf(stderr, "./test_sstablemgr [store_root] [raid_regex] [store_regex]\n");
    exit(-1);
  }

  ob_init_memory_pool();
  //TBSYS_LOGGER.setFileName("test_sstablemgr.log");
  TBSYS_LOGGER.setLogLevel("info");
  return TestSSTableInfo::main(argc, argv);
}

