#include <gtest/gtest.h>
#include "ob_define.h"
#include "chunkserver/ob_disk_manager.h"
#include "common/ob_disk_checker.h"
#include "common/ob_malloc.h"

using namespace oceanbase;
using namespace oceanbase::chunkserver;
using namespace oceanbase::common;

TEST(ObDiskManager, direct_set_error)
{
  ObDiskManager disk_m;
  disk_m.scan("/data",256*1024*1024);
  int32_t disk_no_1 = 1;
  int32_t bad_disk = 0;

  ObDiskCheckerSingleton::get_instance().set_check_stat(disk_no_1, common::CHECK_ERROR);
  ASSERT_EQ(common::OB_IO_ERROR, disk_m.check_disk(bad_disk));
  ASSERT_EQ(disk_no_1, bad_disk);


  int32_t disk_no_2 = 2; 
  ObDiskCheckerSingleton::get_instance().set_check_stat(disk_no_2, common::CHECK_ERROR);
  ASSERT_EQ(common::OB_IO_ERROR, disk_m.check_disk(bad_disk));
  ASSERT_EQ(disk_no_1, bad_disk);

  ObDiskCheckerSingleton::get_instance().set_check_stat(disk_no_1, common::CHECK_NORMAL);
  ASSERT_EQ(common::OB_SUCCESS, disk_m.check_disk(bad_disk));


  ObDiskCheckerSingleton::get_instance().set_check_stat(disk_no_2, common::CHECK_ERROR);
  ASSERT_EQ(common::OB_IO_ERROR, disk_m.check_disk(bad_disk));
  ASSERT_EQ(disk_no_2, bad_disk);
}


TEST(obDiskManager,test)
{
  ObDiskManager disk_m;
  disk_m.scan("/data",256*1024*1024);
  disk_m.dump();

  printf("total capacity:%ld\n",disk_m.get_total_capacity());
  printf("total used:%ld\n",disk_m.get_total_used());

  for(int i=0;i<30;++i)
  {
    int32_t disk_no = disk_m.get_dest_disk();
    printf("get_disk_no : %d\n",disk_no);
    disk_m.add_used_space(disk_no,256*1024*1024);
  }
  
  for(int i=0;i<20;++i)
  {
    int32_t disk_no = disk_m.get_disk_for_migrate();
    printf("get_most_free_disk: %d\n",disk_no);
  }

  int32_t size = 0;
  const int32_t *disk_no_array = disk_m.get_disk_no_array(size);
  for(int32_t i=0;i<size;++i)
  {
    printf(" DISK NO : [%d] ",disk_no_array[i]);
  }
  printf ("\n");
}

int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


