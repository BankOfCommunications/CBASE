#include <iostream>
#include <sstream>
#include <gtest/gtest.h>
#include "file_utils.h"
#include "file_directory_utils.h"
using namespace oceanbase::common;

namespace oceanbase
{

namespace tests
{

namespace common
{

static const std::string basepath="/home/duanfei/local/oceanbase";

class TestFileDirectoryUtils: public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestFileDirectoryUtils, create_full_path)
{
  EXPECT_EQ(FileDirectoryUtils::create_full_path(basepath.c_str()), true);
  const std::string invalidpath = basepath + "/";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(invalidpath.c_str()), false);
  const std::string emptypath = "/";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(emptypath.c_str()), false);
  const std::string path= basepath + "/ invalidpath / test";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(path.c_str()), true);
}

TEST_F(TestFileDirectoryUtils, delete_directory)
{
  const std::string path = basepath + "/create_directory";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(path.c_str()), true);
  EXPECT_EQ(FileDirectoryUtils::delete_directory(path.c_str()), true);
  const std::string delete_dir_path = basepath + "/delete_directory";
  EXPECT_EQ(FileDirectoryUtils::create_directory(delete_dir_path.c_str()), true);
  EXPECT_EQ(FileDirectoryUtils::delete_directory(delete_dir_path.c_str()), true);
  EXPECT_EQ(FileDirectoryUtils::exists(delete_dir_path.c_str()), false);
}

TEST_F(TestFileDirectoryUtils, create_directory)
{
  const std::string path = basepath + "/create_directory";
  EXPECT_EQ(FileDirectoryUtils::create_directory(path.c_str()), true);
}

TEST_F(TestFileDirectoryUtils, exists)
{
  const std::string path = basepath + "/create_directory";
  EXPECT_EQ(FileDirectoryUtils::exists(path.c_str()), true);
}

TEST_F(TestFileDirectoryUtils, is_directory)
{
  const std::string path = basepath + "/create_directory";
  EXPECT_EQ(FileDirectoryUtils::is_directory(path.c_str()), true);

  const std::string filepath = basepath + "/create_directory/test_file";
  FileUtils files;
  int32_t flags = O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE;
  EXPECT_GE(files.open(filepath.c_str(), flags, 0640), 0);
  files.close();

  EXPECT_EQ(FileDirectoryUtils::is_directory(filepath.c_str()), false);
  EXPECT_EQ(FileDirectoryUtils::delete_file(filepath.c_str()), true);
}

TEST_F(TestFileDirectoryUtils, rename)
{
  const std::string path = basepath + "/renamedir";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(path.c_str()), true);

  const std::string rename_dir_path  = basepath + "/tmp_renamedir";
  EXPECT_EQ(FileDirectoryUtils::rename(path.c_str(),rename_dir_path.c_str()), true);
  EXPECT_EQ(FileDirectoryUtils::exists(path.c_str()), false);
  EXPECT_EQ(FileDirectoryUtils::exists(rename_dir_path.c_str()), true);

  EXPECT_EQ(FileDirectoryUtils::delete_directory(rename_dir_path.c_str()), true);
}

TEST_F(TestFileDirectoryUtils, get_size)
{
  const std::string path = basepath + "/get_size_dir";
  EXPECT_EQ(FileDirectoryUtils::create_full_path(path.c_str()), true);
  const std::string filepath = basepath + "/get_size_dir/test_file";

  FileUtils files;
  int32_t flags = O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE;
  EXPECT_GE(files.open(filepath.c_str(), flags, 0640), 0);
  char buf[0xff];
  EXPECT_EQ(files.write(buf, 0xff), 0xff);
  files.lseek(0, SEEK_SET);
  EXPECT_EQ(files.read(buf, 0xff), 0xff);
  files.close();
  
  EXPECT_EQ(FileDirectoryUtils::get_size(filepath.c_str()), 0xff);
  EXPECT_EQ(FileDirectoryUtils::delete_file(filepath.c_str()), true);
}

/*TEST_F(TestFileDirectoryUtils, scan_directory_tree)
{
  std::string fullpath = basepath + "/scandir";
  if (FileDirectoryUtils::exists(fullpath.c_str()))
  {
    EXPECT_EQ(FileDirectoryUtils::delete_direcotry_recursively(fullpath.c_str()), true);
  }
  for (int32_t i = 0; i < 10; ++i)
  {
    std::stringstream diros;
    diros << i;
    std::string dirpath = fullpath + "/" + diros.str();
    EXPECT_EQ(FileDirectoryUtils::create_full_path(dirpath.c_str()), true);
    for (int32_t j = 0; j < 10; ++j)
    {
      std::stringstream fileos;
      fileos << j;
      FileUtils files;
      std::string filepath = fullpath + "/" + diros.str() + "/log." + fileos.str(); 
      int32_t flags = O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE;
      EXPECT_GE(files.open(filepath.c_str(), flags, 0640), 0);
      files.close();
    }
  }  

  const int32_t iTotal = 10 * 10 + 10;
  FileSystemTreeEntry tree;
  EXPECT_EQ(FileDirectoryUtils::scan_directory_tree(fullpath.c_str(), tree, iTotal), iTotal);
  FileSystemTreeEntry othertree;
  EXPECT_EQ(FileDirectoryUtils::scan_directory_tree(fullpath.c_str(), othertree, 10), 10);
}*/

TEST_F(TestFileDirectoryUtils, delete_direcotry_recursively)
{
  FileUtils files;
  const std::string filepath = basepath + "/create_directory/test_file";
  EXPECT_GE(files.open(filepath.c_str(), O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, 0640), 0);
  files.close();
  EXPECT_EQ(FileDirectoryUtils::delete_direcotry_recursively(basepath.c_str()), true);
  EXPECT_EQ(FileDirectoryUtils::exists(basepath.c_str()), false);
}

}//end namespace common

}//end namespace tests

}//end namesapce oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
