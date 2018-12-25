#include "gtest/gtest.h"
#include "ob_malloc.h"

using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
