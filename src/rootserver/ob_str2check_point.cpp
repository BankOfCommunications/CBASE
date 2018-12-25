/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "rootserver/ob_root_table2.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
int main(int argc, char* argv[])
{
  if (argc != 3)
  {
    TBSYS_LOG(INFO, "usage %s str_file_name checkpoint_file_name\n", argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ObTabletInfoManager* rt_tim = new ObTabletInfoManager();
  ObRootTable2* rt_table = new ObRootTable2(rt_tim);
  if (rt_tim == NULL || rt_table == NULL)
  {
    TBSYS_LOG(INFO, "new object error\n");
    return 0;
  }
  FILE *stream = fopen(argv[1], "r");
  if (stream == NULL)
  {
    TBSYS_LOG(INFO, "open str file error %s\n", argv[1]);
    return 0;
  }
  rt_tim->read_from_hex(stream);
  rt_table->read_from_hex(stream);
  fclose(stream);

  if (OB_SUCCESS != rt_table->write_to_file(argv[2]))
  {
    TBSYS_LOG(INFO, "write file error %s\n", argv[2]);
    return 0;
  }
  return 0;

}
