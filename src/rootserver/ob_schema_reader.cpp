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
#include "common/ob_malloc.h"
using namespace oceanbase::common;
int main(int argc, char* argv[])
{
  if (argc != 2)
  {
    printf("usage %s schema_file\n", argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  tbsys::CConfig c1;
  //mod liumz, [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160818:b
  ObSchemaManagerV2* nm = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, 1);
  if (NULL == nm)
  {
    printf("alloc memory for schema manager failed.\n");
  }
  else if (nm->parse_from_file(argv[1], c1))
  {
    nm->print_info();
  }
  /*
  ObSchemaManagerV2 mm(1);
  if ( mm.parse_from_file(argv[1], c1))
  {
    mm.print_info();
  }
  */
  //mod:e
  else
  {
    printf("parse file %s error\n",argv[1]);
  }
  //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160818:b
  if (nm != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, nm);
  }
  //add:e
  return 0;
}
