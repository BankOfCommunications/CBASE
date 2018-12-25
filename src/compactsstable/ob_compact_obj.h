/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compact_obj.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_COMPACT_OBJ_H
#define OB_COMPACT_OBJ_H

#include <stdint.h>

namespace oceanbase
{
  namespace compactsstable
  {
    static const uint8_t COMPACT_OBJ_META_TYPE_BITS = 5;
    static const uint8_t COMPACT_OBJ_META_ATTR_BITS = 3;
    
    struct ObCompactObjMeta
    {
      union
      {
        int8_t meta_;
        struct
        {
          uint8_t attr_ : COMPACT_OBJ_META_ATTR_BITS;
          uint8_t type_ : COMPACT_OBJ_META_TYPE_BITS;          
        };
      };
    }; //__attribute__((packed));

    static const uint8_t COMPACT_OBJ_TYPE_NULL       = 0;
    static const uint8_t COMPACT_OBJ_TYPE_INT8       = 1;
    static const uint8_t COMPACT_OBJ_TYPE_INT16      = 2;
    static const uint8_t COMPACT_OBJ_TYPE_INT32      = 3;
    static const uint8_t COMPACT_OBJ_TYPE_INT64      = 4;
    static const uint8_t COMPACT_OBJ_TYPE_CHAR       = 5;
    static const uint8_t COMPACT_OBJ_TYPE_VARCHAR    = 6;
    static const uint8_t COMPACT_OBJ_TYPE_NUMBER     = 7;
    static const uint8_t COMPACT_OBJ_TYPE_PRECISE    = 8;
    static const uint8_t COMPACT_OBJ_TYPE_TIME       = 9;
    static const uint8_t COMPACT_OBJ_TYPE_CREATE     = 10;
    static const uint8_t COMPACT_OBJ_TYPE_MODIFY     = 11; 
    //add peiouya [DATE_TIME] 20150906:b       
    static const uint8_t COMPACT_OBJ_TYPE_DATE    = 12;
    static const uint8_t COMPACT_OBJ_TYPE_TIME2    = 13;
    //add 20150906:e
    static const uint8_t COMPACT_OBJ_TYPE_ESCAPE     = 0x1f;

    static const uint8_t COMPACT_OBJ_ATTR_NORMAL     = 0;
    static const uint8_t COMPACT_OBJ_ATTR_ADD        = 1;
    static const uint8_t COMPACT_OBJ_ATTR_NULL       = 2;
    static const uint8_t COMPACT_OBJ_ATTR_MIN        = 3;
    static const uint8_t COMPACT_OBJ_ATTR_MAX        = 4;

    static const uint8_t COMPACT_OBJ_ESCAPE_END_ROW  = 0;
    static const uint8_t COMPACT_OBJ_ESCAPE_DEL_ROW  = 1;
    static const uint8_t COMPACT_OBJ_ESCAPE_NOP      = 2;    

    static const uint8_t INT_DATA_SIZE[]             = {0,1,2,4,8};

    static const int64_t COMPACT_OBJ_MAX_VARCHAR_LEN = 65536L; // 64K
    static const uint8_t VARCHAR_LEN_TYPE_LEN        = 2;      // 用uint16_t来记录varhcar的长度
  }
}

#endif
