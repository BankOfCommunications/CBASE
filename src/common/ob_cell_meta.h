/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_iterator.h,v 0.1 2010/08/18 13:24:51 chuanhui Exp $
 *
 * Authors:
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_COMMON_CELL_META_H_
#define OCEANBASE_COMMON_CELL_META_H_

#include "common/ob_common_param.h"

namespace oceanbase
{
  namespace common
  {
    struct ObCellMeta
    {
      const static int64_t TP_NULL              = 0;   // 空类型
      const static int64_t TP_INT8              = 1;
      const static int64_t TP_INT16             = 2;
      const static int64_t TP_INT32             = 3;
      const static int64_t TP_INT64             = 4;
      const static int64_t TP_CHAR              = 5;
      const static int64_t TP_VARCHAR           = 6;
      const static int64_t TP_DECIMAL           = 7;
      const static int64_t TP_TIME              = 8;
      const static int64_t TP_PRECISE_TIME      = 9;
      const static int64_t TP_CREATE_TIME       = 10;
      const static int64_t TP_MODIFY_TIME       = 11;

      const static int64_t TP_FLOAT             = 12; // 过期
      const static int64_t TP_DOUBLE            = 13; // 过期
      const static int64_t TP_EXTEND            = 14;
      const static int64_t TP_BOOL              = 15;
      //add peiouya [DATE_TIME] 20150906:b
      const static int64_t TP_DATE              = 17;
      const static int64_t TP_TIME2              = 18;
      //add 20150906:e

      const static int64_t TP_ESCAPE            = 0x1f;

      const static int64_t AR_NORMAL            = 0;
      const static int64_t AR_ADD               = 1;
      const static int64_t AR_NULL              = 2;
      const static int64_t AR_MIN               = 3;
      const static int64_t AR_MAX               = 4;

      const static int64_t ES_END_ROW           = 0;
      const static int64_t ES_DEL_ROW           = 1;
      const static int64_t ES_NOP_ROW           = 2;
      const static int64_t ES_NOT_EXIST_ROW     = 3;
      const static int64_t ES_NEW_ADD           = 4;
      const static int64_t ES_VALID             = 5;
      const static int64_t ES_TRUN_TAB          = 6; /*add zhaoqiong [Truncate Table]:20160318*/

      uint8_t type_:5;
      uint8_t attr_:3;

      ObCellMeta(): type_(TP_NULL), attr_(0)
      {
      }
    };

    struct ObDecimalMeta
    {
      uint16_t dec_precision_:7;
      uint16_t dec_scale_:6;
      //delete by wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
       //uint16_t dec_nwords_:3;
       //delete e
      //add by wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
      uint16_t dec_vscale_:6;
      //add e
    };
  }
}

#endif /* OCEANBASE_COMMON_CELL_META_H_ */

