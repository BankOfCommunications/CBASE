/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_param_decoder.h,v 0.1 2011/12/02 10:10:30 zhidong Exp $
 *
 * Authors:
 *   xielun.szd <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_PARAM_DECODER_H_
#define OCEANBASE_PARAM_DECODER_H_

namespace oceanbase
{
  namespace common
  {
    class ObCondInfo;
    class ObCellInfo;
    class ObSchemaManagerV2;
  }

  namespace mergeserver
  {
    class ObParamDecoder
    {
    public:
      ObParamDecoder() {};
      virtual ~ObParamDecoder() {};

    private:
      // decode normal or ext type cell to id type
      static int decode_cell(const bool is_ext, const common::ObCellInfo & org_cell,
          const common::ObSchemaManagerV2 & schema, common::ObCellInfo & decoded_cell);
    public:
      /// decode org condition cell with names to id type decoded_cell
      /// warning: not deep copy org_cond.value_ into decoded_cell.value_
      static int decode_cond_cell(const common::ObCondInfo & org_cond,
          const common::ObSchemaManagerV2 & schema, common::ObCellInfo & decoded_cell);
      /// decode org cell with names to id type decoded_cell
      /// warning: not deep copy org_cell.value_ into decoded_cell.value_
      static int decode_org_cell(const common::ObCellInfo & org_cell,
          const common::ObSchemaManagerV2 & schema, common::ObCellInfo & decoded_cell);
    };
  }
}


#endif //OCEANBASE_PARAM_DECODER_H_
