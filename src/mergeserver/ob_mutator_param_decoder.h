/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_mutator_param_decoder.h,v 0.1 2011/12/21 17:57:20 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MUTATOR_PARAM_DECODER_H_
#define OCEANBASE_MUTATOR_PARAM_DECODER_H_

#include "ob_param_decoder.h"

namespace oceanbase
{
  namespace common
  {
    class ObObj;
    class ObMutator;
    class ObGetParam;
    class ObMutatorCellInfo;
  }

  namespace mergeserver
  {
    class ObMutatorParamDecoder:public ObParamDecoder
    {
    public:
      ObMutatorParamDecoder() {};
      virtual ~ObMutatorParamDecoder() {};

    private:
      /// decode mutator cell and add to get param
      static int decode_mutator(const common::ObMutator & org_mutator,
          const common::ObSchemaManagerV2 & schema, common::ObMutator & decoded_mutator,
          common::ObGetParam & return_param, common::ObGetParam & get_param);

    private:
      static int add_mutator(const common::ObMutatorCellInfo & decoded_cell,
          common::ObMutator & decoded_mutator, common::ObGetParam & get_param);

      /// add new cell to get param
      static int add_cell(const common::ObCellInfo & cell, common::ObGetParam & get_param);

    public:
      static int decode(const common::ObMutator & org_mutator,
          const common::ObSchemaManagerV2 & schema, common::ObMutator & decoded_mutator,
          common::ObGetParam & return_param, common::ObGetParam & get_param);
    };
  }
}


#endif //OCEANBASE_MUTATOR_PARAM_DECODER_H_
