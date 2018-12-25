/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_infix_expression.cpp is for what ...
 *
 * Version: $id: ob_infix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */


#include "ob_infix_expression.h"
#include "ob_object.h"

namespace oceanbase
{
  namespace common
  {
    DEFINE_SERIALIZE(ObInfixExpression)
    {
      int err = OB_SUCCESS;
      ObObj obj;
      obj.set_varchar(expr_);
      err = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "infix expression serialization error");
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObInfixExpression)
    {
      ObObj obj;
      int err = OB_SUCCESS;

      if (OB_SUCCESS != (err = obj.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "deserialize object error");
      }
      else if (ObVarcharType != obj.get_type())
      {
        err = OB_OBJ_TYPE_ERROR;
        TBSYS_LOG(WARN, "unexpected infix expression element type. ObVarcharType expected"
            "[type:%d]", obj.get_type());
      }
      else if (OB_SUCCESS != (err = obj.get_varchar(expr_)))
      {
        TBSYS_LOG(WARN, "get varchar value failed [err:%d]", err);
      }
      return err;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObInfixExpression)
    {
      ObObj obj;
      obj.set_varchar(expr_);
      return  obj.get_serialize_size();
    }
  } /* common */
} /* namespace */

