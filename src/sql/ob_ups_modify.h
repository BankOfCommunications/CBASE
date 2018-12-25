/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef _OB_UPS_MODIFY_H
#define _OB_UPS_MODIFY_H 1

#include "ob_husk_filter.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
    }

    class ObUpsModify : public ObHuskFilter<PHY_UPS_MODIFY>
    {
      public:
        ObDmlType get_dml_type() const {return OB_DML_REPLACE;};
    };

    class ObUpsModifyWithDmlType: public ObHuskFilter<PHY_UPS_MODIFY_WITH_DML_TYPE>
    {
      public:
        ObUpsModifyWithDmlType(): dml_type_(OB_DML_REPLACE) {}
        virtual ~ObUpsModifyWithDmlType() {}
      public:
        ObDmlType get_dml_type() const {return dml_type_;}
        void set_dml_type(const ObDmlType dml_type) {dml_type_ = dml_type;}
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
        DECLARE_PHY_OPERATOR_ASSIGN
        {
          int ret = OB_SUCCESS;
          const ObUpsModifyWithDmlType *sub_other = dynamic_cast<const ObUpsModifyWithDmlType*>(other);
          if (NULL == sub_other)
          {
            TBSYS_LOG(ERROR, "dynamic cast phy operator fail, other=%p", other);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            dml_type_ = sub_other->dml_type_;
          }
          return ret;
        }
      private:
        ObDmlType dml_type_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_MODIFY_H */
