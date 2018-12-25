#include "ob_mysql_field.h"
#include "ob_mysql_result_set.h"
#include "common/ob_array.h"
using namespace oceanbase::common;

namespace oceanbase {
  namespace obmysql {
    int ObMySQLResultSet::next_field(ObMySQLField &obmf)
    {
      int ret = OB_SUCCESS;
      int64_t field_cnt = 0;
      const ObIArray<Field> &fields = get_field_columns();
      field_cnt = get_field_cnt();
      if (field_index_ >= field_cnt)
      {
        ret = OB_ITER_END;
      }
      if (OB_SUCCESS == ret)
      {
        ret = fields.at(field_index_++, obmf);
      }
      set_errcode(ret);
      return ret;
    }

    int ObMySQLResultSet::next_param(ObMySQLField &obmf)
    {
      int ret = OB_SUCCESS;
      int64_t field_cnt = 0;
      const ObIArray<Field> &fields = get_param_columns();
      field_cnt = get_param_cnt();
      if (param_index_ >= field_cnt)
      {
        ret = OB_ITER_END;
      }
      if (OB_SUCCESS == ret)
      {
        ret = fields.at(param_index_++, obmf);
      }
      set_errcode(ret);
      return ret;
    }

    uint64_t ObMySQLResultSet::get_field_cnt() const
    {
      int64_t cnt = 0;
      uint64_t ret = 0;

      cnt = get_field_columns().count();
      if (cnt >= 0)
      {
        ret = static_cast<uint64_t>(cnt);
      }

      return ret;
    }

    uint64_t ObMySQLResultSet::get_param_cnt() const
    {
      int64_t cnt = 0;
      uint64_t ret = 0;

      cnt = get_param_columns().count();
      if (cnt >= 0)
      {
        ret = static_cast<uint64_t>(cnt);
      }

      return ret;
    }

    int ObMySQLResultSet::next_row(ObMySQLRow &obmr)
    {
      return ObResultSet::get_next_row(obmr.row_);
    }
  } // end of namespace obmysql
} // end of namespace oceabase
